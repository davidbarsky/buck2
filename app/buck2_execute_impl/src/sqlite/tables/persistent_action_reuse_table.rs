/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file. You may select, at
 * your option, one of the above-listed licenses.
 */

use std::sync::Arc;

use buck2_common::cas_digest::CasDigest;
use buck2_common::cas_digest::CasDigestData;
use buck2_common::cas_digest::CasDigestKind;
use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::TrackedFileDigest;
use buck2_core::content_hash::ContentBasedPathHash;
use buck2_error::BuckErrorContext;
use buck2_error::ErrorTag;
use buck2_error::conversion::from_any_with_tag;
use buck2_error::internal_error;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::materialize::materializer::ActionReuseFilteredInputs;
use buck2_execute::materialize::materializer::ActionReuseKey;
use buck2_execute::materialize::materializer::ActionReuseOutput;
use buck2_execute::materialize::materializer::ActionReuseOutputs;
use buck2_execute::materialize::materializer::ActionReuseWrite;
use buck2_execute::materialize::materializer::DepFileActionReuseData;
use buck2_execute::materialize::materializer::DepFileActionReuseKey;
use buck2_execute::materialize::materializer::IdenticalActionReuseKey;
use buck2_fs::paths::forward_rel_path::ForwardRelativePathBuf;
use chrono::DateTime;
use chrono::Utc;
use dupe::Dupe;
use parking_lot::Mutex;
use prost::Message;
use rusqlite::Connection;
use rusqlite::OptionalExtension;

const ENTRIES_TABLE: &str = "persistent_action_reuse_entries";

#[derive(Clone, PartialEq, Message)]
struct PersistedActionReuseKey {
    #[prost(oneof = "persisted_action_reuse_key::Kind", tags = "1, 2")]
    kind: Option<persisted_action_reuse_key::Kind>,
}

mod persisted_action_reuse_key {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "1")]
        IdenticalAction(super::PersistedIdenticalActionReuseKey),
        #[prost(message, tag = "2")]
        DepFile(super::PersistedDepFileReuseKey),
    }
}

#[derive(Clone, PartialEq, Message)]
struct PersistedIdenticalActionReuseKey {
    #[prost(message, required, tag = "1")]
    action_digest: PersistedActionReuseDigest,
    #[prost(message, required, tag = "2")]
    local_reuse_key: PersistedActionReuseDigest,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedDepFileReuseKey {
    #[prost(message, required, tag = "1")]
    common_digest: PersistedActionReuseDigest,
    #[prost(message, required, tag = "2")]
    declared_dep_files_digest: PersistedActionReuseDigest,
    #[prost(message, required, tag = "3")]
    execution_platform_digest: PersistedActionReuseDigest,
    #[prost(message, required, tag = "4")]
    local_reuse_key: PersistedActionReuseDigest,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedActionReuseDigest {
    #[prost(bytes = "vec", tag = "1")]
    hash: Vec<u8>,
    #[prost(uint32, tag = "2")]
    hash_kind: u32,
    #[prost(uint64, tag = "3")]
    size: u64,
}

impl<Kind: CasDigestKind> From<&CasDigest<Kind>> for PersistedActionReuseDigest {
    fn from(digest: &CasDigest<Kind>) -> Self {
        Self {
            hash: digest.raw_digest().as_bytes().to_vec(),
            hash_kind: digest.raw_digest().algorithm() as u32,
            size: digest.size(),
        }
    }
}

impl From<&CasDigestData> for PersistedActionReuseDigest {
    fn from(digest: &CasDigestData) -> Self {
        Self {
            hash: digest.raw_digest().as_bytes().to_vec(),
            hash_kind: digest.raw_digest().algorithm() as u32,
            size: digest.size(),
        }
    }
}

impl From<&IdenticalActionReuseKey> for PersistedActionReuseKey {
    fn from(key: &IdenticalActionReuseKey) -> Self {
        let IdenticalActionReuseKey {
            action_digest,
            local_reuse_key,
        } = key;
        Self {
            kind: Some(persisted_action_reuse_key::Kind::IdenticalAction(
                PersistedIdenticalActionReuseKey {
                    action_digest: action_digest.into(),
                    local_reuse_key: local_reuse_key.into(),
                },
            )),
        }
    }
}

impl From<&DepFileActionReuseKey> for PersistedActionReuseKey {
    fn from(key: &DepFileActionReuseKey) -> Self {
        let DepFileActionReuseKey {
            common_digest,
            declared_dep_files_digest,
            execution_platform_digest,
            local_reuse_key,
        } = key;
        Self {
            kind: Some(persisted_action_reuse_key::Kind::DepFile(
                PersistedDepFileReuseKey {
                    common_digest: common_digest.into(),
                    declared_dep_files_digest: declared_dep_files_digest.into(),
                    execution_platform_digest: execution_platform_digest.into(),
                    local_reuse_key: local_reuse_key.into(),
                },
            )),
        }
    }
}

impl From<&ActionReuseKey> for PersistedActionReuseKey {
    fn from(key: &ActionReuseKey) -> Self {
        match key {
            ActionReuseKey::IdenticalAction(key) => key.into(),
            ActionReuseKey::DepFile(key) => key.into(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PersistentActionReuseCleanStats {
    pub stale_entry_count: u64,
    pub cleaned_entry_count: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedOutputs {
    #[prost(message, repeated, tag = "1")]
    outputs: Vec<PersistedOutput>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedOutput {
    #[prost(oneof = "persisted_output::ContentHash", tags = "1, 2")]
    content_hash: Option<persisted_output::ContentHash>,
    #[prost(string, tag = "3")]
    output_path: String,
    #[prost(bytes = "vec", tag = "4")]
    artifact_value_fingerprint: Vec<u8>,
}

impl From<&ActionReuseOutput> for PersistedOutput {
    fn from(output: &ActionReuseOutput) -> Self {
        Self {
            output_path: output.output_path.as_str().to_owned(),
            content_hash: Some((&output.content_based_path_hash).into()),
            artifact_value_fingerprint: output.artifact_value_fingerprint.as_ref().to_vec(),
        }
    }
}

impl TryFrom<PersistedOutput> for ActionReuseOutput {
    type Error = buck2_error::Error;

    fn try_from(output: PersistedOutput) -> Result<Self, Self::Error> {
        let Some(content_hash) = output.content_hash else {
            return Err(internal_error!(
                "Action reuse entry is missing an output content hash"
            ));
        };
        Ok(Self {
            output_path: ForwardRelativePathBuf::try_from(output.output_path)
                .buck_error_context("reading action reuse logical output path")?,
            content_based_path_hash: ContentBasedPathHash::try_from(content_hash)
                .buck_error_context("reading action reuse output content hash")?,
            artifact_value_fingerprint: output
                .artifact_value_fingerprint
                .as_slice()
                .try_into()
                .buck_error_context("reading action reuse output fingerprint")?,
        })
    }
}

impl From<&ActionReuseOutputs> for PersistedOutputs {
    fn from(outputs: &ActionReuseOutputs) -> Self {
        Self {
            outputs: outputs.into_iter().map(PersistedOutput::from).collect(),
        }
    }
}

impl TryFrom<PersistedOutputs> for ActionReuseOutputs {
    type Error = buck2_error::Error;

    fn try_from(outputs: PersistedOutputs) -> Result<Self, Self::Error> {
        let mut decoded = Vec::with_capacity(outputs.outputs.len());
        for output in outputs.outputs {
            decoded.push(ActionReuseOutput::try_from(output)?);
        }
        Self::new(decoded)
    }
}

mod persisted_output {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(super) enum ContentHash {
        #[prost(string, tag = "1")]
        Specified(String),
        #[prost(enumeration = "super::PersistedContentHashPlaceholder", tag = "2")]
        Placeholder(i32),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
enum PersistedContentHashPlaceholder {
    OutputArtifact = 0,
    AqueryPlaceholder = 1,
    Scratch = 2,
    RelativePathResolution = 3,
    DepFilesPlaceholder = 4,
}

impl From<&ContentBasedPathHash> for persisted_output::ContentHash {
    fn from(content_hash: &ContentBasedPathHash) -> Self {
        match content_hash {
            ContentBasedPathHash::Specified(value) => Self::Specified(value.clone()),
            ContentBasedPathHash::OutputArtifact => {
                Self::Placeholder(PersistedContentHashPlaceholder::OutputArtifact as i32)
            }
            ContentBasedPathHash::AqueryPlaceholder => {
                Self::Placeholder(PersistedContentHashPlaceholder::AqueryPlaceholder as i32)
            }
            ContentBasedPathHash::Scratch => {
                Self::Placeholder(PersistedContentHashPlaceholder::Scratch as i32)
            }
            ContentBasedPathHash::RelativePathResolution => {
                Self::Placeholder(PersistedContentHashPlaceholder::RelativePathResolution as i32)
            }
            ContentBasedPathHash::DepFilesPlaceholder => {
                Self::Placeholder(PersistedContentHashPlaceholder::DepFilesPlaceholder as i32)
            }
        }
    }
}

impl TryFrom<persisted_output::ContentHash> for ContentBasedPathHash {
    type Error = buck2_error::Error;

    fn try_from(content_hash: persisted_output::ContentHash) -> Result<Self, Self::Error> {
        match content_hash {
            persisted_output::ContentHash::Specified(value) => {
                Ok(ContentBasedPathHash::Specified(value))
            }
            persisted_output::ContentHash::Placeholder(placeholder) => {
                let placeholder = PersistedContentHashPlaceholder::try_from(placeholder)?;
                Ok(match placeholder {
                    PersistedContentHashPlaceholder::OutputArtifact => {
                        ContentBasedPathHash::OutputArtifact
                    }
                    PersistedContentHashPlaceholder::AqueryPlaceholder => {
                        ContentBasedPathHash::AqueryPlaceholder
                    }
                    PersistedContentHashPlaceholder::Scratch => ContentBasedPathHash::Scratch,
                    PersistedContentHashPlaceholder::RelativePathResolution => {
                        ContentBasedPathHash::RelativePathResolution
                    }
                    PersistedContentHashPlaceholder::DepFilesPlaceholder => {
                        ContentBasedPathHash::DepFilesPlaceholder
                    }
                })
            }
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct PersistedFilteredInputs {
    #[prost(message, required, tag = "1")]
    untagged: PersistedFileDigest,
    #[prost(message, repeated, tag = "2")]
    tagged: Vec<PersistedTaggedInput>,
}

#[derive(Clone, PartialEq, Message)]
struct PersistedTaggedInput {
    #[prost(string, tag = "1")]
    tag: String,
    #[prost(message, required, tag = "2")]
    digest: PersistedFileDigest,
}

impl From<&ActionReuseFilteredInputs> for PersistedFilteredInputs {
    fn from(filtered_inputs: &ActionReuseFilteredInputs) -> Self {
        Self {
            untagged: (&filtered_inputs.untagged).into(),
            tagged: filtered_inputs
                .tagged
                .iter()
                .map(|(tag, digest)| PersistedTaggedInput {
                    tag: tag.clone(),
                    digest: digest.into(),
                })
                .collect(),
        }
    }
}

impl PersistedFilteredInputs {
    fn into_domain(
        self,
        digest_config: DigestConfig,
    ) -> buck2_error::Result<ActionReuseFilteredInputs> {
        let untagged = self.untagged.into_tracked(digest_config)?;
        let mut tagged = Vec::with_capacity(self.tagged.len());
        for PersistedTaggedInput { tag, digest } in self.tagged {
            tagged.push((tag, digest.into_tracked(digest_config)?));
        }

        Ok(ActionReuseFilteredInputs {
            untagged,
            tagged: tagged.into_iter().collect(),
        })
    }
}

#[derive(Clone, PartialEq, Message)]
struct PersistedFileDigest {
    #[prost(uint64, tag = "1")]
    size: u64,
    #[prost(bytes = "vec", tag = "2")]
    hash: Vec<u8>,
    #[prost(uint32, tag = "3")]
    hash_kind: u32,
}

impl From<&TrackedFileDigest> for PersistedFileDigest {
    fn from(digest: &TrackedFileDigest) -> Self {
        Self {
            size: digest.size(),
            hash: digest.raw_digest().as_bytes().to_vec(),
            hash_kind: digest.raw_digest().algorithm() as u32,
        }
    }
}

impl PersistedFileDigest {
    fn into_tracked(self, digest_config: DigestConfig) -> buck2_error::Result<TrackedFileDigest> {
        let hash_kind = u8::try_from(self.hash_kind)?;
        let hash_kind = hash_kind
            .try_into()
            .map_err(|error| from_any_with_tag(error, ErrorTag::Tier0))?;
        let digest = FileDigest::from_digest_bytes(hash_kind, &self.hash, self.size)?;
        Ok(TrackedFileDigest::new(
            digest,
            digest_config.cas_digest_config(),
        ))
    }
}

#[derive(Clone, Dupe)]
pub(crate) struct PersistentActionReuseSqliteTable {
    connection: Arc<Mutex<Connection>>,
}

impl PersistentActionReuseSqliteTable {
    pub(crate) fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }

    pub(crate) fn create_table(&self) -> buck2_error::Result<()> {
        let sql = format!(
            r#"CREATE TABLE {ENTRIES_TABLE} (
                cache_key        BLOB PRIMARY KEY,
                last_access_time INTEGER NOT NULL,
                output_payload   BLOB NOT NULL,
                filtered_inputs  BLOB NULL
            );
            CREATE INDEX persistent_action_reuse_entries_by_recency
             ON {ENTRIES_TABLE} (last_access_time);"#,
        );
        self.connection.lock().execute_batch(&sql)?;
        Ok(())
    }

    fn get(
        &self,
        key: &[u8],
    ) -> buck2_error::Result<Option<(ActionReuseOutputs, Option<Vec<u8>>)>> {
        let entry = self
            .connection
            .lock()
            .query_row(
                &format!(
                    "SELECT filtered_inputs, output_payload
                     FROM {ENTRIES_TABLE}
                     WHERE cache_key = ?1"
                ),
                [key],
                |row| {
                    Ok((
                        row.get::<_, Option<Vec<u8>>>("filtered_inputs")?,
                        row.get::<_, Vec<u8>>("output_payload")?,
                    ))
                },
            )
            .optional()?;

        let Some((filtered_inputs, output_payload)) = entry else {
            return Ok(None);
        };

        let outputs = PersistedOutputs::decode(output_payload.as_slice())?;
        Ok(Some((outputs.try_into()?, filtered_inputs)))
    }

    pub(crate) fn get_identical_action(
        &self,
        key: &IdenticalActionReuseKey,
    ) -> buck2_error::Result<Option<ActionReuseOutputs>> {
        let key = PersistedActionReuseKey::from(key).encode_to_vec();
        let Some((outputs, filtered_inputs)) = self.get(&key)? else {
            return Ok(None);
        };
        if filtered_inputs.is_some() {
            return Err(internal_error!(
                "Identical-action persistent reuse entry contains filtered inputs"
            ));
        }
        Ok(Some(outputs))
    }

    pub(crate) fn get_dep_file(
        &self,
        key: &DepFileActionReuseKey,
        digest_config: DigestConfig,
    ) -> buck2_error::Result<Option<DepFileActionReuseData>> {
        let key = PersistedActionReuseKey::from(key).encode_to_vec();
        let Some((outputs, filtered_inputs)) = self.get(&key)? else {
            return Ok(None);
        };
        let Some(filtered_inputs) = filtered_inputs else {
            return Err(internal_error!(
                "Dep-file persistent reuse entry is missing filtered inputs"
            ));
        };
        Ok(Some(DepFileActionReuseData {
            outputs,
            filtered_inputs: PersistedFilteredInputs::decode(filtered_inputs.as_slice())
                .buck_error_context("decoding persistent dep-file filtered inputs")?
                .into_domain(digest_config)?,
        }))
    }

    pub(crate) fn put(
        &self,
        write: &ActionReuseWrite,
        timestamp: DateTime<Utc>,
    ) -> buck2_error::Result<()> {
        let outputs = match write {
            ActionReuseWrite::IdenticalAction { outputs, .. }
            | ActionReuseWrite::DepFile { outputs, .. }
            | ActionReuseWrite::IdenticalActionAndDepFile { outputs, .. } => outputs,
        };
        let payload = PersistedOutputs::from(outputs).encode_to_vec();
        let mut connection = self.connection.lock();
        let tx = connection.transaction()?;
        {
            let mut insert_entry = tx.prepare(&format!(
                "INSERT OR REPLACE INTO {ENTRIES_TABLE}
                 (cache_key, last_access_time, output_payload, filtered_inputs)
                 VALUES (?1, ?2, ?3, ?4)"
            ))?;
            match write {
                ActionReuseWrite::IdenticalAction { key, .. } => {
                    let key = PersistedActionReuseKey::from(key).encode_to_vec();
                    insert_entry.execute(rusqlite::params![
                        key,
                        timestamp.timestamp_millis(),
                        &payload,
                        Option::<Vec<u8>>::None,
                    ])?;
                }
                ActionReuseWrite::DepFile { dep_file, .. } => {
                    let key = PersistedActionReuseKey::from(&dep_file.key).encode_to_vec();
                    insert_entry.execute(rusqlite::params![
                        key,
                        timestamp.timestamp_millis(),
                        &payload,
                        PersistedFilteredInputs::from(&dep_file.filtered_inputs).encode_to_vec(),
                    ])?;
                }
                ActionReuseWrite::IdenticalActionAndDepFile {
                    identical_action,
                    dep_file,
                    ..
                } => {
                    let identical_action =
                        PersistedActionReuseKey::from(identical_action).encode_to_vec();
                    insert_entry.execute(rusqlite::params![
                        identical_action,
                        timestamp.timestamp_millis(),
                        &payload,
                        Option::<Vec<u8>>::None,
                    ])?;
                    let dep_file_key = PersistedActionReuseKey::from(&dep_file.key).encode_to_vec();
                    insert_entry.execute(rusqlite::params![
                        dep_file_key,
                        timestamp.timestamp_millis(),
                        &payload,
                        PersistedFilteredInputs::from(&dep_file.filtered_inputs).encode_to_vec(),
                    ])?;
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub(crate) fn touch(
        &self,
        key: &ActionReuseKey,
        timestamp: DateTime<Utc>,
    ) -> buck2_error::Result<()> {
        let key = PersistedActionReuseKey::from(key).encode_to_vec();
        let connection = self.connection.lock();
        connection.execute(
            &format!("UPDATE {ENTRIES_TABLE} SET last_access_time = ?1 WHERE cache_key = ?2"),
            rusqlite::params![timestamp.timestamp_millis(), key],
        )?;
        Ok(())
    }

    pub(crate) fn clean_stale(
        &self,
        keep_since_time: DateTime<Utc>,
        dry_run: bool,
    ) -> buck2_error::Result<PersistentActionReuseCleanStats> {
        let connection = self.connection.lock();
        let keep_since_timestamp = keep_since_time.timestamp_millis();
        if dry_run {
            let stale_entry_count = connection.query_row(
                &format!(
                    "SELECT COUNT(*) AS entry_count
                     FROM {ENTRIES_TABLE} WHERE last_access_time < ?1"
                ),
                [keep_since_timestamp],
                |row| row.get::<_, u64>("entry_count"),
            )?;
            return Ok(PersistentActionReuseCleanStats {
                stale_entry_count,
                cleaned_entry_count: 0,
            });
        }

        let cleaned_entry_count = connection.execute(
            &format!("DELETE FROM {ENTRIES_TABLE} WHERE last_access_time < ?1"),
            [keep_since_timestamp],
        )?;
        let cleaned_entry_count = u64::try_from(cleaned_entry_count)?;

        Ok(PersistentActionReuseCleanStats {
            stale_entry_count: cleaned_entry_count,
            cleaned_entry_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use buck2_common::cas_digest::CasDigestData;
    use buck2_common::cas_digest::TrackedCasDigest;
    use buck2_core::fs::project::ProjectRootTemp;
    use buck2_execute::artifact_value::ArtifactValueFingerprint;
    use buck2_execute::execute::action_digest::ActionDigest;
    use buck2_execute::materialize::materializer::DepFileActionReuseWrite;
    use buck2_hash::StdBuckHashMap;
    use rusqlite::Connection;

    use super::*;
    use crate::sqlite::materializer_db::testing_materializer_state_sqlite_db;

    fn make_table() -> buck2_error::Result<PersistentActionReuseSqliteTable> {
        let connection = Connection::open_in_memory()?;
        let table = PersistentActionReuseSqliteTable::new(Arc::new(Mutex::new(connection)));
        table.create_table()?;
        Ok(table)
    }

    fn identical_action_key(seed: &[u8], digest_config: DigestConfig) -> IdenticalActionReuseKey {
        let action_digest: ActionDigest =
            FileDigest::from_content(seed, digest_config.cas_digest_config()).coerce();
        let mut local_reuse_key = CasDigestData::digester(digest_config.cas_digest_config());
        local_reuse_key.update(b"local-reuse-key");
        local_reuse_key.update(seed);
        IdenticalActionReuseKey {
            action_digest,
            local_reuse_key: local_reuse_key.finalize(),
        }
    }

    fn dep_file_key(seed: &[u8], digest_config: DigestConfig) -> DepFileActionReuseKey {
        let digest = |domain: &[u8]| {
            let mut digest = CasDigestData::digester(digest_config.cas_digest_config());
            digest.update(domain);
            digest.update(seed);
            digest.finalize()
        };
        DepFileActionReuseKey {
            common_digest: digest(b"common"),
            declared_dep_files_digest: digest(b"declared-dep-files"),
            execution_platform_digest: digest(b"execution-platform"),
            local_reuse_key: digest(b"local-reuse-key"),
        }
    }

    #[test]
    fn persisted_identical_action_key_contains_every_component() {
        let key = identical_action_key(b"identical-key", DigestConfig::testing_default());
        let persisted = PersistedActionReuseKey::from(&key);
        let Some(persisted_action_reuse_key::Kind::IdenticalAction(persisted)) = persisted.kind
        else {
            panic!("identical-action key used the dep-file variant");
        };

        assert_eq!(
            persisted.action_digest.hash,
            key.action_digest.raw_digest().as_bytes()
        );
        assert_eq!(
            persisted.action_digest.hash_kind,
            key.action_digest.raw_digest().algorithm() as u32
        );
        assert_eq!(persisted.action_digest.size, key.action_digest.size());
        assert_eq!(
            persisted.local_reuse_key.hash,
            key.local_reuse_key.raw_digest().as_bytes()
        );
        assert_eq!(
            persisted.local_reuse_key.hash_kind,
            key.local_reuse_key.raw_digest().algorithm() as u32
        );
        assert_eq!(persisted.local_reuse_key.size, key.local_reuse_key.size());
    }

    #[test]
    fn persisted_dep_file_key_contains_every_component() {
        let key = dep_file_key(b"dep-file-key", DigestConfig::testing_default());
        let persisted = PersistedActionReuseKey::from(&key);
        let Some(persisted_action_reuse_key::Kind::DepFile(persisted)) = persisted.kind else {
            panic!("dep-file key used the identical-action variant");
        };

        for (persisted, domain) in [
            (persisted.common_digest, key.common_digest),
            (
                persisted.declared_dep_files_digest,
                key.declared_dep_files_digest,
            ),
            (
                persisted.execution_platform_digest,
                key.execution_platform_digest,
            ),
            (persisted.local_reuse_key, key.local_reuse_key),
        ] {
            assert_eq!(persisted.hash, domain.raw_digest().as_bytes());
            assert_eq!(persisted.hash_kind, domain.raw_digest().algorithm() as u32);
            assert_eq!(persisted.size, domain.size());
        }
    }

    #[test]
    fn every_content_based_path_hash_round_trips_at_the_sqlite_boundary() {
        for content_hash in [
            ContentBasedPathHash::Specified("0123456789abcdef".to_owned()),
            ContentBasedPathHash::OutputArtifact,
            ContentBasedPathHash::AqueryPlaceholder,
            ContentBasedPathHash::Scratch,
            ContentBasedPathHash::RelativePathResolution,
            ContentBasedPathHash::DepFilesPlaceholder,
        ] {
            let persisted = persisted_output::ContentHash::from(&content_hash);
            assert_eq!(
                ContentBasedPathHash::try_from(persisted).unwrap(),
                content_hash
            );
        }
    }

    #[test]
    fn typed_lookups_round_trip_the_same_output_references() {
        let table = make_table().unwrap();
        let digest_config = DigestConfig::testing_default();
        let identical_key = identical_action_key(b"identical", digest_config);
        let dep_file_key = dep_file_key(b"dep-file", digest_config);
        let outputs = ActionReuseOutputs::new(vec![
            ActionReuseOutput {
                output_path: ForwardRelativePathBuf::unchecked_new("second".to_owned()),
                content_based_path_hash: ContentBasedPathHash::Scratch,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[2; 32][..])
                    .unwrap(),
            },
            ActionReuseOutput {
                output_path: ForwardRelativePathBuf::unchecked_new("first".to_owned()),
                content_based_path_hash: ContentBasedPathHash::Specified(
                    "0123456789abcdef".to_owned(),
                ),
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[1; 32][..])
                    .unwrap(),
            },
        ])
        .unwrap();
        let filtered_inputs = ActionReuseFilteredInputs {
            untagged: TrackedCasDigest::from_content(
                b"untagged",
                digest_config.cas_digest_config(),
            ),
            tagged: [
                (
                    "first-tag".to_owned(),
                    TrackedCasDigest::from_content(
                        b"first-tagged-input",
                        digest_config.cas_digest_config(),
                    ),
                ),
                (
                    "second-tag".to_owned(),
                    TrackedCasDigest::from_content(
                        b"second-tagged-input",
                        digest_config.cas_digest_config(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        };

        table
            .put(
                &ActionReuseWrite::IdenticalActionAndDepFile {
                    identical_action: identical_key.clone(),
                    dep_file: DepFileActionReuseWrite {
                        key: dep_file_key.clone(),
                        filtered_inputs: filtered_inputs.clone(),
                    },
                    outputs: outputs.clone(),
                },
                DateTime::<Utc>::from_timestamp_millis(1_000).unwrap(),
            )
            .unwrap();

        assert_eq!(
            table.get_identical_action(&identical_key).unwrap(),
            Some(outputs.clone())
        );
        assert_eq!(
            table.get_dep_file(&dep_file_key, digest_config).unwrap(),
            Some(DepFileActionReuseData {
                outputs,
                filtered_inputs,
            })
        );
        let counts = table
            .connection
            .lock()
            .query_row(
                &format!("SELECT COUNT(*), COUNT(DISTINCT output_payload) FROM {ENTRIES_TABLE}"),
                [],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (2, 1));
    }

    #[test]
    fn put_is_atomic_when_one_lookup_fails() {
        let table = make_table().unwrap();
        let digest_config = DigestConfig::testing_default();
        let identical_key = identical_action_key(b"atomic-identical", digest_config);
        let dep_file_key = dep_file_key(b"atomic-dep-file", digest_config);
        {
            let connection = table.connection.lock();
            let dep_file_key = PersistedActionReuseKey::from(&dep_file_key).encode_to_vec();
            let dep_file_key_sql = connection
                .query_row("SELECT quote(?1)", [dep_file_key], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap();
            connection
                .execute_batch(&format!(
                    "CREATE TEMP TRIGGER reject_dep_file_entry
                     BEFORE INSERT ON {ENTRIES_TABLE}
                     WHEN NEW.cache_key = {dep_file_key_sql}
                     BEGIN SELECT RAISE(ABORT, 'rejected'); END;"
                ))
                .unwrap();
        }

        let result = table.put(
            &ActionReuseWrite::IdenticalActionAndDepFile {
                identical_action: identical_key.clone(),
                dep_file: DepFileActionReuseWrite {
                    key: dep_file_key.clone(),
                    filtered_inputs: ActionReuseFilteredInputs {
                        untagged: TrackedCasDigest::from_content(
                            b"untagged",
                            digest_config.cas_digest_config(),
                        ),
                        tagged: Default::default(),
                    },
                },
                outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                    output_path: ForwardRelativePathBuf::unchecked_new("out".to_owned()),
                    content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                    artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[0; 32][..])
                        .unwrap(),
                }])
                .unwrap(),
            },
            DateTime::<Utc>::from_timestamp_millis(1_000).unwrap(),
        );

        assert!(result.is_err());
        assert!(
            table
                .get_identical_action(&identical_key)
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_dep_file(&dep_file_key, digest_config)
                .unwrap()
                .is_none()
        );
        let entry_count = table
            .connection
            .lock()
            .query_row(
                &format!("SELECT COUNT(*) FROM {ENTRIES_TABLE}"),
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap();
        assert_eq!(entry_count, 0);
    }

    #[test]
    fn replacing_a_key_replaces_its_output_payload() {
        let table = make_table().unwrap();
        let digest_config = DigestConfig::testing_default();
        let key = identical_action_key(b"replace", digest_config);
        for (timestamp, path, content_based_path_hash) in [
            (1_000, "old", ContentBasedPathHash::OutputArtifact),
            (2_000, "new", ContentBasedPathHash::Scratch),
        ] {
            table
                .put(
                    &ActionReuseWrite::IdenticalAction {
                        key: key.clone(),
                        outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                            output_path: ForwardRelativePathBuf::unchecked_new(path.to_owned()),
                            content_based_path_hash,
                            artifact_value_fingerprint: ArtifactValueFingerprint::try_from(
                                &[0; 32][..],
                            )
                            .unwrap(),
                        }])
                        .unwrap(),
                    },
                    DateTime::<Utc>::from_timestamp_millis(timestamp).unwrap(),
                )
                .unwrap();
        }

        let outputs = table.get_identical_action(&key).unwrap().unwrap();
        assert_eq!(outputs[0].output_path.as_str(), "new");
        let entry_count = table
            .connection
            .lock()
            .query_row(
                &format!("SELECT COUNT(*) FROM {ENTRIES_TABLE}"),
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap();
        assert_eq!(entry_count, 1);
    }

    #[test]
    fn touch_updates_the_recency_used_by_clean_stale() {
        let table = make_table().unwrap();
        let digest_config = DigestConfig::testing_default();
        let key = identical_action_key(b"touch", digest_config);
        table
            .put(
                &ActionReuseWrite::IdenticalAction {
                    key: key.clone(),
                    outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                        output_path: ForwardRelativePathBuf::unchecked_new("out".to_owned()),
                        content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                        artifact_value_fingerprint: ArtifactValueFingerprint::try_from(
                            &[0; 32][..],
                        )
                        .unwrap(),
                    }])
                    .unwrap(),
                },
                DateTime::<Utc>::from_timestamp_millis(1_000).unwrap(),
            )
            .unwrap();
        table
            .touch(
                &ActionReuseKey::from(key.clone()),
                DateTime::<Utc>::from_timestamp_millis(3_000).unwrap(),
            )
            .unwrap();

        let stats = table
            .clean_stale(
                DateTime::<Utc>::from_timestamp_millis(2_000).unwrap(),
                false,
            )
            .unwrap();

        assert_eq!(stats.stale_entry_count, 0);
        assert_eq!(stats.cleaned_entry_count, 0);
        assert!(table.get_identical_action(&key).unwrap().is_some());
    }

    #[test]
    fn clean_stale_removes_both_typed_lookup_rows() {
        let table = make_table().unwrap();
        let digest_config = DigestConfig::testing_default();
        let identical_key = identical_action_key(b"shared-identical", digest_config);
        let dep_file_key = dep_file_key(b"shared-dep-file", digest_config);
        table
            .put(
                &ActionReuseWrite::IdenticalActionAndDepFile {
                    identical_action: identical_key.clone(),
                    dep_file: DepFileActionReuseWrite {
                        key: dep_file_key,
                        filtered_inputs: ActionReuseFilteredInputs {
                            untagged: TrackedCasDigest::from_content(
                                b"untagged",
                                digest_config.cas_digest_config(),
                            ),
                            tagged: Default::default(),
                        },
                    },
                    outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                        output_path: ForwardRelativePathBuf::unchecked_new(
                            "shared-output".to_owned(),
                        ),
                        content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                        artifact_value_fingerprint: ArtifactValueFingerprint::try_from(
                            &[0; 32][..],
                        )
                        .unwrap(),
                    }])
                    .unwrap(),
                },
                DateTime::<Utc>::from_timestamp_millis(1_000).unwrap(),
            )
            .unwrap();

        let stats = table
            .clean_stale(
                DateTime::<Utc>::from_timestamp_millis(1_001).unwrap(),
                false,
            )
            .unwrap();

        assert_eq!(stats.stale_entry_count, 2);
        assert_eq!(stats.cleaned_entry_count, 2);
        let entry_count = table
            .connection
            .lock()
            .query_row(
                &format!("SELECT COUNT(*) FROM {ENTRIES_TABLE}"),
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap();
        assert_eq!(entry_count, 0);
    }

    #[test]
    fn dep_file_lookup_rejects_missing_filtered_inputs() {
        let table = make_table().unwrap();
        let digest_config = DigestConfig::testing_default();
        let key = dep_file_key(b"missing-filtered-inputs", digest_config);
        table
            .put(
                &ActionReuseWrite::DepFile {
                    dep_file: DepFileActionReuseWrite {
                        key: key.clone(),
                        filtered_inputs: ActionReuseFilteredInputs {
                            untagged: TrackedCasDigest::from_content(
                                b"untagged",
                                digest_config.cas_digest_config(),
                            ),
                            tagged: Default::default(),
                        },
                    },
                    outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                        output_path: ForwardRelativePathBuf::unchecked_new("out".to_owned()),
                        content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                        artifact_value_fingerprint: ArtifactValueFingerprint::try_from(
                            &[0; 32][..],
                        )
                        .unwrap(),
                    }])
                    .unwrap(),
                },
                Utc::now(),
            )
            .unwrap();
        table
            .connection
            .lock()
            .execute(
                &format!("UPDATE {ENTRIES_TABLE} SET filtered_inputs = NULL"),
                [],
            )
            .unwrap();

        assert!(table.get_dep_file(&key, digest_config).is_err());
    }

    #[test]
    fn persistent_entries_survive_materializer_database_restart() {
        let project_root = ProjectRootTemp::new().unwrap();
        let digest_config = DigestConfig::testing_default();
        let key = identical_action_key(b"restart", digest_config);
        let metadata: StdBuckHashMap<_, _> = [("schema".to_owned(), "1".to_owned())]
            .into_iter()
            .collect();
        let (database, _) = testing_materializer_state_sqlite_db(
            project_root.path(),
            metadata.clone(),
            Default::default(),
            None,
        )
        .unwrap();
        database
            .tables
            .domain_table
            .persistent_action_reuse
            .put(
                &ActionReuseWrite::IdenticalAction {
                    key: key.clone(),
                    outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                        output_path: ForwardRelativePathBuf::unchecked_new("out".to_owned()),
                        content_based_path_hash: ContentBasedPathHash::DepFilesPlaceholder,
                        artifact_value_fingerprint: ArtifactValueFingerprint::try_from(
                            &[0; 32][..],
                        )
                        .unwrap(),
                    }])
                    .unwrap(),
                },
                DateTime::<Utc>::from_timestamp_millis(1_000).unwrap(),
            )
            .unwrap();
        drop(database);

        let (database, load_result) = testing_materializer_state_sqlite_db(
            project_root.path(),
            metadata,
            Default::default(),
            None,
        )
        .unwrap();

        assert!(load_result.unwrap().is_empty());
        assert_eq!(
            database
                .tables
                .domain_table
                .persistent_action_reuse
                .get_identical_action(&key)
                .unwrap()
                .unwrap()[0]
                .content_based_path_hash,
            ContentBasedPathHash::DepFilesPlaceholder
        );
    }

    #[test]
    fn clean_stale_uses_the_recency_index() {
        let table = make_table().unwrap();
        let connection = table.connection.lock();
        let plan = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN
                 SELECT COUNT(*) FROM {ENTRIES_TABLE}
                 WHERE last_access_time < ?1"
            ))
            .unwrap()
            .query_map([1_000], |row| row.get::<_, String>("detail"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");

        assert!(plan.contains("persistent_action_reuse_entries_by_recency"));
    }
}
