/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::LazyLock;

use buck2_common::external_symlink::ExternalSymlink;
use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::FileMetadata;
use buck2_common::file_ops::metadata::Symlink;
use buck2_common::file_ops::metadata::TrackedFileDigest;
use buck2_core::content_hash::ContentBasedPathHash;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_directory::directory::directory_iterator::DirectoryIterator;
use buck2_directory::directory::directory_iterator::DirectoryIteratorPathStack;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_directory::directory::walk::unordered_entry_walk;
use buck2_error::BuckErrorContext;
use buck2_error::conversion::from_any_with_tag;
use buck2_error::internal_error;
use buck2_execute::artifact_value::ArtifactValue;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::directory::ActionDirectoryBuilder;
use buck2_execute::directory::ActionDirectoryEntry;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_execute::directory::ActionSharedDirectory;
use buck2_execute::directory::INTERNER;
use buck2_execute::directory::directory_to_re_tree;
use buck2_execute::directory::re_tree_to_directory;
use buck2_fs::paths::forward_rel_path::ForwardRelativePathBuf;
use buck2_hash::StdBuckHashMap;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use gazebo::prelude::*;
use itertools::Itertools;
use parking_lot::Mutex;
use prost::Message;
use remote_execution as RE;
use rusqlite::Connection;

use crate::materializers::artifact_type::ARTIFACT_TYPE_DIRECTORY;
use crate::materializers::artifact_type::ARTIFACT_TYPE_EXTERNAL_SYMLINK;
use crate::materializers::artifact_type::ARTIFACT_TYPE_FILE;
use crate::materializers::artifact_type::ARTIFACT_TYPE_SYMLINK;
use crate::materializers::artifact_type::ArtifactType;
#[cfg(test)]
use crate::materializers::deferred::artifact_tree::ArtifactMetadata;
use crate::sqlite::materializer_db::MaterializerState;
use crate::sqlite::materializer_db::MaterializerStateEntry;

const STATE_TABLE_NAME: &str = "materializer_state";

#[derive(buck2_error::Error, Debug, PartialEq, Eq)]
#[buck2(tag = Tier0)]
enum MaterializerStateTableError {
    #[error("Internal error: expected field `{}` to be not null for artifact type '{}'", .field, .artifact_type)]
    ExpectedFieldIsMissing {
        field: &'static str,
        artifact_type: ArtifactType,
    },
    #[error(
        "Internal error: expected last access timestamp to be present for sqlite entry which represents an artifact"
    )]
    ExpectedLastAccessTimeForArtifact,
    #[error("Internal error: code should not be reachable for sqlite entry of directory type")]
    CodePathNotSupportedForDirEntry,
    #[error("Internal error: missing an sqlite entry representing directory artifact `{0}`")]
    DirectoryArtifactEntryMissing(ProjectRelativePathBuf),
}

/// Sqlite representation of sha1. Can be converted directly into BLOB type
/// in rusqlite. Note we use a Vec and not a fixed length array here because
/// rusqlite can only convert to Vec.
type SqliteDigest = Vec<u8>;

/// Representation of row in sqlite table. All datatypes used implement rusqlite's `FromSql` trait.
/// A single row might represent different things:
/// 1. Row fully represent an artifact.
///    That could be file, symlink or external symlink.
/// 2. Row represents a directory artifact.
///    In this case all directory's content will be stored in other rows as well in order to be able to recreate [`ActionSharedDirectory`] from DB.
///    Only `path`, `last_access_time` and `artifact_type` equal to [`ArtifactType::Directory`] will be present for such rows.
///    Other rows which have `parent_path` equal to `path` of this row represent directory's content.
/// 3. Row represents a content inside a directory.
///    `path` is prefixed by a directory artifact path (not relative to containing directory).
///    `parent_path` will be present for such rows. `last_access_time` will be missing from such rows.
#[derive(Debug)]
struct SqliteEntry<'a> {
    path: Cow<'a, str>,
    artifact_type: ArtifactType,
    entry_size: Option<u64>,
    entry_hash: Option<Cow<'a, [u8]>>,
    entry_hash_kind: Option<u8>,
    file_is_executable: Option<bool>,
    symlink_target: Option<Cow<'a, str>>,
    /// Only present for entries which represent an actual artifact.
    last_access_time: Option<i64>,
    /// Path of the directory artifact which this entry belongs to.
    /// Entry represents the actual artifact when the value is not present.
    parent_path: Option<Cow<'a, str>>,
    deps: Option<Cow<'a, [u8]>>,
    content_hash_kind: Option<i32>,
    content_hash: Option<Cow<'a, str>>,
}

#[derive(Clone, Copy)]
#[repr(i32)]
enum PersistedContentHashKind {
    Specified = 0,
    OutputArtifact = 1,
    AqueryPlaceholder = 2,
    Scratch = 3,
    RelativePathResolution = 4,
    DepFilesPlaceholder = 5,
}

impl TryFrom<i32> for PersistedContentHashKind {
    type Error = buck2_error::Error;

    fn try_from(kind: i32) -> Result<Self, Self::Error> {
        match kind {
            kind if kind == Self::Specified as i32 => Ok(Self::Specified),
            kind if kind == Self::OutputArtifact as i32 => Ok(Self::OutputArtifact),
            kind if kind == Self::AqueryPlaceholder as i32 => Ok(Self::AqueryPlaceholder),
            kind if kind == Self::Scratch as i32 => Ok(Self::Scratch),
            kind if kind == Self::RelativePathResolution as i32 => Ok(Self::RelativePathResolution),
            kind if kind == Self::DepFilesPlaceholder as i32 => Ok(Self::DepFilesPlaceholder),
            kind => Err(internal_error!(
                "Invalid persisted content hash kind `{kind}`"
            )),
        }
    }
}

struct PersistedContentHash<'a> {
    kind: Option<i32>,
    value: Option<Cow<'a, str>>,
}

impl<'a> From<Option<&'a ContentBasedPathHash>> for PersistedContentHash<'a> {
    fn from(content_hash: Option<&'a ContentBasedPathHash>) -> Self {
        let Some(content_hash) = content_hash else {
            return Self {
                kind: None,
                value: None,
            };
        };
        let (kind, value) = match content_hash {
            ContentBasedPathHash::Specified(value) => (
                PersistedContentHashKind::Specified,
                Some(Cow::Borrowed(value.as_str())),
            ),
            ContentBasedPathHash::OutputArtifact => {
                (PersistedContentHashKind::OutputArtifact, None)
            }
            ContentBasedPathHash::AqueryPlaceholder => {
                (PersistedContentHashKind::AqueryPlaceholder, None)
            }
            ContentBasedPathHash::Scratch => (PersistedContentHashKind::Scratch, None),
            ContentBasedPathHash::RelativePathResolution => {
                (PersistedContentHashKind::RelativePathResolution, None)
            }
            ContentBasedPathHash::DepFilesPlaceholder => {
                (PersistedContentHashKind::DepFilesPlaceholder, None)
            }
        };
        Self {
            kind: Some(kind as i32),
            value,
        }
    }
}

impl TryFrom<PersistedContentHash<'_>> for Option<ContentBasedPathHash> {
    type Error = buck2_error::Error;

    fn try_from(content_hash: PersistedContentHash<'_>) -> Result<Self, Self::Error> {
        let Some(kind) = content_hash.kind else {
            return if content_hash.value.is_none() {
                Ok(None)
            } else {
                Err(internal_error!(
                    "Persisted content hash value is missing its kind"
                ))
            };
        };
        let kind = PersistedContentHashKind::try_from(kind)?;
        let content_hash = match kind {
            PersistedContentHashKind::Specified => match content_hash.value {
                Some(value) => ContentBasedPathHash::Specified(value.into_owned()),
                None => return Err(internal_error!("Missing specified content hash")),
            },
            PersistedContentHashKind::OutputArtifact => match content_hash.value {
                None => ContentBasedPathHash::OutputArtifact,
                Some(_) => return Err(internal_error!("Output artifact hash contained a value")),
            },
            PersistedContentHashKind::AqueryPlaceholder => match content_hash.value {
                None => ContentBasedPathHash::AqueryPlaceholder,
                Some(_) => {
                    return Err(internal_error!("Aquery placeholder hash contained a value"));
                }
            },
            PersistedContentHashKind::Scratch => match content_hash.value {
                None => ContentBasedPathHash::Scratch,
                Some(_) => return Err(internal_error!("Scratch hash contained a value")),
            },
            PersistedContentHashKind::RelativePathResolution => match content_hash.value {
                None => ContentBasedPathHash::RelativePathResolution,
                Some(_) => {
                    return Err(internal_error!(
                        "Relative path resolution hash contained a value"
                    ));
                }
            },
            PersistedContentHashKind::DepFilesPlaceholder => match content_hash.value {
                None => ContentBasedPathHash::DepFilesPlaceholder,
                Some(_) => {
                    return Err(internal_error!(
                        "Dep-files placeholder hash contained a value"
                    ));
                }
            },
        };
        Ok(Some(content_hash))
    }
}

fn digest_parts(digest: &TrackedFileDigest) -> (u64, &[u8], u8) {
    (
        digest.size(),
        digest.raw_digest().as_bytes(),
        digest.raw_digest().algorithm() as _,
    )
}

fn convert_artifact_value_to_sqlite_entries<'a>(
    path: &'a ProjectRelativePath,
    value: &'a ArtifactValue,
    timestamp: &'_ DateTime<Utc>,
) -> Vec<SqliteEntry<'a>> {
    let last_access_time = timestamp.timestamp();
    let mut entries = match value.entry() {
        DirectoryEntry::Dir(directory) => {
            convert_action_shared_directory_to_sqlite_entries(path, directory, last_access_time)
        }
        DirectoryEntry::Leaf(member) => vec![convert_action_directory_member_to_sqlite_entry(
            Cow::Borrowed(path.as_str()),
            member,
            Some(last_access_time),
            None,
        )],
    };
    let root = &mut entries[0];
    root.deps = value
        .deps()
        .map(directory_to_re_tree)
        .map(|tree| Cow::Owned(tree.encode_to_vec()));
    let content_hash = PersistedContentHash::from(value.explicit_content_based_path_hash());
    root.content_hash_kind = content_hash.kind;
    root.content_hash = content_hash.value;
    entries
}

fn convert_action_shared_directory_to_sqlite_entries<'a>(
    path: &'a ProjectRelativePath,
    action_shared_directory: &'a ActionSharedDirectory,
    last_access_time: i64,
) -> Vec<SqliteEntry<'a>> {
    let artifact_path = Cow::Borrowed(path.as_str());
    let mut result = vec![SqliteEntry {
        path: artifact_path.clone(),
        artifact_type: ArtifactType::Directory,
        entry_size: None,
        entry_hash: None,
        entry_hash_kind: None,
        file_is_executable: None,
        symlink_target: None,
        last_access_time: Some(last_access_time),
        parent_path: None,
        deps: None,
        content_hash_kind: None,
        content_hash: None,
    }];
    // Now create and entry for each transitive member of this directory
    let mut walk = unordered_entry_walk(DirectoryEntry::Dir(action_shared_directory));
    while let Some((entry_path, entry)) = walk.next() {
        let parent_path = Some(artifact_path.clone());
        let project_relative_entry_path = {
            let mut res = path.to_owned();
            res.push(entry_path.get());
            Cow::Owned(res.into_forward_relative_path_buf().into_string())
        };
        result.push(match entry {
            DirectoryEntry::Dir(_) => SqliteEntry {
                path: project_relative_entry_path,
                artifact_type: ArtifactType::Directory,
                entry_size: None,
                entry_hash: None,
                entry_hash_kind: None,
                file_is_executable: None,
                symlink_target: None,
                last_access_time: None,
                parent_path,
                deps: None,
                content_hash_kind: None,
                content_hash: None,
            },
            DirectoryEntry::Leaf(action_directory_member) => {
                convert_action_directory_member_to_sqlite_entry(
                    project_relative_entry_path,
                    action_directory_member,
                    None,
                    parent_path,
                )
            }
        });
    }
    result
}

fn convert_action_directory_member_to_sqlite_entry<'a>(
    path: Cow<'a, str>,
    action_directory_member: &'a ActionDirectoryMember,
    last_access_time: Option<i64>,
    parent_path: Option<Cow<'a, str>>,
) -> SqliteEntry<'a> {
    match action_directory_member {
        ActionDirectoryMember::File(file_metadata) => {
            let (entry_size, entry_hash, entry_hash_kind) = digest_parts(&file_metadata.digest);
            SqliteEntry {
                path,
                artifact_type: ArtifactType::File,
                entry_size: Some(entry_size),
                entry_hash: Some(Cow::Borrowed(entry_hash)),
                entry_hash_kind: Some(entry_hash_kind),
                file_is_executable: Some(file_metadata.is_executable),
                symlink_target: None,
                last_access_time,
                parent_path,
                deps: None,
                content_hash_kind: None,
                content_hash: None,
            }
        }
        ActionDirectoryMember::Symlink(symlink) => SqliteEntry {
            path,
            artifact_type: ArtifactType::Symlink,
            entry_size: None,
            entry_hash: None,
            entry_hash_kind: None,
            file_is_executable: None,
            symlink_target: Some(Cow::Borrowed(symlink.target().as_str())),
            last_access_time,
            parent_path,
            deps: None,
            content_hash_kind: None,
            content_hash: None,
        },
        ActionDirectoryMember::ExternalSymlink(external_symlink) => SqliteEntry {
            path,
            artifact_type: ArtifactType::ExternalSymlink,
            entry_size: None,
            entry_hash: None,
            entry_hash_kind: None,
            file_is_executable: None,
            symlink_target: Some(Cow::Borrowed(external_symlink.target_str())),
            last_access_time,
            parent_path,
            deps: None,
            content_hash_kind: None,
            content_hash: None,
        },
    }
}

fn convert_sqlite_entries_to_materializer_state(
    entries: Vec<SqliteEntry>,
    digest_config: DigestConfig,
) -> buck2_error::Result<MaterializerState> {
    #[derive(Default)]
    struct DirectoryData<'a> {
        root: Option<DirectoryRoot<'a>>,
        children: Vec<SqliteEntry<'a>>,
    }

    struct DirectoryRoot<'a> {
        last_access_time: DateTime<Utc>,
        deps: Option<Cow<'a, [u8]>>,
        content_hash_kind: Option<i32>,
        content_hash: Option<Cow<'a, str>>,
    }

    let mut directories: StdBuckHashMap<ProjectRelativePathBuf, DirectoryData> =
        StdBuckHashMap::default();

    let mut results = Vec::new();

    // On a first pass convert entries representing actual artifacts
    // and collect information about each "full"-representation directory.
    for entry in entries {
        let path = ProjectRelativePathBuf::unchecked_new(entry.path.clone().into_owned());
        if let Some(parent_artifact_path) = entry.parent_path.clone() {
            // Represents a part of directory artifact
            let parent_artifact_path =
                ProjectRelativePathBuf::unchecked_new(parent_artifact_path.into_owned());
            let DirectoryData { children, .. } =
                directories.entry(parent_artifact_path).or_default();
            children.push(entry);
        } else {
            // Represents artifact
            let Some(last_access_time) = entry.last_access_time else {
                return Err(MaterializerStateTableError::ExpectedLastAccessTimeForArtifact.into());
            };
            let last_access_time = Utc
                .timestamp_opt(last_access_time, 0)
                .single()
                .ok_or_else(|| internal_error!("invalid timestamp"))?;
            match entry.artifact_type {
                ArtifactType::Directory => {
                    let DirectoryData { root, .. } = directories.entry(path).or_default();
                    _ = root.insert(DirectoryRoot {
                        last_access_time,
                        deps: entry.deps,
                        content_hash_kind: entry.content_hash_kind,
                        content_hash: entry.content_hash,
                    });
                }
                _ => {
                    let state_entry =
                        convert_non_directory_sqlite_entry_to_materializer_state_entry(
                            entry,
                            last_access_time,
                            digest_config,
                        )?;
                    results.push(state_entry);
                }
            }
        }
    }

    // Now recreate "full" directories
    for (path, DirectoryData { root, children }) in directories {
        let dir = {
            let mut builder = ActionDirectoryBuilder::empty();
            for child in children {
                let child_path = child.path.clone();
                let parent_dir_rel_path = {
                    let child_path = ProjectRelativePath::unchecked_new(&child_path);
                    child_path.strip_prefix(&path)?
                };
                if child.artifact_type == ArtifactType::Directory {
                    builder.mkdir(parent_dir_rel_path)?;
                } else {
                    let member = convert_non_directory_sqlite_entry_to_action_directory_member(
                        child,
                        digest_config,
                    )?;
                    builder.insert(parent_dir_rel_path, DirectoryEntry::Leaf(member))?;
                }
            }
            let fingerprint = builder.fingerprint(digest_config.as_directory_serializer());
            fingerprint.shared(&*INTERNER)
        };
        let Some(DirectoryRoot {
            last_access_time,
            deps,
            content_hash_kind,
            content_hash,
        }) = root
        else {
            return Err(MaterializerStateTableError::DirectoryArtifactEntryMissing(path).into());
        };
        let value = decode_artifact_value(
            ActionDirectoryEntry::Dir(dir),
            deps,
            content_hash_kind,
            content_hash,
            digest_config,
        )?;
        results.push(MaterializerStateEntry {
            path,
            value,
            last_access_time,
        })
    }

    Ok(results)
}

fn convert_non_directory_sqlite_entry_to_materializer_state_entry(
    sqlite_entry: SqliteEntry,
    last_access_time: DateTime<Utc>,
    digest_config: DigestConfig,
) -> buck2_error::Result<MaterializerStateEntry> {
    let path = ProjectRelativePathBuf::unchecked_new(sqlite_entry.path.clone().into_owned());
    let deps = sqlite_entry.deps.clone();
    let content_hash_kind = sqlite_entry.content_hash_kind;
    let content_hash = sqlite_entry.content_hash.clone();
    let member =
        convert_non_directory_sqlite_entry_to_action_directory_member(sqlite_entry, digest_config)?;
    let value = decode_artifact_value(
        DirectoryEntry::Leaf(member),
        deps,
        content_hash_kind,
        content_hash,
        digest_config,
    )?;
    Ok(MaterializerStateEntry {
        path,
        value,
        last_access_time,
    })
}

fn decode_artifact_value(
    entry: ActionDirectoryEntry<ActionSharedDirectory>,
    deps: Option<Cow<'_, [u8]>>,
    content_hash_kind: Option<i32>,
    content_hash: Option<Cow<'_, str>>,
    digest_config: DigestConfig,
) -> buck2_error::Result<ArtifactValue> {
    let deps = match deps {
        Some(deps) => {
            let tree = RE::Tree::decode(deps.as_ref())?;
            let deps = re_tree_to_directory(&tree, &DateTime::<Utc>::MAX_UTC, digest_config, true)?
                .fingerprint(digest_config.as_directory_serializer())
                .shared(&*INTERNER);
            Some(deps)
        }
        None => None,
    };
    let value = ArtifactValue::new(entry, deps);
    let content_hash = Option::<ContentBasedPathHash>::try_from(PersistedContentHash {
        kind: content_hash_kind,
        value: content_hash,
    })?;
    Ok(match content_hash {
        Some(content_hash) => value.with_content_based_path_hash(content_hash),
        None => value,
    })
}

fn digest(
    size: Option<u64>,
    entry_hash: Option<&[u8]>,
    entry_hash_kind: Option<u8>,
    artifact_type: ArtifactType,
    digest_config: DigestConfig,
) -> buck2_error::Result<TrackedFileDigest> {
    let size = size.ok_or(MaterializerStateTableError::ExpectedFieldIsMissing {
        field: "size",
        artifact_type,
    })?;
    let entry_hash = entry_hash.ok_or(MaterializerStateTableError::ExpectedFieldIsMissing {
        field: "entry_hash",
        artifact_type,
    })?;
    let entry_hash_kind = entry_hash_kind.ok_or({
        MaterializerStateTableError::ExpectedFieldIsMissing {
            field: "entry_hash_kind",
            artifact_type,
        }
    })?;
    let entry_hash_kind = entry_hash_kind
        .try_into()
        .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))
        .with_buck_error_context(|| format!("Invalid entry_hash_kind: `{entry_hash_kind}`"))?;

    let file_digest = FileDigest::from_digest_bytes(entry_hash_kind, entry_hash, size)?;
    Ok(TrackedFileDigest::new(
        file_digest,
        digest_config.cas_digest_config(),
    ))
}

fn convert_non_directory_sqlite_entry_to_action_directory_member(
    sqlite_entry: SqliteEntry,
    digest_config: DigestConfig,
) -> buck2_error::Result<ActionDirectoryMember> {
    Ok(match sqlite_entry.artifact_type {
        ArtifactType::Directory => {
            return Err(MaterializerStateTableError::CodePathNotSupportedForDirEntry.into());
        }
        ArtifactType::File => ActionDirectoryMember::File(FileMetadata {
            digest: digest(
                sqlite_entry.entry_size,
                sqlite_entry.entry_hash.as_deref(),
                sqlite_entry.entry_hash_kind,
                sqlite_entry.artifact_type,
                digest_config,
            )?,
            is_executable: sqlite_entry.file_is_executable.ok_or({
                MaterializerStateTableError::ExpectedFieldIsMissing {
                    field: "file_is_executable",
                    artifact_type: sqlite_entry.artifact_type,
                }
            })?,
        }),
        ArtifactType::Symlink => {
            let symlink = Symlink::new(
                sqlite_entry
                    .symlink_target
                    .ok_or(MaterializerStateTableError::ExpectedFieldIsMissing {
                        field: "symlink_target",
                        artifact_type: sqlite_entry.artifact_type,
                    })?
                    .into_owned()
                    .into(),
            );
            ActionDirectoryMember::Symlink(Arc::new(symlink))
        }
        ArtifactType::ExternalSymlink => {
            let external_symlink = ExternalSymlink::new(
                sqlite_entry
                    .symlink_target
                    .ok_or(MaterializerStateTableError::ExpectedFieldIsMissing {
                        field: "symlink_target",
                        artifact_type: sqlite_entry.artifact_type,
                    })?
                    .into_owned()
                    .into(),
                ForwardRelativePathBuf::default(),
            )?;
            ActionDirectoryMember::ExternalSymlink(Arc::new(external_symlink))
        }
    })
}

pub struct MaterializerStateSqliteTable {
    connection: Arc<Mutex<Connection>>,
}

impl MaterializerStateSqliteTable {
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }

    pub(crate) fn create_table(&self) -> buck2_error::Result<()> {
        let sql = format!(
            "CREATE TABLE {STATE_TABLE_NAME} (
                path                    TEXT NOT NULL PRIMARY KEY,
                artifact_type           TEXT CHECK(artifact_type IN ('{ARTIFACT_TYPE_DIRECTORY}','{ARTIFACT_TYPE_FILE}','{ARTIFACT_TYPE_SYMLINK}','{ARTIFACT_TYPE_EXTERNAL_SYMLINK}')) NOT NULL,
                digest_size             INTEGER NULL DEFAULT NULL,
                entry_hash              BLOB NULL DEFAULT NULL,
                entry_hash_kind         INTEGER NULL DEFAULT NULL,
                file_is_executable      INTEGER NULL DEFAULT NULL,
                symlink_target          TEXT NULL DEFAULT NULL,
                last_access_time        INTEGER NULL DEFAULT NULL,
                parent_path             TEXT NULL DEFAULT NULL,
                deps                    BLOB NULL DEFAULT NULL,
                content_hash_kind       INTEGER NULL DEFAULT NULL,
                content_hash            TEXT NULL DEFAULT NULL
            )",
        );
        tracing::trace!(sql = %*sql, "creating table");
        self.connection
            .lock()
            .execute(&sql, [])
            .with_buck_error_context(|| format!("creating sqlite table {STATE_TABLE_NAME}"))?;
        self.create_parent_path_index()?;
        Ok(())
    }

    // Index is useful for faster removals
    fn create_parent_path_index(&self) -> buck2_error::Result<()> {
        let sql = format!(
            "CREATE INDEX {STATE_TABLE_NAME}_index ON {STATE_TABLE_NAME} (
                parent_path
            )",
        );
        tracing::trace!(sql = %*sql, "creating index");
        self.connection
            .lock()
            .execute(&sql, [])
            .with_buck_error_context(|| format!("creating index on {STATE_TABLE_NAME}"))?;
        Ok(())
    }

    pub(crate) fn insert(
        &self,
        path: &ProjectRelativePath,
        value: &ArtifactValue,
        timestamp: DateTime<Utc>,
    ) -> buck2_error::Result<()> {
        let entries = convert_artifact_value_to_sqlite_entries(path, value, &timestamp);
        static SQL: LazyLock<String> = LazyLock::new(|| {
            format!(
                "INSERT INTO {STATE_TABLE_NAME} (path, artifact_type, digest_size, entry_hash, entry_hash_kind, file_is_executable, symlink_target, last_access_time, parent_path, deps, content_hash_kind, content_hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
            )
        });
        let mut conn = self.connection.lock();
        let tx = conn.transaction()?;
        for entry in entries {
            tracing::trace!(sql = %*SQL, entry = ?entry, "inserting into table");
            tx.execute(
                &SQL,
                rusqlite::params![
                    entry.path,
                    entry.artifact_type,
                    entry.entry_size,
                    entry.entry_hash,
                    entry.entry_hash_kind,
                    entry.file_is_executable,
                    entry.symlink_target,
                    entry.last_access_time,
                    entry.parent_path,
                    entry.deps,
                    entry.content_hash_kind,
                    entry.content_hash,
                ],
            )
            .with_buck_error_context(|| {
                format!("inserting `{path}` into sqlite table {STATE_TABLE_NAME}")
            })?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn update_access_times(
        &self,
        updates: Vec<&ProjectRelativePathBuf>,
    ) -> buck2_error::Result<()> {
        let mut conn = self.connection.lock();
        let tx = conn.transaction()?;
        for chunk in updates.chunks(100) {
            let sql = format!(
                "UPDATE {} SET last_access_time = {} WHERE path IN ({})",
                STATE_TABLE_NAME,
                Utc::now().timestamp(),
                itertools::repeat_n("?", chunk.len()).join(","),
            );
            tracing::trace!(sql = %sql, chunk = ?chunk, "updating last_access_times");
            tx.execute(&sql, rusqlite::params_from_iter(chunk.map(|p| p.as_str())))
                .with_buck_error_context(|| format!("updating sqlite table {STATE_TABLE_NAME}"))?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn update_artifact_values(
        &self,
        updates: Vec<(ProjectRelativePathBuf, ArtifactValue)>,
        timestamp: DateTime<Utc>,
    ) -> buck2_error::Result<()> {
        let mut connection = self.connection.lock();
        let tx = connection.transaction()?;
        let mut update = tx.prepare(&format!(
            "UPDATE {STATE_TABLE_NAME}
             SET last_access_time = ?1, deps = ?2, content_hash_kind = ?3, content_hash = ?4
             WHERE path = ?5 AND parent_path IS NULL"
        ))?;
        for (path, value) in updates {
            let deps = value
                .deps()
                .map(directory_to_re_tree)
                .map(|tree| tree.encode_to_vec());
            let content_hash = PersistedContentHash::from(value.explicit_content_based_path_hash());
            update.execute(rusqlite::params![
                timestamp.timestamp(),
                deps,
                content_hash.kind,
                content_hash.value,
                path.as_str(),
            ])?;
        }
        drop(update);
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn read_materializer_state(
        &self,
        digest_config: DigestConfig,
    ) -> buck2_error::Result<MaterializerState> {
        let entries = self.read_all_entries().with_buck_error_context(|| {
            format!("error reading row of sqlite table {STATE_TABLE_NAME}")
        })?;
        convert_sqlite_entries_to_materializer_state(entries, digest_config)
    }

    fn read_all_entries(&self) -> buck2_error::Result<Vec<SqliteEntry<'_>>> {
        static SQL: LazyLock<String> = LazyLock::new(|| {
            format!(
                "SELECT path, artifact_type, digest_size, entry_hash, entry_hash_kind, file_is_executable, symlink_target, last_access_time, parent_path, deps, content_hash_kind, content_hash FROM {STATE_TABLE_NAME}",
            )
        });
        tracing::trace!(sql = %*SQL, "reading all from table");
        let connection = self.connection.lock();
        let mut stmt = connection.prepare(&SQL)?;
        stmt.query_map([], |row| -> rusqlite::Result<SqliteEntry> {
            Ok(SqliteEntry {
                path: Cow::Owned(row.get("path")?),
                artifact_type: row.get("artifact_type")?,
                entry_size: row.get("digest_size")?,
                entry_hash: row
                    .get::<_, Option<SqliteDigest>>("entry_hash")?
                    .map(Cow::Owned),
                entry_hash_kind: row.get("entry_hash_kind")?,
                file_is_executable: row.get("file_is_executable")?,
                symlink_target: row
                    .get::<_, Option<String>>("symlink_target")?
                    .map(Cow::Owned),
                last_access_time: row.get("last_access_time")?,
                parent_path: row.get::<_, Option<String>>("parent_path")?.map(Cow::Owned),
                deps: row.get::<_, Option<Vec<u8>>>("deps")?.map(Cow::Owned),
                content_hash_kind: row.get("content_hash_kind")?,
                content_hash: row
                    .get::<_, Option<String>>("content_hash")?
                    .map(Cow::Owned),
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_buck_error_context(|| format!("reading from sqlite table {STATE_TABLE_NAME}"))
    }

    pub(crate) fn delete(&self, paths: Vec<ProjectRelativePathBuf>) -> buck2_error::Result<usize> {
        if paths.is_empty() {
            return Ok(0);
        }

        let mut rows_deleted = 0;

        /// Delete only rows which represent artifacts (that is where `parent_path` is `NULL`). Rows that represent members of a directory artifact are not deleted.
        fn delete_artifact_rows(
            connection: &Arc<Mutex<Connection>>,
            paths: &[ProjectRelativePathBuf],
        ) -> buck2_error::Result<usize> {
            let sql = format!(
                "DELETE FROM {} WHERE path IN ({})",
                STATE_TABLE_NAME,
                // According to rusqlite docs this is the best way to generate the right
                // number of query placeholders.
                itertools::repeat_n("?", paths.len()).join(","),
            );

            tracing::trace!(sql = %sql, chunk = ?paths, "deleting artifact rows from table");
            connection
                .lock()
                .execute(
                    &sql,
                    rusqlite::params_from_iter(paths.iter().map(|p| p.as_str())),
                )
                .with_buck_error_context(|| {
                    format!("deleting artifact rows from sqlite table {STATE_TABLE_NAME}")
                })
        }

        /// Delete rows which represent members of directory artifacts (that is rows where `parent_path` is not `NULL`).
        fn delete_directory_artifact_members(
            connection: &Arc<Mutex<Connection>>,
            paths: &[ProjectRelativePathBuf],
        ) -> buck2_error::Result<usize> {
            let sql = format!(
                "DELETE FROM {} WHERE parent_path IN ({})",
                STATE_TABLE_NAME,
                // According to rusqlite docs this is the best way to generate the right
                // number of query placeholders.
                itertools::repeat_n("?", paths.len()).join(","),
            );

            tracing::trace!(sql = %sql, chunk = ?paths, "deleting directory artifact members from table");
            connection
                .lock()
                .execute(
                    &sql,
                    rusqlite::params_from_iter(paths.iter().map(|p| p.as_str())),
                )
                .with_buck_error_context(|| {
                    format!(
                        "deleting directory artifact members from sqlite table {STATE_TABLE_NAME}"
                    )
                })
        }

        for chunk in paths.chunks(100) {
            rows_deleted += delete_artifact_rows(&self.connection, chunk)?;
            rows_deleted += delete_directory_artifact_members(&self.connection, chunk)?;
        }

        Ok(rows_deleted)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use buck2_common::cas_digest::TrackedCasDigest;
    use buck2_directory::directory::builder::DirectoryBuilder;
    use buck2_directory::directory::dashmap_directory_interner::DashMapDirectoryInterner;
    use buck2_execute::directory::new_symlink;
    use buck2_fs::paths::forward_rel_path::ForwardRelativePath;

    use super::*;

    fn artifact_metadata_eq(a: &ArtifactMetadata, b: &ArtifactMetadata) -> bool {
        match (a, b) {
            (DirectoryEntry::Dir(d1), DirectoryEntry::Dir(d2)) => {
                d1.fingerprint() == d2.fingerprint()
            }
            (DirectoryEntry::Leaf(l1), DirectoryEntry::Leaf(l2)) => l1 == l2,
            _ => false,
        }
    }

    #[test]
    fn test_artifact_metadata_full_dir_sqlite_entry_conversion_succeeds() {
        let digest_config = DigestConfig::testing_default();

        let path = ProjectRelativePath::new("artifact_path").unwrap();
        let last_access_time = DateTime::from_timestamp_nanos(0);

        let metadata = {
            let directory = {
                let mut builder = DirectoryBuilder::empty();
                {
                    let digest =
                        TrackedCasDigest::from_content(b"hello", digest_config.cas_digest_config());
                    let metadata = FileMetadata {
                        digest,
                        is_executable: false,
                    };
                    let file = DirectoryEntry::Leaf(ActionDirectoryMember::File(metadata));
                    builder
                        .insert(ForwardRelativePath::unchecked_new("foo"), file)
                        .unwrap();
                }
                {
                    let empty_directory = DirectoryEntry::Dir(DirectoryBuilder::empty());
                    builder
                        .insert(ForwardRelativePath::unchecked_new("bar"), empty_directory)
                        .unwrap();
                }
                {
                    let mut directory_builder = DirectoryBuilder::empty();
                    let symlink =
                        ActionDirectoryMember::Symlink(Arc::new(Symlink::new("../foo".into())));
                    directory_builder
                        .insert(
                            ForwardRelativePath::unchecked_new("qux"),
                            DirectoryEntry::Leaf(symlink),
                        )
                        .unwrap();
                    let directory = DirectoryEntry::Dir(directory_builder);
                    builder
                        .insert(ForwardRelativePath::unchecked_new("baz"), directory)
                        .unwrap();
                }
                let interner = DashMapDirectoryInterner::new();
                builder
                    .fingerprint(digest_config.as_directory_serializer())
                    .shared(&interner)
            };
            ActionDirectoryEntry::Dir(directory)
        };

        let value = ArtifactValue::new(metadata.clone(), None);
        let entries = convert_artifact_value_to_sqlite_entries(path, &value, &last_access_time);

        let mut state =
            convert_sqlite_entries_to_materializer_state(entries, digest_config).unwrap();

        assert_eq!(state.len(), 1);

        let MaterializerStateEntry {
            path: result_path,
            value: result_value,
            last_access_time: result_last_access_time,
        } = state.pop().unwrap();

        assert_eq!(path, result_path);
        assert!(artifact_metadata_eq(&metadata, result_value.entry()));
        assert!(result_value.deps().is_none());
        assert!(result_value.explicit_content_based_path_hash().is_none());
        assert_eq!(last_access_time, result_last_access_time);
    }

    #[test]
    fn test_artifact_metadata_file_sqlite_entry_conversion_succeeds() {
        let digest_config = DigestConfig::testing_default();

        let digest = TrackedFileDigest::from_content(b"foo", digest_config.cas_digest_config());
        let metadata = DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
            digest,
            is_executable: false,
        }));
        let path = ProjectRelativePath::new("foo/bar").unwrap();
        let last_access_time = DateTime::from_timestamp_nanos(0);

        let value = ArtifactValue::new(metadata.clone(), None);
        let entries = convert_artifact_value_to_sqlite_entries(path, &value, &last_access_time);

        let mut state =
            convert_sqlite_entries_to_materializer_state(entries, digest_config).unwrap();

        assert_eq!(state.len(), 1);

        let MaterializerStateEntry {
            path: result_path,
            value: result_value,
            last_access_time: result_last_access_time,
        } = state.pop().unwrap();

        assert_eq!(path, result_path);
        assert!(artifact_metadata_eq(&metadata, result_value.entry()));
        assert!(result_value.deps().is_none());
        assert!(result_value.explicit_content_based_path_hash().is_none());
        assert_eq!(last_access_time, result_last_access_time);
    }

    #[test]
    fn test_artifact_metadata_symlink_sqlite_entry_conversion_succeeds() {
        let digest_config = DigestConfig::testing_default();

        let symlink = new_symlink("foo/bar").unwrap();
        // We use `new_symlink` as a helper but it can technically create both Symlink and ExternalSymlink.
        // Verify that we have a Symlink here.
        assert_matches!(symlink, ActionDirectoryMember::Symlink(..));

        let metadata = DirectoryEntry::Leaf(symlink);
        let path = ProjectRelativePath::new("foo/bar").unwrap();
        let last_access_time = DateTime::from_timestamp_nanos(0);

        let value = ArtifactValue::new(metadata.clone(), None);
        let entries = convert_artifact_value_to_sqlite_entries(path, &value, &last_access_time);

        let mut state =
            convert_sqlite_entries_to_materializer_state(entries, digest_config).unwrap();

        assert_eq!(state.len(), 1);

        let MaterializerStateEntry {
            path: result_path,
            value: result_value,
            last_access_time: result_last_access_time,
        } = state.pop().unwrap();

        assert_eq!(path, result_path);
        assert!(artifact_metadata_eq(&metadata, result_value.entry()));
        assert!(result_value.deps().is_none());
        assert!(result_value.explicit_content_based_path_hash().is_none());
        assert_eq!(last_access_time, result_last_access_time);
    }

    #[test]
    fn test_artifact_metadata_external_symlink_sqlite_entry_conversion_succeeds() {
        let digest_config = DigestConfig::testing_default();

        let external_symlink = new_symlink(if cfg!(windows) {
            // Not sure if we actually support any external symlink on windows, but better
            // to just check anyways.
            "C:\\external\\symlink\\to\\artifact"
        } else {
            "/mnt/gvfs/third-party2/zstd/28def025ee38919d509596da7d09e7a5262cbf32/1.4.x/platform010/64091f4/share"
        }).unwrap();
        // We use `new_symlink` as a helper but it can technically create both Symlink and ExternalSymlink.
        // Verify that we have an ExternalSymlink here.
        assert_matches!(external_symlink, ActionDirectoryMember::ExternalSymlink(..));

        let metadata = DirectoryEntry::Leaf(external_symlink);
        let path = ProjectRelativePath::new("foo/bar").unwrap();
        let last_access_time = DateTime::from_timestamp_nanos(0);

        let value = ArtifactValue::new(metadata.clone(), None);
        let entries = convert_artifact_value_to_sqlite_entries(path, &value, &last_access_time);

        let mut state =
            convert_sqlite_entries_to_materializer_state(entries, digest_config).unwrap();

        assert_eq!(state.len(), 1);

        let MaterializerStateEntry {
            path: result_path,
            value: result_value,
            last_access_time: result_last_access_time,
        } = state.pop().unwrap();

        assert_eq!(path, result_path);
        assert!(artifact_metadata_eq(&metadata, result_value.entry()));
        assert!(result_value.deps().is_none());
        assert!(result_value.explicit_content_based_path_hash().is_none());
        assert_eq!(last_access_time, result_last_access_time);
    }

    #[test]
    fn test_delete_many() -> buck2_error::Result<()> {
        let conn = Connection::open_in_memory()?;

        let table = MaterializerStateSqliteTable::new(Arc::new(Mutex::new(conn)));
        table.create_table()?;

        // Sqlite has limits on how many variables you can use at once. Check we don't run into
        // those.
        let paths = (0..50000)
            .map(|i| ProjectRelativePathBuf::unchecked_new(format!("foo/{i}")))
            .collect();

        table.delete(paths)?;

        Ok(())
    }
}
