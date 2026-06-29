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

use allocative::Allocative;
use async_trait::async_trait;
use buck2_action_metadata_proto::DepFileInputs;
use buck2_action_metadata_proto::RemoteDepFile;
use buck2_artifact::artifact::artifact_type::Artifact;
use buck2_artifact::artifact::artifact_type::OutputArtifact;
use buck2_artifact::artifact::build_artifact::BuildArtifact;
use buck2_build_api::actions::ActionExecutionCtx;
use buck2_build_api::actions::execute::action_executor::ActionExecutionKind;
use buck2_build_api::actions::execute::action_executor::ActionExecutionMetadata;
use buck2_build_api::actions::execute::action_executor::ActionOutputs;
use buck2_build_api::actions::impls::expanded_command_line::ExpandedCommandLineDigest;
use buck2_build_api::artifact_groups::ArtifactGroup;
use buck2_build_api::interpreter::rule_defs::artifact_tagging::ArtifactTag;
use buck2_build_api::interpreter::rule_defs::cmd_args::CommandLineArtifactVisitor;
use buck2_build_signals::env::WaitingData;
use buck2_common::cas_digest::CasDigestConfig;
use buck2_common::cas_digest::CasDigestData;
use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::FileDigestConfig;
use buck2_common::file_ops::metadata::TrackedFileDigest;
use buck2_core::buck2_env;
use buck2_core::content_hash::ContentBasedPathHash;
use buck2_core::fs::artifact_path_resolver::ArtifactFs;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_core::soft_error;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::directory_hasher::DirectoryDigester;
use buck2_directory::directory::directory_ref::DirectoryRef;
use buck2_directory::directory::directory_selector::DirectorySelector;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_directory::directory::find::find;
use buck2_directory::directory::fingerprinted_directory::FingerprintedDirectory;
use buck2_error::BuckErrorContext;
use buck2_error::internal_error;
use buck2_events::dispatch::span_async_simple;
use buck2_execute::artifact::artifact_dyn::ArtifactDyn;
use buck2_execute::artifact_value::ArtifactValue;
use buck2_execute::artifact_value::ArtifactValueFingerprint;
use buck2_execute::digest::CasDigestToReExt;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::directory::ActionDirectoryBuilder;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_execute::directory::ActionFingerprintedDirectoryRef;
use buck2_execute::directory::ActionImmutableDirectory;
use buck2_execute::directory::ActionSharedDirectory;
use buck2_execute::directory::INTERNER;
use buck2_execute::directory::LazyActionDirectoryBuilder;
use buck2_execute::directory::ReDirectorySerializer;
use buck2_execute::directory::expand_selector_for_dependencies;
use buck2_execute::entry::build_entry_from_disk;
use buck2_execute::execute::action_digest::ActionDigest;
use buck2_execute::execute::action_digest_and_blobs::ActionDigestAndBlobs;
use buck2_execute::execute::action_digest_and_blobs::ActionDigestAndBlobsBuilder;
use buck2_execute::execute::cache_uploader::IntoRemoteDepFile;
use buck2_execute::execute::dep_file_digest::DepFileDigest;
use buck2_execute::execute::request::CommandExecutionPaths;
use buck2_execute::execute::request::InDaemonLocalReuseKey;
use buck2_execute::execute::request::LocalReuseKeys;
use buck2_execute::execute::request::OutputType;
use buck2_execute::execute::request::PersistentLocalReuseKey;
use buck2_execute::execute::result::CommandExecutionResult;
use buck2_execute::materialize::materializer::ActionReuseFilteredInputs;
use buck2_execute::materialize::materializer::ActionReuseKey;
use buck2_execute::materialize::materializer::ActionReuseOutput;
use buck2_execute::materialize::materializer::ActionReuseOutputs;
use buck2_execute::materialize::materializer::ActionReuseWrite;
use buck2_execute::materialize::materializer::DepFileActionReuseKey;
use buck2_execute::materialize::materializer::DepFileActionReuseWrite;
use buck2_execute::materialize::materializer::IdenticalActionReuseKey;
use buck2_execute::materialize::materializer::MaterializationError;
use buck2_execute::materialize::materializer::Materializer;
use buck2_file_watcher::dep_files::FLUSH_DEP_FILES;
use buck2_file_watcher::dep_files::FLUSH_NON_LOCAL_DEP_FILES;
use buck2_fs::fs_util;
use buck2_fs::paths::file_name::FileName;
use buck2_fs::paths::file_name::FileNameBuf;
use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
use buck2_fs::paths::forward_rel_path::ForwardRelativePathBuf;
use buck2_fs::paths::forward_rel_path::ForwardRelativePathNormalizer;
use buck2_hash::BuckDashMap;
use buck2_hash::StdBuckHashMap;
use buck2_hash::StdBuckHashSet;
use buck2_util::strong_hasher::Blake3StrongHasher;
use derive_more::Display;
use dupe::Dupe;
use futures::StreamExt;
use pagable::Pagable;
use parking_lot::MappedMutexGuard;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use starlark_map::ordered_map::OrderedMap;
use strong_hash::StrongHash;
use tracing::instrument;

use crate::actions::impls::run::RunActionKey;

#[allocative::root]
static DEP_FILES: LazyLock<BuckDashMap<RunActionKey, Arc<DepFileState>>> =
    LazyLock::new(BuckDashMap::new);

/// When this is set, we retain directories after fingerprinting, so that we can output them later
/// for debugging via `buck2 audit dep-files`.
fn keep_directories() -> buck2_error::Result<bool> {
    buck2_env!("BUCK2_KEEP_DEP_FILE_DIRECTORIES", bool)
}

/// Forget about all dep files. This isn't really meant to be commonly used, but if an invalid dep
/// file was produced and the user wants unblocking, this will provide it.
fn flush_dep_files() {
    tracing::info!("Flushing all {} dep files", DEP_FILES.len());
    DEP_FILES.clear();
}

/// Flush all dep files that were not produced locally.
/// In general we may want to retain dep files that were produced locally for longer, since (a) they are
/// already on disk and we don't need to download them, and (b) since they were produced locally they are
/// not cached elsewhere so there is more value in retaining them.
fn flush_non_local_dep_files() {
    tracing::info!(
        "Flushing non-local dep files, current size is: {}",
        DEP_FILES.len()
    );
    DEP_FILES.retain(|_, dep_file_state| dep_file_state.was_produced_locally);
    tracing::info!(
        "Number of remaining local dep files is: {}",
        DEP_FILES.len()
    );
}

pub(crate) fn init_flush_dep_files() {
    FLUSH_DEP_FILES.init(flush_dep_files);
    FLUSH_NON_LOCAL_DEP_FILES.init(flush_non_local_dep_files);
}

pub(crate) fn get_dep_files(key: &RunActionKey) -> Option<Arc<DepFileState>> {
    DEP_FILES.get(key).map(|s| s.dupe())
}

/// The input signatures for a DepFileState. We compute those lazily, so we either have the input
/// directories (no computation done), or the actual signatures (computation was done).
#[derive(Allocative)]
enum DepFileStateInputSignatures {
    /// Deferred(Some) means we have the input directories but haven't filtered them using the dep
    /// file yet (which in fact we haven't downloaded yet). Deferred(None) is a case that should
    /// never happen, since it would mean we panicked while producing the signatures, which is not
    /// fallible. It's the moral equivalent of a poisoned mutex.
    Deferred(Option<PartitionedInputs<ActionSharedDirectory>>),

    /// Computed represents the case where we have produced the input signatures. We only do this
    /// once at most.
    Computed(StoredFingerprints),
}

#[derive(Allocative)]
pub(crate) enum StoredFingerprints {
    /// Store only digests. This is what we use in prod because it is small.
    Digests(PartitionedInputs<TrackedFileDigest>),

    /// Store digests + dirs. We allow this via BUCK2_KEEP_DEP_FILE_DIRECTORIES because it gives
    /// more debuggability.
    Dirs(PartitionedInputs<ActionImmutableDirectory>),
}

impl<'a> From<&'a StoredFingerprints> for Cow<'a, PartitionedInputs<TrackedFileDigest>> {
    fn from(fingerprints: &'a StoredFingerprints) -> Self {
        match fingerprints {
            StoredFingerprints::Digests(fingerprints) => Cow::Borrowed(fingerprints),
            StoredFingerprints::Dirs(dirs) => Cow::Owned(PartitionedInputs::from(dirs)),
        }
    }
}

impl PartialEq<PartitionedInputs<ActionImmutableDirectory>> for StoredFingerprints {
    fn eq(&self, other: &PartitionedInputs<ActionImmutableDirectory>) -> bool {
        *Cow::from(self) == PartitionedInputs::from(other)
    }
}

#[derive(Allocative)]
struct HasDeclaredDepFiles {
    declared_dep_files: DeclaredDepFiles,
    input_signatures: Mutex<DepFileStateInputSignatures>,
}

/// The state that resulted from the previous evaluation of a command that produced dep files. This
/// contains everything we need to determine whether re-evaluation is necessary (and if it isn't,
/// to return the previous value).
#[derive(Allocative)]
pub(crate) struct DepFileState {
    digests: CommandDigests,
    result: ActionOutputs,
    was_produced_locally: bool,
    has_declared_dep_files: Option<HasDeclaredDepFiles>,
}

#[derive(Allocative)]
pub(crate) struct CommandDigests {
    pub(crate) cli: ExpandedCommandLineDigest,
    pub(crate) directory: FileDigest,
    pub(crate) in_daemon_reuse_key: InDaemonLocalReuseKey,
    pub(crate) local_worker_directory: Option<TrackedFileDigest>,
}

impl DepFileState {
    pub(crate) fn has_signatures(&self) -> bool {
        match self.has_declared_dep_files {
            Some(ref has_declared_dep_files) => {
                match *has_declared_dep_files.input_signatures.lock() {
                    DepFileStateInputSignatures::Computed(..) => true,
                    DepFileStateInputSignatures::Deferred(..) => false,
                }
            }
            None => false,
        }
    }

    pub(crate) fn declared_dep_files(&self) -> Option<&DeclaredDepFiles> {
        match &self.has_declared_dep_files {
            Some(has_declared_dep_files) => Some(&has_declared_dep_files.declared_dep_files),
            None => None,
        }
    }

    pub(crate) fn result(&self) -> &ActionOutputs {
        &self.result
    }

    /// Compute the signature for this DepFileState, having provided the dep files from
    /// read_dep_files.
    pub(crate) fn locked_compute_fingerprints<'a>(
        &'a self,
        dep_files: Cow<'_, ConcreteDepFiles>,
        keep_directories: bool,
        digest_config: DigestConfig,
        fs: &ArtifactFs,
    ) -> buck2_error::Result<MappedMutexGuard<'a, StoredFingerprints>> {
        let input_signatures = &self
            .has_declared_dep_files
            .as_ref()
            .expect("No dep-files exist, shouldn't try to compute fingerprints!")
            .input_signatures;

        // Now we need to know the signatures on the original action. Produce them if they're
        // missing. We're either storing input directories or outputs here.
        let mut guard = input_signatures.lock();

        if let DepFileStateInputSignatures::Deferred(ref mut directories) = *guard {
            let fingerprints = compute_fingerprints(
                directories
                    .take()
                    .expect("Poisoned DepFileStateInputSignatures")
                    .unshare(),
                dep_files.into_owned(),
                digest_config,
                keep_directories,
                fs,
            )?;

            *guard = DepFileStateInputSignatures::Computed(fingerprints);
        }

        Ok(MutexGuard::map(guard, |v| match v {
            DepFileStateInputSignatures::Computed(signatures) => signatures,
            DepFileStateInputSignatures::Deferred(..) => unreachable!(),
        }))
    }
}

/// The set of dep files declared by a RunAction, matching tags to their labels. We enforce at
/// creation time that tags and labels are both unique.
#[derive(Debug, Allocative, Pagable)]
pub(crate) struct RunActionDepFiles {
    pub(crate) labels: OrderedMap<ArtifactTag, Arc<str>>,
}

impl Display for RunActionDepFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let values: Vec<&str> = self
            .labels
            .values()
            .map(|x| x.as_ref())
            .collect::<Vec<&str>>();
        write!(f, "{values:?}")
    }
}

impl RunActionDepFiles {
    pub(crate) fn new() -> Self {
        Self {
            labels: OrderedMap::new(),
        }
    }
}

fn get_output_path_digest(
    digest_config: DigestConfig,
    output_paths: &[(ProjectRelativePathBuf, OutputType)],
) -> CasDigestData {
    let mut digester = CasDigestData::digester(digest_config.cas_digest_config());
    digester.update(&(output_paths.len() as u64).to_le_bytes());
    for (output_path, output_type) in output_paths.iter() {
        digester.update_length_prefixed(output_path.as_str().as_bytes());
        let output_type = match output_type {
            OutputType::FileOrDirectory => 0,
            OutputType::File => 1,
            OutputType::Directory => 2,
        };
        digester.update(&[output_type]);
    }
    digester.finalize()
}

// A utility struct to hold digests that are included in both remote depfile key and value
pub(crate) struct CommonDigests {
    commandline_cli_digest: ExpandedCommandLineDigest,
    // A digest of all output paths for this action
    output_paths_digest: CasDigestData,
    // A digest of inputs that are untagged (not tied to a dep file)
    untagged_inputs_digest: TrackedFileDigest,
    // A digest of local worker inputs
    local_worker_inputs_digest: Option<TrackedFileDigest>,
}
impl CommonDigests {
    // Take the digest of everythig in the structure
    fn fingerprint(&self, digest_config: DigestConfig) -> CasDigestData {
        let mut digester = CasDigestData::digester(digest_config.cas_digest_config());

        digester.update(self.output_paths_digest.raw_digest().as_bytes());
        digester.update(self.commandline_cli_digest.as_bytes());
        digester.update(self.untagged_inputs_digest.raw_digest().as_bytes());
        if let Some(local_worker_inputs_digest) = &self.local_worker_inputs_digest {
            digester.update(local_worker_inputs_digest.raw_digest().as_bytes());
        }

        digester.finalize()
    }

    // Construct an action so that the action digest can be used as a remote dep file key,
    // which is the key to an action result with dep file metadata.
    // The action itself is unused but we need to upload some action to avoid permission
    // errors when uploading the action result, and the digest needs to match that action.
    pub(crate) fn make_remote_dep_file_action(
        &self,
        digest_config: DigestConfig,
        mergebase: &Option<String>,
        re_platform: &remote_execution::Platform,
    ) -> ActionDigestAndBlobs {
        let mut digester = DepFileDigest::digester(digest_config.cas_digest_config());
        digester.update(self.fingerprint(digest_config).raw_digest().as_bytes());

        // Take the digest of the mergebase to get the closest hit.
        if let Some(m) = mergebase {
            digester.update(m.as_bytes());
        }
        let inner_remote_dep_file_key = hex::encode(digester.finalize().raw_digest().as_bytes());

        let mut blobs = ActionDigestAndBlobsBuilder::new(digest_config);
        let command = blobs.add_command(&remote_execution::Command {
            // Instead of using the digest directly, we could use the constituent digests, or constituent paths
            // which might be useful for debugging.
            arguments: vec![inner_remote_dep_file_key],
            #[allow(deprecated)]
            platform: Some(re_platform.clone()),
            ..Default::default()
        });

        blobs.build(&remote_execution::Action {
            command_digest: Some(command.to_grpc()),
            ..Default::default()
        })
    }

    /// Take a list of declared dep files (label, artifact) and filtered inputs (StoredFingerprints)
    /// and create a list of dep file path and filtered inputs for that input
    fn get_dep_file_inputs(
        &self,
        declared_dep_files: &DeclaredDepFiles,
        filtered_input_fingerprints: &StoredFingerprints,
    ) -> Vec<DepFileInputs> {
        let filtered_input_fingerprints = Cow::from(filtered_input_fingerprints);
        declared_dep_files
            .tagged
            .iter()
            // Both declared inputs and filtered fingerprints are constructed with preserved input ordering
            .zip(filtered_input_fingerprints.tagged.iter())
            .map(|((_, dep_file), (_, filtered_fingerprint))| DepFileInputs {
                dep_file_path: dep_file
                    .output
                    .get_path()
                    .with_short_path(|path| path.as_str().to_owned()),
                filtered_fingerprint: filtered_fingerprint.raw_digest().as_bytes().to_vec(),
            })
            .collect()
    }

    fn make_dep_file_entry_proto(
        &self,
        declared_dep_files: &DeclaredDepFiles,
        filtered_input_fingerprints: &StoredFingerprints,
    ) -> RemoteDepFile {
        let output_paths_digest = self.output_paths_digest.raw_digest().as_bytes().to_vec();
        let commandline_cli_digest = self.commandline_cli_digest.as_bytes().to_vec();
        let untagged_inputs_digest = self.untagged_inputs_digest.raw_digest().as_bytes().to_vec();
        let dep_file_inputs =
            self.get_dep_file_inputs(declared_dep_files, filtered_input_fingerprints);

        RemoteDepFile {
            commandline_cli_digest,
            output_paths_digest,
            untagged_inputs_digest,
            dep_file_inputs,
        }
    }
}

struct NonEmptyDeclaredDepFiles(DeclaredDepFiles);

impl NonEmptyDeclaredDepFiles {
    fn new(declared_dep_files: DeclaredDepFiles) -> Option<Self> {
        if declared_dep_files.is_empty() {
            None
        } else {
            Some(Self(declared_dep_files))
        }
    }

    fn digest(&self, digest_config: DigestConfig) -> CasDigestData {
        let mut entries = self
            .0
            .tagged
            .values()
            .map(|dep_file| {
                (dep_file.label.as_ref().to_owned(), {
                    let mut state = Blake3StrongHasher::new();
                    dep_file.output.get_path().strong_hash(&mut state);
                    *state.finalize().as_bytes()
                })
            })
            .collect::<Vec<_>>();
        entries.sort();
        let mut digester = CasDigestData::digester(digest_config.cas_digest_config());
        digester.update(&(entries.len() as u64).to_le_bytes());
        for (label, path) in entries {
            digester.update_length_prefixed(label.as_bytes());
            digester.update(&path);
        }
        digester.finalize()
    }
}

impl AsRef<DeclaredDepFiles> for NonEmptyDeclaredDepFiles {
    fn as_ref(&self) -> &DeclaredDepFiles {
        &self.0
    }
}

fn dep_file_action_reuse_key(
    digest_config: DigestConfig,
    common_digests: &CommonDigests,
    declared_dep_files: &NonEmptyDeclaredDepFiles,
    re_platform: &remote_execution::Platform,
    local_reuse_key: &PersistentLocalReuseKey,
) -> DepFileActionReuseKey {
    let common_digest = common_digests.fingerprint(digest_config);
    let declared_dep_files_digest = declared_dep_files.digest(digest_config);
    let mut properties = re_platform
        .properties
        .iter()
        .map(|property| (property.name.as_str(), property.value.as_str()))
        .collect::<Vec<_>>();
    properties.sort();
    let mut digester = CasDigestData::digester(digest_config.cas_digest_config());
    digester.update(&(properties.len() as u64).to_le_bytes());
    for (name, value) in properties {
        digester.update_length_prefixed(name.as_bytes());
        digester.update_length_prefixed(value.as_bytes());
    }
    let execution_platform_digest = digester.finalize();
    DepFileActionReuseKey {
        common_digest,
        declared_dep_files_digest,
        execution_platform_digest,
        local_reuse_key: local_reuse_key.as_ref().dupe(),
    }
}

impl From<&StoredFingerprints> for ActionReuseFilteredInputs {
    fn from(fingerprints: &StoredFingerprints) -> Self {
        let fingerprints = Cow::from(fingerprints);
        Self {
            untagged: fingerprints.untagged.dupe(),
            tagged: fingerprints
                .tagged
                .iter()
                .map(|(tag, digest)| (tag.as_ref().to_owned(), digest.dupe()))
                .collect(),
        }
    }
}

impl From<&PartitionedInputs<ActionImmutableDirectory>> for ActionReuseFilteredInputs {
    fn from(fingerprints: &PartitionedInputs<ActionImmutableDirectory>) -> Self {
        let fingerprints = PartitionedInputs::from(fingerprints);
        Self {
            untagged: fingerprints.untagged,
            tagged: fingerprints
                .tagged
                .into_iter()
                .map(|(tag, digest)| (tag.as_ref().to_owned(), digest))
                .collect(),
        }
    }
}

fn action_reuse_outputs(
    declared_outputs: &[BuildArtifact],
    outputs: &ActionOutputs,
) -> buck2_error::Result<ActionReuseOutputs> {
    let mut reuse_outputs = Vec::with_capacity(declared_outputs.len());
    for declared_output in declared_outputs {
        let path = declared_output.get_path();
        let Some(value) = outputs.get(path) else {
            return Err(internal_error!(
                "Action output `{}` was missing from ActionOutputs",
                path
            ));
        };
        reuse_outputs.push(ActionReuseOutput {
            output_path: path.path().to_owned(),
            content_based_path_hash: value.content_based_path_hash(),
            artifact_value_fingerprint: ArtifactValueFingerprint::from(value),
        });
    }
    ActionReuseOutputs::new(reuse_outputs)
}

pub(crate) struct DepFileBundle {
    dep_files_key: RunActionKey,
    input_directory_digest: FileDigest,
    dep_files: DepFilesState,
    common_digests: CommonDigests,
    local_reuse_keys: LocalReuseKeys,
}

enum DepFilesState {
    NoDepFiles,
    Declared {
        declared_dep_files: NonEmptyDeclaredDepFiles,
        shared_declared_inputs: PartitionedInputs<ActionSharedDirectory>,
        filtered_input_fingerprints: Option<StoredFingerprints>,
    },
}

#[async_trait]
impl IntoRemoteDepFile for DepFileBundle {
    fn remote_dep_file_action(
        &self,
        digest_config: DigestConfig,
        mergebase: &Option<String>,
        re_platform: &remote_execution::Platform,
    ) -> ActionDigestAndBlobs {
        self.common_digests
            .make_remote_dep_file_action(digest_config, mergebase, re_platform)
    }

    async fn make_remote_dep_file(
        &mut self,
        digest_config: DigestConfig,
        fs: &ArtifactFs,
        materializer: &dyn Materializer,
        result: &CommandExecutionResult,
    ) -> buck2_error::Result<Option<RemoteDepFile>> {
        let DepFilesState::Declared {
            declared_dep_files,
            shared_declared_inputs,
            filtered_input_fingerprints,
        } = &mut self.dep_files
        else {
            return Ok(None);
        };

        let filtered_input_fingerprints = match filtered_input_fingerprints {
            Some(filtered_input_fingerprints) => filtered_input_fingerprints,
            filtered_input_fingerprints @ None => {
                let action_outputs = ActionOutputs::new(
                    result
                        .outputs
                        .iter()
                        .filter_map(|(o, v)| {
                            Some((o.as_ref().into_build_artifact()?.0.dupe(), v.dupe()))
                        })
                        .collect(),
                );
                let fingerprints = eagerly_compute_fingerprints(
                    digest_config,
                    fs,
                    materializer,
                    shared_declared_inputs,
                    declared_dep_files.as_ref(),
                    &action_outputs,
                )
                .await?;
                filtered_input_fingerprints.insert(fingerprints)
            }
        };
        Ok(Some(self.common_digests.make_dep_file_entry_proto(
            declared_dep_files.as_ref(),
            filtered_input_fingerprints,
        )))
    }
}

impl DepFileBundle {
    pub(crate) fn has_dep_files(&self) -> bool {
        match &self.dep_files {
            DepFilesState::NoDepFiles => false,
            DepFilesState::Declared { .. } => true,
        }
    }

    async fn check_action_reuse(
        &self,
        ctx: &mut dyn ActionExecutionCtx,
        lookup: ActionReuseKey,
        declared_outputs: &[BuildArtifact],
    ) -> Option<ActionOutputs> {
        let materializer = ctx.materializer();
        let result = async {
            let (key, outputs, filtered_inputs) = match lookup {
                ActionReuseKey::IdenticalAction(key) => {
                    let Some(outputs) = materializer.get_identical_action(key.dupe()).await? else {
                        return Ok(None);
                    };
                    (ActionReuseKey::from(key), outputs, None)
                }
                ActionReuseKey::DepFile(key) => {
                    let Some(data) = materializer.get_dep_file(key.dupe()).await? else {
                        return Ok(None);
                    };
                    (
                        ActionReuseKey::from(key),
                        data.outputs,
                        Some(data.filtered_inputs),
                    )
                }
            };
            let mut resolved_outputs = Vec::with_capacity(declared_outputs.len());
            for declared_output in declared_outputs {
                let Some(output) = outputs.get(declared_output.get_path().path()) else {
                    return Ok(None);
                };
                let path = ctx.fs().buck_out_path_resolver().resolve_gen(
                    declared_output.get_path(),
                    Some(&output.content_based_path_hash),
                )?;
                resolved_outputs.push((path, output));
            }
            let values = materializer
                .get_artifact_values_for_materialized_paths(
                    resolved_outputs
                        .iter()
                        .map(|(path, _)| (*path).clone())
                        .collect(),
                    false,
                )
                .await?;
            let mut action_outputs = Vec::with_capacity(declared_outputs.len());
            for ((declared_output, value), (materializer_path, output)) in
                std::iter::zip(std::iter::zip(declared_outputs, values), resolved_outputs)
            {
                let Some((_, value)) = value else {
                    return Ok(None);
                };
                let (entry, _) = build_entry_from_disk(
                    ctx.fs().fs().resolve(&materializer_path),
                    FileDigestConfig::build(ctx.digest_config().cas_digest_config()),
                    ctx.blocking_executor(),
                    ctx.fs().fs().root(),
                )
                .await?;
                let Some(entry) = entry else {
                    return Ok(None);
                };
                let entry = entry.map_dir(|directory| {
                    directory
                        .fingerprint(ctx.digest_config().as_directory_serializer())
                        .shared(&*INTERNER)
                });
                if value.entry() != &entry {
                    return Ok(None);
                }
                if ArtifactValueFingerprint::from(&value) != output.artifact_value_fingerprint {
                    return Ok(None);
                }
                action_outputs.push((declared_output.get_path().dupe(), value));
            }
            let action_outputs = ActionOutputs::new(action_outputs.into_iter().collect());
            if let Some(filtered_inputs) = filtered_inputs.as_ref() {
                let DepFilesState::Declared {
                    declared_dep_files,
                    shared_declared_inputs,
                    ..
                } = &self.dep_files
                else {
                    return Ok(None);
                };
                if !filtered_inputs_match(
                    ctx,
                    false,
                    declared_dep_files.as_ref(),
                    &action_outputs,
                    shared_declared_inputs,
                    StoredFilteredInputs::ActionReuse(filtered_inputs),
                )
                .await?
                {
                    return Ok(None);
                }
            }
            Ok(Some((action_outputs, key)))
        }
        .await;
        let (outputs, key) = match result {
            Ok(Some(hit)) => hit,
            Ok(None) => return None,
            Err(error) => {
                let _ = soft_error!(
                    "action_reuse_lookup_error",
                    error,
                    quiet: true
                );
                return None;
            }
        };
        if let Err(error) = materializer.touch_action_reuse(key).await {
            let _ = soft_error!(
                "action_reuse_touch_error",
                error,
                quiet: true
            );
        }
        Some(outputs)
    }

    pub(crate) async fn check_local_dep_file_cache_for_identical_action(
        &self,
        ctx: &mut dyn ActionExecutionCtx,
        action_digest: &ActionDigest,
        declared_outputs: &[BuildArtifact],
    ) -> buck2_error::Result<(Option<(ActionOutputs, ActionExecutionMetadata)>, bool)> {
        let persistent = &self.local_reuse_keys.persistent;

        // Get the action outputs (if cache hit) and an indicator on whether a full lookup operation should be performed
        let (outputs, check_filtered_inputs) = span_async_simple(
            buck2_data::MatchDepFilesStart {
                checking_filtered_inputs: false,
                remote_cache: false,
            },
            match_if_identical_action(ctx, self, declared_outputs),
            buck2_data::MatchDepFilesEnd {},
        )
        .await?;
        let outputs = match outputs {
            Some(outputs) => Some(outputs),
            None => match persistent {
                Some(persistent) => {
                    let key = IdenticalActionReuseKey {
                        action_digest: action_digest.dupe(),
                        local_reuse_key: persistent.as_ref().dupe(),
                    };
                    self.check_action_reuse(
                        ctx,
                        ActionReuseKey::IdenticalAction(key),
                        declared_outputs,
                    )
                    .await
                }
                None => None,
            },
        };
        let is_hit = outputs.is_some();
        let outputs = outputs.map(|o| {
            (
                o,
                ActionExecutionMetadata {
                    execution_kind: ActionExecutionKind::LocalActionCache,
                    timing: Default::default(),
                    input_files_bytes: None,
                    waiting_data: WaitingData::new(),
                },
            )
        });
        Ok((
            outputs,
            !is_hit && (check_filtered_inputs || self.has_dep_files()),
        ))
    }

    pub(crate) async fn check_local_dep_file_cache(
        &self,
        ctx: &mut dyn ActionExecutionCtx,
        declared_outputs: &[BuildArtifact],
    ) -> buck2_error::Result<Option<(ActionOutputs, ActionExecutionMetadata)>> {
        let persistent = &self.local_reuse_keys.persistent;

        let outputs = span_async_simple(
            buck2_data::MatchDepFilesStart {
                checking_filtered_inputs: true,
                remote_cache: false,
            },
            match_or_clear_dep_file(ctx, self, declared_outputs),
            buck2_data::MatchDepFilesEnd {},
        )
        .await?;
        let outputs = match outputs {
            Some(outputs) => Some(outputs),
            None => {
                let DepFilesState::Declared {
                    declared_dep_files, ..
                } = &self.dep_files
                else {
                    return Ok(None);
                };
                let Some(persistent) = persistent else {
                    return Ok(None);
                };
                let key = dep_file_action_reuse_key(
                    ctx.digest_config(),
                    &self.common_digests,
                    declared_dep_files,
                    ctx.re_platform(),
                    persistent,
                );
                self.check_action_reuse(ctx, ActionReuseKey::DepFile(key), declared_outputs)
                    .await
            }
        };

        let outputs = outputs.map(|o| {
            (
                o,
                ActionExecutionMetadata {
                    execution_kind: ActionExecutionKind::LocalDepFile,
                    timing: Default::default(),
                    input_files_bytes: None,
                    waiting_data: WaitingData::new(),
                },
            )
        });
        Ok(outputs)
    }

    pub(crate) async fn check_remote_dep_file_entry(
        &self,
        digest_config: DigestConfig,
        fs: &ArtifactFs,
        materializer: &dyn Materializer,
        found: &RemoteDepFile,
        result: &CommandExecutionResult,
    ) -> buck2_error::Result<bool> {
        // Everything in the common digest structure is included in the remote dep file key,
        // so they should be the same but it's good to double check.
        let common = &self.common_digests;
        if common.commandline_cli_digest.as_bytes() != found.commandline_cli_digest.as_slice() {
            tracing::debug!("Remote dep files miss: command cli digests are different");
            return Ok(false);
        }
        if common.output_paths_digest.raw_digest().as_bytes()
            != found.output_paths_digest.as_slice()
        {
            tracing::debug!("Remote dep files miss: output paths digest are different");
            return Ok(false);
        }
        if common.untagged_inputs_digest.raw_digest().as_bytes()
            != found.untagged_inputs_digest.as_slice()
        {
            tracing::debug!("Remote dep files miss: untagged inputs digest are different");
            return Ok(false);
        }

        let DepFilesState::Declared {
            declared_dep_files,
            shared_declared_inputs,
            ..
        } = &self.dep_files
        else {
            return Ok(false);
        };
        let declared_dep_files = declared_dep_files.as_ref();

        // Ensure the declared dep files are the same
        if declared_dep_files.tagged.len() != found.dep_file_inputs.len() {
            tracing::debug!("Remote dep files miss: declared dep file counts are different");
            return Ok(false);
        }
        let different_dep_files_count = declared_dep_files
            .tagged
            .iter()
            .zip(found.dep_file_inputs.iter())
            .filter(|((_, declared), found)| {
                declared
                    .output
                    .get_path()
                    .with_short_path(|path| path.as_str() != found.dep_file_path)
            })
            .count();
        if different_dep_files_count != 0 {
            tracing::debug!("Remote dep files miss: declared dep files are different");
            return Ok(false);
        }

        let action_outputs = ActionOutputs::new(
            result
                .outputs
                .iter()
                .filter_map(|(o, v)| Some((o.as_ref().into_build_artifact()?.0.dupe(), v.dupe())))
                .collect(),
        );

        // Now we compare the filtered input digests.
        // To do this,
        // 1. Read the dep file contents, this should have been downloaded as part of the action cache lookup
        // 2. Filter our inputs so that only the inputs mentioned in the dep files are retained
        // 3. Compare that against the one we retrieved from the remote dep file cache: RemoteDepFile.dep_file_inputs
        let dep_files = read_dep_files(
            false,
            // The declared dep files of this and the found action are the same
            declared_dep_files,
            &action_outputs,
            fs,
            materializer,
        )
        .await
        .buck_error_context("Error reading dep files")?;

        let dep_files = match dep_files {
            Some(dep_files) => dep_files,
            None => {
                tracing::debug!("Remote dep files miss: Dep files cannot be materialized");
                return Ok(false);
            }
        };

        let computed_filtered_fingerprints = shared_declared_inputs
            .clone()
            .unshare()
            .filter(dep_files, fs)?
            .fingerprint(digest_config);

        let different_digest_count = computed_filtered_fingerprints
            .tagged
            .iter()
            .zip(found.dep_file_inputs.iter())
            .filter(|((_, f1), found)| {
                f1.fingerprint().raw_digest().as_bytes() != found.filtered_fingerprint.as_slice()
            })
            .count();

        if different_digest_count != 0 {
            tracing::debug!("Remote dep files miss: Filtered input digests are different");
            return Ok(false);
        }

        // Everything matches! The action from cache can be used.
        Ok(true)
    }
}

pub(crate) fn make_dep_file_bundle<'a>(
    ctx: &mut dyn ActionExecutionCtx,
    visitor: DepFilesCommandLineVisitor<'_>,
    expanded_command_line_digest: ExpandedCommandLineDigest,
    execution_paths: &'a CommandExecutionPaths,
    local_worker_execution_paths: Option<&'a CommandExecutionPaths>,
    local_reuse_keys: LocalReuseKeys,
) -> buck2_error::Result<DepFileBundle> {
    let input_directory_digest = execution_paths.input_directory().fingerprint();
    let dep_files_key = RunActionKey::from_action_execution_target(ctx.target());

    let DepFilesCommandLineVisitor {
        inputs: declared_inputs,
        tagged_outputs,
        ..
    } = visitor;

    // Filter out tags with no dep file associated with it
    let tagged_outputs: OrderedMap<ArtifactTag, DeclaredDepFile> = tagged_outputs
        .into_iter()
        .filter_map(|(tag, (label, output))| {
            let output = output?;
            Some((tag, DeclaredDepFile { label, output }))
        })
        .collect();

    let declared_dep_files = DeclaredDepFiles {
        tagged: tagged_outputs,
    };

    let (dep_files, untagged_inputs_digest) =
        match NonEmptyDeclaredDepFiles::new(declared_dep_files) {
            Some(declared_dep_files) => {
                let shared_declared_inputs = declared_inputs
                    .to_directories(ctx)?
                    .share(ctx.digest_config());
                let untagged_inputs_digest = shared_declared_inputs.untagged.fingerprint().dupe();
                (
                    DepFilesState::Declared {
                        declared_dep_files,
                        shared_declared_inputs,
                        filtered_input_fingerprints: None,
                    },
                    untagged_inputs_digest,
                )
            }
            None => (DepFilesState::NoDepFiles, input_directory_digest.dupe()),
        };

    // Construct digests needed to construct a remote dep file key and remote dep file entry (if needed)
    let common_digests = CommonDigests {
        commandline_cli_digest: expanded_command_line_digest,
        untagged_inputs_digest,
        output_paths_digest: get_output_path_digest(
            ctx.digest_config(),
            execution_paths.output_paths(),
        ),
        local_worker_inputs_digest: local_worker_execution_paths
            .map(|p| p.input_directory().fingerprint().dupe()),
    };

    Ok(DepFileBundle {
        dep_files_key,
        input_directory_digest: input_directory_digest.data().dupe(),
        dep_files,
        common_digests,
        local_reuse_keys,
    })
}

/// See if there is an identical action that matches in the cache.
/// If there is, return the outputs. Otherwise, additionally return a boolean to indicate if the lookup was a miss.
#[instrument(level = "debug", skip(bundle, ctx), fields(key = %bundle.dep_files_key))]
async fn match_if_identical_action(
    ctx: &dyn ActionExecutionCtx,
    bundle: &DepFileBundle,
    declared_outputs: &[BuildArtifact],
) -> buck2_error::Result<(Option<ActionOutputs>, bool)> {
    let previous_state = match get_dep_files(&bundle.dep_files_key) {
        Some(d) => d.dupe(),
        None => return Ok((None, false)),
    };

    let actions_match = check_action(bundle, &previous_state, declared_outputs);

    if actions_match == InitialDepFileLookupResult::Hit
        && outputs_match(ctx, previous_state.result()).await?
    {
        tracing::trace!("Dep files are a hit");
        return Ok((Some(previous_state.result.dupe()), false));
    }

    // Don't remote the key from cache in this case as we did not fully check with the dep file content
    tracing::trace!("Local dep file cache does not have an identical action cached");
    Ok((
        None,
        actions_match == InitialDepFileLookupResult::CheckFilteredInputs,
    ))
}

/// Match the dep file recorded for key, or clear it from the map (if it exists).
#[instrument(level = "debug", skip(bundle, ctx), fields(key = %bundle.dep_files_key))]
async fn match_or_clear_dep_file(
    ctx: &dyn ActionExecutionCtx,
    bundle: &DepFileBundle,
    declared_outputs: &[BuildArtifact],
) -> buck2_error::Result<Option<ActionOutputs>> {
    let previous_state = match get_dep_files(&bundle.dep_files_key) {
        Some(d) => d.dupe(),
        None => return Ok(None),
    };

    let dep_files_match = dep_files_match(bundle, &previous_state, declared_outputs, ctx).await?;
    if dep_files_match && outputs_match(ctx, previous_state.result()).await? {
        tracing::trace!("Dep files are a hit");
        return Ok(Some(previous_state.result.dupe()));
    }

    tracing::trace!("Dep files are a miss, removing the key from cache");
    DEP_FILES.remove(&bundle.dep_files_key);

    Ok(None)
}

#[derive(PartialEq)]
enum InitialDepFileLookupResult {
    Hit,
    Miss,
    CheckFilteredInputs,
}

enum StoredFilteredInputs<'a> {
    InDaemon(&'a DepFileState),
    ActionReuse(&'a ActionReuseFilteredInputs),
}

async fn outputs_match(
    ctx: &dyn ActionExecutionCtx,
    outputs: &ActionOutputs,
) -> buck2_error::Result<bool> {
    let fs = ctx.fs();

    // Finally, we need to make sure that the artifacts in the materializer actually
    // match. This is necessary in case a different action wrote to those artifacts and
    // didn't use the same cache key.
    let output_matches = outputs
        .iter()
        .map(|(path, value)| {
            Ok((
                fs.buck_out_path_resolver()
                    .resolve_gen(path, Some(&value.content_based_path_hash()))?,
                value.dupe(),
            ))
        })
        .collect::<buck2_error::Result<Vec<(ProjectRelativePathBuf, ArtifactValue)>>>()?;

    let materializer_accepts = ctx
        .materializer()
        .declare_match(output_matches)
        .await?
        .is_match();
    Ok(materializer_accepts)
}

fn check_action(
    bundle: &DepFileBundle,
    previous_state: &DepFileState,
    declared_outputs: &[BuildArtifact],
) -> InitialDepFileLookupResult {
    let declares_same_dep_files = match &bundle.dep_files {
        DepFilesState::NoDepFiles => previous_state.declared_dep_files().is_none(),
        DepFilesState::Declared {
            declared_dep_files, ..
        } => declared_dep_files
            .as_ref()
            .declares_same_dep_files(previous_state.declared_dep_files()),
    };
    if !declares_same_dep_files {
        // We first need to check if the same dep files existed before or not. If not, then we
        // can't assume they'll still be on disk, and we have to bail.
        tracing::trace!("Dep files miss: Dep files declaration has changed");
        DEP_FILES.remove(&bundle.dep_files_key);
        return InitialDepFileLookupResult::Miss;
    }

    if !outputs_are_reusable(declared_outputs, &previous_state.result) {
        tracing::trace!("Dep files miss: Output declaration has changed");
        DEP_FILES.remove(&bundle.dep_files_key);
        return InitialDepFileLookupResult::Miss;
    }

    if bundle.common_digests.commandline_cli_digest != previous_state.digests.cli {
        tracing::trace!("Dep files miss: Command line has changed");
        DEP_FILES.remove(&bundle.dep_files_key);
        return InitialDepFileLookupResult::Miss;
    }

    if bundle.local_reuse_keys.in_daemon != previous_state.digests.in_daemon_reuse_key {
        tracing::trace!("Dep files miss: In-daemon reuse key has changed");
        DEP_FILES.remove(&bundle.dep_files_key);
        return InitialDepFileLookupResult::Miss;
    }

    if bundle.common_digests.local_worker_inputs_digest
        != previous_state.digests.local_worker_directory
    {
        tracing::trace!("Dep files miss: Local worker inputs have changed");
        DEP_FILES.remove(&bundle.dep_files_key);
        return InitialDepFileLookupResult::Miss;
    }

    if bundle.input_directory_digest == previous_state.digests.directory {
        // The actions are identical
        tracing::trace!("Dep files hit: Command line and directory have not changed");
        return InitialDepFileLookupResult::Hit;
    }
    InitialDepFileLookupResult::CheckFilteredInputs
}

async fn dep_files_match(
    bundle: &DepFileBundle,
    previous_state: &DepFileState,
    declared_outputs: &[BuildArtifact],
    ctx: &dyn ActionExecutionCtx,
) -> buck2_error::Result<bool> {
    let initial_check = check_action(bundle, previous_state, declared_outputs);
    if initial_check == InitialDepFileLookupResult::Hit {
        return Ok(true);
    }

    let DepFilesState::Declared {
        shared_declared_inputs,
        ..
    } = &bundle.dep_files
    else {
        return Ok(false);
    };

    let previous_declared_dep_files = match previous_state.has_declared_dep_files {
        Some(ref has_declared_dep_files) => &has_declared_dep_files.declared_dep_files,
        None => {
            return Ok(false);
        }
    };

    filtered_inputs_match(
        ctx,
        previous_state.has_signatures(),
        previous_declared_dep_files,
        previous_state.result(),
        shared_declared_inputs,
        StoredFilteredInputs::InDaemon(previous_state),
    )
    .await
    .buck_error_context(
        "Error reading persisted dep files. \
            Fix the command that produced an invalid dep file. \
            You may also use `buck2 debug flush-dep-files` to drop all dep file state.",
    )
}

async fn filtered_inputs_match(
    ctx: &dyn ActionExecutionCtx,
    has_signatures: bool,
    declared_dep_files: &DeclaredDepFiles,
    outputs: &ActionOutputs,
    declared_inputs: &PartitionedInputs<ActionSharedDirectory>,
    stored: StoredFilteredInputs<'_>,
) -> buck2_error::Result<bool> {
    let Some(dep_files) = read_dep_files(
        has_signatures,
        declared_dep_files,
        outputs,
        ctx.fs(),
        ctx.materializer(),
    )
    .await?
    else {
        tracing::trace!("Dep files miss: Dep files cannot be materialized");
        return Ok(false);
    };
    let digest_config = ctx.digest_config();
    match stored {
        StoredFilteredInputs::InDaemon(previous_state) => {
            let previous_fingerprints = previous_state.locked_compute_fingerprints(
                Cow::Borrowed(&dep_files),
                keep_directories()?,
                digest_config,
                ctx.fs(),
            )?;
            let current = declared_inputs
                .clone()
                .unshare()
                .filter(dep_files, ctx.fs())?
                .fingerprint(digest_config);
            Ok(*previous_fingerprints == current)
        }
        StoredFilteredInputs::ActionReuse(previous_fingerprints) => {
            let current = declared_inputs
                .clone()
                .unshare()
                .filter(dep_files, ctx.fs())?
                .fingerprint(digest_config);
            Ok(previous_fingerprints == &ActionReuseFilteredInputs::from(&current))
        }
    }
}

/// A stored superset remains reusable, but every output currently declared by the action must be
/// present because newly required outputs cannot be reconstructed from the older result.
fn outputs_are_reusable(declared_outputs: &[BuildArtifact], outputs: &ActionOutputs) -> bool {
    for out in declared_outputs {
        if outputs.get(out.get_path()).is_none() {
            return false;
        }
    }
    true
}

/// Read the dep files for this DepFileState. This will return None if the dep files cannot be
/// materialized (because they e.g. expired).
pub(crate) async fn read_dep_files(
    has_signatures: bool,
    declared_dep_files: &DeclaredDepFiles,
    result: &ActionOutputs,
    fs: &ArtifactFs,
    materializer: &dyn Materializer,
) -> buck2_error::Result<Option<ConcreteDepFiles>> {
    // NOTE: We only materialize if we haven't computed our signatures yet, since we know we
    // can't have computed our signatures without having read the dep file already. In an ideal
    // world this wouldn't be necessary, but in practice contention on the materializer makes
    // this slower.
    if !has_signatures {
        match declared_dep_files
            .materialize(fs, materializer, result)
            .await
        {
            Ok(()) => {}
            Err(MaterializeDepFilesError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
    }

    let dep_files = declared_dep_files.read(fs, result).buck_error_context(
        "Error reading dep files, verify that the action produced valid output",
    )?;

    Ok(dep_files)
}

fn compute_fingerprints(
    directories: PartitionedInputs<ActionDirectoryBuilder>,
    dep_files: ConcreteDepFiles,
    digest_config: DigestConfig,
    keep_directories: bool,
    fs: &ArtifactFs,
) -> buck2_error::Result<StoredFingerprints> {
    let filtered_directories = directories
        .filter(dep_files, fs)?
        .fingerprint(digest_config);
    if keep_directories {
        Ok(StoredFingerprints::Dirs(filtered_directories))
    } else {
        Ok(StoredFingerprints::Digests(PartitionedInputs::from(
            &filtered_directories,
        )))
    }
}

async fn eagerly_compute_fingerprints(
    digest_config: DigestConfig,
    artifact_fs: &ArtifactFs,
    materializer: &dyn Materializer,
    shared_declared_inputs: &PartitionedInputs<ActionSharedDirectory>,
    declared_dep_files: &DeclaredDepFiles,
    result: &ActionOutputs,
) -> buck2_error::Result<StoredFingerprints> {
    let dep_files = read_dep_files(false, declared_dep_files, result, artifact_fs, materializer)
        .await?
        .ok_or_else(|| internal_error!("Dep file not found"))?;

    let fingerprints = compute_fingerprints(
        shared_declared_inputs.clone().unshare(),
        dep_files,
        digest_config,
        keep_directories()?,
        artifact_fs,
    )?;
    Ok(fingerprints)
}

/// Post-process the dep files produced by an action.
pub(crate) async fn populate_dep_files(
    ctx: &dyn ActionExecutionCtx,
    dep_file_bundle: DepFileBundle,
    declared_outputs: &[BuildArtifact],
    result: &ActionOutputs,
    was_produced_locally: bool,
    identical_action_digest: Option<&ActionDigest>,
) -> buck2_error::Result<()> {
    let DepFileBundle {
        dep_files_key,
        input_directory_digest,
        mut dep_files,
        common_digests,
        local_reuse_keys,
    } = dep_file_bundle;
    let LocalReuseKeys {
        in_daemon,
        persistent,
    } = local_reuse_keys;
    let materializer = ctx.materializer();
    let eager_dep_files = ctx.run_action_knobs().eager_dep_files;
    let should_write_action_reuse =
        was_produced_locally && !declared_outputs.is_empty() && persistent.is_some();
    if let DepFilesState::Declared {
        declared_dep_files,
        shared_declared_inputs,
        filtered_input_fingerprints,
    } = &mut dep_files
        && filtered_input_fingerprints.is_none()
        && (eager_dep_files || should_write_action_reuse)
    {
        match eagerly_compute_fingerprints(
            ctx.digest_config(),
            ctx.fs(),
            materializer,
            shared_declared_inputs,
            declared_dep_files.as_ref(),
            result,
        )
        .await
        {
            Ok(fingerprints) => *filtered_input_fingerprints = Some(fingerprints),
            Err(error) if eager_dep_files => return Err(error),
            Err(error) => {
                let _ = soft_error!(
                    "action_reuse_dep_file_fingerprint_error",
                    error,
                    quiet: true
                );
            }
        }
    }
    if was_produced_locally
        && !declared_outputs.is_empty()
        && let Some(persistent) = persistent.as_ref()
    {
        let identical_action =
            identical_action_digest.map(|action_digest| IdenticalActionReuseKey {
                action_digest: action_digest.dupe(),
                local_reuse_key: persistent.as_ref().dupe(),
            });
        let mut dep_file = None;

        if let DepFilesState::Declared {
            declared_dep_files,
            filtered_input_fingerprints: Some(fingerprints),
            ..
        } = &dep_files
        {
            let key = dep_file_action_reuse_key(
                ctx.digest_config(),
                &common_digests,
                declared_dep_files,
                ctx.re_platform(),
                persistent,
            );
            dep_file = Some(DepFileActionReuseWrite {
                key,
                filtered_inputs: fingerprints.into(),
            });
        }
        match action_reuse_outputs(declared_outputs, result) {
            Ok(outputs) => {
                let write = match (identical_action, dep_file) {
                    (Some(key), None) => Some(ActionReuseWrite::IdenticalAction { key, outputs }),
                    (None, Some(dep_file)) => Some(ActionReuseWrite::DepFile { dep_file, outputs }),
                    (Some(identical_action), Some(dep_file)) => {
                        Some(ActionReuseWrite::IdenticalActionAndDepFile {
                            identical_action,
                            dep_file,
                            outputs,
                        })
                    }
                    (None, None) => None,
                };
                if let Some(write) = write
                    && let Err(e) = materializer.put_action_reuse(write).await
                {
                    let _ = soft_error!("action_reuse_put_error", e, quiet: true);
                }
            }
            Err(e) => {
                let _ = soft_error!("action_reuse_outputs_error", e, quiet: true);
            }
        }
    }

    let digests = CommandDigests {
        cli: common_digests.commandline_cli_digest,
        directory: input_directory_digest,
        in_daemon_reuse_key: in_daemon,
        local_worker_directory: common_digests.local_worker_inputs_digest,
    };

    let state = match dep_files {
        DepFilesState::NoDepFiles => DepFileState {
            digests,
            has_declared_dep_files: None,
            result: result.dupe(),
            was_produced_locally,
        },
        DepFilesState::Declared {
            declared_dep_files,
            shared_declared_inputs,
            filtered_input_fingerprints,
        } => {
            let input_signatures = match filtered_input_fingerprints {
                Some(fingerprints) => DepFileStateInputSignatures::Computed(fingerprints),
                None => DepFileStateInputSignatures::Deferred(Some(shared_declared_inputs)),
            };
            DepFileState {
                digests,
                result: result.dupe(),
                was_produced_locally,
                has_declared_dep_files: Some(HasDeclaredDepFiles {
                    declared_dep_files: declared_dep_files.0,
                    input_signatures: Mutex::new(input_signatures),
                }),
            }
        }
    };

    DEP_FILES.insert(dep_files_key, Arc::new(state));
    Ok(())
}

/// Inputs partitioned by tag. `D` is the representation of the set of inputs.
#[derive(Clone, PartialEq, Eq, Allocative)]
pub(crate) struct PartitionedInputs<D> {
    pub(crate) untagged: D,
    pub(crate) tagged: OrderedMap<Arc<str>, D>,
}

impl<D> PartitionedInputs<D> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &D> {
        std::iter::once(&self.untagged).chain(self.tagged.values())
    }
}

impl Default for PartitionedInputs<Vec<ArtifactGroup>> {
    fn default() -> Self {
        Self {
            tagged: Default::default(),
            untagged: Default::default(),
        }
    }
}

impl PartitionedInputs<Vec<ArtifactGroup>> {
    /// Add an input to this set of PartitionedInputs.
    fn visit_input(
        &mut self,
        input: ArtifactGroup,
        tags: Vec<&ArtifactTag>,
        dep_files: &RunActionDepFiles,
    ) {
        for tag in tags {
            match dep_files.labels.get(tag) {
                None => {}
                Some(label) => {
                    self.tagged.get_mut(label).unwrap().push(input);
                    // We already verified that there is only one relevant dep-file tag per input, so we are done.
                    return;
                }
            }
        }

        self.untagged.push(input);
    }

    /// Produce Directories from this set of PartitionedInputs. One directory will be produced for
    /// each tag (and one for untagged). This will actually allocate directories (whereas until now
    /// we only held references to artifacts).
    fn to_directories(
        &self,
        ctx: &dyn ActionExecutionCtx,
    ) -> buck2_error::Result<PartitionedInputs<ActionDirectoryBuilder>> {
        fn reduce(
            ctx: &dyn ActionExecutionCtx,
            inputs: &[ArtifactGroup],
        ) -> buck2_error::Result<ActionDirectoryBuilder> {
            let mut builder = LazyActionDirectoryBuilder::empty();

            for input in inputs {
                let input = ctx.artifact_values(input);
                input.add_to_directory(&mut builder, ctx.fs())?;
            }

            builder.finalize()
        }

        Ok(PartitionedInputs {
            untagged: reduce(ctx, &self.untagged)?,
            tagged: self
                .tagged
                .iter()
                .map(|(tag, inputs)| buck2_error::Ok((tag.dupe(), reduce(ctx, inputs)?)))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl PartitionedInputs<ActionSharedDirectory> {
    fn unshare(self) -> PartitionedInputs<ActionDirectoryBuilder> {
        PartitionedInputs {
            untagged: self.untagged.into_builder(),
            tagged: self
                .tagged
                .into_iter()
                .map(|(k, v)| (k, v.into_builder()))
                .collect(),
        }
    }
}

impl PartitionedInputs<ActionDirectoryBuilder> {
    fn share(self, digest_config: DigestConfig) -> PartitionedInputs<ActionSharedDirectory> {
        PartitionedInputs {
            untagged: self
                .untagged
                .fingerprint(digest_config.as_directory_serializer())
                .shared(&*INTERNER),
            tagged: self
                .tagged
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        v.fingerprint(digest_config.as_directory_serializer())
                            .shared(&*INTERNER),
                    )
                })
                .collect(),
        }
    }

    /// Filter this set of PartitionedInputs using dep files that were produced by the command (or
    /// a previous invocation thereof). For each partition of inputs (i.e. untagged, or specific
    /// tags), only the inputs listed in the dep file will be retained.
    fn filter(
        mut self,
        mut concrete_dep_files: ConcreteDepFiles,
        fs: &ArtifactFs,
    ) -> buck2_error::Result<Self> {
        fn filter(
            label: &Arc<str>,
            builder: &mut ActionDirectoryBuilder,
            concrete_dep_files: &mut ConcreteDepFiles,
            fs: &ArtifactFs,
        ) -> buck2_error::Result<()> {
            let mut matching_selector = match concrete_dep_files.get_selector(label, fs, builder)? {
                Some(s) => s,
                None => return Ok(()),
            };

            expand_selector_for_dependencies(builder, &mut matching_selector);

            // NOTE: We ignore the filtering if it produces an invalid directory. If we can't
            // filter, that means we're selecting into a leaf, which means one of two things:
            //
            // - We hit an external symlink, and we can't traverse through those.
            // - The directory structure changed and was a dir is now a leaf, in which case we'll
            // select the leaf but when comparing this to the dir it'll conflict.
            let _ = matching_selector.filter(builder);

            Ok(())
        }

        for (label, dir) in self.tagged.iter_mut() {
            filter(label, dir, &mut concrete_dep_files, fs)?;
        }

        Ok(self)
    }

    /// Compute fingerprints for all the PartitionedInputs in here. This lets us do comparisons.
    fn fingerprint(
        self,
        digest_config: DigestConfig,
    ) -> PartitionedInputs<ActionImmutableDirectory> {
        PartitionedInputs {
            untagged: self
                .untagged
                .fingerprint(digest_config.as_directory_serializer()),
            tagged: self
                .tagged
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        v.fingerprint(&TaggedInputsDirectorySerializer {
                            cas_digest_config: digest_config.cas_digest_config(),
                        }),
                    )
                })
                .collect(),
        }
    }
}

impl From<&PartitionedInputs<ActionImmutableDirectory>> for PartitionedInputs<TrackedFileDigest> {
    fn from(inputs: &PartitionedInputs<ActionImmutableDirectory>) -> Self {
        PartitionedInputs {
            untagged: inputs.untagged.fingerprint().dupe(),
            tagged: inputs
                .tagged
                .iter()
                .map(|(k, v)| (k.dupe(), v.fingerprint().dupe()))
                .collect(),
        }
    }
}

/// A dep file declared by a command. This includes its label (which we use for stability across
/// command executions), as well as the artifact that the command will write the dep file to.
#[derive(Hash, PartialEq, Eq, Debug, Dupe, Clone, Allocative)]
struct DeclaredDepFile {
    label: Arc<str>,
    output: Artifact,
}

/// All the dep files declared by a command;
#[derive(Default, Debug, Allocative)]
pub(crate) struct DeclaredDepFiles {
    tagged: OrderedMap<ArtifactTag, DeclaredDepFile>,
}

impl DeclaredDepFiles {
    pub(crate) fn is_empty(&self) -> bool {
        self.tagged.is_empty()
    }

    /// Given an ActionOutputs, materialize this set of dep files, so that we may read them later.
    async fn materialize(
        &self,
        fs: &ArtifactFs,
        materializer: &dyn Materializer,
        result: &ActionOutputs,
    ) -> Result<(), MaterializeDepFilesError> {
        let mut paths = Vec::with_capacity(self.tagged.len());

        for declared_dep_file in self.tagged.values() {
            let dep_file = &declared_dep_file.output;
            let content_hash = if declared_dep_file
                .output
                .path_resolution_requires_artifact_value()
            {
                Some(
                    result
                        .get_from_artifact_path(&declared_dep_file.output.get_path())
                        .expect("declared dep file must be one of the ActionOutputs!")
                        .content_based_path_hash(),
                )
            } else {
                None
            };
            let path = dep_file
                .resolve_path(fs, content_hash.as_ref())
                .map_err(|e| MaterializeDepFilesError::MaterializationFailed { source: e })?;
            paths.push(path);
        }

        if paths.is_empty() {
            return Ok(());
        }

        let mut has_not_found = false;
        let mut stream = materializer
            .materialize_many(paths)
            .await
            .map_err(|e| MaterializeDepFilesError::MaterializationFailed { source: e })?;

        while let Some(dep_file) = stream.next().await {
            match dep_file {
                Ok(()) => {}
                Err(MaterializationError::NotFound { .. }) => {
                    has_not_found = true;
                }
                Err(e) => {
                    return Err(MaterializeDepFilesError::MaterializationFailed {
                        source: e.into(),
                    });
                }
            }
        }

        if has_not_found {
            Err(MaterializeDepFilesError::NotFound)
        } else {
            Ok(())
        }
    }

    /// Read this set of dep files, producing ConcreteDepFiles. This can then be used to compute
    /// signatures for the input set that was used, and for future input sets.
    fn read(
        &self,
        fs: &ArtifactFs,
        result: &ActionOutputs,
    ) -> buck2_error::Result<Option<ConcreteDepFiles>> {
        let mut contents =
            StdBuckHashMap::with_capacity_and_hasher(self.tagged.len(), Default::default());

        for declared_dep_file in self.tagged.values() {
            let content_hash = if declared_dep_file
                .output
                .path_resolution_requires_artifact_value()
            {
                Some(
                    result
                        .get_from_artifact_path(&declared_dep_file.output.get_path())
                        .expect("declared dep file must be one of the ActionOutputs!")
                        .content_based_path_hash(),
                )
            } else {
                None
            };
            let dep_file = declared_dep_file
                .output
                .resolve_path(fs, content_hash.as_ref())?;

            let read_dep_file: buck2_error::Result<()> = try {
                let dep_file_path = fs.fs().resolve(&dep_file);
                let dep_file = fs_util::read_to_string_if_exists(&dep_file_path)?;

                let dep_file = match dep_file {
                    Some(dep_file) => dep_file,
                    None => {
                        soft_error!(
                            "missing_dep_file",
                            buck2_error::buck2_error!(
                                buck2_error::ErrorTag::Input,
                                "Dep file is missing at {}",
                                dep_file_path
                            )
                        )?;
                        return Ok(None);
                    }
                };

                contents.insert(declared_dep_file.label.dupe(), dep_file);
            };

            read_dep_file.with_buck_error_context(|| {
                format!(
                    "Action execution produced an invalid `{}` dep file at `{}`",
                    declared_dep_file.label, dep_file,
                )
            })?;
        }

        Ok(Some(ConcreteDepFiles { contents }))
    }

    /// Returns whether two DeclaredDepFile instances have the same dep files. This ignores the tag
    /// identity, but it requires the same paths declared using the same name. This is a
    /// pre-requisite for being able to reuse dep files from a previous invocation.
    fn declares_same_dep_files(&self, other: Option<&Self>) -> bool {
        match other {
            None => self.tagged.is_empty(),
            Some(other) => {
                let this = self.tagged.values().collect::<StdBuckHashSet<_>>();
                let other = other.tagged.values().collect::<StdBuckHashSet<_>>();
                this == other
            }
        }
    }
}

#[derive(buck2_error::Error, Debug)]
#[buck2(tag = Tier0)]
enum MaterializeDepFilesError {
    #[error("Error materializing dep file")]
    MaterializationFailed {
        #[source]
        source: buck2_error::Error,
    },

    #[error("A dep file was not found")]
    NotFound,
}

/// A set of concrete dep files. That is, given a label, a string that represents the
/// content of the corresponding dep file.
#[derive(Clone)]
pub(crate) struct ConcreteDepFiles {
    contents: StdBuckHashMap<Arc<str>, String>,
}

impl ConcreteDepFiles {
    fn get_selector(
        &self,
        label: &Arc<str>,
        fs: &ArtifactFs,
        builder: &ActionDirectoryBuilder,
    ) -> buck2_error::Result<Option<DirectorySelector>> {
        let dep_file = match self.contents.get(label) {
            Some(dep_file) => dep_file,
            None => return Ok(None),
        };

        let mut selector = DirectorySelector::empty();
        for line in dep_file.split('\n') {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let path = ForwardRelativePathNormalizer::normalize_path(line)
                .buck_error_context("Invalid line encountered in dep file")?;

            if let Err(e) = Self::add_path_to_selector(path, &mut selector, fs, builder) {
                soft_error!("failed_to_add_dep_file_path_to_selector", e, error_on_oss: true)?;
                return Ok(None);
            }
        }

        Ok(Some(selector))
    }

    /// If dep-files contain content-based paths, then the paths in our input directory might not
    /// match the paths that were written to the dep-file in the previous invocation of the action.
    ///
    /// Therefore, if we have a path with a content hash, we look in the input directory for all
    /// possible content hashes, and construct paths based on those hashes instead.
    ///
    /// This does mean that we might over-approximate on the tagged input files, since multiple paths
    /// can be added in place of a single entry in the dep-file, but (1) it's unlikely to happen
    /// anyway and (2) the multiple files with conflicting paths are likely to change together anyway,
    /// so this doesn't seem like a big issue.
    fn add_path_to_selector(
        path: Cow<'_, ForwardRelativePath>,
        selector: &mut DirectorySelector,
        fs: &ArtifactFs,
        builder: &ActionDirectoryBuilder,
    ) -> buck2_error::Result<()> {
        if !path.starts_with(fs.buck_out_path_resolver().root()) {
            // This path isn't in buck-out, no content-based hash to replace.
            selector.select(path.as_ref());
            return Ok(());
        }

        let mut before_content_hash_parts = vec![];
        let mut path_iter = path.as_ref().iter();
        // Paths always begin with "buck-out/<ISOLATION_DIR>/<gen or art, etc.>/<CELL>", so
        // we can skip the first 4 segments.
        for _ in 0..4 {
            if let Some(segment) = path_iter.next() {
                before_content_hash_parts.push(segment);
            }
        }

        // If the path uses a configuration-based hash, it will come next.
        let potential_configuration_hash = path_iter.next();
        let should_look_for_content_based_hash = match potential_configuration_hash {
            Some(s) => {
                before_content_hash_parts.push(s);
                !is_hash(s.as_str())
            }
            None => false,
        };

        if !should_look_for_content_based_hash {
            selector.select(path.as_ref());
            return Ok(());
        }

        for segment in &mut path_iter {
            if !is_hash(segment.as_str()) {
                before_content_hash_parts.push(segment);
            } else {
                // We found a content-based hash.
                let dir_in_builder = find(builder.as_ref(), before_content_hash_parts.clone())?;
                let after = path_iter.as_path();
                for after_segment in after.iter() {
                    if is_hash(after_segment.as_str()) {
                        return Err(buck2_error::internal_error!(
                            "Path {} cannot be normalized for dep-files because it has two path segments that look like a content-based hash!",
                            path,
                        ));
                    }
                }
                if let Some(dir_in_builder) = dir_in_builder {
                    match dir_in_builder {
                        DirectoryEntry::Dir(d) => {
                            for (name, _entry) in d.entries() {
                                if is_hash(name.as_str()) {
                                    let full_path: ForwardRelativePathBuf =
                                        before_content_hash_parts
                                            .iter()
                                            .copied()
                                            .chain(std::iter::once(name))
                                            .chain(after.iter())
                                            .collect();
                                    selector.select(&full_path);
                                }
                            }
                        }
                        DirectoryEntry::Leaf(..) => {
                            return Err(buck2_error::internal_error!(
                                "Found content-based hash {} in path {} that was a leaf in the input directory!",
                                segment,
                                path,
                            ));
                        }
                    }
                }
                return Ok(());
            }
        }

        selector.select(path.as_ref());
        Ok(())
    }
}

/// A command line visitor to collect inputs and outputs in a form relevant for dep files
/// computations.
pub(crate) struct DepFilesCommandLineVisitor<'a> {
    pub(crate) inputs: PartitionedInputs<Vec<ArtifactGroup>>,
    pub(crate) tagged_outputs: OrderedMap<ArtifactTag, (Arc<str>, Option<Artifact>)>,
    dep_files: &'a RunActionDepFiles,
}

impl<'a> DepFilesCommandLineVisitor<'a> {
    pub(crate) fn new(dep_files: &'a RunActionDepFiles) -> Self {
        // Prepopulate inputs & outputs to maintain the ordering of the labels declaration.

        let tagged_inputs: OrderedMap<Arc<str>, Vec<ArtifactGroup>> = dep_files
            .labels
            .iter()
            .map(|(_tag, label)| (label.dupe(), Default::default()))
            .collect();

        let tagged_outputs: OrderedMap<ArtifactTag, (Arc<str>, Option<Artifact>)> = dep_files
            .labels
            .iter()
            .map(|(tag, label)| (tag.dupe(), (label.dupe(), None)))
            .collect();
        Self {
            inputs: PartitionedInputs {
                untagged: Default::default(),
                tagged: tagged_inputs,
            },
            tagged_outputs,
            dep_files,
        }
    }

    pub fn inputs(&self) -> impl Iterator<Item = &ArtifactGroup> {
        self.inputs.iter().flat_map(|g| g.iter())
    }
}

impl<'v> CommandLineArtifactVisitor<'v> for DepFilesCommandLineVisitor<'_> {
    fn visit_input(&mut self, input: ArtifactGroup, tags: Vec<&ArtifactTag>) {
        self.inputs.visit_input(input, tags, self.dep_files);
    }

    fn visit_declared_output(&mut self, _artifact: OutputArtifact<'v>, _tags: Vec<&ArtifactTag>) {
        unreachable!("Dep files visitors are run after freezing")
    }

    fn visit_frozen_output(&mut self, artifact: Artifact, tags: Vec<&ArtifactTag>) {
        for tag in tags {
            // NOTE: We have validated tags earlier, so we know that if a tag does not point to
            // a dep file here, it's safe to ignore it. We also know that we'll have exactly 1
            // dep file per tag.
            if let Some((_label, output)) = self.tagged_outputs.get_mut(tag) {
                // NOTE: analysis has been done so we know inputs are bound now.
                *output = Some(artifact);
                return;
            }
        }
    }
}

fn is_hash(s: &str) -> bool {
    if s.len() != 16 {
        return false;
    }

    for c in s.chars() {
        if !c.is_ascii_hexdigit() {
            return false;
        }
    }

    true
}

fn rename_hash_dirs(name: &FileName) -> Option<FileNameBuf> {
    if is_hash(name.as_str()) {
        Some(FileNameBuf::unchecked_new(
            ContentBasedPathHash::DepFilesPlaceholder.as_str(),
        ))
    } else {
        None
    }
}

#[derive(Allocative)]
#[repr(transparent)]
pub struct TaggedInputsDirectorySerializer {
    pub cas_digest_config: CasDigestConfig,
}

impl DirectoryDigester<ActionDirectoryMember, TrackedFileDigest>
    for TaggedInputsDirectorySerializer
{
    fn hash_entries<'a, D, I>(&self, entries: I) -> TrackedFileDigest
    where
        I: IntoIterator<Item = (&'a FileName, DirectoryEntry<D, &'a ActionDirectoryMember>)>,
        D: ActionFingerprintedDirectoryRef<'a>,
    {
        TrackedFileDigest::from_content(
            &ReDirectorySerializer::rename_and_serialize_entries(entries, &rename_hash_dirs),
            self.cas_digest_config,
        )
    }

    fn leaf_size(&self, leaf: &ActionDirectoryMember) -> u64 {
        match leaf {
            ActionDirectoryMember::File(f) => f.digest.size(),
            ActionDirectoryMember::Symlink(_) => 0,
            ActionDirectoryMember::ExternalSymlink(_) => 0,
        }
    }
}

#[cfg(test)]
mod tests {

    use buck2_artifact::actions::key::ActionIndex;
    use buck2_artifact::artifact::artifact_type::testing::BuildArtifactTestingExt;
    use buck2_core::configuration::data::ConfigurationData;
    use buck2_core::target::configured_target_label::ConfiguredTargetLabel;

    use super::*;

    #[test]
    fn test_dep_files_visitor_output_collection() {
        let tag1 = ArtifactTag::testing_new();
        let tag2 = ArtifactTag::testing_new();
        let tag3 = ArtifactTag::testing_new();
        let tag4 = ArtifactTag::testing_new();

        let target =
            ConfiguredTargetLabel::testing_parse("cell//pkg:foo", ConfigurationData::testing_new());
        let artifact1 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar1.h",
            ActionIndex::new(0),
        ));
        let artifact2 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar2.h",
            ActionIndex::new(0),
        ));
        let artifact3 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar3.h",
            ActionIndex::new(0),
        ));
        let artifact4 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar4.h",
            ActionIndex::new(0),
        ));
        let artifact5 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar5.h",
            ActionIndex::new(0),
        ));

        let dep_files = RunActionDepFiles {
            labels: OrderedMap::from_iter([
                (tag1.dupe(), Arc::from("l1")),
                (tag2.dupe(), Arc::from("l2")),
                (tag3.dupe(), Arc::from("l3")),
            ]),
        };

        let mut visitor = DepFilesCommandLineVisitor::new(&dep_files);
        visitor.visit_frozen_output(artifact3, vec![&tag3]);
        visitor.visit_frozen_output(artifact2, vec![&tag2]);
        visitor.visit_frozen_output(artifact1, vec![&tag1]);
        // This should be ignored as it's not included in RunActionDepFiles
        visitor.visit_frozen_output(artifact4, vec![&tag4]);
        // This should be ignored as it does not have a tag
        visitor.visit_frozen_output(artifact5, vec![]);

        // Assert that the order is preserved between the two maps
        let x: Vec<_> = visitor.tagged_outputs.keys().collect();
        let y: Vec<_> = dep_files.labels.keys().collect();
        assert_eq!(x, y);
    }

    #[test]
    fn test_declares_same_dep_files() {
        let target =
            ConfiguredTargetLabel::testing_parse("cell//pkg:foo", ConfigurationData::testing_new());

        let artifact1 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar1.h",
            ActionIndex::new(0),
        ));

        let artifact2 = Artifact::from(BuildArtifact::testing_new(
            target.dupe(),
            "foo/bar2.h",
            ActionIndex::new(0),
        ));

        let depfile1 = DeclaredDepFile {
            label: Arc::from("foo"),
            output: artifact1,
        };

        let depfile2 = DeclaredDepFile {
            label: Arc::from("foo"),
            output: artifact2.dupe(),
        };

        let depfile3 = DeclaredDepFile {
            label: Arc::from("bar"),
            output: artifact2,
        };

        let tag1 = ArtifactTag::testing_new();

        let tag2 = ArtifactTag::testing_new();

        let decl1 = DeclaredDepFiles {
            tagged: OrderedMap::from_iter([(tag1.dupe(), depfile1.dupe())]),
        };

        let decl2 = DeclaredDepFiles {
            tagged: OrderedMap::from_iter([(tag2.dupe(), depfile1.dupe())]),
        };

        let decl3 = DeclaredDepFiles {
            tagged: OrderedMap::from_iter([(tag2.dupe(), depfile2.dupe())]),
        };

        let decl4 = DeclaredDepFiles {
            tagged: OrderedMap::from_iter([(tag2.dupe(), depfile3.dupe())]),
        };

        assert!(decl1.declares_same_dep_files(Some(&decl1)));
        assert!(decl1.declares_same_dep_files(Some(&decl2)));
        assert!(!decl2.declares_same_dep_files(Some(&decl3)));
        assert!(!decl3.declares_same_dep_files(Some(&decl4)));
    }

    #[test]
    fn output_path_digest_frames_paths() {
        let digest_config = DigestConfig::testing_default();
        let output_path = ProjectRelativePathBuf::unchecked_new("foo/bar".to_owned());
        let actual = get_output_path_digest(digest_config, &[(output_path, OutputType::File)]);

        let mut expected = CasDigestData::digester(digest_config.cas_digest_config());
        expected.update(&1u64.to_le_bytes());
        expected.update_length_prefixed(b"foo/bar");
        expected.update(&[1]);
        let expected = expected.finalize();

        assert!(actual == expected);
    }

    #[test]
    fn action_reuse_dep_file_digest_includes_artifact_owner() {
        let digest_config = DigestConfig::testing_default();
        let first_target = ConfiguredTargetLabel::testing_parse(
            "cell//pkg:first",
            ConfigurationData::testing_new(),
        );
        let second_target = ConfiguredTargetLabel::testing_parse(
            "cell//pkg:second",
            ConfigurationData::testing_new(),
        );
        let first = Artifact::from(BuildArtifact::testing_new(
            first_target,
            "same/path.d",
            ActionIndex::new(0),
        ));
        let second = Artifact::from(BuildArtifact::testing_new(
            second_target,
            "same/path.d",
            ActionIndex::new(0),
        ));
        let tag = ArtifactTag::testing_new();
        let first = DeclaredDepFiles {
            tagged: OrderedMap::from_iter([(
                tag.dupe(),
                DeclaredDepFile {
                    label: Arc::from("dep-file"),
                    output: first,
                },
            )]),
        };
        let second = DeclaredDepFiles {
            tagged: OrderedMap::from_iter([(
                tag,
                DeclaredDepFile {
                    label: Arc::from("dep-file"),
                    output: second,
                },
            )]),
        };

        let first = NonEmptyDeclaredDepFiles::new(first)
            .unwrap()
            .digest(digest_config);
        let second = NonEmptyDeclaredDepFiles::new(second)
            .unwrap()
            .digest(digest_config);

        assert!(first != second);
    }

    #[test]
    fn action_reuse_outputs_are_matched_by_logical_path() {
        let target = ConfiguredTargetLabel::testing_parse(
            "cell//pkg:action",
            ConfigurationData::testing_new(),
        );
        let first = BuildArtifact::testing_new(target.dupe(), "first", ActionIndex::new(0));
        let second = BuildArtifact::testing_new(target, "second", ActionIndex::new(0));
        let outputs = ActionReuseOutputs::new(vec![
            ActionReuseOutput {
                output_path: second.get_path().path().to_owned(),
                content_based_path_hash: ContentBasedPathHash::Scratch,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[2; 32][..])
                    .unwrap(),
            },
            ActionReuseOutput {
                output_path: first.get_path().path().to_owned(),
                content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[1; 32][..])
                    .unwrap(),
            },
        ])
        .unwrap();

        assert_eq!(
            outputs.get(first.get_path().path()),
            Some(&ActionReuseOutput {
                output_path: first.get_path().path().to_owned(),
                content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[1; 32][..])
                    .unwrap(),
            })
        );
        assert_eq!(
            outputs.get(second.get_path().path()),
            Some(&ActionReuseOutput {
                output_path: second.get_path().path().to_owned(),
                content_based_path_hash: ContentBasedPathHash::Scratch,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[2; 32][..])
                    .unwrap(),
            })
        );
    }

    #[test]
    fn action_reuse_outputs_accept_a_stored_superset() {
        let target = ConfiguredTargetLabel::testing_parse(
            "cell//pkg:action",
            ConfigurationData::testing_new(),
        );
        let declared = BuildArtifact::testing_new(target.dupe(), "declared", ActionIndex::new(0));
        let removed = BuildArtifact::testing_new(target, "removed", ActionIndex::new(0));
        let outputs = ActionReuseOutputs::new(vec![
            ActionReuseOutput {
                output_path: declared.get_path().path().to_owned(),
                content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[1; 32][..])
                    .unwrap(),
            },
            ActionReuseOutput {
                output_path: removed.get_path().path().to_owned(),
                content_based_path_hash: ContentBasedPathHash::Scratch,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[2; 32][..])
                    .unwrap(),
            },
        ])
        .unwrap();

        assert_eq!(
            outputs
                .get(declared.get_path().path())
                .map(|output| &output.content_based_path_hash),
            Some(&ContentBasedPathHash::OutputArtifact)
        );
    }
}
