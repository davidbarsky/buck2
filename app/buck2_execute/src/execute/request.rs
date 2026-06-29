/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use allocative::Allocative;
use buck2_common::cas_digest::CasDigestData;
use buck2_common::file_ops::metadata::TrackedFileDigest;
use buck2_common::local_resource_state::LocalResourceState;
use buck2_core::content_hash::ContentBasedPathHash;
use buck2_core::execution_types::executor_config::MetaInternalExtraParams;
use buck2_core::execution_types::executor_config::ReGangWorker;
use buck2_core::execution_types::executor_config::RemoteExecutorCustomImage;
use buck2_core::execution_types::executor_config::RemoteExecutorDependency;
use buck2_core::fs::artifact_path_resolver::ArtifactFs;
use buck2_core::fs::buck_out_path::BuckOutScratchPath;
use buck2_core::fs::buck_out_path::BuckOutTestPath;
use buck2_core::fs::buck_out_path::BuildArtifactPath;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_core::soft_error;
pub use buck2_data::NetworkAccess;
use buck2_directory::directory::dashmap_directory_interner::DashMapDirectoryInterner;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::directory_iterator::DirectoryIterator;
use buck2_directory::directory::fingerprinted_directory::FingerprintedDirectory;
use buck2_error::buck2_error;
use buck2_fs::paths::abs_path::AbsPath;
use buck2_hash::BuckIndexSet;
use derive_more::Display;
use dupe::Dupe;
use gazebo::variants::UnpackVariants;
use host_sharing::host_sharing::HostSharingRequirements;
use itertools::Itertools;
use pagable::Pagable;
use prost::Message;
use remote_execution as RE;
use sorted_vector_map::SortedVectorMap;
use starlark_map::sorted_set::SortedSet;

use super::dep_file_digest::DepFileDigest;
use crate::artifact::group::artifact_group_values_dyn::ArtifactGroupValuesDyn;
use crate::digest_config::DigestConfig;
use crate::directory::ActionDirectoryEntry;
use crate::directory::ActionDirectoryMember;
use crate::directory::ActionImmutableDirectory;
use crate::directory::ActionSharedDirectory;
#[cfg(test)]
use crate::execute::environment_inheritance::BUCK_BUILD_ID;
#[cfg(test)]
use crate::execute::environment_inheritance::BUCK2_DAEMON_UUID;
#[cfg(test)]
use crate::execute::environment_inheritance::Buck2DaemonUuid;
#[cfg(test)]
use crate::execute::environment_inheritance::BuckBuildId;
use crate::execute::environment_inheritance::EnvironmentInheritance;
use crate::execute::environment_inheritance::LocalEnvironmentDirective;
use crate::execute::environment_inheritance::LocalExecutionIdentity;
use crate::execute::environment_inheritance::visit_local_execution_environment;
use crate::execute::inputs_directory::inputs_directory;

/// What protobuf messages can be stored in the action metadata blobs.
pub trait ActionMetadataBlobMessage: Message {}

impl ActionMetadataBlobMessage for RE::Action {}
impl ActionMetadataBlobMessage for RE::Command {}
impl ActionMetadataBlobMessage for RE::Tree {}

#[derive(Clone)]
pub struct ActionMetadataBlobData(pub Vec<u8>);

impl ActionMetadataBlobData {
    pub fn from_message(m: &impl ActionMetadataBlobMessage) -> ActionMetadataBlobData {
        let mut blob = Vec::new();
        // Unwrap is safe because it only fails in OOM conditions, which we pretend don't happen
        m.encode(&mut blob).unwrap();
        ActionMetadataBlobData(blob)
    }

    pub fn from_json(json: String) -> ActionMetadataBlobData {
        ActionMetadataBlobData(json.into_bytes())
    }
}

#[derive(Clone)]
pub struct ActionMetadataBlob {
    pub digest: TrackedFileDigest,
    pub path: BuildArtifactPath,
    pub content_hash: ContentBasedPathHash,
}

impl ActionMetadataBlob {
    /// Keeps the metadata's content hash coupled to every resolution of its output path.
    pub fn resolve_path(&self, fs: &ArtifactFs) -> buck2_error::Result<ProjectRelativePathBuf> {
        fs.buck_out_path_resolver()
            .resolve_gen(&self.path, Some(&self.content_hash))
    }
}

pub enum CommandExecutionInput {
    Artifact(Box<dyn ArtifactGroupValuesDyn>),
    ActionMetadata(ActionMetadataBlob),
    ScratchPath(BuckOutScratchPath),
    IncrementalRemoteOutput(
        ProjectRelativePathBuf,
        ActionDirectoryEntry<ActionSharedDirectory>,
    ),
}

/// Context safe to reuse while daemon-owned execution state remains alive.
#[derive(Allocative, Clone, Dupe, Eq, PartialEq)]
pub struct InDaemonLocalReuseKey(CasDigestData);

/// Context safe to reuse after daemon-owned execution state is gone.
#[derive(Allocative, Clone, Dupe, Eq, PartialEq)]
pub struct PersistentLocalReuseKey(CasDigestData);

impl AsRef<CasDigestData> for PersistentLocalReuseKey {
    fn as_ref(&self) -> &CasDigestData {
        &self.0
    }
}

/// Carries both scopes together so persistent gating cannot disable in-daemon reuse.
#[derive(Clone, Eq, PartialEq)]
pub struct LocalReuseKeys {
    pub in_daemon: InDaemonLocalReuseKey,
    pub persistent: Option<PersistentLocalReuseKey>,
}

#[derive(Copy, Clone, Dupe, Eq, PartialEq)]
pub enum PersistentLocalReuseEligibility {
    Eligible,
    Ineligible,
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, Dupe, Hash)]
pub enum OutputCreationBehavior {
    Create,
    Parent,
}

#[derive(Debug, buck2_error::Error)]
#[error("Incompatible executor preferences: `{}` & `{}`", a, b)]
#[buck2(input)]
struct IncompatibleExecutorPreferences {
    a: ExecutorPreference,
    b: ExecutorPreference,
}

#[derive(Copy, Clone, Dupe, Display, Debug, Allocative, PartialEq, Pagable)]
pub enum ExecutorPreference {
    Default,
    /// Fails when executed by a remote-only executor
    LocalRequired,
    /// Does not fail when executed by a remote-only executor
    LocalPreferred,
    /// Fails when executed by a local-only executor
    RemoteRequired,
    /// Does not fail when executed by a local-only executor
    RemotePreferred,
    /// When and-ed with an ExecutorPreference that doesn't *require* local or remote, this erases
    /// any preferences.
    DefaultErasePreferences,
}

impl ExecutorPreference {
    pub fn and(self, other: Self) -> buck2_error::Result<Self> {
        let requires_remote = self.requires_remote() || other.requires_remote();
        let requires_local = self.requires_local() || other.requires_local();

        if requires_remote && requires_local {
            return Err(IncompatibleExecutorPreferences { a: self, b: other }.into());
        }

        if requires_local {
            return Ok(Self::LocalRequired);
        }

        if requires_remote {
            return Ok(Self::RemoteRequired);
        }

        if self.erases_preferences() || other.erases_preferences() {
            return Ok(Self::DefaultErasePreferences);
        }

        for pref in [self, other] {
            if pref.prefers_local() {
                return Ok(Self::LocalPreferred);
            }

            if pref.prefers_remote() {
                return Ok(Self::RemotePreferred);
            }
        }

        Ok(Self::Default)
    }

    pub fn requires_remote(&self) -> bool {
        match self {
            Self::LocalRequired => false,
            Self::LocalPreferred => false,
            Self::RemoteRequired => true,
            Self::RemotePreferred => false,
            Self::Default => false,
            Self::DefaultErasePreferences => false,
        }
    }

    pub fn requires_local(&self) -> bool {
        match self {
            Self::LocalRequired => true,
            Self::LocalPreferred => false,
            Self::RemoteRequired => false,
            Self::RemotePreferred => false,
            Self::Default => false,
            Self::DefaultErasePreferences => false,
        }
    }

    pub fn prefers_local(&self) -> bool {
        match self {
            Self::LocalRequired => true,
            Self::LocalPreferred => true,
            Self::RemoteRequired => false,
            Self::RemotePreferred => false,
            Self::Default => false,
            Self::DefaultErasePreferences => false,
        }
    }

    pub fn prefers_remote(&self) -> bool {
        match self {
            Self::LocalRequired => false,
            Self::LocalPreferred => false,
            Self::RemoteRequired => true,
            Self::RemotePreferred => true,
            Self::Default => false,
            Self::DefaultErasePreferences => false,
        }
    }

    fn erases_preferences(self) -> bool {
        matches!(self, Self::DefaultErasePreferences)
    }
}

pub struct CommandExecutionPaths {
    inputs: Vec<CommandExecutionInput>,
    outputs: BuckIndexSet<CommandExecutionOutput>,

    input_directory: ActionImmutableDirectory,
    output_paths: Vec<(ProjectRelativePathBuf, OutputType)>,

    /// Total size of input files.
    input_files_bytes: u64,
}

impl CommandExecutionPaths {
    pub fn new(
        inputs: Vec<CommandExecutionInput>,
        outputs: BuckIndexSet<CommandExecutionOutput>,
        fs: &ArtifactFs,
        digest_config: DigestConfig,
        interner: Option<&DashMapDirectoryInterner<ActionDirectoryMember, TrackedFileDigest>>,
    ) -> buck2_error::Result<Self> {
        let mut builder = inputs_directory(&inputs, digest_config, fs)?;

        // RE spec requires outputs to be sorted:
        // https://github.com/bazelbuild/remote-apis/blob/1f36c310b28d762b258ea577ed08e8203274efae/build/bazel/remote/execution/v2/remote_execution.proto#L667-L669
        // We sort early here and not when we create RE action in order for local and remote actions to be in-sync.
        let outputs: BuckIndexSet<_> = outputs
            .into_iter()
            .sorted_by_key(|e| {
                let resolved = e
                    .as_ref()
                    .resolve(fs, Some(&ContentBasedPathHash::for_output_artifact()))
                    .expect("Failed to resolve output path");
                resolved.into_path()
            })
            .collect();

        let output_paths = outputs
            .iter()
            .map(|o| {
                let resolved = o
                    .as_ref()
                    .resolve(fs, Some(&ContentBasedPathHash::for_output_artifact()))?;
                if let Some(dir) = resolved.path_to_create() {
                    builder.mkdir(dir)?;
                }
                let output_type = resolved.output_type;
                Ok((resolved.into_path(), output_type))
            })
            .collect::<buck2_error::Result<Vec<_>>>()?;

        let input_directory = builder.fingerprint(digest_config.as_directory_serializer());

        let input_directory = match interner {
            Some(i) => input_directory.shared(i).as_immutable(),
            None => input_directory,
        };

        let input_files_bytes = if buck2_core::faster_directories::is_enabled() {
            input_directory.size()
        } else {
            Self::calculate_inputs_size_bytes(&input_directory)
        };

        Ok(Self {
            inputs,
            outputs,
            input_directory,
            output_paths,
            input_files_bytes,
        })
    }

    fn calculate_inputs_size_bytes(input_directory: &ActionImmutableDirectory) -> u64 {
        let mut input_files_bytes = 0;

        for entry in input_directory.unordered_walk_leaves().without_paths() {
            if let ActionDirectoryMember::File(f) = entry {
                input_files_bytes += f.digest.size();
            }
        }

        input_files_bytes
    }

    pub fn add_outputs_as_inputs(
        self,
        output_paths: impl IntoIterator<Item = CommandExecutionInput>,
        fs: &ArtifactFs,
        digest_config: DigestConfig,
        interner: Option<&DashMapDirectoryInterner<ActionDirectoryMember, TrackedFileDigest>>,
    ) -> buck2_error::Result<Self> {
        let Self {
            mut inputs,
            outputs,
            input_directory: _,
            output_paths: _,
            input_files_bytes: _,
        } = self;
        inputs.extend(output_paths);
        Self::new(inputs, outputs, fs, digest_config, interner)
    }

    pub fn input_directory(&self) -> &ActionImmutableDirectory {
        &self.input_directory
    }

    pub fn output_paths(&self) -> &[(ProjectRelativePathBuf, OutputType)] {
        &self.output_paths
    }

    pub fn input_files_bytes(&self) -> u64 {
        self.input_files_bytes
    }
}

#[derive(Copy, Clone, Dupe, Debug, Display, Allocative, Hash, PartialEq, Eq)]
pub struct WorkerId(pub u64);

pub struct WorkerSpec {
    pub id: WorkerId,
    pub exe: Vec<String>,
    pub env: SortedVectorMap<String, String>,
    pub concurrency: Option<usize>,
    pub streaming: bool,
    pub remote_key: Option<TrackedFileDigest>,
    pub input_paths: CommandExecutionPaths,
}

impl WorkerSpec {
    pub fn inputs(&self) -> &[CommandExecutionInput] {
        &self.input_paths.inputs
    }
}

pub struct RemoteWorkerSpec {
    pub id: WorkerId,
    pub init: Vec<String>,
    pub env: SortedVectorMap<String, String>,
    pub input_paths: CommandExecutionPaths,
    pub concurrency: Option<usize>,
}

/// The data contains the information about the command to be executed.
pub struct CommandExecutionRequest {
    /// Optional arguments including executable prepended to `args` to get full command line.
    /// This is used by workers to separate worker arguments from executable arguments.
    exe: Vec<String>,
    args: Vec<String>,
    paths: CommandExecutionPaths,
    env: SortedVectorMap<String, String>,
    timeout: Option<Duration>,
    pub executor_preference: ExecutorPreference,
    host_sharing_requirements: Arc<HostSharingRequirements>,
    // Used to disable the low pass filter for concurrent local actions. Enabled by default
    low_pass_filter: bool,
    /// Working directory, relative to the project root.
    working_directory: ProjectRelativePathBuf,
    /// Whether we should always prefetch stderr when executing. When it's needed, this lets us
    /// overlap stderr download with output downloads, which might be marginally useful to improve
    /// latency.
    prefetch_lossy_stderr: bool,
    /// Whether to cleanup outputs
    pub outputs_cleanup: bool,
    /// What environment variables to inherit from the Buck2 daemon.
    local_environment_inheritance: Option<EnvironmentInheritance>,
    /// Values local execution injects after the declared and inherited environment.
    /// Whether this command should override the fallback-only behavior on an hybrid executor and
    /// thus always run as if the executor was full-hybrid, assuming it is capable.
    force_full_hybrid_if_capable: bool,
    /// Whether to disable capturing performance counters for this execution.
    disable_miniperf: bool,
    required_local_resources: SortedSet<LocalResourceState>,
    /// Persistent worker to use for execution
    worker: Option<WorkerSpec>,
    /// Persistent remote worker to use for execution
    remote_worker: Option<RemoteWorkerSpec>,
    /// Whether the executor should guarantee that the inodes for all inputs are unique (i.e. avoid
    /// hardlinking identical input files, for example)
    unique_input_inodes: bool,
    /// Remote dep file key, if the action has a dep file.
    /// If this key is set and remote dep file caching is enabled, it will be used to query the cache.
    pub remote_dep_file_key: Option<DepFileDigest>,
    /// RE gang workers for gang scheduling.
    re_gang_workers: Vec<ReGangWorker>,
    /// RE dependencies to pass in action metadata.
    remote_execution_dependencies: Vec<RemoteExecutorDependency>,
    /// RE custom tupperware image.
    remote_execution_custom_image: Option<RemoteExecutorCustomImage>,
    /// RE execution policy.
    meta_internal_extra_params: Arc<MetaInternalExtraParams>,
    /// Failed action outputs to materialize
    outputs_for_error_handler: Vec<BuildArtifactPath>,
    /// String representation of a key that uniquely identifies a RunAction
    run_action_key: Option<String>,

    is_test: bool,
    /// Whether to skip resource control (cgroup) for this command.
    /// Set for local resource setup commands whose backgrounded processes
    /// must survive after the setup script exits.
    skip_resource_control: bool,

    network_access: Option<NetworkAccess>,

    /// When set, the local executor runs this action without network isolation,
    /// ignoring the inherited `network_access` policy; no effect on RE. Set by the test
    /// orchestrator's `disable_local_network_isolation`, which explains the rationale.
    disable_local_network_isolation: bool,
}

impl CommandExecutionRequest {
    pub fn new(
        exe: Vec<String>,
        args: Vec<String>,
        paths: CommandExecutionPaths,
        env: SortedVectorMap<String, String>,
    ) -> Self {
        Self {
            exe,
            args,
            paths,
            env,
            timeout: None,
            executor_preference: ExecutorPreference::Default,
            host_sharing_requirements: Arc::new(HostSharingRequirements::default()),
            low_pass_filter: true,
            working_directory: ProjectRelativePathBuf::default(),
            prefetch_lossy_stderr: false,
            outputs_cleanup: true,
            local_environment_inheritance: None,
            force_full_hybrid_if_capable: false,
            disable_miniperf: false,
            required_local_resources: SortedSet::new(),
            worker: None,
            remote_worker: None,
            unique_input_inodes: false,
            remote_dep_file_key: None,
            re_gang_workers: Vec::new(),
            remote_execution_dependencies: Vec::new(),
            remote_execution_custom_image: None,
            meta_internal_extra_params: MetaInternalExtraParams::default_arc(),
            outputs_for_error_handler: Vec::new(),
            run_action_key: None,
            is_test: false,
            skip_resource_control: false,
            network_access: None,
            disable_local_network_isolation: false,
        }
    }

    pub fn paths(&self) -> &CommandExecutionPaths {
        &self.paths
    }

    pub fn with_outputs_paths_added_as_inputs(
        self,
        output_paths: impl IntoIterator<Item = CommandExecutionInput>,
        fs: &ArtifactFs,
        digest_config: DigestConfig,
        interner: Option<&DashMapDirectoryInterner<ActionDirectoryMember, TrackedFileDigest>>,
    ) -> buck2_error::Result<Self> {
        let override_paths =
            self.paths
                .add_outputs_as_inputs(output_paths, fs, digest_config, interner)?;
        Ok(Self {
            paths: override_paths,
            ..self
        })
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_executor_preference(mut self, executor_preference: ExecutorPreference) -> Self {
        self.executor_preference = executor_preference;
        self
    }

    pub fn with_host_sharing_requirements(
        mut self,
        host_sharing_requirements: Arc<HostSharingRequirements>,
    ) -> Self {
        self.host_sharing_requirements = host_sharing_requirements;
        self
    }

    pub fn with_low_pass_filter(mut self, low_pass_filter: bool) -> Self {
        self.low_pass_filter = low_pass_filter;
        self
    }

    pub fn with_working_directory(mut self, working_directory: ProjectRelativePathBuf) -> Self {
        self.working_directory = working_directory;
        self
    }

    pub fn with_prefetch_lossy_stderr(mut self, prefetch_lossy_stderr: bool) -> Self {
        self.prefetch_lossy_stderr = prefetch_lossy_stderr;
        self
    }

    pub fn with_outputs_cleanup(mut self, outputs_cleanup: bool) -> Self {
        self.outputs_cleanup = outputs_cleanup;
        self
    }

    pub fn with_remote_dep_file_key(mut self, remote_dep_file_key: &DepFileDigest) -> Self {
        self.remote_dep_file_key = Some(remote_dep_file_key.dupe());
        self
    }

    pub fn remote_dep_file_key(&self) -> &Option<DepFileDigest> {
        &self.remote_dep_file_key
    }

    pub fn prefetch_lossy_stderr(&self) -> bool {
        self.prefetch_lossy_stderr
    }

    pub fn outputs_cleanup(&self) -> bool {
        self.outputs_cleanup
    }

    pub fn all_args(&self) -> impl Iterator<Item = &String> {
        self.exe.iter().chain(self.args.iter())
    }

    pub fn all_args_vec(&self) -> Vec<String> {
        self.all_args().cloned().collect()
    }

    pub fn all_args_str(&self) -> String {
        self.all_args().join(" ")
    }

    pub fn exe(&self) -> &[String] {
        &self.exe
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }

    pub fn worker(&self) -> &Option<WorkerSpec> {
        &self.worker
    }

    pub fn remote_worker(&self) -> &Option<RemoteWorkerSpec> {
        &self.remote_worker
    }

    pub fn with_worker(mut self, worker: Option<WorkerSpec>) -> Self {
        self.worker = worker;
        self
    }

    pub fn with_remote_worker(mut self, remote_worker: Option<RemoteWorkerSpec>) -> Self {
        self.remote_worker = remote_worker;
        self
    }

    pub fn inputs(&self) -> &[CommandExecutionInput] {
        &self.paths.inputs
    }

    pub fn outputs(&self) -> impl Iterator<Item = CommandExecutionOutputRef<'_>> + '_ {
        self.paths.outputs.iter().map(|output| output.as_ref())
    }

    pub fn env(&self) -> &SortedVectorMap<String, String> {
        &self.env
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn executor_preference(&self) -> ExecutorPreference {
        self.executor_preference
    }

    pub fn host_sharing_requirements(&self) -> &HostSharingRequirements {
        &self.host_sharing_requirements
    }

    pub fn low_pass_filter(&self) -> bool {
        self.low_pass_filter
    }

    pub fn working_directory(&self) -> &ProjectRelativePath {
        &self.working_directory
    }

    pub fn with_local_environment_inheritance(
        mut self,
        local_environment_inheritance: EnvironmentInheritance,
    ) -> Self {
        self.local_environment_inheritance = Some(local_environment_inheritance);
        self
    }

    pub fn local_environment_inheritance(&self) -> Option<&EnvironmentInheritance> {
        self.local_environment_inheritance.as_ref()
    }

    /// Keeps invocation identity in daemon-local keys while stable keys rely on cache eligibility.
    pub fn local_reuse_keys(
        &self,
        digest_config: DigestConfig,
        fs: &ArtifactFs,
        local_execution_identity: &LocalExecutionIdentity,
        prepared_network_access: Option<NetworkAccess>,
        persistent_eligibility: PersistentLocalReuseEligibility,
    ) -> buck2_error::Result<LocalReuseKeys> {
        let restricted_network = match prepared_network_access {
            None | Some(NetworkAccess::All) => false,
            Some(
                NetworkAccess::None
                | NetworkAccess::Loopback
                | NetworkAccess::Strict
                | NetworkAccess::Private,
            ) => !self.disable_local_network_isolation,
        };
        let persistent = persistent_eligibility == PersistentLocalReuseEligibility::Eligible
            && self.outputs_cleanup
            && self.worker.is_none()
            && self.remote_worker.is_none()
            && self.required_local_resources.is_empty()
            && !self.executor_preference.requires_remote()
            && !restricted_network;

        let mut digester = CasDigestData::digester(digest_config.cas_digest_config());
        digester.update(b"local-reuse-context-v1");
        digester.update(&[u8::from(self.unique_input_inodes)]);

        for input in self.inputs() {
            match input {
                CommandExecutionInput::ActionMetadata(metadata) => {
                    digester.update(&[1]);
                    let path = metadata.resolve_path(fs)?;
                    digester.update_length_prefixed(path.as_str().as_bytes());
                    digester.update(&[metadata.digest.raw_digest().algorithm() as u8]);
                    digester.update_length_prefixed(metadata.digest.raw_digest().as_bytes());
                    digester.update(&metadata.digest.size().to_le_bytes());
                }
                CommandExecutionInput::ScratchPath(scratch_path) => {
                    digester.update(&[2]);
                    let scratch_path = fs.buck_out_path_resolver().resolve_scratch(scratch_path)?;
                    digester.update_length_prefixed(scratch_path.as_str().as_bytes());
                }
                CommandExecutionInput::Artifact(..)
                | CommandExecutionInput::IncrementalRemoteOutput(..) => {}
            }
        }

        match self.timeout {
            Some(timeout) => {
                digester.update(&[1]);
                digester.update(&timeout.as_secs().to_le_bytes());
                digester.update(&timeout.subsec_nanos().to_le_bytes());
            }
            None => digester.update(&[0]),
        }
        let working_directory = fs.fs().resolve(self.working_directory());
        Ok(self.local_reuse_keys_from_environment(
            std::env::vars_os().collect(),
            local_execution_identity,
            &working_directory,
            digester.finalize(),
            digest_config,
            if persistent {
                PersistentLocalReuseEligibility::Eligible
            } else {
                PersistentLocalReuseEligibility::Ineligible
            },
        ))
    }

    pub fn with_force_full_hybrid_if_capable(mut self, force_full_hybrid_if_capable: bool) -> Self {
        self.force_full_hybrid_if_capable = force_full_hybrid_if_capable;
        self
    }

    pub fn force_full_hybrid_if_capable(&self) -> bool {
        self.force_full_hybrid_if_capable
    }

    pub fn with_disable_miniperf(mut self, disable_miniperf: bool) -> Self {
        self.disable_miniperf = disable_miniperf;
        self
    }

    pub fn disable_miniperf(&self) -> bool {
        self.disable_miniperf
    }

    pub fn with_required_local_resources(
        mut self,
        required_local_resources: Vec<LocalResourceState>,
    ) -> buck2_error::Result<Self> {
        let original_len = required_local_resources.len();
        self.required_local_resources = required_local_resources.into_iter().collect();
        if self.required_local_resources.len() != original_len {
            return Err(buck2_error!(
                buck2_error::ErrorTag::Tier0,
                "Each provided local resource state is supposed to come from a different target."
            ));
        }
        Ok(self)
    }

    pub fn required_local_resources(&self) -> &SortedSet<LocalResourceState> {
        &self.required_local_resources
    }

    pub fn with_unique_input_inodes(mut self, unique_input_inodes: bool) -> Self {
        self.unique_input_inodes = unique_input_inodes;
        self
    }

    pub fn unique_input_inodes(&self) -> bool {
        self.unique_input_inodes
    }

    pub fn with_re_gang_workers(mut self, re_gang_workers: Vec<ReGangWorker>) -> Self {
        self.re_gang_workers = re_gang_workers;
        self
    }

    pub fn re_gang_workers(&self) -> &Vec<ReGangWorker> {
        &self.re_gang_workers
    }

    pub fn with_remote_execution_dependencies(
        mut self,
        remote_execution_dependencies: Vec<RemoteExecutorDependency>,
    ) -> Self {
        self.remote_execution_dependencies = remote_execution_dependencies;
        self
    }

    pub fn remote_execution_dependencies(&self) -> &Vec<RemoteExecutorDependency> {
        &self.remote_execution_dependencies
    }

    pub fn with_outputs_for_error_handler(
        mut self,
        outputs_for_error_handler: Vec<BuildArtifactPath>,
    ) -> Self {
        self.outputs_for_error_handler = outputs_for_error_handler;
        self
    }

    pub fn outputs_for_error_handler(&self) -> &Vec<BuildArtifactPath> {
        &self.outputs_for_error_handler
    }

    pub fn with_remote_execution_custom_image(
        mut self,
        remote_execution_custom_image: Option<RemoteExecutorCustomImage>,
    ) -> Self {
        self.remote_execution_custom_image = remote_execution_custom_image;
        self
    }

    pub fn remote_execution_custom_image(&self) -> &Option<RemoteExecutorCustomImage> {
        &self.remote_execution_custom_image
    }

    pub fn with_meta_internal_extra_params(
        mut self,
        meta_internal_extra_params: Arc<MetaInternalExtraParams>,
    ) -> Self {
        self.meta_internal_extra_params = meta_internal_extra_params;
        self
    }

    pub fn meta_internal_extra_params(&self) -> &MetaInternalExtraParams {
        &self.meta_internal_extra_params
    }

    pub fn with_run_action_key(mut self, run_action_key: Option<String>) -> Self {
        self.run_action_key = run_action_key;
        self
    }

    pub fn run_action_key(&self) -> &Option<String> {
        &self.run_action_key
    }

    pub fn with_is_test(mut self) -> Self {
        self.is_test = true;
        self
    }

    pub fn is_test(&self) -> bool {
        self.is_test
    }

    pub fn with_skip_resource_control(mut self) -> Self {
        self.skip_resource_control = true;
        self
    }

    pub fn skip_resource_control(&self) -> bool {
        self.skip_resource_control
    }

    pub fn with_network_access(mut self, v: Option<NetworkAccess>) -> Self {
        self.network_access = v;
        self
    }

    pub fn network_access(&self) -> Option<NetworkAccess> {
        self.network_access
    }

    pub fn with_disable_local_network_isolation(mut self, v: bool) -> Self {
        self.disable_local_network_isolation = v;
        self
    }

    pub fn disable_local_network_isolation(&self) -> bool {
        self.disable_local_network_isolation
    }

    fn local_reuse_keys_from_environment(
        &self,
        mut environment: BTreeMap<OsString, OsString>,
        local_execution_identity: &LocalExecutionIdentity,
        working_directory: &AbsPath,
        local_reuse_context: CasDigestData,
        digest_config: DigestConfig,
        persistent_eligibility: PersistentLocalReuseEligibility,
    ) -> LocalReuseKeys {
        visit_local_execution_environment(
            working_directory,
            self.env.iter(),
            self.local_environment_inheritance(),
            |directive| match directive {
                LocalEnvironmentDirective::Clear => environment.clear(),
                LocalEnvironmentDirective::Remove(key) => {
                    environment.remove(key);
                }
                LocalEnvironmentDirective::Set { key, value } => {
                    environment.insert(key.to_owned(), value.to_owned());
                }
            },
        );
        let digest_environment = |domain: &[u8], environment: &BTreeMap<OsString, OsString>| {
            let mut digester = CasDigestData::digester(digest_config.cas_digest_config());
            digester.update(domain);
            digester.update(&[local_reuse_context.raw_digest().algorithm() as u8]);
            digester.update_length_prefixed(local_reuse_context.raw_digest().as_bytes());
            digester.update(&local_reuse_context.size().to_le_bytes());
            digester.update(&(environment.len() as u64).to_le_bytes());
            for (key, value) in environment {
                digester.update_length_prefixed(key.as_os_str().as_encoded_bytes());
                digester.update_length_prefixed(value.as_os_str().as_encoded_bytes());
            }
            digester.finalize()
        };
        let persistent = match persistent_eligibility {
            PersistentLocalReuseEligibility::Eligible => Some(PersistentLocalReuseKey(
                digest_environment(b"persistent-local-reuse-key-v1", &environment),
            )),
            PersistentLocalReuseEligibility::Ineligible => None,
        };
        for (key, value) in local_execution_identity {
            environment.insert(key.into(), value.into());
        }
        LocalReuseKeys {
            in_daemon: InDaemonLocalReuseKey(digest_environment(
                b"in-daemon-local-reuse-key-v1",
                &environment,
            )),
            persistent,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::path::Path;

    use buck2_common::file_ops::metadata::TrackedFileDigest;
    use buck2_common::local_resource_state::LocalResourceState;
    use buck2_core::category::CategoryRef;
    use buck2_core::cells::CellResolver;
    use buck2_core::cells::cell_root_path::CellRootPathBuf;
    use buck2_core::cells::name::CellName;
    use buck2_core::configuration::data::ConfigurationData;
    use buck2_core::content_hash::ContentBasedPathHash;
    use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
    use buck2_core::fs::artifact_path_resolver::ArtifactFs;
    use buck2_core::fs::buck_out_path::BuckOutPathKind;
    use buck2_core::fs::buck_out_path::BuckOutPathResolver;
    use buck2_core::fs::buck_out_path::BuckOutScratchPath;
    use buck2_core::fs::buck_out_path::BuildArtifactPath;
    use buck2_core::fs::project::ProjectRoot;
    use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
    use buck2_core::target::configured_target_label::ConfiguredTargetLabel;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use buck2_fs::paths::abs_path::AbsPath;
    use buck2_fs::paths::forward_rel_path::ForwardRelativePathBuf;
    use buck2_hash::BuckIndexSet;
    use dupe::Dupe;
    use sorted_vector_map::SortedVectorMap;

    use super::*;

    struct LocalReuseRequestFixture {
        fs: ArtifactFs,
        digest_config: DigestConfig,
        local_execution_identity: LocalExecutionIdentity,
    }

    #[derive(Clone, Copy)]
    enum ScratchPathIdentity {
        ActionKey,
        ContentHash,
    }

    #[derive(Clone, Copy)]
    struct LocalReuseRequestConfig<'a> {
        metadata: &'a [u8],
        metadata_path: &'a str,
        scratch_identifier: &'a str,
        scratch_action_key: &'a str,
        scratch_path_identity: ScratchPathIdentity,
    }

    impl LocalReuseRequestFixture {
        fn new() -> Self {
            let fs = ArtifactFs::new(
                CellResolver::testing_with_name_and_path(
                    CellName::testing_new("root"),
                    CellRootPathBuf::testing_new(""),
                ),
                BuckOutPathResolver::new(ProjectRelativePathBuf::unchecked_new(
                    "buck-out/v2".into(),
                )),
                ProjectRoot::new_unchecked(
                    AbsNormPathBuf::new(
                        Path::new(if cfg!(windows) {
                            "C:\\local-reuse-test"
                        } else {
                            "/local-reuse-test"
                        })
                        .to_owned(),
                    )
                    .unwrap(),
                ),
            );
            Self {
                fs,
                digest_config: DigestConfig::testing_default(),
                local_execution_identity: LocalExecutionIdentity {
                    buck_build_id: BuckBuildId("build-id".to_owned()),
                    buck2_daemon_uuid: Buck2DaemonUuid("daemon-id".to_owned()),
                },
            }
        }

        fn request(&self, config: LocalReuseRequestConfig<'_>) -> CommandExecutionRequest {
            let LocalReuseRequestConfig {
                metadata,
                metadata_path,
                scratch_identifier,
                scratch_action_key,
                scratch_path_identity,
            } = config;
            let owner = BaseDeferredKey::TargetLabel(ConfiguredTargetLabel::testing_parse(
                "root//pkg:target",
                ConfigurationData::testing_new(),
            ));
            let metadata_digest =
                TrackedFileDigest::from_content(metadata, self.digest_config.cas_digest_config());
            let paths = CommandExecutionPaths::new(
                vec![
                    CommandExecutionInput::ActionMetadata(ActionMetadataBlob {
                        digest: metadata_digest.dupe(),
                        path: BuildArtifactPath::new(
                            owner.dupe(),
                            ForwardRelativePathBuf::unchecked_new(metadata_path.into()),
                            BuckOutPathKind::Configuration,
                        ),
                        content_hash: ContentBasedPathHash::new(
                            metadata_digest.raw_digest().as_bytes(),
                        )
                        .unwrap(),
                    }),
                    CommandExecutionInput::ScratchPath(
                        BuckOutScratchPath::new(
                            owner,
                            CategoryRef::new("local_reuse").unwrap(),
                            Some(scratch_identifier),
                            ForwardRelativePathBuf::unchecked_new(scratch_action_key.into()),
                            match scratch_path_identity {
                                ScratchPathIdentity::ActionKey => false,
                                ScratchPathIdentity::ContentHash => true,
                            },
                        )
                        .unwrap(),
                    ),
                ],
                BuckIndexSet::new(),
                &self.fs,
                self.digest_config,
                None,
            )
            .unwrap();
            CommandExecutionRequest::new(Vec::new(), Vec::new(), paths, SortedVectorMap::new())
        }
    }

    #[test]
    fn local_reuse_key_tracks_request_context_and_gates_unsafe_requests() {
        let value = u64::MAX;
        let next_value = value.wrapping_add(1);
        let fixture = LocalReuseRequestFixture::new();
        let metadata = value.to_le_bytes();
        let baseline_config = LocalReuseRequestConfig {
            metadata: &metadata,
            metadata_path: "metadata.json",
            scratch_identifier: "scratch-id",
            scratch_action_key: "scratch",
            scratch_path_identity: ScratchPathIdentity::ActionKey,
        };
        let baseline = fixture.request(baseline_config);
        let baseline_key = baseline
            .local_reuse_keys(
                fixture.digest_config,
                &fixture.fs,
                &fixture.local_execution_identity,
                None,
                PersistentLocalReuseEligibility::Eligible,
            )
            .unwrap();
        let ineligible_key = baseline
            .local_reuse_keys(
                fixture.digest_config,
                &fixture.fs,
                &fixture.local_execution_identity,
                None,
                PersistentLocalReuseEligibility::Ineligible,
            )
            .unwrap();
        assert!(ineligible_key.persistent.is_none());
        assert!(ineligible_key.in_daemon == baseline_key.in_daemon);

        assert!(
            baseline_key
                == fixture
                    .request(baseline_config)
                    .with_executor_preference(ExecutorPreference::LocalRequired)
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                == fixture
                    .request(baseline_config)
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        Some(NetworkAccess::All),
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(LocalReuseRequestConfig {
                        metadata: &next_value.to_le_bytes(),
                        ..baseline_config
                    })
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(LocalReuseRequestConfig {
                        metadata_path: "other-metadata.json",
                        ..baseline_config
                    })
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(LocalReuseRequestConfig {
                        scratch_identifier: "other-scratch-id",
                        ..baseline_config
                    })
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(LocalReuseRequestConfig {
                        scratch_action_key: "other-scratch",
                        ..baseline_config
                    })
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(LocalReuseRequestConfig {
                        scratch_path_identity: ScratchPathIdentity::ContentHash,
                        ..baseline_config
                    })
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(baseline_config)
                    .with_timeout(Duration::from_secs(value % 60 + 1))
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(baseline_config)
                    .with_working_directory(ProjectRelativePathBuf::unchecked_new("subdir".into()))
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
        assert!(
            baseline_key
                != fixture
                    .request(baseline_config)
                    .with_unique_input_inodes(true)
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        None,
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );

        let excluded = fixture
            .request(baseline_config)
            .with_outputs_cleanup(false)
            .local_reuse_keys(
                fixture.digest_config,
                &fixture.fs,
                &fixture.local_execution_identity,
                None,
                PersistentLocalReuseEligibility::Eligible,
            )
            .unwrap();
        assert!(excluded.persistent.is_none());
        assert!(
            fixture
                .request(baseline_config)
                .with_worker(Some(WorkerSpec {
                    id: WorkerId(1),
                    exe: Vec::new(),
                    env: SortedVectorMap::new(),
                    concurrency: None,
                    streaming: false,
                    remote_key: None,
                    input_paths: CommandExecutionPaths::new(
                        Vec::new(),
                        BuckIndexSet::new(),
                        &fixture.fs,
                        fixture.digest_config,
                        None,
                    )
                    .unwrap(),
                }))
                .local_reuse_keys(
                    fixture.digest_config,
                    &fixture.fs,
                    &fixture.local_execution_identity,
                    None,
                    PersistentLocalReuseEligibility::Eligible
                )
                .unwrap()
                .persistent
                .is_none()
        );
        assert!(
            fixture
                .request(baseline_config)
                .with_remote_worker(Some(RemoteWorkerSpec {
                    id: WorkerId(1),
                    init: Vec::new(),
                    env: SortedVectorMap::new(),
                    input_paths: CommandExecutionPaths::new(
                        Vec::new(),
                        BuckIndexSet::new(),
                        &fixture.fs,
                        fixture.digest_config,
                        None,
                    )
                    .unwrap(),
                    concurrency: None,
                }))
                .local_reuse_keys(
                    fixture.digest_config,
                    &fixture.fs,
                    &fixture.local_execution_identity,
                    None,
                    PersistentLocalReuseEligibility::Eligible
                )
                .unwrap()
                .persistent
                .is_none()
        );
        assert!(
            fixture
                .request(baseline_config)
                .with_required_local_resources(vec![LocalResourceState::new(
                    ConfiguredTargetLabel::testing_parse(
                        "root//pkg:resource",
                        ConfigurationData::testing_new(),
                    ),
                    None,
                    Vec::new(),
                )])
                .unwrap()
                .local_reuse_keys(
                    fixture.digest_config,
                    &fixture.fs,
                    &fixture.local_execution_identity,
                    None,
                    PersistentLocalReuseEligibility::Eligible
                )
                .unwrap()
                .persistent
                .is_none()
        );
        assert!(
            fixture
                .request(baseline_config)
                .with_executor_preference(ExecutorPreference::RemoteRequired)
                .local_reuse_keys(
                    fixture.digest_config,
                    &fixture.fs,
                    &fixture.local_execution_identity,
                    None,
                    PersistentLocalReuseEligibility::Eligible
                )
                .unwrap()
                .persistent
                .is_none()
        );

        for network_access in [
            NetworkAccess::None,
            NetworkAccess::Loopback,
            NetworkAccess::Strict,
            NetworkAccess::Private,
        ] {
            assert!(
                fixture
                    .request(baseline_config)
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        Some(network_access),
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
                    .persistent
                    .is_none()
            );
        }
        assert!(
            baseline_key
                == fixture
                    .request(baseline_config)
                    .with_disable_local_network_isolation(true)
                    .local_reuse_keys(
                        fixture.digest_config,
                        &fixture.fs,
                        &fixture.local_execution_identity,
                        Some(NetworkAccess::Private),
                        PersistentLocalReuseEligibility::Eligible
                    )
                    .unwrap()
        );
    }

    #[test]
    fn local_reuse_key_tracks_effective_environment() {
        let value = u64::MAX;
        let next_value = value.wrapping_add(1);
        let fixture = LocalReuseRequestFixture::new();
        let digest_config = fixture.digest_config;
        let working_directory_path = std::env::temp_dir().join("persistent-action-reuse-context");
        let working_directory = AbsPath::new(&working_directory_path).unwrap();
        let request_environment =
            SortedVectorMap::from_iter([("EXPLICIT".to_owned(), format!("explicit-{value}"))]);
        let metadata = value.to_le_bytes();
        let mut request = fixture.request(LocalReuseRequestConfig {
            metadata: &metadata,
            metadata_path: "metadata.json",
            scratch_identifier: "scratch-id",
            scratch_action_key: "scratch",
            scratch_path_identity: ScratchPathIdentity::ActionKey,
        });
        request.env = request_environment;
        request.local_environment_inheritance =
            Some(EnvironmentInheritance::local_command_exclusions());
        let environment: BTreeMap<OsString, OsString> = [
            ("PATH", format!("path-{value}")),
            ("PYTHONPATH", format!("ignored-{value}")),
            ("INHERITED", format!("inherited-{value}")),
            (BUCK_BUILD_ID, "shadowed-build-id".to_owned()),
            (BUCK2_DAEMON_UUID, "shadowed-daemon-id".to_owned()),
        ]
        .into_iter()
        .map(|(key, value)| (OsString::from(key), OsString::from(value)))
        .collect();
        let mut local_reuse_context = CasDigestData::digester(digest_config.cas_digest_config());
        local_reuse_context.update(&value.to_le_bytes());
        let local_reuse_context = local_reuse_context.finalize();
        let local_execution_identity = LocalExecutionIdentity {
            buck_build_id: BuckBuildId(format!("build-{value}")),
            buck2_daemon_uuid: Buck2DaemonUuid(format!("daemon-{value}")),
        };

        let context = request.local_reuse_keys_from_environment(
            environment.clone(),
            &local_execution_identity,
            working_directory,
            local_reuse_context,
            digest_config,
            PersistentLocalReuseEligibility::Eligible,
        );

        let mut changed_inherited = environment.clone();
        changed_inherited.insert(
            OsString::from("INHERITED"),
            OsString::from(format!("inherited-{next_value}")),
        );
        assert!(
            context
                != request.local_reuse_keys_from_environment(
                    changed_inherited,
                    &local_execution_identity,
                    working_directory,
                    local_reuse_context,
                    digest_config,
                    PersistentLocalReuseEligibility::Eligible,
                )
        );

        let mut changed_excluded = environment.clone();
        changed_excluded.insert(
            OsString::from("PYTHONPATH"),
            OsString::from(format!("ignored-{next_value}")),
        );
        assert!(
            context
                == request.local_reuse_keys_from_environment(
                    changed_excluded,
                    &local_execution_identity,
                    working_directory,
                    local_reuse_context,
                    digest_config,
                    PersistentLocalReuseEligibility::Eligible,
                )
        );

        let changed_local_execution_identity = LocalExecutionIdentity {
            buck_build_id: BuckBuildId(format!("build-{next_value}")),
            buck2_daemon_uuid: Buck2DaemonUuid(format!("daemon-{next_value}")),
        };
        let changed_identity_context = request.local_reuse_keys_from_environment(
            environment.clone(),
            &changed_local_execution_identity,
            working_directory,
            local_reuse_context,
            digest_config,
            PersistentLocalReuseEligibility::Eligible,
        );
        assert!(context.in_daemon != changed_identity_context.in_daemon);
        assert!(context.persistent == changed_identity_context.persistent);

        let changed_request_environment =
            SortedVectorMap::from_iter([("EXPLICIT".to_owned(), format!("explicit-{next_value}"))]);
        let mut changed_request = fixture.request(LocalReuseRequestConfig {
            metadata: &metadata,
            metadata_path: "metadata.json",
            scratch_identifier: "scratch-id",
            scratch_action_key: "scratch",
            scratch_path_identity: ScratchPathIdentity::ActionKey,
        });
        changed_request.env = changed_request_environment;
        changed_request.local_environment_inheritance =
            Some(EnvironmentInheritance::local_command_exclusions());
        assert!(
            context
                != changed_request.local_reuse_keys_from_environment(
                    environment.clone(),
                    &local_execution_identity,
                    working_directory,
                    local_reuse_context,
                    digest_config,
                    PersistentLocalReuseEligibility::Eligible,
                )
        );

        let mut changed_local_reuse_context =
            CasDigestData::digester(digest_config.cas_digest_config());
        changed_local_reuse_context.update(&next_value.to_le_bytes());
        let changed_local_reuse_context = changed_local_reuse_context.finalize();
        assert!(
            context
                != request.local_reuse_keys_from_environment(
                    environment.clone(),
                    &local_execution_identity,
                    working_directory,
                    changed_local_reuse_context,
                    digest_config,
                    PersistentLocalReuseEligibility::Eligible,
                )
        );

        let other_working_directory_path =
            std::env::temp_dir().join("persistent-action-reuse-context-other");
        let other_working_directory = AbsPath::new(&other_working_directory_path).unwrap();
        assert!(
            context
                != request.local_reuse_keys_from_environment(
                    environment,
                    &local_execution_identity,
                    other_working_directory,
                    local_reuse_context,
                    digest_config,
                    PersistentLocalReuseEligibility::Eligible,
                )
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_reuse_key_tracks_non_unicode_environment() {
        use std::os::unix::ffi::OsStringExt;

        let fixture = LocalReuseRequestFixture::new();
        let metadata = [0];
        let request = fixture
            .request(LocalReuseRequestConfig {
                metadata: &metadata,
                metadata_path: "metadata.json",
                scratch_identifier: "scratch-id",
                scratch_action_key: "scratch",
                scratch_path_identity: ScratchPathIdentity::ActionKey,
            })
            .with_local_environment_inheritance(EnvironmentInheritance::local_command_exclusions());
        let working_directory_path = std::env::temp_dir();
        let working_directory = AbsPath::new(&working_directory_path).unwrap();
        let environment = BTreeMap::from([(
            OsString::from("NON_UNICODE"),
            OsString::from_vec(vec![0xff]),
        )]);
        let local_execution_identity = LocalExecutionIdentity {
            buck_build_id: BuckBuildId("build-id".to_owned()),
            buck2_daemon_uuid: Buck2DaemonUuid("daemon-id".to_owned()),
        };
        let key = request.local_reuse_keys_from_environment(
            environment,
            &local_execution_identity,
            working_directory,
            CasDigestData::digester(DigestConfig::testing_default().cas_digest_config()).finalize(),
            DigestConfig::testing_default(),
            PersistentLocalReuseEligibility::Eligible,
        );
        let unicode_key = request.local_reuse_keys_from_environment(
            BTreeMap::from([(OsString::from("NON_UNICODE"), OsString::from("different"))]),
            &local_execution_identity,
            working_directory,
            CasDigestData::digester(DigestConfig::testing_default().cas_digest_config()).finalize(),
            DigestConfig::testing_default(),
            PersistentLocalReuseEligibility::Eligible,
        );
        assert!(key != unicode_key);
    }
}

/// Is an output a file or a directory
#[derive(
    PartialEq,
    Eq,
    Hash,
    Debug,
    Copy,
    Clone,
    Dupe,
    Allocative,
    Ord,
    PartialOrd,
    strong_hash::StrongHash,
    Pagable
)]
pub enum OutputType {
    /// We don't know - used to represent legacy code that doesn't yet declare the output type properly.
    /// We aim to mostly remove this alternative over time.
    FileOrDirectory,
    File,
    Directory,
}

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Input)]
enum OutputTypeError {
    #[error("Expected {1:?}, but `{0}` is already declared as {2:?}")]
    CheckPath(String, OutputType, OutputType),
}

impl OutputType {
    /// We are hoping for something of output type self, and got a path of an existing declared type.
    /// Raise an error if they are incompatible.
    pub fn check_path(
        self,
        path_for_error_message: impl Display,
        output_type: OutputType,
    ) -> buck2_error::Result<()> {
        if self == OutputType::Directory && output_type == OutputType::FileOrDirectory {
            // If we treat paths whose declared type is FileOrDirectory like files, then that's incompatible with directory
            soft_error!(
                "declare_wrong_type",
                OutputTypeError::CheckPath(
                    path_for_error_message.to_string(),
                    self,
                    OutputType::File
                )
                .into(),
                deprecation: true,
                quiet: true,
                error_on_oss: true
            )?;
            Ok(())
        } else if self == output_type
            || self == OutputType::FileOrDirectory
            || output_type == OutputType::FileOrDirectory
        {
            Ok(())
        } else {
            Err(
                OutputTypeError::CheckPath(path_for_error_message.to_string(), self, output_type)
                    .into(),
            )
        }
    }
}

#[derive(UnpackVariants, PartialEq, Eq, Hash, Debug)]
pub enum CommandExecutionOutputRef<'a> {
    BuildArtifact {
        path: &'a BuildArtifactPath,
        output_type: OutputType,
    },
    TestPath {
        path: &'a BuckOutTestPath,
        create: OutputCreationBehavior,
    },
}

impl CommandExecutionOutputRef<'_> {
    /// Resolve this output to a ResolvedCommandExecutionOutput that allows access to the output
    /// path as well as any dirs to create.
    pub fn resolve(
        &self,
        fs: &ArtifactFs,
        content_hash: Option<&ContentBasedPathHash>,
    ) -> buck2_error::Result<ResolvedCommandExecutionOutput> {
        match self {
            Self::BuildArtifact { path, output_type } => Ok(ResolvedCommandExecutionOutput {
                path: fs.resolve_build(path, content_hash)?,
                create: OutputCreationBehavior::Parent,
                output_type: *output_type,
            }),
            Self::TestPath { path, create } => Ok(ResolvedCommandExecutionOutput {
                path: fs.buck_out_path_resolver().resolve_test(path),
                create: *create,
                output_type: OutputType::FileOrDirectory,
            }),
        }
    }

    /// Same as `resolve`, but the underlying output path that is returned uses the
    /// configuration hash regardless of whether the output is content-based or not.
    pub fn resolve_configuration_hash_path(
        &self,
        fs: &ArtifactFs,
    ) -> buck2_error::Result<ResolvedCommandExecutionOutput> {
        match self {
            Self::BuildArtifact { path, output_type } => Ok(ResolvedCommandExecutionOutput {
                path: fs.resolve_build_configuration_hash_path(path)?,
                create: OutputCreationBehavior::Parent,
                output_type: *output_type,
            }),
            Self::TestPath { path, create } => Ok(ResolvedCommandExecutionOutput {
                path: fs.buck_out_path_resolver().resolve_test(path),
                create: *create,
                output_type: OutputType::FileOrDirectory,
            }),
        }
    }

    pub fn cloned(&self) -> CommandExecutionOutput {
        match self {
            Self::BuildArtifact { path, output_type } => CommandExecutionOutput::BuildArtifact {
                path: (*path).dupe(),
                output_type: *output_type,
            },
            Self::TestPath { path, create } => CommandExecutionOutput::TestPath {
                path: (*path).clone(),
                create: *create,
            },
        }
    }

    pub fn has_content_based_path(&self) -> bool {
        match self {
            Self::BuildArtifact { path, .. } => path.is_content_based_path(),
            Self::TestPath { .. } => false,
        }
    }
}

#[derive(UnpackVariants, PartialEq, Eq, Hash, Debug)]
pub enum CommandExecutionOutput {
    BuildArtifact {
        path: BuildArtifactPath,
        output_type: OutputType,
    },
    TestPath {
        path: BuckOutTestPath,
        create: OutputCreationBehavior,
    },
}

impl CommandExecutionOutput {
    pub fn as_ref(&self) -> CommandExecutionOutputRef<'_> {
        match self {
            Self::BuildArtifact { path, output_type } => CommandExecutionOutputRef::BuildArtifact {
                path,
                output_type: *output_type,
            },
            Self::TestPath { path, create } => CommandExecutionOutputRef::TestPath {
                path,
                create: *create,
            },
        }
    }

    pub fn has_content_based_path(&self) -> bool {
        match self {
            Self::BuildArtifact { path, .. } => path.is_content_based_path(),
            Self::TestPath { .. } => false,
        }
    }
}

/// The path for an output, and what should be created before running the action that produces this
/// output.
pub struct ResolvedCommandExecutionOutput {
    pub path: ProjectRelativePathBuf,
    pub output_type: OutputType,
    create: OutputCreationBehavior,
}

impl ResolvedCommandExecutionOutput {
    /// Return this output's path.
    pub fn path(&self) -> &ProjectRelativePath {
        &self.path
    }

    /// Return this output's path, ownedly.
    pub fn into_path(self) -> ProjectRelativePathBuf {
        self.path
    }

    /// Return the path that this output requires creating before running the action.
    pub fn path_to_create(&self) -> Option<&ProjectRelativePath> {
        match self.create {
            OutputCreationBehavior::Create => Some(&self.path),
            OutputCreationBehavior::Parent => self.path.parent(),
        }
    }
}
