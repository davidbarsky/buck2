/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use buck2_common::file_ops::metadata::FileMetadata;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_error::internal_error;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::directory::ActionDirectoryBuilder;
use buck2_execute::directory::insert_file;
use buck2_execute::materialize::materializer::CleanStaleArtifactsArgs;
use buck2_execute::materialize::materializer::DeclareArtifactPayload;
use buck2_execute::materialize::materializer::DeferredMaterializerSubscription;
use buck2_execute::materialize::utils::dynamic_priority_handle::DynamicPriorityHandle;
use buck2_execute::materialize::utils::priority_semaphore::Priority;
use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
use buck2_hash::StdBuckHashMap;
use parking_lot::Mutex;

use super::*;
use crate::materializers::deferred::artifact_tree::MaterializingFuture;

#[test]
fn test_find_artifacts() -> buck2_error::Result<()> {
    let artifact1 = ProjectRelativePathBuf::unchecked_new("foo/bar/baz".to_owned());
    let artifact2 = ProjectRelativePathBuf::unchecked_new("foo/bar/bar/qux".to_owned());
    let artifact3 = ProjectRelativePathBuf::unchecked_new("foo/bar/bar/quux".to_owned());
    let artifact4 = ProjectRelativePathBuf::unchecked_new("foo/bar/qux/quuz".to_owned());
    let non_artifact1 = ProjectRelativePathBuf::unchecked_new("foo/bar/qux".to_owned());
    let non_artifact2 = ProjectRelativePathBuf::unchecked_new("foo/bar/bar/corge".to_owned());

    let file = FileMetadata::empty(DigestConfig::testing_default().cas_digest_config());

    // Build deps with artifacts 1-3, and non-artifacts 1-2
    let mut builder = ActionDirectoryBuilder::empty();
    insert_file(
        &mut builder,
        artifact1.join(ForwardRelativePath::new("f1").unwrap()),
        file.dupe(),
    )?;
    insert_file(
        &mut builder,
        artifact2.join(ForwardRelativePath::new("d/f1").unwrap()),
        file.dupe(),
    )?;
    insert_file(&mut builder, artifact3.clone(), file.dupe())?;
    insert_file(&mut builder, non_artifact2, file.dupe())?;
    builder.mkdir(&non_artifact1)?;

    // Build tree with artifacts 1-4
    let mut tree: FileTree<()> = FileTree::new();
    tree.insert(artifact1.iter().map(|f| f.to_owned()), ());
    tree.insert(artifact2.iter().map(|f| f.to_owned()), ());
    tree.insert(artifact3.iter().map(|f| f.to_owned()), ());
    tree.insert(artifact4.iter().map(|f| f.to_owned()), ());

    let expected_artifacts: StdBuckHashSet<_> =
        vec![artifact1, artifact2, artifact3].into_iter().collect();
    let found_artifacts: StdBuckHashSet<_> = tree.find_artifacts(&builder).into_iter().collect();
    assert_eq!(found_artifacts, expected_artifacts);
    Ok(())
}

#[test]
fn test_remove_path() {
    fn insert(tree: &mut FileTree<String>, path: &str) {
        tree.insert(
            ProjectRelativePath::unchecked_new(path)
                .iter()
                .map(|f| f.to_owned()),
            path.to_owned(),
        );
    }

    let mut tree: FileTree<String> = FileTree::new();
    insert(&mut tree, "a/b/c/d");
    insert(&mut tree, "a/b/c/e");
    insert(&mut tree, "a/c");

    let removed_subtree = tree.remove_path(ProjectRelativePath::unchecked_new("a/b"));
    // Convert to StdBuckHashMap<String, String> so it's easier to test
    let removed_subtree: StdBuckHashMap<String, String> = removed_subtree
        .map(|(k, v)| (k.as_str().to_owned(), v))
        .collect();

    assert_eq!(removed_subtree.len(), 2);
    assert_eq!(removed_subtree.get("a/b/c/d"), Some(&"a/b/c/d".to_owned()));
    assert_eq!(removed_subtree.get("a/b/c/e"), Some(&"a/b/c/e".to_owned()));
}

#[cfg(test)]
mod state_machine {
    use std::path::Path;
    use std::sync::Barrier;
    use std::thread;

    use assert_matches::assert_matches;
    use buck2_common::cas_digest::CasDigestData;
    use buck2_common::file_ops::metadata::FileDigest;
    use buck2_common::file_ops::metadata::Symlink;
    use buck2_common::file_ops::metadata::TrackedFileDigest;
    use buck2_core::content_hash::ContentBasedPathHash;
    use buck2_core::fs::project::ProjectRootTemp;
    use buck2_error::BuckErrorContext;
    use buck2_error::buck2_error;
    use buck2_events::daemon_id::DaemonId;
    use buck2_events::source::ChannelEventSource;
    use buck2_execute::artifact_value::ArtifactValueFingerprint;
    use buck2_execute::directory::ActionDirectoryEntry;
    use buck2_execute::directory::ActionSharedDirectory;
    use buck2_execute::directory::INTERNER;
    use buck2_execute::execute::action_digest::ActionDigest;
    use buck2_execute::execute::blocking::IoRequest;
    use buck2_execute::materialize::materializer::ActionReuseOutput;
    use buck2_execute::materialize::materializer::ActionReuseOutputs;
    use buck2_execute::materialize::materializer::ActionReuseWrite;
    use buck2_execute::materialize::materializer::IdenticalActionReuseKey;
    use buck2_fs::fs_util::ReadDir;
    use buck2_fs::fs_util::uncategorized as fs_util;
    use buck2_fs::paths::RelativePathBuf;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
    use buck2_fs::paths::forward_rel_path::ForwardRelativePathBuf;
    use buck2_util::threads::ignore_stack_overflow_checks_for_future;
    use buck2_wrapper_common::invocation_id::TraceId;
    use futures::StreamExt;
    use futures::future::BoxFuture;
    use futures::future::FutureExt;
    use tokio::runtime::Builder;
    use tokio::time::Duration as TokioDuration;
    use tokio::time::sleep;

    use super::*;
    use crate::materializers::deferred::clean_stale::CleanInvalidatedPathRequest;
    use crate::materializers::deferred::command_processor::TestingDeferredMaterializerCommandProcessor;
    use crate::materializers::deferred::subscriptions::MaterializerSubscriptionOperation;
    use crate::materializers::deferred::subscriptions::SubscriptionHandle;
    use crate::sqlite::materializer_db::testing_materializer_state_sqlite_db;

    #[derive(Clone, Copy)]
    enum MaterializerMode {
        DeferredWrites,
        EagerWrites,
    }

    #[derive(Debug, Eq, PartialEq, Allocative)]
    enum Op {
        Clean,
        Materialize,
        MaterializeError,
    }

    #[derive(Allocative)]
    struct StubIoHandler {
        log: Mutex<Vec<(Op, ProjectRelativePathBuf)>>,
        fail: Mutex<bool>,
        fail_paths: Mutex<Vec<ProjectRelativePathBuf>>,
        // If set, add a sleep when materializing to simulate a long materialization period
        materialization_config: StdBuckHashMap<ProjectRelativePathBuf, TokioDuration>,
        #[allocative(skip)]
        read_dir_barriers: Option<Arc<(Barrier, Barrier)>>,
        #[allocative(skip)]
        clean_barriers: Option<Arc<(Barrier, Barrier)>>,
        digest_config: DigestConfig,
        buck_out_path: ProjectRelativePathBuf,
        fs: ProjectRoot,
    }

    impl DeferredMaterializerAccessor<StubIoHandler> {
        // Ensure that the command thread ends so that the command processor is dropped,
        // and the sqlite connection is flushed and closed.
        // Needed since the default destructor assumes the process is about to die and shouldn't need to block.
        fn abort(mut self) {
            self.command_sender
                .send(MaterializerCommand::Abort)
                .unwrap();
            self.command_thread.take().unwrap().join().unwrap();
        }
    }

    impl StubIoHandler {
        fn take_log(&self) -> Vec<(Op, ProjectRelativePathBuf)> {
            std::mem::take(&mut *self.log.lock())
        }

        fn set_fail(&self, fail: bool) {
            *self.fail.lock() = fail;
        }

        fn set_fail_on(&self, paths: Vec<ProjectRelativePathBuf>) {
            *self.fail_paths.lock() = paths;
        }

        pub fn new(fs: ProjectRoot) -> Self {
            Self {
                log: Default::default(),
                fail: Default::default(),
                fail_paths: Default::default(),
                materialization_config: StdBuckHashMap::default(),
                read_dir_barriers: None,
                clean_barriers: None,
                digest_config: DigestConfig::testing_default(),
                buck_out_path: make_path("buck-out/v2"),
                fs,
            }
        }

        pub fn with_materialization_config(
            mut self,
            materialization_config: StdBuckHashMap<ProjectRelativePathBuf, TokioDuration>,
        ) -> Self {
            self.materialization_config = materialization_config;
            self
        }

        pub fn with_read_dir_barriers(
            mut self,
            read_dir_barriers: Arc<(Barrier, Barrier)>,
        ) -> Self {
            self.read_dir_barriers = Some(read_dir_barriers);
            self
        }

        pub fn with_clean_barriers(mut self, clean_barriers: Arc<(Barrier, Barrier)>) -> Self {
            self.clean_barriers = Some(clean_barriers);
            self
        }
    }

    impl StubIoHandler {
        fn actually_write(self: &Arc<Self>, path: &ProjectRelativePathBuf, write: &Arc<WriteFile>) {
            let data = zstd::bulk::decompress(&write.compressed_data, write.decompressed_size)
                .buck_error_context("Error decompressing data")
                .unwrap();
            self.fs.write_file(path, data, write.is_executable).unwrap();
        }
    }

    #[async_trait]
    impl IoHandler for StubIoHandler {
        fn write<'a>(
            self: &Arc<Self>,
            path: ProjectRelativePathBuf,
            write: Arc<WriteFile>,
            version: Version,
            command_sender: Arc<MaterializerSender<Self>>,
            _cancellations: &'a CancellationContext,
        ) -> BoxFuture<'a, Result<(), SharedMaterializingError>> {
            self.actually_write(&path, &write);
            async move {
                let _ = command_sender.send_low_priority(
                    LowPriorityMaterializerCommand::MaterializationFinished {
                        path,
                        timestamp: Utc::now(),
                        version,
                        result: Ok(()),
                    },
                );
                Ok(())
            }
            .boxed()
        }

        async fn immediate_write<'a>(
            self: &Arc<Self>,
            _gen: Box<dyn FnOnce() -> buck2_error::Result<Vec<WriteRequest>> + Send + 'a>,
        ) -> buck2_error::Result<Vec<ArtifactValue>> {
            unimplemented!()
        }

        fn clean_path<'a>(
            self: &Arc<Self>,
            path: ProjectRelativePathBuf,
            version: Version,
            command_sender: Arc<MaterializerSender<Self>>,
            _cancellations: &'a CancellationContext,
        ) -> BoxFuture<'a, Result<(), buck2_error::Error>> {
            self.log.lock().push((Op::Clean, path.clone()));

            async move {
                let _ = command_sender.send_low_priority(
                    LowPriorityMaterializerCommand::CleanupFinished {
                        path,
                        version,
                        result: Ok(()),
                    },
                );
                Ok(())
            }
            .boxed()
        }

        async fn clean_invalidated_path<'a>(
            self: &Arc<Self>,
            request: CleanInvalidatedPathRequest,
            _cancellations: &'a CancellationContext,
        ) -> buck2_error::Result<()> {
            if let Some(barriers) = self.clean_barriers.as_ref() {
                // Allow tests to advance here, execute something and then continue
                barriers.as_ref().0.wait();
                barriers.as_ref().1.wait();
            }
            Box::new(request).execute(&self.fs)
        }

        async fn materialize_entry(
            self: &Arc<Self>,
            path: ProjectRelativePathBuf,
            _method: Arc<ArtifactMaterializationMethod>,
            _entry: ActionDirectoryEntry<ActionSharedDirectory>,
            _priority_control: DynamicPriorityHandle,
            _event_dispatcher: EventDispatcher,
            _cancellations: &CancellationContext,
        ) -> Result<(), MaterializeEntryError> {
            // Simulate a non-immediate materialization if configured
            if let Some(duration) = self.materialization_config.get(&path) {
                sleep(*duration).await;
            }

            if (*self.fail_paths.lock()).contains(&path) || *self.fail.lock() {
                self.log.lock().push((Op::MaterializeError, path));
                Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::MaterializationError,
                    "Injected error"
                )
                .into())
            } else {
                if let ArtifactMaterializationMethod::Write(write) = _method.as_ref() {
                    self.actually_write(&path, write);
                }
                self.log.lock().push((Op::Materialize, path));
                Ok(())
            }
        }

        fn create_ttl_refresh(
            self: &Arc<Self>,
            _tree: &ArtifactTree,
            _min_ttl: Duration,
        ) -> Option<BoxFuture<'static, buck2_error::Result<()>>> {
            unimplemented!()
        }

        fn read_dir(&self, path: &AbsNormPathBuf) -> buck2_error::Result<ReadDir> {
            if let Some(barriers) = self.read_dir_barriers.as_ref() {
                // Allow tests to advance here, execute something and then continue
                barriers.as_ref().0.wait();
                barriers.as_ref().1.wait();
            }
            fs_util::read_dir(path)
        }

        fn buck_out_path(&self) -> &ProjectRelativePathBuf {
            &self.buck_out_path
        }

        fn re_client_manager(&self) -> &Arc<ReConnectionManager> {
            unimplemented!()
        }

        fn fs(&self) -> &ProjectRoot {
            &self.fs
        }

        fn digest_config(&self) -> DigestConfig {
            self.digest_config
        }
    }

    /// A stub command sender. We are calling materializer methods directly so that's all we need.
    fn channel() -> (
        Arc<MaterializerSender<StubIoHandler>>,
        MaterializerReceiver<StubIoHandler>,
    ) {
        // We don't use those counts in tests.
        static SENT: AtomicUsize = AtomicUsize::new(0);
        static RECEIVED: AtomicUsize = AtomicUsize::new(0);

        let (hi_send, hi_recv) = mpsc::unbounded_channel();
        let (lo_send, lo_recv) = mpsc::unbounded_channel();
        let counters = MaterializerCounters {
            sent: &SENT,
            received: &RECEIVED,
        };

        (
            Arc::new(MaterializerSender {
                high_priority: hi_send,
                low_priority: lo_send,
                counters,
                clean_guard: Default::default(),
            }),
            MaterializerReceiver {
                high_priority: hi_recv,
                low_priority: lo_recv,
                counters,
            },
        )
    }

    fn make_path(p: &str) -> ProjectRelativePathBuf {
        ProjectRelativePath::new(p).unwrap().to_owned()
    }

    fn temp_root() -> ProjectRoot {
        ProjectRootTemp::new().unwrap().path().clone()
    }

    async fn materialize_write(
        path: &ProjectRelativePathBuf,
        contents: &'static [u8],
        handle: &mut SubscriptionHandle<StubIoHandler>,
        dm: &DeferredMaterializerAccessor<StubIoHandler>,
    ) -> buck2_error::Result<()> {
        dm.declare_write(Box::new(|| {
            Ok(vec![WriteRequest {
                path: path.clone(),
                content: contents.to_vec(),
                is_executable: false,
                configuration_path: None,
            }])
        }))
        .await?;

        handle.subscribe_to_paths(vec![path.clone()]);

        dm.materialize_many(vec![path.clone()])
            .await?
            .next()
            .await
            .unwrap()?;
        // block until materialization_finished updates the tree
        handle.receiver().recv().await;
        Ok(())
    }

    fn make_db(fs: &ProjectRoot) -> (MaterializerStateSqliteDb, Option<MaterializerState>) {
        let (db, state) = testing_materializer_state_sqlite_db(
            fs,
            StdBuckHashMap::from([("version".to_owned(), "0".to_owned())]),
            StdBuckHashMap::default(),
            None,
        )
        .unwrap();
        (db, state.ok())
    }

    fn make_processor_for_io(
        io: Arc<StubIoHandler>,
    ) -> (
        DeferredMaterializerCommandProcessor<StubIoHandler>,
        Arc<MaterializerSender<StubIoHandler>>,
        MaterializerReceiver<StubIoHandler>,
        ChannelEventSource,
    ) {
        let (db, sqlite_state) = make_db(io.fs());
        let tree = ArtifactTree::initialize(sqlite_state);

        let (daemon_dispatcher_events, daemon_dispatcher_sink) =
            buck2_events::create_source_sink_pair();
        let daemon_dispatcher =
            EventDispatcher::new(TraceId::null(), DaemonId::new(), daemon_dispatcher_sink);

        let (command_sender, command_receiver) = channel();
        (
            DeferredMaterializerCommandProcessor::new(
                io,
                Some(db),
                Handle::current(),
                true,
                command_sender.dupe(),
                tree,
                CancellationContext::testing(),
                Arc::new(DeferredMaterializerStats::default()),
                Default::default(),
                true,
                daemon_dispatcher,
                true,
            ),
            command_sender,
            command_receiver,
            daemon_dispatcher_events,
        )
    }

    fn make_processor(
        materialization_config: StdBuckHashMap<ProjectRelativePathBuf, TokioDuration>,
    ) -> (
        DeferredMaterializerCommandProcessor<StubIoHandler>,
        MaterializerReceiver<StubIoHandler>,
    ) {
        let (dm, _, receiver, _) = make_processor_for_io(Arc::new(
            StubIoHandler::new(temp_root()).with_materialization_config(materialization_config),
        ));
        (dm, receiver)
    }

    #[test]
    fn match_artifacts_updates_present_generated_dependencies() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let _guard = runtime.enter();
        let (mut dm, _) = make_processor(Default::default());
        let digest_config = dm.io.digest_config();

        let mut first_deps = ActionDirectoryBuilder::empty();
        first_deps
            .insert(
                ForwardRelativePath::unchecked_new("first"),
                DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
                    digest: TrackedFileDigest::from_content(
                        b"first",
                        digest_config.cas_digest_config(),
                    ),
                    is_executable: false,
                })),
            )
            .unwrap();
        let first_deps = first_deps
            .fingerprint(digest_config.as_directory_serializer())
            .shared(&*INTERNER);
        let mut second_deps = ActionDirectoryBuilder::empty();
        second_deps
            .insert(
                ForwardRelativePath::unchecked_new("second"),
                DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
                    digest: TrackedFileDigest::from_content(
                        b"second",
                        digest_config.cas_digest_config(),
                    ),
                    is_executable: false,
                })),
            )
            .unwrap();
        let second_deps = second_deps
            .fingerprint(digest_config.as_directory_serializer())
            .shared(&*INTERNER);

        let entry = DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
            digest: TrackedFileDigest::from_content(b"entry", digest_config.cas_digest_config()),
            is_executable: false,
        }));
        let dependencies = [None, Some(first_deps), Some(second_deps)];
        let mut case = 0;
        for stored_first in &dependencies {
            for stored_second in &dependencies {
                for cached_first in &dependencies {
                    for cached_second in &dependencies {
                        let first_path = make_path(&format!("match/{case}/first"));
                        let second_path = make_path(&format!("match/{case}/second"));
                        dm.testing_declare_existing(
                            &first_path,
                            ArtifactValue::new(
                                entry.dupe(),
                                stored_first.as_ref().map(|deps| deps.dupe()),
                            ),
                        );
                        dm.testing_declare_existing(
                            &second_path,
                            ArtifactValue::new(
                                entry.dupe(),
                                stored_second.as_ref().map(|deps| deps.dupe()),
                            ),
                        );

                        let (sender, mut receiver) = oneshot::channel();
                        dm.testing_process_one_command(MaterializerCommand::MatchArtifacts(
                            vec![
                                (
                                    first_path.clone(),
                                    ArtifactValue::new(
                                        entry.dupe(),
                                        cached_first.as_ref().map(|deps| deps.dupe()),
                                    ),
                                ),
                                (
                                    second_path.clone(),
                                    ArtifactValue::new(
                                        entry.dupe(),
                                        cached_second.as_ref().map(|deps| deps.dupe()),
                                    ),
                                ),
                            ],
                            sender,
                        ));
                        assert!(receiver.try_recv().unwrap());

                        for (path, stored_deps, cached_deps) in [
                            (first_path, stored_first, cached_first),
                            (second_path, stored_second, cached_second),
                        ] {
                            let data = dm.tree.prefix_get(&mut path.iter()).unwrap();
                            let ArtifactMaterializationStage::Materialized { active, .. } =
                                &data.stage
                            else {
                                panic!("matched artifact was not materialized");
                            };
                            assert!(*active);
                            let expected_deps = cached_deps.as_ref().or(stored_deps.as_ref());
                            assert_eq!(
                                data.deps.as_ref().map(|deps| deps.fingerprint()),
                                expected_deps.map(|deps| deps.fingerprint()),
                            );
                        }
                        case += 1;
                    }
                }
            }
        }
        assert_eq!(case, 81);
    }

    #[test]
    fn match_artifacts_persists_preserved_artifact_value_fields() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let _guard = runtime.enter();
        let (mut dm, _) = make_processor(Default::default());
        let digest_config = dm.io.digest_config();
        let path = make_path("match/persisted");
        let entry = DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
            digest: TrackedFileDigest::from_content(b"entry", digest_config.cas_digest_config()),
            is_executable: false,
        }));
        let mut deps = ActionDirectoryBuilder::empty();
        deps.insert(
            ForwardRelativePath::unchecked_new("dependency"),
            DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
                digest: TrackedFileDigest::from_content(
                    b"dependency",
                    digest_config.cas_digest_config(),
                ),
                is_executable: false,
            })),
        )
        .unwrap();
        let deps = deps
            .fingerprint(digest_config.as_directory_serializer())
            .shared(&*INTERNER);
        dm.testing_declare_existing(
            &path,
            ArtifactValue::new(entry.dupe(), Some(deps.dupe()))
                .with_content_based_path_hash(ContentBasedPathHash::Scratch),
        );

        let (sender, mut receiver) = oneshot::channel();
        dm.testing_process_one_command(MaterializerCommand::MatchArtifacts(
            vec![(path.clone(), ArtifactValue::new(entry, None))],
            sender,
        ));
        assert!(receiver.try_recv().unwrap());

        let state = dm
            .sqlite_db
            .as_ref()
            .unwrap()
            .tables
            .domain_table
            .materializer_state
            .read_materializer_state(digest_config)
            .unwrap();
        let persisted = state.into_iter().find(|entry| entry.path == path).unwrap();
        assert_eq!(
            persisted.value.deps().map(|deps| deps.fingerprint()),
            Some(deps.fingerprint()),
        );
        assert_eq!(
            persisted.value.explicit_content_based_path_hash(),
            Some(&ContentBasedPathHash::Scratch),
        );
    }

    async fn make_materializer(
        io: Arc<StubIoHandler>,
        clean_stale_config: Option<CleanStaleConfig>,
        mode: MaterializerMode,
    ) -> (
        DeferredMaterializerAccessor<StubIoHandler>,
        SubscriptionHandle<StubIoHandler>,
        ChannelEventSource,
    ) {
        let (mut processor, command_sender, command_receiver, daemon_dispatcher_events) =
            make_processor_for_io(io.dupe());
        let defer_write_actions = match mode {
            MaterializerMode::DeferredWrites => true,
            MaterializerMode::EagerWrites => false,
        };
        processor.defer_write_actions = defer_write_actions;
        let handle = {
            let (sender, recv) = oneshot::channel();
            MaterializerSubscriptionOperation::Create { sender }.execute(&mut processor);
            recv.await.unwrap()
        };

        let command_thread = thread_spawn("buck2-dm", {
            move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(processor.run(
                    command_receiver,
                    TtlRefreshConfiguration {
                        frequency: std::time::Duration::default(),
                        min_ttl: chrono::Duration::zero(),
                        enabled: false,
                    },
                    AccessTimesUpdates::Disabled,
                    clean_stale_config,
                ));
            }
        })
        .buck_error_context("Cannot start materializer thread")
        .unwrap();

        (
            DeferredMaterializerAccessor {
                command_thread: Some(command_thread),
                command_sender,
                materialize_final_artifacts: true,
                defer_write_actions,
                eager_materialization_enabled: true,
                io,
                materializer_state_info: buck2_data::MaterializerStateInfo {
                    num_entries_from_sqlite: 0,
                },
                stats: Arc::new(DeferredMaterializerStats::default()),
            },
            handle,
            daemon_dispatcher_events,
        )
    }

    #[tokio::test]
    async fn test_allocative_profiles_artifact_tree() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let io = Arc::new(StubIoHandler::new(temp_root()));
            let path = make_path("foo/bar");
            let artifact = ArtifactValue::file(io.digest_config().empty_file());
            let (dm, _handle, _daemon_dispatcher_events) =
                make_materializer(io, None, MaterializerMode::DeferredWrites).await;

            dm.declare_existing(vec![DeclareArtifactPayload {
                path,
                artifact,
                configuration_path: None,
            }])
            .await?;

            let source = dm.allocative().await?.flamegraph().write();
            assert!(
                source.contains("artifact_tree"),
                "flamegraph source should contain artifact_tree: {source}"
            );

            dm.abort();
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_declare_reuse() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, _) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            let path = make_path("foo/bar");
            let value = ArtifactValue::file(digest_config.empty_file());

            dm.testing_declare(&path, value.dupe());
            assert_eq!(dm.io.take_log(), &[(Op::Clean, path.clone())]);

            // When redeclaring the same artifact nothing happens.
            dm.testing_declare(&path, value.dupe());
            assert_eq!(dm.io.take_log(), &[]);

            let res = dm
                .materialize_artifact(&path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;
            assert_eq!(dm.io.take_log(), &[(Op::Materialize, path.clone())]);

            dm.testing_materialization_finished(path.clone(), Utc::now(), res);
            assert_eq!(dm.io.take_log(), &[]);

            // When redeclaring the same artifact nothing happens.
            dm.testing_declare(&path, value.dupe());
            assert_eq!(dm.io.take_log(), &[]);

            // When declaring the same artifact but under it, we clean it and it's a new artifact.
            let path2 = make_path("foo/bar/baz");
            dm.testing_declare(&path2, value.dupe());
            assert_eq!(dm.io.take_log(), &[(Op::Clean, path2.clone())]);

            let _ = dm
                .materialize_artifact(&path2, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;
            assert_eq!(dm.io.take_log(), &[(Op::Materialize, path2.clone())]);

            Ok(())
        })
        .await
    }

    fn make_artifact_value_with_symlink_dep(
        target_path: &ProjectRelativePathBuf,
        target_from_symlink: &RelativePathBuf,
        digest_config: DigestConfig,
    ) -> buck2_error::Result<ArtifactValue> {
        let mut deps = ActionDirectoryBuilder::empty();
        let target = ActionDirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata::empty(
            digest_config.cas_digest_config(),
        )));
        deps.insert(target_path.as_forward_relative_path(), target)?;
        let symlink_value = ArtifactValue::new(
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::Symlink(Arc::new(Symlink::new(
                target_from_symlink.clone(),
            )))),
            Some(
                deps.fingerprint(digest_config.as_directory_serializer())
                    .shared(&*INTERNER),
            ),
        );
        Ok(symlink_value)
    }

    #[tokio::test]
    async fn test_materialize_symlink_and_target() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            // Construct a tree with a symlink and its target, materialize both at once
            let symlink_path = make_path("foo/bar_symlink");
            let target_path = make_path("foo/bar_target");
            let target_from_symlink = RelativePathBuf::from_system_path(Path::new("bar_target"))?;

            let mut materialization_config = StdBuckHashMap::default();
            // Materialize the symlink target slowly so that we actually hit the logic point where we
            // await for symlink targets and the entry materialization
            materialization_config.insert(target_path.clone(), TokioDuration::from_millis(100));

            let (mut dm, _) = make_processor(materialization_config);
            let digest_config = dm.io.digest_config();

            // Declare symlink target
            dm.testing_declare(
                &target_path,
                ArtifactValue::file(digest_config.empty_file()),
            );
            assert_eq!(dm.io.take_log(), &[(Op::Clean, target_path.clone())]);

            // Declare symlink
            let symlink_value = make_artifact_value_with_symlink_dep(
                &target_path,
                &target_from_symlink,
                digest_config,
            )?;
            dm.testing_declare(&symlink_path, symlink_value);
            assert_eq!(dm.io.take_log(), &[(Op::Clean, symlink_path.clone())]);

            dm.materialize_artifact(&symlink_path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await
                .map_err(|_| {
                    buck2_error!(
                        buck2_error::ErrorTag::MaterializationError,
                        "error materializing"
                    )
                })?;

            let logs = dm.io.take_log();
            if cfg!(unix) {
                assert_eq!(
                    logs,
                    &[
                        (Op::Materialize, symlink_path.clone()),
                        (Op::Materialize, target_path.clone())
                    ]
                );
            } else {
                assert_eq!(
                    logs,
                    &[
                        (Op::Materialize, target_path.clone()),
                        (Op::Materialize, symlink_path.clone())
                    ]
                );
            }
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_materialize_symlink_first_then_target() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            // Materialize a symlink, then materialize the target. Test that we still
            // materialize deps if the main artifact has already been materialized.
            let symlink_path = make_path("foo/bar_symlink");
            let target_path = make_path("foo/bar_target");
            let target_from_symlink = RelativePathBuf::from_system_path(Path::new("bar_target"))?;

            let mut materialization_config = StdBuckHashMap::default();
            // Materialize the symlink target slowly so that we actually hit the logic point where we
            // await for symlink targets and the entry materialization
            materialization_config.insert(target_path.clone(), TokioDuration::from_millis(100));

            let (mut dm, _) = make_processor(materialization_config);
            let digest_config = dm.io.digest_config();

            // Declare symlink
            let symlink_value = make_artifact_value_with_symlink_dep(
                &target_path,
                &target_from_symlink,
                digest_config,
            )?;
            dm.testing_declare(&symlink_path, symlink_value);
            assert_eq!(dm.io.take_log(), &[(Op::Clean, symlink_path.clone())]);

            // Materialize the symlink, at this point the target is not in the tree so it's ignored
            let res = dm
                .materialize_artifact(&symlink_path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;

            let logs = dm.io.take_log();
            assert_eq!(logs, &[(Op::Materialize, symlink_path.clone())]);

            // Mark the symlink as materialized
            dm.testing_materialization_finished(symlink_path.clone(), Utc::now(), res);
            assert_eq!(dm.io.take_log(), &[]);

            // Declare symlink target
            dm.testing_declare(
                &target_path,
                ArtifactValue::file(digest_config.empty_file()),
            );
            assert_eq!(dm.io.take_log(), &[(Op::Clean, target_path.clone())]);

            // Materialize the symlink again.
            // This time, we don't re-materialize the symlink as that's already been done.
            // But we still materialize the target as that has not been materialized yet.
            dm.materialize_artifact(&symlink_path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await
                .map_err(|_| {
                    buck2_error!(
                        buck2_error::ErrorTag::MaterializationError,
                        "error materializing"
                    )
                })?;

            let logs = dm.io.take_log();
            assert_eq!(logs, &[(Op::Materialize, target_path.clone())]);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_subscription_create_destroy() {
        let (mut dm, mut channel) = make_processor(Default::default());

        let handle = {
            let (sender, recv) = oneshot::channel();
            MaterializerSubscriptionOperation::Create { sender }.execute(&mut dm);
            recv.await.unwrap()
        };

        assert!(dm.subscriptions.has_subscription(&handle));

        drop(handle);

        while let Ok(cmd) = channel.high_priority.try_recv() {
            dm.testing_process_one_command(cmd);
        }

        assert!(!dm.subscriptions.has_any_subscriptions());
    }

    #[tokio::test]
    async fn test_subscription_notifications() {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, mut channel) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();
            let value = ArtifactValue::file(digest_config.empty_file());

            let mut handle = {
                let (sender, recv) = oneshot::channel();
                MaterializerSubscriptionOperation::Create { sender }.execute(&mut dm);
                recv.await.unwrap()
            };

            let foo_bar = make_path("foo/bar");
            let foo_bar_baz = make_path("foo/bar/baz");
            let bar = make_path("bar");
            let qux = make_path("qux");

            dm.testing_declare_existing(&foo_bar, value.dupe());

            handle.subscribe_to_paths(vec![foo_bar_baz.clone(), bar.clone()]);
            while let Ok(cmd) = channel.high_priority.try_recv() {
                dm.testing_process_one_command(cmd);
            }

            dm.testing_declare_existing(&bar, value.dupe());
            dm.testing_declare_existing(&foo_bar_baz, value.dupe());
            dm.testing_declare_existing(&qux, value.dupe());

            let mut paths = Vec::new();
            while let Ok(path) = handle.receiver().try_recv() {
                paths.push(path);
            }

            assert_eq!(paths, vec![foo_bar_baz.clone(), bar, foo_bar_baz]);
        })
        .await
    }

    #[tokio::test]
    async fn test_subscription_subscribe_also_materializes() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, mut channel) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();
            let value = ArtifactValue::file(digest_config.empty_file());

            let mut handle = {
                let (sender, recv) = oneshot::channel();
                MaterializerSubscriptionOperation::Create { sender }.execute(&mut dm);
                recv.await.unwrap()
            };

            let foo_bar = make_path("foo/bar");

            dm.testing_declare(&foo_bar, value.dupe());

            handle.subscribe_to_paths(vec![foo_bar.clone()]);
            while let Ok(cmd) = channel.high_priority.try_recv() {
                dm.testing_process_one_command(cmd);
            }

            // We need to yield to let the materialization task run. If we had a handle to it, we'd
            // just await it, but the subscription isn't retaining those handles.
            let mut log = Vec::new();
            while log.len() < 2 {
                log.extend(dm.io.take_log());
                tokio::task::yield_now().await;
            }

            assert_eq!(
                &log,
                &[
                    (Op::Clean, foo_bar.clone()),
                    (Op::Materialize, foo_bar.clone())
                ]
            );

            // Drain low priority commands. This should include our materialization finished message,
            // at which point we'll notify the subscription handle.
            while let Ok(cmd) = channel.low_priority.try_recv() {
                dm.testing_process_one_low_priority_command(cmd);
            }

            let mut paths = Vec::new();
            while let Ok(path) = handle.receiver().try_recv() {
                paths.push(path);
            }
            assert_eq!(paths, vec![foo_bar]);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_subscription_unsubscribe() {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, mut channel) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();
            let value1 = ArtifactValue::file(digest_config.empty_file());
            let value2 = ArtifactValue::dir(digest_config.empty_directory());

            let mut handle = {
                let (sender, recv) = oneshot::channel();
                MaterializerSubscriptionOperation::Create { sender }.execute(&mut dm);
                recv.await.unwrap()
            };

            let path = make_path("foo/bar");

            handle.subscribe_to_paths(vec![path.clone()]);
            while let Ok(cmd) = channel.high_priority.try_recv() {
                dm.testing_process_one_command(cmd);
            }

            dm.testing_declare_existing(&path, value1.dupe());

            handle.unsubscribe_from_paths(vec![path.clone()]);
            while let Ok(cmd) = channel.high_priority.try_recv() {
                dm.testing_process_one_command(cmd);
            }

            dm.sqlite_db
                .as_mut()
                .expect("db missing")
                .tables
                .domain_table
                .materializer_state
                .delete(vec![path.clone()])
                .buck_error_context("delete failed")
                .unwrap();
            dm.testing_declare_existing(&path, value2.dupe());

            let mut paths = Vec::new();
            while let Ok(path) = handle.receiver().try_recv() {
                paths.push(path);
            }

            // Expect only one notification
            assert_eq!(paths, vec![path]);
        })
        .await
    }

    #[tokio::test]
    async fn test_invalidate_error() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async{
            let (mut dm, _) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            let path = make_path("test/invalidate/failure");
            let value1 = ArtifactValue::file(digest_config.empty_file());
            let value2 = ArtifactValue::dir(digest_config.empty_directory());

            // Start from having something.
            dm.testing_declare_existing(&path, value1);

            // This will collect the existing future and invalidate, and then fail in doing so.
            dm.testing_declare(&path, value2);

            // Now we check that materialization fails. This needs to wait on the previous clean.
            let res = dm
                .materialize_artifact(&path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;

            assert_matches!(
            res,
            Err(SharedMaterializingError::Error(e)) if format!("{e:#}").contains("Injected error")
        );

            // We do not actually get to materializing or cleaning.
            assert_eq!(dm.io.take_log(), &[]);

            Ok(())
        }).await
    }

    #[tokio::test]
    async fn test_materialize_dep_error() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            // Construct a tree with a symlink and its target, materialize both at once
            let symlink_path = make_path("foo/bar_symlink");
            let target_path = make_path("foo/bar_target");
            let target_from_symlink =
                RelativePathBuf::from_system_path(Path::new("bar_target"))?;

            let (mut dm, mut channel) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            let target_value = ArtifactValue::file(digest_config.empty_file());
            let symlink_value = make_artifact_value_with_symlink_dep(
                &target_path,
                &target_from_symlink,
                digest_config,
            )?;
            // Declare and materialize symlink and target
            dm.testing_declare(
                &target_path,
                target_value.clone(),
            );
            dm.testing_declare(
                &symlink_path,
                symlink_value.clone(),
            );
            dm.materialize_artifact(&symlink_path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await
                .map_err(|err| buck2_error!(buck2_error::ErrorTag::MaterializationError, "error materializing {:?}", err))?;
            assert_eq!(
                dm.io.take_log(),
                &[
                    (Op::Clean, target_path.clone()),
                    (Op::Clean, symlink_path.clone()),
                    (Op::Materialize, target_path.clone()),
                    (Op::Materialize, symlink_path.clone()),
                ]
            );

            // Process materialization_finished, change symlink stage to materialized
            while let Ok(cmd) = channel.low_priority.try_recv() {
                dm.testing_process_one_low_priority_command(cmd);
            }

            // Change symlink target value and re-declare
            let content = b"not empty";
            let meta = FileMetadata {
                digest: TrackedFileDigest::from_content(content, digest_config.cas_digest_config()),
                is_executable: false,
            };
            let target_value = ArtifactValue::file(meta);
            dm.testing_declare(
                &target_path,
                target_value,
            );
            assert_eq!(dm.io.take_log(), &[(Op::Clean, target_path.clone())]);

            // Request to materialize symlink, fail to materialize target
            dm.io.set_fail_on(vec![target_path.clone()]);
            let res = dm
                .materialize_artifact(&symlink_path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;
            assert_matches!(
            res,
            Err(SharedMaterializingError::Error(e)) if format!("{e:#}").contains("Injected error")
        );
            assert_eq!(
                dm.io.take_log(),
                &[(Op::MaterializeError, target_path.clone())]
            );
            // Process materialization_finished, _only_ target is cleaned, not symlink
            while let Ok(cmd) = channel.low_priority.try_recv() {
                dm.testing_process_one_low_priority_command(cmd);
            }
            assert_eq!(dm.io.take_log(), &[(Op::Clean, target_path.clone())]);

            // Request symlink again, target is materialized and symlink materialization succeeds
            dm.io.set_fail_on(vec![]);
            dm.materialize_artifact(&symlink_path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await
                .map_err(|err| buck2_error!(buck2_error::ErrorTag::MaterializationError, "error materializing 2 {:?}", err))?;
            assert_eq!(dm.io.take_log(), &[(Op::Materialize, target_path.clone()), ]);
            Ok(())
        }).await
    }

    #[tokio::test]
    async fn test_retry() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, mut channel) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            let path = make_path("test");
            let value1 = ArtifactValue::file(digest_config.empty_file());

            // Declare a value.
            dm.testing_declare(&path, value1);

            // Make materializations fail
            dm.io.set_fail(true);

            // Materializing it fails.
            let res = dm
                .materialize_artifact(&path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;

            assert_matches!(
                res,
                Err(SharedMaterializingError::Error(e)) if format!("{e:#}").contains("Injected error")
            );

            // Unset fail, but we haven't processed materialization_finished yet so this does nothing.
            dm.io.set_fail(false);

            // Rejoining the existing future fails.
            let res = dm
                .materialize_artifact(&path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;

            assert_matches!(
                res,
                Err(SharedMaterializingError::Error(e)) if format!("{e:#}").contains("Injected error")
            );

            // Now process cleanup_finished_vacant and materialization_finished.
            let mut processed = 0;

            while let Ok(cmd) = channel.low_priority.try_recv() {
                eprintln!("got cmd = {cmd:?}");
                dm.testing_process_one_low_priority_command(cmd);
                processed += 1;
            }

            assert_eq!(processed, 2);

            // Materializing works now:
            let res = dm
                .materialize_artifact(&path, EventDispatcher::null())
                .ok_or_else(|| internal_error!("Expected a future"))?
                .await;

            assert_matches!(res, Ok(()));

            Ok(())
        }).await
    }

    const SAMPLE_BUCK_OUT_PATH: &str = "buck-out/v2/art/foo/bar";

    #[tokio::test]
    async fn test_clean_stale() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path(SAMPLE_BUCK_OUT_PATH);
            let project_root = temp_root();
            let io = Arc::new(StubIoHandler::new(project_root.clone()));
            let (dm, mut handle, _) =
                make_materializer(io.dupe(), None, MaterializerMode::DeferredWrites).await;
            materialize_write(&path, b"contents", &mut handle, &dm).await?;
            // Drop dm and flush sqlite connection.
            dm.abort();
            // Create new materializer from db state so that artifacts are not active
            let (dm, _, _) = make_materializer(io, None, MaterializerMode::DeferredWrites).await;

            let res = dm
                .clean_stale_artifacts(CleanStaleArtifactsArgs {
                    keep_since_time: DateTime::<Utc>::MAX_UTC,
                    dry_run: false,
                    tracked_only: false,
                    adaptive_low_disk_threshold: None,
                    adaptive_min_ttl: None,
                })
                .await?;

            let &buck2_data::CleanStaleStats {
                stale_artifact_count,
                stale_bytes,
                cleaned_artifact_count,
                cleaned_bytes,
                ..
            } = res
                .stats
                .as_ref()
                .unwrap_or_else(|| panic!("{}", res.message.unwrap()));
            assert_eq!(
                (
                    stale_artifact_count,
                    stale_bytes,
                    cleaned_artifact_count,
                    cleaned_bytes
                ),
                (1, 8, 1, 8)
            );
            Ok(())
        })
        .await
    }

    async fn put_stale_persistent_action_reuse_entry(
        dm: &DeferredMaterializerAccessor<StubIoHandler>,
        io: &StubIoHandler,
    ) -> buck2_error::Result<IdenticalActionReuseKey> {
        let action_digest: ActionDigest =
            FileDigest::from_content(b"stale action", io.digest_config().cas_digest_config())
                .coerce();
        let mut local_reuse_key = CasDigestData::digester(io.digest_config().cas_digest_config());
        local_reuse_key.update(b"stale local context");
        let key = IdenticalActionReuseKey {
            action_digest,
            local_reuse_key: local_reuse_key.finalize(),
        };
        dm.put_action_reuse(ActionReuseWrite::IdenticalAction {
            key: key.dupe(),
            outputs: ActionReuseOutputs::new(vec![ActionReuseOutput {
                output_path: ForwardRelativePathBuf::unchecked_new("out".to_owned()),
                content_based_path_hash: ContentBasedPathHash::OutputArtifact,
                artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[0; 32][..])?,
            }])?,
        })
        .await?;
        Ok(key)
    }

    #[tokio::test]
    async fn action_reuse_is_disabled_without_sqlite() -> buck2_error::Result<()> {
        let project_root = temp_root();
        let io = Arc::new(StubIoHandler::new(project_root));
        let (mut processor, _, _, _) = make_processor_for_io(io.dupe());
        processor.sqlite_db = None;

        let action_digest: ActionDigest =
            FileDigest::from_content(b"action", io.digest_config().cas_digest_config()).coerce();
        let mut local_reuse_key = CasDigestData::digester(io.digest_config().cas_digest_config());
        local_reuse_key.update(b"local context");
        let local_reuse_key = local_reuse_key.finalize();
        let identical_action_key = IdenticalActionReuseKey {
            action_digest,
            local_reuse_key: local_reuse_key.dupe(),
        };
        let dep_file_key = DepFileActionReuseKey {
            common_digest: local_reuse_key.dupe(),
            declared_dep_files_digest: local_reuse_key.dupe(),
            execution_platform_digest: local_reuse_key.dupe(),
            local_reuse_key,
        };
        let outputs = ActionReuseOutputs::new(vec![ActionReuseOutput {
            output_path: ForwardRelativePathBuf::unchecked_new("out".to_owned()),
            content_based_path_hash: ContentBasedPathHash::OutputArtifact,
            artifact_value_fingerprint: ArtifactValueFingerprint::try_from(&[0; 32][..])?,
        }])?;

        let (sender, receiver) = oneshot::channel();
        processor.testing_process_one_command(MaterializerCommand::GetIdenticalAction(
            identical_action_key.dupe(),
            sender,
        ));
        assert!(receiver.await??.is_none());

        let (sender, receiver) = oneshot::channel();
        processor
            .testing_process_one_command(MaterializerCommand::GetDepFile(dep_file_key, sender));
        assert!(receiver.await??.is_none());

        let (sender, receiver) = oneshot::channel();
        processor.testing_process_one_command(MaterializerCommand::PutActionReuse(
            Box::new(ActionReuseWrite::IdenticalAction {
                key: identical_action_key.dupe(),
                outputs,
            }),
            sender,
        ));
        receiver.await??;

        let (sender, receiver) = oneshot::channel();
        processor.testing_process_one_command(MaterializerCommand::TouchActionReuse(
            identical_action_key.into(),
            sender,
        ));
        receiver.await??;

        Ok(())
    }

    #[tokio::test]
    async fn persistent_action_reuse_clean_stale_stats_are_merged() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let project_root = temp_root();
            let io = Arc::new(StubIoHandler::new(project_root));
            let (dm, _, _) =
                make_materializer(io.dupe(), None, MaterializerMode::EagerWrites).await;
            let key = put_stale_persistent_action_reuse_entry(&dm, &io).await?;

            let res = dm
                .clean_stale_artifacts(CleanStaleArtifactsArgs {
                    keep_since_time: DateTime::<Utc>::MAX_UTC,
                    dry_run: false,
                    tracked_only: false,
                    adaptive_low_disk_threshold: None,
                    adaptive_min_ttl: None,
                })
                .await?;

            assert!(res.message.is_none());
            let stats = res.stats.as_ref().unwrap();
            assert_eq!(stats.stale_persistent_action_reuse_entry_count, 1);
            assert_eq!(stats.cleaned_persistent_action_reuse_entry_count, 1);
            assert!(dm.get_identical_action(key).await?.is_none());
            dm.abort();
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn persistent_action_reuse_clean_stale_manual_path_does_not_require_scheduled_config()
    -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let project_root = temp_root();
            let io = Arc::new(StubIoHandler::new(project_root));
            let (dm, _, _) =
                make_materializer(io.dupe(), None, MaterializerMode::DeferredWrites).await;
            let key = put_stale_persistent_action_reuse_entry(&dm, &io).await?;

            let res = dm
                .clean_stale_artifacts(CleanStaleArtifactsArgs {
                    keep_since_time: DateTime::<Utc>::MAX_UTC,
                    dry_run: false,
                    tracked_only: false,
                    adaptive_low_disk_threshold: None,
                    adaptive_min_ttl: None,
                })
                .await?;

            let stats = res.stats.as_ref().unwrap();
            assert_eq!(stats.cleaned_persistent_action_reuse_entry_count, 1);
            assert!(dm.get_identical_action(key).await?.is_none());
            dm.abort();
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_declare_match_marks_materialized_artifact_active() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path(SAMPLE_BUCK_OUT_PATH);
            let project_root = temp_root();
            let io = Arc::new(StubIoHandler::new(project_root.clone()));
            let (dm, mut handle, _) =
                make_materializer(io.dupe(), None, MaterializerMode::DeferredWrites).await;

            let mut artifacts = dm
                .declare_write(Box::new(|| {
                    Ok(vec![WriteRequest {
                        path: path.clone(),
                        content: b"contents".to_vec(),
                        is_executable: false,
                        configuration_path: None,
                    }])
                }))
                .await?;
            let artifact = artifacts.pop().unwrap();

            handle.subscribe_to_paths(vec![path.clone()]);
            dm.materialize_many(vec![path.clone()])
                .await?
                .next()
                .await
                .unwrap()?;
            handle.receiver().recv().await;
            dm.abort();

            let (dm, _, _) = make_materializer(io, None, MaterializerMode::DeferredWrites).await;
            assert!(
                dm.declare_match(vec![(path.clone(), artifact)])
                    .await?
                    .is_match()
            );

            let res = dm
                .clean_stale_artifacts(CleanStaleArtifactsArgs {
                    keep_since_time: DateTime::<Utc>::MAX_UTC,
                    dry_run: false,
                    tracked_only: false,
                    adaptive_low_disk_threshold: None,
                    adaptive_min_ttl: None,
                })
                .await?;

            let &buck2_data::CleanStaleStats {
                stale_artifact_count,
                stale_bytes,
                cleaned_artifact_count,
                cleaned_bytes,
                ..
            } = res
                .stats
                .as_ref()
                .unwrap_or_else(|| panic!("{}", res.message.unwrap()));
            assert_eq!(
                (
                    stale_artifact_count,
                    stale_bytes,
                    cleaned_artifact_count,
                    cleaned_bytes
                ),
                (0, 0, 0, 0)
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn failed_declare_match_does_not_activate_partial_output_set() -> buck2_error::Result<()>
    {
        ignore_stack_overflow_checks_for_future(async {
            let first_path = make_path("buck-out/v2/gen/root/first");
            let second_path = make_path("buck-out/v2/gen/root/second");
            let project_root = temp_root();
            let io = Arc::new(StubIoHandler::new(project_root.clone()));
            let (dm, mut handle, _) =
                make_materializer(io.dupe(), None, MaterializerMode::DeferredWrites).await;

            let artifacts = dm
                .declare_write(Box::new(|| {
                    Ok(vec![
                        WriteRequest {
                            path: first_path.clone(),
                            content: b"first".to_vec(),
                            is_executable: false,
                            configuration_path: None,
                        },
                        WriteRequest {
                            path: second_path.clone(),
                            content: b"second".to_vec(),
                            is_executable: false,
                            configuration_path: None,
                        },
                    ])
                }))
                .await?;
            handle.subscribe_to_paths(vec![first_path.clone(), second_path.clone()]);
            let mut materializations = dm
                .materialize_many(vec![first_path.clone(), second_path.clone()])
                .await?;
            while let Some(materialization) = materializations.next().await {
                materialization?;
            }
            handle.receiver().recv().await;
            dm.abort();

            let (dm, _, _) =
                make_materializer(io.dupe(), None, MaterializerMode::DeferredWrites).await;
            let first_artifact = artifacts[0].dupe();
            let wrong_second_artifact = ArtifactValue::file(io.digest_config().empty_file());
            assert!(
                !dm.declare_match(vec![
                    (first_path, first_artifact),
                    (second_path, wrong_second_artifact),
                ])
                .await?
                .is_match()
            );

            let result = dm
                .clean_stale_artifacts(CleanStaleArtifactsArgs {
                    keep_since_time: DateTime::<Utc>::MAX_UTC,
                    dry_run: false,
                    tracked_only: false,
                    adaptive_low_disk_threshold: None,
                    adaptive_min_ttl: None,
                })
                .await?;
            let stats = result.stats.unwrap();
            assert_eq!(stats.cleaned_artifact_count, 2);
            dm.abort();
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_stale_interrupt() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path(SAMPLE_BUCK_OUT_PATH);
            let project_root = temp_root();
            let io = Arc::new(StubIoHandler::new(project_root.clone()));
            let (dm, mut handle, _) =
                make_materializer(io.dupe(), None, MaterializerMode::DeferredWrites).await;
            materialize_write(&path, b"contents", &mut handle, &dm).await?;

            let read_dir_barriers =
                Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
            let io = Arc::new(
                StubIoHandler::new(project_root.dupe())
                    .with_read_dir_barriers(read_dir_barriers.dupe()),
            );
            let (dm, _, _) = make_materializer(io, None, MaterializerMode::DeferredWrites).await;

            // Interrupt while scanning buck-out
            let dm = Arc::new(dm);
            let dm_dup = dm.dupe();
            let fut = dm_dup.clean_stale_artifacts(CleanStaleArtifactsArgs {
                keep_since_time: DateTime::<Utc>::MAX_UTC,
                dry_run: false,
                tracked_only: false,
                adaptive_low_disk_threshold: None,
                adaptive_min_ttl: None,
            });
            thread::spawn(move || {
                // Wait until a read_dir request is about to execute
                read_dir_barriers.0.wait();
                // Sending a high_priority command will interrupt the processor
                let noop_command = MaterializerCommand::DeclareExisting(vec![], None, None);
                let _ = dm.command_sender.send(noop_command);
                // Wait after sending so that a second request doesn't start
                read_dir_barriers.1.wait();
            });
            let res = fut.await?;
            let &buck2_data::CleanStaleStats {
                stale_artifact_count,
                stale_bytes,
                cleaned_artifact_count,
                cleaned_bytes,
                ..
            } = res.stats.as_ref().unwrap();
            assert_eq!(
                (
                    stale_artifact_count,
                    stale_bytes,
                    cleaned_artifact_count,
                    cleaned_bytes
                ),
                (0, 0, 0, 0)
            );

            let clean_barriers = Arc::new((Barrier::new(2), Barrier::new(2)));
            let io = Arc::new(
                StubIoHandler::new(project_root.dupe()).with_clean_barriers(clean_barriers.dupe()),
            );
            let (dm, _, _) = make_materializer(io, None, MaterializerMode::DeferredWrites).await;

            // Interrupt while deleting files
            let dm = Arc::new(dm);
            let dm_dup = dm.dupe();
            let fut = dm_dup.clean_stale_artifacts(CleanStaleArtifactsArgs {
                keep_since_time: DateTime::<Utc>::MAX_UTC,
                dry_run: false,
                tracked_only: false,
                adaptive_low_disk_threshold: None,
                adaptive_min_ttl: None,
            });
            thread::spawn(move || {
                // Wait until a single clean request is about to execute
                clean_barriers.0.wait();
                // Sending a high_priority command will drop the clean guard immediately (from this thread)
                let noop_command = MaterializerCommand::DeclareExisting(vec![], None, None);
                let _ = dm.command_sender.send(noop_command);
                // Wait after sending, executing clean request will complete but a second request doesn't start because
                // the single io thread is blocked
                clean_barriers.1.wait();
            });
            let res = fut.await?;
            let &buck2_data::CleanStaleStats {
                stale_artifact_count,
                stale_bytes,
                cleaned_artifact_count,
                cleaned_bytes,
                ..
            } = res.stats.as_ref().unwrap();
            assert_eq!(
                (
                    stale_artifact_count,
                    stale_bytes,
                    cleaned_artifact_count,
                    cleaned_bytes
                ),
                (1, 8, 0, 0)
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_stale_schedule() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path(SAMPLE_BUCK_OUT_PATH);
            let project_root = temp_root();
            // dry run because it's easier and since this is only testing that cleans are triggered by the materializer
            let clean_stale_config = CleanStaleConfig {
                clean_period: std::time::Duration::from_secs(1),
                artifact_ttl: std::time::Duration::from_secs(0),
                start_offset: std::time::Duration::from_secs(0),
                low_disk: None,
                dry_run: true,
            };
            let io = Arc::new(StubIoHandler::new(project_root.dupe()));
            let (dm, mut handle, mut daemon_dispatcher_events) = make_materializer(
                io.dupe(),
                Some(clean_stale_config),
                MaterializerMode::DeferredWrites,
            )
            .await;
            materialize_write(&path, b"contents", &mut handle, &dm).await?;

            let receive_clean_result = |events: &mut ChannelEventSource| {
                let event = events.receive().unwrap();
                match event.unpack_buck().unwrap().data() {
                    buck2_data::buck_event::Data::Instant(instant) => match instant.data.as_ref() {
                        Some(buck2_data::instant_event::Data::CleanStaleResult(res)) => {
                            Some(res.clone())
                        }
                        _ => None,
                    },
                    _ => None,
                }
                .unwrap()
            };
            // The first clean stale request is scheduled at roughly the same time as materialize_write so we may receive an initial clean event
            // before anything is materialized, if so ignore events until an artifact is found (retained != 0).
            // It should only be necessary to wait for a single clean (1 second) but wait for up to 5 just in case.
            let mut i = 0;
            while i < 5 {
                let res = receive_clean_result(&mut daemon_dispatcher_events);
                let stats = res.stats.unwrap();
                if let buck2_data::CleanStaleStats {
                    retained_artifact_count: 0,
                    ..
                } = stats
                {
                    i += 1;
                } else {
                    break;
                }
            }
            let res = receive_clean_result(&mut daemon_dispatcher_events);
            let buck2_data::CleanStaleStats {
                retained_artifact_count,
                ..
            } = res.stats.unwrap();
            assert_eq!(retained_artifact_count, 1);
            // check it's scheduled more than once
            let res = receive_clean_result(&mut daemon_dispatcher_events);
            let buck2_data::CleanStaleStats {
                retained_artifact_count,
                ..
            } = res.stats.unwrap();
            assert_eq!(retained_artifact_count, 1);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_has_artifact_at() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, _) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            let path = make_path("test/dir/path");
            let value1 = ArtifactValue::dir(digest_config.empty_directory());
            dm.testing_declare_existing(&path, value1);

            assert!(dm.testing_has_artifact(path.clone()));
            assert!(!dm.testing_has_artifact(path.join(ForwardRelativePath::new("foo").unwrap())));
            assert!(!dm.testing_has_artifact(path.parent().unwrap().to_owned()));

            dm.materialize_artifact(&path, EventDispatcher::null());
            assert!(dm.testing_has_artifact(path.clone()));
            assert!(!dm.testing_has_artifact(path.join(ForwardRelativePath::new("foo").unwrap())));
            assert!(!dm.testing_has_artifact(path.parent().unwrap().to_owned()));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_artifact_values_for_materialized_paths() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, _) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            // Path not in tree
            let unknown_path = make_path("not/in/tree");
            let result =
                dm.testing_get_artifact_values_for_materialized_paths(vec![unknown_path.clone()]);
            assert_eq!(result.len(), 1);
            assert!(result[0].is_none());

            let declared_file_path = make_path("declared/file");
            let file_value = ArtifactValue::file(digest_config.empty_file());
            dm.testing_declare(&declared_file_path, file_value.dupe());
            let result = dm.testing_get_artifact_values_for_materialized_paths(vec![
                declared_file_path.clone(),
            ]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, declared_file_path);
            assert_eq!(returned_value, file_value);

            let declared_dir_path = make_path("declared/dir");
            let dir_value = ArtifactValue::dir(digest_config.empty_directory());
            dm.testing_declare(&declared_dir_path, dir_value.dupe());
            let result = dm.testing_get_artifact_values_for_materialized_paths(vec![
                declared_dir_path.clone(),
            ]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, declared_dir_path);
            assert_eq!(returned_value, dir_value);

            let materialized_file_path = make_path("materialized/file");
            let file_value = ArtifactValue::file(digest_config.empty_file());
            dm.testing_declare_existing(&materialized_file_path, file_value.dupe());
            let result = dm.testing_get_artifact_values_for_materialized_paths(vec![
                materialized_file_path.clone(),
            ]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, materialized_file_path);
            assert_eq!(returned_value, file_value);

            let materialized_dir_path = make_path("materialized/dir");
            let dir_value = ArtifactValue::dir(digest_config.empty_directory());
            dm.testing_declare_existing(&materialized_dir_path, dir_value.dupe());
            let result = dm.testing_get_artifact_values_for_materialized_paths(vec![
                materialized_dir_path.clone(),
            ]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, materialized_dir_path);
            match returned_value.entry() {
                ActionDirectoryEntry::Dir(_) => {}
                ActionDirectoryEntry::Leaf(_) => panic!("expected directory artifact"),
            }

            // Subpath of an artifact via projected artifact; returns None
            let parent_path = make_path("parent/artifact");
            let parent_value = ArtifactValue::dir(digest_config.empty_directory());
            dm.testing_declare(&parent_path, parent_value);
            let subpath = make_path("parent/artifact/child");
            let result =
                dm.testing_get_artifact_values_for_materialized_paths(vec![subpath.clone()]);
            assert_eq!(result.len(), 1);
            assert!(result[0].is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_artifact_values_for_projected_paths() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let (mut dm, _) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();

            // Build a non-empty directory artifact with internal structure:
            //   child/file.txt
            //   child/subdir/nested.txt
            //   top_file.txt
            let mut builder = ActionDirectoryBuilder::empty();
            insert_file(
                &mut builder,
                ProjectRelativePathBuf::unchecked_new("child/file.txt".to_owned()),
                FileMetadata::empty(digest_config.cas_digest_config()),
            )?;
            insert_file(
                &mut builder,
                ProjectRelativePathBuf::unchecked_new("child/subdir/nested.txt".to_owned()),
                FileMetadata::empty(digest_config.cas_digest_config()),
            )?;
            insert_file(
                &mut builder,
                ProjectRelativePathBuf::unchecked_new("top_file.txt".to_owned()),
                FileMetadata::empty(digest_config.cas_digest_config()),
            )?;
            let shared_dir = builder
                .fingerprint(digest_config.as_directory_serializer())
                .shared(&*INTERNER);
            let dir_value = ArtifactValue::dir(shared_dir);

            let artifact_root = make_path("parent/artifact");
            dm.testing_declare(&artifact_root, dir_value);

            // Subpath of a file within artifact - returns the base artifact entry (Dir), not the projected file
            let file_subpath = make_path("parent/artifact/child/file.txt");
            let result =
                dm.testing_get_root_artifact_values_for_subpaths(vec![file_subpath.clone()]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, artifact_root);
            match returned_value.entry() {
                ActionDirectoryEntry::Dir(_) => {}
                ActionDirectoryEntry::Leaf(_) => panic!("expected directory artifact"),
            }

            // Subpath of a subdirectory within artifact - returns the base artifact entry (Dir)
            let subdir_path = make_path("parent/artifact/child/subdir");
            let result =
                dm.testing_get_root_artifact_values_for_subpaths(vec![subdir_path.clone()]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, artifact_root);
            match returned_value.entry() {
                ActionDirectoryEntry::Dir(_) => {}
                ActionDirectoryEntry::Leaf(_) => panic!("expected directory artifact"),
            }

            // Nonexistent subpath within artifact - still returns the base artifact entry (Dir)
            let nonexistent = make_path("parent/artifact/does_not_exist.txt");
            let result =
                dm.testing_get_root_artifact_values_for_subpaths(vec![nonexistent.clone()]);
            assert_eq!(result.len(), 1);
            let (returned_path, returned_value) = result[0].clone().unwrap();
            assert_eq!(returned_path, artifact_root);
            match returned_value.entry() {
                ActionDirectoryEntry::Dir(_) => {}
                ActionDirectoryEntry::Leaf(_) => panic!("expected directory artifact"),
            }

            Ok(())
        })
        .await
    }

    // ---- Eager materialization tests ----

    /// Helper to extract the priority_control from an Active/Materializing artifact in the tree.
    fn get_priority_control<T: IoHandler>(
        dm: &mut DeferredMaterializerCommandProcessor<T>,
        path: &ProjectRelativePathBuf,
    ) -> DynamicPriorityHandle {
        let mut path_iter = path.iter();
        let data = dm
            .tree
            .prefix_get_mut(&mut path_iter)
            .unwrap_or_else(|| panic!("artifact {} should be in tree", path));
        data.processing
            .active_ref()
            .unwrap_or_else(|| panic!("Expected Active processing for {}", path))
            .priority_control
            .clone()
    }

    /// Helper to extract the materializing future without upgrading priority.
    fn get_materializing_future<T: IoHandler>(
        dm: &mut DeferredMaterializerCommandProcessor<T>,
        path: &ProjectRelativePathBuf,
    ) -> MaterializingFuture {
        let mut path_iter = path.iter();
        let data = dm
            .tree
            .prefix_get_mut(&mut path_iter)
            .unwrap_or_else(|| panic!("artifact {} should be in tree", path));
        match &data
            .processing
            .active_ref()
            .unwrap_or_else(|| panic!("Expected Active/Materializing for {}", path))
            .future
        {
            ProcessingFuture::Materializing(f) => f.clone(),
            ProcessingFuture::Cleaning(_) => panic!("Expected Active/Materializing for {}", path),
        }
    }

    /// Helper to register paths, declare an artifact, and return the processor ready for assertions.
    fn eager_declare<T: IoHandler>(
        dm: &mut DeferredMaterializerCommandProcessor<T>,
        path: &ProjectRelativePathBuf,
        configuration_path: Option<ProjectRelativePathBuf>,
    ) {
        let digest_config = dm.io.digest_config();
        let value = ArtifactValue::file(digest_config.empty_file());
        eager_declare_with_value(dm, path, value, configuration_path);
    }

    /// Like `eager_declare`, but lets the caller supply the `ArtifactValue` (e.g. a symlink
    /// with deps) instead of defaulting to an empty file.
    fn eager_declare_with_value<T: IoHandler>(
        dm: &mut DeferredMaterializerCommandProcessor<T>,
        path: &ProjectRelativePathBuf,
        value: ArtifactValue,
        configuration_path: Option<ProjectRelativePathBuf>,
    ) {
        dm.testing_process_one_command(MaterializerCommand::Declare(
            DeclareArtifactPayload {
                path: path.clone(),
                artifact: value,
                configuration_path,
            },
            Box::new(ArtifactMaterializationMethod::Test),
            EventDispatcher::null(),
            None,
        ));
    }

    /// Register → Declare (Low) → verify materialization completes → Release → verify cancelled
    #[tokio::test]
    async fn test_eager_declare_and_cancel() {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path("buck-out/v2/eager/cancel");
            let (mut dm, _) = make_processor(Default::default());

            // Register and declare → starts materializing at Low
            let sender = dm.command_sender.dupe();
            let leases = dm
                .eager_materializations
                .register(vec![path.clone()], &sender);
            eager_declare(&mut dm, &path, None);
            assert_eq!(dm.io.take_log(), &[(Op::Clean, path.clone())]);

            let priority_control = get_priority_control(&mut dm, &path);
            assert_eq!(priority_control.priority(), Priority::Low);

            // Await eager materialization → should complete at Low (without upgrading priority)
            let fut = get_materializing_future(&mut dm, &path);
            fut.await.expect("Materialization should succeed");
            assert_eq!(dm.io.take_log(), &[(Op::Materialize, path.clone())]);

            // Drop leases and release → should cancel
            let cancel_token = priority_control.cancel_token().clone();
            drop(leases);
            dm.testing_process_one_command(MaterializerCommand::ReleaseEagerPath(Arc::new(
                path.clone(),
            )));

            assert!(
                cancel_token.is_cancelled(),
                "Low priority materialization should be cancelled on release"
            );
        })
        .await
    }

    /// Register → Declare (Low) → Demand materialize (High) → verify materialization completes
    #[tokio::test]
    async fn test_eager_declare_upgrade_and_release() {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path("buck-out/v2/eager/upgrade");
            let (mut dm, _) = make_processor(Default::default());

            // Register and declare → starts eager materialization at Low
            let sender = dm.command_sender.dupe();
            let leases = dm
                .eager_materializations
                .register(vec![path.clone()], &sender);
            eager_declare(&mut dm, &path, None);
            assert_eq!(dm.io.take_log(), &[(Op::Clean, path.clone())]);
            assert_eq!(
                get_priority_control(&mut dm, &path).priority(),
                Priority::Low
            );

            // Demand materialize → upgrades to High, returns existing future
            let fut = dm
                .materialize_artifact(&path, EventDispatcher::null())
                .expect("Expected a materializing future");
            assert_eq!(
                get_priority_control(&mut dm, &path).priority(),
                Priority::High
            );

            // Await materialization → should complete
            fut.await.expect("Materialization should succeed");
            assert_eq!(dm.io.take_log(), &[(Op::Materialize, path.clone())]);

            // Release after materialization completed → should NOT cancel
            let cancel_token = get_priority_control(&mut dm, &path).cancel_token().clone();
            drop(leases);
            dm.testing_process_one_command(MaterializerCommand::ReleaseEagerPath(Arc::new(
                path.clone(),
            )));
            assert!(
                !cancel_token.is_cancelled(),
                "High priority materialization should not be cancelled on release"
            );
        })
        .await
    }

    /// Two actions register same path → one releases → other still holds lease → not cancelled
    #[tokio::test]
    async fn test_eager_multiple_callers_register_path() {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path("buck-out/v2/eager/shared");
            let (mut dm, _) = make_processor(Default::default());
            let sender = dm.command_sender.dupe();

            // Two actions register the same path → both get Arcs to the same lease
            let leases_a = dm
                .eager_materializations
                .register(vec![path.clone()], &sender);
            let leases_b = dm
                .eager_materializations
                .register(vec![path.clone()], &sender);

            // Declare → eager materialization at Low
            eager_declare(&mut dm, &path, None);
            assert_eq!(dm.io.take_log(), &[(Op::Clean, path.clone())]);
            assert_eq!(
                get_priority_control(&mut dm, &path).priority(),
                Priority::Low
            );

            // Await materialization
            let fut = get_materializing_future(&mut dm, &path);
            fut.await.expect("Materialization should succeed");
            assert_eq!(dm.io.take_log(), &[(Op::Materialize, path.clone())]);

            // Action A finishes, drops its leases
            let cancel_token = get_priority_control(&mut dm, &path).cancel_token().clone();
            drop(leases_a);

            // Simulate a ReleaseEagerPath while B still holds a lease
            // release() sees Weak::upgrade() succeeds → returns false → no cancel
            dm.testing_process_one_command(MaterializerCommand::ReleaseEagerPath(Arc::new(
                path.clone(),
            )));
            assert!(
                !cancel_token.is_cancelled(),
                "Should not cancel while another action still holds a lease"
            );

            // Action B finishes, drops its leases → last Arc dropped
            drop(leases_b);
            dm.testing_process_one_command(MaterializerCommand::ReleaseEagerPath(Arc::new(
                path.clone(),
            )));
            assert!(
                cancel_token.is_cancelled(),
                "Should cancel after all leases released"
            );
        })
        .await
    }

    /// Register config path → declare content-hash path with configuration_path → release cancels bridged path.
    #[tokio::test]
    async fn test_eager_configuration_path_lookup_and_release() {
        ignore_stack_overflow_checks_for_future(async {
            let artifact_path = make_path("buck-out/v2/gen/content-hash/foo/bar");
            let config_path = make_path("buck-out/v2/gen/config-hash/foo/bar");
            let (mut dm, _) = make_processor(Default::default());

            let sender = dm.command_sender.dupe();
            let leases = dm
                .eager_materializations
                .register(vec![config_path.clone()], &sender);

            eager_declare(&mut dm, &artifact_path, Some(config_path.clone()));
            assert_eq!(dm.io.take_log(), &[(Op::Clean, artifact_path.clone())]);
            assert_eq!(
                get_priority_control(&mut dm, &artifact_path).priority(),
                Priority::Low,
                "configuration_path lookup should trigger eager materialization at Low"
            );

            let cancel_token = get_priority_control(&mut dm, &artifact_path)
                .cancel_token()
                .clone();
            drop(leases);
            dm.testing_process_one_command(MaterializerCommand::ReleaseEagerPath(Arc::new(
                config_path,
            )));

            assert!(
                cancel_token.is_cancelled(),
                "Releasing configuration_path should cancel bridged low-priority materialization"
            );
        })
        .await
    }

    #[tokio::test]
    async fn test_join_cancelled_future_starts_fresh() {
        ignore_stack_overflow_checks_for_future(async {
            let path = make_path("buck-out/v2/eager/cancel-on-join");
            let (mut dm, _) = make_processor(Default::default());

            let sender = dm.command_sender.dupe();
            let _leases = dm
                .eager_materializations
                .register(vec![path.clone()], &sender);
            eager_declare(&mut dm, &path, None);
            assert_eq!(dm.io.take_log(), &[(Op::Clean, path.clone())]);

            let priority_control = get_priority_control(&mut dm, &path);
            assert_eq!(priority_control.priority(), Priority::Low);
            priority_control.cancel();

            let version_before = dm
                .tree
                .prefix_get_mut(&mut path.iter())
                .unwrap()
                .processing
                .current_version();

            let fut = dm
                .materialize_artifact_with_priority(&path, EventDispatcher::null(), Priority::High)
                .expect("Expected a materializing future");

            let new_priority_control = get_priority_control(&mut dm, &path);
            let version_after = dm
                .tree
                .prefix_get_mut(&mut path.iter())
                .unwrap()
                .processing
                .current_version();
            assert!(version_after > version_before);
            assert!(!new_priority_control.cancel_token().is_cancelled());
            assert_eq!(new_priority_control.priority(), Priority::High);

            fut.await.expect("Fresh materialization should succeed");
            assert!(
                dm.io
                    .take_log()
                    .iter()
                    .any(|(op, p)| *op == Op::Materialize && *p == path),
                "Fresh materialize IO should have been dispatched"
            );
        })
        .await
    }

    /// Releasing an eager cluster must not cancel Low siblings when any member is High.
    #[tokio::test]
    async fn test_eager_release_skips_cancel_when_cluster_has_promoted_member() {
        ignore_stack_overflow_checks_for_future(async {
            let config_path = make_path("buck-out/v2/eager/cluster/config");
            let artifact_a = make_path("buck-out/v2/eager/cluster/a");
            let artifact_b = make_path("buck-out/v2/eager/cluster/b");
            let (mut dm, _) = make_processor(Default::default());

            let sender = dm.command_sender.dupe();
            let leases = dm
                .eager_materializations
                .register(vec![config_path.clone()], &sender);

            eager_declare(&mut dm, &artifact_a, Some(config_path.clone()));
            eager_declare(&mut dm, &artifact_b, Some(config_path.clone()));
            assert_eq!(
                get_priority_control(&mut dm, &artifact_a).priority(),
                Priority::Low
            );
            assert_eq!(
                get_priority_control(&mut dm, &artifact_b).priority(),
                Priority::Low
            );

            let _fut_a = dm
                .materialize_artifact(&artifact_a, EventDispatcher::null())
                .expect("Expected a materializing future");
            assert_eq!(
                get_priority_control(&mut dm, &artifact_a).priority(),
                Priority::High
            );
            assert_eq!(
                get_priority_control(&mut dm, &artifact_b).priority(),
                Priority::Low
            );

            let token_a = get_priority_control(&mut dm, &artifact_a)
                .cancel_token()
                .clone();
            let token_b = get_priority_control(&mut dm, &artifact_b)
                .cancel_token()
                .clone();

            drop(leases);
            dm.testing_process_one_command(MaterializerCommand::ReleaseEagerPath(Arc::new(
                config_path,
            )));

            assert!(
                !token_a.is_cancelled(),
                "High-priority cluster member must not be cancelled"
            );
            assert!(
                !token_b.is_cancelled(),
                "Low-priority cluster member must not be cancelled when a sibling is High"
            );
        })
        .await
    }

    /// High-priority promotion should propagate to direct symlink-dep targets.
    #[tokio::test]
    async fn test_priority_promotion_propagates_to_symlink_deps() -> buck2_error::Result<()> {
        ignore_stack_overflow_checks_for_future(async {
            let symlink_path = make_path("foo/parent_symlink");
            let target_path = make_path("foo/dep_target");
            let target_from_symlink = RelativePathBuf::from_system_path(Path::new("dep_target"))?;

            let (mut dm, _) = make_processor(Default::default());
            let digest_config = dm.io.digest_config();
            let sender = dm.command_sender.dupe();

            let _leases = dm
                .eager_materializations
                .register(vec![symlink_path.clone(), target_path.clone()], &sender);

            eager_declare(&mut dm, &target_path, None);
            assert_eq!(
                get_priority_control(&mut dm, &target_path).priority(),
                Priority::Low
            );

            let symlink_value = make_artifact_value_with_symlink_dep(
                &target_path,
                &target_from_symlink,
                digest_config,
            )?;
            eager_declare_with_value(&mut dm, &symlink_path, symlink_value, None);
            assert_eq!(
                get_priority_control(&mut dm, &symlink_path).priority(),
                Priority::Low
            );

            let _fut = dm
                .materialize_artifact(&symlink_path, EventDispatcher::null())
                .expect("Expected a materializing future");

            assert_eq!(
                get_priority_control(&mut dm, &symlink_path).priority(),
                Priority::High,
            );
            assert_eq!(
                get_priority_control(&mut dm, &target_path).priority(),
                Priority::High,
                "Direct symlink-dep target should be promoted to High along with its parent",
            );
            Ok(())
        })
        .await
    }
}
