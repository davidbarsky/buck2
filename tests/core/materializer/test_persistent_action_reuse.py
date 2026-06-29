# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict

from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.buck_workspace import buck_test
from buck2.tests.e2e_util.helper.utils import filter_events


ACTION_EXECUTION_KIND_LOCAL = 1
ACTION_EXECUTION_KIND_LOCAL_DEP_FILE = 7
ACTION_EXECUTION_KIND_LOCAL_ACTION_CACHE = 10


async def action_execution_kinds(buck: Buck) -> list[int]:
    return await filter_events(
        buck,
        "Event",
        "data",
        "SpanEnd",
        "data",
        "ActionExecution",
        "execution_kind",
    )


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_identical_action_is_reused_after_restart(buck: Buck) -> None:
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_ACTION_CACHE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_invocation_ids_prevent_stale_restart_hit(buck: Buck) -> None:
    target = "root//:invocation_ids"
    result = await buck.build(target)
    output = result.get_build_report().output_for_target(target)
    invocation_ids = output.read_text().splitlines()
    assert len(invocation_ids) == 2
    assert all(invocation_ids)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    await buck.build(target)
    next_invocation_ids = output.read_text().splitlines()
    assert len(next_invocation_ids) == 2
    assert all(next_invocation_ids)
    assert next_invocation_ids[0] != invocation_ids[0]
    assert next_invocation_ids[1] != invocation_ids[1]
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_incremental_action_restart_runs_with_retained_output(
    buck: Buck,
) -> None:
    target = "root//:incremental"
    result = await buck.build(target)
    output = result.get_build_report().output_for_target(target)
    assert output.read_text() == "x"
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    await buck.build(target)
    assert output.read_text() == "xx"
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_incremental_producer_and_downstream_rerun_after_restart(
    buck: Buck,
) -> None:
    target = "root//:incremental_downstream"
    result = await buck.build(target)
    output = result.get_build_report().output_for_target(target)
    assert output.read_text() == "stable\n"
    assert await action_execution_kinds(buck) == [
        ACTION_EXECUTION_KIND_LOCAL,
        ACTION_EXECUTION_KIND_LOCAL,
    ]

    await buck.kill()
    await buck.build(target)
    assert output.read_text() == "stable\n"
    assert await action_execution_kinds(buck) == [
        ACTION_EXECUTION_KIND_LOCAL,
        ACTION_EXECUTION_KIND_LOCAL,
    ]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_incremental_dep_file_action_restart_runs_with_retained_output(
    buck: Buck,
) -> None:
    target = "root//:incremental_dep_file"
    result = await buck.build(target, "--eager-dep-files")
    output = result.get_build_report().output_for_target(target)
    assert output.read_text() == "x"
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    (buck.cwd / "unused.txt").write_text("unused changed\n")
    await buck.build(target, "--eager-dep-files")
    assert output.read_text() == "xx"
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_metadata_dep_file_restart_miss_after_metadata_input_change(
    buck: Buck,
) -> None:
    target = "root//:metadata_dep_file"
    result = await buck.build(target, "--eager-dep-files")
    output = result.get_build_report().output_for_target(target)
    initial_metadata = output.read_text()
    assert "metadata_input.txt" in initial_metadata
    assert "unused.txt" not in initial_metadata
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    (buck.cwd / "unused.txt").write_text("unused changed\n")
    await buck.build(target, "--eager-dep-files")
    assert output.read_text() == initial_metadata
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_DEP_FILE]

    await buck.kill()
    (buck.cwd / "metadata_input.txt").write_text("metadata changed\n")
    await buck.build(target, "--eager-dep-files")
    assert output.read_text() != initial_metadata
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_damaged_dep_file_output_is_rebuilt(buck: Buck) -> None:
    await buck.build("root//:dep_file", "--eager-dep-files")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
    await buck.kill()

    dep_files = list((buck.cwd / "buck-out").glob("**/dep_file.d"))
    assert len(dep_files) == 1
    dep_files[0].write_bytes(b"\xff")
    (buck.cwd / "unused.txt").write_text("unused changed once\n")

    await buck.build("root//:dep_file", "--eager-dep-files")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    (buck.cwd / "unused.txt").write_text("unused changed twice\n")
    await buck.build("root//:dep_file")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_DEP_FILE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_damaged_persistent_action_reuse_database_is_rebuilt(
    buck: Buck,
) -> None:
    target = "root//:identical"
    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
    await buck.kill()

    state_dir = buck.cwd / "buck-out/v2/cache/materializer_state"
    assert state_dir.is_dir()
    state_dir.rename(state_dir.with_suffix(".damaged"))
    state_dir.mkdir()
    (state_dir / "db.sqlite").write_bytes(b"not a sqlite database")

    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_ACTION_CACHE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_damaged_persistent_output_is_rebuilt(buck: Buck) -> None:
    target = "root//:identical"
    result = await buck.build(target)
    output = result.get_build_report().output_for_target(target)
    assert output.read_text() == "identical\n"
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
    await buck.kill()

    output.write_text("damaged\n")
    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
    assert output.read_text() == "identical\n"

    await buck.kill()
    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_ACTION_CACHE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_missing_persistent_output_is_rebuilt(buck: Buck) -> None:
    target = "root//:identical"
    result = await buck.build(target)
    output = result.get_build_report().output_for_target(target)
    assert output.is_file()
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
    await buck.kill()

    output.unlink()
    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
    assert output.read_text() == "identical\n"

    await buck.kill()
    await buck.build(target)
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_ACTION_CACHE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "false"}},
)
async def test_disabled_config_does_not_persist(buck: Buck) -> None:
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_dep_file_restart_reuses_after_unused_input_change(buck: Buck) -> None:
    await buck.build("root//:dep_file", "--eager-dep-files")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    (buck.cwd / "unused.txt").write_text("unused changed\n")
    await buck.build("root//:dep_file")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_DEP_FILE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_lazy_dep_file_is_reused_after_restart(buck: Buck) -> None:
    await buck.build("root//:dep_file")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    (buck.cwd / "unused.txt").write_text("unused changed\n")
    await buck.build("root//:dep_file")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_DEP_FILE]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_dep_file_restart_miss_after_used_input_change(buck: Buck) -> None:
    await buck.build("root//:dep_file", "--eager-dep-files")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    (buck.cwd / "used.txt").write_text("used changed\n")
    await buck.build("root//:dep_file")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]


@buck_test(
    data_dir="persistent_action_reuse",
    extra_buck_config={"buck2": {"sqlite_materializer_state": "true"}},
)
async def test_clean_stale_removes_persistent_restart_cache_entry(
    buck: Buck,
) -> None:
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]

    await buck.kill()
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL_ACTION_CACHE]

    await buck.clean("--stale=0s")
    clean_results = await filter_events(
        buck,
        "Event",
        "data",
        "Instant",
        "data",
        "CleanStaleResult",
        "stats",
    )
    assert any(
        stats.get("cleaned_persistent_action_reuse_entry_count", 0) >= 1
        for stats in clean_results
    )

    await buck.kill()
    await buck.build("root//:identical")
    assert await action_execution_kinds(buck) == [ACTION_EXECUTION_KIND_LOCAL]
