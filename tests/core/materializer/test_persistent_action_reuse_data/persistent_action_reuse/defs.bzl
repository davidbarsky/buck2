# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _identical_impl(ctx):
    out = ctx.actions.declare_output("identical.out")
    ctx.actions.run(
        [
            "sh",
            "-c",
            "printf 'identical\\n' > \"$1\"",
            "--",
            out.as_output(),
        ],
        category = "persistent_identical",
        allow_cache_upload = True,
    )
    return [DefaultInfo(default_output = out)]

identical = rule(impl = _identical_impl, attrs = {})

def _invocation_ids_impl(ctx):
    out = ctx.actions.declare_output("invocation_ids.out")
    ctx.actions.run(
        [
            "sh",
            "-c",
            "printf '%s\\n%s\\n' \"$BUCK_BUILD_ID\" \"$BUCK2_DAEMON_UUID\" > \"$1\"",
            "--",
            out.as_output(),
        ],
        category = "persistent_invocation_ids",
    )
    return [DefaultInfo(default_output = out)]

invocation_ids = rule(impl = _invocation_ids_impl, attrs = {})

def _incremental_impl(ctx):
    out = ctx.actions.declare_output("incremental.out")
    ctx.actions.run(
        [
            "sh",
            "-c",
            "printf x >> \"$1\"",
            "--",
            out.as_output(),
        ],
        category = "persistent_incremental",
        local_only = True,
        no_outputs_cleanup = True,
    )
    return [DefaultInfo(default_output = out)]

incremental = rule(impl = _incremental_impl, attrs = {})

def _incremental_stable_impl(ctx):
    out = ctx.actions.declare_output("incremental_stable.out")
    ctx.actions.run(
        [
            "sh",
            "-c",
            "printf 'stable\\n' > \"$1\"",
            "--",
            out.as_output(),
        ],
        category = "persistent_incremental_stable",
        local_only = True,
        no_outputs_cleanup = True,
    )
    return [DefaultInfo(default_output = out)]

incremental_stable = rule(impl = _incremental_stable_impl, attrs = {})

def _incremental_downstream_impl(ctx):
    out = ctx.actions.declare_output("incremental_downstream.out")
    input = ctx.attrs.input[DefaultInfo].default_outputs[0]
    ctx.actions.run(
        [
            "sh",
            "-c",
            "cat \"$1\" > \"$2\"",
            "--",
            input,
            out.as_output(),
        ],
        category = "persistent_incremental_downstream",
        local_only = True,
    )
    return [DefaultInfo(default_output = out)]

incremental_downstream = rule(
    impl = _incremental_downstream_impl,
    attrs = {
        "input": attrs.dep(),
    },
)

def _incremental_dep_file_impl(ctx):
    out = ctx.actions.declare_output("incremental_dep_file.out")
    dep_file = ctx.actions.declare_output("incremental_dep_file.d")
    tag = ctx.actions.artifact_tag()
    ctx.actions.run(
        cmd_args(
            [
                "sh",
                "-c",
                "cat \"$1\" >/dev/null && printf 'used.txt\\n' > \"$2\" && printf x >> \"$3\"",
                "--",
                tag.tag_artifacts(ctx.attrs.used),
                tag.tag_artifacts(dep_file.as_output()),
                out.as_output(),
            ],
            hidden = tag.tag_artifacts(ctx.attrs.unused),
        ),
        category = "persistent_incremental_dep_file",
        dep_files = {"inputs": tag},
        local_only = True,
        no_outputs_cleanup = True,
    )
    return [DefaultInfo(default_output = out)]

incremental_dep_file = rule(
    impl = _incremental_dep_file_impl,
    attrs = {
        "unused": attrs.source(),
        "used": attrs.source(),
    },
)

def _metadata_dep_file_impl(ctx):
    out = ctx.actions.declare_output("metadata_dep_file.out")
    metadata_dep_file = ctx.actions.declare_output("metadata_dep_file.metadata.d")
    metadata_tag = ctx.actions.artifact_tag()
    ignored_dep_file = ctx.actions.declare_output("metadata_dep_file.ignored.d")
    ignored_tag = ctx.actions.artifact_tag()
    ctx.actions.run(
        cmd_args(
            [
                "sh",
                "-c",
                "cat \"$METADATA\" > \"$4\" && : > \"$2\" && : > \"$3\"",
                "--",
                metadata_tag.tag_artifacts(ctx.attrs.metadata_input),
                metadata_tag.tag_artifacts(metadata_dep_file.as_output()),
                ignored_tag.tag_artifacts(ignored_dep_file.as_output()),
                out.as_output(),
            ],
            hidden = ignored_tag.tag_artifacts(ctx.attrs.unused),
        ),
        category = "persistent_metadata_dep_file",
        allow_dep_file_cache_upload = True,
        dep_files = {
            "metadata": metadata_tag,
            "ignored": ignored_tag,
        },
        incremental_metadata_ignore_tags = [ignored_tag],
        metadata_env_var = "METADATA",
        metadata_path = "metadata.json",
    )
    return [DefaultInfo(default_output = out)]

metadata_dep_file = rule(
    impl = _metadata_dep_file_impl,
    attrs = {
        "metadata_input": attrs.source(),
        "unused": attrs.source(),
    },
)

def _dep_file_impl(ctx):
    out = ctx.actions.declare_output("dep_file.out")
    dep_file = ctx.actions.declare_output("dep_file.d")
    tag = ctx.actions.artifact_tag()
    ctx.actions.run(
        cmd_args(
            [
                "sh",
                "-c",
                "cat \"$1\" > \"$3\" && printf 'used.txt\\n' > \"$2\"",
                "--",
                tag.tag_artifacts(ctx.attrs.used),
                tag.tag_artifacts(dep_file.as_output()),
                out.as_output(),
            ],
            hidden = tag.tag_artifacts(ctx.attrs.unused),
        ),
        category = "persistent_dep_file",
        dep_files = {"inputs": tag},
        allow_dep_file_cache_upload = True,
    )
    return [DefaultInfo(default_output = out)]

dep_file = rule(
    impl = _dep_file_impl,
    attrs = {
        "unused": attrs.source(),
        "used": attrs.source(),
    },
)
