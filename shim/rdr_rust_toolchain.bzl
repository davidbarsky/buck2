load("@prelude//rust:rust_toolchain.bzl", "PanicRuntime", "RustToolchainInfo")
load("//lib:shell.bzl", "shell")

def _rdr_rust_toolchain_impl(ctx):
    toolchain = ctx.attrs.toolchain
    return [
        DefaultInfo(),
        RustToolchainInfo(
            advanced_unstable_linking = True,
            clippy_driver = RunInfo(args = ["clippy-driver"]),
            compiler = RunInfo(args = [toolchain.project("bin/rustc")]),
            default_edition = ctx.attrs.default_edition,
            panic_runtime = PanicRuntime("unwind"),
            rustc_target_triple = ctx.attrs.rustc_target_triple,
            rustdoc = RunInfo(args = ["rustdoc"]),
            sysroot_path = toolchain,
        ),
    ]

_rdr_rust_toolchain = rule(
    impl = _rdr_rust_toolchain_impl,
    attrs = {
        "default_edition": attrs.string(),
        "rustc_target_triple": attrs.string(),
        "toolchain": attrs.source(),
    },
    is_toolchain_rule = True,
)

def rdr_rust_toolchain(
        name,
        default_edition,
        visibility = None):
    toolchain_root = read_root_config("rust", "rdr_toolchain_root")
    if toolchain_root == None:
        fail("Set `rust.rdr_toolchain_root` to an RDR compiler distribution")

    toolchain_target = name + "_rdr_dist"
    toolchain_root = shell.quote(toolchain_root)
    native.genrule(
        name = toolchain_target,
        cacheable = False,
        out = "toolchain",
        cmd = "mkdir -p $OUT/bin && cp {0}/bin/rustc $OUT/bin/rustc && cp -R {0}/lib $OUT/lib && test -x $OUT/bin/rustc".format(toolchain_root),
        labels = ["buck2_test_local_exec"],
    )
    _rdr_rust_toolchain(
        name = name,
        default_edition = default_edition,
        rustc_target_triple = read_root_config("rust", "rdr_target_triple", "aarch64-apple-darwin"),
        toolchain = ":" + toolchain_target,
        visibility = visibility,
    )
