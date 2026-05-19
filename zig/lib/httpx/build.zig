const std = @import("std");
const builtin = @import("builtin");

fn linkPlatformLibs(compile: *std.Build.Step.Compile, target: std.Build.ResolvedTarget) void {
    // libc is needed on all platforms: std.Thread.Mutex uses pthreads,
    // and timing/sleep use C functions (nanosleep, mach_absolute_time).
    compile.root_module.link_libc = true;
    if (target.result.os.tag == .windows) {
        // Winsock symbols are provided by these system libraries on Windows.
        compile.root_module.linkSystemLibrary("ws2_32", .{});
        compile.root_module.linkSystemLibrary("mswsock", .{});
    }
}

/// Build configuration for httpx.zig - Production-ready HTTP library for Zig
/// Supports HTTP/1.1, HTTP/2, HTTP/3 with TLS, connection pooling, and more.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const httpx_module = b.createModule(.{
        .root_source_file = b.path("src/httpx.zig"),
    });

    _ = b.addModule("httpx", .{
        .root_source_file = b.path("src/httpx.zig"),
    });

    const examples = [_]struct { name: []const u8, path: []const u8, skip_run_all: bool = false }{
        .{ .name = "simple_get", .path = "examples/simple_get.zig" },
        .{ .name = "simple_get_deserialize", .path = "examples/simple_get_deserialize.zig", .skip_run_all = true },
        .{ .name = "post_json", .path = "examples/post_json.zig" },
        .{ .name = "concurrent_requests", .path = "examples/concurrent_requests.zig" },
        .{ .name = "custom_headers", .path = "examples/custom_headers.zig" },
        .{ .name = "udp_local", .path = "examples/udp_local.zig" },
        .{ .name = "simple_server", .path = "examples/simple_server.zig", .skip_run_all = true },
        .{ .name = "router_example", .path = "examples/router_example.zig", .skip_run_all = true },
        .{ .name = "middleware_example", .path = "examples/middleware_example.zig", .skip_run_all = true },
        .{ .name = "streaming", .path = "examples/streaming.zig" },
        .{ .name = "interceptors", .path = "examples/interceptors.zig" },
        .{ .name = "connection_pool", .path = "examples/connection_pool.zig" },
        .{ .name = "cookies_demo", .path = "examples/cookies_demo.zig" },
        .{ .name = "simplified_api_aliases", .path = "examples/simplified_api_aliases.zig", .skip_run_all = true },
        .{ .name = "static_files", .path = "examples/static_files.zig", .skip_run_all = true },
        .{ .name = "multi_page_website", .path = "examples/multi_page_website.zig", .skip_run_all = true },
        .{ .name = "http2_example", .path = "examples/http2_example.zig" },
        .{ .name = "http3_example", .path = "examples/http3_example.zig" },
    };

    const run_all_examples = b.step("run-all-examples", "Run all examples sequentially");
    var previous_run_step: ?*std.Build.Step = null;

    inline for (examples) |example| {
        const exe = b.addExecutable(.{
            .name = example.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(example.path),
                .target = target,
                .optimize = optimize,
            }),
        });
        exe.root_module.addImport("httpx", httpx_module);
        linkPlatformLibs(exe, target);

        const install_exe = b.addInstallArtifact(exe, .{});
        const example_step = b.step("example-" ++ example.name, "Build " ++ example.name ++ " example");
        example_step.dependOn(&install_exe.step);

        const run_exe = b.addRunArtifact(exe);
        run_exe.step.dependOn(&install_exe.step);
        const run_step = b.step("run-" ++ example.name, "Run " ++ example.name ++ " example");
        run_step.dependOn(&run_exe.step);

        // Chain non-skipped examples into the run-all-examples step,
        // reusing the same executable (no duplicate compilation).
        if (!example.skip_run_all) {
            if (previous_run_step) |prev| {
                run_exe.step.dependOn(prev);
            }
            previous_run_step = &run_exe.step;
        }
    }

    if (previous_run_step) |last| {
        run_all_examples.dependOn(last);
    }

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/httpx.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    linkPlatformLibs(tests, target);

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");

    // Only run tests when target matches host; otherwise build test artifact only.
    if (target.result.os.tag == builtin.os.tag and target.result.cpu.arch == builtin.cpu.arch) {
        test_step.dependOn(&run_tests.step);
    } else {
        const install_tests = b.addInstallArtifact(tests, .{});
        test_step.dependOn(&install_tests.step);
    }

    const bench_exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/main.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    bench_exe.root_module.addImport("httpx", httpx_module);
    linkPlatformLibs(bench_exe, target);

    const install_bench = b.addInstallArtifact(bench_exe, .{});
    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(&install_bench.step);

    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    const bench_conc_exe = b.addExecutable(.{
        .name = "bench-concurrency",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/concurrency.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    bench_conc_exe.root_module.addImport("httpx", httpx_module);
    linkPlatformLibs(bench_conc_exe, target);

    const install_bench_conc = b.addInstallArtifact(bench_conc_exe, .{});
    const run_bench_conc = b.addRunArtifact(bench_conc_exe);
    run_bench_conc.step.dependOn(&install_bench_conc.step);

    const bench_conc_step = b.step("bench-concurrency", "Run concurrency benchmarks");
    bench_conc_step.dependOn(&run_bench_conc.step);

    // Cross-compilation targets to verify support
    const cross_targets = [_]std.Target.Query{
        .{ .cpu_arch = .x86_64, .os_tag = .linux },
        .{ .cpu_arch = .aarch64, .os_tag = .linux },
        .{ .cpu_arch = .x86, .os_tag = .linux },
        .{ .cpu_arch = .x86_64, .os_tag = .windows },
        .{ .cpu_arch = .aarch64, .os_tag = .windows },
        .{ .cpu_arch = .x86, .os_tag = .windows },
        .{ .cpu_arch = .x86_64, .os_tag = .macos },
        .{ .cpu_arch = .aarch64, .os_tag = .macos },
    };

    const build_all_step = b.step("build-all-targets", "Build library for all supported targets");

    for (cross_targets) |t| {
        const target_cross = b.resolveTargetQuery(t);
        const lib_cross = b.addLibrary(.{
            .name = "httpx",
            .linkage = .static,
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/httpx.zig"),
                .target = target_cross,
                .optimize = optimize,
            }),
        });
        linkPlatformLibs(lib_cross, target_cross);

        // Just build the artifact to verify it compiles
        build_all_step.dependOn(&lib_cross.step);
    }

    const lib = b.addLibrary(.{
        .name = "httpx",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/httpx.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    linkPlatformLibs(lib, target);

    b.installArtifact(lib);

    const docs_step = b.step("docs", "Generate library documentation");
    const install_docs = b.addInstallDirectory(.{
        .source_dir = lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    docs_step.dependOn(&install_docs.step);

    const test_all_step = b.step("test-all", "Run tests, benchmarks, and all runnable examples");
    test_all_step.dependOn(test_step);
    test_all_step.dependOn(bench_step);
    test_all_step.dependOn(run_all_examples);
}
