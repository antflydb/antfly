// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

pub const split_bench_root = "pkg/antfly/src/split_bench_root.zig";
pub const wal_bench_root = "pkg/antfly/src/wal_bench_root.zig";
pub const derived_log_bench_root = "pkg/antfly/src/derived_log_bench_root.zig";
pub const replay_bench_root = "pkg/antfly/src/replay_bench_root.zig";
pub const algebraic_bench_root = "pkg/antfly/src/algebraic_bench_root.zig";

const std = @import("std");

pub const BenchSteps = struct {
    recall_harness: *std.Build.Step.Compile,
};

pub fn addLibraryBenchSteps(ctx: anytype) void {
    const b = ctx.b;
    const target = ctx.target;
    const lib_image_spng_paths = ctx.lib_image_spng_paths;
    const lib_image_enable_spng = ctx.lib_image_enable_spng;
    const inference_linalg_mod = ctx.inference_linalg_mod;
    const inference_audio_mod = ctx.inference_audio_mod;

    const linalg_bench = b.addExecutable(.{
        .name = "lib-linalg-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/lib/linalg_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    linalg_bench.root_module.addImport("inference_linalg", inference_linalg_mod);
    const run_linalg_bench = b.addRunArtifact(linalg_bench);
    if (b.args) |args| {
        run_linalg_bench.addArgs(args);
    }
    const linalg_bench_step = b.step("bench-linalg", "Run lib/linalg benchmarks");
    linalg_bench_step.dependOn(&run_linalg_bench.step);

    const audio_bench = b.addExecutable(.{
        .name = "lib-audio-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/lib/audio_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    audio_bench.root_module.addImport("inference_audio", inference_audio_mod);
    audio_bench.root_module.link_libc = true;
    const run_audio_bench = b.addRunArtifact(audio_bench);
    if (b.args) |args| {
        run_audio_bench.addArgs(args);
    }
    const audio_bench_step = b.step("bench-audio", "Run lib/audio decode and synthesis benchmarks");
    audio_bench_step.dependOn(&run_audio_bench.step);

    const lib_image_bench_build_options = b.addOptions();
    lib_image_bench_build_options.addOption(bool, "enable_spng", lib_image_enable_spng);
    const lib_image_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/image_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lib_image_bench_mod.addOptions("build_options", lib_image_bench_build_options);
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_bench_mod.addIncludePath(.{ .cwd_relative = spng_paths.include_dir });
    }
    const lib_image_bench = b.addExecutable(.{
        .name = "lib-image-bench",
        .root_module = lib_image_bench_mod,
    });
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_bench.root_module.addLibraryPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_bench.root_module.addRPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_bench.root_module.linkSystemLibrary("spng", .{});
        lib_image_bench.root_module.link_libc = true;
    }
    const run_lib_image_bench = b.addRunArtifact(lib_image_bench);
    if (b.args) |args| {
        run_lib_image_bench.addArgs(args);
    } else {
        run_lib_image_bench.addArgs(&.{
            "image-decode-suite",
            "25",
        });
    }
    const lib_image_bench_step = b.step("lib-image-bench", "Run lib/image decode benchmarks");
    lib_image_bench_step.dependOn(&run_lib_image_bench.step);

    const bench_image_step = b.step("bench-image", "Run lib/image decode benchmarks");
    bench_image_step.dependOn(&run_lib_image_bench.step);

    const pdf_bench_image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const pdf_bench_font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const pdf_bench_pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    pdf_bench_pdf_mod.addImport("antfly_image", pdf_bench_image_mod);
    pdf_bench_pdf_mod.addImport("antfly_font", pdf_bench_font_mod);
    const pdf_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/pdf_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    pdf_bench_mod.addImport("antfly_pdf", pdf_bench_pdf_mod);
    const lib_pdf_bench = b.addExecutable(.{
        .name = "lib-pdf-bench",
        .root_module = pdf_bench_mod,
    });
    const run_lib_pdf_bench = b.addRunArtifact(lib_pdf_bench);
    if (b.args) |args| {
        run_lib_pdf_bench.addArgs(args);
    } else {
        run_lib_pdf_bench.addArgs(&.{
            "suite",
            "lib/pdf/testdata/simple_text_fixture.pdf",
            "25",
        });
    }
    const lib_pdf_bench_step = b.step("lib-pdf-bench", "Run lib/pdf benchmarks");
    lib_pdf_bench_step.dependOn(&run_lib_pdf_bench.step);

    const bench_pdf_step = b.step("bench-pdf", "Run lib/pdf benchmarks");
    bench_pdf_step.dependOn(&run_lib_pdf_bench.step);
}

pub fn addBenchSteps(ctx: anytype) BenchSteps {
    const b = ctx.b;
    const target = ctx.target;
    const optimize = ctx.optimize;
    const lmdb_backend = ctx.lmdb_backend;
    const lmdb_evented_async_io = ctx.lmdb_evented_async_io;
    const lite_local_inference_runtime = ctx.lite_local_inference_runtime;
    const antfly_version = ctx.antfly_version;
    const with_tla = ctx.with_tla;
    const lib_mod = ctx.lib_mod;
    const platform_mod = ctx.platform_mod;
    const lmdb_engine_mod = ctx.lmdb_engine_mod;
    const json_mod = ctx.json_mod;
    const indexes_openapi_mod = ctx.indexes_openapi_mod;
    const metadata_openapi_mod = ctx.metadata_openapi_mod;
    const query_openapi_mod = ctx.query_openapi_mod;
    const bloom_mod = ctx.bloom_mod;
    const vector_mod = ctx.vector_mod;
    const vectorindex_mod = ctx.vectorindex_mod;
    const matcher_mod = ctx.matcher_mod;
    const resolver_mod = ctx.resolver_mod;
    const vellum_mod = ctx.vellum_mod;
    const regex_mod = ctx.regex_mod;
    const reranking_mod = ctx.reranking_mod;
    const scraping_mod = ctx.scraping_mod;
    const raft_engine_mod = ctx.raft_engine_mod;
    const run_lib_ha_compat_tests = ctx.run_lib_ha_compat_tests;
    const makeLmdbBuildOptions = ctx.makeLmdbBuildOptions;
    const makeRootBuildOptions = ctx.makeRootBuildOptions;
    const makeLmdbEngineModule = ctx.makeLmdbEngineModule;
    const makeLmdbModule = ctx.makeLmdbModule;
    const addSnowballModule = ctx.addSnowballModule;

    const lmdb_bench_engine_options_c = makeLmdbBuildOptions(b, .c, false, false);
    const lmdb_bench_build_options_c = makeRootBuildOptions(b, .c, false, false, false, true, false, lite_local_inference_runtime, antfly_version);
    const lmdb_bench_engine_mod_c = makeLmdbEngineModule(b, target, .ReleaseFast, true, lmdb_bench_engine_options_c);
    const lmdb_bench_wrapper_mod_c = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, .ReleaseFast, lmdb_bench_build_options_c, lmdb_bench_engine_mod_c, platform_mod);
    const lmdb_bench_mod_c = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lmdb_bench_mod_c.addImport("lmdb", lmdb_bench_wrapper_mod_c);
    lmdb_bench_mod_c.addImport("lmdb_engine", lmdb_bench_engine_mod_c);

    const lmdb_bench_c = b.addExecutable(.{
        .name = "lmdb_bench_c",
        .root_module = lmdb_bench_mod_c,
    });

    const lmdb_bench_engine_options_zig = makeLmdbBuildOptions(b, .zig, lmdb_evented_async_io, false);
    const lmdb_bench_build_options_zig = makeRootBuildOptions(b, .zig, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
    const lmdb_bench_engine_mod_zig = makeLmdbEngineModule(b, target, .ReleaseFast, true, lmdb_bench_engine_options_zig);
    const lmdb_bench_wrapper_mod_zig = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, .ReleaseFast, lmdb_bench_build_options_zig, lmdb_bench_engine_mod_zig, platform_mod);
    const lmdb_bench_mod_zig = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lmdb_bench_mod_zig.addImport("lmdb", lmdb_bench_wrapper_mod_zig);
    lmdb_bench_mod_zig.addImport("lmdb_engine", lmdb_bench_engine_mod_zig);

    const lmdb_bench_zig = b.addExecutable(.{
        .name = "lmdb_bench_zig",
        .root_module = lmdb_bench_mod_zig,
    });

    const run_lmdb_bench_c = b.addRunArtifact(lmdb_bench_c);
    run_lmdb_bench_c.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const run_lmdb_bench_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const lmdb_bench_step = b.step("lmdb-bench", "Compare LMDB wrapper benchmarks on c and zig backends");
    lmdb_bench_step.dependOn(&run_lmdb_bench_c.step);
    lmdb_bench_step.dependOn(&run_lmdb_bench_zig.step);

    const run_lmdb_bench_worker_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_worker_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--worker-thread" });
    const lmdb_bench_worker_step = b.step("lmdb-bench-worker", "Run LMDB wrapper benchmarks on zig worker-thread commit backend");
    lmdb_bench_worker_step.dependOn(&run_lmdb_bench_worker_zig.step);

    const run_lmdb_bench_async_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_async_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--async-io" });
    const lmdb_bench_async_step = b.step("lmdb-bench-async", "Run LMDB wrapper benchmarks on zig async-io commit backend");
    lmdb_bench_async_step.dependOn(&run_lmdb_bench_async_zig.step);

    const run_lmdb_bench_adaptive_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_adaptive_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--adaptive" });
    const lmdb_bench_adaptive_step = b.step("lmdb-bench-adaptive", "Run LMDB wrapper benchmarks on zig adaptive commit backend");
    lmdb_bench_adaptive_step.dependOn(&run_lmdb_bench_adaptive_zig.step);

    const run_lmdb_bench_repeat_c = b.addRunArtifact(lmdb_bench_c);
    run_lmdb_bench_repeat_c.addArgs(&.{ "--samples", "5", "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const run_lmdb_bench_repeat_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_repeat_zig.addArgs(&.{ "--samples", "5", "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const lmdb_bench_repeat_step = b.step("lmdb-bench-repeat", "Repeat LMDB wrapper benchmarks on c and zig backends");
    lmdb_bench_repeat_step.dependOn(&run_lmdb_bench_repeat_c.step);
    lmdb_bench_repeat_step.dependOn(&run_lmdb_bench_repeat_zig.step);

    const run_lmdb_bench_zig_mmap = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_zig_mmap.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--write-map", "--map-async" });

    const lmdb_bench_mmap_step = b.step("lmdb-bench-mmap", "Run LMDB wrapper benchmarks on zig mmap modes");
    lmdb_bench_mmap_step.dependOn(&run_lmdb_bench_zig_mmap.step);

    const split_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const split_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
    const split_bench_engine_mod = makeLmdbEngineModule(b, target, .ReleaseFast, true, split_bench_engine_options);
    const split_bench_root_mod = makeLmdbModule(b, split_bench_root, target, .ReleaseFast, split_bench_build_options, split_bench_engine_mod, platform_mod);
    const split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    split_bench_mod.addImport("split_storage", split_bench_root_mod);

    const split_bench = b.addExecutable(.{
        .name = "split_bench",
        .root_module = split_bench_mod,
    });

    const run_split_bench = b.addRunArtifact(split_bench);
    const split_bench_step = b.step("split-bench", "Benchmark median-key selection and split range copy");
    split_bench_step.dependOn(&run_split_bench.step);

    const run_split_bench_repeat = b.addRunArtifact(split_bench);
    run_split_bench_repeat.addArgs(&.{ "--samples", "5" });
    const split_bench_repeat_step = b.step("split-bench-repeat", "Benchmark median-key selection and split range copy with repeated samples");
    split_bench_repeat_step.dependOn(&run_split_bench_repeat.step);

    const db_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/db_split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    db_split_bench_mod.addImport("antfly-zig", lib_mod);

    const db_split_bench = b.addExecutable(.{
        .name = "db_split_bench",
        .root_module = db_split_bench_mod,
    });

    const run_db_split_bench = b.addRunArtifact(db_split_bench);
    const db_split_bench_step = b.step("db-split-bench", "Benchmark DB split preparation old vs current");
    db_split_bench_step.dependOn(&run_db_split_bench.step);

    const run_db_split_bench_repeat = b.addRunArtifact(db_split_bench);
    run_db_split_bench_repeat.addArgs(&.{ "--samples", "5" });
    const db_split_bench_repeat_step = b.step("db-split-bench-repeat", "Benchmark DB split preparation old vs current with repeated samples");
    db_split_bench_repeat_step.dependOn(&run_db_split_bench_repeat.step);

    const docid_doc_set_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/docid_doc_set_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const docid_doc_set_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/docid_doc_set_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    docid_doc_set_bench_mod.addImport("docid_doc_set_bench_root", docid_doc_set_bench_root_mod);

    const docid_doc_set_bench = b.addExecutable(.{
        .name = "docid_doc_set_bench",
        .root_module = docid_doc_set_bench_mod,
    });

    const run_docid_doc_set_bench = b.addRunArtifact(docid_doc_set_bench);
    run_docid_doc_set_bench.addArgs(&.{ "--samples", "1", "--repeats", "16", "--small", "32", "--medium", "1024", "--large", "16384" });

    const backend_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/backend_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    backend_bench_mod.addImport("antfly_zig", lib_mod);
    const backend_bench = b.addExecutable(.{
        .name = "backend_bench",
        .root_module = backend_bench_mod,
    });

    const run_backend_bench = b.addRunArtifact(backend_bench);
    if (b.args) |args| {
        run_backend_bench.addArgs(args);
    } else {
        run_backend_bench.addArgs(&.{ "--samples", "3", "--keys", "20000", "--value-size", "128", "--hit-repeats", "3", "--miss-repeats", "3", "--scan-repeats", "5" });
    }
    const backend_bench_step = b.step("backend-bench", "Benchmark shared backend workloads across LMDB and LSM backends");
    backend_bench_step.dependOn(&run_backend_bench.step);

    const lsm_backend_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_backend_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lsm_backend_bench_mod.addImport("antfly_zig", lib_mod);
    const lsm_backend_bench = b.addExecutable(.{
        .name = "lsm_backend_bench",
        .root_module = lsm_backend_bench_mod,
    });

    const run_lsm_backend_bench = b.addRunArtifact(lsm_backend_bench);
    if (b.args) |args| {
        run_lsm_backend_bench.addArgs(args);
    } else {
        run_lsm_backend_bench.addArgs(&.{
            "--samples",            "3",
            "--keys",               "20000",
            "--value-size",         "128",
            "--hit-repeats",        "5",
            "--miss-repeats",       "5",
            "--short-scan-len",     "64",
            "--short-scan-repeats", "16",
            "--full-scan-repeats",  "5",
            "--reopen-repeats",     "5",
            "--mixed-repeats",      "3",
            "--storage",            "host",
            "--cache",              "both",
        });
    }
    const lsm_backend_bench_step = b.step("lsm-backend-bench", "Benchmark LSM read and scan paths with optional cache and storage instrumentation");
    lsm_backend_bench_step.dependOn(&run_lsm_backend_bench.step);

    const hbc_storage_read_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_storage_read_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_storage_read_bench_mod.addImport("antfly-zig", lib_mod);
    const hbc_storage_read_bench = b.addExecutable(.{
        .name = "hbc_storage_read_bench",
        .root_module = hbc_storage_read_bench_mod,
    });

    const run_hbc_storage_read_bench = b.addRunArtifact(hbc_storage_read_bench);
    if (b.args) |args| {
        run_hbc_storage_read_bench.addArgs(args);
    } else {
        run_hbc_storage_read_bench.addArgs(&.{
            "--docs",       "75000",
            "--dims",       "512",
            "--queries",    "1000",
            "--candidates", "800",
        });
    }
    const hbc_storage_read_bench_build_step = b.step("hbc-storage-read-bench-build", "Build the HBC-shaped LSM hot-read benchmark");
    hbc_storage_read_bench_build_step.dependOn(&hbc_storage_read_bench.step);
    const hbc_storage_read_bench_step = b.step("hbc-storage-read-bench", "Benchmark HBC-shaped metadata/vector artifact reads through the LSM");
    hbc_storage_read_bench_step.dependOn(&run_hbc_storage_read_bench.step);

    const lsm_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const lsm_write_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lsm_write_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lsm_write_bench_root_mod.addImport("bloom", bloom_mod);
    lsm_write_bench_root_mod.addImport("antfly_platform", platform_mod);
    lsm_write_bench_mod.addImport("antfly_zig", lsm_write_bench_root_mod);
    const lsm_write_bench = b.addExecutable(.{
        .name = "lsm_write_bench",
        .root_module = lsm_write_bench_mod,
    });

    const run_lsm_write_bench = b.addRunArtifact(lsm_write_bench);
    if (b.args) |args| {
        run_lsm_write_bench.addArgs(args);
    } else {
        run_lsm_write_bench.addArgs(&.{
            "--samples",          "3",
            "--keys",             "20000",
            "--hot-keys",         "1000",
            "--overwrite-rounds", "20",
            "--value-size",       "128",
            "--batch-size",       "1000",
            "--storage",          "host",
            "--mode",             "both",
        });
    }
    const lsm_write_bench_step = b.step("lsm-write-bench", "Benchmark LSM write amplification across sorted, random, overwrite, and delete workloads");
    lsm_write_bench_step.dependOn(&run_lsm_write_bench.step);

    const lsm_write_bench_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_write_bench_compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const lsm_write_bench_compare = b.addExecutable(.{
        .name = "lsm_write_bench_compare",
        .root_module = lsm_write_bench_compare_mod,
    });

    const run_lsm_write_bench_compare = b.addRunArtifact(lsm_write_bench_compare);
    if (b.args) |args| {
        run_lsm_write_bench_compare.addArgs(args);
    } else {
        run_lsm_write_bench_compare.addArgs(&.{
            "--before",
            "/tmp/lsm-write-before.jsonl",
            "--after",
            "/tmp/lsm-write-after.jsonl",
        });
    }
    const lsm_write_bench_compare_step = b.step("lsm-write-bench-compare", "Compare two LSM write bench JSONL outputs by scenario and workload");
    lsm_write_bench_compare_step.dependOn(&run_lsm_write_bench_compare.step);

    const text_segment_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/text_segment_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const text_segment_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/text_segment_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    text_segment_bench_root_mod.addImport("bloom", bloom_mod);
    text_segment_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    text_segment_bench_root_mod.addImport("antfly_platform", platform_mod);
    text_segment_write_bench_mod.addImport("antfly_text_bench", text_segment_bench_root_mod);
    const text_segment_write_bench = b.addExecutable(.{
        .name = "text_segment_write_bench",
        .root_module = text_segment_write_bench_mod,
    });

    const run_text_segment_write_bench = b.addRunArtifact(text_segment_write_bench);
    if (b.args) |args| {
        run_text_segment_write_bench.addArgs(args);
    } else {
        run_text_segment_write_bench.addArgs(&.{
            "--samples",       "3",
            "--docs",          "20000",
            "--batch-size",    "1000",
            "--terms-per-doc", "12",
            "--merge-width",   "8",
            "--storage",       "host",
        });
    }
    const text_segment_write_bench_step = b.step("text-segment-write-bench", "Benchmark full-text segment build, on-disk publish, merge, and force-merge");
    text_segment_write_bench_step.dependOn(&run_text_segment_write_bench.step);

    const lsm_backend_bench_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_backend_bench_compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const lsm_backend_bench_compare = b.addExecutable(.{
        .name = "lsm_backend_bench_compare",
        .root_module = lsm_backend_bench_compare_mod,
    });

    const run_lsm_backend_bench_compare = b.addRunArtifact(lsm_backend_bench_compare);
    if (b.args) |args| {
        run_lsm_backend_bench_compare.addArgs(args);
    } else {
        run_lsm_backend_bench_compare.addArgs(&.{
            "--before",
            "/tmp/lsm-before.jsonl",
            "--after",
            "/tmp/lsm-after.jsonl",
        });
    }
    const lsm_backend_bench_compare_step = b.step("lsm-backend-bench-compare", "Compare two LSM backend bench JSONL outputs by scenario and workload");
    lsm_backend_bench_compare_step.dependOn(&run_lsm_backend_bench_compare.step);

    const regex_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/regex/bench/regex_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    regex_bench_mod.addImport("antfly_regex", regex_mod);
    regex_bench_mod.addImport("antfly_vellum", vellum_mod);
    const regex_bench = b.addExecutable(.{
        .name = "regex_bench",
        .root_module = regex_bench_mod,
    });

    const run_regex_bench = b.addRunArtifact(regex_bench);
    if (b.args) |args| {
        run_regex_bench.addArgs(args);
    }
    const regex_bench_step = b.step("regex-bench", "Benchmark regex haystack matching and vellum automaton traversal");
    regex_bench_step.dependOn(&run_regex_bench.step);

    const sql_parser_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql_parser_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    sql_parser_bench_mod.addImport("bloom", bloom_mod);
    sql_parser_bench_mod.addImport("antfly_platform", platform_mod);
    sql_parser_bench_mod.addImport("antfly_regex", regex_mod);
    sql_parser_bench_mod.addImport("antfly-json", json_mod);
    sql_parser_bench_mod.addImport("antfly_reranking", reranking_mod);
    sql_parser_bench_mod.addImport("antfly_vector", vector_mod);
    sql_parser_bench_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    sql_parser_bench_mod.addImport("antfly_metadata_openapi", metadata_openapi_mod);
    sql_parser_bench_mod.addImport("antfly_query_openapi", query_openapi_mod);
    const sql_parser_bench = b.addExecutable(.{
        .name = "sql_parser_bench",
        .root_module = sql_parser_bench_mod,
    });
    const run_sql_parser_bench = b.addRunArtifact(sql_parser_bench);
    if (b.args) |args| {
        run_sql_parser_bench.addArgs(args);
    }
    const sql_parser_bench_step = b.step("sql-parser-bench", "Benchmark generated SQL parser corpus throughput");
    sql_parser_bench_step.dependOn(&run_sql_parser_bench.step);

    const sql_parser_fuzz_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql/parser_fuzz.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const sql_parser_fuzz = b.addExecutable(.{
        .name = "sql_parser_fuzz",
        .root_module = sql_parser_fuzz_mod,
    });
    const run_sql_parser_fuzz = b.addRunArtifact(sql_parser_fuzz);
    if (b.args) |args| {
        run_sql_parser_fuzz.addArgs(args);
    }
    const sql_parser_fuzz_step = b.step("sql-parser-fuzz", "Run deterministic generated SQL parser fuzzing");
    sql_parser_fuzz_step.dependOn(&run_sql_parser_fuzz.step);

    const wal_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const wal_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
    const wal_bench_engine_mod = makeLmdbEngineModule(b, target, .ReleaseFast, true, wal_bench_engine_options);
    const wal_bench_wal_mod = makeLmdbModule(b, wal_bench_root, target, .ReleaseFast, wal_bench_build_options, wal_bench_engine_mod, platform_mod);
    const wal_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/wal_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    wal_bench_mod.addImport("wal", wal_bench_wal_mod);
    wal_bench_wal_mod.addImport("bloom", bloom_mod);

    const wal_bench = b.addExecutable(.{
        .name = "wal_bench",
        .root_module = wal_bench_mod,
    });

    const run_wal_bench = b.addRunArtifact(wal_bench);
    const wal_bench_step = b.step("wal-bench", "Benchmark WAL append throughput with and without group commit");
    wal_bench_step.dependOn(&run_wal_bench.step);

    const run_wal_bench_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_repeat.addArgs(&.{ "--samples", "5" });
    const wal_bench_repeat_step = b.step("wal-bench-repeat", "Benchmark WAL append throughput with repeated samples");
    wal_bench_repeat_step.dependOn(&run_wal_bench_repeat.step);

    const run_wal_bench_repeat_long = b.addRunArtifact(wal_bench);
    run_wal_bench_repeat_long.addArgs(&.{ "--samples", "15" });
    const wal_bench_repeat_long_step = b.step("wal-bench-repeat-long", "Benchmark WAL sync backend with 15 repeated samples");
    wal_bench_repeat_long_step.dependOn(&run_wal_bench_repeat_long.step);

    const run_wal_bench_repeat_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_repeat_stress.addArgs(&.{ "--samples", "5", "--sync-delay-us", "2000" });
    const wal_bench_repeat_stress_step = b.step("wal-bench-repeat-stress", "Benchmark WAL sync backend with repeated stressed samples");
    wal_bench_repeat_stress_step.dependOn(&run_wal_bench_repeat_stress.step);

    const run_wal_bench_worker = b.addRunArtifact(wal_bench);
    run_wal_bench_worker.addArg("--worker-thread");
    const wal_bench_worker_step = b.step("wal-bench-worker", "Benchmark WAL append throughput with worker-thread commit backend");
    wal_bench_worker_step.dependOn(&run_wal_bench_worker.step);

    const run_wal_bench_worker_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_worker_repeat.addArgs(&.{ "--samples", "5", "--worker-thread" });
    const wal_bench_worker_repeat_step = b.step("wal-bench-worker-repeat", "Benchmark WAL append throughput with repeated worker-thread samples");
    wal_bench_worker_repeat_step.dependOn(&run_wal_bench_worker_repeat.step);

    const run_wal_bench_worker_repeat_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_worker_repeat_stress.addArgs(&.{ "--samples", "5", "--worker-thread", "--sync-delay-us", "2000" });
    const wal_bench_worker_repeat_stress_step = b.step("wal-bench-worker-repeat-stress", "Benchmark WAL worker-thread backend with repeated stressed samples");
    wal_bench_worker_repeat_stress_step.dependOn(&run_wal_bench_worker_repeat_stress.step);

    const run_wal_bench_async = b.addRunArtifact(wal_bench);
    run_wal_bench_async.addArg("--async-io");
    const wal_bench_async_step = b.step("wal-bench-async", "Benchmark WAL append throughput with async-io commit backend");
    wal_bench_async_step.dependOn(&run_wal_bench_async.step);

    const run_wal_bench_async_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_async_repeat.addArgs(&.{ "--samples", "5", "--async-io" });
    const wal_bench_async_repeat_step = b.step("wal-bench-async-repeat", "Benchmark WAL append throughput with repeated async-io samples");
    wal_bench_async_repeat_step.dependOn(&run_wal_bench_async_repeat.step);

    const run_wal_bench_async_repeat_long = b.addRunArtifact(wal_bench);
    run_wal_bench_async_repeat_long.addArgs(&.{ "--samples", "15", "--async-io" });
    const wal_bench_async_repeat_long_step = b.step("wal-bench-async-repeat-long", "Benchmark WAL append throughput with 15 async-io samples");
    wal_bench_async_repeat_long_step.dependOn(&run_wal_bench_async_repeat_long.step);

    const run_wal_bench_async_repeat_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_async_repeat_stress.addArgs(&.{ "--samples", "5", "--async-io", "--sync-delay-us", "2000" });
    const wal_bench_async_repeat_stress_step = b.step("wal-bench-async-repeat-stress", "Benchmark WAL async-io backend with repeated stressed samples");
    wal_bench_async_repeat_stress_step.dependOn(&run_wal_bench_async_repeat_stress.step);

    const run_wal_bench_adaptive = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive.addArg("--adaptive");
    const wal_bench_adaptive_step = b.step("wal-bench-adaptive", "Benchmark WAL append throughput with adaptive commit backend");
    wal_bench_adaptive_step.dependOn(&run_wal_bench_adaptive.step);

    const run_wal_bench_adaptive_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive_repeat.addArgs(&.{ "--samples", "5", "--adaptive" });
    const wal_bench_adaptive_repeat_step = b.step("wal-bench-adaptive-repeat", "Benchmark WAL append throughput with repeated adaptive samples");
    wal_bench_adaptive_repeat_step.dependOn(&run_wal_bench_adaptive_repeat.step);

    const run_wal_bench_adaptive_repeat_long = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive_repeat_long.addArgs(&.{ "--samples", "15", "--adaptive" });
    const wal_bench_adaptive_repeat_long_step = b.step("wal-bench-adaptive-repeat-long", "Benchmark WAL append throughput with 15 adaptive samples");
    wal_bench_adaptive_repeat_long_step.dependOn(&run_wal_bench_adaptive_repeat_long.step);

    const run_wal_bench_adaptive_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive_stress.addArgs(&.{ "--samples", "5", "--adaptive", "--sync-delay-us", "2000" });
    const wal_bench_adaptive_stress_step = b.step("wal-bench-adaptive-stress", "Benchmark WAL adaptive backend with artificial sync delay");
    wal_bench_adaptive_stress_step.dependOn(&run_wal_bench_adaptive_stress.step);

    const derived_log_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const derived_log_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
    const derived_log_bench_engine_mod = makeLmdbEngineModule(b, target, .ReleaseFast, true, derived_log_bench_engine_options);
    const derived_log_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(derived_log_bench_root),
        .target = target,
        .optimize = .ReleaseFast,
    });
    derived_log_bench_root_mod.addOptions("build_options", derived_log_bench_build_options);
    derived_log_bench_root_mod.addImport("lmdb_engine", derived_log_bench_engine_mod);
    derived_log_bench_root_mod.addImport("bloom", bloom_mod);
    derived_log_bench_root_mod.addImport("antfly_platform", platform_mod);
    derived_log_bench_root_mod.link_libc = true;
    const derived_log_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/derived_log_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    derived_log_bench_mod.addImport("derived_log", derived_log_bench_root_mod);

    const derived_log_bench = b.addExecutable(.{
        .name = "derived_log_bench",
        .root_module = derived_log_bench_mod,
    });

    const run_derived_log_bench = b.addRunArtifact(derived_log_bench);
    const derived_log_bench_step = b.step("derived-log-bench", "Benchmark derived log throughput with and without group commit");
    derived_log_bench_step.dependOn(&run_derived_log_bench.step);

    const run_derived_log_bench_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_repeat.addArgs(&.{ "--samples", "5" });
    const derived_log_bench_repeat_step = b.step("derived-log-bench-repeat", "Benchmark derived log throughput with repeated samples");
    derived_log_bench_repeat_step.dependOn(&run_derived_log_bench_repeat.step);

    const run_derived_log_bench_repeat_long = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_repeat_long.addArgs(&.{ "--samples", "15" });
    const derived_log_bench_repeat_long_step = b.step("derived-log-bench-repeat-long", "Benchmark derived log sync backend with 15 repeated samples");
    derived_log_bench_repeat_long_step.dependOn(&run_derived_log_bench_repeat_long.step);

    const run_derived_log_bench_repeat_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_repeat_stress.addArgs(&.{ "--samples", "5", "--sync-delay-us", "2000" });
    const derived_log_bench_repeat_stress_step = b.step("derived-log-bench-repeat-stress", "Benchmark derived log sync backend with repeated stressed samples");
    derived_log_bench_repeat_stress_step.dependOn(&run_derived_log_bench_repeat_stress.step);

    const run_derived_log_bench_worker = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_worker.addArg("--worker-thread");
    const derived_log_bench_worker_step = b.step("derived-log-bench-worker", "Benchmark derived log throughput with worker-thread commit backend");
    derived_log_bench_worker_step.dependOn(&run_derived_log_bench_worker.step);

    const run_derived_log_bench_worker_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_worker_repeat.addArgs(&.{ "--samples", "5", "--worker-thread" });
    const derived_log_bench_worker_repeat_step = b.step("derived-log-bench-worker-repeat", "Benchmark derived log throughput with repeated worker-thread samples");
    derived_log_bench_worker_repeat_step.dependOn(&run_derived_log_bench_worker_repeat.step);

    const run_derived_log_bench_worker_repeat_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_worker_repeat_stress.addArgs(&.{ "--samples", "5", "--worker-thread", "--sync-delay-us", "2000" });
    const derived_log_bench_worker_repeat_stress_step = b.step("derived-log-bench-worker-repeat-stress", "Benchmark derived log worker-thread backend with repeated stressed samples");
    derived_log_bench_worker_repeat_stress_step.dependOn(&run_derived_log_bench_worker_repeat_stress.step);

    const run_derived_log_bench_async = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async.addArg("--async-io");
    const derived_log_bench_async_step = b.step("derived-log-bench-async", "Benchmark derived log throughput with async-io commit backend");
    derived_log_bench_async_step.dependOn(&run_derived_log_bench_async.step);

    const run_derived_log_bench_async_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async_repeat.addArgs(&.{ "--samples", "5", "--async-io" });
    const derived_log_bench_async_repeat_step = b.step("derived-log-bench-async-repeat", "Benchmark derived log throughput with repeated async-io samples");
    derived_log_bench_async_repeat_step.dependOn(&run_derived_log_bench_async_repeat.step);

    const run_derived_log_bench_async_repeat_long = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async_repeat_long.addArgs(&.{ "--samples", "15", "--async-io" });
    const derived_log_bench_async_repeat_long_step = b.step("derived-log-bench-async-repeat-long", "Benchmark derived log throughput with 15 async-io samples");
    derived_log_bench_async_repeat_long_step.dependOn(&run_derived_log_bench_async_repeat_long.step);

    const run_derived_log_bench_async_repeat_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async_repeat_stress.addArgs(&.{ "--samples", "5", "--async-io", "--sync-delay-us", "2000" });
    const derived_log_bench_async_repeat_stress_step = b.step("derived-log-bench-async-repeat-stress", "Benchmark derived log async-io backend with repeated stressed samples");
    derived_log_bench_async_repeat_stress_step.dependOn(&run_derived_log_bench_async_repeat_stress.step);

    const run_derived_log_bench_adaptive = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive.addArg("--adaptive");
    const derived_log_bench_adaptive_step = b.step("derived-log-bench-adaptive", "Benchmark derived log throughput with adaptive commit backend");
    derived_log_bench_adaptive_step.dependOn(&run_derived_log_bench_adaptive.step);

    const run_derived_log_bench_adaptive_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive_repeat.addArgs(&.{ "--samples", "5", "--adaptive" });
    const derived_log_bench_adaptive_repeat_step = b.step("derived-log-bench-adaptive-repeat", "Benchmark derived log throughput with repeated adaptive samples");
    derived_log_bench_adaptive_repeat_step.dependOn(&run_derived_log_bench_adaptive_repeat.step);

    const run_derived_log_bench_adaptive_repeat_long = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive_repeat_long.addArgs(&.{ "--samples", "15", "--adaptive" });
    const derived_log_bench_adaptive_repeat_long_step = b.step("derived-log-bench-adaptive-repeat-long", "Benchmark derived log throughput with 15 adaptive samples");
    derived_log_bench_adaptive_repeat_long_step.dependOn(&run_derived_log_bench_adaptive_repeat_long.step);

    const run_derived_log_bench_adaptive_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive_stress.addArgs(&.{ "--samples", "5", "--adaptive", "--sync-delay-us", "2000" });
    const derived_log_bench_adaptive_stress_step = b.step("derived-log-bench-adaptive-stress", "Benchmark derived log adaptive backend with artificial sync delay");
    derived_log_bench_adaptive_stress_step.dependOn(&run_derived_log_bench_adaptive_stress.step);

    const json_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/json/bench/json_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    json_bench_mod.addImport("antfly-json", json_mod);

    const json_bench = b.addExecutable(.{
        .name = "json_bench",
        .root_module = json_bench_mod,
    });

    const run_json_bench = b.addRunArtifact(json_bench);
    if (b.args) |args| {
        run_json_bench.addArgs(args);
    }
    const json_bench_step = b.step("json-bench", "Benchmark std.json vs antfly-json parsing");
    json_bench_step.dependOn(&run_json_bench.step);

    // Benchmark executable
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("antfly-zig", lib_mod);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });

    const run_bench = b.addRunArtifact(bench);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    // Quickstart-shaped benchmark: mirrors the workload of
    // `test_text_quickstart_and_document_artifact` (e2e/antfly/test_quickstart.py)
    // so the per-iteration cost can be compared against the per-primitive
    // numbers reported by `bench`. Uses a slim root module so it only depends
    // on text/search code (and skips OpenAPI codegen).
    const quickstart_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/quickstart_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    quickstart_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    quickstart_bench_root_mod.addImport("bloom", bloom_mod);
    addSnowballModule(b, quickstart_bench_root_mod);

    const quickstart_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/quickstart_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
    });
    quickstart_bench_mod.addImport("antfly_quickstart_bench", quickstart_bench_root_mod);

    const quickstart_bench = b.addExecutable(.{
        .name = "quickstart_bench",
        .root_module = quickstart_bench_mod,
    });

    const run_quickstart_bench = b.addRunArtifact(quickstart_bench);
    if (b.args) |args| {
        run_quickstart_bench.addArgs(args);
    }
    const quickstart_bench_step = b.step("quickstart-bench", "Run the quickstart-shaped end-to-end benchmark");
    quickstart_bench_step.dependOn(&run_quickstart_bench.step);

    const compat_mod = b.createModule(.{
        .root_source_file = b.path("bench/compat_runner.zig"),
        .target = target,
        .optimize = optimize,
    });
    compat_mod.addImport("antfly-zig", lib_mod);

    const compat = b.addExecutable(.{
        .name = "compat_runner",
        .root_module = compat_mod,
    });

    const run_compat = b.addRunArtifact(compat);
    run_compat.addArg("compat/cases");
    const compat_step = b.step("compat", "Run the shared compatibility corpus");
    compat_step.dependOn(&run_compat.step);
    compat_step.dependOn(&run_lib_ha_compat_tests.step);

    const search_benchmark_index_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_index.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    search_benchmark_index_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_index = b.addExecutable(.{
        .name = "search_benchmark_index",
        .root_module = search_benchmark_index_mod,
    });
    const install_search_benchmark_index = b.addInstallArtifact(search_benchmark_index, .{});

    const search_benchmark_query_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_query.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    search_benchmark_query_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_query = b.addExecutable(.{
        .name = "search_benchmark_query",
        .root_module = search_benchmark_query_mod,
    });
    const install_search_benchmark_query = b.addInstallArtifact(search_benchmark_query, .{});

    const search_benchmark_codec_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_codec_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    search_benchmark_codec_bench_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_codec_bench = b.addExecutable(.{
        .name = "search_benchmark_codec_bench",
        .root_module = search_benchmark_codec_bench_mod,
    });
    const install_search_benchmark_codec_bench = b.addInstallArtifact(search_benchmark_codec_bench, .{});

    const run_search_benchmark_codec_bench = b.addRunArtifact(search_benchmark_codec_bench);
    if (b.args) |args| {
        run_search_benchmark_codec_bench.addArgs(args);
    }
    const search_bench_codec_step = b.step("search-bench-codec-bench", "Benchmark StreamVByte codec used by search postings");
    search_bench_codec_step.dependOn(&run_search_benchmark_codec_bench.step);

    const wand_skip_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/wand_skip_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    wand_skip_bench_mod.addImport("antfly-zig", lib_mod);
    const wand_skip_bench = b.addExecutable(.{
        .name = "wand_skip_bench",
        .root_module = wand_skip_bench_mod,
    });
    const run_wand_skip_bench = b.addRunArtifact(wand_skip_bench);
    if (b.args) |args| {
        run_wand_skip_bench.addArgs(args);
    }
    const wand_skip_bench_step = b.step("wand-skip-bench", "Profile WAND advance vs score iter.next() ratio across query shapes");
    wand_skip_bench_step.dependOn(&run_wand_skip_bench.step);

    const search_bench_build_step = b.step("search-bench-build", "Build search-benchmark-game antfly-zig adapter and search codec benchmark binaries");
    search_bench_build_step.dependOn(&install_search_benchmark_index.step);
    search_bench_build_step.dependOn(&install_search_benchmark_query.step);
    search_bench_build_step.dependOn(&install_search_benchmark_codec_bench.step);

    const storage_fixture_promote_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_fixture_promote.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_fixture_promote = b.addExecutable(.{
        .name = "storage_fixture_promote",
        .root_module = storage_fixture_promote_mod,
    });

    const run_storage_fixture_promote = b.addRunArtifact(storage_fixture_promote);
    if (b.args) |args| {
        run_storage_fixture_promote.addArgs(args);
    }
    const storage_fixture_promote_step = b.step("storage-fixture-promote", "Promote a storage sim fixture into the checked-in replay corpus");
    storage_fixture_promote_step.dependOn(&run_storage_fixture_promote.step);

    const lmdb_fixture_promote_step = b.step("lmdb-fixture-promote", "Promote an LMDB replay fixture into pkg/antfly/src/storage/lmdb_sim_fixtures");
    lmdb_fixture_promote_step.dependOn(&run_storage_fixture_promote.step);

    const merge_cycle_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/merge_cycle_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    merge_cycle_mod.addImport("antfly-zig", lib_mod);

    const merge_cycle = b.addExecutable(.{
        .name = "merge_cycle_bench",
        .root_module = merge_cycle_mod,
    });

    const run_merge_cycle = b.addRunArtifact(merge_cycle);
    const merge_cycle_step = b.step("merge-cycle", "Run the merge-cycle benchmark");
    merge_cycle_step.dependOn(&run_merge_cycle.step);

    const merge_cost_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/merge_cost_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    merge_cost_mod.addImport("antfly-zig", lib_mod);

    const merge_cost = b.addExecutable(.{
        .name = "merge_cost_bench",
        .root_module = merge_cost_mod,
    });

    const run_merge_cost = b.addRunArtifact(merge_cost);
    const merge_cost_step = b.step("merge-cost", "Run the direct merge cost benchmark");
    merge_cost_step.dependOn(&run_merge_cost.step);

    const hbc_parity_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_parity.zig"),
        .target = target,
        .optimize = optimize,
    });
    hbc_parity_mod.addImport("antfly-zig", lib_mod);

    const hbc_parity = b.addExecutable(.{
        .name = "hbc_parity",
        .root_module = hbc_parity_mod,
    });

    const run_hbc_parity = b.addRunArtifact(hbc_parity);
    const hbc_parity_step = b.step("hbc-parity", "Run the deterministic HBC parity harness");
    hbc_parity_step.dependOn(&run_hbc_parity.step);

    const hbc_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/bench/hbc_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_bench = b.addExecutable(.{
        .name = "hbc_bench",
        .root_module = hbc_bench_mod,
    });

    const run_hbc_bench = b.addRunArtifact(hbc_bench);
    if (b.args) |args| {
        run_hbc_bench.addArgs(args);
    }
    const hbc_bench_step = b.step("hbc-bench", "Benchmark HBC kmeans vs hilbert split algorithms");
    hbc_bench_step.dependOn(&run_hbc_bench.step);

    const hbc_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_write_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_write_bench = b.addExecutable(.{
        .name = "hbc_write_bench",
        .root_module = hbc_write_bench_mod,
    });

    const run_hbc_write_bench = b.addRunArtifact(hbc_write_bench);
    if (b.args) |args| {
        run_hbc_write_bench.addArgs(args);
    } else {
        run_hbc_write_bench.addArgs(&.{
            "--samples",    "3",
            "--vectors",    "10000",
            "--dims",       "128",
            "--batch-size", "1000",
            "--leaf-size",  "128",
            "--storage",    "host",
        });
    }
    const hbc_write_bench_step = b.step("hbc-write-bench", "Benchmark HBC bulk build and online batched write amplification");
    hbc_write_bench_step.dependOn(&run_hbc_write_bench.step);

    const run_hbc_write_guardrail = b.addRunArtifact(hbc_write_bench);
    if (b.args) |args| {
        run_hbc_write_guardrail.addArgs(args);
    } else {
        run_hbc_write_guardrail.addArgs(&.{
            "--samples",    "1",
            "--vectors",    "5000",
            "--dims",       "1536",
            "--batch-size", "500",
            "--leaf-size",  "168",
            "--storage",    "host",
        });
    }
    const hbc_write_guardrail_step = b.step("hbc-write-guardrail", "Run a VectorDBBench-shaped HBC write-amplification smoke guardrail");
    hbc_write_guardrail_step.dependOn(&run_hbc_write_guardrail.step);

    const hbc_read_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_read_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_read_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_read_bench = b.addExecutable(.{
        .name = "hbc_read_bench",
        .root_module = hbc_read_bench_mod,
    });

    const run_hbc_read_bench = b.addRunArtifact(hbc_read_bench);
    if (b.args) |args| {
        run_hbc_read_bench.addArgs(args);
    } else {
        run_hbc_read_bench.addArgs(&.{
            "--samples",    "3",
            "--vectors",    "10000",
            "--dims",       "128",
            "--queries",    "200",
            "--k",          "10",
            "--batch-size", "1000",
            "--leaf-size",  "128",
            "--storage",    "host",
            "--build",      "both",
        });
    }
    const hbc_read_bench_step = b.step("hbc-read-bench", "Benchmark HBC query read paths with storage and search-profile counters");
    hbc_read_bench_step.dependOn(&run_hbc_read_bench.step);

    const hbc_isolate_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_isolate.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const hbc_isolate_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/hbc_isolate_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_isolate_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    hbc_isolate_root_mod.addImport("bloom", bloom_mod);
    hbc_isolate_root_mod.addImport("antfly_vector", vector_mod);
    hbc_isolate_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    hbc_isolate_root_mod.addImport("antfly_platform", platform_mod);
    hbc_isolate_mod.addImport("antfly_hbc_isolate_root", hbc_isolate_root_mod);

    const hbc_isolate = b.addExecutable(.{
        .name = "hbc_isolate",
        .root_module = hbc_isolate_mod,
    });

    const run_hbc_isolate = b.addRunArtifact(hbc_isolate);
    if (b.args) |args| {
        run_hbc_isolate.addArgs(args);
    }
    const hbc_isolate_step = b.step("hbc-isolate", "Run the deterministic raw Zig HBC isolate benchmark");
    hbc_isolate_step.dependOn(&run_hbc_isolate.step);

    const dense_stack_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/dense_stack_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    dense_stack_bench_mod.addImport("antfly-zig", lib_mod);
    const capi_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    capi_bench_mod.addImport("antfly-zig", lib_mod);
    dense_stack_bench_mod.addImport("antfly_capi", capi_bench_mod);

    const dense_stack_bench = b.addExecutable(.{
        .name = "dense_stack_bench",
        .root_module = dense_stack_bench_mod,
    });

    const run_dense_stack_bench = b.addRunArtifact(dense_stack_bench);
    if (b.args) |args| {
        run_dense_stack_bench.addArgs(args);
    }
    const build_dense_stack_bench_step = b.step("dense-stack-bench-build", "Build dense_stack_bench without running it");
    build_dense_stack_bench_step.dependOn(&dense_stack_bench.step);
    const dense_stack_bench_step = b.step("dense-stack-bench", "Benchmark dense DB search vs dense CAPI layers");
    dense_stack_bench_step.dependOn(&run_dense_stack_bench.step);

    const replay_bench_build_options = b.addOptions();
    replay_bench_build_options.addOption([]const u8, "lmdb_backend", @tagName(lmdb_backend));
    replay_bench_build_options.addOption(bool, "lmdb_evented_async_io", lmdb_evented_async_io);
    replay_bench_build_options.addOption(bool, "storage_sim_soak", false);
    replay_bench_build_options.addOption(bool, "with_tla", with_tla);
    replay_bench_build_options.addOption(bool, "link_libc", true);
    replay_bench_build_options.addOption(bool, "swarm_runtime_focused_test", false);
    replay_bench_build_options.addOption(bool, "bench_minimal_deps", true);

    const replay_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/replay_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const replay_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(replay_bench_root),
        .target = target,
        .optimize = .ReleaseFast,
    });
    replay_bench_root_mod.addOptions("build_options", replay_bench_build_options);
    replay_bench_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    replay_bench_root_mod.addImport("antfly-json", json_mod);
    replay_bench_root_mod.addImport("bloom", bloom_mod);
    replay_bench_root_mod.addImport("antfly_vector", vector_mod);
    replay_bench_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    replay_bench_root_mod.addImport("antfly_matcher", matcher_mod);
    replay_bench_root_mod.addImport("antfly_resolver", resolver_mod);
    replay_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    replay_bench_root_mod.addImport("antfly_regex", regex_mod);
    replay_bench_root_mod.addImport("antfly_reranking", reranking_mod);
    replay_bench_root_mod.addImport("antfly_scraping", scraping_mod);
    replay_bench_root_mod.addImport("antfly_platform", platform_mod);
    addSnowballModule(b, replay_bench_root_mod);
    replay_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const replay_bench = b.addExecutable(.{
        .name = "replay_bench",
        .root_module = replay_bench_mod,
    });

    const run_replay_bench = b.addRunArtifact(replay_bench);
    if (b.args) |args| {
        run_replay_bench.addArgs(args);
    }
    const replay_bench_step = b.step("replay-bench", "Benchmark replay stream write and catch-up paths");
    replay_bench_step.dependOn(&run_replay_bench.step);

    const dense_ingest_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/dense_ingest_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    dense_ingest_guardrail_mod.addImport("antfly-zig", replay_bench_root_mod);

    const dense_ingest_guardrail = b.addExecutable(.{
        .name = "dense_ingest_guardrail",
        .root_module = dense_ingest_guardrail_mod,
    });
    const install_dense_ingest_guardrail = b.addInstallArtifact(dense_ingest_guardrail, .{});

    const run_dense_ingest_guardrail = b.addRunArtifact(dense_ingest_guardrail);
    if (b.args) |args| {
        run_dense_ingest_guardrail.addArgs(args);
    } else {
        run_dense_ingest_guardrail.addArgs(&.{
            "--docs",
            "5000",
            "--dims",
            "1536",
            "--batch-size",
            "500",
            "--sync-level",
            "write",
            "--status-probe-every",
            "1",
            "--max-dense-lsm-run-bytes",
            "1073741824",
            "--max-dense-l0-runs",
            "64",
            "--max-status-probe-ns",
            "500000000",
        });
    }
    const build_dense_ingest_guardrail_step = b.step("dense-ingest-guardrail-build", "Build the dedicated dense ingest guardrail without running it");
    build_dense_ingest_guardrail_step.dependOn(&dense_ingest_guardrail.step);
    const install_dense_ingest_guardrail_step = b.step("dense-ingest-guardrail-install", "Build and install the dedicated dense ingest guardrail");
    install_dense_ingest_guardrail_step.dependOn(&install_dense_ingest_guardrail.step);

    const batch_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/batch_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    batch_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const batch_bench = b.addExecutable(.{
        .name = "batch_bench",
        .root_module = batch_bench_mod,
    });

    const run_batch_bench = b.addRunArtifact(batch_bench);
    if (b.args) |args| {
        run_batch_bench.addArgs(args);
    }
    const batch_bench_step = b.step("batch-bench", "Benchmark overwrite-heavy batch writes and bulk-session coalescing");
    batch_bench_step.dependOn(&run_batch_bench.step);

    const run_relational_index_bench_matrix = b.addRunArtifact(batch_bench);
    if (b.args) |args| {
        run_relational_index_bench_matrix.addArgs(args);
    } else {
        run_relational_index_bench_matrix.addArgs(&.{
            "--matrix",
            "--matrix-preset",
            "smoke",
            "--primary",
            "mem",
            "--overwrite-passes",
            "1",
            "--batch-size",
            "1000",
            "--query-repeats",
            "3",
            "--query-limit",
            "100",
        });
    }
    const relational_index_bench_matrix_step = b.step("relational-index-bench-matrix", "Run relational index benchmark matrix");
    relational_index_bench_matrix_step.dependOn(&run_relational_index_bench_matrix.step);

    const docid_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/docid_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    docid_write_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const docid_write_bench = b.addExecutable(.{
        .name = "docid_write_bench",
        .root_module = docid_write_bench_mod,
    });

    const run_docid_write_bench = b.addRunArtifact(docid_write_bench);
    run_docid_write_bench.addArgs(&.{ "--docs", "512", "--batch-size", "128", "--body-repeat", "1" });

    const docid_query_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/docid_query_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    docid_query_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const docid_query_bench = b.addExecutable(.{
        .name = "docid_query_bench",
        .root_module = docid_query_bench_mod,
    });

    const run_docid_query_bench = b.addRunArtifact(docid_query_bench);
    run_docid_query_bench.addArgs(&.{ "--docs", "4096", "--queries", "16", "--repeats", "8", "--filter-size", "256", "--limit", "32" });

    const docid_bench_step = b.step("docid-bench", "Run DOCID doc-set, write-path, and query benchmarks");
    docid_bench_step.dependOn(&run_docid_doc_set_bench.step);
    docid_bench_step.dependOn(&run_docid_write_bench.step);
    docid_bench_step.dependOn(&run_docid_query_bench.step);

    const algebraic_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/algebraic_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(algebraic_bench_root),
        .target = target,
        .optimize = .ReleaseFast,
    });
    algebraic_bench_root_mod.addOptions("build_options", replay_bench_build_options);
    algebraic_bench_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    algebraic_bench_root_mod.addImport("antfly-json", json_mod);
    algebraic_bench_root_mod.addImport("bloom", bloom_mod);
    algebraic_bench_root_mod.addImport("antfly_vector", vector_mod);
    algebraic_bench_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    algebraic_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    algebraic_bench_root_mod.addImport("antfly_regex", regex_mod);
    algebraic_bench_root_mod.addImport("antfly_platform", platform_mod);
    algebraic_bench_root_mod.addImport("antfly_reranking", reranking_mod);
    addSnowballModule(b, algebraic_bench_root_mod);
    algebraic_bench_mod.addImport("antfly-zig", algebraic_bench_root_mod);

    const algebraic_bench = b.addExecutable(.{
        .name = "algebraic_bench",
        .root_module = algebraic_bench_mod,
    });

    const run_algebraic_bench = b.addRunArtifact(algebraic_bench);
    if (b.args) |args| {
        run_algebraic_bench.addArgs(args);
    } else {
        run_algebraic_bench.addArgs(&.{
            "--docs",
            "20000",
            "--repeats",
            "25",
            "--batch-size",
            "500",
        });
    }
    const algebraic_bench_step = b.step("algebraic-bench", "Benchmark algebraic aggregations against document-scan aggregations");
    algebraic_bench_step.dependOn(&run_algebraic_bench.step);

    const algebraic_summary_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/algebraic_summary.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_summary = b.addExecutable(.{
        .name = "algebraic_summary",
        .root_module = algebraic_summary_mod,
    });
    const run_algebraic_summary = b.addRunArtifact(algebraic_summary);
    if (b.args) |args| {
        run_algebraic_summary.addArgs(args);
    }
    const algebraic_summary_step = b.step("algebraic-summary", "Summarize algebraic benchmark JSONL output");
    algebraic_summary_step.dependOn(&run_algebraic_summary.step);

    const run_algebraic_performance_guardrail = b.addRunArtifact(algebraic_summary);
    run_algebraic_performance_guardrail.addArgs(&.{
        "--input",
        "bench/storage/algebraic_performance_guardrail_fixture.jsonl",
        "--baseline",
        "bench/storage/algebraic_performance_guardrail_baseline.jsonl",
        "--require-performance-evidence",
        "--min-lsm-dataset-cases",
        "1",
        "--min-lsm-query-records",
        "3",
        "--min-cold-query-records",
        "2",
        "--min-warm-query-records",
        "2",
        "--min-constrained-query-records",
        "3",
        "--min-wide-query-records",
        "3",
        "--min-stats-query-records",
        "3",
        "--min-cardinality-query-records",
        "3",
        "--min-range-query-records",
        "3",
        "--min-histogram-query-records",
        "3",
        "--min-fanout-dataset-cases",
        "1",
        "--min-public-query-comparison-pairs",
        "2",
        "--min-lsm-sorted-ingest-runs",
        "1",
        "--max-lsm-flushes",
        "0",
        "--max-lsm-write-pressure-compactions",
        "0",
        "--max-correctness-failures",
        "0",
        "--max-algebraic-query-ms",
        "2",
        "--max-public-query-http-us",
        "100",
        "--max-algebraic-bytes-per-doc",
        "10",
        "--max-symbol-bytes-per-doc",
        "0",
        "--max-support-bytes-per-doc",
        "0",
        "--max-accumulator-flush-count",
        "0",
        "--max-path-dictionary-fst-rebuild-count",
        "1",
        "--max-public-query-load-rss-peak-bytes",
        "0",
        "--max-public-query-search-rss-peak-bytes",
        "0",
        "--max-churn-algebraic-update-ms",
        "2",
        "--max-algebraic-query-ms-ratio-vs-baseline",
        "1.0",
        "--max-public-query-http-us-ratio-vs-baseline",
        "1.0",
        "--max-algebraic-bytes-per-doc-ratio-vs-baseline",
        "1.0",
        "--max-churn-algebraic-update-ms-ratio-vs-baseline",
        "1.0",
    });

    const algebraic_planner_ownership_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("tools/guardrails/algebraic_planner_ownership_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_planner_ownership_guardrail = b.addExecutable(.{
        .name = "algebraic_planner_ownership_guardrail",
        .root_module = algebraic_planner_ownership_guardrail_mod,
    });
    const run_algebraic_planner_ownership_guardrail = b.addRunArtifact(algebraic_planner_ownership_guardrail);

    const algebraic_archive_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/algebraic_archive_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_archive_guardrail = b.addExecutable(.{
        .name = "algebraic_archive_guardrail",
        .root_module = algebraic_archive_guardrail_mod,
    });
    const run_algebraic_archive_guardrail = b.addRunArtifact(algebraic_archive_guardrail);
    run_algebraic_archive_guardrail.addArgs(&.{
        "--archive",
        "bench/storage/algebraic_production_archive_fixture",
        "--require-thresholds",
        "--require-baseline",
        "--require-non-smoke",
        "--min-docs",
        "100",
        "--min-repeats",
        "1",
        "--min-churn-ops",
        "1",
        "--min-public-docs",
        "100",
        "--min-graph-docs",
        "100",
    });

    const algebraic_guardrail_step = b.step("algebraic-guardrail", "Run CI-safe algebraic guardrails");
    algebraic_guardrail_step.dependOn(&run_algebraic_performance_guardrail.step);
    algebraic_guardrail_step.dependOn(&run_algebraic_planner_ownership_guardrail.step);
    algebraic_guardrail_step.dependOn(&run_algebraic_archive_guardrail.step);

    const rw_lock_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/rw_lock_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    rw_lock_bench_mod.addImport("antfly-zig", lib_mod);

    const rw_lock_bench = b.addExecutable(.{
        .name = "rw_lock_bench",
        .root_module = rw_lock_bench_mod,
    });

    const run_rw_lock_bench = b.addRunArtifact(rw_lock_bench);
    if (b.args) |args| {
        run_rw_lock_bench.addArgs(args);
    }
    const rw_lock_bench_step = b.step("rw-lock-bench", "Benchmark mixed search/write load against the DB RW apply lock");
    rw_lock_bench_step.dependOn(&run_rw_lock_bench.step);

    const open_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/open_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    open_bench_mod.addImport("antfly-zig", lib_mod);

    const open_bench = b.addExecutable(.{
        .name = "open_bench",
        .root_module = open_bench_mod,
    });

    const run_open_bench = b.addRunArtifact(open_bench);
    if (b.args) |args| {
        run_open_bench.addArgs(args);
    }
    const open_bench_step = b.step("open-bench", "Benchmark DB.open for configurable index mixes and replay backlog");
    open_bench_step.dependOn(&run_open_bench.step);

    const artifact_rebuild_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/artifact_rebuild_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    artifact_rebuild_bench_mod.addImport("antfly-zig", lib_mod);

    const artifact_rebuild_bench = b.addExecutable(.{
        .name = "artifact_rebuild_bench",
        .root_module = artifact_rebuild_bench_mod,
    });

    const run_artifact_rebuild_bench = b.addRunArtifact(artifact_rebuild_bench);
    if (b.args) |args| {
        run_artifact_rebuild_bench.addArgs(args);
    }
    const build_artifact_rebuild_bench_step = b.step("artifact-rebuild-bench-build", "Build artifact_rebuild_bench without running it");
    build_artifact_rebuild_bench_step.dependOn(&artifact_rebuild_bench.step);
    const artifact_rebuild_bench_step = b.step("artifact-rebuild-bench", "Benchmark loaded-root startup artifact rebuild progress and reopen cost");
    artifact_rebuild_bench_step.dependOn(&run_artifact_rebuild_bench.step);

    const provisioned_warmup_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/provisioned_warmup_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    provisioned_warmup_bench_mod.addImport("antfly-zig", lib_mod);

    const provisioned_warmup_bench = b.addExecutable(.{
        .name = "provisioned_warmup_bench",
        .root_module = provisioned_warmup_bench_mod,
    });

    const run_provisioned_warmup_bench = b.addRunArtifact(provisioned_warmup_bench);
    if (b.args) |args| {
        run_provisioned_warmup_bench.addArgs(args);
    }
    const provisioned_warmup_bench_step = b.step("provisioned-warmup-bench", "Benchmark provisioned cache warmup against first read/write latency");
    provisioned_warmup_bench_step.dependOn(&run_provisioned_warmup_bench.step);

    const provisioned_dense_ingest_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/provisioned_dense_ingest_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    provisioned_dense_ingest_guardrail_mod.addImport("antfly-zig", lib_mod);

    const provisioned_dense_ingest_guardrail = b.addExecutable(.{
        .name = "provisioned_dense_ingest_guardrail",
        .root_module = provisioned_dense_ingest_guardrail_mod,
    });
    const install_provisioned_dense_ingest_guardrail = b.addInstallArtifact(provisioned_dense_ingest_guardrail, .{});

    const run_provisioned_dense_ingest_guardrail = b.addRunArtifact(provisioned_dense_ingest_guardrail);
    if (b.args) |args| {
        run_provisioned_dense_ingest_guardrail.addArgs(args);
    } else {
        run_provisioned_dense_ingest_guardrail.addArgs(&.{
            "--docs",
            "50000",
            "--dims",
            "1536",
            "--batch-size",
            "100",
            "--sync-level",
            "write",
        });
    }
    const build_provisioned_dense_ingest_guardrail_step = b.step("provisioned-dense-ingest-guardrail-build", "Build the provisioned table dense ingest guardrail without running it");
    build_provisioned_dense_ingest_guardrail_step.dependOn(&provisioned_dense_ingest_guardrail.step);
    const install_provisioned_dense_ingest_guardrail_step = b.step("provisioned-dense-ingest-guardrail-install", "Build and install the provisioned table dense ingest guardrail");
    install_provisioned_dense_ingest_guardrail_step.dependOn(&install_provisioned_dense_ingest_guardrail.step);
    const provisioned_dense_ingest_guardrail_step = b.step("provisioned-dense-ingest-guardrail", "Benchmark the provisioned table write path without HTTP for VectorDBBench-shaped dense ingest");
    provisioned_dense_ingest_guardrail_step.dependOn(&run_provisioned_dense_ingest_guardrail.step);

    const public_query_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/public_query_guardrail.zig"),
        .target = target,
        .optimize = optimize,
    });
    public_query_guardrail_mod.addImport("antfly-zig", lib_mod);

    const public_query_guardrail = b.addExecutable(.{
        .name = "public_query_guardrail",
        .root_module = public_query_guardrail_mod,
    });

    const run_public_query_guardrail = b.addRunArtifact(public_query_guardrail);
    if (b.args) |args| {
        run_public_query_guardrail.addArgs(args);
    } else {
        run_public_query_guardrail.addArgs(&.{
            "--docs",
            "5000",
            "--dims",
            "384",
            "--queries",
            "25",
            "--repeats",
            "10",
            "--k",
            "100",
            "--batch-size",
            "250",
            "--search-threads",
            "5",
            "--sync-level",
            "write",
        });
    }
    const build_public_query_guardrail_step = b.step("public-query-guardrail-build", "Build the dedicated public query guardrail without running it");
    build_public_query_guardrail_step.dependOn(&public_query_guardrail.step);
    const public_query_guardrail_step = b.step("public-query-guardrail", "Benchmark the public /db/v1/tables/<table>/query path against direct DB search and health responsiveness");
    public_query_guardrail_step.dependOn(&run_public_query_guardrail.step);

    const raft_apply_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/raft_apply_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    raft_apply_bench_mod.addImport("antfly-zig", lib_mod);
    raft_apply_bench_mod.addImport("raft_engine", raft_engine_mod);

    const raft_apply_bench = b.addExecutable(.{
        .name = "raft_apply_bench",
        .root_module = raft_apply_bench_mod,
    });

    const run_raft_apply_bench = b.addRunArtifact(raft_apply_bench);
    if (b.args) |args| {
        run_raft_apply_bench.addArgs(args);
    }
    const raft_apply_bench_step = b.step("raft-apply-bench", "Benchmark committed-entry encoding and data raft apply store persistence");
    raft_apply_bench_step.dependOn(&run_raft_apply_bench.step);

    const managed_host_wal_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/managed_host_wal_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    managed_host_wal_bench_mod.addImport("antfly-zig", lib_mod);
    managed_host_wal_bench_mod.addImport("raft_engine", raft_engine_mod);

    const managed_host_wal_bench = b.addExecutable(.{
        .name = "managed_host_wal_bench",
        .root_module = managed_host_wal_bench_mod,
    });

    const run_managed_host_wal_bench = b.addRunArtifact(managed_host_wal_bench);
    if (b.args) |args| {
        run_managed_host_wal_bench.addArgs(args);
    }
    const managed_host_wal_bench_step = b.step("managed-host-wal-bench", "Benchmark ManagedHost proposal persistence with WAL-backed raft state and restart");
    managed_host_wal_bench_step.dependOn(&run_managed_host_wal_bench.step);

    const dense_ingest_guardrail_step = b.step("dense-ingest-guardrail", "Run a VectorDBBench-shaped dense ingest smoke guardrail");
    dense_ingest_guardrail_step.dependOn(&run_dense_ingest_guardrail.step);

    const vector_write_guardrails_step = b.step("vector-write-guardrails", "Run local VectorDBBench-shaped vector write guardrails");
    vector_write_guardrails_step.dependOn(hbc_write_guardrail_step);
    vector_write_guardrails_step.dependOn(dense_ingest_guardrail_step);

    const dense_profile_summary = b.addExecutable(.{
        .name = "dense_profile_summary",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/vectors/dense_profile_summary.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    const run_dense_profile_summary = b.addRunArtifact(dense_profile_summary);
    if (b.args) |args| {
        run_dense_profile_summary.addArgs(args);
    }
    const dense_profile_summary_step = b.step("dense-profile-summary", "Summarize dense-stack-bench profile JSONL output");
    dense_profile_summary_step.dependOn(&run_dense_profile_summary.step);

    const lmdb_commit_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_commit_compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lmdb_commit_compare_mod.addImport("antfly-zig", lib_mod);

    const lmdb_commit_compare = b.addExecutable(.{
        .name = "lmdb_commit_compare",
        .root_module = lmdb_commit_compare_mod,
    });

    const run_lmdb_commit_compare = b.addRunArtifact(lmdb_commit_compare);
    if (b.args) |args| {
        run_lmdb_commit_compare.addArgs(args);
    }
    const lmdb_commit_compare_step = b.step("lmdb-commit-compare", "Benchmark LMDB commit cost in isolation");
    lmdb_commit_compare_step.dependOn(&run_lmdb_commit_compare.step);

    const hbc_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/bench/hbc_split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_split_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_split_bench = b.addExecutable(.{
        .name = "hbc_split_bench",
        .root_module = hbc_split_bench_mod,
    });

    const run_hbc_split_bench = b.addRunArtifact(hbc_split_bench);
    if (b.args) |args| {
        run_hbc_split_bench.addArgs(args);
    }
    const hbc_split_bench_step = b.step("hbc-split-bench", "Benchmark dense-only HBC split child rebuild");
    hbc_split_bench_step.dependOn(&run_hbc_split_bench.step);

    const sparse_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/sparse_split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    sparse_split_bench_mod.addImport("antfly-zig", lib_mod);

    const sparse_split_bench = b.addExecutable(.{
        .name = "sparse_split_bench",
        .root_module = sparse_split_bench_mod,
    });

    const run_sparse_split_bench = b.addRunArtifact(sparse_split_bench);
    if (b.args) |args| {
        run_sparse_split_bench.addArgs(args);
    }
    const sparse_split_bench_step = b.step("sparse-split-bench", "Benchmark sparse-only split handoff");
    sparse_split_bench_step.dependOn(&run_sparse_split_bench.step);

    const rabitq_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/rabitq_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    rabitq_bench_mod.addImport("antfly-zig", lib_mod);
    rabitq_bench_mod.addImport("antfly_vector", vector_mod);

    const rabitq_bench = b.addExecutable(.{
        .name = "rabitq_bench",
        .root_module = rabitq_bench_mod,
    });

    const run_rabitq_bench = b.addRunArtifact(rabitq_bench);
    if (b.args) |args| {
        run_rabitq_bench.addArgs(args);
    }
    const rabitq_bench_step = b.step("rabitq-bench", "Benchmark RaBitQ primitives and estimator");
    rabitq_bench_step.dependOn(&run_rabitq_bench.step);

    const recall_harness_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/recall_harness.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    recall_harness_mod.addImport("antfly-zig", lib_mod);

    const recall_harness = b.addExecutable(.{
        .name = "recall_harness",
        .root_module = recall_harness_mod,
    });

    const run_recall_harness = b.addRunArtifact(recall_harness);
    run_recall_harness.stdio = .inherit;
    if (b.args) |args| {
        run_recall_harness.addArgs(args);
    }
    const recall_harness_step = b.step("recall-harness", "Run Zig recall suites against exported vector datasets");
    recall_harness_step.dependOn(&run_recall_harness.step);

    return .{
        .recall_harness = recall_harness,
    };
}
