// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

const RequiredFile = struct {
    name: []const u8,
    must_contain: []const []const u8,
};

const Config = struct {
    archive: []const u8 = "bench/storage/algebraic_production_archive_fixture",
    require_thresholds: bool = false,
    require_baseline: bool = false,
    require_non_smoke: bool = false,
    min_docs: u64 = 0,
    min_repeats: u64 = 0,
    min_churn_ops: u64 = 0,
    min_public_docs: u64 = 0,
    min_graph_docs: u64 = 0,
    min_adaptive_docs: u64 = 0,
    min_cold_docs: u64 = 0,
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const cfg = try parseArgs(init.minimal.args);
    var failures: usize = 0;

    const files = [_]RequiredFile{
        .{
            .name = "environment.txt",
            .must_contain = &.{
                "timestamp_utc=",
                "git_commit=",
                "git_status_porcelain_begin",
                "git_status_porcelain_end",
                "uname=",
                "docs=",
                "repeats=",
                "batch_size=",
                "churn_ops=",
                "fanout=",
                "public_docs=",
                "public_dims=",
                "baseline=",
                "min_stats_query_records=",
                "min_cardinality_query_records=",
                "min_range_query_records=",
                "min_histogram_query_records=",
                "min_lsm_sorted_ingest_runs=",
                "lsm_flush_threshold_bytes=",
                "lsm_bulk_ingest_flush_threshold_multiplier=",
                "lsm_bulk_ingest_flush_threshold_bytes_multiplier=",
                "lsm_direct_bulk_ingest=",
                "max_correctness_failures=0",
                "max_unclassified_algebraic_comparisons=0",
            },
        },
        .{
            .name = "summary-command.txt",
            .must_contain = &.{
                "zig build algebraic-summary",
                "--require-performance-evidence",
                "--min-lsm-dataset-cases",
                "--min-public-query-comparison-pairs",
                "--min-cold-query-records",
                "--min-warm-query-records",
                "--min-wide-query-records",
                "--min-stats-query-records",
                "--min-cardinality-query-records",
                "--min-range-query-records",
                "--min-histogram-query-records",
                "--min-lsm-sorted-ingest-runs",
                "--max-correctness-failures",
                "--max-unclassified-algebraic-comparisons",
            },
        },
        .{
            .name = "algebraic-production-hardening-combined.jsonl",
            .must_contain = &.{
                "\"event\":\"dataset\"",
                "\"event\":\"dataset_lsm_config\"",
                "\"lsm_flush_threshold_bytes\"",
                "\"lsm_bulk_ingest_flush_threshold_multiplier\"",
                "\"lsm_bulk_ingest_flush_threshold_bytes_multiplier\"",
                "\"lsm_direct_bulk_ingest\"",
                "\"event\":\"query\"",
                "\"event\":\"public_query_guardrail_summary\"",
            },
        },
        .{
            .name = "algebraic-production-hardening-summary.jsonl",
            .must_contain = &.{
                "\"event\":\"performance_evidence_summary\"",
                "\"event\":\"lsm_analytics_summary\"",
                "\"event\":\"public_query_comparison_summary\"",
                "\"event\":\"graph_algebraic_traversal_summary\"",
                "\"correctness_failures\":0",
                "\"max_symbol_bytes_per_doc\"",
                "\"max_support_bytes_per_doc\"",
                "\"max_path_dictionary_fst_rebuild_count\"",
                "\"max_public_query_search_rss_peak_bytes\"",
                "\"cold_query_records\"",
                "\"warm_query_records\"",
                "\"wide_query_records\"",
                "\"stats_query_records\"",
                "\"cardinality_query_records\"",
                "\"range_query_records\"",
                "\"histogram_query_records\"",
                "\"total_lsm_sorted_ingest_runs\"",
                "\"unclassified_algebraic_comparisons\":0",
            },
        },
    };

    for (files) |file| {
        const contents = readArchiveFile(io, alloc, cfg.archive, file.name) catch |err| {
            std.debug.print("algebraic_archive_guardrail missing archive={s} file={s} err={s}\n", .{ cfg.archive, file.name, @errorName(err) });
            failures += 1;
            continue;
        };
        defer alloc.free(contents);

        if (std.mem.trim(u8, contents, " \t\r\n").len == 0) {
            std.debug.print("algebraic_archive_guardrail empty archive={s} file={s}\n", .{ cfg.archive, file.name });
            failures += 1;
        }
        for (file.must_contain) |needle| {
            if (std.mem.indexOf(u8, contents, needle) == null) {
                std.debug.print("algebraic_archive_guardrail missing archive={s} file={s} needle={s}\n", .{ cfg.archive, file.name, needle });
                failures += 1;
            }
        }
    }

    const environment = try readArchiveFile(io, alloc, cfg.archive, "environment.txt");
    defer alloc.free(environment);
    const summary_command = try readArchiveFile(io, alloc, cfg.archive, "summary-command.txt");
    defer alloc.free(summary_command);
    const summary = try readArchiveFile(io, alloc, cfg.archive, "algebraic-production-hardening-summary.jsonl");
    defer alloc.free(summary);

    if (cfg.require_non_smoke and !hasKeyValue(environment, "smoke", "0")) {
        std.debug.print("algebraic_archive_guardrail expected non-smoke archive={s}\n", .{cfg.archive});
        failures += 1;
    }
    try checkMinEnvU64(environment, cfg.archive, "docs", cfg.min_docs, &failures);
    try checkMinEnvU64(environment, cfg.archive, "repeats", cfg.min_repeats, &failures);
    try checkMinEnvU64(environment, cfg.archive, "churn_ops", cfg.min_churn_ops, &failures);
    try checkMinEnvU64(environment, cfg.archive, "public_docs", cfg.min_public_docs, &failures);
    try checkMinEnvU64(environment, cfg.archive, "graph_docs", cfg.min_graph_docs, &failures);
    try checkMinEnvU64(environment, cfg.archive, "adaptive_docs", cfg.min_adaptive_docs, &failures);
    try checkMinEnvU64(environment, cfg.archive, "cold_docs", cfg.min_cold_docs, &failures);
    if (cfg.require_thresholds) {
        const threshold_keys = [_][]const u8{
            "max_algebraic_query_ms",
            "max_public_query_http_us",
            "max_algebraic_bytes_per_doc",
            "max_symbol_bytes_per_doc",
            "max_support_bytes_per_doc",
            "max_path_dictionary_fst_rebuild_count",
            "max_lsm_flushes",
            "max_lsm_write_pressure_compactions",
            "max_public_query_search_rss_peak_bytes",
            "max_churn_algebraic_update_ms",
            "max_unclassified_algebraic_comparisons",
        };
        for (threshold_keys) |key| {
            if (!hasNonEmptyKey(environment, key)) {
                std.debug.print("algebraic_archive_guardrail missing threshold archive={s} key={s}\n", .{ cfg.archive, key });
                failures += 1;
            }
        }
        const threshold_flags = [_][]const u8{
            "--max-algebraic-query-ms",
            "--max-public-query-http-us",
            "--max-algebraic-bytes-per-doc",
            "--max-symbol-bytes-per-doc",
            "--max-support-bytes-per-doc",
            "--max-path-dictionary-fst-rebuild-count",
            "--max-lsm-flushes",
            "--max-lsm-write-pressure-compactions",
            "--max-public-query-search-rss-peak-bytes",
            "--max-churn-algebraic-update-ms",
            "--max-unclassified-algebraic-comparisons",
        };
        for (threshold_flags) |flag| {
            if (std.mem.indexOf(u8, summary_command, flag) == null) {
                std.debug.print("algebraic_archive_guardrail missing summary threshold flag archive={s} flag={s}\n", .{ cfg.archive, flag });
                failures += 1;
            }
        }
    }
    if (cfg.require_baseline) {
        if (!hasNonEmptyKey(environment, "baseline")) {
            std.debug.print("algebraic_archive_guardrail missing baseline archive={s}\n", .{cfg.archive});
            failures += 1;
        }
        if (std.mem.indexOf(u8, summary, "\"event\":\"performance_baseline_comparison\"") == null) {
            std.debug.print("algebraic_archive_guardrail missing baseline comparison archive={s}\n", .{cfg.archive});
            failures += 1;
        }
        if (std.mem.indexOf(u8, summary_command, "--baseline") == null) {
            std.debug.print("algebraic_archive_guardrail missing baseline summary flag archive={s}\n", .{cfg.archive});
            failures += 1;
        }
    }

    if (failures != 0) {
        std.debug.print("algebraic_archive_guardrail failed archive={s} failures={d}\n", .{ cfg.archive, failures });
        return error.AlgebraicArchiveGuardrailFailed;
    }
    std.debug.print("algebraic_archive_guardrail ok archive={s}\n", .{cfg.archive});
}

fn parseArgs(args_in: std.process.Args) !Config {
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    var cfg = Config{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--archive")) {
            cfg.archive = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--require-thresholds")) {
            cfg.require_thresholds = true;
        } else if (std.mem.eql(u8, arg, "--require-baseline")) {
            cfg.require_baseline = true;
        } else if (std.mem.eql(u8, arg, "--require-non-smoke")) {
            cfg.require_non_smoke = true;
        } else if (std.mem.eql(u8, arg, "--min-docs")) {
            cfg.min_docs = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-repeats")) {
            cfg.min_repeats = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-churn-ops")) {
            cfg.min_churn_ops = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-public-docs")) {
            cfg.min_public_docs = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-graph-docs")) {
            cfg.min_graph_docs = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-adaptive-docs")) {
            cfg.min_adaptive_docs = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-cold-docs")) {
            cfg.min_cold_docs = try parseNextU64(&args, arg);
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    return cfg;
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for argument: {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseInt(u64, raw, 10) catch |err| {
        std.debug.print("invalid integer for argument: {s} value={s} err={s}\n", .{ flag, raw, @errorName(err) });
        return error.InvalidArgument;
    };
}

fn readArchiveFile(io: std.Io, alloc: std.mem.Allocator, archive: []const u8, file: []const u8) ![]u8 {
    const path = try std.fs.path.join(alloc, &.{ archive, file });
    defer alloc.free(path);
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(128 * 1024 * 1024));
}

fn hasNonEmptyKey(contents: []const u8, key: []const u8) bool {
    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOfScalar(u8, line, '=')) |pos| {
            if (!std.mem.eql(u8, line[0..pos], key)) continue;
            return std.mem.trim(u8, line[pos + 1 ..], " \t\r").len > 0;
        }
    }
    return false;
}

fn hasKeyValue(contents: []const u8, key: []const u8, value: []const u8) bool {
    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOfScalar(u8, line, '=')) |pos| {
            if (!std.mem.eql(u8, line[0..pos], key)) continue;
            return std.mem.eql(u8, std.mem.trim(u8, line[pos + 1 ..], " \t\r"), value);
        }
    }
    return false;
}

fn envU64(contents: []const u8, key: []const u8) ?u64 {
    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOfScalar(u8, line, '=')) |pos| {
            if (!std.mem.eql(u8, line[0..pos], key)) continue;
            const raw = std.mem.trim(u8, line[pos + 1 ..], " \t\r");
            return std.fmt.parseInt(u64, raw, 10) catch null;
        }
    }
    return null;
}

fn checkMinEnvU64(contents: []const u8, archive: []const u8, key: []const u8, min: u64, failures: *usize) !void {
    if (min == 0) return;
    const actual = envU64(contents, key) orelse {
        std.debug.print("algebraic_archive_guardrail missing numeric environment key archive={s} key={s} min={d}\n", .{ archive, key, min });
        failures.* += 1;
        return;
    };
    if (actual < min) {
        std.debug.print("algebraic_archive_guardrail environment below minimum archive={s} key={s} actual={d} min={d}\n", .{ archive, key, actual, min });
        failures.* += 1;
    }
}
