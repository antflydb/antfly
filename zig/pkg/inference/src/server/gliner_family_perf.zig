// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional, serial service-latency artifact writer shared by the pinned
//! GLiNER2.5 family tests. Callers own the request call and keep correctness,
//! response destruction, identity, and ownership checks outside the measured
//! interval.
const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");

pub const warmup_count: usize = 3;
pub const measured_count: usize = 20;

pub fn sourceHead() ![]const u8 {
    const value = platform.env.getenv("ANTFLY_GLINER25_PERF_SOURCE_HEAD") orelse
        return error.MissingGliner25PerfSourceHead;
    if (value.len != 40 and value.len != 64) return error.InvalidGliner25PerfSourceHead;
    for (value) |char| if (!std.ascii.isHex(char)) return error.InvalidGliner25PerfSourceHead;
    return value;
}

pub const Config = struct {
    allocator: std.mem.Allocator,
    output_dir: []u8,
    profile_filter: ?[]u8,

    pub fn fromEnv(allocator: std.mem.Allocator) !?Config {
        const output = platform.env.getenv("ANTFLY_GLINER25_PERF_OUTPUT_DIR") orelse return null;
        if (output.len == 0) return error.InvalidGliner25PerfOutputDir;
        const owned_output = try allocator.dupe(u8, output);
        errdefer allocator.free(owned_output);
        const filter = if (platform.env.getenv("ANTFLY_GLINER25_PERF_PROFILE")) |value| blk: {
            if (value.len == 0) return error.InvalidGliner25PerfProfile;
            break :blk try allocator.dupe(u8, value);
        } else null;
        return .{ .allocator = allocator, .output_dir = owned_output, .profile_filter = filter };
    }

    pub fn deinit(self: *Config) void {
        self.allocator.free(self.output_dir);
        if (self.profile_filter) |value| self.allocator.free(value);
        self.* = undefined;
    }

    pub fn profileEnabled(self: Config, profile: []const u8) bool {
        return self.profile_filter == null or std.mem.eql(u8, self.profile_filter.?, profile);
    }
};

pub const Metadata = struct {
    profile: []const u8,
    backend: []const u8,
    case_id: []const u8,
    path: []const u8,
    model_repo: []const u8,
    model_revision: []const u8,
    model_sha256: []const u8,
    model_size_bytes: usize,
    sidecars: SidecarPins,
    capture_sha256: []const u8,
    source_head: []const u8,
    source_diff_sha256: ?[]const u8 = null,
    binary_sha256: ?[]const u8 = null,
    input_bytes: usize,
    prepared_tokens: usize,
    runtime_cpu_thread_budget: usize,
    sync_pool_parallelism: bool,
    fixture_allocator: []const u8,
    production_allocator: []const u8,
    timing_boundary: []const u8,
    caveats: []const []const u8,
    memory: MemorySnapshot,
    validation_preflights: ValidationPreflights = .{},
    measurement_scope: []const u8 = "validated_test_fixture_latency",
};

pub const ValidationPreflights = struct {
    direct: usize = 1,
    http_handler: usize = 1,
};

pub const Pin = struct {
    sha256: []const u8,
    size_bytes: usize,
};

pub const SidecarPins = struct {
    config_json: Pin,
    encoder_config_json: Pin,
    tokenizer_json: Pin,
    tokenizer_config_json: Pin,
};

pub const IdleLedger = struct {
    host_weight_bytes: usize,
    backend_weight_bytes: usize,
    host_kv_bytes: usize,
    backend_kv_bytes: usize,
    host_scratch_bytes: usize,
    backend_scratch_bytes: usize,
};

pub const MemorySnapshot = struct {
    process_footprint_bytes: u64,
    process_rss_bytes: u64,
    idle_ledger: IdleLedger,
    ownership: ?OwnershipBreakdown = null,
    external_max_rss_source: []const u8 = "/usr/bin/time -l campaign wrapper",
};

pub const OwnershipBreakdown = struct {
    model: IdleLedger,
    tokenizer_load: IdleLedger,
    tokenizer_cache: IdleLedger,
    weight_cache: IdleLedger,
    workspace: IdleLedger,
};

pub const SampleSet = struct {
    cold_ns: ?u64 = null,
    samples_ns: [measured_count]u64 = undefined,
    len: usize = 0,

    pub fn init() SampleSet {
        return .{};
    }

    pub fn recordCold(self: *SampleSet, elapsed_ns: u64) !void {
        if (self.cold_ns != null) return error.DuplicateGliner25ColdSample;
        self.cold_ns = elapsed_ns;
    }

    pub fn appendMeasured(self: *SampleSet, elapsed_ns: u64) !void {
        if (self.len == measured_count) return error.TooManyGliner25PerfSamples;
        self.samples_ns[self.len] = elapsed_ns;
        self.len += 1;
    }

    pub fn write(self: *const SampleSet, allocator: std.mem.Allocator, config: Config, metadata: Metadata) !void {
        if (self.len != measured_count) return error.IncompleteGliner25PerfSamples;
        try safeComponent(metadata.profile);
        try safeComponent(metadata.backend);
        try safeComponent(metadata.case_id);
        try safeComponent(metadata.path);

        var sorted = self.samples_ns;
        std.mem.sort(u64, &sorted, {}, struct {
            fn lessThan(_: void, left: u64, right: u64) bool {
                return left < right;
            }
        }.lessThan);
        var total: u128 = 0;
        for (self.samples_ns) |sample| total += sample;
        const mean_ns = @as(f64, @floatFromInt(total)) / @as(f64, measured_count);
        const median_ns = (@as(f64, @floatFromInt(sorted[measured_count / 2 - 1])) +
            @as(f64, @floatFromInt(sorted[measured_count / 2]))) / 2.0;
        const p95_index = @min(measured_count - 1, (measured_count * 95 + 99) / 100 - 1);
        const p95_ns = sorted[p95_index];
        const serial_rps = if (mean_ns == 0) 0 else @as(f64, @floatFromInt(std.time.ns_per_s)) / mean_ns;
        const report = .{
            .schema = "antfly.gliner25_family_service_perf.v1",
            .qualification = false,
            .profile = metadata.profile,
            .backend = metadata.backend,
            .case_id = metadata.case_id,
            .path = metadata.path,
            .model = .{
                .repo = metadata.model_repo,
                .revision = metadata.model_revision,
                .sha256 = metadata.model_sha256,
                .size_bytes = metadata.model_size_bytes,
                .sidecars = .{
                    .@"config.json" = metadata.sidecars.config_json,
                    .@"encoder_config/config.json" = metadata.sidecars.encoder_config_json,
                    .@"tokenizer.json" = metadata.sidecars.tokenizer_json,
                    .@"tokenizer_config.json" = metadata.sidecars.tokenizer_config_json,
                },
            },
            .capture_sha256 = metadata.capture_sha256,
            .source_head = metadata.source_head,
            .source_diff_sha256 = metadata.source_diff_sha256,
            .binary_sha256 = metadata.binary_sha256,
            .build_mode = @tagName(builtin.mode),
            .zig_version = builtin.zig_version_string,
            .runtime_cpu_thread_budget = metadata.runtime_cpu_thread_budget,
            .sync_pool_parallelism = metadata.sync_pool_parallelism,
            .fixture_allocator = metadata.fixture_allocator,
            .production_allocator = metadata.production_allocator,
            .measurement_scope = metadata.measurement_scope,
            .input_bytes = metadata.input_bytes,
            .prepared_tokens = metadata.prepared_tokens,
            .timing_boundary = metadata.timing_boundary,
            .caveats = metadata.caveats,
            .memory = metadata.memory,
            .validation_preflights = metadata.validation_preflights,
            .warmups = warmup_count,
            .warmups_follow_validation_preflight = true,
            .measured_samples = measured_count,
            .cold_first_direct_ns = self.cold_ns,
            .mean_ms = mean_ns / std.time.ns_per_ms,
            .median_ms = median_ns / std.time.ns_per_ms,
            .p95_ms = @as(f64, @floatFromInt(p95_ns)) / std.time.ns_per_ms,
            .serial_rps = serial_rps,
            .samples_ns = self.samples_ns,
        };
        const bytes = try std.json.Stringify.valueAlloc(allocator, report, .{});
        defer allocator.free(bytes);
        try std.Io.Dir.cwd().createDirPath(std.testing.io, config.output_dir);
        const filename = try std.fmt.allocPrint(allocator, "gliner25-{s}-{s}-{s}-{s}.json", .{ metadata.profile, metadata.backend, metadata.case_id, metadata.path });
        defer allocator.free(filename);
        const output_path = try std.fs.path.join(allocator, &.{ config.output_dir, filename });
        defer allocator.free(output_path);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = output_path, .data = bytes });
    }
};

pub fn nowNs() u64 {
    return platform.time.monotonicNs();
}

fn safeComponent(value: []const u8) !void {
    if (value.len == 0) return error.InvalidGliner25PerfComponent;
    for (value) |char| if (!std.ascii.isAlphanumeric(char) and char != '_' and char != '-')
        return error.InvalidGliner25PerfComponent;
}

test "GLiNER family performance summary requires the complete sample count" {
    var samples = SampleSet.init();
    try samples.recordCold(7);
    try std.testing.expectError(error.DuplicateGliner25ColdSample, samples.recordCold(8));
    for (0..measured_count) |index| try samples.appendMeasured(@intCast(index + 1));
    try std.testing.expectError(error.TooManyGliner25PerfSamples, samples.appendMeasured(21));
    try safeComponent("multi_v1");
    try std.testing.expectError(error.InvalidGliner25PerfComponent, safeComponent("../escape"));
}
