// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test-driver contracts for independent native whole-head answer and NLL
//! measurements. These receipts never confer serving or release authority.
const std = @import("std");
const checkpoint = @import("attention_match_checkpoint.zig");
const teacher = @import("../../backends/context_teacher_forced.zig");
const allocations = @import("host_allocation_tracker.zig");
const queries = @import("attention_match_queries.zig");
const native_release = @import("attention_match_release.zig");

pub const max_output_tokens = 256;
pub const Request = struct {
    encoding: checkpoint.Encoding,
    output_tokens: usize = max_output_tokens,
    minimum_savings_ratio: f64 = 1.5,

    pub fn capacity(self: Request, prompt_tokens: usize, model_limit: usize) !usize {
        if (self.encoding == .f32 or self.output_tokens == 0 or self.output_tokens > max_output_tokens or
            !std.math.isFinite(self.minimum_savings_ratio) or self.minimum_savings_ratio < 1.5 or self.minimum_savings_ratio > 4)
            return error.InvalidNativeQualityRequest;
        const capacity_value = try std.math.add(usize, prompt_tokens, self.output_tokens);
        // The private query repeat may exceed the wire window, but must stay
        // inside the actual model's position limit. It is never published.
        const query_end = try std.math.add(usize, prompt_tokens, 512);
        if (prompt_tokens < 2 or capacity_value > checkpoint.max_context_tokens or query_end > model_limit)
            return error.CompactionQueryLimit;
        return capacity_value;
    }
};

pub const Frozen = struct {
    identity: checkpoint.Identity,
    observed: native_release.Observed,
    execution: native_release.Execution,
    configuration_sha256: [32]u8,
    implementation_sha256: [32]u8,
    prompt_sha256: [32]u8,
    checkpoint_sha256: [32]u8,
    installed_sha256: [32]u8,
    source_checkpoint_sha256: [32]u8,
    calibration: queries.Seal,
    source_revision: u64,
    destination_revision_before_install: u64,
    installed_revision: u64,
    logical_tokens: usize,
    completed_chunks: usize,
    validated_heads: usize,
    preserved_heads: usize = 0,
    protected_prefix_rows: usize,
    protected_recent_rows: usize,
    fp8_heads: usize = 0,
    fp8_fallback_heads: usize = 0,
    rotated4_heads: usize = 0,
    rotated4_fallback_heads: usize = 0,

    pub fn validate(self: Frozen) !void {
        self.observed.destination.validate() catch return error.InvalidNativeQualityPublication;
        self.execution.validate() catch return error.InvalidNativeQualityPublication;
        if (std.mem.allEqual(u8, &self.observed.backend_environment_sha256, 0) or
            !std.mem.eql(u8, &self.configuration_sha256, &native_release.configurationFingerprint(self.execution, self.observed.backend_environment_sha256)))
            return error.InvalidNativeQualityPublication;
        inline for (.{ "weights", "tokenizer", "template", "layout" }) |field|
            if (std.mem.allEqual(u8, &@field(self.identity, field), 0)) return error.InvalidNativeQualityPublication;
        inline for (.{ "implementation_sha256", "prompt_sha256", "checkpoint_sha256", "installed_sha256", "source_checkpoint_sha256" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidNativeQualityPublication;
        self.calibration.validate() catch return error.InvalidNativeQualityPublication;
        if (!std.mem.eql(u8, &self.checkpoint_sha256, &self.installed_sha256) or
            self.source_revision == std.math.maxInt(u64) or self.destination_revision_before_install == std.math.maxInt(u64) or
            self.installed_revision != self.destination_revision_before_install + 1 or
            self.calibration.source_length != self.logical_tokens or self.calibration.repeat_length != 512 or
            self.calibration.train_positions != 256 or self.calibration.heldout_positions != 64 or
            self.logical_tokens < 2 or self.logical_tokens > checkpoint.max_context_tokens or
            self.protected_prefix_rows < 128 or self.protected_recent_rows < 2048 or
            self.protected_prefix_rows >= self.logical_tokens or
            self.protected_recent_rows >= self.logical_tokens - self.protected_prefix_rows or
            self.completed_chunks == 0 or self.validated_heads == 0 or
            self.fp8_heads > self.validated_heads or self.rotated4_heads > self.validated_heads - self.fp8_heads or
            self.fp8_fallback_heads > self.validated_heads or self.rotated4_fallback_heads > self.validated_heads)
            return error.InvalidNativeQualityPublication;
    }
};

pub const Reference = struct {
    count: usize,
    sha256: [32]u8,
    provider: teacher.ReferenceProvider,

    pub fn reveal(self: Reference, frozen: Frozen, capacity: usize) ![]const i64 {
        try frozen.validate();
        if (self.count < teacher.min_reference_tokens or self.count > capacity or
            std.mem.allEqual(u8, &self.sha256, 0)) return error.InvalidNativeQualityReference;
        const tokens = try self.provider.reveal();
        if (tokens.len != self.count or
            !std.mem.eql(u8, &self.sha256, &try teacher.referenceSequenceDigest(tokens)))
            return error.InvalidNativeQualityReference;
        return tokens;
    }
};

pub const Greedy = struct {
    tokens: [max_output_tokens]i64 = @splat(0),
    count: usize = 0,
    hit_eos: bool = false,
    forward_steps: usize = 0,
    prepared_forward_steps: usize = 0,
    serving_forward_steps: usize = 0,
    ordinary_serving_forward_steps: usize = 0,
    ordinary_backend_frames: usize = 0,
    cold_staged_forward_steps: usize = 0,
    prepared_layer_blocks: usize = 0,
    owner_id: u64 = 0,
    public_text: []u8 = &.{},
    release: allocations.RequestAllocationOwner.ReleaseReceipt = .{},
};

pub const Scores = struct {
    log_probabilities: [max_output_tokens]f64 = @splat(0),
    count: usize = 0,
    nll: f64 = 0,
    forward_steps: usize = 0,
    prepared_forward_steps: usize = 0,
    serving_forward_steps: usize = 0,
    ordinary_serving_forward_steps: usize = 0,
    ordinary_backend_frames: usize = 0,
    cold_staged_forward_steps: usize = 0,
    prepared_layer_blocks: usize = 0,
    attention_calls: usize = 0,
    donor_appends: usize = 0,
    owner_id: u64 = 0,
    release: allocations.RequestAllocationOwner.ReleaseReceipt = .{},
};

/// Public checkpoint serving can use its staged installed-attention path on
/// the first cold step while model slots become ready. Later steps must use
/// every prepared layer tail; partial execution never proves the route.
pub fn validatePreparedStep(blocks: u64, layers: usize, first: bool) !void {
    if (layers == 0 or (blocks != layers and !(first and blocks == 0)))
        return error.InvalidNativeQualityPreparedRoute;
}

/// Parent-clock durations of the real serial collector phases. These are
/// diagnostics, separate from serving throughput or destination-soak evidence.
pub const PhaseDurations = struct {
    exact_prefill: u64 = 0,
    query_collection: u64 = 0,
    fit: u64 = 0,
    transport: u64 = 0,
    install: u64 = 0,
    model_slots: u64 = 0,
    exact_greedy: u64 = 0,
    compacted_greedy: u64 = 0,
    reference_reveal: u64 = 0,
    exact_teacher: u64 = 0,
    compacted_teacher: u64 = 0,
    teardown: u64 = 0,
};

pub const Report = struct {
    allocator: std.mem.Allocator,
    frozen: Frozen,
    calibration_consumers: [256]queries.Consumer = @splat(std.mem.zeroes(queries.Consumer)),
    phase_durations_ns: PhaseDurations = .{},
    source_serving_prefill: bool = false,
    collection_elapsed_ns: u64 = 0,
    model_slot_runtime_kv_bytes_before: u64 = 0,
    model_slot_runtime_kv_bytes_after: u64 = 0,
    continuation_runtime_kv_bytes: u64 = 0,
    compacted_greedy_runtime_kv_bytes_before: u64 = 0,
    compacted_greedy_runtime_kv_bytes_after: u64 = 0,
    compacted_teacher_runtime_kv_bytes_before: u64 = 0,
    compacted_teacher_runtime_kv_bytes_after: u64 = 0,
    model_slots_prepared_without_kv_allocation: bool = false,
    exact_greedy: Greedy = .{},
    compacted_greedy: Greedy = .{},
    exact_teacher: Scores = .{},
    compacted_teacher: Scores = .{},
    reference_sha256: [32]u8 = @splat(0),
    reference_revealed_after_both_greedy_branches: bool = false,
    source_unchanged: bool = false,
    source_owner_id: u64 = 0,
    compacted_owner_id: u64 = 0,
    query_scope_released: bool = false,
    query_scope_delta_bytes: ?u64 = null,
    admitted_peak_bytes: usize = 0,
    source_global_bytes: usize = 0,
    candidate_global_bytes: usize = 0,
    max_head_mass_error: f64 = 0,
    max_head_output_rmse: f64 = 0,
    fp64_accelerated_value_solves: u64 = 0,
    source_release: allocations.RequestAllocationOwner.ReleaseReceipt = .{},
    compacted_release: allocations.RequestAllocationOwner.ReleaseReceipt = .{},
    collection_complete: bool = false,

    pub fn deinit(self: *Report) void {
        self.allocator.free(self.exact_greedy.public_text);
        self.allocator.free(self.compacted_greedy.public_text);
        self.exact_greedy.public_text = &.{};
        self.compacted_greedy.public_text = &.{};
    }
};

test "attention_match Metal quality admission keeps query clones private and references opaque" {
    const request: Request = .{ .encoding = .f16 };
    try std.testing.expectEqual(@as(usize, 32768), try request.capacity(32512, 131072));
    try std.testing.expectError(error.CompactionQueryLimit, request.capacity(32512, 32768));
    try std.testing.expectError(error.CompactionQueryLimit, request.capacity(32513, 131072));
    const Provider = struct {
        calls: usize = 0,
        fn reveal(raw: *anyopaque) ![]const i64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return &.{ 7, 9 };
        }
    };
    var provider: Provider = .{};
    const reference: Reference = .{ .count = 2, .sha256 = try teacher.referenceSequenceDigest(&.{ 7, 9 }), .provider = .{ .ctx = &provider, .reveal_fn = Provider.reveal } };
    var calibration: queries.Seal = .{
        .source_length = 7936,
        .repeat_length = 512,
        .consumer_count = 1,
        .batch_count = 1,
        .train_positions = 256,
        .heldout_positions = 64,
        .train_elements = 512,
        .heldout_elements = 128,
        .geometry_sha256 = @splat(9),
        .train_positions_sha256 = @splat(10),
        .heldout_positions_sha256 = @splat(11),
        .train_values_sha256 = @splat(12),
        .heldout_values_sha256 = @splat(13),
        .sha256 = @splat(0),
    };
    calibration.sha256 = calibration.digest();
    var frozen: Frozen = .{
        .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
        .observed = .{ .destination = .{ .device_registry_id = 1, .physical_memory_bytes = 24 * 1024 * 1024 * 1024, .device_name_sha256 = @splat(13), .compiler_identity_sha256 = @splat(14) }, .backend_environment_sha256 = @splat(15) },
        .execution = .{ .forward_frame = true, .precomputed_tail = true },
        .configuration_sha256 = @splat(0),
        .implementation_sha256 = @splat(5),
        .prompt_sha256 = @splat(6),
        .checkpoint_sha256 = @splat(7),
        .installed_sha256 = @splat(7),
        .source_checkpoint_sha256 = @splat(8),
        .calibration = calibration,
        .source_revision = 1,
        .destination_revision_before_install = 1,
        .installed_revision = 2,
        .logical_tokens = 7936,
        .completed_chunks = 1,
        .validated_heads = 3,
        .protected_prefix_rows = 128,
        .protected_recent_rows = 2048,
    };
    frozen.configuration_sha256 = native_release.configurationFingerprint(frozen.execution, frozen.observed.backend_environment_sha256);
    frozen.installed_sha256 = @splat(0);
    try std.testing.expectError(error.InvalidNativeQualityPublication, reference.reveal(frozen, 256));
    try std.testing.expectEqual(@as(usize, 0), provider.calls);
    frozen.installed_sha256 = @splat(8);
    try std.testing.expectError(error.InvalidNativeQualityPublication, reference.reveal(frozen, 256));
    try std.testing.expectEqual(@as(usize, 0), provider.calls);
    frozen.installed_sha256 = frozen.checkpoint_sha256;
    // The source and installation destination have independent revisions.
    frozen.source_revision = 50;
    try frozen.validate();
    frozen.destination_revision_before_install += 1;
    try std.testing.expectError(error.InvalidNativeQualityPublication, frozen.validate());
    frozen.destination_revision_before_install -= 1;
    frozen.calibration.train_values_sha256[0] ^= 1;
    try std.testing.expectError(error.InvalidNativeQualityPublication, reference.reveal(frozen, 256));
    try std.testing.expectEqual(@as(usize, 0), provider.calls);
    frozen.calibration.train_values_sha256[0] ^= 1;
    try std.testing.expectEqualSlices(i64, &.{ 7, 9 }, try reference.reveal(frozen, 256));
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
}

test "attention_match Metal quality route permits observed cold staging and rejects warm or partial fallbacks" {
    try validatePreparedStep(0, 35, true);
    try validatePreparedStep(35, 35, true);
    try validatePreparedStep(35, 35, false);
    try validatePreparedStep(42, 42, false);
    try std.testing.expectError(error.InvalidNativeQualityPreparedRoute, validatePreparedStep(0, 35, false));
    try std.testing.expectError(error.InvalidNativeQualityPreparedRoute, validatePreparedStep(34, 35, true));
    try std.testing.expectError(error.InvalidNativeQualityPreparedRoute, validatePreparedStep(36, 35, false));
    try std.testing.expectError(error.InvalidNativeQualityPreparedRoute, validatePreparedStep(0, 0, true));
}
