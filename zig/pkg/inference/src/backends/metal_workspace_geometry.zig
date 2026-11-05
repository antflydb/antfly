// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold logical geometry from the SAME pure C helpers used by the SDK
//! producers. No runtime/provider/Model pointer, callback, allocator or IO.
//! Physical admission must still query the concrete device/mode allocation
//! bound and pin the actual current backing; logical capacity grants no credit.
const std = @import("std");
const build_options = @import("build_options");

pub const Greedy = extern struct {
    scratch_bytes: usize,
    token_bytes: usize,
};
pub const Staging = extern struct {
    offset_bytes: usize,
    required_bytes: usize,
    next_offset_bytes: usize,
    capacity_bytes: usize,
};
pub const GreedyInput = struct {
    hidden: usize,
    intermediate: usize,
    layers: usize,
    heads: usize,
    kv_heads: usize,
    head_dim: usize,
    vocab: usize,
    kv_tokens: usize,
};
pub const StagingInput = struct {
    capacity_bytes: usize,
    offset_bytes: usize,
    length_bytes: usize,
    buffer_present: bool,
    standalone: bool,
};

extern fn antfly_metal_greedy_workspace_requirements(hidden: usize, intermediate: usize, layers: usize, heads: usize, kv_heads: usize, head_dim: usize, vocab: usize, kv_tokens: usize, output: *Greedy) c_int;
extern fn antfly_metal_host_staging_requirements(current_capacity: usize, current_offset: usize, length: usize, buffer_present: u32, standalone: u32, output: *Staging) c_int;

fn checked(status: c_int) !void {
    return switch (status) {
        0 => {},
        -3 => error.WorkspaceGeometryOverflow,
        else => error.InvalidWorkspaceGeometry,
    };
}
pub fn greedy(input: GreedyInput) !Greedy {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var result: Greedy = undefined;
    try checked(antfly_metal_greedy_workspace_requirements(input.hidden, input.intermediate, input.layers, input.heads, input.kv_heads, input.head_dim, input.vocab, input.kv_tokens, &result));
    return result;
}
/// Invoke once for standalone staging, or for each ACTUAL selected one of the
/// two frame banks with its own current capacity/cursor. Never combine two
/// independent interval peaks or invent capacity from an unrelated bank.
/// Standalone preserves offset_bytes as next_offset_bytes without using it.
pub fn staging(input: StagingInput) !Staging {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var result: Staging = undefined;
    try checked(antfly_metal_host_staging_requirements(input.capacity_bytes, input.offset_bytes, input.length_bytes, @intFromBool(input.buffer_present), @intFromBool(input.standalone), &result));
    return result;
}

test "Metal cold greedy geometry shares exact producer sizes and rejects overflow without partial output" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(2 * @sizeOf(usize), @sizeOf(Greedy));
    const cases = [_]struct { input: GreedyInput, scratch: usize }{
        .{ .input = .{ .hidden = 64, .intermediate = 128, .layers = 1, .heads = 2, .kv_heads = 1, .head_dim = 32, .vocab = 64, .kv_tokens = 0 }, .scratch = 6656 },
        .{ .input = .{ .hidden = 128, .intermediate = 256, .layers = 2, .heads = 1, .kv_heads = 1, .head_dim = 128, .vocab = 64, .kv_tokens = 0 }, .scratch = 9728 },
        .{ .input = .{ .hidden = 1024, .intermediate = 128, .layers = 2, .heads = 8, .kv_heads = 2, .head_dim = 256, .vocab = 64, .kv_tokens = 16 }, .scratch = 37952 },
    };
    for (cases) |case| {
        const result = try greedy(case.input);
        try std.testing.expectEqual(case.scratch, result.scratch_bytes);
        try std.testing.expectEqual(@as(usize, 4), result.token_bytes);
        var many_layers = case.input;
        many_layers.layers = 256; // SAME physical scratch, not a per-layer sum
        try std.testing.expectEqualDeep(result, try greedy(many_layers));
    }
    var invalid = cases[0].input;
    invalid.layers = 0;
    try std.testing.expectError(error.InvalidWorkspaceGeometry, greedy(invalid));
    invalid = cases[0].input;
    invalid.hidden = std.math.maxInt(usize);
    try std.testing.expectError(error.WorkspaceGeometryOverflow, greedy(invalid));
    invalid = cases[0].input;
    invalid.heads = std.math.maxInt(usize);
    try std.testing.expectError(error.WorkspaceGeometryOverflow, greedy(invalid));
    invalid = cases[0].input;
    invalid.kv_tokens = std.math.maxInt(usize);
    try std.testing.expectError(error.WorkspaceGeometryOverflow, greedy(invalid));
    var retained = Greedy{ .scratch_bytes = 17, .token_bytes = 19 };
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_greedy_workspace_requirements(std.math.maxInt(usize), 128, 1, 2, 1, 32, 64, 0, &retained));
    try std.testing.expectEqualDeep(Greedy{ .scratch_bytes = 17, .token_bytes = 19 }, retained);
}

test "Metal cold staging geometry preserves standalone reuse growth and independent frame cursors" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(4 * @sizeOf(usize), @sizeOf(Staging));
    const first = try staging(.{ .capacity_bytes = 0, .offset_bytes = 99, .length_bytes = 4, .buffer_present = false, .standalone = true });
    try std.testing.expectEqualDeep(Staging{ .offset_bytes = 0, .required_bytes = 4, .next_offset_bytes = 99, .capacity_bytes = 4096 }, first);
    const fits = try staging(.{ .capacity_bytes = 32, .offset_bytes = 99, .length_bytes = 16, .buffer_present = true, .standalone = true });
    try std.testing.expectEqual(@as(usize, 32), fits.capacity_bytes); // existing small backing is reused
    const absent = try staging(.{ .capacity_bytes = 32, .offset_bytes = 99, .length_bytes = 16, .buffer_present = false, .standalone = true });
    try std.testing.expectEqual(@as(usize, 4096), absent.capacity_bytes);
    const doubled = try staging(.{ .capacity_bytes = 7000, .offset_bytes = 0, .length_bytes = 7001, .buffer_present = true, .standalone = true });
    try std.testing.expectEqual(@as(usize, 14000), doubled.capacity_bytes); // actual lineage, not rounded from zero
    const bank0 = try staging(.{ .capacity_bytes = 4096, .offset_bytes = 1, .length_bytes = 4, .buffer_present = true, .standalone = false });
    try std.testing.expectEqualDeep(Staging{ .offset_bytes = 256, .required_bytes = 260, .next_offset_bytes = 512, .capacity_bytes = 4096 }, bank0);
    const bank1 = try staging(.{ .capacity_bytes = 4096, .offset_bytes = 4090, .length_bytes = 16, .buffer_present = true, .standalone = false });
    try std.testing.expectEqualDeep(Staging{ .offset_bytes = 4096, .required_bytes = 4112, .next_offset_bytes = 4352, .capacity_bytes = 8192 }, bank1);
    const next0 = try staging(.{ .capacity_bytes = bank0.capacity_bytes, .offset_bytes = bank0.next_offset_bytes, .length_bytes = 8, .buffer_present = true, .standalone = false });
    try std.testing.expectEqual(@as(usize, 512), next0.offset_bytes);
    try std.testing.expectEqual(@as(usize, 768), next0.next_offset_bytes);
    try std.testing.expectEqual(@as(usize, 4352), bank1.next_offset_bytes); // untouched peer bank
}

test "Metal cold staging rejects invalid or overflowing cursor before output or SDK mutation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectError(error.InvalidWorkspaceGeometry, staging(.{ .capacity_bytes = 0, .offset_bytes = 0, .length_bytes = 0, .buffer_present = false, .standalone = true }));
    try std.testing.expectError(error.WorkspaceGeometryOverflow, staging(.{ .capacity_bytes = 0, .offset_bytes = std.math.maxInt(usize), .length_bytes = 4, .buffer_present = false, .standalone = false }));
    try std.testing.expectError(error.WorkspaceGeometryOverflow, staging(.{ .capacity_bytes = 0, .offset_bytes = 0, .length_bytes = std.math.maxInt(usize), .buffer_present = false, .standalone = false }));
    var retained = Staging{ .offset_bytes = 11, .required_bytes = 13, .next_offset_bytes = 17, .capacity_bytes = 19 };
    const original = retained;
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_host_staging_requirements(0, std.math.maxInt(usize), 4, 0, 0, &retained));
    try std.testing.expectEqualDeep(original, retained);
    try std.testing.expectEqual(@as(c_int, -2), antfly_metal_host_staging_requirements(0, 0, 4, 2, 0, &retained));
    try std.testing.expectEqualDeep(original, retained);
    // A standalone SIZE_MAX extent does not overflow logical geometry. The
    // separate concrete device/mode allocation-bound query must reject it.
    const logical_only = try staging(.{ .capacity_bytes = 0, .offset_bytes = 0, .length_bytes = std.math.maxInt(usize), .buffer_present = false, .standalone = true });
    try std.testing.expectEqual(std.math.maxInt(usize), logical_only.capacity_bytes);
}

pub const Rows = extern struct { output_bytes: usize, index_bytes: usize };
pub const Pool = extern struct { input_bytes: usize, output_bytes: usize, metadata_bytes: usize };
extern fn antfly_metal_row_workspace_requirements(rows: usize, width: usize, output: *Rows) c_int;
extern fn antfly_metal_pool_workspace_requirements(batch: usize, rows: usize, width: usize, dimensions: usize, segmented: u32, output: *Pool) c_int;

pub fn rows(count: usize, width: usize) !Rows {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var result: Rows = undefined;
    try checked(antfly_metal_row_workspace_requirements(count, width, &result));
    return result;
}
pub fn pool(batch: usize, sequence: usize, width: usize, dimensions: usize, segmented: bool) !Pool {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var result: Pool = undefined;
    try checked(antfly_metal_pool_workspace_requirements(batch, sequence, width, dimensions, @intFromBool(segmented), &result));
    return result;
}

/// ONE actual embedding lookup before the graph frame: Private F32 output,
/// retained Shared ID staging span, and distinct temporary CPU u32 IDs. Feed
/// the span to staging() with the real bank/capacity/cursor before admission.
/// The model's weight table is a different actual owner, never declared here.
pub const EmbeddingRoles = struct {
    output_private_bytes: usize,
    ids_staging_span_bytes: usize,
    ids_cpu_bytes: usize,
};
pub fn embeddingRoles(tokens: usize, hidden: usize) !EmbeddingRoles {
    const value = try rows(tokens, hidden);
    return .{ .output_private_bytes = value.output_bytes, .ids_staging_span_bytes = value.index_bytes, .ids_cpu_bytes = value.index_bytes };
}

/// ONE actual takeRows call. Upload storage is an exact selected caller fact,
/// not inferred from a default/environment threshold. Shared IDs have no
/// additional upload staging; Private IDs require a separate same-sized
/// Shared SDK staging object. CPU F32 IDs are a third, separate allocation.
pub const TakeRowsRoles = struct {
    output_private_bytes: usize,
    ids_device_bytes: usize,
    ids_upload_shared_bytes: usize,
    ids_cpu_bytes: usize,
};
pub fn takeRowsRoles(batch: usize, hidden: usize, ids_are_private: bool) !TakeRowsRoles {
    const value = try rows(batch, hidden);
    return .{ .output_private_bytes = value.output_bytes, .ids_device_bytes = value.index_bytes, .ids_upload_shared_bytes = if (ids_are_private) value.index_bytes else 0, .ids_cpu_bytes = value.index_bytes };
}

/// Actual selected RMS+scalar L2 tail: the RMS backing remains alive while
/// scalar reads it. These are TWO live allocations, not independent peaks.
/// A unit-weight cache hit creates no new device/host/staging allocation.
/// The derived constant's SDK storage mode remains an explicit caller fact.
pub const DirectNormalizeRoles = struct {
    rms_private_bytes: usize,
    scale_private_bytes: usize,
    unit_device_bytes: usize,
    unit_upload_shared_bytes: usize,
    unit_cpu_bytes: usize,
};
pub fn directNormalizeRoles(batch: usize, hidden: usize, unit_missing: bool, unit_is_private: bool) !DirectNormalizeRoles {
    const output = try rows(batch, hidden);
    const unit = if (unit_missing) (try rows(1, hidden)).output_bytes else 0;
    return .{ .rms_private_bytes = output.output_bytes, .scale_private_bytes = output.output_bytes, .unit_device_bytes = unit, .unit_upload_shared_bytes = if (unit_is_private) unit else 0, .unit_cpu_bytes = unit };
}

/// Actual selected maskedMeanBatch (+optional in-place normalization): ONE
/// Private output and ONE metadata span in the real Shared staging bank.
/// mean uses sequence=S, segmented=false; normalization of already pooled
/// last/cls/max rows uses sequence=1, segmented=false (metadata=16*B).
/// forwarded_mask is true only for the actual sequence_pool adapter, whose
/// distinct i64 mask stays live while MetalCompute builds backend metadata.
/// Segmented SDK geometry is available through pool(); it is intentionally
/// excluded here because its actual caller uses a fixed stack metadata array.
/// This does NOT describe upstream generic cls/max pooling or a declined
/// fused path; those need their actual selected owner's declaration.
pub const FusedPoolRoles = struct {
    output_private_bytes: usize,
    metadata_staging_span_bytes: usize,
    metadata_cpu_bytes: usize,
    forwarded_mask_cpu_bytes: usize,
};
pub fn fusedPoolRoles(batch: usize, sequence: usize, width: usize, dimensions: usize, forwarded_mask: bool) !FusedPoolRoles {
    const value = try pool(batch, sequence, width, dimensions, false);
    const mask = if (forwarded_mask) try std.math.mul(usize, (try rows(batch, sequence)).output_bytes, 2) else 0;
    return .{ .output_private_bytes = value.output_bytes, .metadata_staging_span_bytes = value.metadata_bytes, .metadata_cpu_bytes = value.metadata_bytes, .forwarded_mask_cpu_bytes = mask };
}

/// Actual Private-buffer readback creates ONE same-sized Shared SDK staging
/// object; a Shared input uses its own contents and creates none. Host output
/// allocation belongs to the existing output envelope, not SDK Root credit.
pub fn readbackSharedBytes(count: usize, width: usize, source_is_private: bool) !usize {
    const value = try rows(count, width);
    return if (source_is_private) value.output_bytes else 0;
}

/// In-place row normalization aliases the already declared output. No new
/// output, unit cache or upload/staging allocation is created by that body.
pub const inplace_normalize_additional_bytes: usize = 0;

test "Metal selected tail roles preserve distinct CPU SDK and live output owners without alternative sums" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const embed = try embeddingRoles(32, 128);
    try std.testing.expectEqualDeep(EmbeddingRoles{ .output_private_bytes = 16384, .ids_staging_span_bytes = 128, .ids_cpu_bytes = 128 }, embed);
    try std.testing.expectEqual(@as(usize, 4096), (try staging(.{ .capacity_bytes = 0, .offset_bytes = 0, .length_bytes = embed.ids_staging_span_bytes, .buffer_present = false, .standalone = true })).capacity_bytes);
    const shared_ids = try takeRowsRoles(2, 128, false);
    try std.testing.expectEqualDeep(TakeRowsRoles{ .output_private_bytes = 1024, .ids_device_bytes = 8, .ids_upload_shared_bytes = 0, .ids_cpu_bytes = 8 }, shared_ids);
    const private_ids = try takeRowsRoles(2, 128, true);
    try std.testing.expectEqual(@as(usize, 8), private_ids.ids_upload_shared_bytes);
    const cold = try directNormalizeRoles(2, 128, true, true);
    try std.testing.expectEqualDeep(DirectNormalizeRoles{ .rms_private_bytes = 1024, .scale_private_bytes = 1024, .unit_device_bytes = 512, .unit_upload_shared_bytes = 512, .unit_cpu_bytes = 512 }, cold);
    const warm = try directNormalizeRoles(2, 128, false, true);
    try std.testing.expectEqual(@as(usize, 0), warm.unit_device_bytes + warm.unit_upload_shared_bytes + warm.unit_cpu_bytes);
    try std.testing.expectEqual(@as(usize, 1024), warm.rms_private_bytes);
    try std.testing.expectEqual(@as(usize, 1024), warm.scale_private_bytes);
    const mean = try fusedPoolRoles(2, 16, 128, 128, true);
    try std.testing.expectEqualDeep(FusedPoolRoles{ .output_private_bytes = 1024, .metadata_staging_span_bytes = 272, .metadata_cpu_bytes = 272, .forwarded_mask_cpu_bytes = 256 }, mean);
    const pooled_rows = try fusedPoolRoles(2, 1, 128, 128, false);
    try std.testing.expectEqual(@as(usize, 32), pooled_rows.metadata_staging_span_bytes);
    const segmented = try pool(2, 32, 128, 128, true);
    try std.testing.expectEqual(@as(usize, 32), segmented.metadata_bytes);
    try std.testing.expectEqual(@as(usize, 0), pooled_rows.forwarded_mask_cpu_bytes);
    try std.testing.expectEqual(@as(usize, 1024), try readbackSharedBytes(2, 128, true));
    try std.testing.expectEqual(@as(usize, 0), try readbackSharedBytes(2, 128, false));
    try std.testing.expectEqual(@as(usize, 0), inplace_normalize_additional_bytes);
}

test "Metal shared row and pool geometry rejects overflow before output publication" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(2 * @sizeOf(usize), @sizeOf(Rows));
    try std.testing.expectEqual(3 * @sizeOf(usize), @sizeOf(Pool));
    try std.testing.expectError(error.InvalidWorkspaceGeometry, embeddingRoles(0, 128));
    try std.testing.expectError(error.InvalidWorkspaceGeometry, fusedPoolRoles(65, 16, 128, 128, false));
    try std.testing.expectError(error.InvalidWorkspaceGeometry, fusedPoolRoles(2, 16, 128, 129, false));
    try std.testing.expectError(error.WorkspaceGeometryOverflow, directNormalizeRoles(2, std.math.maxInt(usize), true, false));
    var old_rows = Rows{ .output_bytes = 13, .index_bytes = 17 };
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_row_workspace_requirements(std.math.maxInt(usize), 128, &old_rows));
    try std.testing.expectEqualDeep(Rows{ .output_bytes = 13, .index_bytes = 17 }, old_rows);
    var old_pool = Pool{ .input_bytes = 19, .output_bytes = 23, .metadata_bytes = 29 };
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_pool_workspace_requirements(2, std.math.maxInt(usize), 128, 128, 0, &old_pool));
    try std.testing.expectEqualDeep(Pool{ .input_bytes = 19, .output_bytes = 23, .metadata_bytes = 29 }, old_pool);
}

/// Actual selected .last graph caller holds BOTH arrays across backend
/// takeRows: Pipeline.activeFinalRowIndices i64[B], then the checked u32[B]
/// conversion in gpt.tryDenseQwen3Prefill. These CPU owners are distinct from
/// takeRowsRoles.ids_cpu_bytes (the backend's third, F32[B] allocation) and
/// from every SDK ID/staging buffer. Other selected paths do not declare them.
pub const SelectedRowsCpuRoles = struct {
    active_rows_i64_bytes: usize,
    checked_rows_u32_bytes: usize,
};
pub fn selectedRowsCpuRoles(batch: usize) !SelectedRowsCpuRoles {
    const value = try rows(batch, 1);
    return .{ .active_rows_i64_bytes = std.math.mul(usize, value.index_bytes, 2) catch return error.WorkspaceGeometryOverflow, .checked_rows_u32_bytes = value.index_bytes };
}

test "Metal selected last row caller preserves all three distinct CPU index arrays" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const caller = try selectedRowsCpuRoles(2);
    try std.testing.expectEqualDeep(SelectedRowsCpuRoles{ .active_rows_i64_bytes = 16, .checked_rows_u32_bytes = 8 }, caller);
    const backend = try takeRowsRoles(2, 128, false);
    try std.testing.expectEqual(@as(usize, 8), backend.ids_cpu_bytes);
    try std.testing.expectEqual(@as(usize, 32), caller.active_rows_i64_bytes + caller.checked_rows_u32_bytes + backend.ids_cpu_bytes);
    try std.testing.expectEqual(@as(usize, 0), backend.ids_upload_shared_bytes);
    try std.testing.expectError(error.WorkspaceGeometryOverflow, selectedRowsCpuRoles(std.math.maxInt(usize) / 4));
}

pub const FramePool = extern struct { input_bytes: usize, output_bytes: usize, mask_bytes: usize };
extern fn antfly_metal_frame_pool_workspace_requirements(batch: usize, sequence: usize, hidden: usize, output: *FramePool) c_int;
/// Actual canonical poolNormalize callback: requires an active frame and the
/// selected u32-mask program. Distinct from maskedMeanBatch/pool() above.
pub fn framePool(batch: usize, sequence: usize, hidden: usize) !FramePool {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var output: FramePool = undefined;
    try checked(antfly_metal_frame_pool_workspace_requirements(batch, sequence, hidden, &output));
    return output;
}
pub const FramePoolRoles = struct {
    output_private_bytes: usize,
    mask_staging_span_bytes: usize,
    mask_u32_cpu_bytes: usize,
    forwarded_i64_mask_cpu_bytes: usize,
};
/// sequence_pool supplies a temporary i64 mask while the backend converts it
/// into a separate u32 mask. Only u32 bytes enter the actual Shared frame bank;
/// this path has no i64(B*S+B) metadata and no separate normalization output.
pub fn framePoolRoles(batch: usize, sequence: usize, hidden: usize, forwarded_mask: bool) !FramePoolRoles {
    const value = try framePool(batch, sequence, hidden);
    const forwarded = if (forwarded_mask) std.math.mul(usize, value.mask_bytes, 2) catch return error.WorkspaceGeometryOverflow else 0;
    return .{ .output_private_bytes = value.output_bytes, .mask_staging_span_bytes = value.mask_bytes, .mask_u32_cpu_bytes = value.mask_bytes, .forwarded_i64_mask_cpu_bytes = forwarded };
}

test "Metal canonical frame pool roles match actual u32 program instead of independent i64 batch pool" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(3 * @sizeOf(usize), @sizeOf(FramePool));
    const mean = try framePoolRoles(2, 16, 128, true);
    try std.testing.expectEqualDeep(FramePoolRoles{ .output_private_bytes = 1024, .mask_staging_span_bytes = 128, .mask_u32_cpu_bytes = 128, .forwarded_i64_mask_cpu_bytes = 256 }, mean);
    const single = try framePoolRoles(2, 1, 128, true);
    try std.testing.expectEqual(@as(usize, 8), single.mask_staging_span_bytes);
    try std.testing.expectEqual(@as(usize, 16), single.forwarded_i64_mask_cpu_bytes);
    try std.testing.expectEqual(@as(usize, 32), (try fusedPoolRoles(2, 1, 128, 128, true)).metadata_staging_span_bytes);
    try std.testing.expectEqual(@as(usize, 0), (try framePoolRoles(2, 16, 128, false)).forwarded_i64_mask_cpu_bytes);
    try std.testing.expectError(error.InvalidWorkspaceGeometry, framePoolRoles(0, 16, 128, true));
    try std.testing.expectError(error.WorkspaceGeometryOverflow, framePoolRoles(2, std.math.maxInt(usize), 128, true));
    var old = FramePool{ .input_bytes = 13, .output_bytes = 17, .mask_bytes = 19 };
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_frame_pool_workspace_requirements(2, std.math.maxInt(usize), 128, &old));
    try std.testing.expectEqualDeep(FramePool{ .input_bytes = 13, .output_bytes = 17, .mask_bytes = 19 }, old);
}

/// Exact fixed-slot requests used by the authored dense causal block. These
/// supplement the selected command plan; merge by slot before physical capture.
pub const PrefillScratch = extern struct {
    q_bytes: usize,
    kv_bytes: usize,
    direct_hidden_bytes: usize,
    intermediate_bytes: usize,
    tail_logits_bytes: usize,
    slots: [21]usize,
    bytes: [21]usize,
};
pub const PrefillScratchInput = struct {
    rows: usize,
    heads: usize,
    kv_heads: usize,
    head_dim: usize,
    hidden: usize,
    intermediate: usize,
    tail_vocab: usize = 0,
};
extern fn antfly_metal_prefill_scratch_requirements(rows: usize, heads: usize, kv_heads: usize, head_dim: usize, hidden: usize, intermediate: usize, tail_vocab: usize, output: *PrefillScratch) c_int;
pub fn prefillScratch(input: PrefillScratchInput) !PrefillScratch {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var output: PrefillScratch = undefined;
    try checked(antfly_metal_prefill_scratch_requirements(input.rows, input.heads, input.kv_heads, input.head_dim, input.hidden, input.intermediate, input.tail_vocab, &output));
    return output;
}

test "Metal shared dense prefill scratch includes exact hot intermediate banks and keeps overflow output unchanged" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(47 * @sizeOf(usize), @sizeOf(PrefillScratch));
    const actual = try prefillScratch(.{ .rows = 32, .heads = 1, .kv_heads = 1, .head_dim = 128, .hidden = 128, .intermediate = 256 });
    try std.testing.expectEqual(@as(usize, 16384), actual.q_bytes);
    try std.testing.expectEqual(@as(usize, 32768), actual.intermediate_bytes);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 17, 18, 19, 20, 21, 22, 23, 24, 25 }, &actual.slots);
    for (actual.slots, actual.bytes) |slot, bytes| try std.testing.expectEqual(@as(usize, if (slot == 11) 0 else if (slot >= 21) 32768 else 16384), bytes);
    const asymmetric = try prefillScratch(.{ .rows = 7, .heads = 4, .kv_heads = 2, .head_dim = 32, .hidden = 256, .intermediate = 384, .tail_vocab = 17 });
    try std.testing.expectEqual(@as(usize, 7 * 4 * 32 * 4), asymmetric.q_bytes);
    try std.testing.expectEqual(@as(usize, 7 * 2 * 32 * 4), asymmetric.kv_bytes);
    try std.testing.expectEqual(@as(usize, 7 * 256 * 4), asymmetric.direct_hidden_bytes);
    try std.testing.expectEqual(@as(usize, 68), asymmetric.tail_logits_bytes);
    var unchanged = actual;
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_prefill_scratch_requirements(std.math.maxInt(usize), 2, 1, 128, 128, 256, 0, &unchanged));
    try std.testing.expectEqualDeep(actual, unchanged);
    try std.testing.expectError(error.InvalidWorkspaceGeometry, prefillScratch(.{ .rows = 0, .heads = 1, .kv_heads = 1, .head_dim = 128, .hidden = 128, .intermediate = 256 }));
}

/// SAME single-sequence attention staging geometry, not batch*sequence.
/// Zero sequence denotes an auxiliary family preparation with no attention.
pub const DenseAttention = extern struct {
    sequence: usize = 0,
    heads: usize = 0,
    kv_heads: usize = 0,
    head_dim: usize = 0,
};
extern fn antfly_metal_dense_f16kv_geometry(request: *const DenseAttention, capabilities: u32, output: *usize) c_int;
extern fn antfly_metal_dense_f16kv_replace(capacity: usize, required: usize, key_present: bool, value_present: bool) bool;
pub fn denseF16Kv(input: DenseAttention, capabilities: u32) !usize {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    var result: usize = undefined;
    try checked(antfly_metal_dense_f16kv_geometry(&input, capabilities, &result));
    return result;
}
pub fn denseF16KvReplaces(capacity_bytes: usize, required: usize, key_present: bool, value_present: bool) bool {
    if (comptime !build_options.enable_metal) return false;
    return antfly_metal_dense_f16kv_replace(capacity_bytes, required, key_present, value_present);
}

test "Metal dense attention F16 pair geometry preserves exact selector and joint member replacement" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    try std.testing.expectEqual(4 * @sizeOf(usize), @sizeOf(DenseAttention));
    const selected: DenseAttention = .{ .sequence = 16, .heads = 2, .kv_heads = 1, .head_dim = 128 };
    try std.testing.expectEqual(@as(usize, 4096), try denseF16Kv(selected, 15));
    var large = selected;
    large.sequence = 64;
    large.kv_heads = 2;
    try std.testing.expectEqual(@as(usize, 32768), try denseF16Kv(large, 15));
    inline for (.{ @as(u32, 0), 1, 2, 4, 8, 7, 11, 13, 14 }) |capabilities| try std.testing.expectEqual(@as(usize, 0), try denseF16Kv(selected, capabilities));
    var other = selected;
    other.sequence = 15;
    try std.testing.expectEqual(@as(usize, 0), try denseF16Kv(other, 15));
    other = selected;
    other.head_dim = 256;
    try std.testing.expectEqual(@as(usize, 0), try denseF16Kv(other, 15));
    other.head_dim = 48;
    try std.testing.expectEqual(@as(usize, 0), try denseF16Kv(other, 15));
    try std.testing.expectEqual(@as(usize, 0), try denseF16Kv(.{}, 15));
    try std.testing.expect(!denseF16KvReplaces(4096, 4096, true, true));
    try std.testing.expect(denseF16KvReplaces(4096, 4096, true, false));
    try std.testing.expect(denseF16KvReplaces(4096, 4096, false, true));
    try std.testing.expect(denseF16KvReplaces(4096, 4096, false, false));
    try std.testing.expect(denseF16KvReplaces(4095, 4096, true, true));
    try std.testing.expect(!denseF16KvReplaces(0, 0, false, false));
    other = selected;
    other.sequence = std.math.maxInt(usize);
    var preserved: usize = 17;
    try std.testing.expectEqual(@as(c_int, -3), antfly_metal_dense_f16kv_geometry(&other, 15, &preserved));
    try std.testing.expectEqual(@as(usize, 17), preserved);
    other = selected;
    other.kv_heads = 3;
    try std.testing.expectError(error.InvalidWorkspaceGeometry, denseF16Kv(other, 15));
}

/// CPU reference linears preserve their original host result and defer upload
/// until the original device consumer. This is a value-only declaration of
/// those concrete constructors and host-live intervals; it grants no credit.
pub const ReferenceLayer = struct {
    uploads: [5]usize = @splat(0),
    downloads: [5]usize = @splat(0),
    cpu_peak_bytes: usize,
};
fn referenceSum(values: []const usize) !usize {
    var result: usize = 0;
    for (values) |value| result = try std.math.add(usize, result, value);
    return result;
}
fn referenceMatrixBytes(row_count: usize, width: usize) !usize {
    return std.math.mul(usize, try std.math.mul(usize, row_count, width), @sizeOf(f32));
}

pub fn referenceQuantizedLayer(row_count: usize, hidden: usize, attention_width: usize, kv_width: usize, intermediate: usize, first: bool, shared_upload_limit: usize) !ReferenceLayer {
    return referenceSelectedLayer(row_count, hidden, attention_width, kv_width, intermediate, !first, .{ true, true, true }, true, shared_upload_limit);
}

test "reference quantized layer declares original upload and readback constructors with checked host liveness" {
    // T32,H128,A128,K128,I256: weight and activation lifetimes both matter.
    const first = try referenceQuantizedLayer(32, 128, 128, 128, 256, true, 64 * 1024);
    try std.testing.expectEqualSlices(usize, &.{ 16384, 16384, 16384, 0, 0 }, &first.uploads);
    try std.testing.expectEqual(@as(usize, 311296), first.cpu_peak_bytes);
    try std.testing.expectEqualSlices(usize, &.{ 16384, 16384, 16384, 16384, 16384 }, &first.downloads);
    const later = try referenceQuantizedLayer(32, 128, 128, 128, 256, false, 64 * 1024);
    try std.testing.expectEqualSlices(usize, &.{ 16384, 16384, 16384, 16384, 16384 }, &later.uploads);
    try std.testing.expectEqual(@as(usize, 0), later.downloads[4]);
    try std.testing.expectEqual(@as(usize, 16 * 1024), later.cpu_peak_bytes - first.cpu_peak_bytes);
    const private = try referenceQuantizedLayer(32, 128, 128, 128, 256, false, 16383);
    try std.testing.expectEqual(@as(usize, 16384), private.downloads[4]);
    try std.testing.expectError(error.InvalidWorkspaceGeometry, referenceQuantizedLayer(1, 128, 128, 128, 256, true, 0));
    try std.testing.expectError(error.Overflow, referenceQuantizedLayer(std.math.maxInt(usize), 128, 128, 128, 256, true, 0));
}

/// Original dense-frame dataflow with mixed Q/K/V formats and either the
/// ordinary Host reference tail or the all-dense device tail. Other FFN mixes
/// must be described using their own actual preferred/fallback selector.
pub fn referenceSelectedLayer(row_count: usize, hidden: usize, attention_width: usize, kv_width: usize, intermediate: usize, previous_host: bool, quantized_qkv: [3]bool, host_tail: bool, shared_upload_limit: usize) !ReferenceLayer {
    if (row_count <= 1 or hidden == 0 or attention_width == 0 or kv_width == 0 or intermediate == 0) return error.InvalidWorkspaceGeometry;
    const h = try referenceMatrixBytes(row_count, hidden);
    const q = try referenceMatrixBytes(row_count, attention_width);
    const k = try referenceMatrixBytes(row_count, kv_width);
    const i = try referenceMatrixBytes(row_count, intermediate);
    const previous: usize = if (previous_host) h else 0;
    const host_q: usize = if (quantized_qkv[0]) q else 0;
    const host_k: usize = if (quantized_qkv[1]) k else 0;
    const host_v: usize = if (quantized_qkv[2]) k else 0;
    const live = try referenceSum(&.{ previous, host_q, host_k, host_v });
    var peak = try referenceSum(&.{ previous, if (previous_host) h else 0 });
    if (quantized_qkv[0]) peak = @max(peak, try referenceSum(&.{ previous, h, try referenceMatrixBytes(hidden, attention_width), q }));
    if (quantized_qkv[1]) peak = @max(peak, try referenceSum(&.{ previous, host_q, h, try referenceMatrixBytes(hidden, kv_width), k }));
    if (quantized_qkv[2]) peak = @max(peak, try referenceSum(&.{ previous, host_q, host_k, h, try referenceMatrixBytes(hidden, kv_width), k }));
    // Each Host CT is cloned only for its own synchronous upload, while all
    // original Q/K/V CTs remain live until this dense block returns.
    peak = @max(peak, try referenceSum(&.{ live, @max(host_q, @max(host_k, host_v)) }));
    if (previous_host) peak = @max(peak, try referenceSum(&.{ live, h }));
    const residual_mirror: usize = if (host_tail and (!previous_host or h > shared_upload_limit)) h else 0;
    if (host_tail) {
        const wo = try referenceMatrixBytes(attention_width, hidden);
        const wg = try referenceMatrixBytes(hidden, intermediate);
        const wd = try referenceMatrixBytes(intermediate, hidden);
        const candidates = [_]usize{
            try referenceSum(&.{ live, q, wo, h }),
            try referenceSum(&.{ live, h, h }),
            try referenceSum(&.{ live, h, h, h, residual_mirror }),
            try referenceSum(&.{ live, h, h, h, h }),
            try referenceSum(&.{ live, h, h, h, h, wg, i }),
            try referenceSum(&.{ live, h, h, h, h, i, wg, i }),
            try referenceSum(&.{ live, h, h, h, h, i, i, i }),
            try referenceSum(&.{ live, h, h, h, h, i, i, i, i }),
            // Ordinary runGatedFfnPhaseMt releases gate/up/activation BEFORE
            // the down projection; the product alone remains alongside tail.
            try referenceSum(&.{ live, h, h, h, h, h, i, wd }),
            try referenceSum(&.{ live, h, h, h, h, h, h, i }),
            try referenceSum(&.{ live, h, h, h, h, h, h, h, i }),
            // The last returned Host hidden is cloned for final RMS upload.
            try referenceSum(&.{ h, h }),
        };
        for (candidates) |candidate| peak = @max(peak, candidate);
    }
    return .{
        .uploads = .{ host_q, host_k, host_v, if (previous_host) h else 0, if (previous_host) h else 0 },
        .downloads = .{ if (quantized_qkv[0]) h else 0, if (quantized_qkv[1]) h else 0, if (quantized_qkv[2]) h else 0, if (host_tail) q else 0, residual_mirror },
        .cpu_peak_bytes = peak,
    };
}

test "reference selected mixed QKV roles preserve original Host and device tail lifetimes" {
    const all = try referenceSelectedLayer(32, 128, 128, 128, 256, false, .{ true, true, true }, true, 65536);
    try std.testing.expectEqualDeep(try referenceQuantizedLayer(32, 128, 128, 128, 256, true, 65536), all);
    const later = try referenceSelectedLayer(32, 128, 128, 128, 256, true, .{ true, true, true }, true, 16383);
    try std.testing.expectEqualDeep(try referenceQuantizedLayer(32, 128, 128, 128, 256, false, 16383), later);
    const mixed_host = try referenceSelectedLayer(32, 128, 128, 128, 256, false, .{ false, true, false }, true, 65536);
    try std.testing.expectEqualSlices(usize, &.{ 0, 16384, 0, 0, 0 }, &mixed_host.uploads);
    try std.testing.expectEqualSlices(usize, &.{ 0, 16384, 0, 16384, 16384 }, &mixed_host.downloads);
    try std.testing.expect(mixed_host.cpu_peak_bytes < all.cpu_peak_bytes);
    const mixed_device = try referenceSelectedLayer(32, 128, 128, 128, 256, true, .{ false, true, true }, false, 65536);
    try std.testing.expectEqualSlices(usize, &.{ 0, 16384, 16384, 16384, 16384 }, &mixed_device.uploads);
    try std.testing.expectEqualSlices(usize, &.{ 0, 16384, 16384, 0, 0 }, &mixed_device.downloads);
    try std.testing.expectEqual(@as(usize, 131072), mixed_device.cpu_peak_bytes);
    try std.testing.expect(mixed_device.cpu_peak_bytes < later.cpu_peak_bytes);
    try std.testing.expectError(error.Overflow, referenceSelectedLayer(std.math.maxInt(usize), 128, 128, 128, 256, false, .{ true, false, true }, true, 0));
}
