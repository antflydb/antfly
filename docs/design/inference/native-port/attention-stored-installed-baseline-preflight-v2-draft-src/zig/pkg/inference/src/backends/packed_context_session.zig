// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Direct packed ingestion and a request-owned replay/publication boundary.
//! Exact global owners reserve the admitted continuation; sliding owners use a
//! bounded ring. A maintenance lease freezes all live owners until replay and
//! synchronous fits have drained. This internal route does not authorize
//! approximate serving or select an artifact's retention profile.
const std = @import("std");
const policy = @import("../runtime/context_policy.zig");
const contract = @import("../runtime/kv/context_session.zig");
const views = @import("../runtime/kv/context_view.zig");
const payload = @import("../runtime/kv/payload.zig");
const storage = @import("packed_context_storage.zig");
const replay = @import("model_replay_context.zig");
const attention = @import("context_kernels.zig");
const fitter = @import("context_compaction.zig");
const shared = @import("metal_generated_kernels.zig");
const mr = @import("metal_runtime.zig");
const execution = @import("../execution_control.zig");
const execution_pool = @import("context_execution_pool.zig");
const query_protocol = @import("context_query_protocol.zig");
const query_campaign = @import("context_query_campaign_pool.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");
const fault_injection = @import("../context_fault_injection.zig");
const teacher_forced = @import("context_teacher_forced.zig");
const Ref = shared.Ref;
const Buffer = shared.Buffer;
var next_identity = std.atomic.Value(u64).init(1);

pub const OwnerAllocationSnapshot = struct {
    session_scope_exclusive: bool = false,
    /// Opaque per-session Metal runtime/pipeline allocations are not measured.
    full_owner_physical_available: bool = false,
    allocation: owner_alloc.Snapshot = .{},
};
pub const OwnerReleaseReceipt = struct {
    released: bool = false,
    physical_available: bool = false,
    post_release_live_bytes: u64 = 0,
};
pub const CommittedContextStorageDiagnostic = struct {
    diagnostic_only: bool = true,
    /// Compatibility alias for total_owner_count.
    owner_count: u32,
    global_owner_count: u32,
    total_owner_count: u32,
    physical_bytes: u64,
    replay_control_bytes: u64,
    tracker_snapshot: owner_alloc.Snapshot,
};
fn fingerprintU32(hash: *std.crypto.hash.sha2.Sha256, value: u32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hash.update(&bytes);
}

fn fingerprintU64(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}

/// Bind every consumer to its canonical KV owner and query geometry. Owners
/// carry payload bytes; consumers still determine which model layer reads
/// those bytes and with what GQA/RoPE/window contract.
fn fingerprintLayout(hash: *std.crypto.hash.sha2.Sha256, layout: policy.Layout) !void {
    if (layout.len == 0 or layout.len > layout.layers.len) return error.InvalidContextLayout;
    fingerprintU32(hash, @intCast(layout.len));
    for (layout.layers[0..layout.len], 0..) |layer, index| {
        if (layer.owner > index or layer.kv_heads == 0 or layer.query_heads == 0 or
            layer.query_heads % layer.kv_heads != 0 or layer.head_dim == 0 or
            layer.rope_active_dim > layer.rope_frequency_dim or
            !std.math.isFinite(layer.rope_theta) or layer.rope_theta <= 0)
            return error.InvalidContextLayout;
        fingerprintU32(hash, @intCast(index));
        fingerprintU32(hash, layer.owner);
        fingerprintU32(hash, layer.sliding_window);
        fingerprintU32(hash, layer.kv_heads);
        fingerprintU32(hash, layer.query_heads);
        fingerprintU32(hash, layer.head_dim);
        fingerprintU32(hash, layer.rope_active_dim);
        fingerprintU32(hash, layer.rope_frequency_dim);
        fingerprintU32(hash, @bitCast(layer.rope_theta));
    }
}

fn fingerprintView(hash: *std.crypto.hash.sha2.Sha256, layer: u32, scale: f32, softcap: f32, view: views.View) !void {
    _ = try view.validate();
    fingerprintU32(hash, layer);
    fingerprintU32(hash, view.version);
    fingerprintU64(hash, view.epoch);
    fingerprintU32(hash, view.logical_tokens);
    fingerprintU32(hash, view.capacity_rows);
    fingerprintU32(hash, view.bias_rows);
    fingerprintU32(hash, view.kv_heads);
    fingerprintU32(hash, view.head_dim);
    fingerprintU32(hash, @intCast(view.heads.len));
    fingerprintU32(hash, @intCast(view.spans.len));
    fingerprintU32(hash, @bitCast(scale));
    fingerprintU32(hash, @bitCast(softcap));
    for (view.heads) |head| {
        fingerprintU32(hash, head.kv_start);
        fingerprintU32(hash, head.capacity_rows);
        fingerprintU32(hash, head.bias_start);
        fingerprintU32(hash, head.bias_rows);
        fingerprintU32(hash, head.span_start);
        fingerprintU32(hash, head.span_count);
    }
    for (view.spans) |span| {
        fingerprintU32(hash, span.physical_start);
        fingerprintU32(hash, span.row_count);
        fingerprintU32(hash, span.logical_start);
        fingerprintU32(hash, span.logical_count);
        fingerprintU32(hash, @backingInt(span.kind));
        fingerprintU32(hash, span.bias_start);
    }
}

const CanonicalOwnerFingerprint = struct {
    layer: u32,
    scale: f32,
    softcap: f32,
    view: views.View,
    keys: []const u8,
    values: []const u8,
    mass: ?[]const u8 = null,
};

/// Hash only the descriptor fields consumed by attention and the occupied
/// payload rows they name. Buffer handles, allocation addresses, session IDs,
/// and reserved continuation capacity never enter either digest.
fn fingerprintOwner(
    descriptor: *std.crypto.hash.sha2.Sha256,
    payload_hash: *std.crypto.hash.sha2.Sha256,
    input: CanonicalOwnerFingerprint,
) !u64 {
    try fingerprintView(descriptor, input.layer, input.scale, input.softcap, input.view);
    var readback: u64 = 0;
    if (input.view.version == views.abi_version) {
        for (input.view.heads, 0..) |head, h| for (input.view.headSpans(h)) |span| {
            const row_bytes = try std.math.mul(usize, @as(usize, input.view.head_dim), @sizeOf(f16));
            const offset = try std.math.mul(usize, @as(usize, head.kv_start + span.physical_start), row_bytes);
            const length = try std.math.mul(usize, @as(usize, span.row_count), row_bytes);
            if (offset > input.keys.len or length > input.keys.len - offset or
                offset > input.values.len or length > input.values.len - offset)
                return error.InvalidContextView;
            payload_hash.update(input.keys[offset..][0..length]);
            payload_hash.update(input.values[offset..][0..length]);
            readback = try std.math.add(u64, readback, try std.math.mul(u64, 2, @as(u64, length)));
            if (span.kind == .compacted) {
                const mass = input.mass orelse return error.InvalidContextView;
                const mass_offset = try std.math.mul(usize, @as(usize, head.bias_start + span.bias_start), @sizeOf(f32));
                const mass_length = try std.math.mul(usize, @as(usize, span.row_count), @sizeOf(f32));
                if (mass_offset > mass.len or mass_length > mass.len - mass_offset)
                    return error.InvalidContextView;
                payload_hash.update(mass[mass_offset..][0..mass_length]);
                readback = try std.math.add(u64, readback, @as(u64, mass_length));
            }
        };
        return readback;
    }
    if (input.view.version != views.interleaved_abi_version or input.mass != null)
        return error.InvalidContextView;
    const width = try std.math.mul(usize, @as(usize, input.view.kv_heads), @as(usize, input.view.head_dim));
    const row_bytes = try std.math.mul(usize, width, @sizeOf(f16));
    for (input.view.spans) |span| {
        if (span.kind != .exact) return error.InvalidContextView;
        const offset = try std.math.mul(usize, @as(usize, span.physical_start), row_bytes);
        const length = try std.math.mul(usize, @as(usize, span.row_count), row_bytes);
        if (offset > input.keys.len or length > input.keys.len - offset or
            offset > input.values.len or length > input.values.len - offset)
            return error.InvalidContextView;
        payload_hash.update(input.keys[offset..][0..length]);
        payload_hash.update(input.values[offset..][0..length]);
        readback = try std.math.add(u64, readback, try std.math.mul(u64, 2, @as(u64, length)));
    }
    return readback;
}

pub const FailureFingerprint = struct {
    epoch: u64,
    logical_tokens: u32,
    /// Digest of the exact prompt token sequence retained by the request
    /// owner. Callers derive it from the already-rendered encoded prompt.
    content_sha256: [32]u8,
    /// Canonical host descriptor digest. This never reads KV bytes back from
    /// the backend and is separate from maintenance readback counters.
    descriptors_sha256: [32]u8,
    /// Test-only digest of every logically visible K/V row. This is diagnostic
    /// readback and is never added to production maintenance readback metrics.
    kv_sha256: [32]u8,
    diagnostic_kv_readback_bytes: u64,
};

pub const Config = struct {
    /// The original must fit exactly before any approximate decision. This
    /// deliberately does not reuse the legacy 50-percent retention estimate.
    plan: policy.Plan,
    layout: policy.Layout,
    max_query_rows: u32,
    physical_budget_bytes: usize,
    /// Hard logical limit for request-owned host metadata. Null uses the
    /// caller's combined physical reservation as a conservative ceiling.
    owner_host_limit_bytes: ?usize = null,
    /// Optional stable request-owner tracker whose lifetime begins before
    /// payload preparation and ends after every request object is released.
    /// The session borrows this pointer and returns its allocations to the
    /// entry snapshot; the outer owner remains responsible for destruction.
    owner_allocation_tracker: ?*owner_alloc.HostAllocationTracker = null,
    /// The request retains its model handle and this exclusive lease until
    /// every live/private session and tracked buffer has been destroyed.
    execution_lease: ?*execution_pool.Lease = null,
    /// Qualification-only opt-in to the experimental split-row kernel for
    /// single-token decode. Prefill/replay retain the correctness kernel and
    /// scratch is fixed at admission.
    optimize_decode: bool = false,
    /// Qualification-only opt-in to the eagerly-created tiled ABI2 prefill
    /// kernel. Single-token decode retains its separately selected route.
    optimize_prefill: bool = false,
    cancelled: ?*const std.atomic.Value(bool) = null,
    execution_control: ?execution.InferenceExecutionControl = null,
};
pub const Allocation = struct {
    physical_bytes: usize = 0,
    global_bytes: usize = 0,
    sliding_bytes: usize = 0,
    decode_scratch_bytes: usize = 0,
    descriptor_span_bytes: usize = 0,
    descriptor_head_bytes: usize = 0,
    descriptor_arena_bytes: usize = 0,
    backend_control_bytes: usize = 0,
    global_owners: usize = 0,
    sliding_owners: usize = 0,
};
fn rounded(n: usize) !usize {
    return (try std.math.add(usize, n, std.heap.pageSize() - 1)) & ~(std.heap.pageSize() - 1);
}
fn ringCapacity(config: Config, layer: usize) !u32 {
    return std.math.cast(u32, @min(config.plan.logical_token_limit, @as(u64, config.layout.layers[layer].sliding_window) + config.max_query_rows - 1)) orelse error.InvalidContextLayout;
}
fn splitDecodePlan(config: Config, layer: usize) !attention.SplitDecodePlan {
    const shape = config.layout.layers[layer];
    const max_rows: u32 = if (shape.sliding_window == 0)
        @intCast(config.plan.logical_token_limit)
    else
        try ringCapacity(config, layer);
    return attention.splitDecodePlan(max_rows, shape.query_heads, shape.head_dim);
}
fn descriptorSpansPerHead(config: Config) u32 {
    const blocks = 1 + (config.plan.logical_token_limit - 1) / 512;
    return @intCast(@min(@as(u64, views.max_spans), blocks + 1));
}
pub fn allocation(config: Config) !Allocation {
    const p = config.plan;
    try p.policy.validate();
    if (p.policy.mode != .exact or p.prompt_tokens == 0 or p.prompt_tokens >= p.logical_token_limit or
        p.logical_token_limit > p.context_limit or p.logical_token_limit > std.math.maxInt(u32) or p.retained_row_limit != p.logical_token_limit or
        config.max_query_rows == 0 or config.max_query_rows > 8192 or config.layout.len == 0 or config.layout.len > 256) return error.InvalidPackedContextPlan;
    const admitted = try p.policy.admit(p.context_limit, null, p.prompt_tokens, p.logical_token_limit - p.prompt_tokens, 0);
    if (admitted.context_limit != p.context_limit or admitted.retained_row_limit != p.retained_row_limit) return error.InvalidPackedContextPlan;
    var result = Allocation{};
    for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
        if (l.owner > i or l.kv_heads == 0 or l.kv_heads > views.max_heads or l.query_heads == 0 or l.query_heads % l.kv_heads != 0 or
            l.head_dim == 0 or l.head_dim > 512 or l.head_dim % 2 != 0 or
            (config.optimize_prefill and l.head_dim != 256 and l.head_dim != 512) or
            @as(u64, config.max_query_rows) * l.query_heads * l.head_dim > std.math.maxInt(u32) or
            @as(u64, config.max_query_rows) * l.query_heads * 32 > std.math.maxInt(u32)) return error.InvalidContextLayout;
        const owner = config.layout.layers[l.owner];
        if (owner.owner != l.owner or owner.head_dim != l.head_dim or owner.kv_heads != l.kv_heads or
            owner.query_heads != l.query_heads or owner.sliding_window != l.sliding_window) return error.InvalidContextLayout;
        if (l.owner != i) continue;
        const span_records = try std.math.mul(usize, l.kv_heads, descriptorSpansPerHead(config));
        result.descriptor_span_bytes = try std.math.add(usize, result.descriptor_span_bytes, try std.math.mul(usize, span_records, @sizeOf(views.Span)));
        result.descriptor_head_bytes = try std.math.add(usize, result.descriptor_head_bytes, try std.math.mul(usize, l.kv_heads, @sizeOf(views.Head)));
        if (config.optimize_decode) {
            const split = try splitDecodePlan(config, i);
            result.decode_scratch_bytes = @max(result.decode_scratch_bytes, split.scratch_bytes);
        }
        if (l.sliding_window == 0) {
            const size = try storage.Generation.emptyAllocation(l.head_dim, l.kv_heads, @intCast(p.logical_token_limit));
            result.global_bytes = try std.math.add(usize, result.global_bytes, size.physical_bytes);
            result.global_owners += 1;
        } else {
            const elements = @as(u64, try ringCapacity(config, i)) * l.head_dim * l.kv_heads;
            if (elements > std.math.maxInt(u32)) return error.InvalidContextLayout;
            result.sliding_bytes = try std.math.add(usize, result.sliding_bytes, 2 * try rounded(@intCast(elements * 2)));
            result.sliding_owners += 1;
        }
    }
    result.descriptor_span_bytes = try rounded(result.descriptor_span_bytes);
    result.descriptor_head_bytes = try rounded(result.descriptor_head_bytes);
    result.descriptor_arena_bytes = try std.math.add(usize, result.descriptor_span_bytes, result.descriptor_head_bytes);
    // Copier dummy, attention dummy/status, fixed descriptor arenas and the
    // admitted split scratch are all persistent backend control allocations.
    result.backend_control_bytes = try std.math.add(usize, 3 * std.heap.pageSize(), result.descriptor_arena_bytes);
    result.backend_control_bytes = try std.math.add(usize, result.backend_control_bytes, result.decode_scratch_bytes);
    // Stable owner arrays and origins are constructed during a maintenance fork.
    const records = try std.math.add(usize, try rounded(result.global_owners * @sizeOf(storage.Generation)), try rounded(result.sliding_owners * @sizeOf(Ring)));
    result.physical_bytes = try std.math.add(usize, result.global_bytes, result.sliding_bytes);
    result.physical_bytes = try std.math.add(usize, result.physical_bytes, result.backend_control_bytes);
    result.physical_bytes = try std.math.add(usize, result.physical_bytes, records + try rounded((result.global_owners + result.sliding_owners) * @sizeOf(replay.Origin)));
    return result;
}
const Ring = struct {
    keys: Buffer,
    values: Buffer,
    capacity: u32,
    spans: [2]views.Span = undefined,
    len: u32 = 0,
    fn deinit(self: *Ring) void {
        self.keys.deinit();
        self.values.deinit();
    }
    fn describe(self: *Ring, end: u32, retained: u32) void {
        const start = end -| retained;
        const physical = start % self.capacity;
        const first = @min(end - start, self.capacity - physical);
        self.spans[0] = .{ .physical_start = physical, .row_count = first, .logical_start = start, .logical_count = first };
        self.len = 1;
        if (start + first < end) {
            self.spans[1] = .{ .physical_start = 0, .row_count = end - start - first, .logical_start = start + first, .logical_count = end - start - first };
            self.len = 2;
        }
    }
};

pub const ExactBlockCapture = struct {
    layer: u32,
    logical_start: u32,
    rows: u32,
    heads: u32,
    dim: u32,
    physical_starts: [views.max_heads]u32,
    keys: []const u8,
    values: []const u8,

    pub const Kind = enum { keys, values };

    /// Borrowed host-visible Metal backing for one exact logical row. The
    /// Session and execution lease must outlive every returned slice.
    pub fn row(self: ExactBlockCapture, kind: Kind, logical_row: u32, head: u32) ![]const u8 {
        if (logical_row >= self.rows or head >= self.heads) return error.InvalidCampaignCapture;
        const row_bytes = try std.math.mul(usize, self.dim, @sizeOf(f16));
        const physical_row = try std.math.add(u32, self.physical_starts[head], logical_row);
        const offset = try std.math.mul(usize, physical_row, row_bytes);
        const bytes = switch (kind) {
            .keys => self.keys,
            .values => self.values,
        };
        if (offset > bytes.len or row_bytes > bytes.len - offset) return error.InvalidCampaignCapture;
        return bytes[offset..][0..row_bytes];
    }
};

test "context packed exact block capture honors per-head physical starts" {
    var keys: [40]u8 = undefined;
    var values: [40]u8 = undefined;
    for (&keys, &values, 0..) |*key, *value, index| {
        key.* = @intCast(index);
        value.* = @intCast(100 + index);
    }
    var starts: [views.max_heads]u32 = @splat(0);
    starts[0] = 2;
    starts[1] = 6;
    const block = ExactBlockCapture{
        .layer = 4,
        .logical_start = 512,
        .rows = 2,
        .heads = 2,
        .dim = 2,
        .physical_starts = starts,
        .keys = &keys,
        .values = &values,
    };
    try std.testing.expectEqualSlices(u8, keys[8..12], try block.row(.keys, 0, 0));
    try std.testing.expectEqualSlices(u8, keys[12..16], try block.row(.keys, 1, 0));
    try std.testing.expectEqualSlices(u8, values[24..28], try block.row(.values, 0, 1));
    try std.testing.expectEqualSlices(u8, values[28..32], try block.row(.values, 1, 1));
    try std.testing.expectError(error.InvalidCampaignCapture, block.row(.keys, 2, 0));
    try std.testing.expectError(error.InvalidCampaignCapture, block.row(.keys, 0, 2));
}

pub const Session = struct {
    allocator: std.mem.Allocator,
    allocation_tracker: *owner_alloc.HostAllocationTracker,
    owns_allocation_tracker: bool,
    allocation_start: owner_alloc.Snapshot,
    config: Config,
    sizes: Allocation,
    globals: []storage.Generation,
    sliding: []Ring,
    owner_indices: [256]u32 = @splat(0),
    global_layers: [256]u32 = @splat(0),
    scales: [256]?f32 = @splat(null),
    softcaps: [256]?f32 = @splat(null),
    copier: storage.Copier,
    attention: attention.Kernels,
    decode_scratch: ?Buffer,
    descriptor_spans: Buffer,
    descriptor_heads: Buffer,
    descriptor_span_offsets: [256]usize = @splat(0),
    descriptor_head_offsets: [256]usize = @splat(0),
    publication: views.Publication = .{},
    identity: u64,
    logical_tokens: u32 = 0,
    next_layer: u32 = 0,
    step_end: u32 = 0,
    failed: bool = false,
    frozen: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    metrics: contract.Metrics = .{},
    split_decode_dispatches: u64 = 0,
    flash_prefill_dispatches: u64 = 0,
    framed_attention_dispatches: u64 = 0,
    pending_frame_runtime: ?*mr.RawMetalDecodeRuntime = null,
    pending_epoch: u64 = 0,
    pending_start: u32 = 0,
    pending_rows: u32 = 0,
    pending_global_appended: [256]bool = @splat(false),
    pending_global_ready: [256]bool = @splat(false),
    pending_global_ingesting_empty: [256]bool = @splat(false),
    pending_ring_appended: [256]bool = @splat(false),
    pending_ring_spans: [256][2]views.Span = undefined,
    pending_ring_lens: [256]u32 = @splat(0),
    pending_split_dispatches: u64 = 0,
    pending_flash_dispatches: u64 = 0,

    pub fn init(_: std.mem.Allocator, config: Config) !Session {
        const sizes = try allocation(config);
        if (sizes.physical_bytes > config.physical_budget_bytes) return error.ContextStagingBudgetExceeded;
        if (config.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (config.execution_control) |control| try control.check();
        var protection = if (config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        const owns_allocation_tracker = config.owner_allocation_tracker == null;
        const allocation_tracker = config.owner_allocation_tracker orelse try std.heap.c_allocator.create(owner_alloc.HostAllocationTracker);
        errdefer if (owns_allocation_tracker) std.heap.c_allocator.destroy(allocation_tracker);
        if (owns_allocation_tracker) {
            const tracker_control = owner_alloc.HostAllocationTracker.externalAllocationPhysicalSize(allocation_tracker) orelse 0;
            allocation_tracker.* = owner_alloc.HostAllocationTracker.init(config.owner_host_limit_bytes orelse config.physical_budget_bytes, tracker_control);
        }
        const tracked_allocator = allocation_tracker.allocator();
        const allocation_start = allocation_tracker.snapshot();
        var copier = if (config.execution_lease) |lease|
            try storage.Copier.initBorrowedTracked(try lease.execution(.storage), allocation_tracker)
        else
            try storage.Copier.initTracked(allocation_tracker);
        errdefer copier.deinit();
        copier.core.cancelled = config.cancelled;
        var kernels = if (config.execution_lease) |lease|
            try attention.Kernels.initBorrowedTracked(try lease.execution(.attention), if (config.optimize_prefill) try lease.flashExecution() else null, allocation_tracker)
        else
            try attention.Kernels.initTracked(config.optimize_prefill, allocation_tracker);
        errdefer kernels.deinit();
        kernels.core.cancelled = config.cancelled;
        var decode_scratch: ?Buffer = if (sizes.decode_scratch_bytes > 0) try kernels.core.buffer(sizes.decode_scratch_bytes) else null;
        errdefer if (decode_scratch) |*scratch| scratch.deinit();
        var descriptor_spans = try kernels.core.buffer(sizes.descriptor_span_bytes);
        errdefer descriptor_spans.deinit();
        var descriptor_heads = try kernels.core.buffer(sizes.descriptor_head_bytes);
        errdefer descriptor_heads.deinit();
        const globals = try tracked_allocator.alloc(storage.Generation, sizes.global_owners);
        errdefer tracked_allocator.free(globals);
        const rings = try tracked_allocator.alloc(Ring, sizes.sliding_owners);
        errdefer tracked_allocator.free(rings);
        var global_count: usize = 0;
        var ring_count: usize = 0;
        errdefer for (globals[0..global_count]) |*g| g.deinit();
        errdefer for (rings[0..ring_count]) |*r| r.deinit();
        var self = Session{ .allocator = tracked_allocator, .allocation_tracker = allocation_tracker, .owns_allocation_tracker = owns_allocation_tracker, .allocation_start = allocation_start, .config = config, .sizes = sizes, .globals = globals, .sliding = rings, .copier = copier, .attention = kernels, .decode_scratch = decode_scratch, .descriptor_spans = descriptor_spans, .descriptor_heads = descriptor_heads, .identity = next_identity.fetchAdd(1, .monotonic) };
        var span_offset: usize = 0;
        var head_offset: usize = 0;
        for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
            if (l.owner != i) {
                self.owner_indices[i] = self.owner_indices[l.owner];
                self.descriptor_span_offsets[i] = self.descriptor_span_offsets[l.owner];
                self.descriptor_head_offsets[i] = self.descriptor_head_offsets[l.owner];
            } else if (l.sliding_window == 0) {
                self.descriptor_span_offsets[i] = span_offset;
                self.descriptor_head_offsets[i] = head_offset;
                span_offset += @as(usize, l.kv_heads) * descriptorSpansPerHead(config) * @sizeOf(views.Span);
                head_offset += @as(usize, l.kv_heads) * @sizeOf(views.Head);
                globals[global_count] = try storage.Generation.initEmpty(tracked_allocator, &self.copier, l.head_dim, l.kv_heads, @intCast(config.plan.logical_token_limit), 1, config.physical_budget_bytes);
                self.global_layers[global_count] = @intCast(i);
                self.owner_indices[i] = @intCast(global_count);
                global_count += 1;
            } else {
                self.descriptor_span_offsets[i] = span_offset;
                self.descriptor_head_offsets[i] = head_offset;
                span_offset += @as(usize, l.kv_heads) * descriptorSpansPerHead(config) * @sizeOf(views.Span);
                head_offset += @as(usize, l.kv_heads) * @sizeOf(views.Head);
                const capacity = try ringCapacity(config, i);
                var keys = try self.copier.core.buffer(@as(usize, capacity) * l.kv_heads * l.head_dim * 2);
                errdefer keys.deinit();
                rings[ring_count] = .{ .keys = keys, .values = try self.copier.core.buffer(keys.logical_bytes), .capacity = capacity };
                self.owner_indices[i] = @intCast(ring_count);
                ring_count += 1;
            }
        }
        self.metrics.live_bytes = sizes.physical_bytes;
        self.metrics.peak_bytes = sizes.physical_bytes;
        return self;
    }
    /// A maintenance object must be closed first, just like a synchronous
    /// model callback must return before its request owner can be destroyed.
    pub fn deinit(self: *Session) void {
        _ = self.deinitMeasured();
    }
    pub fn drainedOwnerSnapshot(self: *Session) !@import("../runtime/kv/host_allocation_tracker.zig").Snapshot {
        const owner = self.config.owner_allocation_tracker orelse return error.ContextPhysicalAccountingUnavailable;
        // Production total-owner snapshots require the model-pinned lease to
        // drain every queue, including the external decoder frame owner.
        const lease = self.config.execution_lease orelse return error.ContextPhysicalAccountingUnavailable;
        try lease.drainForBufferRelease();
        if (self.pending_frame_runtime != null or self.frozen or self.failed or self.next_layer != 0)
            return error.InvalidContextTransaction;
        return owner.snapshot();
    }

    pub fn qualificationFingerprint(self: *Session) !teacher_forced.QualityFingerprint {
        return self.qualificationFingerprintImpl(false);
    }
    fn qualificationFingerprintImpl(self: *Session, allow_frozen: bool) !teacher_forced.QualityFingerprint {
        if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
        if (@import("builtin").cpu.arch.endian() != .little) return error.UnsupportedEndian;
        const lease = self.config.execution_lease orelse return error.ContextPhysicalAccountingUnavailable;
        try lease.drainForBufferRelease();
        if (self.failed or (!allow_frozen and self.frozen) or self.next_layer != 0 or self.pending_frame_runtime != null or
            self.publication.pending or self.publication.readers != 0 or self.logical_tokens == 0)
            return error.InvalidContextTransaction;

        var descriptor = std.crypto.hash.sha2.Sha256.init(.{});
        descriptor.update("antfly.packed-context.descriptor.v2\x00");
        fingerprintU64(&descriptor, self.publication.epoch);
        fingerprintU32(&descriptor, self.logical_tokens);
        fingerprintU32(&descriptor, @intCast(self.globals.len));
        fingerprintU32(&descriptor, @intCast(self.sliding.len));
        try fingerprintLayout(&descriptor, self.config.layout);

        var payload_hash = std.crypto.hash.sha2.Sha256.init(.{});
        payload_hash.update("antfly.packed-context.payload.v1\x00");
        var readback: u64 = 0;
        for (self.config.layout.layers[0..self.config.layout.len], 0..) |layout, layer| {
            if (layout.owner != layer) continue;
            const scale = self.scales[layer] orelse return error.InvalidContextTransaction;
            const softcap = self.softcaps[layer] orelse return error.InvalidContextTransaction;
            if (layout.sliding_window == 0) {
                const generation = &self.globals[self.owner_indices[layer]];
                const source = try generation.source();
                try source.validate();
                readback = try std.math.add(u64, readback, try fingerprintOwner(&descriptor, &payload_hash, .{
                    .layer = @intCast(layer),
                    .scale = scale,
                    .softcap = softcap,
                    .view = source.view,
                    .keys = generation.keys.bytes[0..generation.keys.logical_bytes],
                    .values = generation.values.bytes[0..generation.values.logical_bytes],
                    .mass = if (generation.mass) |mass| mass.bytes[0..mass.logical_bytes] else null,
                }));
            } else {
                const ring = &self.sliding[self.owner_indices[layer]];
                const source = self.ringSource(layer, self.logical_tokens);
                try source.validate();
                readback = try std.math.add(u64, readback, try fingerprintOwner(&descriptor, &payload_hash, .{
                    .layer = @intCast(layer),
                    .scale = scale,
                    .softcap = softcap,
                    .view = source.view,
                    .keys = ring.keys.bytes[0..ring.keys.logical_bytes],
                    .values = ring.values.bytes[0..ring.values.logical_bytes],
                }));
            }
        }
        var descriptor_digest: [32]u8 = undefined;
        descriptor.final(&descriptor_digest);
        payload_hash.update(&descriptor_digest);
        var payload_digest: [32]u8 = undefined;
        payload_hash.final(&payload_digest);
        return .{
            .context_instance_id = self.identity,
            .publication_epoch = self.publication.epoch,
            .logical_tokens = self.logical_tokens,
            .payload_sha256 = payload_digest,
            .descriptor_sha256 = descriptor_digest,
            .diagnostic_kv_readback_bytes = readback,
        };
    }

    /// Stable logical fingerprint for fault isolation. The execution lease is
    /// drained first. Descriptor and logically visible host-mapped KV bytes
    /// are hashed as diagnostic evidence; these bytes remain separate from
    /// production maintenance-readback accounting. A caller must pass the
    /// digest of the exact encoded prompt owned by this session.
    pub fn failureFingerprint(self: *Session, content_sha256: [32]u8) !FailureFingerprint {
        if (std.mem.allEqual(u8, &content_sha256, 0)) return error.InvalidContextFingerprint;
        const fingerprint = try self.qualificationFingerprint();
        return .{
            .epoch = fingerprint.publication_epoch,
            .logical_tokens = fingerprint.logical_tokens,
            .content_sha256 = content_sha256,
            .descriptors_sha256 = fingerprint.descriptor_sha256,
            .kv_sha256 = fingerprint.payload_sha256,
            .diagnostic_kv_readback_bytes = fingerprint.diagnostic_kv_readback_bytes,
        };
    }

    /// Test-only view of immutable exact packed-prefix KV. The caller must
    /// first drain the execution lease, then stream/copy the borrowed rows
    /// before the Session is mutated or destroyed. Compacted spans and
    /// cross-span blocks are rejected rather than reconstructed.
    pub fn captureExactBlock(self: *const Session, layer: u32, start: u32, rows: u32) !ExactBlockCapture {
        if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
        if (rows == 0 or self.failed or self.publication.pending or self.publication.readers != 0 or
            self.pending_frame_runtime != null or self.frozen or self.next_layer != 0 or
            start > self.logical_tokens or rows > self.logical_tokens - start)
            return error.InvalidCampaignCapture;
        var owner_index: ?usize = null;
        for (self.globals, 0..) |_, index| if (self.global_layers[index] == layer) {
            if (owner_index != null) return error.InvalidContextLayout;
            owner_index = index;
        };
        const generation = &self.globals[owner_index orelse return error.InvalidContextLayout];
        const source = try generation.source();
        const end = try std.math.add(u32, start, rows);
        var result = ExactBlockCapture{
            .layer = layer,
            .logical_start = start,
            .rows = rows,
            .heads = source.view.kv_heads,
            .dim = source.view.head_dim,
            .physical_starts = @splat(0),
            .keys = generation.keys.bytes[0..generation.keys.logical_bytes],
            .values = generation.values.bytes[0..generation.values.logical_bytes],
        };
        for (0..source.view.kv_heads) |head| {
            var found = false;
            for (source.view.headSpans(head)) |span| {
                const span_end = try std.math.add(u32, span.logical_start, span.logical_count);
                if (start < span.logical_start or end > span_end) continue;
                if (span.kind != .exact or span.row_count != span.logical_count) return error.InvalidCampaignCapture;
                result.physical_starts[head] = try std.math.add(u32, source.view.heads[head].kv_start, try std.math.add(u32, span.physical_start, start - span.logical_start));
                found = true;
                break;
            }
            if (!found) return error.InvalidCampaignCapture;
            _ = try result.row(.keys, rows - 1, @intCast(head));
            _ = try result.row(.values, rows - 1, @intCast(head));
        }
        return result;
    }

    /// Drains the model-pinned execution owner before test-only mapped query
    /// and exact-KV capture. Keeping this on Session avoids exposing its lease
    /// while making the synchronization requirement observable and mandatory.
    pub fn drainCampaignCaptureReadback(self: *const Session) !void {
        if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
        const lease = self.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        try lease.drainForBufferRelease();
        if (self.failed) return error.CampaignCaptureLiveSessionFailed;
        if (self.publication.pending or self.publication.readers != 0)
            return error.CampaignCapturePublicationBusy;
        if (self.pending_frame_runtime != null) return error.CampaignCaptureFramePending;
        if (self.frozen) return error.CampaignCaptureMaintenanceActive;
        if (self.next_layer != 0) return error.CampaignCaptureIncompleteStep;
    }
    pub fn deinitMeasured(self: *Session) OwnerReleaseReceipt {
        if (self.frozen) @panic("packed context destroyed with active maintenance owner");
        // A registered C completion owns this session and its descriptor/status
        // buffers until the owner frame waits or cancels. Preserve the process
        // rather than leaving a callback to freed memory in ReleaseFast.
        if (self.config.execution_lease) |lease| lease.drainForBufferRelease() catch {};
        if (self.pending_frame_runtime != null) @panic("packed context destroyed with a pending Metal frame");
        for (self.globals) |*g| g.deinit();
        for (self.sliding) |*r| r.deinit();
        self.allocator.free(self.globals);
        self.allocator.free(self.sliding);
        if (self.decode_scratch) |*scratch| scratch.deinit();
        self.descriptor_heads.deinit();
        self.descriptor_spans.deinit();
        self.attention.deinit();
        self.copier.deinit();
        const allocation_tracker = self.allocation_tracker;
        const owns_allocation_tracker = self.owns_allocation_tracker;
        const allocation_start = self.allocation_start;
        const after = allocation_tracker.snapshot();
        if (after.logical_live_bytes != allocation_start.logical_live_bytes or
            after.shared_backing_live_bytes != allocation_start.shared_backing_live_bytes or
            (after.physical_available and after.physical_live_bytes != allocation_start.physical_live_bytes))
            @panic("packed context released with live owner allocations");
        const post_release_live_bytes = if (after.tracked_combined_physical_available)
            after.tracked_combined_live_bytes
        else
            0;
        if (owns_allocation_tracker) std.heap.c_allocator.destroy(allocation_tracker);
        self.* = undefined;
        return .{
            .released = true,
            .physical_available = after.tracked_combined_physical_available,
            .post_release_live_bytes = post_release_live_bytes,
        };
    }
    pub fn ownerAllocationStart(self: *Session) OwnerAllocationSnapshot {
        return .{ .session_scope_exclusive = true, .allocation = self.allocation_start };
    }
    pub fn ownerAllocationSnapshot(self: *Session) OwnerAllocationSnapshot {
        return .{ .session_scope_exclusive = true, .allocation = self.allocation_tracker.snapshot() };
    }
    /// Tracker-derived complete request-context owner delta. The caller takes
    /// the baseline before first replay-control materialization and the final
    /// sample after branch/query/loader teardown. `replay_control_bytes` is an
    /// independently measured ownedBytes-plan delta and must be zero in both
    /// accepted samples; projected plan bytes never replace this measurement.
    pub fn committedContextStorageDiagnostic(self: *Session) !CommittedContextStorageDiagnostic {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        const lease = self.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        try lease.drainForBufferRelease();
        if (self.frozen or self.failed or self.pending_frame_runtime != null or self.next_layer != 0 or
            self.publication.pending or self.publication.readers != 0)
            return error.InvalidContextTransaction;
        const snapshot = self.allocation_tracker.snapshot();
        if (!snapshot.tracked_combined_physical_available or !self.allocation_start.tracked_combined_physical_available or
            snapshot.owner_identity != self.allocation_start.owner_identity or
            snapshot.tracked_combined_live_bytes < self.allocation_start.tracked_combined_live_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        var controls: u64 = 0;
        for (self.globals) |generation| {
            const owned = try generation.ownedBytes();
            if (owned < generation.plan.sizes.physical_bytes) return error.ContextPhysicalAccountingUnavailable;
            controls = try std.math.add(u64, controls, @intCast(owned - generation.plan.sizes.physical_bytes));
        }
        return .{
            .owner_count = @intCast(self.globals.len + self.sliding.len),
            .global_owner_count = @intCast(self.globals.len),
            .total_owner_count = @intCast(self.globals.len + self.sliding.len),
            .physical_bytes = snapshot.tracked_combined_live_bytes - self.allocation_start.tracked_combined_live_bytes,
            .replay_control_bytes = controls,
            .tracker_snapshot = snapshot,
        };
    }
    pub fn hook(self: *Session) contract.Hook {
        return .{ .ctx = self, .identity = self.identity, .plan = self.config.plan, .vtable = &.{ .next_chunk = nextChunk, .attention = dispatch, .attention_framed = dispatchFramed } };
    }
    fn check(self: *Session) !void {
        if (self.failed) return error.ContextSessionFailed;
        if (self.config.cancelled) |stop| if (stop.load(.acquire)) {
            if (self.next_layer != 0) self.failed = true;
            return error.Cancelled;
        };
        if (self.config.execution_control) |control| control.check() catch |err| {
            if (self.next_layer != 0) self.failed = true;
            return err;
        };
    }
    fn nextChunk(ctx: *anyopaque, identity: u64, start: u32, requested: u32) !u32 {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        if (identity != self.identity) return error.StaleContextView;
        try self.check();
        if (self.frozen) return error.ContextReplayActive;
        if (self.pending_frame_runtime != null or self.next_layer != 0 or start != self.logical_tokens or requested == 0 or start >= self.config.plan.logical_token_limit) return error.InvalidContextStep;
        return @intCast(@min(self.config.plan.logical_token_limit - start, @min(requested, self.config.max_query_rows)));
    }
    fn ref(r: contract.DeviceRef) Ref {
        return .{ .handle = r.handle, .offset = r.offset, .bytes = r.bytes };
    }
    fn dispatch(ctx: *anyopaque, identity: u64, r: contract.Attention, q: contract.DeviceRef, k: contract.DeviceRef, v: contract.DeviceRef, out: contract.DeviceRef) !void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        if (identity != self.identity) return error.StaleContextView;
        try self.check();
        if (self.frozen) return error.ContextReplayActive;
        errdefer self.failed = true;
        var protection = if (self.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        try self.run(r, ref(q), ref(k), ref(v), ref(out));
        try self.check();
    }
    fn dispatchFramed(ctx: *anyopaque, identity: u64, frame: contract.Frame, r: contract.Attention, q: contract.DeviceRef, k: contract.DeviceRef, v: contract.DeviceRef, out: contract.DeviceRef) !void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        if (identity != self.identity) return error.StaleContextView;
        try self.check();
        if (self.frozen) return error.ContextReplayActive;
        const runtime: *mr.RawMetalDecodeRuntime = @ptrCast(@alignCast(frame.runtime));
        if (self.config.execution_lease) |lease| try lease.registerExternalRuntime(runtime);
        if (self.pending_frame_runtime == null) {
            if (self.next_layer != 0) return error.InvalidContextStep;
            self.pending_frame_runtime = runtime;
            self.pending_start = self.logical_tokens;
            self.pending_rows = r.query_rows;
            // The session mutex and pending-frame marker exclude replay while
            // this frame encodes. Holding a publication reader here would
            // reject the frame's own owner-layer appends.
            self.pending_epoch = self.publication.epoch;
            @memset(self.pending_global_appended[0..], false);
            @memset(self.pending_ring_appended[0..], false);
            self.pending_split_dispatches = 0;
            self.pending_flash_dispatches = 0;
            if (mr.termite_metal_decode_runtime_set_context_frame_completion(runtime, self, completeFrame) != 0) {
                self.pending_frame_runtime = null;
                self.pending_epoch = 0;
                return error.ContextFrameOwnershipMismatch;
            }
        } else if (self.pending_frame_runtime.? != runtime or self.pending_start != r.query_start or self.pending_rows != r.query_rows) {
            return error.ContextFrameOwnershipMismatch;
        }
        errdefer self.failed = true;
        try self.runImpl(runtime, r, ref(q), ref(k), ref(v), ref(out));
    }
    fn ringSource(self: *Session, layer: usize, logical: u32) storage.Source {
        const l = self.config.layout.layers[layer];
        const ring = &self.sliding[self.owner_indices[layer]];
        return .{ .view = .{ .epoch = self.publication.epoch, .logical_tokens = logical, .capacity_rows = ring.capacity, .kv_heads = l.kv_heads, .head_dim = l.head_dim, .spans = ring.spans[0..ring.len] }, .keys = ring.keys.ref(), .values = ring.values.ref() };
    }
    fn run(self: *Session, r: contract.Attention, q: Ref, k: Ref, v: Ref, out: Ref) !void {
        return self.runImpl(null, r, q, k, v, out);
    }
    fn runImpl(self: *Session, owner_runtime: ?*mr.RawMetalDecodeRuntime, r: contract.Attention, q: Ref, k: Ref, v: Ref, out: Ref) !void {
        if (r.consumer_layer != self.next_layer or r.consumer_layer >= self.config.layout.len or r.query_start != self.logical_tokens or
            r.query_rows == 0 or r.query_rows > self.config.max_query_rows) return error.InvalidContextStep;
        const l = self.config.layout.layers[r.consumer_layer];
        if (r.owner_layer != l.owner or r.query_heads != l.query_heads or r.kv_heads != l.kv_heads or r.head_dim != l.head_dim or
            r.sliding_window != l.sliding_window or r.skip_kv_write != (r.consumer_layer != l.owner) or
            !std.math.isFinite(r.scale) or r.scale <= 0 or !std.math.isFinite(r.logit_softcap) or r.logit_softcap < 0) return error.InvalidContextLayout;
        const end = try std.math.add(u32, r.query_start, r.query_rows);
        if (end > self.config.plan.logical_token_limit) return error.InvalidContextStep;
        if (self.next_layer == 0) self.step_end = end else if (end != self.step_end) return error.InvalidContextStep;
        for ([_]Ref{ q, k, v }) |input| if (input.handle == out.handle) return error.AliasedContextStaging;
        for (self.globals) |g| {
            if (out.handle == g.keys.handle or out.handle == g.values.handle or (g.mass != null and out.handle == g.mass.?.handle)) return error.AliasedContextStaging;
        }
        for (self.sliding) |ring| if (out.handle == ring.keys.handle or out.handle == ring.values.handle) return error.AliasedContextStaging;
        if (self.scales[l.owner]) |s| if (s != r.scale or self.softcaps[l.owner].? != r.logit_softcap) return error.InvalidContextLayout;
        self.scales[l.owner] = r.scale;
        self.softcaps[l.owner] = r.logit_softcap;
        var bias = self.attention.core.dummy.ref();
        const source = if (l.sliding_window == 0) blk: {
            const g = &self.globals[self.owner_indices[l.owner]];
            if (!r.skip_kv_write) {
                if (owner_runtime) |runtime| {
                    const index = self.owner_indices[l.owner];
                    self.pending_global_ready[index] = g.ready;
                    self.pending_global_ingesting_empty[index] = g.ingesting_empty;
                    try g.appendOnActiveRuntime(&self.copier, &self.publication, runtime, r.query_rows, k, v);
                    self.pending_global_appended[index] = true;
                } else try g.append(&self.copier, &self.publication, r.query_rows, k, v);
            }
            if (g.mass) |b| bias = b.ref();
            break :blk try g.source();
        } else blk: {
            const ring = &self.sliding[self.owner_indices[l.owner]];
            if (!r.skip_kv_write) {
                if (owner_runtime) |runtime| {
                    const index = self.owner_indices[l.owner];
                    self.pending_ring_spans[index] = ring.spans;
                    self.pending_ring_lens[index] = ring.len;
                    try self.copier.appendRingOnActiveRuntime(runtime, r.query_start, r.query_rows, ring.capacity, l.head_dim * l.kv_heads, k, v, ring.keys.ref(), ring.values.ref());
                    self.pending_ring_appended[index] = true;
                } else try self.copier.appendRing(r.query_start, r.query_rows, ring.capacity, l.head_dim * l.kv_heads, k, v, ring.keys.ref(), ring.values.ref());
                ring.describe(end, ring.capacity);
            }
            break :blk self.ringSource(l.owner, end);
        };
        const epoch = if (owner_runtime != null) self.pending_epoch else try self.publication.acquire();
        defer if (owner_runtime == null) self.publication.release(epoch) catch unreachable;
        const before = self.attention.status_readback_bytes;
        defer self.metrics.attention_status_readback_bytes +|= self.attention.status_readback_bytes - before;
        const split_decode: ?attention.SplitDecode = if (self.config.optimize_decode and r.query_rows == 1) blk: {
            const scratch = self.decode_scratch orelse return error.InvalidContextLayout;
            const split = try splitDecodePlan(self.config, l.owner);
            if (scratch.logical_bytes < split.scratch_bytes) return error.ContextStagingBudgetExceeded;
            break :blk .{ .scratch = scratch.ref(), .split_count = split.split_count };
        } else null;
        const span_bytes = std.mem.sliceAsBytes(source.view.spans);
        const head_bytes = std.mem.sliceAsBytes(source.view.heads);
        const span_offset = self.descriptor_span_offsets[l.owner];
        const head_offset = self.descriptor_head_offsets[l.owner];
        if (span_bytes.len > self.descriptor_spans.logical_bytes - span_offset or head_bytes.len > self.descriptor_heads.logical_bytes - head_offset)
            return error.ContextDescriptorArenaExceeded;
        @memcpy(self.descriptor_spans.bytes[span_offset..][0..span_bytes.len], span_bytes);
        @memcpy(self.descriptor_heads.bytes[head_offset..][0..head_bytes.len], head_bytes);
        const use_flash = self.config.optimize_prefill and r.query_rows > 1 and source.view.version == views.abi_version;
        const request = attention.Request{ .view = source.view, .current_epoch = epoch, .query_start = r.query_start, .query_rows = r.query_rows, .query_heads = r.query_heads, .sliding_window = r.sliding_window, .scale = r.scale, .logit_softcap = r.logit_softcap, .split_decode = split_decode, .flash_prefill = use_flash, .descriptors = .{ .spans = try self.descriptor_spans.ref().slice(span_offset, span_bytes.len), .heads = try self.descriptor_heads.ref().slice(head_offset, head_bytes.len) } };
        if (owner_runtime) |runtime|
            try self.attention.attentionOnActiveRuntime(runtime, request, q, source.keys, source.values, bias, out)
        else
            try self.attention.attention(request, q, source.keys, source.values, bias, out);
        if (owner_runtime != null) {
            if (split_decode != null) self.pending_split_dispatches +|= 1;
            if (use_flash) self.pending_flash_dispatches +|= 1;
        } else {
            if (split_decode != null) self.split_decode_dispatches +|= 1;
            if (use_flash) self.flash_prefill_dispatches +|= 1;
        }
        self.next_layer += 1;
        if (self.next_layer == self.config.layout.len) {
            for (self.config.layout.layers[0..self.config.layout.len], 0..) |shape, layer| if (shape.owner == layer and shape.sliding_window > 0) {
                self.sliding[self.owner_indices[layer]].describe(end, shape.sliding_window);
            };
            self.logical_tokens = end;
            self.next_layer = 0;
            self.metrics.logical_tokens = end;
            self.metrics.epoch = epoch;
            self.refreshRetainedRows();
        }
    }
    fn rollbackPendingFrame(self: *Session) void {
        for (self.globals, 0..) |*generation, index| if (self.pending_global_appended[index]) {
            for (generation.plan.heads) |head| {
                const last = &generation.plan.spans[head.span_start + head.span_count - 1];
                last.row_count -|= self.pending_rows;
                last.logical_count -|= self.pending_rows;
            }
            generation.plan.descriptor.logical_tokens = self.pending_start;
            generation.ready = self.pending_global_ready[index];
            generation.ingesting_empty = self.pending_global_ingesting_empty[index];
            if (!generation.ready) @memset(generation.filled, false);
        };
        for (self.sliding, 0..) |*ring, index| if (self.pending_ring_appended[index]) {
            ring.spans = self.pending_ring_spans[index];
            ring.len = self.pending_ring_lens[index];
        };
        self.logical_tokens = self.pending_start;
        self.next_layer = 0;
        self.step_end = self.pending_start;
        self.metrics.logical_tokens = self.pending_start;
        self.refreshRetainedRows();
    }
    fn clearPendingFrame(self: *Session) void {
        self.pending_frame_runtime = null;
        self.pending_epoch = 0;
        self.pending_start = 0;
        self.pending_rows = 0;
        @memset(self.pending_global_appended[0..], false);
        @memset(self.pending_ring_appended[0..], false);
        self.pending_split_dispatches = 0;
        self.pending_flash_dispatches = 0;
    }
    fn completeFrame(raw: ?*anyopaque, completed: c_int) callconv(.c) c_int {
        const self: *Session = @ptrCast(@alignCast(raw orelse return -1));
        if (!self.mutex.tryLock()) return -2;
        defer self.mutex.unlock();
        if (self.pending_frame_runtime == null) return -3;
        const numerical_ok = completed != 0 and std.mem.bytesToValue(u32, self.attention.status.bytes[0..4]) == 0;
        const structurally_complete = self.next_layer == 0 and
            self.logical_tokens == self.pending_start + self.pending_rows;
        if (!numerical_ok or !structurally_complete) {
            self.rollbackPendingFrame();
            self.failed = true;
            self.metrics.rejected_transactions +|= 1;
            self.metrics.attention_status_readback_bytes +|= if (completed != 0) 4 else 0;
            self.clearPendingFrame();
            return -4;
        }
        self.metrics.attention_status_readback_bytes +|= 4;
        self.split_decode_dispatches +|= self.pending_split_dispatches;
        self.flash_prefill_dispatches +|= self.pending_flash_dispatches;
        self.framed_attention_dispatches +|= self.config.layout.len;
        self.clearPendingFrame();
        return 0;
    }
    /// Admission is the total live + private-branch peak. The live request's
    /// continuation remains reserved and its position cannot advance until the
    /// returned maintenance object closes. Replay must finish at the prompt
    /// boundary, before any generated token has entered the live KV.
    pub const ReplayOptions = struct {
        capture_start: ?u32 = null,
        query_sources: ?[]const query_protocol.Source = null,
        /// Loaded architecture position limit for a private branch. This is
        /// independent of the public serving window and is capped defensively.
        private_position_limit: ?u32 = null,
        /// Qualification-only exact global replay split. Null preserves the
        /// serial production/default route. The private branch owns and
        /// admits one page-rounded shared scratch buffer for this option.
        qualification_global_attention_split: ?@import("replay_context.zig").SplitConfig = null,
    };
    const ReplayControlResidency = struct { resident_bytes: usize, missing_bytes: usize };

    /// Must be called under the sequence lock. Replay controls belong to the
    /// immutable original generation after first use; private branches borrow
    /// them but must not charge them again.
    fn replayControlResidency(self: *Session) !ReplayControlResidency {
        const control_bytes = try storage.replayControlBytes();
        var result = ReplayControlResidency{ .resident_bytes = 0, .missing_bytes = 0 };
        for (self.globals) |*generation| {
            if (generation.hasReplayReaders()) return error.InvalidContextTransaction;
            const base_bytes = generation.plan.sizes.physical_bytes;
            const controlled_bytes = try std.math.add(usize, base_bytes, control_bytes);
            const owned_bytes = try generation.ownedBytes();
            if (owned_bytes == base_bytes) {
                result.missing_bytes = try std.math.add(usize, result.missing_bytes, control_bytes);
            } else if (owned_bytes == controlled_bytes) {
                result.resident_bytes = try std.math.add(usize, result.resident_bytes, control_bytes);
            } else {
                return error.ContextPhysicalAccountingUnavailable;
            }
        }
        return result;
    }

    /// Materializes the immutable replay controls that remain owned by the
    /// live session after a private branch closes. Call this before taking a
    /// committed-byte or capture baseline so first-use residency is admitted
    /// and cannot appear as a branch leak. The operation is idempotent.
    pub fn ensureReplayControls(self: *Session, peak_budget: usize) !usize {
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        try self.check();
        if (self.frozen or self.pending_frame_runtime != null or self.next_layer != 0 or
            self.logical_tokens != self.config.plan.prompt_tokens or self.publication.pending or
            self.publication.readers != 0)
            return error.InvalidContextTransaction;

        const reported_live_bytes = self.metrics.live_bytes;
        self.refreshLiveBytes();
        if (self.metrics.live_bytes != reported_live_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        const residency = try self.replayControlResidency();
        const projected_live_bytes = try std.math.add(usize, reported_live_bytes, residency.missing_bytes);
        if (projected_live_bytes > peak_budget) return error.ContextReplayBudgetExceeded;

        // A backend/allocator failure may occur after an earlier control was
        // created. Keep that persistent residency visible even on error.
        defer {
            self.refreshLiveBytes();
            self.metrics.peak_bytes = @max(self.metrics.peak_bytes, self.metrics.live_bytes);
        }
        const control_bytes = try storage.replayControlBytes();
        for (self.globals) |*generation| {
            if (try generation.ownedBytes() != generation.plan.sizes.physical_bytes) continue;
            var lease = try generation.acquireReplay(control_bytes);
            lease.deinit();
        }
        self.refreshLiveBytes();
        if (self.metrics.live_bytes != projected_live_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        self.metrics.peak_bytes = @max(self.metrics.peak_bytes, projected_live_bytes);
        return residency.missing_bytes;
    }

    pub fn beginReplay(self: *Session, additional_tokens: u32, peak_budget: usize) !Maintenance {
        return self.beginReplayWithOptions(additional_tokens, peak_budget, .{});
    }
    pub fn beginStoredReplayDiagnostic(self: *Session, additional_tokens: u32, peak_budget: usize, identity: StoredCampaignIdentity) !Maintenance {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        const lease = self.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        if (identity.load_generation != lease.generation or identity.session_identity != self.identity or
            identity.publication_epoch != self.publication.epoch) return error.StaleContextView;
        var maintenance = try self.beginReplayWithOptions(additional_tokens, peak_budget, .{});
        errdefer maintenance.deinit();
        const fingerprint = try self.qualificationFingerprintImpl(true);
        if (fingerprint.context_instance_id != identity.session_identity or
            fingerprint.publication_epoch != identity.publication_epoch or
            !std.meta.eql(fingerprint.descriptor_sha256, identity.source_descriptor_sha256) or
            !std.meta.eql(fingerprint.payload_sha256, identity.source_payload_sha256))
            return error.InvalidPayloadProvenance;
        maintenance.stored_identity = identity;
        return maintenance;
    }
    /// The source slice must outlive the returned Maintenance. Legacy callers
    /// retain the single-branch diagnostic sampler through beginReplay.
    pub fn beginReplayWithOptions(self: *Session, additional_tokens: u32, peak_budget: usize, options: ReplayOptions) !Maintenance {
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        try self.check();
        if (self.frozen or self.pending_frame_runtime != null) return error.ContextReplayActive;
        if (options.qualification_global_attention_split != null and !@import("builtin").is_test)
            return error.UnqualifiedCompactionArtifact;
        if (self.next_layer != 0 or self.logical_tokens != self.config.plan.prompt_tokens or additional_tokens == 0) return error.InvalidReferenceReplay;
        const origins = try self.allocator.alloc(replay.Origin, self.sizes.global_owners + self.sizes.sliding_owners);
        defer self.allocator.free(origins);
        var count: usize = 0;
        for (self.config.layout.layers[0..self.config.layout.len], 0..) |l, layer| if (l.owner == layer) {
            origins[count] = .{ .layer = @intCast(layer), .state = if (l.sliding_window == 0) .{ .global = &self.globals[self.owner_indices[layer]] } else .{ .sliding = self.ringSource(layer, self.logical_tokens) } };
            count += 1;
        };
        const position_limit = options.private_position_limit orelse self.config.plan.context_limit;
        const private_end = try std.math.add(u32, self.logical_tokens, additional_tokens);
        if (position_limit == 0 or position_limit > 131072 or private_end > position_limit)
            return error.ContextReplayBudgetExceeded;
        var config = replay.Config{ .plan = try (policy.Policy{}).admit(position_limit, null, self.logical_tokens, additional_tokens, 0), .layout = self.config.layout, .max_query_rows = self.config.max_query_rows, .capture_start = options.capture_start, .query_sources = options.query_sources, .global_attention_split = options.qualification_global_attention_split, .physical_budget_bytes = 0, .cancelled = self.config.cancelled, .execution_control = self.config.execution_control, .execution_lease = self.config.execution_lease, .allocation_tracker = self.allocation_tracker };
        const size = try replay.allocation(config, origins);
        const residency = try self.replayControlResidency();
        if (try std.math.add(usize, residency.resident_bytes, residency.missing_bytes) != size.owner_control_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        const incremental_physical_bytes = try std.math.sub(usize, size.physical_bytes, residency.resident_bytes);
        const peak = try std.math.add(usize, self.metrics.live_bytes, incremental_physical_bytes);
        if (peak > peak_budget) return error.ContextReplayBudgetExceeded;
        // Branch construction still validates its complete allocation shape;
        // admission above charges only bytes not already resident on live.
        config.physical_budget_bytes = size.physical_bytes;
        // Partial construction can create control objects retained by an
        // original even after its failed branch releases every lease.
        defer self.refreshLiveBytes();
        const branch = try replay.Session.init(self.allocator, config, origins);
        self.frozen = true;
        self.metrics.query_bytes = size.query_bytes;
        self.metrics.peak_bytes = @max(self.metrics.peak_bytes, peak);
        return .{ .live = self, .branch = branch, .epoch = self.publication.epoch, .incremental_physical_bytes = incremental_physical_bytes };
    }
    fn refreshRetainedRows(self: *Session) void {
        self.metrics.retained_rows = 0;
        for (self.globals) |g| for (0..g.plan.heads.len) |h| {
            var retained: u32 = 0;
            for (g.plan.descriptor.headSpans(h)) |s| retained += s.row_count;
            self.metrics.retained_rows = @max(self.metrics.retained_rows, retained);
        };
    }
    fn refreshLiveBytes(self: *Session) void {
        var bytes = self.sizes.physical_bytes - self.sizes.global_bytes;
        for (self.globals) |*g| bytes += g.ownedBytes() catch unreachable;
        self.metrics.live_bytes = bytes;
    }
};

/// Defensive binding to the token eligibility produced by the renderer and
/// tokenizer. A retention profile cannot select an instruction or partial
/// payload block merely by supplying a plausible logical offset.
fn validateSelection(eligible: *const payload.Eligible, parts: []const storage.Segment, logical: u32) !void {
    if (eligible.labels.len != logical) return error.InvalidPayloadProvenance;
    var start: u64 = 0;
    for (parts) |part| {
        const end = start + part.logical_count;
        if (end > logical) return error.InvalidPayloadProvenance;
        var lossy = false;
        for (part.retained) |count| if (count != part.logical_count) {
            lossy = true;
        };
        if (lossy) {
            if (part.logical_count != 512) return error.InvalidPayloadProvenance;
            var found = false;
            for (eligible.blocks) |block| if (block.start == start and block.end == end) {
                found = true;
            };
            if (!found or eligible.labels[@intCast(start)] == 0) return error.InvalidPayloadProvenance;
            const label = eligible.labels[@intCast(start)];
            for (eligible.labels[@intCast(start)..@intCast(end)]) |value| if (value != label) return error.InvalidPayloadProvenance;
        }
        start = end;
    }
    if (start != logical) return error.InvalidPayloadProvenance;
}

/// Move-only, request-scoped ownership. Close on every exit, including quality
/// rejection and cancellation. No fit can publish while replay leases exist.
pub const StoredCampaignIdentity = struct {
    load_generation: u64,
    session_identity: u64,
    publication_epoch: u64,
    query_campaign_identity: u64,
    query_campaign_sha256: [32]u8,
    query_payload_sha256: [32]u8 = @splat(0),
    query_content_sha256: [32]u8 = @splat(0),
    query_payload_readback_bytes: u64 = 0,
    source_descriptor_sha256: [32]u8,
    source_payload_sha256: [32]u8,
};

pub const StoredArtifact = struct {
    owner_layer: u32,
    head: u32,
    span: u32,
    logical_start: u32,
    logical_count: u32,
    retained_rows: u32,
    head_dim: u32,
    keys_f16: []const u8,
    values_f16: []const u8,
    bias_f32: []const u8,
    sha256: [32]u8,
};

const LoadedStoredArtifact = struct {
    owner_layer: u32,
    head: u32,
    span: u32,
    logical_start: u32,
    logical_count: u32,
    retained_rows: u32,
    head_dim: u32,
    keys: Buffer,
    values: Buffer,
    bias: Buffer,
    sha256: [32]u8,

    fn candidate(self: *const LoadedStoredArtifact) fitter.StoredCandidate {
        return .{ .rows = self.retained_rows, .keys = self.keys.ref(), .values = self.values.ref(), .bias = self.bias.ref() };
    }
};

fn updateStoredArtifactIdentity(hash: *std.crypto.hash.sha2.Sha256, identity: StoredCampaignIdentity, artifact: anytype) void {
    hash.update("antfly.context.stored-artifact.v1\x00");
    fingerprintU64(hash, identity.load_generation);
    fingerprintU64(hash, identity.session_identity);
    fingerprintU64(hash, identity.publication_epoch);
    fingerprintU64(hash, identity.query_campaign_identity);
    hash.update(&identity.query_campaign_sha256);
    hash.update(&identity.query_payload_sha256);
    hash.update(&identity.query_content_sha256);
    fingerprintU64(hash, identity.query_payload_readback_bytes);
    hash.update(&identity.source_descriptor_sha256);
    hash.update(&identity.source_payload_sha256);
    fingerprintU32(hash, artifact.owner_layer);
    fingerprintU32(hash, artifact.head);
    fingerprintU32(hash, artifact.span);
    fingerprintU32(hash, artifact.logical_start);
    fingerprintU32(hash, artifact.logical_count);
    fingerprintU32(hash, artifact.retained_rows);
    fingerprintU32(hash, artifact.head_dim);
}

fn storedInputArtifactDigest(identity: StoredCampaignIdentity, artifact: StoredArtifact) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    updateStoredArtifactIdentity(&hash, identity, artifact);
    hash.update(artifact.keys_f16);
    hash.update(artifact.values_f16);
    hash.update(artifact.bias_f32);
    return hash.finalResult();
}

/// Test-only coordinator entrypoint for the canonical stored artifact identity.
/// It exposes no upload, validation, Candidate, Report, or publication authority.
pub fn storedArtifactDiagnosticDigest(identity: StoredCampaignIdentity, artifact: StoredArtifact) ![32]u8 {
    if (!@import("builtin").is_test) return error.DiagnosticOnly;
    return storedInputArtifactDigest(identity, artifact);
}

fn storedLoadedArtifactDigest(identity: StoredCampaignIdentity, artifact: *const LoadedStoredArtifact) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    updateStoredArtifactIdentity(&hash, identity, artifact.*);
    hash.update(artifact.keys.bytes[0..artifact.keys.logical_bytes]);
    hash.update(artifact.values.bytes[0..artifact.values.logical_bytes]);
    hash.update(artifact.bias.bytes[0..artifact.bias.logical_bytes]);
    return hash.finalResult();
}

pub const StoredLoaderBudget = struct {
    entries_logical_bytes: usize,
    entries_physical_upper_bytes: usize,
    buffers_shared_bytes: usize,
    physical_upper_bytes: usize,
};

const StoredLoaderFaultStage = enum {
    after_keys,
    after_values,
    after_bias,
    after_digest,
};

const StoredLoaderFault = struct {
    entry_index: usize,
    stage: StoredLoaderFaultStage,

    fn checkpoint(self: ?StoredLoaderFault, entry_index: usize, stage: StoredLoaderFaultStage) !void {
        if (self) |fault| if (fault.entry_index == entry_index and fault.stage == stage)
            return error.InjectedStoredLoaderFailure;
    }
};

fn storedArtifactSharedBytes(artifact: StoredArtifact) !usize {
    const elements = try std.math.mul(usize, @as(usize, artifact.retained_rows), @as(usize, artifact.head_dim));
    const candidate_bytes = try std.math.mul(usize, elements, @sizeOf(f16));
    const bias_bytes = try std.math.mul(usize, @as(usize, artifact.retained_rows), @sizeOf(f32));
    const key_value = try std.math.mul(usize, 2, try rounded(candidate_bytes));
    return std.math.add(usize, key_value, try rounded(bias_bytes));
}

fn storedLoaderBudget(artifacts: []const StoredArtifact) !StoredLoaderBudget {
    if (artifacts.len == 0) return error.InvalidPayloadProvenance;
    const entries_logical = try std.math.mul(usize, artifacts.len, @sizeOf(LoadedStoredArtifact));
    const entries_upper = try rounded(entries_logical);
    var shared_bytes: usize = 0;
    for (artifacts) |artifact| shared_bytes = try std.math.add(usize, shared_bytes, try storedArtifactSharedBytes(artifact));
    return .{
        .entries_logical_bytes = entries_logical,
        .entries_physical_upper_bytes = entries_upper,
        .buffers_shared_bytes = shared_bytes,
        .physical_upper_bytes = try std.math.add(usize, entries_upper, shared_bytes),
    };
}

/// Test-binary-only admission query used by the external stored-artifact
/// provider. It exposes the exact existing loader calculation so the
/// coordinator never duplicates upload-budget arithmetic.
pub fn storedLoaderBudgetDiagnostic(artifacts: []const StoredArtifact) !StoredLoaderBudget {
    if (!@import("builtin").is_test) return error.DiagnosticOnly;
    return storedLoaderBudget(artifacts);
}

fn admitStoredLoader(artifacts: []const StoredArtifact, physical_budget_bytes: usize) !StoredLoaderBudget {
    const budget = try storedLoaderBudget(artifacts);
    if (budget.physical_upper_bytes > physical_budget_bytes) return error.ContextStagingBudgetExceeded;
    return budget;
}

fn admitStoredLoaderEntry(used_physical_bytes: u64, next_shared_bytes: u64, physical_budget_bytes: u64) !void {
    if (try std.math.add(u64, used_physical_bytes, next_shared_bytes) > physical_budget_bytes)
        return error.ContextStagingBudgetExceeded;
}

fn validateStoredLoaderRelease(before: owner_alloc.Snapshot, after: owner_alloc.Snapshot, owner_identity: u64, entries_logical: u64, entries_physical: u64, shared_bytes: u64) !void {
    const physical_bytes = try std.math.add(u64, entries_physical, shared_bytes);
    if (!before.tracked_combined_physical_available or !after.tracked_combined_physical_available or
        before.owner_identity != owner_identity or after.owner_identity != owner_identity or
        before.logical_live_bytes < after.logical_live_bytes or before.logical_live_bytes - after.logical_live_bytes != entries_logical or
        before.physical_live_bytes < after.physical_live_bytes or before.physical_live_bytes - after.physical_live_bytes != entries_physical or
        before.shared_backing_live_bytes < after.shared_backing_live_bytes or before.shared_backing_live_bytes - after.shared_backing_live_bytes != shared_bytes or
        before.private_device_live_bytes != after.private_device_live_bytes or
        before.tracked_combined_live_bytes < after.tracked_combined_live_bytes or
        before.tracked_combined_live_bytes - after.tracked_combined_live_bytes != physical_bytes)
        return error.ContextPhysicalAccountingUnavailable;
}

/// Request-owned immutable upload lease. Construction verifies each artifact
/// digest before and after copying into separately owned tracked Metal buffers.
/// `claim` is irreversible: a failed validation/publication cannot reuse the
/// same stored bytes or historical evidence under a second transaction.
pub const StoredLoader = struct {
    allocator: std.mem.Allocator,
    tracker: *owner_alloc.HostAllocationTracker,
    execution_lease: *execution_pool.Lease,
    identity: StoredCampaignIdentity,
    entries: []LoadedStoredArtifact,
    initialized: usize = 0,
    physical_bytes: u64,
    entries_logical_bytes: u64,
    entries_physical_bytes: u64,
    buffer_shared_bytes: u64,
    tracker_owner_identity: u64,
    manifest_sha256: [32]u8,
    claimed: bool = false,
    released: bool = false,

    fn loadEntry(runtime: *mr.RawMetalDecodeRuntime, tracker: *owner_alloc.HostAllocationTracker, identity: StoredCampaignIdentity, artifact: StoredArtifact, entry_index: usize, fault: ?StoredLoaderFault) !LoadedStoredArtifact {
        const candidate_elements = try std.math.mul(usize, @as(usize, artifact.retained_rows), @as(usize, artifact.head_dim));
        const candidate_bytes = try std.math.mul(usize, candidate_elements, @sizeOf(f16));
        const bias_bytes = try std.math.mul(usize, @as(usize, artifact.retained_rows), @sizeOf(f32));
        if (artifact.logical_count == 0 or artifact.retained_rows == 0 or artifact.retained_rows >= artifact.logical_count or
            artifact.keys_f16.len != candidate_bytes or artifact.values_f16.len != candidate_bytes or artifact.bias_f32.len != bias_bytes or
            !std.meta.eql(storedInputArtifactDigest(identity, artifact), artifact.sha256)) return error.InvalidPayloadProvenance;
        var keys = try Buffer.initTracked(runtime, candidate_bytes, tracker);
        errdefer keys.deinit();
        try StoredLoaderFault.checkpoint(fault, entry_index, .after_keys);
        var values = try Buffer.initTracked(runtime, candidate_bytes, tracker);
        errdefer values.deinit();
        try StoredLoaderFault.checkpoint(fault, entry_index, .after_values);
        var bias = try Buffer.initTracked(runtime, bias_bytes, tracker);
        errdefer bias.deinit();
        try StoredLoaderFault.checkpoint(fault, entry_index, .after_bias);
        @memcpy(keys.bytes[0..candidate_bytes], artifact.keys_f16);
        @memcpy(values.bytes[0..candidate_bytes], artifact.values_f16);
        @memcpy(bias.bytes[0..bias_bytes], artifact.bias_f32);
        const entry = LoadedStoredArtifact{ .owner_layer = artifact.owner_layer, .head = artifact.head, .span = artifact.span, .logical_start = artifact.logical_start, .logical_count = artifact.logical_count, .retained_rows = artifact.retained_rows, .head_dim = artifact.head_dim, .keys = keys, .values = values, .bias = bias, .sha256 = artifact.sha256 };
        if (!std.meta.eql(storedLoadedArtifactDigest(identity, &entry), artifact.sha256)) return error.InvalidPayloadProvenance;
        try StoredLoaderFault.checkpoint(fault, entry_index, .after_digest);
        return entry;
    }

    pub fn init(
        tracker: *owner_alloc.HostAllocationTracker,
        execution_lease: *execution_pool.Lease,
        identity: StoredCampaignIdentity,
        artifacts: []const StoredArtifact,
        physical_budget_bytes: usize,
    ) !StoredLoader {
        return initImpl(tracker, execution_lease, identity, artifacts, physical_budget_bytes, null);
    }

    fn initWithFault(
        tracker: *owner_alloc.HostAllocationTracker,
        execution_lease: *execution_pool.Lease,
        identity: StoredCampaignIdentity,
        artifacts: []const StoredArtifact,
        physical_budget_bytes: usize,
        fault: StoredLoaderFault,
    ) !StoredLoader {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        return initImpl(tracker, execution_lease, identity, artifacts, physical_budget_bytes, fault);
    }

    fn initImpl(
        tracker: *owner_alloc.HostAllocationTracker,
        execution_lease: *execution_pool.Lease,
        identity: StoredCampaignIdentity,
        artifacts: []const StoredArtifact,
        physical_budget_bytes: usize,
        fault: ?StoredLoaderFault,
    ) !StoredLoader {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (artifacts.len == 0 or identity.load_generation == 0 or identity.session_identity == 0 or
            identity.publication_epoch == 0 or identity.query_campaign_identity == 0 or
            execution_lease.generation != identity.load_generation) return error.InvalidPayloadProvenance;
        const budget = try admitStoredLoader(artifacts, physical_budget_bytes);
        const before = tracker.snapshot();
        if (!before.tracked_combined_physical_available) return error.ContextPhysicalAccountingUnavailable;
        const allocator = tracker.allocator();
        const entries = try allocator.alloc(LoadedStoredArtifact, artifacts.len);
        const after_entries = tracker.snapshot();
        if (!after_entries.tracked_combined_physical_available or after_entries.owner_identity != before.owner_identity or
            after_entries.logical_live_bytes < before.logical_live_bytes or after_entries.logical_live_bytes - before.logical_live_bytes != @as(u64, @intCast(budget.entries_logical_bytes)) or
            after_entries.physical_live_bytes <= before.physical_live_bytes or after_entries.physical_live_bytes - before.physical_live_bytes > @as(u64, @intCast(budget.entries_physical_upper_bytes)) or
            after_entries.shared_backing_live_bytes != before.shared_backing_live_bytes or after_entries.private_device_live_bytes != before.private_device_live_bytes)
        {
            allocator.free(entries);
            return error.ContextPhysicalAccountingUnavailable;
        }
        const entries_physical = after_entries.physical_live_bytes - before.physical_live_bytes;
        var self = StoredLoader{ .allocator = allocator, .tracker = tracker, .execution_lease = execution_lease, .identity = identity, .entries = entries, .physical_bytes = 0, .entries_logical_bytes = @intCast(budget.entries_logical_bytes), .entries_physical_bytes = entries_physical, .buffer_shared_bytes = 0, .tracker_owner_identity = before.owner_identity, .manifest_sha256 = undefined };
        errdefer self.deinitPartial();
        const runtime = (try execution_lease.execution(.fitter)).runtime;
        for (artifacts, 0..) |artifact, index| {
            if (index > 0) {
                const prior = artifacts[index - 1];
                const ordered = artifact.owner_layer > prior.owner_layer or
                    (artifact.owner_layer == prior.owner_layer and artifact.head > prior.head) or
                    (artifact.owner_layer == prior.owner_layer and artifact.head == prior.head and artifact.span > prior.span);
                if (!ordered) return error.InvalidPayloadProvenance;
            }
            const entry_upper = try storedArtifactSharedBytes(artifact);
            const before_entry = try std.math.add(u64, self.entries_physical_bytes, self.buffer_shared_bytes);
            try admitStoredLoaderEntry(before_entry, @intCast(entry_upper), @intCast(physical_budget_bytes));
            entries[index] = try loadEntry(runtime, tracker, identity, artifact, index, fault);
            self.initialized += 1;
            var entry_shared: u64 = 0;
            entry_shared = try std.math.add(u64, entry_shared, @as(u64, @intCast(entries[index].keys.tracked_backing_bytes)));
            entry_shared = try std.math.add(u64, entry_shared, @as(u64, @intCast(entries[index].values.tracked_backing_bytes)));
            entry_shared = try std.math.add(u64, entry_shared, @as(u64, @intCast(entries[index].bias.tracked_backing_bytes)));
            if (entry_shared != @as(u64, @intCast(entry_upper))) return error.ContextPhysicalAccountingUnavailable;
            self.buffer_shared_bytes = try std.math.add(u64, self.buffer_shared_bytes, entry_shared);
        }
        var manifest = std.crypto.hash.sha2.Sha256.init(.{});
        manifest.update("antfly.context.stored-loader.v1\x00");
        fingerprintU32(&manifest, @intCast(entries.len));
        for (entries) |entry| manifest.update(&entry.sha256);
        manifest.final(&self.manifest_sha256);
        if (self.buffer_shared_bytes != @as(u64, @intCast(budget.buffers_shared_bytes))) return error.ContextPhysicalAccountingUnavailable;
        self.physical_bytes = try std.math.add(u64, self.entries_physical_bytes, self.buffer_shared_bytes);
        if (self.physical_bytes > @as(u64, @intCast(physical_budget_bytes))) return error.ContextStagingBudgetExceeded;
        const after = tracker.snapshot();
        if (!after.tracked_combined_physical_available or after.owner_identity != before.owner_identity or
            after.logical_live_bytes < before.logical_live_bytes or after.logical_live_bytes - before.logical_live_bytes != self.entries_logical_bytes or
            after.physical_live_bytes < before.physical_live_bytes or after.physical_live_bytes - before.physical_live_bytes != self.entries_physical_bytes or
            after.shared_backing_live_bytes < before.shared_backing_live_bytes or after.shared_backing_live_bytes - before.shared_backing_live_bytes != self.buffer_shared_bytes or
            after.private_device_live_bytes != before.private_device_live_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        return self;
    }

    fn deinitPartial(self: *StoredLoader) void {
        for (self.entries[0..self.initialized]) |*entry| {
            entry.keys.deinit();
            entry.values.deinit();
            entry.bias.deinit();
        }
        self.allocator.free(self.entries);
    }

    /// Drain the pinned execution owner, destroy the loader, then prove the
    /// exact loader-owned host/shared components were released. Unrelated live
    /// generation bytes may change between construction and this teardown.
    pub fn deinitChecked(self: *StoredLoader) !owner_alloc.Snapshot {
        if (self.released) return error.StoredLoaderReleased;
        // Drain errors leave the loader intact and retryable. Once destruction
        // starts, released becomes terminal before post-release accounting so
        // a proof failure can never trigger a second free from caller cleanup.
        try self.execution_lease.drainForBufferRelease();
        const tracker = self.tracker;
        const owner_identity = self.tracker_owner_identity;
        const entries_logical = self.entries_logical_bytes;
        const entries_physical = self.entries_physical_bytes;
        const shared_bytes = self.buffer_shared_bytes;
        const before = tracker.snapshot();
        self.deinitPartial();
        self.released = true;
        const after = tracker.snapshot();
        try validateStoredLoaderRelease(before, after, owner_identity, entries_logical, entries_physical, shared_bytes);
        return after;
    }

    fn claim(self: *StoredLoader, lease: *execution_pool.Lease, identity: StoredCampaignIdentity) !void {
        if (self.released or self.claimed or self.execution_lease != lease or self.identity.load_generation != identity.load_generation or
            !std.meta.eql(self.identity, identity) or lease.generation != identity.load_generation) return error.StaleContextView;
        const snapshot = self.tracker.snapshot();
        if (!snapshot.tracked_combined_physical_available or snapshot.owner_identity != self.tracker_owner_identity or
            snapshot.logical_live_bytes < self.entries_logical_bytes or snapshot.physical_live_bytes < self.entries_physical_bytes or
            snapshot.shared_backing_live_bytes < self.buffer_shared_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        for (self.entries) |*entry| if (!std.meta.eql(storedLoadedArtifactDigest(identity, entry), entry.sha256))
            return error.InvalidPayloadProvenance;
        self.claimed = true;
    }
};

pub const StoredPublicationReceipt = struct {
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    load_generation: u64,
    session_identity: u64,
    publication_epoch_before: u64,
    publication_epoch_after: u64,
    query_campaign_identity: u64,
    query_campaign_sha256: [32]u8,
    query_payload_sha256: [32]u8 = @splat(0),
    query_content_sha256: [32]u8 = @splat(0),
    query_payload_readback_bytes: u64 = 0,
    source_descriptor_sha256: [32]u8,
    source_payload_sha256: [32]u8,
    artifact_count: u32,
    loader_physical_bytes: u64,
    loader_tracker_owner_identity: u64,
    loader_entries_physical_bytes: u64,
    loader_buffer_shared_bytes: u64,
    loader_manifest_sha256: [32]u8,
    validation_status_readback_bytes: u64,
};

/// Test-only final closure hook. The caller owns immutable open descriptors
/// for source/build/artifact evidence and must revalidate those same objects
/// here. It receives identities only and no Session, destination, Batch, or
/// Candidate authority.
pub const StoredPrecommitVerifier = struct {
    ctx: *anyopaque,
    verify_fn: *const fn (*anyopaque, StoredCampaignIdentity, [32]u8) anyerror!void,
};

test "stored loader admission rejects budget before allocation" {
    var bytes: [8]u8 = @splat(0);
    const artifacts = [_]StoredArtifact{.{
        .owner_layer = 4,
        .head = 0,
        .span = 0,
        .logical_start = 0,
        .logical_count = 4,
        .retained_rows = 2,
        .head_dim = 1,
        .keys_f16 = bytes[0..4],
        .values_f16 = bytes[0..4],
        .bias_f32 = bytes[0..8],
        .sha256 = @splat(0),
    }};
    const budget = try admitStoredLoader(&artifacts, std.math.maxInt(usize));
    try std.testing.expectEqual(try rounded(@sizeOf(LoadedStoredArtifact)), budget.entries_physical_upper_bytes);
    try std.testing.expectEqual(3 * std.heap.pageSize(), budget.buffers_shared_bytes);
    try std.testing.expectError(error.ContextStagingBudgetExceeded, admitStoredLoader(&artifacts, budget.physical_upper_bytes - 1));
}

test "stored loader budget covers every entry before upload" {
    var bytes: [8]u8 = @splat(0);
    const artifact = StoredArtifact{ .owner_layer = 4, .head = 0, .span = 0, .logical_start = 0, .logical_count = 4, .retained_rows = 2, .head_dim = 1, .keys_f16 = bytes[0..4], .values_f16 = bytes[0..4], .bias_f32 = bytes[0..8], .sha256 = @splat(0) };
    const artifacts = [_]StoredArtifact{ artifact, artifact };
    const budget = try storedLoaderBudget(&artifacts);
    try std.testing.expectEqual(6 * std.heap.pageSize(), budget.buffers_shared_bytes);
    try std.testing.expectError(error.ContextStagingBudgetExceeded, admitStoredLoader(&artifacts, budget.physical_upper_bytes - 1));
}

test "stored loader release validates only loader scoped deltas" {
    const before = owner_alloc.Snapshot{ .available = true, .owner_identity = 9, .physical_available = true, .logical_live_bytes = 1200, .physical_live_bytes = 2400, .shared_backing_live_bytes = 8192, .private_device_live_bytes = 3000, .tracked_combined_physical_available = true, .tracked_combined_live_bytes = 13592 };
    const after = owner_alloc.Snapshot{ .available = true, .owner_identity = 9, .physical_available = true, .logical_live_bytes = 1000, .physical_live_bytes = 2000, .shared_backing_live_bytes = 4096, .private_device_live_bytes = 3000, .tracked_combined_physical_available = true, .tracked_combined_live_bytes = 9096 };
    try validateStoredLoaderRelease(before, after, 9, 200, 400, 4096);
    var leaked = after;
    leaked.shared_backing_live_bytes += 4096;
    leaked.tracked_combined_live_bytes += 4096;
    try std.testing.expectError(error.ContextPhysicalAccountingUnavailable, validateStoredLoaderRelease(before, leaked, 9, 200, 400, 4096));
    var wrong_owner = after;
    wrong_owner.owner_identity = 10;
    try std.testing.expectError(error.ContextPhysicalAccountingUnavailable, validateStoredLoaderRelease(before, wrong_owner, 9, 200, 400, 4096));
}

test "stored loader incremental budget rejects before next upload" {
    try admitStoredLoaderEntry(4096, 8192, 12288);
    try std.testing.expectError(error.ContextStagingBudgetExceeded, admitStoredLoaderEntry(4096, 8192, 12287));
}

fn expectStoredLoaderTrackerSnapshot(expected: owner_alloc.Snapshot, actual: owner_alloc.Snapshot) !void {
    try std.testing.expectEqual(expected.owner_identity, actual.owner_identity);
    try std.testing.expectEqual(expected.logical_live_bytes, actual.logical_live_bytes);
    try std.testing.expectEqual(expected.physical_live_bytes, actual.physical_live_bytes);
    try std.testing.expectEqual(expected.shared_backing_live_bytes, actual.shared_backing_live_bytes);
    try std.testing.expectEqual(expected.private_device_live_bytes, actual.private_device_live_bytes);
    try std.testing.expectEqual(expected.tracked_combined_live_bytes, actual.tracked_combined_live_bytes);
}

test "context Metal stored loader releases every failed prefix and accepts unrelated live bytes at teardown" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const identity = StoredCampaignIdentity{
        .load_generation = 7,
        .session_identity = 11,
        .publication_epoch = 13,
        .query_campaign_identity = 17,
        .query_campaign_sha256 = @splat(0x19),
        .source_descriptor_sha256 = @splat(0x21),
        .source_payload_sha256 = @splat(0x23),
    };
    var key_bytes = [_]u8{ 0x00, 0x3c, 0x00, 0x40 };
    var value_bytes = [_]u8{ 0x00, 0x38, 0x00, 0x3c };
    var bias_bytes = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    var artifacts = [_]StoredArtifact{
        .{ .owner_layer = 4, .head = 0, .span = 0, .logical_start = 0, .logical_count = 4, .retained_rows = 2, .head_dim = 1, .keys_f16 = &key_bytes, .values_f16 = &value_bytes, .bias_f32 = &bias_bytes, .sha256 = undefined },
        .{ .owner_layer = 4, .head = 0, .span = 1, .logical_start = 4, .logical_count = 4, .retained_rows = 2, .head_dim = 1, .keys_f16 = &key_bytes, .values_f16 = &value_bytes, .bias_f32 = &bias_bytes, .sha256 = undefined },
    };
    for (&artifacts) |*artifact| artifact.sha256 = storedInputArtifactDigest(identity, artifact.*);
    const budget = try storedLoaderBudget(&artifacts);

    var pool = try execution_pool.Pool.init(identity.load_generation, execution_pool.admitted_backend_bytes);
    defer pool.deinit();
    try pool.warm(null);
    var lease = try pool.acquire(identity.load_generation);
    defer if (lease.pool != null) lease.poisonAndRelease();

    // The request-local allocator fault fails the entries slice itself. No
    // second cleanup owner may free the nonexistent slice.
    var entries_fault_tracker = owner_alloc.HostAllocationTracker.init(null, 0);
    const entries_baseline = entries_fault_tracker.snapshot();
    try entries_fault_tracker.armNextPositiveAllocationFailure();
    try std.testing.expectError(error.OutOfMemory, StoredLoader.init(&entries_fault_tracker, &lease, identity, &artifacts, budget.physical_upper_bytes));
    try expectStoredLoaderTrackerSnapshot(entries_baseline, entries_fault_tracker.snapshot());

    // Every entry and every partial key/value/bias/digest prefix is releasable,
    // and a later construction on the same owner still succeeds.
    var tracker = owner_alloc.HostAllocationTracker.init(null, 0);
    const baseline = tracker.snapshot();
    inline for (.{ StoredLoaderFaultStage.after_keys, .after_values, .after_bias, .after_digest }) |stage| {
        for (0..artifacts.len) |entry_index| {
            try std.testing.expectError(error.InjectedStoredLoaderFailure, StoredLoader.initWithFault(&tracker, &lease, identity, &artifacts, budget.physical_upper_bytes, .{ .entry_index = entry_index, .stage = stage }));
            try expectStoredLoaderTrackerSnapshot(baseline, tracker.snapshot());
        }
    }
    var malformed = artifacts;
    malformed[1].sha256[0] ^= 1;
    try std.testing.expectError(error.InvalidPayloadProvenance, StoredLoader.init(&tracker, &lease, identity, &malformed, budget.physical_upper_bytes));
    try expectStoredLoaderTrackerSnapshot(baseline, tracker.snapshot());

    var consumed = try StoredLoader.init(&tracker, &lease, identity, &artifacts, budget.physical_upper_bytes);
    consumed.buffer_shared_bytes = try std.math.add(u64, consumed.buffer_shared_bytes, std.heap.pageSize());
    try std.testing.expectError(error.ContextPhysicalAccountingUnavailable, consumed.deinitChecked());
    try std.testing.expect(consumed.released);
    try std.testing.expectError(error.StoredLoaderReleased, consumed.deinitChecked());
    try expectStoredLoaderTrackerSnapshot(baseline, tracker.snapshot());

    var loader = try StoredLoader.init(&tracker, &lease, identity, &artifacts, budget.physical_upper_bytes);
    const unrelated = try tracker.allocator().alloc(u8, 37);
    const with_unrelated = tracker.snapshot();
    try std.testing.expect(with_unrelated.logical_live_bytes > baseline.logical_live_bytes);
    const after_loader = try loader.deinitChecked();
    try std.testing.expect(after_loader.logical_live_bytes > baseline.logical_live_bytes);
    tracker.allocator().free(unrelated);
    try expectStoredLoaderTrackerSnapshot(baseline, tracker.snapshot());
    try lease.release();
}

test "stored publication receipt is diagnostic without serving or release authority" {
    const receipt = StoredPublicationReceipt{
        .load_generation = 1,
        .session_identity = 2,
        .publication_epoch_before = 3,
        .publication_epoch_after = 4,
        .query_campaign_identity = 5,
        .query_campaign_sha256 = @splat(0x11),
        .source_descriptor_sha256 = @splat(0x22),
        .source_payload_sha256 = @splat(0x33),
        .artifact_count = 36,
        .loader_physical_bytes = 4096,
        .loader_tracker_owner_identity = 7,
        .loader_entries_physical_bytes = 2048,
        .loader_buffer_shared_bytes = 4096,
        .loader_manifest_sha256 = @splat(0x44),
        .validation_status_readback_bytes = 256,
    };
    try std.testing.expect(receipt.diagnostic_only);
    try std.testing.expect(!receipt.serving_authority);
    try std.testing.expect(!receipt.release_authority);
}

test "stored artifact digest binds bytes coordinates and campaign identity" {
    const identity = StoredCampaignIdentity{
        .load_generation = 7,
        .session_identity = 11,
        .publication_epoch = 13,
        .query_campaign_identity = 17,
        .query_campaign_sha256 = @splat(0x19),
        .source_descriptor_sha256 = @splat(0x21),
        .source_payload_sha256 = @splat(0x43),
    };
    var keys = [_]u8{ 1, 2, 3, 4 };
    const values = [_]u8{ 5, 6, 7, 8 };
    const bias = [_]u8{ 9, 10, 11, 12, 13, 14, 15, 16 };
    var artifact = StoredArtifact{ .owner_layer = 4, .head = 0, .span = 1, .logical_start = 512, .logical_count = 512, .retained_rows = 2, .head_dim = 1, .keys_f16 = &keys, .values_f16 = &values, .bias_f32 = &bias, .sha256 = undefined };
    artifact.sha256 = storedInputArtifactDigest(identity, artifact);
    try std.testing.expectEqual(artifact.sha256, storedInputArtifactDigest(identity, artifact));
    keys[0] ^= 1;
    try std.testing.expect(!std.meta.eql(artifact.sha256, storedInputArtifactDigest(identity, artifact)));
    keys[0] ^= 1;
    var other = identity;
    other.publication_epoch += 1;
    try std.testing.expect(!std.meta.eql(artifact.sha256, storedInputArtifactDigest(other, artifact)));
}

pub const Maintenance = struct {
    live: *Session,
    branch: replay.Session,
    epoch: u64,
    /// Request-local branch bytes after excluding replay controls already
    /// resident on the immutable live owners.
    incremental_physical_bytes: usize,
    stored_identity: ?StoredCampaignIdentity = null,
    published: bool = false,
    pub fn deinit(self: *Maintenance) void {
        self.live.metrics.replay_status_readback_bytes +|= self.branch.status_readback_bytes;
        self.branch.deinit();
        self.live.refreshLiveBytes();
        self.live.metrics.query_bytes = 0;
        self.live.frozen = false;
        self.* = undefined;
    }
    pub fn seal(self: *Maintenance) !void {
        try self.branch.seal();
    }
    /// Bind a fresh sealed campaign to this already-active gate maintenance.
    /// This avoids a second replay and keeps the same frozen live descriptor
    /// through validation and the one all-owner Batch commit.
    pub fn bindStoredIdentityDiagnostic(self: *Maintenance, campaign: *const query_campaign.Campaign, identity: StoredCampaignIdentity) !void {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (self.stored_identity != null or self.published or !self.branch.sealed or !self.live.frozen)
            return error.InvalidContextTransaction;
        const lease = self.live.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        const fingerprint = try self.live.qualificationFingerprintImpl(true);
        const campaign_fingerprint = try campaign.storedDiagnosticFingerprint();
        const payload_evidence = try campaign.storedDiagnosticPayloadDigest();
        const content_evidence = try campaign.storedDiagnosticContentDigest();
        if (identity.load_generation != lease.generation or identity.session_identity != self.live.identity or
            identity.publication_epoch != self.live.publication.epoch or identity.query_campaign_identity != campaign.campaign_identity or
            !std.meta.eql(identity.query_campaign_sha256, campaign_fingerprint) or
            !std.meta.eql(identity.query_payload_sha256, payload_evidence.sha256) or
            !std.meta.eql(identity.query_content_sha256, content_evidence.sha256) or
            identity.query_payload_readback_bytes != payload_evidence.readback_bytes or
            std.mem.allEqual(u8, &identity.query_payload_sha256, 0) or
            std.mem.allEqual(u8, &identity.query_content_sha256, 0) or
            !std.meta.eql(identity.source_descriptor_sha256, fingerprint.descriptor_sha256) or
            !std.meta.eql(identity.source_payload_sha256, fingerprint.payload_sha256))
            return error.InvalidPayloadProvenance;
        self.stored_identity = identity;
    }
    const QuerySource = union(enum) {
        branch,
        campaign: *const query_campaign.Campaign,
    };
    const FitInput = struct {
        request: fitter.Request,
        training: Ref,
        validation: Ref,
    };
    fn fitRequest(self: *Maintenance, owner_index: usize, rows: u32, workspace: usize, training_rows: u32, validation_rows: u32, validation_group_rows: u32) !fitter.Request {
        const layer = self.live.global_layers[owner_index];
        // The current fitter implements ordinary softmax only. Never claim
        // that fitting a softcapped model uses the same numerical objective.
        if (self.live.softcaps[layer] == null or self.live.softcaps[layer].? != 0) return error.UnsupportedCompactionProfile;
        return .{ .source_rows = 512, .head_dim = self.live.config.layout.layers[layer].head_dim, .kv_heads = 1, .kv_head = 0, .training_rows = training_rows, .validation_rows = validation_rows, .validation_group_rows = validation_group_rows, .scale = self.live.scales[layer] orelse return error.InvalidContextLayout, .staging_budget_bytes = workspace, .options = .{ .retained_rows = rows } };
    }
    fn queryGeometry(self: *Maintenance, source: QuerySource, owner_index: usize) !query_campaign.FitGeometry {
        const layer = self.live.global_layers[owner_index];
        return switch (source) {
            .branch => blk: {
                const pool = try self.branch.sealedQueries(layer);
                break :blk .{ .training_rows = pool.shape.training_rows, .validation_rows = pool.shape.validation_rows, .validation_group_rows = pool.shape.held_per_stratum };
            },
            .campaign => |campaign| campaign.fitGeometry(layer),
        };
    }
    fn fitInput(self: *Maintenance, source: QuerySource, owner_index: usize, head: usize, rows: u32, workspace: usize) !FitInput {
        const layer = self.live.global_layers[owner_index];
        const shape = self.live.config.layout.layers[layer];
        const geometry = try self.queryGeometry(source, owner_index);
        const request = try self.fitRequest(owner_index, rows, workspace, geometry.training_rows, geometry.validation_rows, geometry.validation_group_rows);
        return switch (source) {
            .branch => blk: {
                const pool = try self.branch.sealedQueries(layer);
                const query_offset = head * (pool.shape.training_rows + pool.shape.validation_rows) * shape.head_dim * 4;
                const train_bytes = @as(usize, pool.shape.training_rows) * shape.head_dim * 4;
                const held_bytes = @as(usize, pool.shape.validation_rows) * shape.head_dim * 4;
                break :blk .{
                    .request = request,
                    .training = try pool.queries.ref().slice(query_offset, train_bytes),
                    .validation = try pool.queries.ref().slice(query_offset + train_bytes, held_bytes),
                };
            },
            .campaign => |campaign| blk: {
                const training = try campaign.view(layer, @intCast(head), .training);
                const validation = try campaign.view(layer, @intCast(head), .validation);
                if (training.rows != geometry.training_rows or validation.rows != geometry.validation_rows or
                    validation.validation_group_rows != geometry.validation_group_rows)
                    return error.InvalidQueryCampaignPartition;
                break :blk .{ .request = request, .training = training.queries, .validation = validation.queries };
            },
        };
    }
    /// Frozen retention decisions only. All candidate heads must pass the
    /// unchanged training/held-out mass and per-consumer output gates. Exact
    /// heads bypass the fitter. Quality rejection, allocation failure or
    /// cancellation destroys every staged owner and preserves the originals.
    /// The peak includes live KV, sealed query pools, both generations and one
    /// serial fitter workspace. No model/artifact profile is chosen here.
    pub fn publish(self: *Maintenance, eligible: *const payload.Eligible, parts: []const []const storage.Segment, retained_limit: u32, peak_budget: usize) !void {
        return self.publishWithQueries(.branch, eligible, parts, retained_limit, peak_budget, if (@import("builtin").is_test) null else {});
    }
    pub fn publishCampaign(self: *Maintenance, campaign: *const query_campaign.Campaign, eligible: *const payload.Eligible, parts: []const []const storage.Segment, retained_limit: u32, peak_budget: usize) !void {
        return self.publishCampaignWithFault(campaign, eligible, parts, retained_limit, peak_budget, if (@import("builtin").is_test) null else {});
    }
    pub fn publishCampaignWithFault(self: *Maintenance, campaign: *const query_campaign.Campaign, eligible: *const payload.Eligible, parts: []const []const storage.Segment, retained_limit: u32, peak_budget: usize, fault_controller: fault_injection.Slot) !void {
        const lease = self.live.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        if (campaign.load_generation != lease.generation or campaign.campaign_identity == 0)
            return error.StaleContextView;
        return self.publishWithQueries(.{ .campaign = campaign }, eligible, parts, retained_limit, peak_budget, fault_controller);
    }
    /// Test-only all-owner transaction for immutable stored payloads. The
    /// loader and fresh query campaign are consumed on first attempt. Every
    /// lossy span has exactly one stored artifact; every artifact is used once.
    /// Genuine device training/held validation and synchronous copy complete
    /// before the existing Batch atomically commits all owners.
    pub fn publishStoredCampaignDiagnostic(
        self: *Maintenance,
        campaign: *query_campaign.Campaign,
        loader: *StoredLoader,
        eligible: *const payload.Eligible,
        parts: []const []const storage.Segment,
        retained_limit: u32,
        peak_budget: usize,
        precommit: ?StoredPrecommitVerifier,
    ) !StoredPublicationReceipt {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        const live = self.live;
        if (!live.mutex.tryLock()) return error.ContextTransactionBusy;
        defer live.mutex.unlock();
        try live.check();
        const lease = live.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        const identity = self.stored_identity orelse return error.InvalidPayloadProvenance;
        if (self.published or !live.frozen or !self.branch.sealed or live.publication.epoch != self.epoch or
            live.logical_tokens != self.branch.config.plan.prompt_tokens or parts.len != live.globals.len or
            identity.session_identity != live.identity or identity.publication_epoch != live.publication.epoch or
            identity.load_generation != lease.generation or identity.query_campaign_identity != campaign.campaign_identity or
            campaign.load_generation != lease.generation) return error.InvalidContextTransaction;
        errdefer live.metrics.rejected_transactions +|= 1;
        if (!std.meta.eql(try campaign.storedDiagnosticFingerprint(), identity.query_campaign_sha256))
            return error.InvalidPayloadProvenance;
        const payload_evidence = try campaign.storedDiagnosticPayloadDigest();
        const content_evidence = try campaign.storedDiagnosticContentDigest();
        if (!std.meta.eql(payload_evidence.sha256, identity.query_payload_sha256) or
            !std.meta.eql(content_evidence.sha256, identity.query_content_sha256) or
            payload_evidence.readback_bytes != identity.query_payload_readback_bytes)
            return error.InvalidPayloadProvenance;
        // Both claims are deliberately irreversible, including on later
        // quality rejection, cancellation, allocation failure or commit error.
        try loader.claim(lease, identity);
        try campaign.claimStoredDiagnostic(identity.load_generation, identity.query_campaign_identity, identity.query_campaign_sha256);

        var shapes: [256]storage.Shape = undefined;
        if (parts.len > shapes.len) return error.InvalidContextLayout;
        var workspace: usize = 0;
        var new_bytes: usize = 0;
        var old_bytes: usize = 0;
        var lossy: usize = 0;
        for (live.globals, parts, 0..) |*generation, segments, owner_index| {
            if (!generation.plan.descriptor.coversExact(0, live.logical_tokens)) return error.InvalidReferenceReplay;
            shapes[owner_index] = .{ .head_dim = generation.plan.descriptor.head_dim, .kv_heads = generation.plan.descriptor.kv_heads, .continuation_rows = @intCast(live.config.plan.logical_token_limit - live.logical_tokens), .max_retained_rows = retained_limit };
            const size = try storage.allocation(shapes[owner_index], segments);
            try validateSelection(eligible, segments, live.logical_tokens);
            new_bytes = try std.math.add(usize, new_bytes, size.physical_bytes);
            old_bytes = try std.math.add(usize, old_bytes, try generation.ownedBytes());
            if (segments.len > descriptorSpansPerHead(live.config)) return error.ContextDescriptorArenaExceeded;
            for (segments, 0..) |segment, span| for (segment.retained, 0..) |count, head| if (count != segment.logical_count) {
                const input = try self.fitInput(.{ .campaign = campaign }, owner_index, head, count, std.math.maxInt(usize));
                workspace = @max(workspace, (try fitter.validationAllocation(input.request)).physical_bytes);
                var matches: usize = 0;
                var logical_start: u32 = 0;
                for (segments[0..span]) |prior| logical_start = try std.math.add(u32, logical_start, prior.logical_count);
                for (loader.entries) |entry| {
                    if (entry.owner_layer == live.global_layers[owner_index] and entry.head == @as(u32, @intCast(head)) and entry.span == @as(u32, @intCast(span)) and
                        entry.logical_start == logical_start and entry.logical_count == segment.logical_count and entry.retained_rows == count and
                        entry.head_dim == generation.plan.descriptor.head_dim) matches += 1;
                }
                if (matches != 1) return error.InvalidPayloadProvenance;
                lossy += 1;
            };
        }
        if (lossy != loader.entries.len) return error.InvalidPayloadProvenance;
        const sealed_query_bytes = try std.math.add(usize, self.branch.sizes.sealed_bytes, campaign.sizes.physical_bytes);
        const loader_bytes = std.math.cast(usize, loader.physical_bytes) orelse return error.ContextStagingBudgetExceeded;
        const preserved_live_bytes = try std.math.sub(usize, live.metrics.live_bytes, old_bytes);
        const loader_and_query = try std.math.add(usize, sealed_query_bytes, try std.math.add(usize, loader_bytes, std.heap.pageSize()));
        const outside_batch = try std.math.add(usize, preserved_live_bytes, loader_and_query);
        const generations = try std.math.add(usize, old_bytes, new_bytes);
        const staged_records = try rounded(try std.math.mul(usize, parts.len, @sizeOf(storage.Generation)));
        const batch_peak = try std.math.add(usize, generations, try std.math.add(usize, workspace, staged_records));
        const peak = try std.math.add(usize, outside_batch, batch_peak);
        if (peak > peak_budget) return error.ContextStagingBudgetExceeded;

        var protection = if (live.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        var batch = try storage.Batch.init(live.allocator, &live.copier, &live.publication, live.globals, shapes[0..parts.len], parts, peak_budget - outside_batch, workspace, live.config.cancelled);
        defer batch.deinit();
        var kernels = try fitter.Kernels.initBorrowedTracked(try lease.execution(.fitter), live.allocation_tracker);
        defer kernels.deinit();
        kernels.core.cancelled = live.config.cancelled;
        kernels.execution_control = live.config.execution_control;
        defer live.metrics.status_readback_bytes +|= kernels.status_readback_bytes;
        live.metrics.peak_bytes = @max(live.metrics.peak_bytes, peak);
        var validated: usize = 0;
        for (batch.staged, live.globals, parts, 0..) |*next, *old, segments, owner_index| {
            const source = try old.source();
            for (old.plan.heads, 0..) |head, head_index| {
                if (head.span_count != 1) return error.InvalidReferenceReplay;
                const dim = old.plan.descriptor.head_dim;
                var logical: u32 = 0;
                for (segments, 0..) |segment, span| {
                    try live.check();
                    if (segment.retained[head_index] == segment.logical_count) {
                        try next.copyExact(&live.copier, head_index, span, source);
                    } else {
                        const row_start = try std.math.add(u32, head.kv_start, logical);
                        const offset = try std.math.mul(usize, @as(usize, row_start), try std.math.mul(usize, dim, @sizeOf(f16)));
                        const bytes = try std.math.mul(usize, @as(usize, segment.logical_count), try std.math.mul(usize, dim, @sizeOf(f16)));
                        const input = try self.fitInput(.{ .campaign = campaign }, owner_index, head_index, segment.retained[head_index], workspace);
                        var selected: ?*const LoadedStoredArtifact = null;
                        for (loader.entries) |*entry| if (entry.owner_layer == live.global_layers[owner_index] and entry.head == @as(u32, @intCast(head_index)) and entry.span == @as(u32, @intCast(span)) and
                            entry.logical_start == logical and entry.logical_count == segment.logical_count and entry.retained_rows == segment.retained[head_index] and entry.head_dim == dim)
                        {
                            if (selected != null) return error.InvalidPayloadProvenance;
                            selected = entry;
                        };
                        _ = try next.validateAndCopyStoredDiagnostic(&kernels, head_index, span, input.request, try source.keys.slice(offset, bytes), try source.values.slice(offset, bytes), input.training, input.validation, (selected orelse return error.InvalidPayloadProvenance).candidate());
                        validated += 1;
                    }
                    logical = try std.math.add(u32, logical, segment.logical_count);
                }
            }
            try next.finish();
        }
        if (validated != loader.entries.len) return error.InvalidPayloadProvenance;
        try live.check();
        if (precommit) |closure| try closure.verify_fn(closure.ctx, identity, loader.manifest_sha256);
        // The verifier may have consumed time or observed cancellation. No
        // destination owner becomes visible until this final check and the
        // immediately following single commit.
        try live.check();
        try batch.commit();
        self.published = true;
        live.refreshLiveBytes();
        return .{
            .load_generation = identity.load_generation,
            .session_identity = identity.session_identity,
            .publication_epoch_before = identity.publication_epoch,
            .publication_epoch_after = live.publication.epoch,
            .query_campaign_identity = identity.query_campaign_identity,
            .query_campaign_sha256 = identity.query_campaign_sha256,
            .query_payload_sha256 = identity.query_payload_sha256,
            .query_content_sha256 = identity.query_content_sha256,
            .query_payload_readback_bytes = identity.query_payload_readback_bytes,
            .source_descriptor_sha256 = identity.source_descriptor_sha256,
            .source_payload_sha256 = identity.source_payload_sha256,
            .artifact_count = @intCast(validated),
            .loader_physical_bytes = loader.physical_bytes,
            .loader_tracker_owner_identity = loader.tracker_owner_identity,
            .loader_entries_physical_bytes = loader.entries_physical_bytes,
            .loader_buffer_shared_bytes = loader.buffer_shared_bytes,
            .loader_manifest_sha256 = loader.manifest_sha256,
            .validation_status_readback_bytes = kernels.status_readback_bytes,
        };
    }
    fn publishWithQueries(self: *Maintenance, query_source: QuerySource, eligible: *const payload.Eligible, parts: []const []const storage.Segment, retained_limit: u32, peak_budget: usize, fault_controller: fault_injection.Slot) !void {
        const live = self.live;
        if (!live.mutex.tryLock()) return error.ContextTransactionBusy;
        defer live.mutex.unlock();
        try live.check();
        if (self.published or !live.frozen or !self.branch.sealed or live.publication.epoch != self.epoch or
            live.logical_tokens != self.branch.config.plan.prompt_tokens or parts.len != live.globals.len) return error.InvalidContextTransaction;
        errdefer live.metrics.rejected_transactions +|= 1;
        var shapes: [256]storage.Shape = undefined;
        var workspace: usize = 0;
        var new_bytes: usize = 0;
        var old_bytes: usize = 0;
        var blocks: u32 = 0;
        for (live.globals, parts, 0..) |*g, segments, index| {
            if (!g.plan.descriptor.coversExact(0, live.logical_tokens)) return error.InvalidReferenceReplay;
            shapes[index] = .{ .head_dim = g.plan.descriptor.head_dim, .kv_heads = g.plan.descriptor.kv_heads, .continuation_rows = @intCast(live.config.plan.logical_token_limit - live.logical_tokens), .max_retained_rows = retained_limit };
            const size = try storage.allocation(shapes[index], segments);
            try validateSelection(eligible, segments, live.logical_tokens);
            new_bytes = try std.math.add(usize, new_bytes, size.physical_bytes);
            old_bytes = try std.math.add(usize, old_bytes, try g.ownedBytes());
            if (segments.len > descriptorSpansPerHead(live.config)) return error.ContextDescriptorArenaExceeded;
            for (segments) |segment| for (segment.retained) |count| if (count != segment.logical_count) {
                const geometry = try self.queryGeometry(query_source, index);
                workspace = @max(workspace, (try fitter.allocation(try self.fitRequest(index, count, std.math.maxInt(usize), geometry.training_rows, geometry.validation_rows, geometry.validation_group_rows))).physical_bytes);
                blocks += 1;
            };
        }
        // The fitter runtime's dummy page coexists with its candidate/workspace.
        const query_bytes = switch (query_source) {
            .branch => @as(usize, 0),
            .campaign => |campaign| campaign.sizes.physical_bytes,
        };
        const sealed_query_bytes = try std.math.add(usize, self.branch.sizes.sealed_bytes, query_bytes);
        const outside_batch = try std.math.add(usize, live.metrics.live_bytes - old_bytes, try std.math.add(usize, sealed_query_bytes, std.heap.pageSize()));
        const batch_peak = try std.math.add(usize, old_bytes + new_bytes, workspace + try rounded(parts.len * @sizeOf(storage.Generation)));
        const peak = try std.math.add(usize, outside_batch, batch_peak);
        if (peak > peak_budget) return error.ContextStagingBudgetExceeded;
        var protection = if (live.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        var batch = try storage.Batch.init(live.allocator, &live.copier, &live.publication, live.globals, shapes[0..parts.len], parts, peak_budget - outside_batch, workspace, live.config.cancelled);
        defer batch.deinit();
        var kernels = if (live.config.execution_lease) |lease|
            try fitter.Kernels.initBorrowedTracked(try lease.execution(.fitter), live.allocation_tracker)
        else
            try fitter.Kernels.initTracked(live.allocation_tracker);
        defer kernels.deinit();
        kernels.core.cancelled = live.config.cancelled;
        kernels.execution_control = live.config.execution_control;
        defer live.metrics.status_readback_bytes +|= kernels.status_readback_bytes;
        live.metrics.peak_bytes = @max(live.metrics.peak_bytes, peak);
        var completed_fits: u32 = 0;
        for (batch.staged, live.globals, parts, 0..) |*next, *old, segments, index| {
            const source = try old.source();
            for (old.plan.heads, 0..) |head, h| {
                // Direct ingestion creates one dense exact span per head;
                // replay forbids recursively fitting a compacted generation.
                if (head.span_count != 1) return error.InvalidReferenceReplay;
                const dim = old.plan.descriptor.head_dim;
                var logical: u32 = 0;
                for (segments, 0..) |segment, s| {
                    try live.check();
                    if (segment.retained[h] == segment.logical_count) {
                        try next.copyExact(&live.copier, h, s, source);
                    } else {
                        const offset = @as(usize, head.kv_start + logical) * dim * 2;
                        const bytes = @as(usize, segment.logical_count) * dim * 2;
                        const input = try self.fitInput(query_source, index, h, segment.retained[h], workspace);
                        if (comptime @import("builtin").is_test) if (fault_controller) |fault| {
                            if (fault.shouldInjectNonfinite(completed_fits, blocks)) switch (query_source) {
                                .campaign => |campaign| try @constCast(campaign).injectNonfiniteTrainingScalar(live.global_layers[index], @intCast(h)),
                                .branch => return error.UnqualifiedFaultInjection,
                            };
                        };
                        next.fitAndPack(&kernels, h, s, input.request, try source.keys.slice(offset, bytes), try source.values.slice(offset, bytes), input.training, input.validation) catch |err| {
                            if (comptime @import("builtin").is_test) if (fault_controller) |fault| {
                                if (err == error.NonFiniteCompactionInput and fault.kind == .nonfinite)
                                    return fault.recordNonfiniteDetection(completed_fits, blocks);
                            };
                            return err;
                        };
                        completed_fits += 1;
                        if (comptime @import("builtin").is_test) if (fault_controller) |fault| {
                            if (fault.kind == .gpu_failure)
                                return fault.recordGpuCompletionFailure(completed_fits, blocks);
                        };
                        if (comptime @import("builtin").is_test) if (fault_controller) |fault|
                            try fault.checkpoint(.fit, completed_fits, blocks);
                    }
                    logical += segment.logical_count;
                }
            }
            try next.finish();
        }
        try live.check();
        // All candidate generations and fits are staged and validated here.
        // Inject immediately before the one atomic publication commit so a
        // cancelled publish proves the original generation stays installed.
        if (comptime @import("builtin").is_test) if (fault_controller) |fault|
            try fault.checkpoint(.publish, 1, 1);
        try batch.commit();
        self.published = true;
        live.refreshLiveBytes();
        live.metrics.epoch = live.publication.epoch;
        live.metrics.compacted_blocks += blocks;
        live.refreshRetainedRows();
    }
};

fn fixtureConfig() !Config {
    var layout = policy.Layout{ .len = 6 };
    for (layout.layers[0..6], 0..) |*l, i| l.* = .{ .owner = @intCast(i % 3), .sliding_window = if (i % 3 == 0) 16 else 0, .kv_heads = 2, .query_heads = 4, .head_dim = if (i % 3 == 0) 256 else 512, .rope_active_dim = 256, .rope_frequency_dim = 256, .rope_theta = 10000 };
    var config = Config{ .plan = try (policy.Policy{}).admit(4096, null, 1025, 3, 0), .layout = layout, .max_query_rows = 64, .physical_budget_bytes = 0 };
    config.physical_budget_bytes = (try allocation(config)).physical_bytes;
    return config;
}
fn device(r: Ref) contract.DeviceRef {
    return .{ .handle = r.handle, .offset = r.offset, .bytes = r.bytes };
}
fn fixtureStep(hook: contract.Hook, layout: policy.Layout, start: u32, rows: u32, q: Buffer, k: Buffer, v: Buffer, out: Buffer) !void {
    for (layout.layers[0..layout.len], 0..) |l, layer| {
        const r = contract.Attention{ .consumer_layer = @intCast(layer), .owner_layer = l.owner, .query_start = start, .query_rows = rows, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = l.head_dim, .sliding_window = l.sliding_window, .skip_kv_write = layer != l.owner, .scale = 1 };
        try hook.attention(r, device(q.ref()), device(k.ref()), device(v.ref()), device(out.ref()));
        for (out.floats()[0 .. rows * l.query_heads * l.head_dim]) |value| try std.testing.expectApproxEqAbs(@as(f32, 1), value, 0.002);
    }
}

fn fixtureFramedStep(hook: contract.Hook, layout: policy.Layout, runtime: *mr.RawMetalDecodeRuntime, start: u32, rows: u32, q: Buffer, k: Buffer, v: Buffer, out: Buffer) !void {
    for (layout.layers[0..layout.len], 0..) |l, layer| {
        const r = contract.Attention{ .consumer_layer = @intCast(layer), .owner_layer = l.owner, .query_start = start, .query_rows = rows, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = l.head_dim, .sliding_window = l.sliding_window, .skip_kv_write = layer != l.owner, .scale = 1 };
        try hook.attentionFramed(.{ .runtime = @ptrCast(runtime) }, r, device(q.ref()), device(k.ref()), device(v.ref()), device(out.ref()));
    }
}

test "context packed live allocation validates donors and bounds sliding storage" {
    var config = try fixtureConfig();
    const first = try allocation(config);
    var optimized = config;
    optimized.optimize_decode = true;
    const optimized_sizes = try allocation(optimized);
    try std.testing.expect(optimized_sizes.decode_scratch_bytes > 0);
    try std.testing.expectEqual(first.physical_bytes + optimized_sizes.decode_scratch_bytes, optimized_sizes.physical_bytes);
    config.plan = try (policy.Policy{}).admit(131072, null, 65536, 64, 0);
    const long = try allocation(config);
    try std.testing.expectEqual(first.sliding_bytes, long.sliding_bytes);
    try std.testing.expect(long.global_bytes > first.global_bytes);
    config.layout.layers[5].head_dim = 256;
    try std.testing.expectError(error.InvalidContextLayout, allocation(config));
    try std.testing.expectError(error.InvalidPackedContextPlan, storage.Generation.emptyAllocation(512, 2, 0));
}

test "context Metal packed session returns borrowed owner tracker to entry baseline" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var tracker = owner_alloc.HostAllocationTracker.init(null, 0);
    const tracked = tracker.allocator();
    const preflight = try tracked.alloc(u8, 37);
    defer tracked.free(preflight);
    const before = tracker.snapshot();

    var config = try fixtureConfig();
    config.owner_allocation_tracker = &tracker;
    var live = try Session.init(std.testing.allocator, config);
    const during = tracker.snapshot();
    try std.testing.expect(during.logical_live_bytes > before.logical_live_bytes);
    const receipt = live.deinitMeasured();
    const after = tracker.snapshot();
    try std.testing.expect(receipt.released);
    try std.testing.expectEqual(before.logical_live_bytes, after.logical_live_bytes);
    try std.testing.expectEqual(before.physical_live_bytes, after.physical_live_bytes);
    try std.testing.expectEqual(before.shared_backing_live_bytes, after.shared_backing_live_bytes);
    if (after.tracked_combined_physical_available)
        try std.testing.expectEqual(after.tracked_combined_live_bytes, receipt.post_release_live_bytes);
}

test "context Metal packed lifecycle ingests replays rejects rolls back publishes and continues" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    var stop = std.atomic.Value(bool).init(false);
    var config = try fixtureConfig();
    config.cancelled = &stop;
    var denied = config;
    denied.physical_budget_bytes -= 1;
    try std.testing.expectError(error.ContextStagingBudgetExceeded, Session.init(a, denied));
    var live = try Session.init(a, config);
    defer live.deinit();
    for (live.globals) |*g| try std.testing.expectError(error.IncompleteContextGeneration, g.source());
    var q = try Buffer.init(live.copier.core.runtime, 64 * 4 * 512 * 4);
    defer q.deinit();
    var k = try Buffer.init(live.copier.core.runtime, 64 * 2 * 512 * 4);
    defer k.deinit();
    var v = try Buffer.init(live.copier.core.runtime, k.logical_bytes);
    defer v.deinit();
    var out = try Buffer.init(live.copier.core.runtime, q.logical_bytes);
    defer out.deinit();
    @memset(q.bytes, 0);
    @memset(k.bytes, 0);
    @memset(v.floats(), 1);
    while (live.logical_tokens < config.plan.prompt_tokens) {
        const rows = try live.hook().nextChunk(live.logical_tokens, @intCast(config.plan.prompt_tokens - live.logical_tokens));
        try fixtureStep(live.hook(), config.layout, live.logical_tokens, rows, q, k, v, out);
    }
    try std.testing.expectEqual(@as(u32, 1025), live.logical_tokens);
    try std.testing.expectError(error.ContextReplayBudgetExceeded, live.beginReplay(32, 1));
    for (live.globals) |*g| try std.testing.expect(!g.hasReplayReaders());

    // First-use replay controls are persistent session residency, not private
    // branch bytes. Admission is all-or-none by budget, and preparation is
    // idempotent before committed/capture baselines are taken.
    const live_before_controls = live.metrics.live_bytes;
    const replay_control_bytes = try storage.replayControlBytes();
    const all_control_bytes = try std.math.mul(usize, replay_control_bytes, live.globals.len);
    try std.testing.expect(all_control_bytes > 0);
    try std.testing.expectError(error.ContextReplayBudgetExceeded, live.ensureReplayControls(live_before_controls + all_control_bytes - 1));
    try std.testing.expectEqual(live_before_controls, live.metrics.live_bytes);
    try std.testing.expectEqual(all_control_bytes, try live.ensureReplayControls(live_before_controls + all_control_bytes));
    const controlled_live_bytes = live_before_controls + all_control_bytes;
    try std.testing.expectEqual(controlled_live_bytes, live.metrics.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), try live.ensureReplayControls(controlled_live_bytes));
    try std.testing.expectEqual(controlled_live_bytes, live.metrics.live_bytes);

    const branch_config = replay.Config{
        .plan = try (policy.Policy{}).admit(config.plan.context_limit, null, live.logical_tokens, 32, 0),
        .layout = config.layout,
        .max_query_rows = config.max_query_rows,
        .physical_budget_bytes = 0,
    };
    const branch_size = try replay.estimate(branch_config);
    try std.testing.expectEqual(all_control_bytes, branch_size.owner_control_bytes);
    const branch_incremental_bytes = try std.math.sub(usize, branch_size.physical_bytes, all_control_bytes);
    const exact_replay_peak = try std.math.add(usize, controlled_live_bytes, branch_incremental_bytes);

    // An abandoned or cancelled replay releases all leases without publishing.
    // Its exact peak succeeds: the resident controls are not charged twice.
    {
        var aborted = try live.beginReplay(32, exact_replay_peak);
        try std.testing.expectEqual(branch_incremental_bytes, aborted.incremental_physical_bytes);
        try std.testing.expectEqual(try std.math.add(usize, branch_incremental_bytes, all_control_bytes), aborted.branch.sizes.physical_bytes);
        defer aborted.deinit();
        try std.testing.expectError(error.ContextReplayActive, live.hook().nextChunk(1025, 1));
        try std.testing.expectError(error.ContextReplayActive, live.beginReplay(32, 1024 * 1024 * 1024));
        stop.store(true, .release);
        try std.testing.expectError(error.Cancelled, aborted.branch.hook().nextChunk(1025, 1));
        stop.store(false, .release);
    }
    for (live.globals) |*g| try std.testing.expect(!g.hasReplayReaders());
    try std.testing.expectEqual(controlled_live_bytes, live.metrics.live_bytes);
    var maintenance = try live.beginReplay(32, 1024 * 1024 * 1024);
    var active = true;
    defer if (active) maintenance.deinit();
    try std.testing.expectError(error.IncompleteReferenceReplay, maintenance.seal());
    try fixtureStep(maintenance.branch.hook(), config.layout, 1025, 32, q, k, v, out);
    try maintenance.seal();
    for (live.globals) |*g| try std.testing.expect(!g.hasReplayReaders());
    try std.testing.expectError(error.ContextReplayActive, live.hook().nextChunk(1025, 1));
    const old_handles = [_]*anyopaque{ live.globals[0].keys.handle, live.globals[1].keys.handle };
    const before = live.metrics.live_bytes;
    var labels: [1025]u32 = @splat(0);
    @memset(labels[1..513], 1);
    var eligible_blocks = [_]@import("../runtime/kv/reference_replay.zig").Range{.{ .start = 1, .end = 513 }};
    const eligible = payload.Eligible{ .allocator = a, .labels = &labels, .ranges = &eligible_blocks, .blocks = &eligible_blocks };
    const segments = [_]storage.Segment{ .{ .logical_count = 1, .retained = &.{ 1, 1 } }, .{ .logical_count = 512, .retained = &.{ 256, 512 } }, .{ .logical_count = 512, .retained = &.{ 512, 512 } } };
    const all = [_][]const storage.Segment{ &segments, &segments };
    labels[32] = 0;
    try std.testing.expectError(error.InvalidPayloadProvenance, maintenance.publish(&eligible, &all, 1028, 1024 * 1024 * 1024));
    labels[32] = 1;
    try std.testing.expectError(error.ContextStagingBudgetExceeded, maintenance.publish(&eligible, &all, 1028, 1));
    try std.testing.expectError(error.ContextRetainedBudgetExceeded, maintenance.publish(&eligible, &all, 1027, 1024 * 1024 * 1024));
    const exact = [_]storage.Segment{.{ .logical_count = 1025, .retained = &.{ 1025, 1025 } }};
    try std.testing.expectError(error.ContextAllocationNotReduced, maintenance.publish(&eligible, &.{ &exact, &exact }, 1028, 1024 * 1024 * 1024));
    // Fail the second owner only after the first has been fitted and staged.
    const pool = try maintenance.branch.sealedQueries(2);
    const old_query = pool.queries.floats()[0];
    pool.queries.floats()[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteCompactionInput, maintenance.publish(&eligible, &all, 1028, 1024 * 1024 * 1024));
    pool.queries.floats()[0] = old_query;
    try std.testing.expectEqual(@as(u64, 1), live.publication.epoch);
    try std.testing.expect(!live.publication.pending);
    for (live.globals, old_handles) |g, handle| try std.testing.expectEqual(handle, g.keys.handle);
    try maintenance.publish(&eligible, &all, 1028, 1024 * 1024 * 1024);
    try std.testing.expect(live.metrics.live_bytes < before);
    try std.testing.expectEqual(@as(u64, 3), live.publication.epoch);
    try std.testing.expectError(error.InvalidContextTransaction, maintenance.publish(&eligible, &all, 1028, 1024 * 1024 * 1024));
    try std.testing.expectEqual(@as(u32, 1025), live.logical_tokens);
    maintenance.deinit();
    active = false;
    try fixtureStep(live.hook(), config.layout, 1025, 3, q, k, v, out);
    try std.testing.expectEqual(@as(u32, 1028), live.logical_tokens);
    try std.testing.expectEqual(@as(u64, 0), live.metrics.kv_readback_bytes);
    try std.testing.expectEqual(@as(u32, 2), live.metrics.compacted_blocks);
    try std.testing.expectError(error.InvalidContextStep, live.hook().nextChunk(1028, 1));
    std.debug.print("context_packed_lifecycle: original_bytes={d} committed_bytes={d} peak_bytes={d} global_owners=2 replay_tokens=32 continuation_tokens=3 epoch={d} rollback_preserved=true kv_readback_bytes=0\n", .{ before, live.metrics.live_bytes, live.metrics.peak_bytes, live.publication.epoch });
}

test "context Metal packed owner frame commits once and cancellation clears callback ownership" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const config = try fixtureConfig();

    var live = try Session.init(a, config);
    defer live.deinit();
    const runtime = live.copier.core.runtime;
    var q = try Buffer.init(runtime, 4 * 512 * 4);
    defer q.deinit();
    var k = try Buffer.init(runtime, 2 * 512 * 4);
    defer k.deinit();
    var v = try Buffer.init(runtime, k.logical_bytes);
    defer v.deinit();
    var out = try Buffer.init(runtime, q.logical_bytes);
    defer out.deinit();
    @memset(q.bytes, 0);
    @memset(k.bytes, 0);
    @memset(v.floats(), 1);

    try mr.beginFrame(runtime);
    errdefer mr.cancelFrame(runtime) catch {};
    try fixtureFramedStep(live.hook(), config.layout, runtime, 0, 1, q, k, v, out);
    try mr.submitFrame(runtime);
    try mr.waitFrame(runtime);
    try std.testing.expectEqual(@as(u32, 1), live.logical_tokens);
    try std.testing.expectEqual(@as(u64, config.layout.len), live.framed_attention_dispatches);
    try std.testing.expectEqual(@as(u64, config.layout.len), live.attention.framed_dispatches);
    try std.testing.expectEqual(@as(u64, 0), live.attention.standalone_dispatches);
    try std.testing.expectEqual(@as(u64, 4), live.metrics.attention_status_readback_bytes);

    var cancelled = try Session.init(a, config);
    const cancelled_runtime = cancelled.copier.core.runtime;
    try mr.beginFrame(cancelled_runtime);
    const l = config.layout.layers[0];
    const request = contract.Attention{ .consumer_layer = 0, .owner_layer = l.owner, .query_start = 0, .query_rows = 1, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = l.head_dim, .sliding_window = l.sliding_window, .skip_kv_write = false, .scale = 1 };
    try cancelled.hook().attentionFramed(.{ .runtime = @ptrCast(cancelled_runtime) }, request, device(q.ref()), device(k.ref()), device(v.ref()), device(out.ref()));
    try std.testing.expectError(error.SubmissionFailed, mr.cancelFrame(cancelled_runtime));
    try std.testing.expectEqual(@as(u32, 0), cancelled.logical_tokens);
    try std.testing.expect(cancelled.pending_frame_runtime == null);
    cancelled.deinit();

    var partial = try Session.init(a, config);
    const partial_runtime = partial.copier.core.runtime;
    try mr.beginFrame(partial_runtime);
    try partial.hook().attentionFramed(.{ .runtime = @ptrCast(partial_runtime) }, request, device(q.ref()), device(k.ref()), device(v.ref()), device(out.ref()));
    try std.testing.expectError(error.CommandBufferFailed, mr.flushActiveFrame(partial_runtime));
    try std.testing.expectEqual(@as(u32, 0), partial.logical_tokens);
    try std.testing.expect(partial.pending_frame_runtime == null);
    partial.deinit();
}

test "context Metal packed request rejects stale hooks and poisons interrupted model steps" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    var stop = std.atomic.Value(bool).init(false);
    var config = try fixtureConfig();
    config.cancelled = &stop;
    const baseline_physical_bytes = config.physical_budget_bytes;
    config.optimize_decode = true;
    const optimized_sizes = try allocation(config);
    try std.testing.expectEqual(baseline_physical_bytes + optimized_sizes.decode_scratch_bytes, optimized_sizes.physical_bytes);
    config.physical_budget_bytes = optimized_sizes.physical_bytes;
    var denied = config;
    denied.physical_budget_bytes -= 1;
    try std.testing.expectError(error.ContextStagingBudgetExceeded, Session.init(a, denied));
    var live = try Session.init(a, config);
    defer live.deinit();
    var q = try Buffer.init(live.copier.core.runtime, 4 * 512 * 4);
    defer q.deinit();
    var k = try Buffer.init(live.copier.core.runtime, 2 * 512 * 4);
    defer k.deinit();
    var v = try Buffer.init(live.copier.core.runtime, k.logical_bytes);
    defer v.deinit();
    var out = try Buffer.init(live.copier.core.runtime, q.logical_bytes);
    defer out.deinit();
    @memset(q.bytes, 0);
    @memset(k.bytes, 0);
    @memset(v.floats(), 1);
    var stale = live.hook();
    stale.identity += 1;
    try std.testing.expectError(error.StaleContextView, stale.nextChunk(0, 1));
    stop.store(true, .release);
    try std.testing.expectError(error.Cancelled, live.hook().nextChunk(0, 1));
    stop.store(false, .release);
    try std.testing.expectEqual(@as(u32, 1), try live.hook().nextChunk(0, 1));
    const r = contract.Attention{ .consumer_layer = 0, .owner_layer = 0, .query_start = 0, .query_rows = 1, .query_heads = 4, .kv_heads = 2, .head_dim = 256, .sliding_window = 16, .skip_kv_write = false, .scale = 1 };
    try live.hook().attention(r, device(q.ref()), device(k.ref()), device(v.ref()), device(out.ref()));
    try std.testing.expectEqual(@as(u64, 1), live.split_decode_dispatches);
    try std.testing.expectEqual(optimized_sizes.physical_bytes, live.metrics.live_bytes);
    try std.testing.expectEqual(optimized_sizes.physical_bytes, live.metrics.peak_bytes);
    try std.testing.expectError(error.InvalidReferenceReplay, live.beginReplay(32, 1024 * 1024 * 1024));
    stop.store(true, .release);
    try std.testing.expectError(error.Cancelled, live.hook().nextChunk(0, 1));
    stop.store(false, .release);
    try std.testing.expectError(error.ContextSessionFailed, live.hook().nextChunk(0, 1));
    try std.testing.expectEqual(@as(u32, 0), live.logical_tokens);
}

test "context coordinator admission ownership rollback publication and final metrics" {
    const coordinator = @import("compaction_coordinator.zig");
    const a = std.testing.allocator;
    const config = try fixtureConfig();
    const ids: [1025]i64 = @splat(7);
    var labels: [1025]u32 = @splat(0);
    @memset(labels[1..513], 1);
    var blocks = [_]@import("../runtime/kv/reference_replay.zig").Range{.{ .start = 1, .end = 513 }};
    const eligible = payload.Eligible{ .allocator = a, .labels = &labels, .ranges = &blocks, .blocks = &blocks };
    const identity: coordinator.profiles.Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    const profile = coordinator.profiles.Profile{ .identity = identity, .owners = &.{ .{ .layer = 1, .retained = &.{ 256, 256 } }, .{ .layer = 2, .retained = &.{ 256, 256 } } }, .max_prompt_tokens = 2048, .max_output_tokens = 8, .minimum_saved_bytes = 16384 };
    const Probe = struct {
        fn run(alloc: std.mem.Allocator, c: Config, tokens: []const i64, e: *const payload.Eligible, p: coordinator.profiles.Profile) !void {
            var req = try coordinator.prepare(alloc, c, tokens, e, &.{1}, p, p.identity, 1028, 1024 * 1024 * 1024);
            defer req.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ config, &ids, &eligible, profile });
    var request = try coordinator.prepare(a, config, &ids, &eligible, &.{1}, profile, identity, 1028, 1024 * 1024 * 1024);
    defer request.deinit();
    var optimized_config = config;
    optimized_config.optimize_decode = true;
    const optimized_sizes = try allocation(optimized_config);
    optimized_config.physical_budget_bytes = optimized_sizes.physical_bytes;
    var optimized_request = try coordinator.prepare(a, optimized_config, &ids, &eligible, &.{1}, profile, identity, 1028, 1024 * 1024 * 1024);
    defer optimized_request.deinit();
    try std.testing.expectEqual(request.backend_peak_bytes + optimized_sizes.decode_scratch_bytes, optimized_request.backend_peak_bytes);
    try std.testing.expectEqual(request.host_peak_bytes, optimized_request.host_peak_bytes);
    try std.testing.expectEqual(request.execution_peak_bytes + optimized_sizes.decode_scratch_bytes, optimized_request.execution_peak_bytes);
    try std.testing.expectError(error.ContextStagingBudgetExceeded, coordinator.prepare(a, config, &ids, &eligible, &.{1}, profile, identity, 1028, request.peak_bytes - 1));
    // Request owns the exact prepared eligibility snapshot.
    labels[32] = 0;
    try std.testing.expectEqual(@as(u32, 1), request.eligible.labels[32]);
    try std.testing.expectError(error.InvalidPayloadProvenance, coordinator.prepare(a, config, &ids, &eligible, &.{1}, profile, identity, 1028, request.peak_bytes));
    labels[32] = 1;
    if (comptime !@import("build_options").enable_metal) return;
    var live = try Session.init(a, config);
    defer live.deinit();
    var q = try Buffer.init(live.copier.core.runtime, 64 * 4 * 512 * 4);
    defer q.deinit();
    var k = try Buffer.init(live.copier.core.runtime, 64 * 2 * 512 * 4);
    defer k.deinit();
    var v = try Buffer.init(live.copier.core.runtime, k.logical_bytes);
    defer v.deinit();
    var out = try Buffer.init(live.copier.core.runtime, q.logical_bytes);
    defer out.deinit();
    @memset(q.bytes, 0);
    @memset(k.bytes, 0);
    @memset(v.floats(), 1);
    while (live.logical_tokens < config.plan.prompt_tokens) {
        const rows = try live.hook().nextChunk(live.logical_tokens, @intCast(config.plan.prompt_tokens - live.logical_tokens));
        try fixtureStep(live.hook(), config.layout, live.logical_tokens, rows, q, k, v, out);
    }
    const Runner = struct {
        layout: policy.Layout,
        q: Buffer,
        k: Buffer,
        v: Buffer,
        out: Buffer,
        fail: bool,
        fn step(ctx: *anyopaque, hook: contract.Hook, position: usize, tokens: []const i64) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try fixtureStep(hook, self.layout, @intCast(position), @intCast(tokens.len), self.q, self.k, self.v, self.out);
            if (self.fail) return error.Cancelled;
        }
    };
    var runner = Runner{ .layout = config.layout, .q = q, .k = k, .v = v, .out = out, .fail = true };
    try std.testing.expectError(error.Cancelled, request.run(&live, &runner, Runner.step, true));
    try std.testing.expect(!live.frozen and live.logical_tokens == 1025 and live.publication.epoch == 1);
    for (live.globals) |*g| try std.testing.expect(!g.hasReplayReaders());
    const aborted_status = live.metrics.replay_status_readback_bytes;
    try std.testing.expect(aborted_status > 0);
    try std.testing.expectError(error.InvalidContextTransaction, request.run(&live, &runner, Runner.step, true));
    var retry = try coordinator.prepare(a, config, &ids, &eligible, &.{1}, profile, identity, 1028, request.peak_bytes);
    defer retry.deinit();
    runner.fail = false;
    try retry.run(&live, &runner, Runner.step, true);
    try std.testing.expect(!live.frozen and live.publication.epoch == 2 and live.metrics.compacted_blocks == 4);
    try std.testing.expect(live.metrics.live_bytes < config.physical_budget_bytes and live.metrics.peak_bytes <= retry.peak_bytes);
    try std.testing.expect(live.metrics.replay_status_readback_bytes > aborted_status);
    try std.testing.expectEqual(@as(u32, 769), live.metrics.retained_rows);
    try std.testing.expectEqual(@as(usize, 0), live.metrics.query_bytes);
    // No generated token is needed to make publication metrics accurate.
    try fixtureStep(live.hook(), config.layout, 1025, 1, q, k, v, out);
}

const CanonicalFingerprintTestResult = struct {
    descriptor: [32]u8,
    payload: [32]u8,
    readback: u64,
};

fn canonicalFingerprintTestFixture(
    scale: f32,
    softcap: f32,
    keys: []const u8,
    values: []const u8,
    mass: []const u8,
) !CanonicalFingerprintTestResult {
    var layout = policy.Layout{ .len = 2 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 1, .query_heads = 2, .head_dim = 2, .rope_active_dim = 2, .rope_frequency_dim = 2, .rope_theta = 10_000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 1, .query_heads = 2, .head_dim = 2, .rope_active_dim = 2, .rope_frequency_dim = 2, .rope_theta = 10_000 };
    return canonicalFingerprintTestFixtureWithLayout(layout, scale, softcap, keys, values, mass);
}

fn canonicalFingerprintTestFixtureWithLayout(
    layout: policy.Layout,
    scale: f32,
    softcap: f32,
    keys: []const u8,
    values: []const u8,
    mass: []const u8,
) !CanonicalFingerprintTestResult {
    const heads = [_]views.Head{.{
        .kv_start = 0,
        .capacity_rows = 4,
        .bias_start = 0,
        .bias_rows = 2,
        .span_start = 0,
        .span_count = 2,
    }};
    const spans = [_]views.Span{
        .{ .physical_start = 0, .row_count = 2, .logical_start = 0, .logical_count = 512, .kind = .compacted, .bias_start = 0 },
        .{ .physical_start = 2, .row_count = 1, .logical_start = 512, .logical_count = 1, .kind = .exact },
    };
    const view = views.View{
        .version = views.abi_version,
        .epoch = 7,
        .logical_tokens = 513,
        .capacity_rows = 4,
        .bias_rows = 2,
        .kv_heads = 1,
        .head_dim = 2,
        .heads = &heads,
        .spans = &spans,
    };
    var descriptor = std.crypto.hash.sha2.Sha256.init(.{});
    descriptor.update("antfly.packed-context.descriptor.v2\x00");
    fingerprintU64(&descriptor, 7);
    fingerprintU32(&descriptor, 513);
    fingerprintU32(&descriptor, 1);
    fingerprintU32(&descriptor, 0);
    try fingerprintLayout(&descriptor, layout);
    var payload_hash = std.crypto.hash.sha2.Sha256.init(.{});
    payload_hash.update("antfly.packed-context.payload.v1\x00");
    const readback = try fingerprintOwner(&descriptor, &payload_hash, .{
        .layer = 0,
        .scale = scale,
        .softcap = softcap,
        .view = view,
        .keys = keys,
        .values = values,
        .mass = mass,
    });
    var descriptor_digest: [32]u8 = undefined;
    descriptor.final(&descriptor_digest);
    payload_hash.update(&descriptor_digest);
    var payload_digest: [32]u8 = undefined;
    payload_hash.final(&payload_digest);
    return .{ .descriptor = descriptor_digest, .payload = payload_digest, .readback = readback };
}

test "context qualification fingerprint binds shared consumer layout" {
    const keys = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 0, 0, 0 };
    const values = [_]u8{ 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 0, 0, 0, 0 };
    const mass = [_]u8{ 0, 0, 0, 63, 0, 0, 128, 63 };
    var layout = policy.Layout{ .len = 2 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 1, .query_heads = 2, .head_dim = 2, .rope_active_dim = 2, .rope_frequency_dim = 2, .rope_theta = 10_000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 1, .query_heads = 2, .head_dim = 2, .rope_active_dim = 2, .rope_frequency_dim = 2, .rope_theta = 10_000 };
    const base = try canonicalFingerprintTestFixtureWithLayout(layout, 0.5, 30, &keys, &values, &mass);

    var changed_query_geometry = layout;
    changed_query_geometry.layers[1].query_heads = 4;
    const query_changed = try canonicalFingerprintTestFixtureWithLayout(changed_query_geometry, 0.5, 30, &keys, &values, &mass);
    try std.testing.expect(!std.mem.eql(u8, &base.descriptor, &query_changed.descriptor));
    try std.testing.expect(!std.mem.eql(u8, &base.payload, &query_changed.payload));

    var changed_rope = layout;
    changed_rope.layers[1].rope_theta = 1_000_000;
    const rope_changed = try canonicalFingerprintTestFixtureWithLayout(changed_rope, 0.5, 30, &keys, &values, &mass);
    try std.testing.expect(!std.mem.eql(u8, &base.descriptor, &rope_changed.descriptor));
}

test "context qualification fingerprint binds attention scale softcap and compacted mass" {
    const keys = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 0, 0, 0 };
    const values = [_]u8{ 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 0, 0, 0, 0 };
    const mass = [_]u8{ 0, 0, 0, 63, 0, 0, 128, 63 };
    const base = try canonicalFingerprintTestFixture(0.5, 30, &keys, &values, &mass);
    const changed_scale = try canonicalFingerprintTestFixture(0.25, 30, &keys, &values, &mass);
    const changed_softcap = try canonicalFingerprintTestFixture(0.5, 50, &keys, &values, &mass);
    var changed_mass_bytes = mass;
    changed_mass_bytes[0] ^= 1;
    const changed_mass = try canonicalFingerprintTestFixture(0.5, 30, &keys, &values, &changed_mass_bytes);
    try std.testing.expect(!std.mem.eql(u8, &base.descriptor, &changed_scale.descriptor));
    try std.testing.expect(!std.mem.eql(u8, &base.descriptor, &changed_softcap.descriptor));
    try std.testing.expectEqualSlices(u8, &base.descriptor, &changed_mass.descriptor);
    try std.testing.expect(!std.mem.eql(u8, &base.payload, &changed_mass.payload));
    try std.testing.expectEqual(@as(u64, 32), base.readback);
}

test "context qualification fingerprint excludes allocation identity and reserved continuation" {
    const keys = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 0, 0, 0 };
    const values = [_]u8{ 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 0, 0, 0, 0 };
    const mass = [_]u8{ 0, 0, 0, 63, 0, 0, 128, 63 };
    var second_keys = keys;
    var second_values = values;
    const first = try canonicalFingerprintTestFixture(0.5, 30, &keys, &values, &mass);
    const distinct_allocations = try canonicalFingerprintTestFixture(0.5, 30, &second_keys, &second_values, &mass);
    try std.testing.expectEqualSlices(u8, &first.descriptor, &distinct_allocations.descriptor);
    try std.testing.expectEqualSlices(u8, &first.payload, &distinct_allocations.payload);

    second_keys[12] = 0xa5;
    second_values[15] = 0x5a;
    const poisoned_reservation = try canonicalFingerprintTestFixture(0.5, 30, &second_keys, &second_values, &mass);
    try std.testing.expectEqualSlices(u8, &first.descriptor, &poisoned_reservation.descriptor);
    try std.testing.expectEqualSlices(u8, &first.payload, &poisoned_reservation.payload);

    const first_receipt = teacher_forced.QualityFingerprint{
        .context_instance_id = 1,
        .publication_epoch = 7,
        .logical_tokens = 513,
        .payload_sha256 = first.payload,
        .descriptor_sha256 = first.descriptor,
        .diagnostic_kv_readback_bytes = first.readback,
    };
    var sibling_receipt = first_receipt;
    sibling_receipt.context_instance_id = 2;
    try std.testing.expect(first_receipt.context_instance_id != sibling_receipt.context_instance_id);
    try std.testing.expectEqualSlices(u8, &first_receipt.descriptor_sha256, &sibling_receipt.descriptor_sha256);
    try std.testing.expectEqualSlices(u8, &first_receipt.payload_sha256, &sibling_receipt.payload_sha256);
}

fn storedPublicationFixtureSources() [query_protocol.max_sources]query_protocol.Source {
    return .{
        .{ .kind = .request_question_replay, .partition = .training, .start = 100, .end = 120, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 100, .end = 120, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 120, .end = 140, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 100, .end = 120, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 120, .end = 140, .active = false },
    };
}

fn storedPublicationFixtureBranchSources(base: [query_protocol.max_sources]query_protocol.Source, branch: query_campaign.Branch) [query_protocol.max_sources]query_protocol.Source {
    var result = base;
    for (&result) |*source| source.active = false;
    switch (branch) {
        .question => result[0].active = true,
        .fit => {
            result[1].active = true;
            result[2].active = true;
        },
        .gate => {
            result[3].active = true;
            result[4].active = true;
        },
    }
    return result;
}

fn storedPublicationFixturePositions(allocator: std.mem.Allocator, plan: query_protocol.Plan, heads: u32) ![]u32 {
    const rows = try std.math.add(u32, plan.training_rows, plan.validation_rows);
    const positions = try allocator.alloc(u32, try std.math.mul(u32, heads, rows));
    errdefer allocator.free(positions);
    @memset(positions, std.math.maxInt(u32));
    for (0..heads) |head| for (0..plan.strata) |stratum| for (100..140) |position| {
        const selected = (try plan.select(@intCast(position), @intCast(stratum))) orelse continue;
        const index = @as(u32, @intCast(head)) * rows + selected.row;
        if (positions[index] != std.math.maxInt(u32)) return error.InvalidQueryCampaignSelection;
        positions[index] = @intCast(position);
    };
    for (positions) |position| if (position == std.math.maxInt(u32)) return error.IncompleteQueryCampaign;
    return positions;
}

fn absorbStoredPublicationFixtureBranch(live: *Session, campaign: *query_campaign.Campaign, branch: query_campaign.Branch, sources: [query_protocol.max_sources]query_protocol.Source, session_identity: u64) !void {
    const active = storedPublicationFixtureBranchSources(sources, branch);
    const plan = try query_protocol.Plan.init(&active, 4);
    const rows = try std.math.add(u32, plan.training_rows, plan.validation_rows);
    var queries: [2]Buffer = undefined;
    var positions: [2][]u32 = undefined;
    var parts: [2]query_campaign.DevicePartition = undefined;
    var query_count: usize = 0;
    var position_count: usize = 0;
    defer {
        for (queries[0..query_count]) |*buffer| buffer.deinit();
        for (positions[0..position_count]) |value| live.allocator.free(value);
    }
    inline for (.{ @as(u32, 1), @as(u32, 2) }, 0..) |layer, index| {
        const shape = live.config.layout.layers[layer];
        queries[index] = try Buffer.initTracked(live.copier.core.runtime, try std.math.mul(usize, @as(usize, try std.math.mul(u32, shape.kv_heads, rows)), try std.math.mul(usize, shape.head_dim, @sizeOf(f32))), live.allocation_tracker);
        query_count += 1;
        positions[index] = try storedPublicationFixturePositions(live.allocator, plan, shape.kv_heads);
        position_count += 1;
        @memset(queries[index].bytes, 0);
        parts[index] = .{
            .campaign_identity = campaign.campaign_identity,
            .load_generation = campaign.load_generation,
            .branch = branch,
            .session_identity = session_identity,
            .layer = layer,
            .partition = if (branch == .gate) .validation else .training,
            .rows_per_head = rows,
            .heads = shape.kv_heads,
            .dim = shape.head_dim,
            .stride_rows = rows,
            .row_offset = 0,
            .queries = queries[index].ref(),
            .positions = positions[index],
        };
    }
    try campaign.absorb(branch, &parts);
}

fn initStoredPublicationFixtureCampaign(live: *Session, identity: u64, load_generation: u64) !query_campaign.Campaign {
    const sources = storedPublicationFixtureSources();
    const size = try query_campaign.Campaign.allocation(live.config.layout, &sources);
    var campaign = try query_campaign.Campaign.init(live.allocator, live.config.execution_lease.?, live.allocation_tracker, live.config.layout, &sources, size.physical_bytes, identity, load_generation);
    errdefer campaign.deinit();
    try absorbStoredPublicationFixtureBranch(live, &campaign, .question, sources, identity * 10 + 1);
    try absorbStoredPublicationFixtureBranch(live, &campaign, .fit, sources, identity * 10 + 2);
    try absorbStoredPublicationFixtureBranch(live, &campaign, .gate, sources, identity * 10 + 3);
    try campaign.seal();
    return campaign;
}

fn storedPublicationFixtureArtifacts(live: *Session, identity: StoredCampaignIdentity, biases: *[4][480]f32) ![4]StoredArtifact {
    var result: [4]StoredArtifact = undefined;
    var count: usize = 0;
    for (live.globals, 0..) |*generation, owner_index| {
        const dim = generation.plan.descriptor.head_dim;
        for (generation.plan.heads, 0..) |head, head_index| {
            if (count >= result.len) return error.InvalidContextLayout;
            @memset(biases[count][0..], @floatCast(@log(@as(f64, 512.0 / 480.0))));
            const row_start = try std.math.add(u32, head.kv_start, 1);
            const offset = try std.math.mul(usize, @as(usize, row_start), try std.math.mul(usize, dim, @sizeOf(f16)));
            const bytes = try std.math.mul(usize, 480, try std.math.mul(usize, dim, @sizeOf(f16)));
            result[count] = .{
                .owner_layer = live.global_layers[owner_index],
                .head = @intCast(head_index),
                .span = 1,
                .logical_start = 1,
                .logical_count = 512,
                .retained_rows = 480,
                .head_dim = dim,
                .keys_f16 = generation.keys.bytes[offset..][0..bytes],
                .values_f16 = generation.values.bytes[offset..][0..bytes],
                .bias_f32 = std.mem.sliceAsBytes(biases[count][0..]),
                .sha256 = undefined,
            };
            result[count].sha256 = try storedArtifactDiagnosticDigest(identity, result[count]);
            count += 1;
        }
    }
    if (count != result.len) return error.InvalidContextLayout;
    return result;
}

fn beginStoredPublicationFixtureMaintenance(live: *Session, identity: StoredCampaignIdentity, q: Buffer, k: Buffer, v: Buffer, out: Buffer) !Maintenance {
    var maintenance = try live.beginStoredReplayDiagnostic(32, 1024 * 1024 * 1024, identity);
    errdefer maintenance.deinit();
    try fixtureStep(maintenance.branch.hook(), live.config.layout, live.logical_tokens, 32, q, k, v, out);
    try maintenance.seal();
    return maintenance;
}

test "context Metal stored campaign rejects a middle owner atomically then commits all owners and releases accounting" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const generation: u64 = 7;
    var pool = try execution_pool.Pool.init(generation, execution_pool.admitted_backend_bytes);
    defer pool.deinit();
    try pool.warm(null);
    var lease = try pool.acquire(generation);
    defer if (lease.pool != null) lease.poisonAndRelease();
    var tracker = owner_alloc.HostAllocationTracker.init(null, 0);
    const owner_baseline = tracker.snapshot();
    var stop = std.atomic.Value(bool).init(false);
    var config = try fixtureConfig();
    config.cancelled = &stop;
    config.execution_lease = &lease;
    config.owner_allocation_tracker = &tracker;
    var live = try Session.init(a, config);
    var live_active = true;
    defer if (live_active) live.deinit();

    var q = try Buffer.init(live.copier.core.runtime, 64 * 4 * 512 * 4);
    defer q.deinit();
    var k = try Buffer.init(live.copier.core.runtime, 64 * 2 * 512 * 4);
    defer k.deinit();
    var v = try Buffer.init(live.copier.core.runtime, k.logical_bytes);
    defer v.deinit();
    var out = try Buffer.init(live.copier.core.runtime, q.logical_bytes);
    defer out.deinit();
    @memset(q.bytes, 0);
    @memset(k.bytes, 0);
    @memset(v.floats(), 1);
    while (live.logical_tokens < config.plan.prompt_tokens) {
        const rows = try live.hook().nextChunk(live.logical_tokens, @intCast(config.plan.prompt_tokens - live.logical_tokens));
        try fixtureStep(live.hook(), config.layout, live.logical_tokens, rows, q, k, v, out);
    }
    _ = try live.ensureReplayControls(1024 * 1024 * 1024);

    var labels: [1025]u32 = @splat(0);
    @memset(labels[1..513], 1);
    var eligible_blocks = [_]@import("../runtime/kv/reference_replay.zig").Range{.{ .start = 1, .end = 513 }};
    const eligible = payload.Eligible{ .allocator = a, .labels = &labels, .ranges = &eligible_blocks, .blocks = &eligible_blocks };
    const segments = [_]storage.Segment{ .{ .logical_count = 1, .retained = &.{ 1, 1 } }, .{ .logical_count = 512, .retained = &.{ 480, 480 } }, .{ .logical_count = 512, .retained = &.{ 512, 512 } } };
    const all = [_][]const storage.Segment{ &segments, &segments };

    // Mutation after identity binding is rejected before validation dispatch.
    {
        var campaign = try initStoredPublicationFixtureCampaign(&live, 41, generation);
        var campaign_active = true;
        var maintenance: Maintenance = undefined;
        var maintenance_active = false;
        var loader: StoredLoader = undefined;
        var loader_active = false;
        defer if (maintenance_active) maintenance.deinit();
        defer if (loader_active and !loader.released) {
            _ = loader.deinitChecked() catch {};
        };
        defer if (campaign_active) campaign.deinit();
        const live_fingerprint = try live.qualificationFingerprint();
        const campaign_payload = try campaign.storedDiagnosticPayloadDigest();
        const campaign_content = try campaign.storedDiagnosticContentDigest();
        const identity = StoredCampaignIdentity{
            .load_generation = generation,
            .session_identity = live.identity,
            .publication_epoch = live.publication.epoch,
            .query_campaign_identity = campaign.campaign_identity,
            .query_campaign_sha256 = try campaign.storedDiagnosticFingerprint(),
            .query_payload_sha256 = campaign_payload.sha256,
            .query_content_sha256 = campaign_content.sha256,
            .query_payload_readback_bytes = campaign_payload.readback_bytes,
            .source_descriptor_sha256 = live_fingerprint.descriptor_sha256,
            .source_payload_sha256 = live_fingerprint.payload_sha256,
        };
        maintenance = try beginStoredPublicationFixtureMaintenance(&live, identity, q, k, v, out);
        maintenance_active = true;
        var biases: [4][480]f32 = undefined;
        const artifacts = try storedPublicationFixtureArtifacts(&live, identity, &biases);
        const loader_budget = try storedLoaderBudget(&artifacts);
        loader = try StoredLoader.init(&tracker, &lease, identity, &artifacts, loader_budget.physical_upper_bytes);
        loader_active = true;
        const old_handles = [_]*anyopaque{ live.globals[0].keys.handle, live.globals[1].keys.handle };
        try campaign.injectNonfiniteTrainingScalar(2, 0);
        try std.testing.expectError(error.InvalidPayloadProvenance, maintenance.publishStoredCampaignDiagnostic(&campaign, &loader, &eligible, &all, 1028, 1024 * 1024 * 1024, null));
        try std.testing.expectEqual(@as(u64, 1), live.publication.epoch);
        try std.testing.expect(!live.publication.pending);
        for (live.globals, old_handles) |owner, handle| try std.testing.expectEqual(handle, owner.keys.handle);
        campaign.deinit();
        campaign_active = false;
        _ = try loader.deinitChecked();
        loader_active = false;
        maintenance.deinit();
        maintenance_active = false;
    }

    // A query that was already nonfinite when its payload identity was bound
    // reaches genuine device validation. The second owner rejects only after
    // the first owner's heads validate, and the one Batch retains originals.
    {
        var campaign = try initStoredPublicationFixtureCampaign(&live, 42, generation);
        var campaign_active = true;
        var maintenance: Maintenance = undefined;
        var maintenance_active = false;
        var loader: StoredLoader = undefined;
        var loader_active = false;
        defer if (maintenance_active) maintenance.deinit();
        defer if (loader_active and !loader.released) {
            _ = loader.deinitChecked() catch {};
        };
        defer if (campaign_active) campaign.deinit();
        try campaign.injectNonfiniteTrainingScalar(2, 0);
        const live_fingerprint = try live.qualificationFingerprint();
        const campaign_payload = try campaign.storedDiagnosticPayloadDigest();
        const campaign_content = try campaign.storedDiagnosticContentDigest();
        const identity = StoredCampaignIdentity{
            .load_generation = generation,
            .session_identity = live.identity,
            .publication_epoch = live.publication.epoch,
            .query_campaign_identity = campaign.campaign_identity,
            .query_campaign_sha256 = try campaign.storedDiagnosticFingerprint(),
            .query_payload_sha256 = campaign_payload.sha256,
            .query_content_sha256 = campaign_content.sha256,
            .query_payload_readback_bytes = campaign_payload.readback_bytes,
            .source_descriptor_sha256 = live_fingerprint.descriptor_sha256,
            .source_payload_sha256 = live_fingerprint.payload_sha256,
        };
        maintenance = try beginStoredPublicationFixtureMaintenance(&live, identity, q, k, v, out);
        maintenance_active = true;
        var biases: [4][480]f32 = undefined;
        const artifacts = try storedPublicationFixtureArtifacts(&live, identity, &biases);
        const loader_budget = try storedLoaderBudget(&artifacts);
        loader = try StoredLoader.init(&tracker, &lease, identity, &artifacts, loader_budget.physical_upper_bytes);
        loader_active = true;
        const old_handles = [_]*anyopaque{ live.globals[0].keys.handle, live.globals[1].keys.handle };
        try std.testing.expectError(error.NonFiniteCompactionInput, maintenance.publishStoredCampaignDiagnostic(&campaign, &loader, &eligible, &all, 1028, 1024 * 1024 * 1024, null));
        try std.testing.expectEqual(@as(u64, 1), live.publication.epoch);
        try std.testing.expect(!live.publication.pending);
        for (live.globals, old_handles) |owner, handle| try std.testing.expectEqual(handle, owner.keys.handle);
        campaign.deinit();
        campaign_active = false;
        _ = try loader.deinitChecked();
        loader_active = false;
        maintenance.deinit();
        maintenance_active = false;
    }

    // A fresh campaign and loader validate the same exact stored bytes, then
    // one Batch replaces both two-head owners atomically.
    {
        var campaign = try initStoredPublicationFixtureCampaign(&live, 43, generation);
        var campaign_active = true;
        var maintenance: Maintenance = undefined;
        var maintenance_active = false;
        var loader: StoredLoader = undefined;
        var loader_active = false;
        defer if (maintenance_active) maintenance.deinit();
        defer if (loader_active and !loader.released) {
            _ = loader.deinitChecked() catch {};
        };
        defer if (campaign_active) campaign.deinit();
        const live_fingerprint = try live.qualificationFingerprint();
        const campaign_payload = try campaign.storedDiagnosticPayloadDigest();
        const campaign_content = try campaign.storedDiagnosticContentDigest();
        const identity = StoredCampaignIdentity{
            .load_generation = generation,
            .session_identity = live.identity,
            .publication_epoch = live.publication.epoch,
            .query_campaign_identity = campaign.campaign_identity,
            .query_campaign_sha256 = try campaign.storedDiagnosticFingerprint(),
            .query_payload_sha256 = campaign_payload.sha256,
            .query_content_sha256 = campaign_content.sha256,
            .query_payload_readback_bytes = campaign_payload.readback_bytes,
            .source_descriptor_sha256 = live_fingerprint.descriptor_sha256,
            .source_payload_sha256 = live_fingerprint.payload_sha256,
        };
        maintenance = try beginStoredPublicationFixtureMaintenance(&live, identity, q, k, v, out);
        maintenance_active = true;
        var biases: [4][480]f32 = undefined;
        const artifacts = try storedPublicationFixtureArtifacts(&live, identity, &biases);
        const loader_budget = try storedLoaderBudget(&artifacts);
        loader = try StoredLoader.init(&tracker, &lease, identity, &artifacts, loader_budget.physical_upper_bytes);
        loader_active = true;
        const old_handles = [_]*anyopaque{ live.globals[0].keys.handle, live.globals[1].keys.handle };
        const receipt = try maintenance.publishStoredCampaignDiagnostic(&campaign, &loader, &eligible, &all, 1028, 1024 * 1024 * 1024, null);
        try std.testing.expect(receipt.diagnostic_only);
        try std.testing.expect(!receipt.serving_authority);
        try std.testing.expect(!receipt.release_authority);
        try std.testing.expectEqual(@as(u32, 4), receipt.artifact_count);
        try std.testing.expectEqual(@as(u64, 3), receipt.publication_epoch_after);
        for (live.globals, old_handles) |owner, handle| try std.testing.expect(owner.keys.handle != handle);
        for (loader.entries) |*entry| {
            var owner_index: usize = 0;
            while (live.global_layers[owner_index] != entry.owner_layer) : (owner_index += 1) {}
            const owner = &live.globals[owner_index];
            const head = owner.plan.heads[entry.head];
            const span = owner.plan.spans[head.span_start + entry.span];
            try std.testing.expectEqual(@as(u32, 480), span.row_count);
            const offset = try std.math.mul(usize, @as(usize, head.kv_start + span.physical_start), try std.math.mul(usize, entry.head_dim, @sizeOf(f16)));
            const bytes = entry.keys.logical_bytes;
            try std.testing.expectEqualSlices(u8, entry.keys.bytes[0..bytes], owner.keys.bytes[offset..][0..bytes]);
            try std.testing.expectEqualSlices(u8, entry.values.bytes[0..bytes], owner.values.bytes[offset..][0..bytes]);
            const bias_offset = try std.math.mul(usize, @as(usize, head.bias_start + span.bias_start), @sizeOf(f32));
            try std.testing.expectEqualSlices(u8, entry.bias.bytes[0..entry.bias.logical_bytes], owner.mass.?.bytes[bias_offset..][0..entry.bias.logical_bytes]);
        }
        campaign.deinit();
        campaign_active = false;
        const after_loader = try loader.deinitChecked();
        loader_active = false;
        try std.testing.expect(after_loader.tracked_combined_live_bytes > owner_baseline.tracked_combined_live_bytes);
        maintenance.deinit();
        maintenance_active = false;
    }

    const release = live.deinitMeasured();
    live_active = false;
    try std.testing.expect(release.released);
    try expectStoredLoaderTrackerSnapshot(owner_baseline, tracker.snapshot());
    try lease.release();
}
