// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Synchronous request-owned context storage. GPU buffers never escape a read
//! lease; all owner generations are validated before a single publication.
//! This route deliberately declines fused graph replay until it is qualified.
const std = @import("std");
const policy_mod = @import("../runtime/context_policy.zig");
const views = @import("../runtime/kv/context_view.zig");
const contract = @import("../runtime/kv/context_session.zig");
const shared = @import("metal_generated_kernels.zig");
const attention_mod = @import("context_kernels.zig");
const fitter_mod = @import("context_compaction.zig");
const mr = @import("metal_runtime.zig");
const Buffer = shared.Buffer;
const Ref = shared.Ref;
const absent = std.math.maxInt(u32);
var next_identity = std.atomic.Value(u64).init(1);

pub const Config = struct {
    plan: policy_mod.Plan,
    layout: policy_mod.Layout,
    max_query_rows: u32,
    physical_budget_bytes: usize,
    cancelled: ?*const std.atomic.Value(bool) = null,
    execution_control: ?@import("../execution_control.zig").InferenceExecutionControl = null,
    /// Offline numerical capture only: ingest exact KV and collect queries
    /// after this position without fitting/publication. Production init rejects
    /// this field. It is not the serving branch or a capability override.
    calibration_replay_start: ?u32 = null,
};

const Geometry = struct {
    capacity: u32,
    retained_capacity: u32,
    prefix: u32,
    ring: u32,
    page_prefix: u32,
    page_count: u32,
    consumers: u32,
    train_per_stratum: u32,
    held_per_stratum: u32,
    training_rows: u32,
    validation_rows: u32,
    span_capacity: usize,
    paged: bool,
};
fn geometry(config: Config, index: usize) !Geometry {
    const l = config.layout.layers[index];
    const p = config.plan.policy;
    var consumers: u32 = 0;
    var strata: u32 = 0;
    for (config.layout.layers[0..config.layout.len]) |item| if (item.owner == index) {
        consumers += 1;
        if (item.kv_heads != l.kv_heads or item.query_heads == 0 or item.query_heads % item.kv_heads != 0)
            return error.InvalidContextLayout;
        strata = try std.math.add(u32, strata, item.query_heads / item.kv_heads);
    };
    const capture = p.mode == .attention_matching and l.sliding_window == 0;
    if (strata == 0 or (capture and strata > 128)) return error.InvalidContextLayout;
    // A recent interval contains at least this many whole quartets regardless
    // of prefix alignment. Keep every captured slot strictly after the source
    // block, including a quartet that straddles either interval boundary.
    const calibration = @import("../runtime/kv/attention_matching.zig");
    const quartets = (p.keep_recent_tokens -| 3) / 4;
    const exact_training = if (config.calibration_replay_start) |start|
        try QueryBounds.init(start, @intCast(config.plan.logical_token_limit), false)
    else
        null;
    const exact_validation = if (config.calibration_replay_start) |start|
        try QueryBounds.init(start, @intCast(config.plan.logical_token_limit), true)
    else
        null;
    // Production must reserve against alignment-independent whole quartets.
    // An offline replay interval is already fixed, so size it from the exact
    // disjoint populations used by capture and by model_replay_context.
    const train_population = if (exact_training) |bounds| bounds.last_ordinal - bounds.first_ordinal + 1 else quartets * 3;
    const held_population = if (exact_validation) |bounds| bounds.last_ordinal - bounds.first_ordinal + 1 else quartets;
    const train = if (capture) @min(calibration.max_training_rows / strata, train_population) else 0;
    const held = if (capture) @min(calibration.max_validation_rows / strata, held_population) else 0;
    if (capture and (train == 0 or held == 0)) return error.InvalidContextQueryWindow;
    const sliding = l.sliding_window > 0;
    const rolling = !sliding and p.mode == .rolling;
    const prefix = if (rolling) p.keep_prefix_tokens else 0;
    const window = if (sliding) l.sliding_window else if (rolling) p.max_kv_tokens.? - prefix else 0;
    const ring = if (window > 0) try std.math.add(u32, window, config.max_query_rows - 1) else 0;
    const retained_capacity = if (ring > 0) try std.math.add(u32, prefix, ring) else std.math.cast(u32, config.plan.retained_row_limit) orelse return error.ContextKvBudgetExceeded;
    const page_prefix = if (capture) @min(p.keep_prefix_tokens, retained_capacity) else 0;
    // Exact tail pages and compacted blocks occupy 256-row slots in one arena.
    // One additional slot holds an unpublished candidate. Only the last exact
    // page can be partial; its slack never increases the retained-row budget.
    const page_count = if (capture) try std.math.add(u32, std.math.divCeil(u32, retained_capacity - page_prefix, 256) catch unreachable, 1) else 0;
    const capacity = if (capture) try std.math.add(u32, page_prefix, try std.math.mul(u32, page_count, 256)) else retained_capacity;
    const span_capacity: usize = if (capture) @as(usize, page_count) + 1 else 3;
    if (span_capacity > views.max_spans) return error.InvalidContextLayout;
    return .{ .capacity = capacity, .retained_capacity = retained_capacity, .prefix = prefix, .ring = ring, .page_prefix = page_prefix, .page_count = page_count, .consumers = consumers, .train_per_stratum = train, .held_per_stratum = held, .training_rows = train * strata, .validation_rows = held * strata, .span_capacity = span_capacity, .paged = capture };
}
fn rounded(n: usize) !usize {
    const page = std.heap.pageSize();
    return (try std.math.add(usize, @max(n, 1), page - 1)) & ~(page - 1);
}
fn sum(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b);
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}

const queryPartition = @import("context_query_sampling.zig").partition;
const QueryBounds = @import("context_query_sampling.zig").Bounds;

pub const Allocation = struct {
    live_bytes: usize = 0,
    staging_bytes: usize = 0,
    query_bytes: usize = 0,
    workspace_bytes: usize = 0,
    metadata_bytes: usize = 0,
    physical_bytes: usize = 0,
};
/// Includes arena/page slack, one unpublished block per owner, bounded query
/// pools, descriptor generations, and one shared source-gather/fitter workspace.
pub fn allocation(config: Config) !Allocation {
    try config.plan.policy.validate();
    // The legacy online session has no selected-payload lifecycle. Never let
    // a future capability toggle route explicit targets into whole-history fits.
    if (config.plan.policy.compaction_targets.count != 0) return error.UnsupportedContextCombination;
    if (config.max_query_rows == 0 or config.layout.len == 0 or config.layout.len > 256 or
        config.plan.logical_token_limit == 0 or config.plan.logical_token_limit > std.math.maxInt(u32) or
        config.plan.retained_row_limit == 0) return error.InvalidContextLayout;
    if (config.plan.logical_token_limit > config.plan.context_limit or config.plan.prompt_tokens == 0 or
        config.plan.prompt_tokens >= config.plan.logical_token_limit) return error.InvalidContextLayout;
    const expected = try config.plan.policy.admit(config.plan.context_limit, null, config.plan.prompt_tokens, config.plan.logical_token_limit - config.plan.prompt_tokens, 0);
    if (expected.retained_row_limit != config.plan.retained_row_limit) return error.InvalidContextLayout;
    if (config.calibration_replay_start) |start| {
        if (!@import("builtin").is_test or config.plan.policy.mode != .attention_matching or
            start != config.plan.prompt_tokens or start < @as(u64, config.plan.policy.keep_prefix_tokens) + 512 or
            start >= config.plan.logical_token_limit or config.plan.logical_token_limit - start > @import("../runtime/kv/reference_replay.zig").max_additional_tokens or
            config.plan.retained_row_limit != config.plan.logical_token_limit) return error.InvalidReferenceReplay;
    }
    var a = Allocation{};
    var owners: usize = 0;
    var descriptor_bytes: usize = 0;
    var source_bytes: usize = 0;
    for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
        if (l.owner > i or l.kv_heads == 0 or l.query_heads == 0 or l.query_heads % l.kv_heads != 0 or
            l.head_dim == 0 or l.head_dim > 512 or l.head_dim % 2 != 0) return error.InvalidContextLayout;
        const owner = config.layout.layers[l.owner];
        if (l.head_dim != owner.head_dim or l.kv_heads != owner.kv_heads or
            l.sliding_window != owner.sliding_window) return error.InvalidContextLayout;
        if (l.owner != i) continue;
        const g = try geometry(config, i);
        if (config.calibration_replay_start) |start| if (g.paged) {
            const training = try QueryBounds.init(start, @intCast(config.plan.logical_token_limit), false);
            const validation = try QueryBounds.init(start, @intCast(config.plan.logical_token_limit), true);
            _ = try training.slot(training.first_ordinal, g.train_per_stratum);
            _ = try validation.slot(validation.first_ordinal, g.held_per_stratum);
        };
        owners += 1;
        a.metadata_bytes = try sum(a.metadata_bytes, try mul(try rounded(try mul(g.span_capacity, @sizeOf(views.Span))), if (g.paged) 2 else 1));
        const elements = try mul(try mul(g.capacity, l.kv_heads), l.head_dim);
        if (elements > std.math.maxInt(u32)) return error.ContextKvBudgetExceeded;
        const kv = try mul(try rounded(try mul(elements, 2)), 2);
        const bias = if (g.paged) try rounded(try mul(try mul(g.capacity, l.kv_heads), 4)) else 0;
        const generation_bytes = try sum(kv, bias);
        if (g.paged) {
            const live_rows = g.capacity - 256;
            const live_bytes = try sum(try mul(try rounded(try mul(try mul(live_rows, l.kv_heads), l.head_dim * 2)), 2), try rounded(try mul(try mul(live_rows, l.kv_heads), 4)));
            a.live_bytes = try sum(a.live_bytes, live_bytes);
            a.staging_bytes = try sum(a.staging_bytes, generation_bytes - live_bytes);
            source_bytes = @max(source_bytes, try rounded(try mul(512, try mul(l.kv_heads, l.head_dim * 2))));
            const vectors = try mul(g.training_rows + g.validation_rows, l.kv_heads);
            if (try mul(vectors, l.head_dim) > std.math.maxInt(u32)) return error.InvalidContextLayout;
            a.metadata_bytes = try sum(a.metadata_bytes, try rounded(try mul(vectors, @sizeOf(u32))));
            const queries = try sum(try rounded(try mul(try mul(vectors, l.head_dim), 4)), try rounded(try mul(vectors, 4)));
            a.query_bytes = try sum(a.query_bytes, queries);
            const fit = try fitter_mod.allocation(.{ .source_rows = 512, .head_dim = l.head_dim, .kv_heads = l.kv_heads, .kv_head = 0, .training_rows = g.training_rows, .validation_rows = g.validation_rows, .scale = 1, .staging_budget_bytes = 0 });
            a.workspace_bytes = @max(a.workspace_bytes, fit.physical_bytes);
        } else a.live_bytes = try sum(a.live_bytes, generation_bytes);
        descriptor_bytes = @max(descriptor_bytes, try rounded(try mul(g.span_capacity, @sizeOf(views.Span))));
    }
    // Three kernel runtimes own dummy buffers; attention owns a status page. Runtime
    // pipeline/driver allocations belong to backend admission, not KV bytes.
    a.workspace_bytes = try sum(a.workspace_bytes, try sum(try mul(source_bytes, 2), try sum(descriptor_bytes, 4 * std.heap.pageSize())));
    a.metadata_bytes = try sum(a.metadata_bytes, try rounded(try mul(owners, @sizeOf(Owner))));
    a.physical_bytes = try sum(a.metadata_bytes, try sum(try sum(a.live_bytes, a.staging_bytes), try sum(a.query_bytes, a.workspace_bytes)));
    return a;
}

const Generation = struct {
    keys: Buffer,
    values: Buffer,
    mass: ?Buffer,
    spans: []views.Span,
    len: usize = 0,
    rows: u32 = 0,
    fn init(allocator: std.mem.Allocator, runtime: *mr.RawMetalDecodeRuntime, g: Geometry, l: policy_mod.LayerLayout) !Generation {
        const bytes = @as(usize, g.capacity) * l.kv_heads * l.head_dim * 2;
        var keys = try Buffer.init(runtime, bytes);
        errdefer keys.deinit();
        var values = try Buffer.init(runtime, bytes);
        errdefer values.deinit();
        var mass: ?Buffer = if (g.paged) try Buffer.init(runtime, @as(usize, g.capacity) * l.kv_heads * 4) else null;
        errdefer if (mass) |*b| b.deinit();
        if (mass) |b| @memset(b.bytes[0..b.logical_bytes], 0);
        const spans = try allocator.alloc(views.Span, g.span_capacity);
        return .{ .keys = keys, .values = values, .mass = mass, .spans = spans };
    }
    fn deinit(self: *Generation, allocator: std.mem.Allocator) void {
        self.keys.deinit();
        self.values.deinit();
        if (self.mass) |*b| b.deinit();
        allocator.free(self.spans);
    }
};
const Descriptors = struct {
    spans: []views.Span,
    len: usize = 0,
    rows: u32 = 0,
};
const Owner = struct {
    layer: u32,
    shape: policy_mod.LayerLayout,
    geometry: Geometry,
    live: Generation,
    staged: ?Descriptors = null,
    queries: ?Buffer = null,
    mapping: ?Buffer = null,
    positions: []u32 = &.{},
    scale: ?f32 = null,
    fn deinit(self: *Owner, allocator: std.mem.Allocator) void {
        self.live.deinit(allocator);
        if (self.staged) |gen| allocator.free(gen.spans);
        if (self.queries) |*b| b.deinit();
        if (self.mapping) |*b| b.deinit();
        allocator.free(self.positions);
    }
};

pub const Session = struct {
    allocator: std.mem.Allocator,
    config: Config,
    identity: u64,
    copies: shared.Kernels,
    attention: attention_mod.Kernels,
    fitter: fitter_mod.Kernels,
    source_keys: ?Buffer = null,
    source_values: ?Buffer = null,
    owners: std.ArrayList(Owner) = .empty,
    owner_indices: [256]u16 = undefined,
    consumer_stratum_offsets: [256]u32 = undefined,
    publication: views.Publication = .{},
    metrics: contract.Metrics,
    mutex: std.atomic.Mutex = .unlocked,
    next_layer: u32 = 0,
    step_start: u32 = 0,
    step_end: u32 = 0,
    logical_tokens: u32 = 0,
    compacted_blocks: u32 = 0,
    retain_exact_remainder: bool = false,
    failed: bool = false,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Session {
        const sizes = try allocation(config);
        if (sizes.physical_bytes > config.physical_budget_bytes) return error.ContextStagingBudgetExceeded;
        if (config.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (config.execution_control) |control| try control.check();
        var protection = if (config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        // Never wrap identities: delayed calls cannot bind a newly reused slot.
        var identity = next_identity.load(.monotonic);
        while (true) {
            if (identity == std.math.maxInt(u64)) return error.ContextEpochOverflow;
            if (next_identity.cmpxchgWeak(identity, identity + 1, .monotonic, .monotonic)) |current| {
                identity = current;
            } else break;
        }
        var copies = try shared.Kernels.initSource(@embedFile("context_storage.metal"), "context_storage");
        errdefer copies.deinit();
        var attn = try attention_mod.Kernels.init();
        errdefer attn.deinit();
        var fitter = try fitter_mod.Kernels.init();
        errdefer fitter.deinit();
        var self = Session{ .allocator = allocator, .config = config, .identity = identity, .copies = copies, .attention = attn, .fitter = fitter, .metrics = .{ .live_bytes = sizes.live_bytes, .staging_bytes = sizes.staging_bytes, .query_bytes = sizes.query_bytes, .peak_bytes = sizes.physical_bytes } };
        // GPU work is synchronous; staged buffers have no outstanding users.
        errdefer {
            for (self.owners.items) |*o| o.deinit(allocator);
            self.owners.deinit(allocator);
            if (self.source_keys) |*b| b.deinit();
            if (self.source_values) |*b| b.deinit();
        }
        var owner_count: usize = 0;
        var source_bytes: usize = 0;
        for (config.layout.layers[0..config.layout.len], 0..) |l, i| if (l.owner == i) {
            owner_count += 1;
            if ((try geometry(config, i)).paged) source_bytes = @max(source_bytes, @as(usize, 512) * l.kv_heads * l.head_dim * 2);
        };
        if (source_bytes > 0) {
            self.source_keys = try Buffer.init(copies.runtime, source_bytes);
            self.source_values = try Buffer.init(copies.runtime, source_bytes);
        }
        try self.owners.ensureTotalCapacityPrecise(allocator, owner_count);
        self.fitter.core.cancelled = config.cancelled;
        self.fitter.execution_control = config.execution_control;
        self.attention.core.cancelled = config.cancelled;
        for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
            if (l.owner != i) {
                self.owner_indices[i] = self.owner_indices[l.owner];
                var stratum_offset: u32 = 0;
                for (config.layout.layers[0..i]) |prior| if (prior.owner == l.owner) {
                    stratum_offset = try std.math.add(u32, stratum_offset, prior.query_heads / prior.kv_heads);
                };
                self.consumer_stratum_offsets[i] = stratum_offset;
                continue;
            }
            const g = try geometry(config, i);
            var o = Owner{ .layer = @intCast(i), .shape = l, .geometry = g, .live = try Generation.init(allocator, copies.runtime, g, l) };
            errdefer o.deinit(allocator);
            if (g.paged) {
                o.staged = .{ .spans = try allocator.alloc(views.Span, g.span_capacity) };
                const vectors = @as(usize, g.training_rows + g.validation_rows) * l.kv_heads;
                o.queries = try Buffer.init(copies.runtime, vectors * l.head_dim * 4);
                o.mapping = try Buffer.init(copies.runtime, vectors * 4);
                o.positions = try allocator.alloc(u32, vectors);
                @memset(o.positions, absent);
            }
            self.owner_indices[i] = @intCast(self.owners.items.len);
            self.consumer_stratum_offsets[i] = 0;
            try self.owners.append(allocator, o);
        }
        return self;
    }
    /// The request owner must end calls before destroying the session. Every
    /// method waits for GPU completion, including all error/cancellation paths.
    pub fn deinit(self: *Session) void {
        for (self.owners.items) |*o| o.deinit(self.allocator);
        self.owners.deinit(self.allocator);
        if (self.source_keys) |*b| b.deinit();
        if (self.source_values) |*b| b.deinit();
        self.copies.deinit();
        self.attention.deinit();
        self.fitter.deinit();
        self.* = undefined;
    }
    pub fn hook(self: *Session) contract.Hook {
        return .{ .ctx = self, .identity = self.identity, .plan = self.config.plan, .vtable = &.{ .attention = dispatch, .next_chunk = nextChunk } };
    }
    fn nextChunk(ctx: *anyopaque, identity: u64, start: u32, requested: u32) !u32 {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        if (identity != self.identity) return error.StaleContextView;
        if (self.failed) return error.ContextSessionFailed;
        try self.checkCancelled();
        if (self.next_layer != 0 or start != self.logical_tokens or requested == 0 or start >= self.config.plan.logical_token_limit)
            return error.InvalidContextStep;
        var end: u64 = @min(self.config.plan.logical_token_limit, @as(u64, start) + @min(requested, self.config.max_query_rows));
        if (self.config.plan.policy.mode == .attention_matching and !self.retain_exact_remainder and self.config.calibration_replay_start == null) {
            const p = self.config.plan.policy;
            end = @min(end, @as(u64, p.keep_prefix_tokens) + p.keep_recent_tokens + (@as(u64, self.compacted_blocks) + 1) * 512);
        }
        return @intCast(end - start);
    }
    fn dispatch(ctx: *anyopaque, identity: u64, request: contract.Attention, q: contract.DeviceRef, k: contract.DeviceRef, v: contract.DeviceRef, out: contract.DeviceRef) !void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        if (identity != self.identity) return error.StaleContextView;
        if (self.failed) return error.ContextSessionFailed;
        try self.checkCancelled();
        var protection = if (self.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        // A partially executed model step may not be retried with stale KV.
        errdefer self.failed = true;
        try self.run(request, ref(q), ref(k), ref(v), ref(out));
    }
    fn ref(value: contract.DeviceRef) Ref {
        return .{ .handle = value.handle, .offset = value.offset, .bytes = value.bytes };
    }
    fn checkCancelled(self: *const Session) !void {
        if (self.config.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.config.execution_control) |control| try control.check();
    }
    fn encode(self: *Session, params: shared.Params, inputs: []const Ref) !void {
        try self.checkCancelled();
        var handles: [8]?*anyopaque = @splat(self.copies.dummy.handle);
        var offsets: [8]usize = @splat(0);
        var lengths: [8]usize = @splat(self.copies.dummy.logical_bytes);
        for (inputs, 0..) |r, i| {
            if (r.offset % 4 != 0) return error.MetalBufferRange;
            handles[i] = r.handle;
            offsets[i] = r.offset;
            lengths[i] = r.bytes;
        }
        if (mr.termite_metal_generated_dispatch(self.copies.runtime, self.copies.pipeline, &handles, &offsets, &lengths, 8, &params, @sizeOf(shared.Params), params.n) != 0) return error.ContextMetalDispatchFailed;
        try self.checkCancelled();
    }
    fn copy(self: *Session, source: Ref, dest: Ref, bytes: usize) !void {
        if (bytes == 0) return;
        if (bytes % 4 != 0 or source.handle == dest.handle or bytes > source.bytes or bytes > dest.bytes or bytes / 4 > std.math.maxInt(u32))
            return error.MetalBufferRange;
        try self.encode(.{ .op = 0, .n = @intCast(bytes / 4) }, &.{ source, self.copies.dummy.ref(), dest });
        self.metrics.source_gather_bytes +|= bytes;
    }
    fn run(self: *Session, r: contract.Attention, q: Ref, k: Ref, v: Ref, out: Ref) !void {
        try self.checkCancelled();
        if (r.consumer_layer != self.next_layer or r.consumer_layer >= self.config.layout.len or r.query_rows == 0 or
            r.query_rows > self.config.max_query_rows or r.logit_softcap != 0) return error.InvalidContextStep;
        const shape = self.config.layout.layers[r.consumer_layer];
        if (r.owner_layer != shape.owner or r.query_heads != shape.query_heads or r.kv_heads != shape.kv_heads or
            r.head_dim != shape.head_dim or r.sliding_window != shape.sliding_window or r.skip_kv_write != (shape.owner != r.consumer_layer) or
            !std.math.isFinite(r.scale) or r.scale <= 0) return error.InvalidContextLayout;
        for ([_]Ref{ q, k, v }) |input| if (input.handle == out.handle) return error.AliasedContextStaging;
        const end = try std.math.add(u32, r.query_start, r.query_rows);
        if (self.next_layer == 0) {
            if (r.query_start != self.logical_tokens or end > self.config.plan.logical_token_limit) return error.InvalidContextStep;
            // Never compact across a prefill query's causal subdivisions.
            if (self.config.plan.policy.mode == .attention_matching and !self.retain_exact_remainder and self.config.calibration_replay_start == null) {
                const boundary = @as(u64, self.config.plan.policy.keep_prefix_tokens) + self.config.plan.policy.keep_recent_tokens +
                    (@as(u64, self.compacted_blocks) + 1) * 512;
                if (end > boundary) return error.ContextChunkCrossesBoundary;
            }
            self.step_start = r.query_start;
            self.step_end = end;
        } else if (r.query_start != self.step_start or end != self.step_end) return error.InvalidContextStep;
        const o = &self.owners.items[self.owner_indices[r.consumer_layer]];
        if (o.scale) |scale| {
            if (scale != r.scale) return error.InvalidContextLayout;
        } else o.scale = r.scale;
        if (!r.skip_kv_write) try self.append(o, r, k, v);
        const epoch = try self.publication.acquire();
        {
            defer self.publication.release(epoch) catch unreachable;
            const prior_readback = self.attention.status_readback_bytes;
            defer self.metrics.attention_status_readback_bytes +|= self.attention.status_readback_bytes - prior_readback;
            const rolling_window: u32 = if (shape.sliding_window == 0 and self.config.plan.policy.mode == .rolling)
                self.config.plan.policy.max_kv_tokens.? - self.config.plan.policy.keep_prefix_tokens
            else
                shape.sliding_window;
            try self.attention.attention(.{ .view = self.view(o, &o.live, epoch, end), .current_epoch = epoch, .query_start = r.query_start, .query_rows = r.query_rows, .query_heads = r.query_heads, .sliding_window = rolling_window, .keep_prefix_tokens = o.geometry.prefix, .scale = r.scale }, q, o.live.keys.ref(), o.live.values.ref(), if (o.live.mass) |b| b.ref() else self.attention.core.dummy.ref(), out);
        }
        if (o.queries != null and !self.retain_exact_remainder) try self.capture(o, r, q);
        self.next_layer += 1;
        if (self.next_layer == self.config.layout.len) {
            self.maintain(end) catch |err| {
                // Attention for this step completed and a quality rejection
                // leaves every live generation untouched. Continue only when
                // the entire requested remainder fits the existing reservation.
                // Allocation, cancellation, nonfinite input and device errors
                // must still poison the partially executed model request.
                if (err != error.CompactionQualityRejected or self.publication.pending or !self.canRetainRemainder(end)) return err;
                try self.checkCancelled();
                self.retain_exact_remainder = true;
                self.metrics.exact_tail_fallbacks +|= 1;
            };
            // The extra ring rows protect earlier queries while a chunk is
            // in flight. Retire those descriptors after every consumer has
            // finished; physical capacity stays reserved for the next chunk.
            for (self.owners.items) |*owner| if (owner.geometry.ring > 0) {
                ringSpans(owner, end, owner.geometry.ring - self.config.max_query_rows + 1);
            };
            self.logical_tokens = end;
            self.next_layer = 0;
            self.metrics.logical_tokens = end;
            self.metrics.epoch = self.publication.epoch;
            self.metrics.retained_rows = 0;
            for (self.owners.items) |*owner| self.metrics.retained_rows = @max(self.metrics.retained_rows, owner.live.rows);
        }
    }
    fn view(_: *Session, o: *const Owner, gen: anytype, epoch: u64, logical: u32) views.View {
        return .{ .epoch = epoch, .logical_tokens = logical, .capacity_rows = o.geometry.capacity, .bias_rows = if (o.live.mass != null) o.geometry.capacity else 0, .kv_heads = o.shape.kv_heads, .head_dim = o.shape.head_dim, .spans = gen.spans[0..gen.len] };
    }
    fn append(self: *Session, o: *Owner, r: contract.Attention, k: Ref, v: Ref) !void {
        const width = o.shape.kv_heads * o.shape.head_dim;
        const count = try std.math.mul(u32, r.query_rows, width);
        if (k.bytes < @as(usize, count) * 4 or v.bytes < @as(usize, count) * 4) return error.MetalBufferRange;
        if (o.geometry.ring > 0) {
            try self.encode(.{ .op = 1, .n = count, .dim = width, .position = r.query_start, .stride = o.geometry.ring, .index = o.geometry.prefix }, &.{ k, v, o.live.keys.ref(), o.live.values.ref() });
            ringSpans(o, self.step_end, o.geometry.ring);
        } else if (o.geometry.paged) {
            if (@as(u64, o.live.rows) + r.query_rows > o.geometry.retained_capacity) return error.ContextKvBudgetExceeded;
            var consumed: u32 = 0;
            while (consumed < r.query_rows) {
                const logical = r.query_start + consumed;
                const prefix = logical < o.geometry.page_prefix;
                const extend = o.live.len > 0 and o.live.spans[o.live.len - 1].kind == .exact and
                    (prefix or (o.live.spans[o.live.len - 1].logical_start >= o.geometry.page_prefix and o.live.spans[o.live.len - 1].row_count < 256));
                const occupied = if (extend) o.live.spans[o.live.len - 1].row_count else 0;
                const physical = if (extend) o.live.spans[o.live.len - 1].physical_start else if (prefix) 0 else try freePage(o.geometry, o.live.spans[0..o.live.len]);
                const rows = @min(r.query_rows - consumed, (if (prefix) o.geometry.page_prefix else @as(u32, 256)) - occupied);
                const input_start = @as(usize, consumed) * width * 4;
                const input_bytes = @as(usize, rows) * width * 4;
                const dest_start = @as(usize, physical + occupied) * width * 2;
                const dest_bytes = @as(usize, rows) * width * 2;
                try self.encode(.{ .op = 1, .n = rows * width, .dim = width, .stride = rows }, &.{ try k.slice(input_start, input_bytes), try v.slice(input_start, input_bytes), try o.live.keys.ref().slice(dest_start, dest_bytes), try o.live.values.ref().slice(dest_start, dest_bytes) });
                if (extend) {
                    const last = &o.live.spans[o.live.len - 1];
                    last.row_count += rows;
                    last.logical_count += rows;
                    o.live.rows += rows;
                } else addSpan(&o.live, .{ .physical_start = physical, .row_count = rows, .logical_start = logical, .logical_count = rows });
                consumed += rows;
            }
        } else {
            if (@as(u64, o.live.rows) + r.query_rows > o.geometry.capacity) return error.ContextKvBudgetExceeded;
            const row_bytes = @as(usize, width) * 2;
            try self.encode(.{ .op = 1, .n = count, .dim = width, .position = 0, .stride = r.query_rows }, &.{ k, v, try o.live.keys.ref().slice(@as(usize, o.live.rows) * row_bytes, @as(usize, r.query_rows) * row_bytes), try o.live.values.ref().slice(@as(usize, o.live.rows) * row_bytes, @as(usize, r.query_rows) * row_bytes) });
            if (o.live.len > 0 and o.live.spans[o.live.len - 1].kind == .exact) {
                const last = &o.live.spans[o.live.len - 1];
                last.row_count += r.query_rows;
                last.logical_count += r.query_rows;
                o.live.rows += r.query_rows;
            } else addSpan(&o.live, .{ .physical_start = o.live.rows, .row_count = r.query_rows, .logical_start = r.query_start, .logical_count = r.query_rows });
        }
        _ = try self.view(o, &o.live, self.publication.epoch, self.step_end).validate();
    }
    fn ringSpans(o: *Owner, end: u32, retained_ring: u32) void {
        o.live.len = 0;
        o.live.rows = 0;
        const prefix_end = @min(end, o.geometry.prefix);
        if (prefix_end > 0) addSpan(&o.live, .{ .physical_start = 0, .row_count = prefix_end, .logical_start = 0, .logical_count = prefix_end });
        const start = @max(prefix_end, end -| retained_ring);
        if (start < end) {
            const physical = o.geometry.prefix + (start - o.geometry.prefix) % o.geometry.ring;
            const first = @min(end - start, o.geometry.capacity - physical);
            addSpan(&o.live, .{ .physical_start = physical, .row_count = first, .logical_start = start, .logical_count = first });
            if (start + first < end) addSpan(&o.live, .{ .physical_start = o.geometry.prefix, .row_count = end - start - first, .logical_start = start + first, .logical_count = end - start - first });
        }
    }
    fn addSpan(gen: anytype, span: views.Span) void {
        std.debug.assert(gen.len < gen.spans.len);
        gen.spans[gen.len] = span;
        gen.len += 1;
        gen.rows += span.row_count;
    }
    /// Candidate pages are selected against the published descriptors only.
    /// The request lock prevents appends during a transaction. A successful
    /// descriptor swap retires the two source pages; cancellation changes no
    /// ownership and makes the unpublished destination immediately reusable.
    fn freePage(g: Geometry, spans: []const views.Span) !u32 {
        pages: for (0..g.page_count) |page| {
            const start = g.page_prefix + @as(u32, @intCast(page)) * 256;
            for (spans) |span| {
                if (start < @as(u64, span.physical_start) + span.row_count and span.physical_start < start + 256) continue :pages;
            }
            return start;
        }
        return error.ContextKvBudgetExceeded;
    }
    fn stageDescriptors(o: *Owner, source_start: u32, destination: u32) ![2]u32 {
        const stage = &o.staged.?;
        stage.len = 0;
        stage.rows = 0;
        var source: [2]u32 = undefined;
        var found: u32 = 0;
        for (o.live.spans[0..o.live.len]) |span| {
            if (span.logical_start >= source_start and span.logical_start < source_start + 512) {
                if (found >= 2 or span.logical_start != source_start + found * 256 or span.kind != .exact or span.row_count != 256 or span.logical_count != 256) return error.InvalidContextView;
                source[found] = span.physical_start;
                if (found == 0) addSpan(stage, .{ .physical_start = destination, .row_count = 256, .logical_start = source_start, .logical_count = 512, .kind = .compacted, .bias_start = destination });
                found += 1;
            } else addSpan(stage, span);
        }
        if (found != 2 or stage.rows + 256 != o.live.rows) return error.InvalidContextView;
        return source;
    }
    fn capture(self: *Session, o: *Owner, r: contract.Attention, q: Ref) !void {
        const g = o.geometry;
        if (self.config.calibration_replay_start) |start| if (@as(u64, r.query_start) + r.query_rows <= start) return;
        const training_bounds = if (self.config.calibration_replay_start) |start| try QueryBounds.init(start, @intCast(self.config.plan.logical_token_limit), false) else null;
        const validation_bounds = if (self.config.calibration_replay_start) |start| try QueryBounds.init(start, @intCast(self.config.plan.logical_token_limit), true) else null;
        const vectors = g.training_rows + g.validation_rows;
        const groups = r.query_heads / r.kv_heads;
        const stratum_offset = self.consumer_stratum_offsets[r.consumer_layer];
        const map = std.mem.bytesAsSlice(u32, @as([]align(4) u8, @alignCast(o.mapping.?.bytes[0..o.mapping.?.logical_bytes])));
        @memset(map, absent);
        if (q.bytes < @as(usize, r.query_rows) * r.query_heads * r.head_dim * 4) return error.MetalBufferRange;
        for (0..r.query_rows) |row| {
            const pos = r.query_start + @as(u32, @intCast(row));
            if (self.config.calibration_replay_start) |start| if (pos < start) continue;
            const partition = queryPartition(pos);
            const population_rows = if (partition.held) g.held_per_stratum else g.train_per_stratum;
            const bounds = if (partition.held) validation_bounds else training_bounds;
            const sample_slot = if (bounds) |b| (try b.slot(partition.ordinal, population_rows)) orelse continue else partition.ordinal % population_rows;
            for (0..r.query_heads) |head| {
                const h: u32 = @intCast(head);
                const stratum = stratum_offset + h % groups;
                const slot = if (partition.held) g.training_rows + stratum * g.held_per_stratum + sample_slot else stratum * g.train_per_stratum + sample_slot;
                const dest = (h / groups) * vectors + slot;
                map[dest] = @intCast(row * r.query_heads + head);
                o.positions[dest] = pos;
            }
        }
        try self.encode(.{ .op = 2, .n = @intCast(map.len * r.head_dim), .dim = r.head_dim }, &.{ q, o.mapping.?.ref(), o.queries.?.ref() });
    }
    fn maintain(self: *Session, logical: u32) !void {
        const p = self.config.plan.policy;
        if (p.mode != .attention_matching or self.retain_exact_remainder or self.config.calibration_replay_start != null) return;
        const source_start = p.keep_prefix_tokens + self.compacted_blocks * 512;
        if (@as(u64, source_start) + 512 + p.keep_recent_tokens > logical) return;
        const epoch = try self.publication.begin();
        const started = shared.Profile.now();
        defer self.metrics.compaction_nanoseconds +|= shared.Profile.now() -| started;
        errdefer {
            if (self.publication.submitted and self.publication.completion == .pending) self.publication.complete(epoch, false) catch {};
            self.publication.cancel() catch {};
            self.metrics.rejected_transactions +|= 1;
        }
        try self.publication.submit();
        // Every arena reserves one unpublished page. A failure preserves all
        // live bytes/descriptors; prior compacted blocks and exact tail pages
        // are shared unchanged by the two descriptor generations.
        for (self.owners.items) |*o| {
            if (o.staged == null) continue;
            for (o.positions) |position| if (position == absent or position < source_start + 512) return error.InsufficientContextQueries;
            const stage = &o.staged.?;
            const destination = try freePage(o.geometry, o.live.spans[0..o.live.len]);
            const source = try stageDescriptors(o, source_start, destination);
            const row_bytes = @as(usize, o.shape.kv_heads) * o.shape.head_dim * 2;
            const source_bytes = 512 * row_bytes;
            // The first source is usually contiguous. Reused pages need not
            // be adjacent; gather only that original block, entirely on GPU,
            // into the one shared scratch pair reserved at session creation.
            var keys = try o.live.keys.ref().slice(@as(usize, source[0]) * row_bytes, 256 * row_bytes);
            var values = try o.live.values.ref().slice(@as(usize, source[0]) * row_bytes, 256 * row_bytes);
            if (source[1] == source[0] + 256) {
                keys = try o.live.keys.ref().slice(@as(usize, source[0]) * row_bytes, source_bytes);
                values = try o.live.values.ref().slice(@as(usize, source[0]) * row_bytes, source_bytes);
            } else {
                for (source, 0..) |physical, index| {
                    const offset = @as(usize, physical) * row_bytes;
                    try self.copy(try o.live.keys.ref().slice(offset, 256 * row_bytes), try self.source_keys.?.ref().slice(index * 256 * row_bytes, 256 * row_bytes), 256 * row_bytes);
                    try self.copy(try o.live.values.ref().slice(offset, 256 * row_bytes), try self.source_values.?.ref().slice(index * 256 * row_bytes, 256 * row_bytes), 256 * row_bytes);
                }
                keys = try self.source_keys.?.ref().slice(0, source_bytes);
                values = try self.source_values.?.ref().slice(0, source_bytes);
            }
            for (0..o.shape.kv_heads) |h| {
                const query_stride = @as(usize, o.geometry.training_rows + o.geometry.validation_rows) * o.shape.head_dim * 4;
                const train_bytes = @as(usize, o.geometry.training_rows) * o.shape.head_dim * 4;
                const held_bytes = @as(usize, o.geometry.validation_rows) * o.shape.head_dim * 4;
                const prior_readback = self.fitter.status_readback_bytes;
                defer self.metrics.status_readback_bytes +|= self.fitter.status_readback_bytes - prior_readback;
                var fit_request = fitter_mod.Request{ .source_rows = 512, .head_dim = o.shape.head_dim, .kv_heads = o.shape.kv_heads, .kv_head = @intCast(h), .training_rows = o.geometry.training_rows, .validation_rows = o.geometry.validation_rows, .validation_group_rows = o.geometry.held_per_stratum, .scale = o.scale.?, .staging_budget_bytes = (try allocation(self.config)).workspace_bytes };
                const training = try o.queries.?.ref().slice(h * query_stride, train_bytes);
                const held = try o.queries.?.ref().slice(h * query_stride + train_bytes, held_bytes);
                var candidate = self.fitter.fit(fit_request, keys, values, training, held) catch |err| retry: {
                    if (err != error.CompactionQualityRejected) return err;
                    // The failed candidate has already been drained/freed.
                    // Reuse the same bounded workspace budget and immutable
                    // source with a residual-driven selection, never weaker gates.
                    self.metrics.fit_fallbacks +|= 1;
                    fit_request.options.key_selection = .matching_pursuit;
                    break :retry try self.fitter.fit(fit_request, keys, values, training, held);
                };
                defer candidate.deinit();
                try self.fitter.packHead(&candidate, @intCast(h), o.shape.kv_heads, try o.live.keys.ref().slice(@as(usize, destination) * row_bytes, 256 * row_bytes), try o.live.values.ref().slice(@as(usize, destination) * row_bytes, 256 * row_bytes), try o.live.mass.?.ref().slice(@as(usize, destination) * o.shape.kv_heads * 4, 256 * @as(usize, o.shape.kv_heads) * 4));
            }
            _ = try self.view(o, stage, epoch, logical).validate();
        }
        // All three command queues have completed synchronously. Cancellation
        // gets one last check before the allocation-free atomic publication.
        try self.checkCancelled();
        try self.publication.complete(epoch, true);
        try self.publication.commit(epoch);
        for (self.owners.items) |*o| if (o.staged) |*stage| {
            std.mem.swap([]views.Span, &o.live.spans, &stage.spans);
            std.mem.swap(usize, &o.live.len, &stage.len);
            std.mem.swap(u32, &o.live.rows, &stage.rows);
        };
        self.compacted_blocks += 1;
        self.metrics.compacted_blocks = self.compacted_blocks;
    }

    fn canRetainRemainder(self: *const Session, logical: u32) bool {
        if (logical > self.config.plan.logical_token_limit) return false;
        const remaining = self.config.plan.logical_token_limit - logical;
        for (self.owners.items) |owner| {
            if (owner.staged != null and @as(u64, owner.live.rows) + remaining > owner.geometry.retained_capacity) return false;
        }
        return true;
    }
};

fn testLayout() policy_mod.Layout {
    const global = policy_mod.LayerLayout{ .owner = 0, .sliding_window = 0, .kv_heads = 1, .query_heads = 2, .head_dim = 32, .rope_active_dim = 32, .rope_frequency_dim = 32, .rope_theta = 10000 };
    var layout = policy_mod.Layout{ .len = 4 };
    layout.layers[0] = global;
    layout.layers[1] = global;
    layout.layers[1].owner = 1;
    layout.layers[1].sliding_window = 8;
    layout.layers[2] = global;
    layout.layers[2].owner = 2;
    layout.layers[2].kv_heads = 2;
    layout.layers[3] = global;
    return layout;
}
fn device(buffer: Buffer) contract.DeviceRef {
    return .{ .handle = buffer.handle, .bytes = buffer.logical_bytes };
}
fn testConfig(p: policy_mod.Policy, logical: u32, chunk: u32) !Config {
    var config = Config{ .plan = try p.admit(logical + 1, null, logical - 1, 1, 0), .layout = testLayout(), .max_query_rows = chunk, .physical_budget_bytes = 0 };
    config.physical_budget_bytes = (try allocation(config)).physical_bytes;
    return config;
}
// Test-only byte audit of published ranges. Unpublished arena pages are
// intentionally excluded: writes there must not change the live generation.
fn testPublishedDigest(owner: *const Owner) u64 {
    var hash = std.hash.Wyhash.init(0);
    hash.update(std.mem.sliceAsBytes(owner.live.spans[0..owner.live.len]));
    const row_bytes = @as(usize, owner.shape.kv_heads) * owner.shape.head_dim * 2;
    for (owner.live.spans[0..owner.live.len]) |span| {
        const offset = @as(usize, span.physical_start) * row_bytes;
        const bytes = @as(usize, span.row_count) * row_bytes;
        hash.update(owner.live.keys.bytes[offset..][0..bytes]);
        hash.update(owner.live.values.bytes[offset..][0..bytes]);
        if (span.kind == .compacted) hash.update(owner.live.mass.?.bytes[@as(usize, span.bias_start) * owner.shape.kv_heads * 4 ..][0 .. @as(usize, span.row_count) * owner.shape.kv_heads * 4]);
    }
    return hash.final();
}

test "context session budget covers paged arenas queries staging and page slack" {
    var config = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = 2048, .keep_prefix_tokens = 0, .keep_recent_tokens = 512 }, 1537, 128);
    const sizes = try allocation(config);
    try std.testing.expect(sizes.staging_bytes > 0 and sizes.query_bytes > 0 and sizes.workspace_bytes > 0);
    try std.testing.expectEqual(config.physical_budget_bytes, sizes.live_bytes + sizes.staging_bytes + sizes.query_bytes + sizes.workspace_bytes + sizes.metadata_bytes);
    config.max_query_rows = 0;
    try std.testing.expectError(error.InvalidContextLayout, allocation(config));
    config.max_query_rows = 128;
    config.plan.policy.keep_recent_tokens = 1;
    config.plan = try config.plan.policy.admit(1538, null, 1536, 1, 0);
    try std.testing.expectError(error.InvalidContextQueryWindow, allocation(config));
}

test "context session staging stays bounded as retained capacity grows" {
    const small = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = 8192, .keep_prefix_tokens = 3, .keep_recent_tokens = 512 }, 8192, 128);
    const large = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = 65536, .keep_prefix_tokens = 3, .keep_recent_tokens = 512 }, 65536, 128);
    const a = try allocation(small);
    const b = try allocation(large);
    try std.testing.expect(b.live_bytes > a.live_bytes * 6);
    try std.testing.expectEqual(a.staging_bytes, b.staging_bytes);
    try std.testing.expectEqual(a.query_bytes, b.query_bytes);
    // Gathering uses the largest original block once across all owners, not
    // a second complete KV generation. Descriptor upload may grow by pages.
    try std.testing.expect(b.workspace_bytes <= a.workspace_bytes + 2 * std.heap.pageSize());
    try std.testing.expect(b.staging_bytes * 16 < b.live_bytes);
    for (large.layout.layers[0..large.layout.len], 0..) |layer, index| {
        if (layer.owner != index or layer.sliding_window != 0) continue;
        const g = try geometry(large, index);
        try std.testing.expectEqual(@as(u32, 65536), g.retained_capacity);
        try std.testing.expect(g.capacity >= g.retained_capacity + 256 and g.capacity < g.retained_capacity + 512);
    }
}

test "context reference capture requires full exact capacity and bounded replay" {
    const p = policy_mod.Policy{ .mode = .attention_matching, .max_kv_tokens = 16384, .keep_prefix_tokens = 0, .keep_recent_tokens = 512 };
    var config = try testConfig(p, 1536, 128);
    config.plan = try p.admit(16384, null, 1024, 512, 0);
    config.calibration_replay_start = 1024;
    _ = try allocation(config);
    config.calibration_replay_start = 1023;
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config));
    config.calibration_replay_start = 1024;
    config.plan = try p.admit(16384, null, 1024, 8193, 0);
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config));
    var bounded = p;
    bounded.max_kv_tokens = 2048;
    config.plan = try bounded.admit(16384, null, 1024, 1536, 0);
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config));
    config.plan = try (policy_mod.Policy{}).admit(16384, null, 1024, 512, 0);
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config));
}

test "context reference capture sizes odd replay from exact hash partitions" {
    const p = policy_mod.Policy{ .mode = .attention_matching, .max_kv_tokens = 1081, .keep_recent_tokens = 60 };
    var config = try testConfig(p, 1081, 128);
    config.plan = try p.admit(2048, null, 1024, 57, 0);
    config.calibration_replay_start = 1024;
    const train_bounds = try QueryBounds.init(1024, 1081, false);
    const held_bounds = try QueryBounds.init(1024, 1081, true);
    const train_population = train_bounds.last_ordinal - train_bounds.first_ordinal + 1;
    const held_population = held_bounds.last_ordinal - held_bounds.first_ordinal + 1;
    const calibration = @import("../runtime/kv/attention_matching.zig");
    for (config.layout.layers[0..config.layout.len], 0..) |layer, index| {
        if (layer.owner != index or layer.sliding_window != 0) continue;
        const g = try geometry(config, index);
        const strata = g.training_rows / g.train_per_stratum;
        try std.testing.expectEqual(@min(calibration.max_training_rows / strata, train_population), g.train_per_stratum);
        try std.testing.expectEqual(@min(calibration.max_validation_rows / strata, held_population), g.held_per_stratum);
    }
}

test "context reference replay crosses maintenance boundary without fitting or capturing ingestion" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const p = policy_mod.Policy{ .mode = .attention_matching, .max_kv_tokens = 2048, .keep_prefix_tokens = 0, .keep_recent_tokens = 512 };
    var config = try testConfig(p, 1536, 128);
    // Owner 0 contributes two GQA strata and its shared consumer contributes
    // four. Exercise the actual device capture map with cumulative offsets.
    config.layout.layers[3].query_heads = 4;
    config.plan = try p.admit(2048, null, 1024, 512, 0);
    config.calibration_replay_start = 1024;
    config.physical_budget_bytes = (try allocation(config)).physical_bytes;
    var session = try Session.init(std.testing.allocator, config);
    defer session.deinit();
    try std.testing.expectEqual(@as(u32, 0), session.consumer_stratum_offsets[0]);
    try std.testing.expectEqual(@as(u32, 2), session.consumer_stratum_offsets[3]);
    var q = try Buffer.init(session.copies.runtime, 128 * 4 * 32 * 4);
    defer q.deinit();
    var k = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer k.deinit();
    var v = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer v.deinit();
    var out = try Buffer.init(session.copies.runtime, 128 * 4 * 32 * 4);
    defer out.deinit();
    @memset(k.floats(), 0);
    var start: u32 = 0;
    while (start < 1536) {
        const rows = try session.hook().nextChunk(start, 1536 - start);
        @memset(q.floats(), if (start < 1024) 1 else 2);
        @memset(v.floats(), if (start < 1024) 1 else 2);
        for (config.layout.layers[0..config.layout.len], 0..) |layer, index| {
            try session.hook().attention(.{ .consumer_layer = @intCast(index), .owner_layer = layer.owner, .query_start = start, .query_rows = rows, .query_heads = layer.query_heads, .kv_heads = layer.kv_heads, .head_dim = layer.head_dim, .sliding_window = layer.sliding_window, .skip_kv_write = layer.owner != index, .scale = 1 }, device(q), device(k), device(v), device(out));
        }
        start += rows;
        if (start == 1024) for (session.owners.items) |owner| for (owner.positions) |position|
            try std.testing.expectEqual(absent, position);
    }
    try std.testing.expectEqual(@as(u64, 1), session.publication.epoch);
    try std.testing.expectEqual(@as(u64, 0), session.fitter.status_readback_bytes);
    try std.testing.expectEqual(@as(u32, 0), session.metrics.compacted_blocks);
    for (session.owners.items) |owner| {
        if (owner.shape.sliding_window != 0) continue;
        for (owner.positions) |position| try std.testing.expect(position >= 1024 and position < 1536);
        const g = owner.geometry;
        for (0..owner.shape.kv_heads) |head| {
            const positions = owner.positions[head * (g.training_rows + g.validation_rows) ..];
            for (0..g.training_rows / g.train_per_stratum) |stratum| {
                for ([_]bool{ false, true }) |held| {
                    const bounds = try QueryBounds.init(1024, 1536, held);
                    const count = if (held) g.held_per_stratum else g.train_per_stratum;
                    const offset = (if (held) g.training_rows else @as(u32, 0)) + stratum * count;
                    try std.testing.expectEqual(bounds.first_position, positions[offset]);
                    try std.testing.expectEqual(bounds.last_position, positions[offset + count - 1]);
                }
            }
        }
        for (owner.queries.?.floats()) |value| try std.testing.expectEqual(@as(f32, 2), value);
        const original = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(owner.live.values.bytes[0 .. 1024 * owner.shape.kv_heads * owner.shape.head_dim * 2])));
        for (original) |value| try std.testing.expectEqual(@as(f16, 1), value);
        try std.testing.expectEqual(@as(u32, 1536), owner.live.rows);
    }
}

test "context reference query sampling spans whole replay with disjoint uniform strata" {
    // Odd boundaries, tiny populations and the installed-model replay length.
    for ([_][2]u32{ .{ 1, 13 }, .{ 1024, 1536 }, .{ 2817, 5646 }, .{ 131000, 131072 } }) |interval| {
        for ([_]bool{ false, true }) |held| {
            const bounds = try QueryBounds.init(interval[0], interval[1], held);
            const population = bounds.last_ordinal - bounds.first_ordinal + 1;
            for ([_]u32{ 1, @min(population, 25), @min(population, 76), @min(population, 128), @min(population, 384), population }) |count| {
                var found: u32 = 0;
                var first: u32 = absent;
                var last: u32 = absent;
                var previous_ordinal: ?u32 = null;
                for (interval[0]..interval[1]) |position| {
                    const part = queryPartition(@intCast(position));
                    if (part.held != held) continue;
                    if (try bounds.slot(part.ordinal, count)) |slot| {
                        try std.testing.expectEqual(found, slot);
                        if (found == 0) first = @intCast(position);
                        last = @intCast(position);
                        if (previous_ordinal) |previous| {
                            const step = part.ordinal - previous;
                            try std.testing.expect(step >= (population - 1) / (count - 1));
                            try std.testing.expect(step <= try std.math.divCeil(u32, population - 1, count - 1));
                        }
                        previous_ordinal = part.ordinal;
                        found += 1;
                    }
                }
                try std.testing.expectEqual(count, found);
                if (count > 1) {
                    try std.testing.expectEqual(bounds.first_position, first);
                    try std.testing.expectEqual(bounds.last_position, last);
                }
            }
            try std.testing.expectError(error.InsufficientContextQueries, bounds.slot(bounds.first_ordinal, 0));
            try std.testing.expectError(error.InsufficientContextQueries, bounds.slot(bounds.first_ordinal, population + 1));
            try std.testing.expectEqual(null, try bounds.slot(bounds.last_ordinal + 1, 1));
        }
    }
    try std.testing.expectError(error.InsufficientContextQueries, QueryBounds.init(2, 2, false));
    const part = queryPartition(2);
    try std.testing.expectError(error.InsufficientContextQueries, QueryBounds.init(2, 3, !part.held));
}

test "context query split covers periodic phases and preserves a bounded future reservoir" {
    var training: [8]u32 = @splat(0);
    var validation: [8]u32 = @splat(0);
    var train_positions: [381]u32 = @splat(absent);
    var held_positions: [127]u32 = @splat(absent);
    for (0..4096) |i| {
        const pos: u32 = @intCast(i);
        const part = queryPartition(pos);
        if (part.held) {
            validation[i % 8] += 1;
            held_positions[part.ordinal % held_positions.len] = pos;
        } else {
            training[i % 8] += 1;
            train_positions[part.ordinal % train_positions.len] = pos;
        }
        if (pos >= 512) {
            for (train_positions) |past| try std.testing.expect(past != absent and past >= pos + 1 - 512 and !queryPartition(past).held);
            for (held_positions) |past| try std.testing.expect(past != absent and past >= pos + 1 - 512 and queryPartition(past).held);
        }
    }
    for (training, validation) |train_count, held_count| try std.testing.expect(train_count > 0 and held_count > 0);
}

test "context Metal session rolls prefix and exact SWA across multirow wrap" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const config = try testConfig(.{ .mode = .rolling, .max_kv_tokens = 11, .keep_prefix_tokens = 3, .keep_recent_tokens = 8 }, 140, 7);
    var session = try Session.init(std.testing.allocator, config);
    defer session.deinit();
    var q = try Buffer.init(session.copies.runtime, 7 * 2 * 32 * 4);
    defer q.deinit();
    var k = try Buffer.init(session.copies.runtime, 7 * 2 * 32 * 4);
    defer k.deinit();
    var v = try Buffer.init(session.copies.runtime, 7 * 2 * 32 * 4);
    defer v.deinit();
    var out = try Buffer.init(session.copies.runtime, 7 * 2 * 32 * 4);
    defer out.deinit();
    @memset(q.floats(), 1);
    @memset(k.floats(), 0);
    var start: u32 = 0;
    while (start < 140) : (start += 7) {
        for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
            for (0..7) |row| @memset(v.floats()[row * l.kv_heads * 32 ..][0 .. l.kv_heads * 32], @floatFromInt(start + row));
            try session.hook().attention(.{ .consumer_layer = @intCast(i), .owner_layer = l.owner, .query_start = start, .query_rows = 7, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = 32, .sliding_window = l.sliding_window, .skip_kv_write = l.owner != i, .scale = 1 }, device(q), device(k), device(v), device(out));
            for (0..7) |row| {
                const pos = start + row;
                var total: f32 = 0;
                var count: f32 = 0;
                for (0..pos + 1) |past| {
                    if (pos - past < 8 or (l.sliding_window == 0 and past < 3)) {
                        total += @floatFromInt(past);
                        count += 1;
                    }
                }
                for (out.floats()[row * 64 ..][0..64]) |got| try std.testing.expectApproxEqAbs(total / count, got, 2e-5);
            }
        }
        for (session.owners.items) |owner| {
            const limit: u32 = if (owner.shape.sliding_window > 0) 8 else 11;
            try std.testing.expect(owner.live.rows <= limit);
            try std.testing.expect(owner.geometry.capacity > limit);
        }
    }
    try std.testing.expectEqual(@as(u32, 140), session.metrics.logical_tokens);
    try std.testing.expectEqual(@as(u64, 0), session.metrics.kv_readback_bytes);
    var stale = session.hook();
    stale.identity -= 1;
    try std.testing.expectError(error.StaleContextView, stale.attention(undefined, device(q), device(k), device(v), device(out)));
    try std.testing.expect(!session.failed);
}

test "context Metal session compacts all owners repeatedly and captures every donor consumer" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const blocks = 10;
    const logical = 3 + 513 + blocks * 512 + 1;
    const config = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = 3 + 769 + blocks * 256, .keep_prefix_tokens = 3, .keep_recent_tokens = 513 }, logical, 128);
    var session = try Session.init(std.testing.allocator, config);
    defer session.deinit();
    var zero_keys = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer zero_keys.deinit();
    @memset(zero_keys.floats(), 0);
    var arena_bytes: usize = 0;
    var query_bytes: usize = 0;
    for (session.owners.items) |owner| {
        arena_bytes += owner.live.keys.bytes.len + owner.live.values.bytes.len;
        if (owner.live.mass) |buffer| arena_bytes += buffer.bytes.len;
        if (owner.queries) |buffer| query_bytes += buffer.bytes.len;
        if (owner.mapping) |buffer| query_bytes += buffer.bytes.len;
    }
    try std.testing.expectEqual(session.metrics.live_bytes + session.metrics.staging_bytes, arena_bytes);
    try std.testing.expectEqual(session.metrics.query_bytes, query_bytes);
    var q = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer q.deinit();
    var k = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer k.deinit();
    var v = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer v.deinit();
    var out = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer out.deinit();
    @memset(k.floats(), 0);
    const first_handle = session.owners.items[0].live.keys.handle;
    var first_compacted: ?views.Span = null;
    var frozen_values: [256 * 32]f16 = undefined;
    var totals: [2 * 32]f64 = @splat(0);
    var start: u32 = 0;
    while (start < logical) {
        const rows = try session.hook().nextChunk(start, @min(128, logical - start));
        for (0..rows) |row| for (0..2) |head| for (0..32) |dim| {
            const position = start + @as(u32, @intCast(row));
            const base: f32 = if (position == logical - 1) -123 else @as(f32, @floatFromInt(((position -| 3) / 256) % 5)) - 2;
            const value = base + @as(f32, @floatFromInt(head)) / 2 + @as(f32, @floatFromInt(dim % 3)) / 4;
            v.floats()[(row * 2 + head) * 32 + dim] = value;
            totals[head * 32 + dim] += value;
        };
        for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
            @memset(q.floats(), @floatFromInt(i + 1));
            // Inputs are interleaved using the actual KV-head count. The
            // one-head owners receive head zero's independently tracked data.
            if (l.kv_heads == 1) {
                for (0..rows) |row| @memcpy(k.floats()[row * 32 ..][0..32], v.floats()[row * 2 * 32 ..][0..32]);
            }
            const values = if (l.kv_heads == 1) device(k) else device(v);
            try session.hook().attention(.{ .consumer_layer = @intCast(i), .owner_layer = l.owner, .query_start = start, .query_rows = rows, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = 32, .sliding_window = l.sliding_window, .skip_kv_write = l.owner != i, .scale = 1 }, device(q), device(zero_keys), values, device(out));
            if (l.sliding_window == 0) for (0..2) |head| for (0..32) |dim| {
                const kv_head = head / (2 / l.kv_heads);
                const expected: f32 = @floatCast(totals[kv_head * 32 + dim] / @as(f64, @floatFromInt(start + rows)));
                try std.testing.expectApproxEqAbs(expected, out.floats()[(@as(usize, rows) - 1) * 64 + head * 32 + dim], 0.001);
            };
        }
        const first_owner = &session.owners.items[0];
        if (session.compacted_blocks > 0) {
            const span = first_owner.live.spans[1];
            const offset = @as(usize, span.physical_start) * 32 * 2;
            const bytes = first_owner.live.values.bytes[offset..][0 .. 256 * 32 * 2];
            if (first_compacted) |prior| {
                try std.testing.expectEqualDeep(prior, span);
                try std.testing.expectEqualSlices(u8, std.mem.asBytes(&frozen_values), bytes);
            } else {
                first_compacted = span;
                @memcpy(std.mem.asBytes(&frozen_values), bytes);
            }
        }
        start += rows;
    }
    try std.testing.expectEqual(@as(u32, blocks), session.metrics.compacted_blocks);
    try std.testing.expectEqual(@as(u64, blocks + 1), session.metrics.epoch);
    try std.testing.expectEqual(@as(u64, blocks * 96), session.metrics.status_readback_bytes);
    try std.testing.expectEqual(first_handle, session.owners.items[0].live.keys.handle);
    try std.testing.expect(session.metrics.source_gather_bytes > 0);
    try std.testing.expect(session.metrics.source_gather_bytes <= blocks * 512 * 3 * 32 * 4);
    try std.testing.expectEqual(@as(u64, 0), session.metrics.kv_readback_bytes);
    const owner = &session.owners.items[0];
    const train = owner.queries.?.floats();
    for (train[0 .. 2 * owner.geometry.train_per_stratum * 32]) |value| try std.testing.expectEqual(@as(f32, 1), value);
    for (train[2 * owner.geometry.train_per_stratum * 32 .. owner.geometry.training_rows * 32]) |value|
        try std.testing.expectEqual(@as(f32, 4), value);
}

test "context Metal session rejects budget cancellation and failed all owner publication" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var config = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = 1536, .keep_prefix_tokens = 0, .keep_recent_tokens = 512 }, 1025, 128);
    config.physical_budget_bytes -= 1;
    try std.testing.expectError(error.ContextStagingBudgetExceeded, Session.init(std.testing.allocator, config));
    config.physical_budget_bytes += 1;
    var stop = std.atomic.Value(bool).init(true);
    config.cancelled = &stop;
    try std.testing.expectError(error.Cancelled, Session.init(std.testing.allocator, config));
    stop.store(false, .release);
    var session = try Session.init(std.testing.allocator, config);
    defer session.deinit();
    const first = session.owners.items[0].live.keys.handle;
    const second = session.owners.items[2].live.keys.handle;
    var q = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer q.deinit();
    var k = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer k.deinit();
    var v = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer v.deinit();
    var out = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
    defer out.deinit();
    @memset(k.floats(), 0);
    @memset(v.floats(), 1);
    try std.testing.expect(session.mutex.tryLock());
    try std.testing.expectError(error.ContextTransactionBusy, session.hook().attention(undefined, device(q), device(k), device(v), device(out)));
    session.mutex.unlock();
    var start: u32 = 0;
    while (start < 1024) : (start += 128) {
        for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
            @memset(q.floats(), 1);
            // First global owner stages successfully; a later owner's real
            // query reservoir rejects. Neither owner may publish its candidate.
            if (start == 896 and i == 2) @memset(session.owners.items[2].queries.?.floats(), std.math.nan(f32));
            const request = contract.Attention{ .consumer_layer = @intCast(i), .owner_layer = l.owner, .query_start = start, .query_rows = 128, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = 32, .sliding_window = l.sliding_window, .skip_kv_write = l.owner != i, .scale = 1 };
            if (start == 896 and i == 3) {
                try std.testing.expectError(error.NonFiniteCompactionInput, session.hook().attention(request, device(q), device(k), device(v), device(out)));
            } else try session.hook().attention(request, device(q), device(k), device(v), device(out));
        }
    }
    try std.testing.expectEqual(first, session.owners.items[0].live.keys.handle);
    try std.testing.expectEqual(second, session.owners.items[2].live.keys.handle);
    try std.testing.expectEqual(@as(u64, 1), session.publication.epoch);
    try std.testing.expectEqual(@as(u64, 1), session.metrics.rejected_transactions);
    try std.testing.expectEqual(@as(u64, 64), session.metrics.status_readback_bytes);
    try std.testing.expect(!session.publication.pending);
    try std.testing.expect(session.failed);
    try std.testing.expectError(error.ContextSessionFailed, session.hook().attention(undefined, device(q), device(k), device(v), device(out)));
}

test "context Metal quality rejection retains the remainder only within reserved capacity" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    for ([_]u32{ 0, 1 }) |accepted_blocks| {
        const boundary: u32 = 1024 + accepted_blocks * 512;
        for ([_]bool{ true, false }) |headroom| {
            const config = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = if (headroom) boundary + 1 else boundary - accepted_blocks * 256, .keep_prefix_tokens = 0, .keep_recent_tokens = 512 }, boundary + 1, 128);
            var session = try Session.init(std.testing.allocator, config);
            defer session.deinit();
            var first = session.owners.items[0].live.keys.handle;
            var second = session.owners.items[2].live.keys.handle;
            var prior_readbacks: u64 = 0;
            var first_digest: u64 = undefined;
            var second_digest: u64 = undefined;
            var q = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
            defer q.deinit();
            var k = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
            defer k.deinit();
            var v = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
            defer v.deinit();
            var out = try Buffer.init(session.copies.runtime, 128 * 2 * 32 * 4);
            defer out.deinit();
            @memset(q.floats(), 0);
            @memset(v.floats(), 1);
            var start: u32 = 0;
            while (start < boundary) : (start += 128) {
                if (start == boundary - 128) {
                    first = session.owners.items[0].live.keys.handle;
                    second = session.owners.items[2].live.keys.handle;
                    prior_readbacks = session.metrics.status_readback_bytes;
                }
                @memset(k.floats(), 0);
                for (0..128 * 2) |row| k.floats()[row * 32] = if (start % 512 < 256) -1 else 1;
                for (config.layout.layers[0..config.layout.len], 0..) |layer, index| {
                    if (start == boundary - 128 and index == 3) {
                        first_digest = testPublishedDigest(&session.owners.items[0]);
                        second_digest = testPublishedDigest(&session.owners.items[2]);
                        // Earlier owner fits successfully; the later owner has
                        // finite held-out queries exposing missing positive keys.
                        // This is a quality rejection, not an invalid-input error.
                        const later = &session.owners.items[2];
                        const stride = later.geometry.training_rows + later.geometry.validation_rows;
                        @memset(later.queries.?.floats(), 0);
                        for (0..later.shape.kv_heads) |head| for (0..later.geometry.validation_rows) |row| {
                            later.queries.?.floats()[(head * stride + later.geometry.training_rows + row) * 32] = 1;
                        };
                    }
                    const request = contract.Attention{ .consumer_layer = @intCast(index), .owner_layer = layer.owner, .query_start = start, .query_rows = 128, .query_heads = layer.query_heads, .kv_heads = layer.kv_heads, .head_dim = 32, .sliding_window = layer.sliding_window, .skip_kv_write = layer.owner != index, .scale = 1 };
                    if (!headroom and start == boundary - 128 and index == 3) {
                        try std.testing.expectError(error.CompactionQualityRejected, session.hook().attention(request, device(q), device(k), device(v), device(out)));
                    } else try session.hook().attention(request, device(q), device(k), device(v), device(out));
                }
            }
            try std.testing.expectEqual(first, session.owners.items[0].live.keys.handle);
            try std.testing.expectEqual(second, session.owners.items[2].live.keys.handle);
            try std.testing.expectEqual(first_digest, testPublishedDigest(&session.owners.items[0]));
            try std.testing.expectEqual(second_digest, testPublishedDigest(&session.owners.items[2]));
            try std.testing.expectEqual(@as(u64, 1 + accepted_blocks), session.publication.epoch);
            try std.testing.expectEqual(@as(u64, 1), session.metrics.rejected_transactions);
            try std.testing.expectEqual(accepted_blocks, session.metrics.compacted_blocks);
            try std.testing.expectEqual(prior_readbacks + 96, session.metrics.status_readback_bytes);
            try std.testing.expect(!session.publication.pending);
            try std.testing.expectEqual(headroom, session.retain_exact_remainder);
            try std.testing.expectEqual(!headroom, session.failed);
            try std.testing.expectEqual(@as(u32, if (headroom) 1 else 0), session.metrics.exact_tail_fallbacks);
            if (!headroom) {
                try std.testing.expectError(error.ContextSessionFailed, session.hook().nextChunk(boundary, 1));
                continue;
            }
            try std.testing.expectEqual(@as(u32, 1), try session.hook().nextChunk(boundary, 1));
            for (config.layout.layers[0..config.layout.len], 0..) |layer, index| {
                const request = contract.Attention{ .consumer_layer = @intCast(index), .owner_layer = layer.owner, .query_start = boundary, .query_rows = 1, .query_heads = layer.query_heads, .kv_heads = layer.kv_heads, .head_dim = 32, .sliding_window = layer.sliding_window, .skip_kv_write = layer.owner != index, .scale = 1 };
                try session.hook().attention(request, device(q), device(k), device(v), device(out));
                for (out.floats()[0 .. 2 * 32]) |value| try std.testing.expectApproxEqAbs(@as(f32, 1), value, 1e-5);
            }
            try std.testing.expectEqual(boundary + 1, session.metrics.logical_tokens);
            try std.testing.expectEqual(boundary + 1 - accepted_blocks * 256, session.metrics.retained_rows);
            try std.testing.expectEqual(prior_readbacks + 96, session.metrics.status_readback_bytes); // No repeated fit.
        }
    }
}

test "context Metal session releases arena and scratch allocations across request lifetimes" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const config = try testConfig(.{ .mode = .attention_matching, .max_kv_tokens = 2048, .keep_prefix_tokens = 3, .keep_recent_tokens = 512 }, 1537, 1);
    // Keep a separate device handle alive while request runtimes are destroyed.
    // currentAllocatedSize covers Metal resources, unlike testing.allocator's
    // host metadata leak checks. This is still not a complete process-RSS audit.
    var monitor = try shared.Kernels.initSource(@embedFile("context_storage.metal"), "context_storage");
    defer monitor.deinit();
    var q = try Buffer.init(monitor.runtime, 2 * 32 * 4);
    defer q.deinit();
    var k = try Buffer.init(monitor.runtime, 2 * 32 * 4);
    defer k.deinit();
    var v = try Buffer.init(monitor.runtime, 2 * 32 * 4);
    defer v.deinit();
    var out = try Buffer.init(monitor.runtime, 2 * 32 * 4);
    defer out.deinit();
    @memset(q.floats(), 1);
    @memset(k.floats(), 0);
    @memset(v.floats(), 1);
    var baseline: u64 = 0;
    var peak: u64 = 0;
    var after: u64 = 0;
    for (0..9) |iteration| {
        {
            var session = try Session.init(std.testing.allocator, config);
            defer session.deinit();
            for (config.layout.layers[0..config.layout.len], 0..) |layer, index| {
                try session.hook().attention(.{ .consumer_layer = @intCast(index), .owner_layer = layer.owner, .query_start = 0, .query_rows = 1, .query_heads = layer.query_heads, .kv_heads = layer.kv_heads, .head_dim = 32, .sliding_window = layer.sliding_window, .skip_kv_write = layer.owner != index, .scale = 1 }, device(q), device(k), device(v), device(out));
                for (out.floats()) |value| try std.testing.expectEqual(@as(f32, 1), value);
            }
            peak = @max(peak, mr.currentAllocatedSize(monitor.runtime));
        }
        after = mr.currentAllocatedSize(monitor.runtime);
        if (iteration == 0) {
            baseline = after; // Warm the driver's pipeline caches first.
        } else try std.testing.expectEqual(baseline, after);
    }
    // Ensure the measurement actually observes the request allocations.
    try std.testing.expect(peak > baseline);
    std.debug.print("context_storage_lifecycle: measured_requests=8 baseline_metal_bytes={d} peak_metal_bytes={d} final_metal_bytes={d}\n", .{ baseline, peak, after });
}

test "context online session rejects selected payloads before allocating storage" {
    var selected = policy_mod.Policy{ .mode = .attention_matching, .max_kv_tokens = 4096 };
    try selected.compaction_targets.add(.{ .message_index = 0, .part_index = 0 });
    const plan = try selected.admit(8192, null, 1024, 1, 0);
    try std.testing.expectError(error.UnsupportedContextCombination, allocation(.{ .plan = plan, .layout = testLayout(), .max_query_rows = 8, .physical_budget_bytes = 0 }));
}
