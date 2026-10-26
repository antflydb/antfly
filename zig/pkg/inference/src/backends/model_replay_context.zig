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

//! Private post-ingestion model branch. Global owners lease the original packed
//! exact generation; sliding owners copy only the required seed and own their
//! continuation. The caller holds the sequence lock while constructing a branch
//! and drains the model producer before invoking its synchronous attention hook.
//! No sampler, grammar, output, usage or live sequence state is borrowed here.
//! This internal qualification route does not enable approximate serving.
const std = @import("std");
const policy = @import("../runtime/context_policy.zig");
const contract = @import("../runtime/kv/context_session.zig");
const views = @import("../runtime/kv/context_view.zig");
const storage = @import("packed_context_storage.zig");
const replay = @import("replay_context.zig");
const attention = @import("context_kernels.zig");
const sampling = @import("context_query_sampling.zig");
const query_protocol = @import("context_query_protocol.zig");
const shared = @import("metal_generated_kernels.zig");
const mr = @import("metal_runtime.zig");
const execution = @import("../execution_control.zig");
const execution_pool = @import("context_execution_pool.zig");
const Buffer = shared.Buffer;
const Ref = shared.Ref;
const absent = std.math.maxInt(u32);
var next_identity = std.atomic.Value(u64).init(1);

pub const Origin = struct {
    layer: u32,
    state: union(enum) { global: *storage.Generation, sliding: storage.Source },
    fn source(self: Origin) !storage.Source {
        return switch (self.state) {
            .global => |g| g.source(),
            .sliding => |s| s,
        };
    }
};
pub const Config = struct {
    /// An exact, private plan from prompt_tokens to logical_token_limit. The
    /// replay horizon is independent of the live request's output reservation.
    plan: policy.Plan,
    layout: policy.Layout,
    max_query_rows: u32,
    /// Experimental exact global replay attention. Null preserves the serial
    /// kernel. The predeclared row/split limits are admitted before allocation.
    global_attention_split: ?replay.SplitConfig = null,
    /// First logical position eligible for query sampling. The branch still
    /// replays every token from plan.prompt_tokens so generated-response
    /// studies can condition on their fixed study prompt without training on
    /// that prompt twice. Defaults to the first appended token.
    capture_start: ?u32 = null,
    /// Frozen source ranges and global quotas. Qualification and serving must
    /// construct this from the same compiled protocol. Null preserves the
    /// legacy single-branch diagnostic sampler only.
    query_sources: ?[]const query_protocol.Source = null,
    /// Borrowed from the outer request; it outlives live and private sessions.
    execution_lease: ?*execution_pool.Lease = null,
    /// Original sliding sources are borrowed device refs and carry no host
    /// allocator ownership. The outer request supplies the private branch's
    /// tracker explicitly; global owners additionally verify this binding.
    allocation_tracker: ?*shared.BufferAllocationTracker = null,
    physical_budget_bytes: usize,
    cancelled: ?*const std.atomic.Value(bool) = null,
    execution_control: ?execution.InferenceExecutionControl = null,
};
pub const Allocation = struct {
    /// Additional branch allocation, including conservatively charged control
    /// objects retained by global owners after the last branch closes.
    physical_bytes: usize = 0,
    owner_control_bytes: usize = 0,
    private_kv_bytes: usize = 0,
    query_bytes: usize = 0,
    query_mapping_bytes: usize = 0,
    descriptor_span_bytes: usize = 0,
    descriptor_head_bytes: usize = 0,
    descriptor_arena_bytes: usize = 0,
    replay_split_scratch_bytes: usize = 0,
    seed_copy_bytes: usize = 0,
    /// Pools, owner records and the two surviving kernel runtimes' pages.
    /// Original owners continue to own owner_control_bytes separately.
    sealed_bytes: usize = 0,
};
fn rounded(n: usize) !usize {
    return (try std.math.add(usize, n, std.heap.pageSize() - 1)) & ~(std.heap.pageSize() - 1);
}
pub const QueryShape = struct {
    training: ?sampling.Bounds,
    validation: ?sampling.Bounds,
    protocol: ?query_protocol.Plan,
    train_per_stratum: u32,
    held_per_stratum: u32,
    training_rows: u32,
    validation_rows: u32,
    vectors: u32,
    pub fn init(config: Config, layer: usize) !QueryShape {
        const shape = config.layout.layers[layer];
        if (shape.owner != layer or shape.kv_heads == 0) return error.InvalidContextLayout;
        var strata: u32 = 0;
        for (config.layout.layers[0..config.layout.len]) |consumer| if (consumer.owner == layer) {
            if (consumer.kv_heads != shape.kv_heads or consumer.head_dim != shape.head_dim or
                consumer.query_heads == 0 or consumer.query_heads % consumer.kv_heads != 0)
                return error.InvalidContextLayout;
            strata = try std.math.add(u32, strata, consumer.query_heads / consumer.kv_heads);
        };
        if (strata == 0 or strata > 128) return error.InvalidContextLayout;
        if (config.query_sources) |sources| {
            const protocol = try query_protocol.Plan.init(sources, strata);
            if (protocol.strata != strata) return error.InvalidQueryProtocol;
            for (sources) |source| if (source.active and
                (source.start < config.plan.prompt_tokens or source.end > config.plan.logical_token_limit))
                return error.InvalidQueryProtocol;
            const total = try std.math.add(u32, protocol.training_rows, protocol.validation_rows);
            if (total == 0 or total > query_protocol.max_training_rows + query_protocol.max_validation_rows)
                return error.InvalidQueryProtocol;
            return .{ .training = null, .validation = null, .protocol = protocol, .train_per_stratum = protocol.training_per_source_stratum, .held_per_stratum = protocol.validation_per_source_stratum, .training_rows = protocol.training_rows, .validation_rows = protocol.validation_rows, .vectors = total * shape.kv_heads };
        }
        const capture_start = config.capture_start orelse @as(u32, @intCast(config.plan.prompt_tokens));
        const training = try sampling.Bounds.init(capture_start, @intCast(config.plan.logical_token_limit), false);
        const validation = try sampling.Bounds.init(capture_start, @intCast(config.plan.logical_token_limit), true);
        const limits = @import("../runtime/kv/attention_matching.zig");
        const train = @min(limits.max_training_rows / strata, training.last_ordinal - training.first_ordinal + 1);
        const held = @min(limits.max_validation_rows / strata, validation.last_ordinal - validation.first_ordinal + 1);
        if (train == 0 or held == 0) return error.InsufficientContextQueries;
        return .{ .training = training, .validation = validation, .protocol = null, .train_per_stratum = train, .held_per_stratum = held, .training_rows = train * strata, .validation_rows = held * strata, .vectors = (train + held) * strata * shape.kv_heads };
    }
};
fn seedRows(config: Config, layer: usize) u32 {
    // One extra seed row is harmless and also supports a one-token window.
    return @intCast(@min(config.plan.prompt_tokens, config.layout.layers[layer].sliding_window));
}
fn tailRows(config: Config) u32 {
    return @intCast(config.plan.logical_token_limit - config.plan.prompt_tokens);
}
fn consumerCount(config: Config, owner_layer: usize) !u32 {
    var count: u32 = 0;
    for (config.layout.layers[0..config.layout.len]) |shape| {
        if (shape.owner == owner_layer) count = try std.math.add(u32, count, 1);
    }
    if (count == 0) return error.InvalidContextLayout;
    return count;
}
fn privateShape(config: Config, layer: usize) storage.Shape {
    const l = config.layout.layers[layer];
    return .{ .head_dim = l.head_dim, .kv_heads = l.kv_heads, .continuation_rows = tailRows(config), .max_retained_rows = seedRows(config, layer) + tailRows(config) };
}

/// Validate all source geometry and the complete additional peak before any GPU
/// allocation or lease. Original buffers and backend pipeline memory remain
/// charged by their owners; this budget must coexist with those reservations.
pub fn allocation(config: Config, origins: []const Origin) !Allocation {
    const result = try estimate(config);
    var cursor: usize = 0;
    for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
        if (l.owner != i) continue;
        if (cursor >= origins.len or origins[cursor].layer != i) return error.InvalidContextLayout;
        const origin = origins[cursor];
        const src = try origin.source();
        try src.validate();
        if (src.view.logical_tokens != config.plan.prompt_tokens or src.view.head_dim != l.head_dim or src.view.kv_heads != l.kv_heads)
            return error.InvalidContextLayout;
        if (cursor > 0 and src.view.epoch != (try origins[0].source()).view.epoch) return error.StaleContextView;
        if (src.keys.handle == src.values.handle) return error.AliasedContextStaging;
        for (origins[0..cursor]) |prior| {
            const other = try prior.source();
            for ([_]Ref{ src.keys, src.values }) |r| for ([_]Ref{ other.keys, other.values }) |o|
                if (r.handle == o.handle) return error.AliasedContextStaging;
        }
        if (l.sliding_window == 0) {
            if (origin.state != .global) return error.InvalidContextLayout;
            _ = try replay.allocation(src.view, tailRows(config), @intCast(config.plan.logical_token_limit));
        } else {
            if (origin.state != .sliding) return error.InvalidContextLayout;
            const end: u32 = @intCast(config.plan.prompt_tokens);
            if (!src.view.coversExact(end - seedRows(config, i), end)) return error.MissingContextHistory;
        }
        cursor += 1;
    }
    if (cursor != origins.len) return error.InvalidContextLayout;
    return result;
}

/// Same allocation formula as construction, without needing live buffers. This
/// permits whole-request admission before ingestion creates any device state.
pub fn estimate(config: Config) !Allocation {
    try config.plan.policy.validate();
    const p = config.plan;
    if (p.policy.mode != .exact or p.prompt_tokens == 0 or p.prompt_tokens >= p.logical_token_limit or
        p.logical_token_limit > p.context_limit or p.logical_token_limit > std.math.maxInt(u32) or
        p.logical_token_limit - p.prompt_tokens > @import("../runtime/kv/reference_replay.zig").max_additional_tokens or
        p.retained_row_limit != p.logical_token_limit or config.max_query_rows == 0 or config.max_query_rows > 8192 or
        config.layout.len == 0 or config.layout.len > 256) return error.InvalidReferenceReplay;
    if (config.capture_start) |start| if (start < p.prompt_tokens or start >= p.logical_token_limit)
        return error.InvalidReferenceReplay;
    const admitted = try p.policy.admit(p.context_limit, null, p.prompt_tokens, tailRows(config), 0);
    if (admitted.context_limit != p.context_limit or admitted.retained_row_limit != p.retained_row_limit) return error.InvalidReferenceReplay;
    var result = Allocation{};
    if (config.global_attention_split) |split| if (split.max_query_rows == 0 or
        split.max_query_rows > replay.split_max_query_rows or split.max_query_rows > config.max_query_rows or
        split.split_count < 2 or split.split_count > replay.split_max_splits)
        return error.InvalidReferenceReplay;
    var cursor: usize = 0;
    for (config.layout.layers[0..config.layout.len], 0..) |l, i| {
        if (l.owner > i or l.kv_heads == 0 or l.kv_heads > views.max_heads or l.query_heads == 0 or
            l.query_heads % l.kv_heads != 0 or l.head_dim == 0 or l.head_dim > 512 or l.head_dim % 2 != 0 or
            @as(u64, config.max_query_rows) * l.query_heads * l.head_dim > std.math.maxInt(u32) or
            @as(u64, config.max_query_rows) * l.query_heads * 32 > std.math.maxInt(u32)) return error.InvalidContextLayout;
        const owner = config.layout.layers[l.owner];
        if (owner.owner != l.owner or owner.head_dim != l.head_dim or owner.kv_heads != l.kv_heads or
            owner.sliding_window != l.sliding_window) return error.InvalidContextLayout;
        if (l.sliding_window == 0) if (config.global_attention_split) |split| {
            const bytes = try replay.splitScratchBytes(split.max_query_rows, l.query_heads, l.head_dim, split.split_count);
            result.replay_split_scratch_bytes = @max(result.replay_split_scratch_bytes, bytes);
        };
        if (l.owner != i) continue;
        cursor += 1;
        if (l.sliding_window == 0) {
            var heads: [views.max_heads]views.Head = undefined;
            var spans: [views.max_heads]views.Span = undefined;
            const prompt: u32 = @intCast(p.prompt_tokens);
            for (0..l.kv_heads) |h| {
                heads[h] = .{ .kv_start = try std.math.mul(u32, @intCast(h), prompt), .capacity_rows = prompt, .bias_start = 0, .bias_rows = 0, .span_start = @intCast(h), .span_count = 1 };
                spans[h] = .{ .physical_start = 0, .row_count = prompt, .logical_start = 0, .logical_count = prompt };
            }
            const base = views.View{ .version = views.abi_version, .epoch = 1, .logical_tokens = prompt, .capacity_rows = try std.math.mul(u32, prompt, l.kv_heads), .kv_heads = l.kv_heads, .head_dim = l.head_dim, .spans = spans[0..l.kv_heads], .heads = heads[0..l.kv_heads] };
            const size = try replay.allocation(base, tailRows(config), @intCast(p.logical_token_limit));
            result.physical_bytes = try std.math.add(usize, result.physical_bytes, size.physical_bytes);
            result.owner_control_bytes += size.owner_control_bytes;
            result.private_kv_bytes += 2 * try rounded(size.tail_bytes);
            const q = try QueryShape.init(config, i);
            // Device query pool + immutable map slice per consumer + host
            // position records. Queued frame commands must not observe a later
            // layer's host rewrite of one shared mapping.
            const vector_map_bytes = try std.math.mul(usize, @as(usize, q.vectors), 4);
            const map_bytes = try rounded(try std.math.mul(usize, vector_map_bytes, @as(usize, try consumerCount(config, i))));
            const vector_bytes = try std.math.mul(usize, try std.math.mul(usize, @as(usize, q.vectors), @as(usize, l.head_dim)), 4);
            const bytes = try std.math.add(usize, try rounded(vector_bytes), try std.math.add(usize, map_bytes, try rounded(vector_map_bytes)));
            result.query_mapping_bytes = try std.math.add(usize, result.query_mapping_bytes, map_bytes);
            result.query_bytes = try std.math.add(usize, result.query_bytes, bytes);
            result.physical_bytes = try std.math.add(usize, result.physical_bytes, bytes);
        } else {
            const rows = seedRows(config, i);
            const retained: [views.max_heads]u32 = @splat(rows);
            const size = try storage.allocation(privateShape(config, i), &.{.{ .logical_count = rows, .retained = retained[0..l.kv_heads] }});
            result.physical_bytes = try std.math.add(usize, result.physical_bytes, size.physical_bytes);
            result.private_kv_bytes += size.device_bytes;
            result.seed_copy_bytes += @as(usize, rows) * l.kv_heads * l.head_dim * 4;
            result.descriptor_span_bytes = try std.math.add(usize, result.descriptor_span_bytes, try std.math.mul(usize, @as(usize, l.kv_heads), @sizeOf(views.Span)));
            result.descriptor_head_bytes = try std.math.add(usize, result.descriptor_head_bytes, try std.math.mul(usize, @as(usize, l.kv_heads), @sizeOf(views.Head)));
        }
    }
    result.descriptor_span_bytes = try rounded(result.descriptor_span_bytes);
    result.descriptor_head_bytes = try rounded(result.descriptor_head_bytes);
    result.descriptor_arena_bytes = try std.math.add(usize, result.descriptor_span_bytes, result.descriptor_head_bytes);
    result.physical_bytes = try std.math.add(usize, result.physical_bytes, result.descriptor_arena_bytes);
    result.physical_bytes = try std.math.add(usize, result.physical_bytes, result.replay_split_scratch_bytes);
    // Copier dummy, attention dummy/status, persistent descriptor arenas and
    // the stable owner array. Driver/pipeline objects use backend admission.
    result.physical_bytes = try std.math.add(usize, result.physical_bytes, 3 * std.heap.pageSize() + try rounded(cursor * @sizeOf(Owner)));
    result.sealed_bytes = result.query_bytes + result.descriptor_arena_bytes +
        result.replay_split_scratch_bytes + 3 * std.heap.pageSize() + try rounded(cursor * @sizeOf(Owner));
    return result;
}

pub const QueryPool = struct {
    shape: QueryShape,
    queries: Buffer,
    mapping: Buffer,
    mapping_stride: usize,
    consumer_count: u32,
    positions: []u32,
    fn init(a: std.mem.Allocator, copier: *storage.Copier, config: Config, layer: usize) !QueryPool {
        const shape = try QueryShape.init(config, layer);
        var queries = try copier.core.buffer(@as(usize, shape.vectors) * config.layout.layers[layer].head_dim * 4);
        errdefer queries.deinit();
        const mapping_stride = @as(usize, shape.vectors) * 4;
        const consumers = try consumerCount(config, layer);
        var mapping = try copier.core.buffer(try std.math.mul(usize, mapping_stride, consumers));
        errdefer mapping.deinit();
        const positions = try a.alloc(u32, shape.vectors);
        @memset(positions, absent);
        return .{ .shape = shape, .queries = queries, .mapping = mapping, .mapping_stride = mapping_stride, .consumer_count = consumers, .positions = positions };
    }
    fn deinit(self: *QueryPool, a: std.mem.Allocator) void {
        self.queries.deinit();
        self.mapping.deinit();
        a.free(self.positions);
    }
    fn destination(shape: QueryShape, stratum_offset: u32, query_heads: u32, kv_heads: u32, position: u32, head: u32) !?u32 {
        if (kv_heads == 0 or query_heads == 0 or query_heads % kv_heads != 0 or head >= query_heads)
            return error.InvalidContextLayout;
        const groups = query_heads / kv_heads;
        const stratum = try std.math.add(u32, stratum_offset, head % groups);
        const slot = if (shape.protocol) |protocol| blk: {
            const selected = (try protocol.select(position, stratum)) orelse return null;
            break :blk @as(u32, if (selected.partition == .validation) shape.training_rows else 0) + selected.row;
        } else blk: {
            const partition = sampling.partition(position);
            const count = if (partition.held) shape.held_per_stratum else shape.train_per_stratum;
            const bounds = if (partition.held) shape.validation orelse return error.InvalidContextLayout else shape.training orelse return error.InvalidContextLayout;
            const sample = (try bounds.slot(partition.ordinal, count)) orelse return null;
            break :blk if (partition.held) shape.training_rows + stratum * count + sample else stratum * count + sample;
        };
        const stride = try std.math.add(u32, shape.training_rows, shape.validation_rows);
        return try std.math.add(u32, try std.math.mul(u32, head / groups, stride), slot);
    }
    fn capture(self: *QueryPool, copier: *storage.Copier, owner_runtime: ?*mr.RawMetalDecodeRuntime, consumer_index: u32, stratum_offset: u32, r: contract.Attention, q: Ref) !void {
        if (consumer_index >= self.consumer_count) return error.InvalidContextLayout;
        const byte_offset = try std.math.mul(usize, consumer_index, self.mapping_stride);
        const map_bytes = self.mapping.bytes[byte_offset..][0..self.mapping_stride];
        const map = std.mem.bytesAsSlice(u32, @as([]align(4) u8, @alignCast(map_bytes)));
        @memset(map, absent);
        const shape = self.shape;
        for (0..r.query_rows) |row| {
            const pos = r.query_start + @as(u32, @intCast(row));
            for (0..r.query_heads) |head| {
                const h: u32 = @intCast(head);
                const dest = (try destination(shape, stratum_offset, r.query_heads, r.kv_heads, pos, h)) orelse continue;
                if (dest >= map.len or self.positions[dest] != absent) return error.InvalidReferenceReplay;
                map[dest] = @intCast(row * r.query_heads + head);
                self.positions[dest] = pos;
            }
        }
        const mapping_ref = try self.mapping.ref().slice(byte_offset, self.mapping_stride);
        if (owner_runtime) |runtime|
            try copier.captureQueriesOnActiveRuntime(runtime, q, mapping_ref, map_bytes, self.queries.ref(), r.query_rows * r.query_heads, r.head_dim)
        else
            try copier.captureQueriesRef(q, mapping_ref, map_bytes, self.queries.ref(), r.query_rows * r.query_heads, r.head_dim);
    }
};
pub const QueryPartition = enum { training, validation };
pub const QueryPartitionView = struct {
    partition: QueryPartition,
    rows_per_head: u32,
    heads: u32,
    dim: u32,
    stride_rows: u32,
    row_offset: u32,
    queries: []const f32,
    positions: []const u32,

    pub fn queryHead(self: QueryPartitionView, head: usize) ![]const f32 {
        if (head >= @as(usize, self.heads)) return error.InvalidContextLayout;
        const start = (head * @as(usize, self.stride_rows) + @as(usize, self.row_offset)) * @as(usize, self.dim);
        return self.queries[start..][0 .. @as(usize, self.rows_per_head) * self.dim];
    }
    pub fn positionHead(self: QueryPartitionView, head: usize) ![]const u32 {
        if (head >= @as(usize, self.heads)) return error.InvalidContextLayout;
        const start = head * @as(usize, self.stride_rows) + @as(usize, self.row_offset);
        return self.positions[start..][0..@as(usize, self.rows_per_head)];
    }
};
/// Device-resident sealed query view for request-local campaign assembly.
/// `queries` and `positions` deliberately describe the complete QueryPool;
/// consumers use `row_offset` and `stride_rows` and must reject sliced refs.
/// The borrow expires when this Session is destroyed.
pub const DeviceQueryPartitionView = struct {
    session_identity: u64,
    owner_layer: u32,
    partition: QueryPartition,
    rows_per_head: u32,
    heads: u32,
    dim: u32,
    stride_rows: u32,
    row_offset: u32,
    queries: shared.Ref,
    positions: []const u32,
};
pub const Owner = struct {
    layer: u32,
    state: union(enum) { global: replay.Replay, sliding: storage.Generation, retired },
    queries: ?QueryPool = null,
    scale: ?f32 = null,
    softcap: ?f32 = null,
    fn deinit(self: *Owner, a: std.mem.Allocator) void {
        if (self.queries) |*q| q.deinit(a);
        switch (self.state) {
            .global => |*g| g.deinit(),
            .sliding => |*g| g.deinit(),
            .retired => {},
        }
    }
};
fn trackerForOrigins(origins: []const Origin, requested: ?*shared.BufferAllocationTracker) !?*shared.BufferAllocationTracker {
    var selected = requested;
    var have_selected = requested != null;
    for (origins) |origin| {
        const tracker = switch (origin.state) {
            .global => |generation| generation.keys.allocation_tracker,
            .sliding => continue,
        };
        if (!have_selected) {
            selected = tracker;
            have_selected = true;
        } else if (selected != tracker) {
            return error.ContextFrameOwnershipMismatch;
        }
    }
    return selected;
}
pub const Session = struct {
    allocator: std.mem.Allocator,
    config: Config,
    sizes: Allocation,
    identity: u64,
    copier: storage.Copier,
    attention: attention.Kernels,
    descriptor_spans: ?Buffer,
    descriptor_heads: ?Buffer,
    replay_split_scratch: ?Buffer,
    descriptor_span_offsets: [256]usize = @splat(0),
    descriptor_head_offsets: [256]usize = @splat(0),
    owners: []Owner,
    owner_indices: [256]u16 = @splat(0),
    consumer_stratum_offsets: [256]u32 = @splat(0),
    consumer_mapping_indices: [256]u32 = @splat(0),
    publication: views.Publication = .{},
    logical_tokens: u32,
    next_layer: u32 = 0,
    step_end: u32 = 0,
    failed: bool = false,
    sealed: bool = false,
    pending_frame_runtime: ?*mr.RawMetalDecodeRuntime = null,
    pending_start: u32 = 0,
    pending_end: u32 = 0,
    pending_rows: u32 = 0,
    pending_epoch: u64 = 0,
    pending_global_active: [256]bool = @splat(false),
    pending_global_tail_rows: [256]u32 = @splat(0),
    pending_sliding_active: [256]bool = @splat(false),
    pending_sliding_ready: [256]bool = @splat(false),
    pending_sliding_ingesting_empty: [256]bool = @splat(false),
    pending_global_status: [256]bool = @splat(false),
    pending_sliding_status: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    status_readback_bytes: u64 = 0,
    kv_readback_bytes: u64 = 0,
    serial_replay_attention_dispatches: u64 = 0,
    split_replay_attention_dispatches: u64 = 0,

    pub const AttentionDispatchCounts = struct {
        serial: u64 = 0,
        split: u64 = 0,
    };

    /// Qualification route proof. Counts are branch-local and remain valid
    /// after seal retires the underlying per-owner replay objects.
    pub fn attentionDispatchCounts(self: *const Session) AttentionDispatchCounts {
        return .{ .serial = self.serial_replay_attention_dispatches, .split = self.split_replay_attention_dispatches };
    }

    pub fn init(a: std.mem.Allocator, config: Config, origins: []const Origin) !Session {
        const sizes = try allocation(config, origins);
        if (sizes.physical_bytes > config.physical_budget_bytes) return error.ContextReplayBudgetExceeded;
        if (config.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (config.execution_control) |control| try control.check();
        var protection = if (config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        const allocation_tracker = try trackerForOrigins(origins, config.allocation_tracker);
        var copier = if (config.execution_lease) |lease|
            try storage.Copier.initBorrowedTracked(try lease.execution(.storage), allocation_tracker)
        else
            try storage.Copier.initTracked(allocation_tracker);
        errdefer copier.deinit();
        copier.core.cancelled = config.cancelled;
        var kernels = if (config.execution_lease) |lease|
            try attention.Kernels.initBorrowedTracked(try lease.execution(.attention), null, allocation_tracker)
        else
            try attention.Kernels.initTracked(false, allocation_tracker);
        errdefer kernels.deinit();
        kernels.core.cancelled = config.cancelled;
        var descriptor_spans: ?Buffer = if (sizes.descriptor_span_bytes > 0) try kernels.core.buffer(sizes.descriptor_span_bytes) else null;
        errdefer if (descriptor_spans) |*buffer| buffer.deinit();
        var descriptor_heads: ?Buffer = if (sizes.descriptor_head_bytes > 0) try kernels.core.buffer(sizes.descriptor_head_bytes) else null;
        errdefer if (descriptor_heads) |*buffer| buffer.deinit();
        var replay_split_scratch: ?Buffer = if (sizes.replay_split_scratch_bytes > 0)
            try kernels.core.buffer(sizes.replay_split_scratch_bytes)
        else
            null;
        errdefer if (replay_split_scratch) |*buffer| buffer.deinit();
        const owners = try a.alloc(Owner, origins.len);
        errdefer a.free(owners);
        var count: usize = 0;
        errdefer for (owners[0..count]) |*o| o.deinit(a);
        for (origins) |origin| {
            const layer = origin.layer;
            if (origin.state == .global) {
                var pool = try QueryPool.init(a, &copier, config, layer);
                errdefer pool.deinit(a);
                const branch_config = replay.Config{ .tail_capacity = tailRows(config), .position_limit = @intCast(config.plan.logical_token_limit), .physical_budget_bytes = config.physical_budget_bytes, .cancelled = config.cancelled, .execution_control = config.execution_control };
                const branch = if (config.execution_lease) |lease|
                    try replay.Replay.initBorrowedTracked(origin.state.global, branch_config, copier.core.allocation_tracker, try lease.execution(.replay))
                else
                    try replay.Replay.initTracked(origin.state.global, branch_config, copier.core.allocation_tracker);
                owners[count] = .{ .layer = layer, .state = .{ .global = branch }, .queries = pool };
            } else {
                const rows = seedRows(config, layer);
                const retained: [views.max_heads]u32 = @splat(rows);
                var plan = try storage.Plan.init(a, privateShape(config, layer), &.{.{ .logical_count = rows, .retained = retained[0..config.layout.layers[layer].kv_heads] }});
                var transferred = false;
                defer if (!transferred) plan.deinit();
                plan.descriptor.logical_tokens = @intCast(config.plan.prompt_tokens);
                for (plan.spans) |*span| span.logical_start = @as(u32, @intCast(config.plan.prompt_tokens)) - rows;
                var generation = try storage.Generation.init(&copier, plan, plan.sizes.physical_bytes, 0);
                transferred = true;
                errdefer generation.deinit();
                for (0..plan.heads.len) |head| try generation.copyExact(&copier, head, 0, origin.state.sliding);
                try generation.finish();
                owners[count] = .{ .layer = layer, .state = .{ .sliding = generation } };
            }
            count += 1;
        }
        var self = Session{ .allocator = a, .config = config, .sizes = sizes, .identity = next_identity.fetchAdd(1, .monotonic), .copier = copier, .attention = kernels, .descriptor_spans = descriptor_spans, .descriptor_heads = descriptor_heads, .replay_split_scratch = replay_split_scratch, .owners = owners, .logical_tokens = @intCast(config.plan.prompt_tokens) };
        var span_offset: usize = 0;
        var head_offset: usize = 0;
        for (owners, 0..) |owner, i| {
            var stratum_offset: u32 = 0;
            var mapping_index: u32 = 0;
            for (config.layout.layers[0..config.layout.len], 0..) |l, layer| if (l.owner == owner.layer) {
                self.owner_indices[layer] = @intCast(i);
                self.consumer_stratum_offsets[layer] = stratum_offset;
                self.consumer_mapping_indices[layer] = mapping_index;
                const groups = l.query_heads / l.kv_heads;
                stratum_offset = std.math.add(u32, stratum_offset, groups) catch return error.InvalidContextLayout;
                mapping_index += 1;
            };
            if (config.layout.layers[owner.layer].sliding_window > 0) {
                self.descriptor_span_offsets[owner.layer] = span_offset;
                self.descriptor_head_offsets[owner.layer] = head_offset;
                span_offset += config.layout.layers[owner.layer].kv_heads * @sizeOf(views.Span);
                head_offset += config.layout.layers[owner.layer].kv_heads * @sizeOf(views.Head);
            }
        }
        try self.check();
        return self;
    }
    pub fn deinit(self: *Session) void {
        if (self.config.execution_lease) |lease| lease.drainForBufferRelease() catch {};
        if (self.pending_frame_runtime != null) @panic("model replay destroyed with a pending Metal frame");
        for (self.owners) |*o| o.deinit(self.allocator);
        self.allocator.free(self.owners);
        if (self.descriptor_spans) |*buffer| buffer.deinit();
        if (self.descriptor_heads) |*buffer| buffer.deinit();
        if (self.replay_split_scratch) |*buffer| buffer.deinit();
        self.attention.deinit();
        self.copier.deinit();
        self.* = undefined;
    }
    pub fn hook(self: *Session) contract.Hook {
        return .{ .ctx = self, .identity = self.identity, .plan = self.config.plan, .vtable = &.{ .next_chunk = nextChunk, .attention = dispatch, .attention_framed = dispatchFramed } };
    }
    fn check(self: *Session) !void {
        if (self.failed) return error.ContextSessionFailed;
        if (self.sealed) return error.ContextReplaySealed;
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
        if (self.pending_frame_runtime != null) return error.ContextTransactionBusy;
        errdefer self.failed = true;
        var protection = if (self.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        try self.runImpl(null, r, ref(q), ref(k), ref(v), ref(out));
        try self.check();
    }
    fn dispatchFramed(ctx: *anyopaque, identity: u64, frame: contract.Frame, r: contract.Attention, q: contract.DeviceRef, k: contract.DeviceRef, v: contract.DeviceRef, out: contract.DeviceRef) !void {
        const self: *Session = @ptrCast(@alignCast(ctx));
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        if (identity != self.identity) return error.StaleContextView;
        try self.check();
        // Validate the range before registering a completion callback. A
        // rejected hook must never leave overflow-prone rollback metadata.
        const end = std.math.add(u32, r.query_start, r.query_rows) catch return error.InvalidContextStep;
        if (r.query_start != self.logical_tokens or r.query_rows == 0 or
            r.query_rows > self.config.max_query_rows or end > self.config.plan.logical_token_limit)
            return error.InvalidContextStep;
        const runtime: *mr.RawMetalDecodeRuntime = @ptrCast(@alignCast(frame.runtime));
        if (self.config.execution_lease) |lease| try lease.registerExternalRuntime(runtime);
        if (self.pending_frame_runtime == null) {
            if (self.next_layer != 0) return error.InvalidContextStep;
            self.pending_frame_runtime = runtime;
            self.pending_start = self.logical_tokens;
            self.pending_end = end;
            self.pending_rows = r.query_rows;
            self.pending_epoch = self.publication.epoch;
            @memset(self.pending_global_active[0..], false);
            @memset(self.pending_sliding_active[0..], false);
            @memset(self.pending_global_status[0..], false);
            self.pending_sliding_status = false;
            if (mr.termite_metal_decode_runtime_set_context_frame_completion(runtime, self, completeFrame) != 0) {
                self.clearPendingFrame();
                return error.ContextFrameOwnershipMismatch;
            }
        } else if (self.pending_frame_runtime.? != runtime or self.pending_start != r.query_start or self.pending_rows != r.query_rows) {
            return error.ContextFrameOwnershipMismatch;
        }
        errdefer self.failed = true;
        try self.runImpl(runtime, r, ref(q), ref(k), ref(v), ref(out));
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
        if (self.next_layer == 0) {
            self.step_end = end;
        } else if (end != self.step_end) return error.InvalidContextStep;
        for ([_]Ref{ q, k, v }) |input| if (input.handle == out.handle) return error.AliasedContextStaging;
        // Reject output aliases against *all* branch and leased state, including
        // other layers, before a dispatch can corrupt another owner or reader.
        for (self.owners) |o| {
            switch (o.state) {
                .global => |g| for ([_]Ref{ g.lease.source.keys, g.lease.source.values, g.keys.ref(), g.values.ref(), g.heads.ref(), g.status.ref() }) |owned| {
                    if (out.handle == owned.handle) return error.AliasedContextStaging;
                },
                .sliding => |g| if (out.handle == g.keys.handle or out.handle == g.values.handle) return error.AliasedContextStaging,
                .retired => return error.ContextReplaySealed,
            }
            if (o.queries) |pool| if (out.handle == pool.queries.handle or out.handle == pool.mapping.handle) return error.AliasedContextStaging;
        }
        const owner = &self.owners[self.owner_indices[r.consumer_layer]];
        if (owner.scale) |s| {
            if (s != r.scale or owner.softcap.? != r.logit_softcap) return error.InvalidContextLayout;
        }
        owner.scale = r.scale;
        owner.softcap = r.logit_softcap;
        switch (owner.state) {
            .global => |*g| {
                const owner_index = self.owner_indices[r.consumer_layer];
                const split_attention: ?replay.SplitAttention = if (self.config.global_attention_split) |split|
                    if (r.query_rows <= split.max_query_rows)
                        .{ .scratch = (self.replay_split_scratch orelse return error.InvalidReferenceReplay).ref(), .split_count = split.split_count }
                    else
                        null
                else
                    null;
                if (owner_runtime) |runtime| {
                    if (!self.pending_global_active[owner_index]) {
                        self.pending_global_active[owner_index] = true;
                        self.pending_global_tail_rows[owner_index] = g.tail_rows;
                    }
                    if (!r.skip_kv_write) try g.appendOnActiveRuntime(runtime, r.query_start, r.query_rows, k, v);
                    try g.attentionOnActiveRuntime(runtime, .{ .query_start = r.query_start, .query_rows = r.query_rows, .query_heads = r.query_heads, .scale = r.scale, .logit_softcap = r.logit_softcap, .split_attention = split_attention }, q, out);
                    self.pending_global_status[owner_index] = true;
                    try owner.queries.?.capture(&self.copier, runtime, self.consumer_mapping_indices[r.consumer_layer], self.consumer_stratum_offsets[r.consumer_layer], r, q);
                } else {
                    const before = g.status_readback_bytes;
                    defer self.status_readback_bytes +|= g.status_readback_bytes - before;
                    if (!r.skip_kv_write) try g.append(r.query_start, r.query_rows, k, v);
                    try g.attention(.{ .query_start = r.query_start, .query_rows = r.query_rows, .query_heads = r.query_heads, .scale = r.scale, .logit_softcap = r.logit_softcap, .split_attention = split_attention }, q, out);
                    try owner.queries.?.capture(&self.copier, null, self.consumer_mapping_indices[r.consumer_layer], self.consumer_stratum_offsets[r.consumer_layer], r, q);
                }
                if (split_attention != null)
                    self.split_replay_attention_dispatches +|= 1
                else
                    self.serial_replay_attention_dispatches +|= 1;
            },
            .sliding => |*g| {
                const owner_index = self.owner_indices[r.consumer_layer];
                if (owner_runtime) |runtime| {
                    if (!self.pending_sliding_active[owner_index]) {
                        self.pending_sliding_active[owner_index] = true;
                        self.pending_sliding_ready[owner_index] = g.ready;
                        self.pending_sliding_ingesting_empty[owner_index] = g.ingesting_empty;
                    }
                    if (!r.skip_kv_write) try g.appendOnActiveRuntime(&self.copier, &self.publication, runtime, r.query_rows, k, v);
                    const spans = std.mem.sliceAsBytes(g.plan.descriptor.spans);
                    const heads = std.mem.sliceAsBytes(g.plan.descriptor.heads);
                    const span_arena = self.descriptor_spans orelse return error.ContextDescriptorArenaRequired;
                    const head_arena = self.descriptor_heads orelse return error.ContextDescriptorArenaRequired;
                    const span_offset = self.descriptor_span_offsets[owner.layer];
                    const head_offset = self.descriptor_head_offsets[owner.layer];
                    if (spans.len > span_arena.logical_bytes - span_offset or heads.len > head_arena.logical_bytes - head_offset)
                        return error.ContextDescriptorArenaExceeded;
                    @memcpy(span_arena.bytes[span_offset..][0..spans.len], spans);
                    @memcpy(head_arena.bytes[head_offset..][0..heads.len], heads);
                    try self.attention.attentionOnActiveRuntime(runtime, .{ .view = g.plan.descriptor, .current_epoch = self.publication.epoch, .query_start = r.query_start, .query_rows = r.query_rows, .query_heads = r.query_heads, .sliding_window = r.sliding_window, .scale = r.scale, .logit_softcap = r.logit_softcap, .descriptors = .{ .spans = try span_arena.ref().slice(span_offset, spans.len), .heads = try head_arena.ref().slice(head_offset, heads.len) } }, q, g.keys.ref(), g.values.ref(), self.attention.core.dummy.ref(), out);
                    self.pending_sliding_status = true;
                } else {
                    if (!r.skip_kv_write) try g.append(&self.copier, &self.publication, r.query_rows, k, v);
                    const before = self.attention.status_readback_bytes;
                    defer self.status_readback_bytes +|= self.attention.status_readback_bytes - before;
                    try self.attention.attention(.{ .view = g.plan.descriptor, .current_epoch = self.publication.epoch, .query_start = r.query_start, .query_rows = r.query_rows, .query_heads = r.query_heads, .sliding_window = r.sliding_window, .scale = r.scale, .logit_softcap = r.logit_softcap }, q, g.keys.ref(), g.values.ref(), self.attention.core.dummy.ref(), out);
                }
            },
            .retired => return error.ContextReplaySealed,
        }
        self.next_layer += 1;
        if (self.next_layer == self.config.layout.len) {
            self.logical_tokens = end;
            self.next_layer = 0;
        }
    }
    fn rollbackPendingFrame(self: *Session) void {
        for (self.owners, 0..) |*owner, index| switch (owner.state) {
            .global => |*g| if (self.pending_global_active[index]) {
                g.tail_rows = self.pending_global_tail_rows[index];
            },
            .sliding => |*g| if (self.pending_sliding_active[index]) {
                if (g.plan.descriptor.logical_tokens > self.pending_start) {
                    for (g.plan.heads) |head| {
                        const last = &g.plan.spans[head.span_start + head.span_count - 1];
                        last.row_count -|= self.pending_rows;
                        last.logical_count -|= self.pending_rows;
                    }
                    g.plan.descriptor.logical_tokens = self.pending_start;
                }
                g.ready = self.pending_sliding_ready[index];
                g.ingesting_empty = self.pending_sliding_ingesting_empty[index];
                if (!g.ready) @memset(g.filled, false);
            },
            .retired => {},
        };
        for (self.owners) |*owner| if (owner.queries) |*pool| for (pool.positions) |*position| {
            if (position.* >= self.pending_start and position.* < self.pending_end)
                position.* = absent;
        };
        self.logical_tokens = self.pending_start;
        self.step_end = self.pending_start;
        self.next_layer = 0;
    }
    fn clearPendingFrame(self: *Session) void {
        self.pending_frame_runtime = null;
        self.pending_start = 0;
        self.pending_end = 0;
        self.pending_rows = 0;
        self.pending_epoch = 0;
        @memset(self.pending_global_active[0..], false);
        @memset(self.pending_sliding_active[0..], false);
        @memset(self.pending_global_status[0..], false);
        self.pending_sliding_status = false;
    }
    fn completeFrame(raw: ?*anyopaque, completed: c_int) callconv(.c) c_int {
        const self: *Session = @ptrCast(@alignCast(raw orelse return -1));
        if (!self.mutex.tryLock()) return -2;
        defer self.mutex.unlock();
        if (self.pending_frame_runtime == null) return -3;
        var numerical_ok = completed != 0;
        if (numerical_ok) {
            for (self.owners, 0..) |*owner, index| if (self.pending_global_status[index]) switch (owner.state) {
                .global => |*g| {
                    g.status_readback_bytes +|= 4;
                    self.status_readback_bytes +|= 4;
                    numerical_ok = numerical_ok and std.mem.bytesToValue(u32, g.status.bytes[0..4]) == 0;
                },
                else => numerical_ok = false,
            };
            if (self.pending_sliding_status) {
                self.attention.status_readback_bytes +|= 4;
                self.status_readback_bytes +|= 4;
                numerical_ok = numerical_ok and std.mem.bytesToValue(u32, self.attention.status.bytes[0..4]) == 0;
            }
            if (self.config.cancelled) |stop| numerical_ok = numerical_ok and !stop.load(.acquire);
            if (self.config.execution_control) |control| control.check() catch {
                numerical_ok = false;
            };
        }
        const structurally_complete = self.next_layer == 0 and self.logical_tokens == self.pending_end and self.publication.epoch == self.pending_epoch;
        if (!numerical_ok or !structurally_complete) {
            self.rollbackPendingFrame();
            self.failed = true;
            self.clearPendingFrame();
            return -4;
        }
        self.clearPendingFrame();
        return 0;
    }
    /// Completeness is explicit: a partially executed or failed branch cannot
    /// supply training/held-out pools to a publication transaction.
    pub fn finish(self: *Session) !void {
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        try self.check();
        if (self.pending_frame_runtime != null) return error.ContextTransactionBusy;
        try self.verifyComplete();
    }
    fn verifyComplete(self: *const Session) !void {
        if (self.next_layer != 0 or self.logical_tokens != self.config.plan.logical_token_limit) return error.IncompleteReferenceReplay;
        const capture_start = self.config.capture_start orelse @as(u32, @intCast(self.config.plan.prompt_tokens));
        for (self.owners) |owner| if (owner.queries) |pool| for (pool.positions) |pos|
            if (pos == absent or pos < capture_start or pos >= self.logical_tokens) return error.IncompleteReferenceReplay;
    }
    /// Finish GPU replay and retain only captured queries. Dropping every lease
    /// is required before an all-owner Batch can fit/publish a new generation.
    /// Closing this session later destroys the pools; their allocator/runtime
    /// therefore outlive every synchronous fit that borrows them.
    pub fn seal(self: *Session) !void {
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        try self.check();
        if (self.pending_frame_runtime != null) return error.ContextTransactionBusy;
        try self.verifyComplete();
        for (self.owners) |*owner| {
            switch (owner.state) {
                .global => |*g| g.deinit(),
                .sliding => |*g| g.deinit(),
                .retired => unreachable,
            }
            owner.state = .retired;
        }
        self.sealed = true;
    }
    pub fn sealedQueries(self: *const Session, owner_layer: u32) !*const QueryPool {
        if (self.failed) return error.ContextSessionFailed;
        if (!self.sealed) return error.IncompleteReferenceReplay;
        if (owner_layer >= self.config.layout.len or self.config.layout.layers[owner_layer].owner != owner_layer or
            self.config.layout.layers[owner_layer].sliding_window != 0) return error.InvalidContextLayout;
        if (self.owners[self.owner_indices[owner_layer]].queries) |*pool| return pool;
        return error.InvalidContextLayout;
    }
    /// Borrow one sealed sampling partition without copying it to host scratch.
    /// The view remains valid only while this private session remains alive.
    /// Capture tooling can stream each head directly to an evidence file and
    /// account the existing QueryPool allocation rather than hiding a second
    /// materialized query tensor in its peak.
    pub fn sealedQueryPartition(self: *const Session, owner_layer: u32, partition: QueryPartition) !QueryPartitionView {
        const pool = try self.sealedQueries(owner_layer);
        const l = self.config.layout.layers[owner_layer];
        const rows = switch (partition) {
            .training => pool.shape.training_rows,
            .validation => pool.shape.validation_rows,
        };
        const offset = switch (partition) {
            .training => 0,
            .validation => pool.shape.training_rows,
        };
        return .{ .partition = partition, .rows_per_head = rows, .heads = l.kv_heads, .dim = l.head_dim, .stride_rows = pool.shape.training_rows + pool.shape.validation_rows, .row_offset = offset, .queries = pool.queries.floats(), .positions = pool.positions };
    }
    /// Export the same sealed partition without a host query-vector view. The
    /// caller attaches its own campaign identity, model generation and branch;
    /// those are not properties of this private replay Session.
    pub fn sealedDeviceQueryPartition(self: *const Session, owner_layer: u32, partition: QueryPartition) !DeviceQueryPartitionView {
        const pool = try self.sealedQueries(owner_layer);
        const l = self.config.layout.layers[owner_layer];
        const rows = switch (partition) {
            .training => pool.shape.training_rows,
            .validation => pool.shape.validation_rows,
        };
        const offset = switch (partition) {
            .training => 0,
            .validation => pool.shape.training_rows,
        };
        const stride = try std.math.add(u32, pool.shape.training_rows, pool.shape.validation_rows);
        const vectors = try std.math.mul(u32, stride, l.kv_heads);
        const expected_bytes = try std.math.mul(usize, @as(usize, vectors), try std.math.mul(usize, l.head_dim, @sizeOf(f32)));
        const queries = pool.queries.ref();
        if (queries.offset != 0 or queries.bytes != expected_bytes or pool.positions.len != vectors)
            return error.InvalidContextLayout;
        return .{
            .session_identity = self.identity,
            .owner_layer = owner_layer,
            .partition = partition,
            .rows_per_head = rows,
            .heads = l.kv_heads,
            .dim = l.head_dim,
            .stride_rows = stride,
            .row_offset = offset,
            .queries = queries,
            .positions = pool.positions,
        };
    }
};

fn fixtureConfig() !Config {
    var layout = policy.Layout{ .len = 4 };
    for (layout.layers[0..4], 0..) |*l, i| l.* = .{ .owner = @intCast(i % 2), .sliding_window = if (i % 2 == 0) 4 else 0, .kv_heads = 2, .query_heads = 8, .head_dim = if (i % 2 == 0) 256 else 512, .rope_active_dim = 256, .rope_frequency_dim = 256, .rope_theta = 10000 };
    return .{ .plan = try (policy.Policy{}).admit(100, null, 9, 12, 0), .layout = layout, .max_query_rows = 3, .physical_budget_bytes = 0 };
}

test "context model replay rejects invalid horizons layouts and absent owners before allocation" {
    var config = try fixtureConfig();
    try std.testing.expectError(error.InvalidContextLayout, allocation(config, &.{}));
    config.plan.logical_token_limit = 10000;
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config, &.{}));
    config = try fixtureConfig();
    config.max_query_rows = 0;
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config, &.{}));
    config = try fixtureConfig();
    config.capture_start = @intCast(config.plan.prompt_tokens - 1);
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config, &.{}));
    config.capture_start = @intCast(config.plan.logical_token_limit);
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(config, &.{}));
    config = try fixtureConfig();
    config.layout.layers[0].owner = 1;
    try std.testing.expectError(error.InvalidContextLayout, allocation(config, &.{}));
}

test "context model replay rejects active protocol ranges outside replay horizon" {
    var config = try fixtureConfig();
    const sources = [_]query_protocol.Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 8, .end = 12, .active = true },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 8, .end = 12, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 12, .end = 20, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 8, .end = 12, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 12, .end = 20, .active = false },
    };
    config.query_sources = &sources;
    try std.testing.expectError(error.InvalidQueryProtocol, estimate(config));
}

test "context model replay maps heterogeneous consumer GQA strata without overlap" {
    const sources = [_]query_protocol.Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 100, .end = 356, .active = true },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 100, .end = 164, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 164, .end = 356, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 100, .end = 164, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 164, .end = 356, .active = false },
    };
    const protocol = try query_protocol.Plan.init(&sources, 6);
    const shape = QueryShape{
        .training = null,
        .validation = null,
        .protocol = protocol,
        .train_per_stratum = protocol.training_per_source_stratum,
        .held_per_stratum = protocol.validation_per_source_stratum,
        .training_rows = protocol.training_rows,
        .validation_rows = protocol.validation_rows,
        .vectors = (protocol.training_rows + protocol.validation_rows) * 2,
    };
    const positions = try std.testing.allocator.alloc(u32, shape.vectors);
    defer std.testing.allocator.free(positions);
    @memset(positions, absent);

    // The first consumer contributes four GQA strata and the second two. A
    // consumer ordinal (0, 1) would overlap strata 1..3; cumulative offsets
    // (0, 4) cover all six strata exactly once for each owner KV head.
    const consumers = .{
        .{ .query_heads = 8, .kv_heads = 2, .stratum_offset = 0 },
        .{ .query_heads = 4, .kv_heads = 2, .stratum_offset = 4 },
    };
    for (100..356) |position| inline for (consumers) |consumer| {
        for (0..consumer.query_heads) |head| {
            const dest = (try QueryPool.destination(
                shape,
                consumer.stratum_offset,
                consumer.query_heads,
                consumer.kv_heads,
                @intCast(position),
                @intCast(head),
            )) orelse continue;
            try std.testing.expect(dest < positions.len);
            try std.testing.expectEqual(absent, positions[dest]);
            positions[dest] = @intCast(position);
        }
    };
    for (positions) |position| try std.testing.expect(position != absent);
    try std.testing.expectError(
        error.InvalidQueryProtocol,
        QueryPool.destination(shape, 5, 4, 2, 100, 3),
    );
}

test "context model replay admission reserves immutable framed upload arenas" {
    var config = try fixtureConfig();
    const serial_sizes = try estimate(config);
    try std.testing.expectEqual(@as(usize, 0), serial_sizes.replay_split_scratch_bytes);
    config.global_attention_split = .{ .max_query_rows = 3, .split_count = 8 };
    const sizes = try estimate(config);
    const global_shape = try QueryShape.init(config, 1);
    const expected_maps = try rounded(@as(usize, global_shape.vectors) * 4 * 2);
    try std.testing.expectEqual(expected_maps, sizes.query_mapping_bytes);
    try std.testing.expectEqual(try rounded(2 * @sizeOf(views.Span)), sizes.descriptor_span_bytes);
    try std.testing.expectEqual(try rounded(2 * @sizeOf(views.Head)), sizes.descriptor_head_bytes);
    try std.testing.expectEqual(sizes.descriptor_span_bytes + sizes.descriptor_head_bytes, sizes.descriptor_arena_bytes);
    const expected_scratch = try replay.splitScratchBytes(3, 8, 512, 8);
    try std.testing.expectEqual(expected_scratch, sizes.replay_split_scratch_bytes);
    try std.testing.expectEqual(serial_sizes.physical_bytes + expected_scratch, sizes.physical_bytes);
    try std.testing.expectEqual(serial_sizes.sealed_bytes + expected_scratch, sizes.sealed_bytes);
    try std.testing.expect(sizes.physical_bytes >= sizes.query_mapping_bytes + sizes.descriptor_arena_bytes);
    config.global_attention_split = .{ .max_query_rows = 4, .split_count = 8 };
    try std.testing.expectError(error.InvalidReferenceReplay, estimate(config));
}

fn halves(b: Buffer) []f16 {
    return std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(b.bytes[0..b.logical_bytes])));
}
fn device(r: Ref) contract.DeviceRef {
    return .{ .handle = r.handle, .offset = r.offset, .bytes = r.bytes };
}

test "context Metal model replay isolates sliding state donors cancellation and retired global runtime" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    var producer = try storage.Copier.init();
    var producer_alive = true;
    defer if (producer_alive) producer.deinit();
    var plan = try storage.Plan.init(a, .{ .head_dim = 512, .kv_heads = 2, .continuation_rows = 1, .max_retained_rows = 10 }, &.{.{ .logical_count = 9, .retained = &.{ 9, 9 } }});
    var original = storage.Generation.init(&producer, plan, plan.sizes.physical_bytes, 0) catch |err| {
        plan.deinit();
        return err;
    };
    var original_alive = true;
    defer if (original_alive) original.deinit();
    @memset(original.keys.bytes, 0);
    @memset(original.values.bytes, 0);
    for (original.plan.heads, 0..) |h, head| for (0..9) |row| {
        @memset(halves(original.values)[(h.kv_start + row) * 512 ..][0..512], @floatFromInt(row + 100 + head * 1000));
    };
    @memset(original.filled, true);
    try original.finish();
    var seed_keys = try Buffer.init(producer.core.runtime, 4 * 2 * 256 * 2);
    defer seed_keys.deinit();
    var seed_values = try Buffer.init(producer.core.runtime, seed_keys.logical_bytes);
    defer seed_values.deinit();
    @memset(seed_keys.bytes, 0);
    for ([_]u32{ 7, 8, 5, 6 }, 0..) |position, physical| for (0..2) |head| {
        @memset(halves(seed_values)[(physical * 2 + head) * 256 ..][0..256], @floatFromInt(position + head * 1000));
    };
    const spans = [_]views.Span{
        .{ .physical_start = 2, .row_count = 2, .logical_start = 5, .logical_count = 2 },
        .{ .physical_start = 0, .row_count = 2, .logical_start = 7, .logical_count = 2 },
    };
    const origins = [_]Origin{
        .{ .layer = 0, .state = .{ .sliding = .{ .view = .{ .epoch = 1, .logical_tokens = 9, .capacity_rows = 4, .head_dim = 256, .kv_heads = 2, .spans = &spans }, .keys = seed_keys.ref(), .values = seed_values.ref() } } },
        .{ .layer = 1, .state = .{ .global = &original } },
    };
    var config = try fixtureConfig();
    const sizes = try allocation(config, &origins);
    try std.testing.expectEqual(@as(usize, 4 * 2 * 256 * 4), sizes.seed_copy_bytes);
    try std.testing.expectEqual(@as(usize, 0), sizes.replay_split_scratch_bytes);
    config.physical_budget_bytes = sizes.physical_bytes - 1;
    try std.testing.expectError(error.ContextReplayBudgetExceeded, Session.init(a, config, &origins));
    try std.testing.expect(!original.hasReplayReaders());
    config.physical_budget_bytes += 1;
    var branch = try Session.init(a, config, &origins);
    defer branch.deinit();
    try std.testing.expect(branch.replay_split_scratch == null);
    try std.testing.expectEqual(original.keys.handle, branch.owners[1].state.global.lease.source.keys.handle);
    try std.testing.expect(seed_keys.handle != branch.owners[0].state.sliding.keys.handle);
    var cancelled = std.atomic.Value(bool).init(false);
    config.cancelled = &cancelled;
    var sibling = try Session.init(a, config, &origins);
    defer sibling.deinit();
    cancelled.store(true, .release);
    try std.testing.expectError(error.Cancelled, sibling.hook().nextChunk(9, 3));
    try std.testing.expectEqual(@as(u32, 9), sibling.logical_tokens);
    try std.testing.expectError(error.IncompleteReferenceReplay, branch.finish());
    try std.testing.expectError(error.IncompleteReferenceReplay, branch.seal());
    try std.testing.expectError(error.IncompleteReferenceReplay, branch.sealedQueries(1));
    var stale = branch.hook();
    stale.identity +%= 1;
    try std.testing.expectError(error.StaleContextView, stale.nextChunk(9, 3));
    cancelled.store(false, .release);
    var alias_branch = try Session.init(a, config, &origins);
    const alias_runtime = alias_branch.copier.core.runtime;
    try mr.beginFrame(alias_runtime);
    const alias_layer = config.layout.layers[0];
    try std.testing.expectError(error.AliasedContextStaging, alias_branch.hook().attentionFramed(.{ .runtime = @ptrCast(alias_runtime) }, .{ .consumer_layer = 0, .owner_layer = alias_layer.owner, .query_start = 9, .query_rows = 3, .query_heads = alias_layer.query_heads, .kv_heads = alias_layer.kv_heads, .head_dim = alias_layer.head_dim, .sliding_window = alias_layer.sliding_window, .skip_kv_write = false, .scale = 0.125 }, device(seed_keys.ref()), device(seed_keys.ref()), device(seed_values.ref()), device(seed_keys.ref())));
    try std.testing.expectError(error.SubmissionFailed, mr.cancelFrame(alias_runtime));
    try std.testing.expect(alias_branch.pending_frame_runtime == null);
    try std.testing.expectEqual(@as(u32, 9), alias_branch.logical_tokens);
    alias_branch.deinit();
    const publication = views.Publication{};
    try std.testing.expectError(error.ContextReplayActive, original.append(&producer, &publication, 1, seed_keys.ref(), seed_values.ref()));
    original.deinit();
    original_alive = false;
    producer.deinit();
    producer_alive = false;
    // Global lease owns both Metal objects and host backing after both the
    // original Generation and its creating runtime have been destroyed.
    var q = try Buffer.init(branch.copier.core.runtime, 3 * 8 * 512 * 4);
    defer q.deinit();
    var k = try Buffer.init(branch.copier.core.runtime, 3 * 2 * 512 * 4);
    defer k.deinit();
    var v = try Buffer.init(branch.copier.core.runtime, k.logical_bytes);
    defer v.deinit();
    var out = try Buffer.init(branch.copier.core.runtime, q.logical_bytes);
    defer out.deinit();
    // A frame queues all model layers before submission. Each queued dispatch
    // must retain immutable q/v inputs and a distinct output until the frame
    // completes; reusing the host-visible buffers would make every layer see
    // the final layer's data and would not exercise the production lifetime.
    var frame_q: [4]?Buffer = @splat(null);
    var frame_v: [4]?Buffer = @splat(null);
    var frame_out: [4]?Buffer = @splat(null);
    defer for (&frame_q) |*buffer| if (buffer.*) |*value| value.deinit();
    defer for (&frame_v) |*buffer| if (buffer.*) |*value| value.deinit();
    defer for (&frame_out) |*buffer| if (buffer.*) |*value| value.deinit();
    for (0..config.layout.len) |layer| {
        frame_q[layer] = try Buffer.init(branch.copier.core.runtime, q.logical_bytes);
        frame_v[layer] = try Buffer.init(branch.copier.core.runtime, v.logical_bytes);
        frame_out[layer] = try Buffer.init(branch.copier.core.runtime, out.logical_bytes);
    }
    @memset(k.floats(), 0);
    for (0..4) |chunk| {
        const start: u32 = @intCast(9 + chunk * 3);
        try std.testing.expectEqual(@as(u32, 3), try branch.hook().nextChunk(start, 10));
        const framed = chunk == 0;
        if (framed) try mr.beginFrame(branch.copier.core.runtime);
        for (config.layout.layers[0..4], 0..) |l, layer| {
            const layer_q = if (framed) &frame_q[layer].? else &q;
            const layer_v = if (framed) &frame_v[layer].? else &v;
            const layer_out = if (framed) &frame_out[layer].? else &out;
            @memset(layer_q.floats(), @floatFromInt(layer + 1));
            for (0..3) |row| for (0..2) |head| {
                @memset(layer_v.floats()[(row * 2 + head) * l.head_dim ..][0..l.head_dim], @floatFromInt(start + row + l.owner * 100 + head * 1000));
            };
            const request = contract.Attention{ .consumer_layer = @intCast(layer), .owner_layer = l.owner, .query_start = start, .query_rows = 3, .query_heads = l.query_heads, .kv_heads = l.kv_heads, .head_dim = l.head_dim, .sliding_window = l.sliding_window, .skip_kv_write = layer != l.owner, .scale = 0.125 };
            if (framed)
                try branch.hook().attentionFramed(.{ .runtime = @ptrCast(branch.copier.core.runtime) }, request, device(layer_q.ref()), device(k.ref()), device(layer_v.ref()), device(layer_out.ref()))
            else
                try branch.hook().attention(request, device(layer_q.ref()), device(k.ref()), device(layer_v.ref()), device(layer_out.ref()));
            if (framed and layer + 1 != config.layout.len) continue;
            if (framed) {
                try std.testing.expect(branch.pending_frame_runtime != null);
                try mr.submitFrame(branch.copier.core.runtime);
                try mr.waitFrame(branch.copier.core.runtime);
                try std.testing.expect(branch.pending_frame_runtime == null);
            }
            if (framed) {
                for (config.layout.layers[0..4], 0..) |framed_layer, framed_index| for (0..3) |row| for (0..8) |head| {
                    const expected = @as(f32, @floatFromInt(framed_layer.owner * 100 + head / 4 * 1000)) + if (framed_layer.sliding_window == 0) @as(f32, @floatFromInt(start + row)) / 2 else @as(f32, @floatFromInt(start + row)) - 1.5;
                    for (frame_out[framed_index].?.floats()[(row * 8 + head) * framed_layer.head_dim ..][0..framed_layer.head_dim]) |value|
                        try std.testing.expectApproxEqAbs(expected, value, 0.0005);
                };
            } else for (0..3) |row| for (0..8) |head| {
                const expected = @as(f32, @floatFromInt(l.owner * 100 + head / 4 * 1000)) + if (l.sliding_window == 0) @as(f32, @floatFromInt(start + row)) / 2 else @as(f32, @floatFromInt(start + row)) - 1.5;
                for (layer_out.floats()[(row * 8 + head) * l.head_dim ..][0..l.head_dim]) |value|
                    try std.testing.expectApproxEqAbs(expected, value, 0.0005);
            };
        }
    }
    try branch.finish();
    try std.testing.expectEqual(@as(u32, 21), branch.logical_tokens);
    try std.testing.expectEqual(@as(u64, 0), branch.kv_readback_bytes);
    try std.testing.expectEqual(Session.AttentionDispatchCounts{ .serial = 8, .split = 0 }, branch.attentionDispatchCounts());
    var consumer_two_values: usize = 0;
    var consumer_four_values: usize = 0;
    for (branch.owners[1].queries.?.queries.floats()) |value| {
        if (value == 2) consumer_two_values += 1 else if (value == 4) consumer_four_values += 1 else return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(consumer_two_values, consumer_four_values);
    try std.testing.expectEqual(branch.owners[1].queries.?.queries.floats().len, consumer_two_values + consumer_four_values);
    try std.testing.expectError(error.InvalidContextStep, branch.hook().nextChunk(21, 1));
    // Cancellation before mutation permits reuse; cancellation between layers
    // poisons the partially executed model step even if the flag is cleared.
    cancelled.store(false, .release);
    const sliding = config.layout.layers[0];
    @memset(v.floats(), 0);
    try mr.beginFrame(sibling.copier.core.runtime);
    try sibling.hook().attentionFramed(.{ .runtime = @ptrCast(sibling.copier.core.runtime) }, .{ .consumer_layer = 0, .owner_layer = 0, .query_start = 9, .query_rows = 3, .query_heads = sliding.query_heads, .kv_heads = sliding.kv_heads, .head_dim = sliding.head_dim, .sliding_window = sliding.sliding_window, .skip_kv_write = false, .scale = 0.125 }, device(q.ref()), device(k.ref()), device(v.ref()), device(out.ref()));
    cancelled.store(true, .release);
    try std.testing.expectError(error.SubmissionFailed, mr.cancelFrame(sibling.copier.core.runtime));
    try std.testing.expect(sibling.pending_frame_runtime == null);
    try std.testing.expectEqual(@as(u32, 9), sibling.logical_tokens);
    cancelled.store(false, .release);
    try std.testing.expectError(error.ContextSessionFailed, sibling.finish());
    try branch.finish();
    try branch.seal();
    const pool = try branch.sealedQueries(1);
    for (pool.queries.floats()) |value| try std.testing.expect(value == 2 or value == 4);
    const training = try branch.sealedQueryPartition(1, .training);
    const validation = try branch.sealedQueryPartition(1, .validation);
    const device_training = try branch.sealedDeviceQueryPartition(1, .training);
    const device_validation = try branch.sealedDeviceQueryPartition(1, .validation);
    try std.testing.expectEqual(pool.shape.training_rows, training.rows_per_head);
    try std.testing.expectEqual(pool.shape.validation_rows, validation.rows_per_head);
    try std.testing.expectEqual(@as(usize, training.rows_per_head * training.dim), (try training.queryHead(0)).len);
    try std.testing.expectEqual(@as(usize, validation.rows_per_head), (try validation.positionHead(0)).len);
    try std.testing.expectEqualSlices(u32, pool.positions[0..pool.shape.training_rows], try training.positionHead(0));
    try std.testing.expectEqualSlices(u32, pool.positions[pool.shape.training_rows .. pool.shape.training_rows + pool.shape.validation_rows], try validation.positionHead(0));
    try std.testing.expectEqual(branch.identity, device_training.session_identity);
    try std.testing.expectEqual(@as(u32, 1), device_training.owner_layer);
    try std.testing.expectEqual(pool.queries.handle, device_training.queries.handle);
    try std.testing.expectEqual(@as(usize, 0), device_training.queries.offset);
    try std.testing.expectEqual(pool.queries.logical_bytes, device_training.queries.bytes);
    try std.testing.expectEqualSlices(u32, pool.positions, device_training.positions);
    try std.testing.expectEqual(pool.shape.training_rows, device_validation.row_offset);
    try std.testing.expectEqual(pool.queries.logical_bytes, device_validation.queries.bytes);
    try std.testing.expectError(error.InvalidContextLayout, training.queryHead(training.heads));
    try std.testing.expectError(error.ContextReplaySealed, branch.hook().nextChunk(21, 1));
    try std.testing.expectError(error.ContextReplaySealed, branch.seal());
}
