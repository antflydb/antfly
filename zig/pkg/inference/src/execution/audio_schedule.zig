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

// A bounded host instruction tape. Prepared plans retain model-owned bindings
// and an admitted activation arena, never request tensors, allocators, or pins.
// Replay binds current inputs and frees temporary wrappers at last use.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const memory = @import("ml").graph.passes.memory;
const CT = ops.CT;
/// Two stride-two convolution stages produce one row per four input frames.
pub fn subsampledRows(frames: usize) usize {
    return frames / 4 + @intFromBool(frames % 4 != 0);
}

/// Ordered inputs shared by audio projection and workspace admission.
pub const ExecutionInputs = struct {
    pub const hidden = 0;
    pub const relative = 1;
    pub const valid = 2;
    pub const segment_start = 3;
    pub const segment_valid = 4;
    pub const scales = 5;
    pub const scale_count = 12;
    pub const count = scales + scale_count;

    pub const Metadata = struct {
        shape: []const i64,
        elements: usize,

        pub fn f32Bytes(self: Metadata) !usize {
            var element_count: usize = 1;
            for (self.shape) |extent| {
                const width = std.math.cast(usize, extent) orelse return error.InvalidShape;
                element_count = try std.math.mul(usize, element_count, width);
            }
            if (element_count != self.elements) return error.InvalidShape;
            return std.math.mul(usize, element_count, @sizeOf(f32));
        }
    };

    pub fn describe(shapes: *[count][2]i64, rows: usize, width: usize, context_left: usize, heads: usize) ![count]Metadata {
        const row_dim = std.math.cast(i64, rows) orelse return error.InvalidPreparedAudioRecipe;
        const hidden_dim = std.math.cast(i64, width) orelse return error.InvalidPreparedAudioRecipe;
        const context_dim = std.math.cast(i64, context_left) orelse return error.InvalidPreparedAudioRecipe;
        if (heads == 0 or width % heads != 0) return error.InvalidPreparedAudioRecipe;
        const head_dim = width / heads;
        shapes[hidden] = .{ row_dim, hidden_dim };
        shapes[relative] = .{ context_dim, hidden_dim };
        var inputs: [count]Metadata = undefined;
        inputs[hidden] = .{ .shape = &shapes[hidden], .elements = try std.math.mul(usize, rows, width) };
        inputs[relative] = .{ .shape = &shapes[relative], .elements = try std.math.mul(usize, context_left, width) };
        for (valid..count) |slot| {
            const elements = if (slot < scales) rows else head_dim;
            shapes[slot][0] = @intCast(elements);
            inputs[slot] = .{ .shape = shapes[slot][0..1], .elements = elements };
        }
        return inputs;
    }
};

/// Copied operation geometry only. The actual Stage certifies the model and
/// rebuilds mutable backend policy/learned-layout requests after every yield.
pub const AudioRecipe = struct {
    source_bf16: bool = false,
    gradient_clipping_bits: u32 = 0,
    frames: [4]usize = @splat(0),
    count: u3,
    hidden: usize,
    output_hidden: usize,
    intermediate: usize,
    blocks: usize,
    heads: usize,
    mel_bins: usize,
    conv_kernel: usize,
    norm_eps_bits: u32,
    residual_bits: u32,
    chunk: usize,
    context_left: usize,
    logit_cap_bits: u32,
    invalid_logits_bits: u32,
    exclude_farthest: bool,
    frontend_device: bool,
    force_dft: bool,
    force_unfused: bool,
    host_ops_empty: bool,
    graph_allowed: bool,
    packed_layers: u16,
    depthwise_cached: bool = false,

    pub fn validate(self: AudioRecipe) !void {
        if (self.count == 0 or self.count > 4 or self.hidden == 0 or self.heads == 0 or self.hidden % self.heads != 0 or
            self.output_hidden == 0 or self.intermediate == 0 or self.blocks != 12 or self.mel_bins != 128 or
            self.context_left == 0 or self.packed_layers >> 12 != 0) return error.InvalidPreparedAudioRecipe;
        for (self.frames[0..self.count]) |frames| if (frames == 0) return error.InvalidPreparedAudioRecipe;
        for (self.frames[self.count..]) |frames| if (frames != 0) return error.InvalidPreparedAudioRecipe;
        for ([_]usize{ self.hidden, self.output_hidden, self.context_left, try self.rows() }) |extent|
            if (std.math.cast(i64, extent) == null) return error.InvalidPreparedAudioRecipe;
    }
    pub fn rows(self: AudioRecipe) !usize {
        return self.rowsAndSegments(null);
    }

    pub fn rowsAndSegments(self: AudioRecipe, segments: ?[]usize) !usize {
        if (self.count > self.frames.len) return error.InvalidPreparedAudioRecipe;
        if (segments) |output| if (output.len != self.count) return error.InvalidPreparedAudioRecipe;
        var total: usize = 0;
        for (self.frames[0..self.count], 0..) |frames, i| {
            const span = subsampledRows(frames);
            if (segments) |output| output[i] = span;
            total = try std.math.add(usize, total, span);
        }
        return total;
    }
    /// Identical to the actual execution key; singleton subsampling has no
    /// segments and therefore does not enter the CUDA learned execution route.
    pub fn key(self: AudioRecipe, segments: []const usize, start: usize, end: usize) u64 {
        var hash = std.hash.Wyhash.init(0x616e74666c796175);
        hash.update(std.mem.sliceAsBytes(segments));
        std.hash.autoHash(&hash, .{ start, end });
        std.hash.autoHash(&hash, .{ self.hidden, self.intermediate, self.heads, self.conv_kernel, self.norm_eps_bits, self.residual_bits, self.chunk, self.context_left, self.logit_cap_bits, self.invalid_logits_bits, self.exclude_farthest });
        if (self.source_bf16) {
            hash.update("bf16-source-audio-v1");
            std.hash.autoHash(&hash, self.gradient_clipping_bits);
        }
        return hash.final();
    }
    pub fn frontendKey(buffer: []u8, kind: []const u8, fft: usize, frame: usize, mel: usize) ![]const u8 {
        return std.fmt.bufPrint(buffer, "audio_tower.prepared.frontend.f32.v1.{s}.{d}.{d}.{d}", .{ kind, fft, frame, mel });
    }
    pub fn depthwiseWeightName(buffer: []u8, layer: usize) ![]const u8 {
        if (layer >= 12) return error.InvalidPreparedAudioRecipe;
        return std.fmt.bufPrint(buffer, "a.blk.{d}.conv_dw.weight", .{layer});
    }
    pub fn depthwiseKey(buffer: []u8, name: []const u8, kernel: usize, hidden: usize) ![]const u8 {
        return std.fmt.bufPrint(buffer, "audio_tower.prepared.depthwise.v1.{s}.{d}.{d}", .{ name, kernel, hidden });
    }
    /// Actual accepted source layouts, shared before cold quota and before
    /// the original CPU transpose/upload. No numerical work or allocation.
    pub fn validateDepthwiseSource(shape: []const i64, kernel: usize, hidden: usize, allow_hidden_3d: bool) !usize {
        if (shape.len < 2 or kernel == 0 or hidden == 0) return error.InvalidTensorShape;
        const elements = try std.math.mul(usize, kernel, hidden);
        var count: usize = 1;
        for (shape) |extent| {
            if (extent <= 0) return error.InvalidTensorShape;
            count = try std.math.mul(usize, count, std.math.cast(usize, extent) orelse return error.InvalidTensorShape);
        }
        if (count != elements) return error.InvalidTensorShape;
        const k = std.math.cast(i64, kernel) orelse return error.InvalidTensorShape;
        const h = std.math.cast(i64, hidden) orelse return error.InvalidTensorShape;
        if (allow_hidden_3d and shape.len == 3 and shape[0] == h and shape[1] == 1 and shape[2] == k) return elements;
        if ((shape[0] == k and shape[1] == h) or (shape[0] == h and shape[1] == k)) return elements;
        return error.InvalidTensorShape;
    }
    pub const Constant = enum { relative, ones, query_scale, relative_key, depthwise };
    pub fn towerKey(self: AudioRecipe, buffer: []u8, kind: AudioRecipe.Constant, layer: usize) ![]const u8 {
        if (self.source_bf16) {
            var historical = self;
            historical.source_bf16 = false;
            var scratch: [192]u8 = undefined;
            const historical_key = try historical.towerKey(&scratch, kind, layer);
            return std.fmt.bufPrint(buffer, "{s}.bf16-source-v1", .{historical_key});
        }
        return switch (kind) {
            .relative => std.fmt.bufPrint(buffer, "audio_tower.prepared.relative.v1.{d}.{d}", .{ self.context_left, self.hidden }),
            .ones => std.fmt.bufPrint(buffer, "audio_tower.prepared.ones.v1.{d}", .{self.output_hidden}),
            .query_scale => std.fmt.bufPrint(buffer, "audio_tower.prepared.query_scale.v1.{d}.{d}", .{ layer, self.hidden / self.heads }),
            .relative_key => std.fmt.bufPrint(buffer, "audio_tower.prepared.relative_key.v1.{d}.{d}.{d}", .{ layer, self.context_left, self.hidden }),
            .depthwise => blk: {
                var name: [128]u8 = undefined;
                break :blk depthwiseKey(buffer, try depthwiseWeightName(&name, layer), self.conv_kernel, self.hidden);
            },
        };
    }
    pub fn qkvWeightName(buffer: []u8, layer: usize, component: usize) ![]const u8 {
        const names = [_][]const u8{ "q_proj", "k_proj", "v_proj" };
        if (layer >= 12 or component >= names.len) return error.InvalidPreparedAudioRecipe;
        return std.fmt.bufPrint(buffer, "audio_tower.layers.{d}.self_attn.{s}.linear.weight", .{ layer, names[component] });
    }
};
pub const retained_plan_capacity = 16;

pub const AudioFramePlan = struct { layers: usize = 1, in_flight: usize = 1 };
pub fn audioFramePlan(rows: usize, hidden: usize, intermediate: usize) AudioFramePlan {
    if (rows == 0 or hidden == 0 or intermediate == 0) return .{};
    const floats = std.math.add(usize, std.math.mul(usize, hidden, 12) catch return .{}, std.math.mul(usize, intermediate, 4) catch return .{}) catch return .{};
    const bytes = std.math.mul(usize, std.math.mul(usize, rows, floats) catch return .{}, 4) catch return .{};
    // At most one submitted group and one encoding group. Views and activation
    // buffers for both fit inside the existing 768-MiB audio reservation.
    const group = @min(@as(usize, 4), (384 * 1024 * 1024) / (std.math.mul(usize, bytes, 2) catch return .{}));
    return if (group == 0) .{} else .{ .layers = group, .in_flight = 2 };
}

/// Identifies a learned schedule from logical geometry and alias relationships.
/// Tensor addresses are compared for aliasing and never enter the cache key.
pub const ScheduleKey = struct {
    hash: std.hash.Wyhash,

    pub fn init(base: u64, policy: u8, input_count: usize) ScheduleKey {
        var hash = std.hash.Wyhash.init(base);
        hash.update(std.mem.asBytes(&policy));
        hash.update(std.mem.asBytes(&input_count));
        return .{ .hash = hash };
    }

    pub fn addInput(self: *ScheduleKey, shape: []const i32, value: CT, prior: []const CT) void {
        self.hash.update(std.mem.asBytes(&shape.len));
        self.hash.update(std.mem.sliceAsBytes(shape));
        for (prior) |previous| {
            const same = previous == value;
            self.hash.update(std.mem.asBytes(&same));
        }
    }

    pub fn finish(self: *ScheduleKey) u64 {
        return self.hash.final();
    }
};

pub const capacity = 2048;
pub const instruction_capacity = 1024;
pub const Constant = struct { id: u16, model: bool, name: [256]u8, len: u16, required: bool = true, bound: ?CT = null };
pub const Plan = struct {
    key: u64 = 0,
    stamp: u64 = 0,
    warmed: bool = false,
    ready: bool = false,
    disabled: bool = false,
    replays: u64 = 0,
    eager_ns: u64 = 0,
    replay_ns: u64 = 0,
    selection: Selection = .{},
    storage: @import("prepared_tensor_storage.zig").Storage = .{ .host_bytes = @sizeOf(Plan) },
    metadata_bytes: usize = @sizeOf(Plan),
    metadata_views: ?[]?CT = null,
    retry_after: u64 = 0,
    retry_delay: u8 = 1,
    workspaces: []?CT = &.{},
    arena: ?CT = null,
    arena_charge: ?*@import("resource_retirement.zig").Charge = null,
    arena_size: usize = 0,
    input_workspace_bytes: usize = 0,
    arena_recipe: ArenaLayout = .{},
    input_workspace_bank: ?CT = null,
    input_workspace_charge: ?*@import("resource_retirement.zig").Charge = null,
    prepared: bool = false,
    bindings_ready: bool = false,
    linear_only: bool = false,
    nodes: []Node = &.{},
    count: usize = 0,
    constants: []Constant = &.{},
    constant_count: usize = 0,
    last_use: [capacity]u16 = @splat(0),
    next_id: u16 = 0,
    output: u16 = 0,
    /// Exact retained request for the learned schedule. Publication happens
    /// after every array is allocated so a failed reservation can be retried.
    pub fn learnedMetadataBytes(self: *const Plan) !usize {
        if (self.metadata_views != null or self.count > self.nodes.len or
            self.count > instruction_capacity or self.constant_count > self.constants.len) return error.AudioScheduleInvalid;
        const nodes = try std.math.mul(usize, self.count, @sizeOf(Node) + 2 * @sizeOf(?CT));
        const constants = try std.math.mul(usize, self.constant_count, @sizeOf(Constant));
        return std.math.add(usize, nodes, constants);
    }

    pub fn freezeLearnedMetadata(self: *Plan, reserved_bytes: usize) !void {
        const bytes = try self.learnedMetadataBytes();
        if (reserved_bytes != bytes) return error.AudioScheduleInvalid;
        const metadata_bytes = try std.math.add(usize, self.metadata_bytes, bytes);
        const host_bytes = try std.math.add(usize, self.storage.host_bytes, bytes);
        const a = std.heap.c_allocator;
        const nodes = try a.dupe(Node, self.nodes[0..self.count]);
        errdefer a.free(nodes);
        const constants = try a.dupe(Constant, self.constants[0..self.constant_count]);
        errdefer a.free(constants);
        const views = try a.alloc(?CT, 2 * self.count);
        @memset(views, null);

        self.nodes = nodes;
        self.constants = constants;
        self.storage.outputs = views[0..self.count];
        self.workspaces = views[self.count..];
        self.metadata_views = views;
        self.metadata_bytes = metadata_bytes;
        self.storage.host_bytes = host_bytes;
    }

    pub fn chargedBackendBytes(self: *const Plan) usize {
        var bytes = self.storage.backend_bytes;
        for ([_]?*@import("resource_retirement.zig").Charge{ self.arena_charge, self.input_workspace_charge }) |charge| {
            if (charge) |owner| bytes +|= owner.cacheBackendBytes();
        }
        return bytes;
    }

    /// Caller owns residency exclusion and has destroyed all published views.
    /// The logical cache slot can retire while the SDK retains the charge.
    pub fn retireFundedArenasLocked(self: *Plan, cache: *@import("../runtime/tier/cache.zig").SharedCache, arena_bytes: usize, scratch_bytes: usize) void {
        std.debug.assert(self.arena == null and self.input_workspace_bank == null);
        for ([_]*?*@import("resource_retirement.zig").Charge{ &self.arena_charge, &self.input_workspace_charge }, [_]usize{ arena_bytes, scratch_bytes }) |slot, bytes| {
            const charge = slot.* orelse continue;
            slot.* = null;
            cache.noteLogicalPhysicalOwnerRemovalForTier(.backend, bytes);
            charge.deferCacheCreatorDrop() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
    }

    pub fn deinit(self: *Plan) void {
        self.dropPrepared();
        if (self.metadata_views) |views| {
            std.heap.c_allocator.free(self.nodes);
            std.heap.c_allocator.free(self.constants);
            std.heap.c_allocator.free(views);
            self.nodes = &.{};
            self.constants = &.{};
            self.storage.outputs = &.{};
            self.workspaces = &.{};
            self.metadata_views = null;
            self.count = 0;
            self.constant_count = 0;
        }
    }
    pub fn dropPrepared(self: *Plan) void {
        if (self.storage.drop_ct) |drop| {
            for (self.constants[0..self.constant_count]) |*c| if (c.bound) |ct| {
                drop(ct);
                c.bound = null;
            };
            self.storage.deinit();
            for (self.workspaces) |*workspace| if (workspace.*) |ct| {
                drop(ct);
                workspace.* = null;
            };
            if (self.input_workspace_bank) |ct| drop(ct);
            if (self.arena) |ct| drop(ct);
        }
        self.arena = null;
        self.input_workspace_bytes = 0;
        self.input_workspace_bank = null;
    }
    /// Assign non-overlapping lifetimes to linear outputs. Escaping values
    /// and their view ancestors remain independently owned by the caller.
    pub fn layout(self: *Plan) !usize {
        if (self.count > self.nodes.len or self.count > instruction_capacity or self.output >= capacity) return error.AudioScheduleInvalid;
        var ends: [capacity]u32 = undefined;
        for (self.last_use, &ends) |end, *target| target.* = end;
        // The returned tensor and every backing view ancestor outlive replay.
        const escapes = std.math.maxInt(u32);
        ends[self.output] = escapes;
        var sources: [capacity]u32 = @splat(@import("ml").graph.null_node);
        for (self.nodes[0..self.count]) |*node| {
            if (node.output >= capacity or node.input_count > node.inputs.len) return error.AudioScheduleInvalid;
            for (node.inputs[0..node.input_count]) |input| if (input >= capacity) return error.AudioScheduleInvalid;
            if (node.args == .sliceLastDim) sources[node.output] = node.args.sliceLastDim.tensor;
        }
        try memory.computeAliasLastUseInto(&ends, &ends, &sources, &.{}, .{ .propagation = .sources });
        const Range = struct { offset: usize, bytes: usize, end: u16 };
        var ranges: [instruction_capacity]Range = undefined;
        var used: usize = 0;
        var peak: usize = 0;
        for (self.nodes[0..self.count], 0..) |*node, i| {
            node.arena_offset = null;
            if (ends[node.output] == escapes or (self.linear_only and node.prepared_slot == null)) continue;
            const d = node.outputDimensions() orelse continue;
            const bytes = try std.math.mul(usize, try std.math.mul(usize, d.rows, d.out_dim), 4);
            const size = (try std.math.add(usize, bytes, 4095)) & ~@as(usize, 4095);
            // Keep a partition ordered by offset. Adjacent expired ranges
            // can satisfy a wider producer without growing retained storage.
            var cursor: usize = 0;
            while (cursor + 1 < used) {
                if (ranges[cursor].end < i and ranges[cursor + 1].end < i) {
                    std.debug.assert(ranges[cursor].offset + ranges[cursor].bytes == ranges[cursor + 1].offset);
                    ranges[cursor].bytes += ranges[cursor + 1].bytes;
                    ranges[cursor].end = @max(ranges[cursor].end, ranges[cursor + 1].end);
                    std.mem.copyForwards(Range, ranges[cursor + 1 .. used - 1], ranges[cursor + 2 .. used]);
                    used -= 1;
                } else cursor += 1;
            }
            var best: ?usize = null;
            for (ranges[0..used], 0..) |range, j| if (range.end < i and range.bytes >= size) {
                if (best == null or range.bytes < ranges[best.?].bytes) best = j;
            };
            if (best) |j| {
                node.arena_offset = ranges[j].offset;
                if (ranges[j].bytes > size) {
                    const remainder = Range{ .offset = ranges[j].offset + size, .bytes = ranges[j].bytes - size, .end = ranges[j].end };
                    std.mem.copyBackwards(Range, ranges[j + 2 .. used + 1], ranges[j + 1 .. used]);
                    ranges[j + 1] = remainder;
                    ranges[j].bytes = size;
                    used += 1;
                }
                ranges[j].end = @intCast(ends[node.output]);
            } else {
                node.arena_offset = peak;
                ranges[used] = .{ .offset = peak, .bytes = size, .end = @intCast(ends[node.output]) };
                used += 1;
                peak = try std.math.add(usize, peak, size);
            }
        }
        return peak;
    }
    pub const ArenaLayout = struct {
        outputs_bytes: usize = 0,
        input_workspace_bytes: usize = 0,
        total_bytes: usize = 0,

        /// The schedule funds one reusable scratch range. The owner may
        /// place it after the output arena or upgrade it as a separate bank.
        pub fn workspaceBytes(self: ArenaLayout, available_views: usize, count: usize, limit: usize) usize {
            if (available_views != count or self.input_workspace_bytes > limit) return 0;
            return self.input_workspace_bytes;
        }

        pub const BankRequest = struct { bytes: usize, workspace_bytes: usize };

        pub fn initialBank(self: ArenaLayout, available_views: usize, count: usize, limit: usize) ?BankRequest {
            if (self.outputs_bytes > limit) return null;
            const scratch = self.workspaceBytes(available_views, count, limit);
            return .{ .bytes = if (scratch != 0) self.total_bytes else self.outputs_bytes, .workspace_bytes = scratch };
        }

        pub const View = struct { offset: usize, bytes: usize, shape: [2]i32 };

        fn view(offset: usize, bound: usize, rows: usize, width: usize) !View {
            const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(f32));
            if (bytes == 0 or offset > bound or bytes > bound - offset) return error.AudioScheduleInvalid;
            return .{
                .offset = offset,
                .bytes = bytes,
                .shape = .{ std.math.cast(i32, rows) orelse return error.AudioScheduleInvalid, std.math.cast(i32, width) orelse return error.AudioScheduleInvalid },
            };
        }

        pub fn outputView(self: ArenaLayout, node: *const Node) !?View {
            const offset = node.arena_offset orelse return null;
            const dims = node.outputDimensions() orelse return error.AudioScheduleInvalid;
            return try view(offset, self.outputs_bytes, dims.rows, dims.out_dim);
        }

        pub const WorkspacePlacement = enum { combined, separate };

        pub fn workspaceView(self: ArenaLayout, node: *const Node, placement: WorkspacePlacement) !?View {
            const shape = node.inputWorkspaceShape() orelse return null;
            return switch (placement) {
                .combined => try view(self.outputs_bytes, self.total_bytes, shape.rows, shape.width),
                .separate => try view(0, self.input_workspace_bytes, shape.rows, shape.width),
            };
        }
    };

    pub const InitialArena = struct {
        bytes: usize,
        output_views: usize,
        workspace_bytes: usize,
        workspace_views: usize,
        output_only_bytes: usize,

        pub fn outputOnly(self: InitialArena) ?InitialArena {
            if (self.output_only_bytes == 0 or self.workspace_bytes == 0) return null;
            return .{
                .bytes = self.output_only_bytes,
                .output_views = self.output_views,
                .workspace_bytes = 0,
                .workspace_views = 0,
                .output_only_bytes = self.output_only_bytes,
            };
        }
    };

    pub fn initialArena(self: *const Plan, bank_limit: usize) !?InitialArena {
        const recipe = self.arena_recipe;
        const bank = recipe.initialBank(self.workspaces.len, self.count, bank_limit) orelse return null;
        const views = try self.arenaViewCounts(recipe, true, if (bank.workspace_bytes == 0) null else .combined);
        return .{
            .bytes = bank.bytes,
            .output_views = views.outputs,
            .workspace_bytes = bank.workspace_bytes,
            .workspace_views = views.workspaces,
            .output_only_bytes = recipe.outputs_bytes,
        };
    }

    /// A value-only request may cross the provider boundary. The receiver
    /// revalidates it against the current learned plan after admission.
    pub const ArenaRequest = struct {
        key: u64,
        stamp: u64,
        bytes: usize,
        views: usize,
        initial: ?InitialArena = null,
    };

    pub fn scratchUpgrade(self: *const Plan, clock: u64, bank_limit: usize) !?ArenaRequest {
        if (!self.prepared or self.disabled or clock < self.retry_after or self.input_workspace_bytes != 0) return null;
        const bytes = self.arena_recipe.workspaceBytes(self.workspaces.len, self.count, bank_limit);
        if (bytes == 0) return null;
        const counts = try self.arenaViewCounts(self.arena_recipe, false, .separate);
        return .{ .key = self.key, .stamp = self.stamp, .bytes = bytes, .views = counts.workspaces };
    }

    pub fn deferArenaRetry(self: *Plan, clock: u64) void {
        self.retry_after = clock +| self.retry_delay;
        self.retry_delay = @min(@as(u8, 64), self.retry_delay *| 2);
    }

    pub fn pendingArena(self: *const Plan, clock: u64, bank_limit: usize) !?ArenaRequest {
        if (self.prepared) return self.scratchUpgrade(clock, bank_limit);
        if (!self.bindings_ready or self.disabled or clock < self.retry_after or self.arena != null) return null;
        const request = (try self.initialArena(bank_limit)) orelse return null;
        if (request.bytes == 0) return null;
        return .{ .key = self.key, .stamp = self.stamp, .bytes = request.bytes, .views = try std.math.add(usize, request.output_views, request.workspace_views), .initial = request };
    }

    pub const ArenaViewCounts = struct {
        outputs: usize = 0,
        workspaces: usize = 0,
    };

    /// Quote host headers using the same verified layout as publication.
    /// A separate upgrade never reserves the existing output views again.
    pub fn arenaViewCounts(self: *const Plan, recipe: ArenaLayout, outputs: bool, workspace: ?ArenaLayout.WorkspacePlacement) !ArenaViewCounts {
        if (self.count > self.nodes.len) return error.AudioScheduleInvalid;
        var counts: ArenaViewCounts = .{};
        for (self.nodes[0..self.count]) |*node| {
            if (outputs and self.storage.outputs.len != 0 and !self.linear_only and (try recipe.outputView(node)) != null)
                counts.outputs += 1;
            if (workspace) |placement| {
                if ((try recipe.workspaceView(node, placement)) != null) counts.workspaces += 1;
            }
        }
        return counts;
    }

    /// Serialized projections share one disjoint scratch tail. Both fused and
    /// unfused paths are funded before replay; no clamp temporary is implicit.
    pub fn arenaLayout(self: *Plan) !ArenaLayout {
        const outputs = try self.layout();
        var scratch: usize = 0;
        for (self.nodes[0..self.count]) |node| if (node.inputWorkspaceShape()) |shape| {
            const bytes = try std.math.mul(usize, try std.math.mul(usize, shape.rows, shape.width), @sizeOf(f32));
            scratch = @max(scratch, (try std.math.add(usize, bytes, 4095)) & ~@as(usize, 4095));
        };
        return .{ .outputs_bytes = outputs, .input_workspace_bytes = scratch, .total_bytes = try std.math.add(usize, outputs, scratch) };
    }

    /// Weight operands supplied by prepared slots do not need tensor bindings.
    /// A constant used elsewhere in the tape remains a required input.
    pub fn resolveRequiredConstants(self: *Plan) void {
        var required: std.StaticBitSet(capacity) = .empty;
        for (self.nodes[0..self.count]) |*node| {
            const prepared_weight: ?u16 = if (node.prepared_slot != null) switch (node.args) {
                .linearNoBias => |a| if (a.input != a.weight) a.weight else null,
                .clippedLinear => |a| if (a.input != a.weight) a.weight else null,
                else => null,
            } else null;
            for (node.inputs[0..node.input_count]) |id|
                if (prepared_weight != id) required.set(id);
        }
        for (self.constants[0..self.constant_count]) |*constant|
            constant.required = required.isSet(constant.id);
    }

    pub fn replay(self: *Plan, cb: *const ops.ComputeBackend, inputs: []const CT, cleanup: @import("cleanup_context.zig").Context) !CT {
        var values: [capacity]?CT = @splat(null);
        var owned: [capacity]bool = @splat(false);
        defer for (values, owned) |value, own| {
            if (own) cb.freeWithCleanup(value.?, cleanup);
        };
        for (inputs, 0..) |ct, i| values[i] = ct;
        for (self.constants[0..self.constant_count]) |c| {
            if (!c.required) continue;
            if (c.bound) |bound| {
                values[c.id] = bound;
                continue;
            }
            const name = c.name[0..c.len];
            values[c.id] = if (c.model) (try cb.modelConstant(name, null)) orelse return error.AudioScheduleConstantMissing else try cb.getWeight(name);
            owned[c.id] = true;
        }
        for (self.nodes[0..self.count], 0..) |*node, i| {
            try cb.checkExecutionControl();
            const persistent = self.storage.outputs.len > i and self.storage.outputs[i] != null;
            var hint: ?CT = if (persistent) self.storage.outputs[i] else null;
            if (!persistent) if (self.arena) |arena| if (try self.arena_recipe.outputView(node)) |region| {
                if (cb.vtable.audioArenaView) |view| hint = try view(cb.ptr, arena, region.offset, region.bytes, region.shape);
            };
            const result = node.run(cb, &values, hint, if (self.workspaces.len > i) self.workspaces[i] else null, cleanup) catch |err| {
                if (!persistent) if (hint) |ct| cb.freeWithCleanup(ct, cleanup);
                return err;
            };
            if (!persistent) if (hint) |ct| if (result != ct) {
                cb.freeWithCleanup(ct, cleanup);
            };
            values[node.output] = result;
            owned[node.output] = !persistent or result != hint.?;
            for (node.inputs[0..node.input_count]) |id| {
                if (id != self.output and self.last_use[id] == i and owned[id]) {
                    cb.freeWithCleanup(values[id].?, cleanup);
                    values[id] = null;
                    owned[id] = false;
                }
            }
            // An unconsumed result has no later retirement edge.
            if (node.output != self.output and self.last_use[node.output] <= i and owned[node.output]) {
                cb.freeWithCleanup(result, cleanup);
                values[node.output] = null;
                owned[node.output] = false;
            }
        }
        try cb.checkExecutionControl();
        if (!owned[self.output]) return error.AudioScheduleInvalid;
        const output = values[self.output] orelse return error.AudioScheduleInvalid;
        owned[self.output] = false;
        self.replays += 1;
        return output;
    }
};
pub const Capture = struct {
    plan: *Plan,
    failed: bool = false,
    handles: [capacity]?CT = @splat(null),
    nodes: [instruction_capacity]Node = undefined,
    constants: [512]Constant = undefined,
    pub fn init(plan: *Plan, inputs: []const CT) Capture {
        std.debug.assert(plan.metadata_views == null);
        plan.last_use = @splat(0);
        plan.count = 0;
        plan.constant_count = 0;
        plan.next_id = @intCast(inputs.len);
        var self = Capture{ .plan = plan };
        for (inputs, 0..) |ct, i| self.handles[i] = ct;
        return self;
    }
    pub fn find(self: *Capture, ct: CT) ?u16 {
        for (self.handles[0..self.plan.next_id], 0..) |candidate, i| if (candidate == ct) return @intCast(i);
        return null;
    }
    pub fn forget(self: *Capture, ct: CT) void {
        for (self.handles[0..self.plan.next_id]) |*candidate| if (candidate.* == ct) {
            candidate.* = null;
        };
    }
    pub fn constant(self: *Capture, ct: CT, name: []const u8, model: bool) !u16 {
        if (self.plan.next_id == capacity or self.plan.constant_count == 512 or name.len > 256) return error.AudioScheduleFull;
        const id = self.plan.next_id;
        self.plan.next_id += 1;
        var c = Constant{ .id = id, .model = model, .name = undefined, .len = @intCast(name.len) };
        @memcpy(c.name[0..name.len], name);
        self.plan.constants = &self.constants;
        self.plan.constants[self.plan.constant_count] = c;
        self.plan.constant_count += 1;
        self.handles[id] = ct;
        return id;
    }
    pub fn append(self: *Capture, node: Node, ct: CT) void {
        if (self.failed) return;
        if (self.plan.count == self.nodes.len or self.plan.next_id == capacity or node.input_count > node.inputs.len) {
            self.failed = true;
            return;
        }
        for (node.inputs[0..node.input_count]) |id| {
            if (id >= self.plan.next_id) {
                self.failed = true;
                return;
            }
        }
        var n = node;
        n.output = self.plan.next_id;
        self.plan.next_id += 1;
        self.plan.last_use[n.output] = @intCast(self.plan.count);
        for (n.inputs[0..n.input_count]) |id| self.plan.last_use[id] = @intCast(self.plan.count);
        self.handles[n.output] = ct;
        self.plan.nodes = &self.nodes;
        self.plan.nodes[self.plan.count] = n;
        self.plan.count += 1;
    }
    pub fn finish(self: *Capture, ct: CT) void {
        self.plan.ready = false;
        if (!self.failed) {
            if (self.find(ct)) |output| {
                // A replay returns an owned result. Borrowed inputs and
                // retained constants cannot escape as that result.
                for (self.plan.nodes[0..self.plan.count]) |node| {
                    if (node.output != output) continue;
                    self.plan.output = output;
                    self.plan.ready = true;
                    break;
                }
            }
        }
        self.plan.disabled = !self.plan.ready;
    }
};
pub const LinearDimensions = struct { rows: usize, in_dim: usize, out_dim: usize };
pub const Node = struct {
    prepared_slot: ?usize = null,
    output_rows: usize = 0,
    output_dim: usize = 0,
    arena_offset: ?usize = null,
    output: u16 = 0,
    inputs: [8]u16 = @splat(0),
    input_count: u8 = 0,
    args: union(enum) {
        rmsNorm: struct { input: u16, weight: u16, dim: usize, eps: f32 },
        rmsNormClamped: struct { input: u16, weight: u16, p: ops.NormClampParams },
        rmsNormResidual: struct { input: u16, weight: u16, residual: u16, dim: usize, eps: f32, norm_scale: f32, output_scale: f32 },
        linearNoBias: struct { input: u16, weight: u16, rows: usize, in_dim: usize, out_dim: usize },
        clippedLinear: struct { input: u16, weight: u16, p: ops.ClippedLinearParams },
        clampScalar: struct { input: u16, min_value: ?f32, max_value: ?f32 },
        clampSilu: struct { input: u16, min_value: ?f32, max_value: ?f32 },
        silu: struct { input: u16 },
        add: struct { a: u16, b: u16 },
        gluRows: struct { input: u16, rows: usize, dim: usize },
        depthwiseCausalConv1d: struct { input: u16, weight: u16, rows: usize, dim: usize, kernel_size: usize },
        depthwiseCausalConv1dSegmented: struct { input: u16, weight: u16, starts: u16, rows: usize, dim: usize, kernel_size: usize },
        gemma4AudioLocalAttention: struct { q: u16, k: u16, v: u16, rel: u16, q_dim_scales: u16, valid: u16, params: ops.Gemma4AudioLocalAttentionParams },
        sliceLastDim: struct { tensor: u16, start: usize, stop: usize },
        linearPackedNamed: struct { input: u16, names_storage: [3][192]u8, names_lengths: [3]u16, name_count: u8, widths: [3]usize, rows: usize, in_dim: usize },
    },
    pub fn outputDimensions(self: *const Node) ?LinearDimensions {
        switch (self.args) {
            .linearNoBias, .clippedLinear, .linearPackedNamed => if (self.prepared_slot == null) return null,
            else => {},
        }
        if (self.output_rows != 0 and self.output_dim != 0) return .{ .rows = self.output_rows, .in_dim = 0, .out_dim = self.output_dim };
        if (self.prepared_slot != null) return self.linearDimensions();
        return null;
    }
    pub const WorkspaceShape = struct { rows: usize, width: usize };
    pub fn inputWorkspaceShape(self: *const Node) ?WorkspaceShape {
        if (self.prepared_slot == null) return null;
        return switch (self.args) {
            .clippedLinear => |a| if (a.p.flags & 3 != 0) .{ .rows = a.p.rows, .width = a.p.in_dim } else null,
            else => null,
        };
    }
    pub fn linearDimensions(self: *const Node) ?LinearDimensions {
        return switch (self.args) {
            .linearNoBias => |a| .{ .rows = a.rows, .in_dim = a.in_dim, .out_dim = a.out_dim },
            .clippedLinear => |a| .{ .rows = a.p.rows, .in_dim = a.p.in_dim, .out_dim = a.p.out_dim },
            .linearPackedNamed => |a| blk: {
                var width: usize = 0;
                for (a.widths[0..a.name_count]) |w| width += w;
                break :blk .{ .rows = a.rows, .in_dim = a.in_dim, .out_dim = width };
            },
            else => null,
        };
    }
    fn run(self: *const Node, cb: *const ops.ComputeBackend, values: *[capacity]?CT, hint: ?CT, workspace: ?CT, cleanup: @import("cleanup_context.zig").Context) !CT {
        if (self.prepared_slot) |slot| if (cb.vtable.audioPreparedLinear) |linear| {
            const d = self.linearDimensions().?;
            const id = switch (self.args) {
                .linearNoBias => |a| a.input,
                .clippedLinear => |a| a.input,
                .linearPackedNamed => |a| a.input,
                else => unreachable,
            };
            const p: ?ops.ClippedLinearParams = switch (self.args) {
                .clippedLinear => |a| a.p,
                else => null,
            };
            return (try linear(cb.ptr, &.{ .input = values[id].?, .slot = slot, .rows = d.rows, .in_dim = d.in_dim, .out_dim = d.out_dim, .clip = p, .output = hint, .input_workspace = workspace })) orelse error.AudioSchedulePreparedSlotMissing;
        };
        if (hint) |output| if (cb.vtable.audioNodeInto) |into| {
            if (try into(cb.ptr, self, values, output)) return output;
        };
        switch (self.args) {
            .rmsNorm => |a| {
                return (try cb.vtable.rmsNorm(cb.ptr, values[a.input].?, values[a.weight].?, a.dim, a.eps));
            },
            .rmsNormClamped => |a| {
                return (try cb.vtable.rmsNormClamped.?(cb.ptr, values[a.input].?, values[a.weight].?, a.p)) orelse error.AudioScheduleRouteChanged;
            },
            .rmsNormResidual => |a| {
                return (try cb.vtable.rmsNormResidual.?(cb.ptr, values[a.input].?, values[a.weight].?, values[a.residual].?, a.dim, a.eps, a.norm_scale, a.output_scale)) orelse error.AudioScheduleRouteChanged;
            },
            .linearNoBias => |a| {
                return (try cb.vtable.linearNoBias(cb.ptr, values[a.input].?, values[a.weight].?, a.rows, a.in_dim, a.out_dim));
            },
            .clippedLinear => |a| {
                if (try cb.clippedLinear(values[a.input].?, values[a.weight].?, a.p)) |out| return out;
                return stagedClippedLinear(cb, values[a.input].?, values[a.weight].?, a.p, cleanup);
            },
            .clampScalar => |a| {
                return (try cb.vtable.clampScalar.?(cb.ptr, values[a.input].?, a.min_value, a.max_value)) orelse error.AudioScheduleRouteChanged;
            },
            .clampSilu => |a| {
                return (try cb.vtable.clampSilu.?(cb.ptr, values[a.input].?, a.min_value, a.max_value)) orelse error.AudioScheduleRouteChanged;
            },
            .silu => |a| {
                return (try cb.vtable.silu(cb.ptr, values[a.input].?));
            },
            .add => |a| {
                return (try cb.vtable.add(cb.ptr, values[a.a].?, values[a.b].?));
            },
            .gluRows => |a| {
                return (try cb.vtable.gluRows.?(cb.ptr, values[a.input].?, a.rows, a.dim)) orelse error.AudioScheduleRouteChanged;
            },
            .depthwiseCausalConv1d => |a| {
                return (try cb.vtable.depthwiseCausalConv1d.?(cb.ptr, values[a.input].?, values[a.weight].?, a.rows, a.dim, a.kernel_size)) orelse error.AudioScheduleRouteChanged;
            },
            .depthwiseCausalConv1dSegmented => |a| {
                return (try cb.vtable.depthwiseCausalConv1dSegmented.?(cb.ptr, values[a.input].?, values[a.weight].?, values[a.starts].?, a.rows, a.dim, a.kernel_size)) orelse error.AudioScheduleRouteChanged;
            },
            .gemma4AudioLocalAttention => |a| {
                return (try cb.vtable.regions.audio_attention.localRelative.?(cb.ptr, values[a.q].?, values[a.k].?, values[a.v].?, values[a.rel].?, values[a.q_dim_scales].?, values[a.valid].?, a.params)) orelse error.AudioScheduleRouteChanged;
            },
            .sliceLastDim => |a| {
                return (try cb.vtable.sliceLastDim.?(cb.ptr, values[a.tensor].?, a.start, a.stop));
            },
            .linearPackedNamed => |a| {
                var names: [3][]const u8 = undefined;
                for (0..a.name_count) |i| names[i] = a.names_storage[i][0..a.names_lengths[i]];
                return (try cb.vtable.linearPackedNamed.?(cb.ptr, values[a.input].?, names[0..a.name_count], a.widths[0..a.name_count], a.rows, a.in_dim)) orelse error.AudioScheduleRouteChanged;
            },
        }
    }
};

pub const History = struct {
    const Entry = struct { key: u64 = 0, stamp: u64 = 0, count: u8 = 0 };
    entries: [32]Entry = @splat(.{}),
    pub fn observe(self: *History, key: u64, stamp: u64) bool {
        var victim: usize = 0;
        for (&self.entries, 0..) |*entry, i| {
            if (entry.stamp != 0 and entry.key == key) {
                entry.stamp = stamp;
                entry.count = @min(@as(u8, 2), entry.count + 1);
                return entry.count >= 2;
            }
            if (entry.stamp < self.entries[victim].stamp) victim = i;
        }
        self.entries[victim] = .{ .key = key, .stamp = stamp, .count = 1 };
        return false;
    }
};

test "embeddinggemma2 audio schedule metadata requires recurrence and forgets one-off shapes" {
    var history = History{};
    try std.testing.expect(!history.observe(0, 1));
    try std.testing.expect(history.observe(0, 2));
    for (1..65) |key| try std.testing.expect(!history.observe(key, key + 2));
    try std.testing.expect(!history.observe(0, 100));
}

test "embeddinggemma2 audio schedule rejects an unrecorded output without retaining a request handle" {
    const a = std.testing.allocator;
    const plan = try a.create(Plan);
    defer a.destroy(plan);
    plan.* = .{};
    var capture = Capture.init(plan, &.{});
    capture.finish(@ptrFromInt(1));
    try std.testing.expect(plan.disabled);
    try std.testing.expect(!plan.ready);
}

test "audio schedule only publishes a value produced by the captured stage" {
    const a = std.testing.allocator;
    const plan = try a.create(Plan);
    defer a.destroy(plan);
    plan.* = .{};
    const input: CT = @ptrFromInt(16);
    const result: CT = @ptrFromInt(32);
    var capture = Capture.init(plan, &.{input});
    capture.append(.{ .args = .{ .sliceLastDim = .{ .tensor = 0, .start = 0, .stop = 1 } }, .inputs = @splat(0), .input_count = 1 }, result);
    capture.finish(input);
    try std.testing.expect(plan.disabled);
    try std.testing.expect(!plan.ready);

    capture.finish(result);
    try std.testing.expect(!plan.disabled);
    try std.testing.expect(plan.ready);
    try std.testing.expectEqual(@as(u16, 1), plan.output);
}

/// A declining optional route must not invalidate a correctly recorded request.
fn stagedClippedLinear(cb: *const ops.ComputeBackend, input: CT, weight: CT, p: ops.ClippedLinearParams, cleanup: @import("cleanup_context.zig").Context) !CT {
    const lo = if (p.flags & 1 != 0) @as(?f32, p.input_min) else null;
    const hi = if (p.flags & 2 != 0) @as(?f32, p.input_max) else null;
    const clipped = if (p.flags & 3 != 0) (try cb.clampScalar(input, lo, hi)) orelse return error.AudioScheduleRouteChanged else input;
    defer if (clipped != input) cb.freeWithCleanup(clipped, cleanup);
    const result = try cb.linearNoBias(clipped, weight, p.rows, p.in_dim, p.out_dim);
    errdefer cb.freeWithCleanup(result, cleanup);
    const omin = if (p.flags & 4 != 0) @as(?f32, p.output_min) else null;
    const omax = if (p.flags & 8 != 0) @as(?f32, p.output_max) else null;
    if (p.flags & 16 != 0) {
        if (try cb.clampSilu(result, omin, omax)) |out| {
            cb.freeWithCleanup(result, cleanup);
            return out;
        }
    }
    const bounded = if (p.flags & 12 != 0) (try cb.clampScalar(result, omin, omax)) orelse return error.AudioScheduleRouteChanged else result;
    if (p.flags & 16 != 0) {
        errdefer if (bounded != result) cb.freeWithCleanup(bounded, cleanup);
        const out = try cb.silu(bounded);
        if (bounded != result) cb.freeWithCleanup(bounded, cleanup);
        cb.freeWithCleanup(result, cleanup);
        return out;
    }
    if (bounded != result) cb.freeWithCleanup(result, cleanup);
    return bounded;
}

/// Five interleaved pairs reject isolated scheduler outliers. A losing plan
/// cools down for eight visits, then requalifies; successful plans periodically
/// resample. A request is executed exactly once, with its own control.
pub const Selection = struct {
    eager: [5]u64 = @splat(0),
    replay: [5]u64 = @splat(0),
    eager_count: usize = 0,
    replay_count: usize = 0,
    cooldown: u8 = 0,
    steady: u8 = 0,
    sampling: bool = false,
    pub fn useReplay(self: *Selection) bool {
        self.sampling = false;
        if (self.cooldown > 0) {
            self.cooldown -= 1;
            return false;
        }
        if (self.eager_count < 5 or self.replay_count < 5) {
            self.sampling = true;
            return self.eager_count > self.replay_count;
        }
        self.steady += 1;
        if (self.steady < 64) return true;
        self.* = .{ .sampling = true };
        return false;
    }
    fn median(values: [5]u64) u64 {
        var sorted = values;
        std.mem.sort(u64, &sorted, {}, std.sort.asc(u64));
        return sorted[2];
    }
    pub fn record(self: *Selection, replayed: bool, ns: u64) void {
        if (ns == 0 or !self.sampling) return;
        if (replayed and self.replay_count < 5) {
            self.replay[self.replay_count] = ns;
            self.replay_count += 1;
        }
        if (!replayed and self.eager_count < 5) {
            self.eager[self.eager_count] = ns;
            self.eager_count += 1;
        }
        if (self.eager_count == 5 and self.replay_count == 5) {
            const baseline = median(self.eager);
            if (median(self.replay) > baseline + baseline / 20) self.* = .{ .cooldown = 8 };
        }
    }
};

test "embeddinggemma2 audio selection rejects isolated outliers and retries after cooldown" {
    var selection = Selection{};
    for (0..5) |i| {
        try std.testing.expect(!selection.useReplay());
        selection.record(false, if (i == 0) 1 else 100);
        try std.testing.expect(selection.useReplay());
        selection.record(true, if (i == 0) 10000 else 80);
    }
    try std.testing.expect(selection.useReplay());
    selection = .{};
    for (0..5) |_| {
        try std.testing.expect(!selection.useReplay());
        selection.record(false, 100);
        try std.testing.expect(selection.useReplay());
        selection.record(true, 300);
    }
    for (0..8) |_| {
        try std.testing.expect(!selection.useReplay());
        selection.record(false, 1);
    }
    for (0..5) |_| {
        try std.testing.expect(!selection.useReplay());
        selection.record(false, 100);
        try std.testing.expect(selection.useReplay());
        selection.record(true, 70);
    }
    try std.testing.expect(selection.useReplay());
}

test "embeddinggemma2 audio arena extends view lifetimes and excludes escaping storage" {
    const plan = try std.testing.allocator.create(Plan);
    defer std.testing.allocator.destroy(plan);
    var nodes: [4]Node = undefined;
    plan.* = .{ .nodes = &nodes, .count = 4, .output = 5 };
    plan.nodes[0] = .{ .output = 2, .prepared_slot = 0, .args = .{ .linearNoBias = .{ .input = 0, .weight = 1, .rows = 1, .in_dim = 2, .out_dim = 2 } } };
    plan.nodes[1] = .{ .output = 3, .args = .{ .sliceLastDim = .{ .tensor = 2, .start = 0, .stop = 1 } } };
    plan.nodes[2] = .{ .output = 4, .prepared_slot = 1, .args = .{ .linearNoBias = .{ .input = 0, .weight = 1, .rows = 1, .in_dim = 2, .out_dim = 2 } } };
    plan.nodes[3] = .{ .output = 5, .args = .{ .add = .{ .a = 3, .b = 4 } } };
    plan.last_use[2] = 1;
    plan.last_use[3] = 3;
    plan.last_use[4] = 3;
    try std.testing.expectEqual(@as(usize, 8192), try plan.layout());
    try std.testing.expect(plan.nodes[0].arena_offset != plan.nodes[2].arena_offset);
    // A returned view keeps its backing allocation outside the mutable arena.
    plan.output = 3;
    try std.testing.expectEqual(@as(usize, 4096), try plan.layout());
    try std.testing.expect(plan.nodes[0].arena_offset == null);
    plan.output = 5;
    plan.nodes[1].args = .{ .silu = .{ .input = 2 } };
    try std.testing.expectEqual(@as(usize, 4096), try plan.layout());
    try std.testing.expectEqual(plan.nodes[0].arena_offset, plan.nodes[2].arena_offset);
}

test "embeddinggemma2 audio arena splits and coalesces expired lifetimes" {
    var nodes: [5]Node = undefined;
    for (&nodes, 0..) |*node, i| node.* = .{ .output = @intCast(i + 1), .output_rows = 1, .output_dim = if (i == 3) 3072 else 1024, .args = .{ .clampScalar = .{ .input = 0, .min_value = null, .max_value = null } } };
    var plan = Plan{ .nodes = &nodes, .count = nodes.len, .output = 5 };
    plan.last_use[1] = 2;
    plan.last_use[2] = 2;
    plan.last_use[3] = 2;
    plan.last_use[4] = 4;
    try std.testing.expectEqual(@as(usize, 12288), try plan.layout());
    try std.testing.expectEqual(@as(?usize, 0), nodes[3].arena_offset);
    // A smaller producer leaves the remainder available to a live sibling.
    nodes[0].output_dim = 2048;
    nodes[1].output_dim = 1024;
    nodes[2].output_dim = 1024;
    plan.last_use[1] = 0;
    plan.last_use[2] = 3;
    plan.last_use[3] = 3;
    plan.count = 3;
    try std.testing.expectEqual(@as(usize, 8192), try plan.layout());
    try std.testing.expectEqual(@as(?usize, 4096), nodes[2].arena_offset);
}

test "learned audio metadata requires the exact reservation before publishing views" {
    var nodes = [_]Node{.{ .output = 1, .args = .{ .clampScalar = .{ .input = 0, .min_value = null, .max_value = null } } }};
    var plan = Plan{ .nodes = &nodes, .count = nodes.len };
    const bytes = try plan.learnedMetadataBytes();
    try std.testing.expectError(error.AudioScheduleInvalid, plan.freezeLearnedMetadata(bytes - 1));
    try std.testing.expect(plan.metadata_views == null and plan.nodes.ptr == nodes[0..].ptr);
    try plan.freezeLearnedMetadata(bytes);
    defer plan.deinit();
    try std.testing.expectEqual(@sizeOf(Plan) + bytes, plan.storage.host_bytes);
    try std.testing.expectEqual(@sizeOf(Plan) + bytes, plan.metadata_bytes);
    try std.testing.expectEqual(@as(usize, 1), plan.storage.outputs.len);
    try std.testing.expectEqual(@as(usize, 1), plan.workspaces.len);
    try std.testing.expect(plan.storage.outputs[0] == null and plan.workspaces[0] == null);
    try std.testing.expectError(error.AudioScheduleInvalid, plan.freezeLearnedMetadata(bytes));
    plan.deinit();
    try std.testing.expect(plan.metadata_views == null and plan.nodes.len == 0 and plan.workspaces.len == 0);
}

test "embeddinggemma2 audio arena randomized lifetimes never overlap live outputs" {
    var generator = std.Random.DefaultPrng.init(0x616e74666c79);
    const random = generator.random();
    for (0..100) |_| {
        var nodes: [32]Node = undefined;
        var plan = Plan{ .nodes = &nodes, .count = nodes.len, .output = 32 };
        for (&nodes, 0..) |*node, i| {
            node.* = .{ .output = @intCast(i + 1), .output_rows = 1, .output_dim = 512 * (1 + random.uintLessThan(usize, 8)), .args = .{ .clampScalar = .{ .input = 0, .min_value = null, .max_value = null } } };
            plan.last_use[i + 1] = @intCast(i + random.uintLessThan(usize, nodes.len - i));
        }
        const bytes = try plan.layout();
        for (nodes, 0..) |node, i| if (node.arena_offset) |start| {
            const end = start + node.output_rows * node.output_dim * 4;
            try std.testing.expect(end <= bytes);
            for (nodes[0..i], 0..) |earlier, j| if (earlier.arena_offset) |other| {
                if (plan.last_use[j + 1] < i) continue;
                const other_end = other + earlier.output_rows * earlier.output_dim * 4;
                try std.testing.expect(end <= other or other_end <= start);
            };
        };
    }
}

test "audio arena uses its full declared instruction capacity and rejects overflowing storage" {
    const a = std.testing.allocator;
    const count = instruction_capacity;
    const nodes = try a.alloc(Node, count + 1);
    defer a.free(nodes);
    const plan = try a.create(Plan);
    defer a.destroy(plan);
    plan.* = .{ .nodes = nodes, .count = count, .output = 0 };
    for (nodes[0..count], 0..) |*node, index| {
        node.* = .{ .output = @intCast(index + 1), .output_rows = 1, .output_dim = 1, .args = .{ .clampScalar = .{ .input = 0, .min_value = null, .max_value = null } } };
        plan.last_use[index + 1] = count;
    }
    try std.testing.expectEqual(@as(usize, count * 4096), try plan.layout());
    for (nodes[0..count], 0..) |node, index| try std.testing.expectEqual(@as(?usize, index * 4096), node.arena_offset);
    plan.count = count + 1;
    try std.testing.expectError(error.AudioScheduleInvalid, plan.layout());
    plan.count = count;
    nodes[0].output_dim = std.math.maxInt(usize) / 4;
    try std.testing.expectError(error.Overflow, plan.layout());
    plan.output = capacity;
    try std.testing.expectError(error.AudioScheduleInvalid, plan.layout());
}

test "audio arena recipe funds one disjoint maximum clipped projection workspace" {
    const a = std.testing.allocator;
    const plan = try a.create(Plan);
    defer a.destroy(plan);
    var nodes: [3]Node = undefined;
    plan.* = .{ .nodes = &nodes, .count = 3, .output = 3 };
    for (&nodes, 0..) |*node, i| node.* = .{ .prepared_slot = i, .output = @intCast(i + 1), .args = .{ .clippedLinear = .{ .input = 0, .weight = 0, .p = .{ .rows = 3, .in_dim = if (i == 1) 512 else 32, .out_dim = 32, .flags = if (i == 2) 16 else 3, .input_min = -1, .input_max = 1, .output_min = -2, .output_max = 2 } } } };
    plan.last_use[1] = 2;
    plan.last_use[2] = 2;
    const recipe = try plan.arenaLayout();
    try std.testing.expectEqual(@as(usize, 8192), recipe.outputs_bytes);
    try std.testing.expectEqual(@as(usize, 8192), recipe.input_workspace_bytes);
    try std.testing.expectEqual(@as(usize, 16384), recipe.total_bytes);
    try std.testing.expectEqual(recipe.input_workspace_bytes, recipe.workspaceBytes(nodes.len, nodes.len, 8192));
    try std.testing.expectEqual(@as(usize, 0), recipe.workspaceBytes(nodes.len, nodes.len, 4096));
    try std.testing.expectEqual(Plan.ArenaLayout.BankRequest{ .bytes = 16384, .workspace_bytes = 8192 }, recipe.initialBank(nodes.len, nodes.len, 8192).?);
    try std.testing.expectEqual(Plan.ArenaLayout.BankRequest{ .bytes = 8192, .workspace_bytes = 0 }, recipe.initialBank(0, nodes.len, 8192).?);
    try std.testing.expect(recipe.initialBank(nodes.len, nodes.len, 4096) == null);
    var workspace_views: [nodes.len]?CT = @splat(null);
    plan.workspaces = &workspace_views;
    plan.arena_recipe = recipe;
    const initial = (try plan.initialArena(8192)).?;
    const fallback = initial.outputOnly().?;
    try std.testing.expectEqual(recipe.outputs_bytes, fallback.bytes);
    try std.testing.expectEqual(initial.output_views, fallback.output_views);
    try std.testing.expectEqual(@as(usize, 0), fallback.workspace_bytes);
    try std.testing.expectEqual(@as(usize, 0), fallback.workspace_views);
    try std.testing.expect(fallback.outputOnly() == null);
    const output = (try recipe.outputView(&nodes[1])).?;
    const combined = (try recipe.workspaceView(&nodes[1], .combined)).?;
    const separate = (try recipe.workspaceView(&nodes[1], .separate)).?;
    try std.testing.expect(output.offset + output.bytes <= combined.offset);
    try std.testing.expectEqual(recipe.outputs_bytes, combined.offset);
    try std.testing.expectEqual(@as(usize, 0), separate.offset);
    try std.testing.expectEqual(combined.bytes, separate.bytes);
    try std.testing.expectEqual(combined.shape, separate.shape);
    try std.testing.expect((try recipe.workspaceView(&nodes[2], .combined)) == null);
    try std.testing.expect((try recipe.outputView(&nodes[2])) == null);
    nodes[1].arena_offset = combined.offset;
    try std.testing.expectError(error.AudioScheduleInvalid, recipe.outputView(&nodes[1]));
    nodes[1].prepared_slot = null;
    try std.testing.expectEqual(@as(usize, 4096), (try plan.arenaLayout()).input_workspace_bytes);
}

test "Metal audio frame groups bound two live groups and avoid overflow" {
    try std.testing.expectEqual(AudioFramePlan{ .layers = 4, .in_flight = 2 }, audioFramePlan(64, 1024, 4096));
    for ([_]usize{ 64, 256, 512, 1024, 2048, 8192 }) |rows| {
        const plan = audioFramePlan(rows, 1024, 4096);
        if (plan.in_flight == 2) try std.testing.expect(rows * (1024 * 12 + 4096 * 4) * 4 * plan.layers * 2 <= 384 * 1024 * 1024);
    }
    try std.testing.expectEqual(AudioFramePlan{}, audioFramePlan(std.math.maxInt(usize), 1024, 4096));
}

test "audio schedule keeps constants shared by prepared and ordinary operands" {
    var nodes = [_]Node{
        .{ .prepared_slot = 1, .inputs = @splat(0), .input_count = 2, .args = .{ .linearNoBias = .{ .input = 0, .weight = 1, .rows = 1, .in_dim = 1, .out_dim = 1 } } },
        .{ .inputs = @splat(0), .input_count = 2, .args = .{ .linearNoBias = .{ .input = 0, .weight = 1, .rows = 1, .in_dim = 1, .out_dim = 1 } } },
    };
    for (&nodes) |*node| node.inputs[1] = 1;
    var constants = [_]Constant{.{ .id = 1, .model = false, .name = undefined, .len = 0 }};
    var plan: Plan = .{ .nodes = &nodes, .count = nodes.len, .constants = &constants, .constant_count = constants.len };
    plan.resolveRequiredConstants();
    try std.testing.expect(constants[0].required);
    nodes[1].prepared_slot = 2;
    plan.resolveRequiredConstants();
    try std.testing.expect(!constants[0].required);
    nodes[1].args.linearNoBias.input = 1;
    nodes[1].inputs[0] = 1;
    plan.resolveRequiredConstants();
    try std.testing.expect(constants[0].required);
}

test "audio schedule keys track geometry and aliasing across different input owners" {
    const Probe = struct {
        fn key(inputs: [2]CT, second_shape: []const i32) u64 {
            var builder = ScheduleKey.init(42, 7, inputs.len);
            builder.addInput(&.{ 2, 4 }, inputs[0], &.{});
            builder.addInput(second_shape, inputs[1], inputs[0..1]);
            return builder.finish();
        }
    };
    const a: CT = @ptrFromInt(16);
    const b: CT = @ptrFromInt(32);
    const c: CT = @ptrFromInt(48);
    const d: CT = @ptrFromInt(64);
    const baseline = Probe.key(.{ a, b }, &.{ 2, 4 });
    var previous = std.hash.Wyhash.init(42);
    previous.update(&.{7});
    previous.update(std.mem.asBytes(&@as(usize, 2)));
    for (0..2) |_| {
        previous.update(std.mem.asBytes(&@as(usize, 2)));
        previous.update(std.mem.sliceAsBytes(&[_]i32{ 2, 4 }));
    }
    previous.update(std.mem.asBytes(&false));
    try std.testing.expectEqual(previous.final(), baseline);
    try std.testing.expectEqual(baseline, Probe.key(.{ c, d }, &.{ 2, 4 }));
    try std.testing.expect(baseline != Probe.key(.{ a, a }, &.{ 2, 4 }));
    try std.testing.expect(baseline != Probe.key(.{ a, b }, &.{ 4, 2 }));
}
