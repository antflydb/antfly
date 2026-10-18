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
const ops = @import("ops.zig");
const CT = ops.CT;
pub const capacity = 2048;
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
    host_bytes: usize = @sizeOf(Plan),
    metadata_bytes: usize = @sizeOf(Plan),
    metadata_owned: bool = false,
    retry_after: u64 = 0,
    retry_delay: u8 = 1,
    outputs: []?CT = &.{},
    backend_bytes: usize = 0,
    arena: ?CT = null,
    arena_size: usize = 0,
    drop_ct: ?*const fn (CT) void = null,
    prepared: bool = false,
    linear_only: bool = false,
    nodes: []Node = &.{},
    count: usize = 0,
    constants: []Constant = &.{},
    constant_count: usize = 0,
    last_use: [capacity]u16 = @splat(0),
    next_id: u16 = 0,
    output: u16 = 0,
    pub fn deinit(self: *Plan) void {
        self.dropPrepared();
        if (self.metadata_owned) {
            std.heap.c_allocator.free(self.nodes);
            std.heap.c_allocator.free(self.constants);
            std.heap.c_allocator.free(self.outputs);
        }
    }
    pub fn dropPrepared(self: *Plan) void {
        if (self.drop_ct) |drop| {
            for (self.constants[0..self.constant_count]) |*c| if (c.bound) |ct| {
                drop(ct);
                c.bound = null;
            };
            for (self.outputs) |*output| if (output.*) |ct| {
                drop(ct);
                output.* = null;
            };
            if (self.arena) |ct| drop(ct);
        }
        self.arena = null;
    }
    /// Assign non-overlapping lifetimes to linear outputs. Escaping values
    /// and their view ancestors remain independently owned by the caller.
    pub fn layout(self: *Plan) !usize {
        var ends = self.last_use;
        var escaping: [capacity]bool = @splat(false);
        escaping[self.output] = true;
        var n = self.count;
        while (n > 0) {
            n -= 1;
            const node = &self.nodes[n];
            switch (node.args) {
                .sliceLastDim => |a| {
                    ends[a.tensor] = @max(ends[a.tensor], ends[node.output]);
                    if (escaping[node.output]) escaping[a.tensor] = true;
                },
                else => {},
            }
        }
        const Range = struct { offset: usize, bytes: usize, end: u16 };
        var ranges: [1024]Range = undefined;
        var used: usize = 0;
        var peak: usize = 0;
        for (self.nodes[0..self.count], 0..) |*node, i| {
            node.arena_offset = null;
            if (escaping[node.output] or (self.linear_only and node.prepared_slot == null)) continue;
            const d = node.outputDimensions() orelse continue;
            const bytes = try std.math.mul(usize, try std.math.mul(usize, d.rows, d.out_dim), 4);
            const size = std.mem.alignForward(usize, bytes, 4096);
            var best: ?usize = null;
            for (ranges[0..used], 0..) |range, j| if (range.end < i and range.bytes >= size) {
                if (best == null or range.bytes < ranges[best.?].bytes) best = j;
            };
            if (best) |j| {
                node.arena_offset = ranges[j].offset;
                ranges[j].end = ends[node.output];
            } else {
                node.arena_offset = peak;
                ranges[used] = .{ .offset = peak, .bytes = size, .end = ends[node.output] };
                used += 1;
                peak = try std.math.add(usize, peak, size);
            }
        }
        return peak;
    }
    pub fn replay(self: *Plan, cb: *const ops.ComputeBackend, inputs: []const CT) !CT {
        var values: [capacity]?CT = @splat(null);
        var owned: [capacity]bool = @splat(false);
        defer for (values, owned) |value, own| {
            if (own) cb.free(value.?);
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
            const persistent = self.outputs.len > i and self.outputs[i] != null;
            var hint: ?CT = if (persistent) self.outputs[i] else null;
            if (!persistent) if (self.arena) |arena| if (node.arena_offset) |offset| {
                const d = node.outputDimensions().?;
                if (cb.vtable.audioArenaView) |view| hint = try view(cb.ptr, arena, offset, d.rows, d.out_dim);
            };
            const result = node.run(cb, &values, hint) catch |err| {
                if (!persistent) if (hint) |ct| cb.free(ct);
                return err;
            };
            if (!persistent) if (hint) |ct| if (result != ct) {
                cb.free(ct);
            };
            values[node.output] = result;
            owned[node.output] = !persistent or result != hint.?;
            for (node.inputs[0..node.input_count]) |id| {
                if (id != self.output and self.last_use[id] == i and owned[id]) {
                    cb.free(values[id].?);
                    values[id] = null;
                    owned[id] = false;
                }
            }
        }
        owned[self.output] = false;
        self.replays += 1;
        return values[self.output] orelse error.AudioScheduleInvalid;
    }
};
pub const Capture = struct {
    plan: *Plan,
    failed: bool = false,
    handles: [capacity]?CT = @splat(null),
    nodes: [1024]Node = undefined,
    constants: [512]Constant = undefined,
    pub fn init(plan: *Plan, inputs: []const CT) Capture {
        std.debug.assert(!plan.metadata_owned);
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
        if (self.plan.count == 1024 or self.plan.next_id == capacity) {
            self.failed = true;
            return;
        }
        var n = node;
        n.output = self.plan.next_id;
        self.plan.next_id += 1;
        for (n.inputs[0..n.input_count]) |id| self.plan.last_use[id] = @intCast(self.plan.count);
        self.handles[n.output] = ct;
        self.plan.nodes = &self.nodes;
        self.plan.nodes[self.plan.count] = n;
        self.plan.count += 1;
    }
    pub fn finish(self: *Capture, ct: CT) void {
        self.plan.output = self.find(ct) orelse {
            self.failed = true;
            self.plan.ready = false;
            self.plan.disabled = true;
            return;
        };
        self.plan.ready = !self.failed and self.plan.count > 0;
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
    fn run(self: *const Node, cb: *const ops.ComputeBackend, values: *[capacity]?CT, hint: ?CT) !CT {
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
            return (try linear(cb.ptr, values[id].?, slot, d.rows, d.in_dim, d.out_dim, p, hint)) orelse error.AudioSchedulePreparedSlotMissing;
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
                return stagedClippedLinear(cb, values[a.input].?, values[a.weight].?, a.p);
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
                return (try cb.vtable.gemma4AudioLocalAttention.?(cb.ptr, values[a.q].?, values[a.k].?, values[a.v].?, values[a.rel].?, values[a.q_dim_scales].?, values[a.valid].?, a.params)) orelse error.AudioScheduleRouteChanged;
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

/// A declining optional route must not invalidate a correctly recorded request.
pub fn stagedClippedLinear(cb: *const ops.ComputeBackend, input: CT, weight: CT, p: ops.ClippedLinearParams) !CT {
    const lo = if (p.flags & 1 != 0) @as(?f32, p.input_min) else null;
    const hi = if (p.flags & 2 != 0) @as(?f32, p.input_max) else null;
    const clipped = if (p.flags & 3 != 0) (try cb.clampScalar(input, lo, hi)) orelse return error.AudioScheduleRouteChanged else input;
    defer if (clipped != input) cb.free(clipped);
    const result = try cb.linearNoBias(clipped, weight, p.rows, p.in_dim, p.out_dim);
    errdefer cb.free(result);
    const omin = if (p.flags & 4 != 0) @as(?f32, p.output_min) else null;
    const omax = if (p.flags & 8 != 0) @as(?f32, p.output_max) else null;
    if (p.flags & 16 != 0) {
        if (try cb.clampSilu(result, omin, omax)) |out| {
            cb.free(result);
            return out;
        }
    }
    const bounded = if (p.flags & 12 != 0) (try cb.clampScalar(result, omin, omax)) orelse return error.AudioScheduleRouteChanged else result;
    if (p.flags & 16 != 0) {
        errdefer if (bounded != result) cb.free(bounded);
        const out = try cb.silu(bounded);
        if (bounded != result) cb.free(bounded);
        cb.free(result);
        return out;
    }
    if (bounded != result) cb.free(result);
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
