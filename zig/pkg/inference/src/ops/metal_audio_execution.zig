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

// A bounded host instruction tape. It retains names and scalar parameters,
// never request tensors, allocators, activations, or weight pins. Replay binds
// current inputs and immutable model entries, then frees temporaries at last use.
const std = @import("std");
const ops = @import("ops.zig");
const CT = ops.CT;
pub const capacity = 2048;
pub const Constant = struct { id: u16, model: bool, name: [256]u8, len: u16 };
pub const Plan = struct {
    key: u64 = 0,
    stamp: u64 = 0,
    warmed: bool = false,
    ready: bool = false,
    disabled: bool = false,
    replays: u64 = 0,
    eager_ns: u64 = 0,
    replay_ns: u64 = 0,
    nodes: [1024]Node = undefined,
    count: usize = 0,
    constants: [512]Constant = undefined,
    constant_count: usize = 0,
    last_use: [capacity]u16 = @splat(0),
    next_id: u16 = 0,
    output: u16 = 0,
    pub fn replay(self: *Plan, cb: *const ops.ComputeBackend, inputs: []const CT) !CT {
        var values: [capacity]?CT = @splat(null);
        var owned: [capacity]bool = @splat(false);
        defer for (values, owned) |value, own| {
            if (own) cb.free(value.?);
        };
        for (inputs, 0..) |ct, i| values[i] = ct;
        for (self.constants[0..self.constant_count]) |c| {
            const name = c.name[0..c.len];
            values[c.id] = if (c.model) (try cb.modelConstant(name, null)) orelse return error.AudioScheduleConstantMissing else try cb.getWeight(name);
            owned[c.id] = true;
        }
        for (self.nodes[0..self.count], 0..) |*node, i| {
            try cb.checkExecutionControl();
            values[node.output] = try node.run(cb, &values);
            owned[node.output] = true;
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
    pub fn init(plan: *Plan, inputs: []const CT) Capture {
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
pub const Node = struct {
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
    fn run(self: *const Node, cb: *const ops.ComputeBackend, values: *[capacity]?CT) !CT {
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
                return (try cb.vtable.clippedLinear.?(cb.ptr, values[a.input].?, values[a.weight].?, a.p)) orelse error.AudioScheduleRouteChanged;
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
