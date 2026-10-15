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

//! Optional cuDNN 9 fused SDPA for official EmbeddingGemma2 text and vision.
//! The ABI is loaded dynamically so ordinary CUDA installations do not gain
//! a link-time cuDNN dependency.
const std = @import("std");

const Desc = ?*anyopaque;
const Handle = ?*anyopaque;
const Status = c_int;
const success: Status = 0;

const DescriptorType = enum(c_int) { enginecfg = 3, engineheur = 4, execution_plan = 5, operation_graph = 15, variant_pack = 16, tensor = 17, sdpa_fwd = 41, diagonal_band = 44 };
const AttributeType = enum(c_int) { handle = 0, data_type = 1, boolean = 2, int64 = 3, float = 4, void_ptr = 6, heur_mode = 8, pointwise_mode = 14, backend_descriptor = 15 };

const Table = struct {
    get_version: *const fn () callconv(.c) usize,
    create: *const fn (*Handle) callconv(.c) Status,
    destroy: *const fn (Handle) callconv(.c) Status,
    set_stream: *const fn (Handle, ?*anyopaque) callconv(.c) Status,
    create_desc: *const fn (DescriptorType, *Desc) callconv(.c) Status,
    destroy_desc: *const fn (Desc) callconv(.c) Status,
    set: *const fn (Desc, c_int, AttributeType, i64, ?*const anyopaque) callconv(.c) Status,
    get: *const fn (Desc, c_int, AttributeType, i64, *i64, ?*anyopaque) callconv(.c) Status,
    finalize: *const fn (Desc) callconv(.c) Status,
    execute: *const fn (Handle, Desc, Desc) callconv(.c) Status,
};

pub const Layout = struct { dims: [4]i64, strides: [4]i64 };

/// Per-row valid lengths for binary masks with a nonempty contiguous prefix.
/// Interior holes and left padding need the general masked attention kernels.
pub fn rightPaddedLengths(mask: []const i64, batch: usize, sequence: usize, lengths: []i32) bool {
    if (batch == 0 or sequence == 0 or sequence > std.math.maxInt(i32) or lengths.len != batch) return false;
    const elements = std.math.mul(usize, batch, sequence) catch return false;
    if (mask.len != elements) return false;
    for (0..batch) |b| {
        var count: usize = 0;
        var padding = false;
        for (mask[b * sequence ..][0..sequence]) |value| {
            if (value == 0) {
                padding = true;
            } else if (value == 1 and !padding) {
                count += 1;
            } else return false;
        }
        if (count == 0) return false;
        lengths[b] = @intCast(count);
    }
    return true;
}

pub fn bshdLayout(batch: usize, heads: usize, sequence: usize, dim: usize) !Layout {
    if (batch == 0 or heads == 0 or sequence == 0 or dim == 0) return error.InvalidShape;
    const hd = try std.math.mul(usize, heads, dim);
    const shd = try std.math.mul(usize, sequence, hd);
    inline for (.{ batch, heads, sequence, dim, hd, shd }) |value| {
        if (value > std.math.maxInt(i64)) return error.InvalidShape;
    }
    return .{
        .dims = .{ @intCast(batch), @intCast(heads), @intCast(sequence), @intCast(dim) },
        .strides = .{ @intCast(shd), @intCast(dim), @intCast(hd), 1 },
    };
}

const Plan = struct {
    key: Key = .{},
    q: Desc = null,
    k: Desc = null,
    v: Desc = null,
    o: Desc = null,
    scale: Desc = null,
    seq_len_q: Desc = null,
    seq_len_kv: Desc = null,
    left_bound: Desc = null,
    right_bound: Desc = null,
    negative_inf: Desc = null,
    scores: [3]Desc = @splat(null),
    masks: [2]Desc = @splat(null),
    subgraph: Desc = null,
    operation: Desc = null,
    graph: Desc = null,
    engine_config: Desc = null,
    execution: Desc = null,
    workspace_bytes: usize = 0,

    fn deinit(self: *Plan, f: *const Table) void {
        inline for (.{ "execution", "engine_config", "graph", "operation", "subgraph" }) |field| {
            if (@field(self, field) != null) _ = f.destroy_desc(@field(self, field));
        }
        for (&self.masks) |*d| {
            if (d.* != null) _ = f.destroy_desc(d.*);
        }
        for (&self.scores) |*d| {
            if (d.* != null) _ = f.destroy_desc(d.*);
        }
        inline for (.{ "negative_inf", "right_bound", "left_bound", "seq_len_kv", "seq_len_q", "scale", "o", "v", "k", "q" }) |field| {
            if (@field(self, field) != null) _ = f.destroy_desc(@field(self, field));
        }
        self.* = .{};
    }
};

pub const Key = struct {
    batch: usize = 0,
    q_heads: usize = 0,
    kv_heads: usize = 0,
    sequence: usize = 0,
    dim: usize = 0,
    scale: f32 = 1,
    local_window_radius: usize = 0,
    padding_mask: bool = false,

    fn eql(a: Key, b: Key) bool {
        return a.batch == b.batch and a.q_heads == b.q_heads and a.kv_heads == b.kv_heads and
            a.sequence == b.sequence and a.dim == b.dim and a.local_window_radius == b.local_window_radius and a.padding_mask == b.padding_mask and
            @as(u32, @bitCast(a.scale)) == @as(u32, @bitCast(b.scale));
    }
};

fn keyFingerprint(key: Key) usize {
    var value: usize = key.batch;
    inline for (.{ key.q_heads, key.kv_heads, key.sequence, key.dim, key.local_window_radius, @as(usize, @intFromBool(key.padding_mask)), @as(usize, @intCast(@as(u32, @bitCast(key.scale)))) }) |part|
        value = (value ^ part) *% 0x9e3779b1;
    return value;
}

pub const Module = struct {
    pub const Stats = struct {
        plan_hits: u64 = 0,
        plan_misses: u64 = 0,
        plan_declines: u64 = 0,
        executes: u64 = 0,
        vision_executes: u64 = 0,
        text_executes: u64 = 0,
        workspace_high_water: usize = 0,
    };
    lib: std.DynLib,
    f: Table,
    handle: Handle,
    plans: [16]Plan = @splat(.{}),
    plan_count: usize = 0,
    declined: [16]usize = @splat(0),
    declined_count: usize = 0,
    stats_: Stats = .{},
    version: usize,

    pub fn stats(self: *const Module) Stats {
        return self.stats_;
    }

    pub fn init(stream: ?*anyopaque) !Module {
        var lib = std.DynLib.open("libcudnn.so.9") catch return error.CudnnUnavailable;
        errdefer lib.close();
        const f = Table{
            .get_version = try symbol(&lib, @TypeOf(@as(Table, undefined).get_version), "cudnnGetVersion"),
            .create = try symbol(&lib, @TypeOf(@as(Table, undefined).create), "cudnnCreate"),
            .destroy = try symbol(&lib, @TypeOf(@as(Table, undefined).destroy), "cudnnDestroy"),
            .set_stream = try symbol(&lib, @TypeOf(@as(Table, undefined).set_stream), "cudnnSetStream"),
            .create_desc = try symbol(&lib, @TypeOf(@as(Table, undefined).create_desc), "cudnnBackendCreateDescriptor"),
            .destroy_desc = try symbol(&lib, @TypeOf(@as(Table, undefined).destroy_desc), "cudnnBackendDestroyDescriptor"),
            .set = try symbol(&lib, @TypeOf(@as(Table, undefined).set), "cudnnBackendSetAttribute"),
            .get = try symbol(&lib, @TypeOf(@as(Table, undefined).get), "cudnnBackendGetAttribute"),
            .finalize = try symbol(&lib, @TypeOf(@as(Table, undefined).finalize), "cudnnBackendFinalize"),
            .execute = try symbol(&lib, @TypeOf(@as(Table, undefined).execute), "cudnnBackendExecute"),
        };
        const version = f.get_version();
        if (version < 91300) return error.CudnnVersionUnsupported;
        var handle: Handle = null;
        try check(f.create(&handle));
        errdefer _ = f.destroy(handle);
        try check(f.set_stream(handle, stream));
        return .{ .lib = lib, .f = f, .handle = handle, .version = version };
    }

    pub fn deinit(self: *Module) void {
        for (&self.plans) |*entry| entry.deinit(&self.f);
        if (self.handle != null) _ = self.f.destroy(self.handle);
        self.lib.close();
        self.* = undefined;
    }

    pub fn workspaceBytes(self: *Module, sequence: usize) !?usize {
        return self.workspaceBytesFor(.{ .batch = 1, .q_heads = 12, .kv_heads = 12, .sequence = sequence, .dim = 64, .scale = 0.125 });
    }

    pub fn workspaceBytesFor(self: *Module, key: Key) !?usize {
        const entry = try self.getPlan(key) orelse return null;
        return entry.workspace_bytes;
    }

    pub fn execute(self: *Module, sequence: usize, q: usize, k: usize, v: usize, o: usize, workspace: usize) !bool {
        const result = try self.executeFor(.{ .batch = 1, .q_heads = 12, .kv_heads = 12, .sequence = sequence, .dim = 64, .scale = 0.125 }, q, k, v, o, workspace, 0);
        if (result) self.stats_.vision_executes += 1;
        return result;
    }

    pub fn executeText(self: *Module, key: Key, q: usize, k: usize, v: usize, o: usize, workspace: usize) !bool {
        const result = try self.executeFor(key, q, k, v, o, workspace, 0);
        if (result) self.stats_.text_executes += 1;
        return result;
    }

    pub fn executeTextPadded(self: *Module, key: Key, q: usize, k: usize, v: usize, o: usize, workspace: usize, lengths: usize) !bool {
        const result = try self.executeFor(key, q, k, v, o, workspace, lengths);
        if (result) self.stats_.text_executes += 1;
        return result;
    }

    fn executeFor(self: *Module, key: Key, q: usize, k: usize, v: usize, o: usize, workspace: usize, lengths: usize) !bool {
        if (key.padding_mask != (lengths != 0)) return error.InvalidShape;
        const entry = try self.getPlan(key) orelse return false;
        var variant: Desc = null;
        try check(self.f.create_desc(.variant_pack, &variant));
        defer _ = self.f.destroy_desc(variant);
        var ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var scale = key.scale;
        var left: i32 = @intCast(key.local_window_radius + 1);
        var right: i32 = @intCast(key.local_window_radius);
        var negative_inf: f32 = -std.math.inf(f32);
        var pointers = [_]?*anyopaque{ @ptrFromInt(q), @ptrFromInt(k), @ptrFromInt(v), @ptrFromInt(o), @ptrCast(&scale), @ptrCast(&left), @ptrCast(&right), @ptrCast(&negative_inf), null, null };
        var binding_count: usize = if (key.local_window_radius == 0) 5 else 8;
        if (key.padding_mask) {
            ids[binding_count] = 9;
            ids[binding_count + 1] = 10;
            pointers[binding_count] = @ptrFromInt(lengths);
            pointers[binding_count + 1] = @ptrFromInt(lengths);
            binding_count += 2;
        }
        var workspace_ptr: ?*anyopaque = if (workspace == 0) null else @ptrFromInt(workspace);
        try check(self.f.set(variant, 1000, .int64, @intCast(binding_count), @ptrCast(&ids)));
        try check(self.f.set(variant, 1001, .void_ptr, @intCast(binding_count), @ptrCast(&pointers)));
        try check(self.f.set(variant, 1003, .void_ptr, 1, @ptrCast(&workspace_ptr)));
        try check(self.f.finalize(variant));
        try check(self.f.execute(self.handle, entry.execution, variant));
        self.stats_.executes += 1;
        return true;
    }

    fn getPlan(self: *Module, key: Key) !?*Plan {
        if (!eligible(key, self.version)) return null;
        for (self.plans[0..self.plan_count]) |*p| if (p.key.eql(key)) {
            self.stats_.plan_hits += 1;
            return p;
        };
        const fingerprint = keyFingerprint(key);
        for (self.declined[0..self.declined_count]) |value| if (value == fingerprint) {
            self.stats_.plan_declines += 1;
            return null;
        };
        if (self.plan_count == self.plans.len) {
            self.stats_.plan_declines += 1;
            return null;
        }
        self.stats_.plan_misses += 1;
        var candidate: Plan = .{ .key = key };
        errdefer candidate.deinit(&self.f);
        self.buildPlan(&candidate) catch {
            candidate.deinit(&self.f);
            if (self.declined_count < self.declined.len) {
                self.declined[self.declined_count] = fingerprint;
                self.declined_count += 1;
            }
            self.stats_.plan_declines += 1;
            return null;
        };
        self.plans[self.plan_count] = candidate;
        self.plan_count += 1;
        return &self.plans[self.plan_count - 1];
    }

    fn buildPlan(self: *Module, p: *Plan) !void {
        const q_layout = try bshdLayout(p.key.batch, p.key.q_heads, p.key.sequence, p.key.dim);
        const kv_layout = try bshdLayout(p.key.batch, p.key.kv_heads, p.key.sequence, p.key.dim);
        try self.tensor(&p.q, 1, q_layout, 9, false);
        try self.tensor(&p.k, 2, kv_layout, 9, false);
        try self.tensor(&p.v, 3, kv_layout, 9, false);
        try self.tensor(&p.o, 4, q_layout, 9, false);
        try self.tensor(&p.scale, 5, .{ .dims = .{ 1, 1, 1, 1 }, .strides = .{ 1, 1, 1, 1 } }, 0, true);
        if (p.key.padding_mask) {
            const length_layout: Layout = .{ .dims = .{ @intCast(p.key.batch), 1, 1, 1 }, .strides = .{ 1, 1, 1, 1 } };
            try self.tensor(&p.seq_len_q, 9, length_layout, 4, false);
            try self.tensor(&p.seq_len_kv, 10, length_layout, 4, false);
        }
        if (p.key.local_window_radius != 0) {
            const scalar_layout: Layout = .{ .dims = .{ 1, 1, 1, 1 }, .strides = .{ 1, 1, 1, 1 } };
            try self.tensor(&p.left_bound, 6, scalar_layout, 4, true);
            try self.tensor(&p.right_bound, 7, scalar_layout, 4, true);
            try self.tensor(&p.negative_inf, 8, scalar_layout, 0, true);
            const score_layout = try scoreLayout(p.key.batch, p.key.q_heads, p.key.sequence);
            for (&p.scores, 0..) |*score, i| try self.virtualTensor(score, @intCast(100 + i), score_layout, 0);
            for (&p.masks, 0..) |*mask, i| {
                try check(self.f.create_desc(.diagonal_band, mask));
                var x = p.scores[i];
                var y = p.scores[i + 1];
                var bound = if (i == 0) p.right_bound else p.left_bound;
                var fill = p.negative_inf;
                try check(self.f.set(mask.*, 3000, .backend_descriptor, 1, @ptrCast(&x)));
                try check(self.f.set(mask.*, if (i == 0) 3004 else 3003, .backend_descriptor, 1, @ptrCast(&bound)));
                try check(self.f.set(mask.*, 3005, .backend_descriptor, 1, @ptrCast(&fill)));
                try check(self.f.set(mask.*, 3006, .backend_descriptor, 1, @ptrCast(&y)));
                var comparison: c_int = if (i == 0) 303 else 304;
                try check(self.f.set(mask.*, 3007, .pointwise_mode, 1, @ptrCast(&comparison)));
                try check(self.f.finalize(mask.*));
            }
            try check(self.f.create_desc(.operation_graph, &p.subgraph));
            var handle = self.handle;
            try check(self.f.set(p.subgraph, 800, .handle, 1, @ptrCast(&handle)));
            try check(self.f.set(p.subgraph, 801, .backend_descriptor, p.masks.len, @ptrCast(&p.masks)));
            try check(self.f.finalize(p.subgraph));
        }
        try check(self.f.create_desc(.sdpa_fwd, &p.operation));
        inline for (.{ .{ 2800, "q" }, .{ 2801, "k" }, .{ 2802, "v" }, .{ 2803, "o" }, .{ 2805, "scale" } }) |item| {
            var d = @field(p, item[1]);
            try check(self.f.set(p.operation, item[0], .backend_descriptor, 1, @ptrCast(&d)));
        }
        if (p.key.padding_mask) {
            inline for (.{ .{ 2809, "seq_len_q" }, .{ 2810, "seq_len_kv" } }) |item| {
                var d = @field(p, item[1]);
                try check(self.f.set(p.operation, item[0], .backend_descriptor, 1, @ptrCast(&d)));
            }
        }
        if (p.key.local_window_radius != 0) {
            var subgraph = p.subgraph;
            var input_uid: i64 = 100;
            var output_uid: i64 = 102;
            try check(self.f.set(p.operation, 2811, .backend_descriptor, 1, @ptrCast(&subgraph)));
            try check(self.f.set(p.operation, 2812, .int64, 1, @ptrCast(&input_uid)));
            try check(self.f.set(p.operation, 2813, .int64, 1, @ptrCast(&output_uid)));
        }
        try check(self.f.finalize(p.operation));
        try check(self.f.create_desc(.operation_graph, &p.graph));
        var handle = self.handle;
        var op = p.operation;
        try check(self.f.set(p.graph, 800, .handle, 1, @ptrCast(&handle)));
        try check(self.f.set(p.graph, 801, .backend_descriptor, 1, @ptrCast(&op)));
        try check(self.f.finalize(p.graph));
        var heur: Desc = null;
        try check(self.f.create_desc(.engineheur, &heur));
        defer _ = self.f.destroy_desc(heur);
        var graph = p.graph;
        var mode: c_int = 3;
        try check(self.f.set(heur, 201, .backend_descriptor, 1, @ptrCast(&graph)));
        try check(self.f.set(heur, 200, .heur_mode, 1, @ptrCast(&mode)));
        try check(self.f.finalize(heur));
        var available: i64 = 0;
        try check(self.f.get(heur, 202, .backend_descriptor, 0, &available, null));
        if (available <= 0) return error.CudnnUnsupported;
        const requested: usize = @min(@as(usize, @intCast(available)), 8);
        var configs: [8]Desc = @splat(null);
        var created: usize = 0;
        defer {
            for (configs[0..created]) |config| {
                if (config != null and config != p.engine_config) _ = self.f.destroy_desc(config);
            }
        }
        while (created < requested) : (created += 1) try check(self.f.create_desc(.enginecfg, &configs[created]));
        var count: i64 = 0;
        try check(self.f.get(heur, 202, .backend_descriptor, @intCast(requested), &count, @ptrCast(&configs)));
        if (count <= 0 or count > @as(i64, @intCast(requested))) return error.CudnnUnsupported;
        for (configs[0..@intCast(count)]) |config| {
            var execution: Desc = null;
            try check(self.f.create_desc(.execution_plan, &execution));
            var ec = config;
            var handle2 = self.handle;
            if (self.f.set(execution, 400, .handle, 1, @ptrCast(&handle2)) == success and
                self.f.set(execution, 401, .backend_descriptor, 1, @ptrCast(&ec)) == success and
                self.f.finalize(execution) == success)
            {
                p.engine_config = config;
                p.execution = execution;
                break;
            }
            _ = self.f.destroy_desc(execution);
        }
        if (p.execution == null) return error.CudnnUnsupported;
        var workspace: i64 = 0;
        count = 0;
        try check(self.f.get(p.execution, 402, .int64, 1, &count, @ptrCast(&workspace)));
        if (count != 1 or workspace < 0) return error.CudnnUnsupported;
        p.workspace_bytes = @intCast(workspace);
        self.stats_.workspace_high_water = @max(self.stats_.workspace_high_water, p.workspace_bytes);
    }

    fn tensor(self: *Module, out: *Desc, uid: i64, layout: Layout, dtype: c_int, by_value: bool) !void {
        try check(self.f.create_desc(.tensor, out));
        var dt = dtype;
        var alignment: i64 = 16;
        var id = uid;
        var bv: c_int = @intFromBool(by_value);
        try check(self.f.set(out.*, 901, .data_type, 1, @ptrCast(&dt)));
        try check(self.f.set(out.*, 902, .int64, 4, @ptrCast(&layout.dims)));
        try check(self.f.set(out.*, 903, .int64, 4, @ptrCast(&layout.strides)));
        try check(self.f.set(out.*, 900, .int64, 1, @ptrCast(&alignment)));
        try check(self.f.set(out.*, 906, .int64, 1, @ptrCast(&id)));
        if (by_value) try check(self.f.set(out.*, 908, .boolean, 1, @ptrCast(&bv)));
        try check(self.f.finalize(out.*));
    }

    fn virtualTensor(self: *Module, out: *Desc, uid: i64, layout: Layout, dtype: c_int) !void {
        try check(self.f.create_desc(.tensor, out));
        var dt = dtype;
        var alignment: i64 = 16;
        var id = uid;
        var yes: c_int = 1;
        try check(self.f.set(out.*, 901, .data_type, 1, @ptrCast(&dt)));
        try check(self.f.set(out.*, 902, .int64, 4, @ptrCast(&layout.dims)));
        try check(self.f.set(out.*, 903, .int64, 4, @ptrCast(&layout.strides)));
        try check(self.f.set(out.*, 900, .int64, 1, @ptrCast(&alignment)));
        try check(self.f.set(out.*, 906, .int64, 1, @ptrCast(&id)));
        try check(self.f.set(out.*, 907, .boolean, 1, @ptrCast(&yes)));
        try check(self.f.finalize(out.*));
    }
};

fn eligible(key: Key, version: usize) bool {
    if (key.batch == 0 or key.batch > 32 or key.sequence == 0 or !std.math.isFinite(key.scale)) return false;
    if (key.padding_mask and version < 91500) return false;
    const scale_bits: u32 = @bitCast(key.scale);
    const vision = !key.padding_mask and key.local_window_radius == 0 and key.batch == 1 and key.q_heads == 12 and key.kv_heads == 12 and
        key.dim == 64 and key.sequence <= 3136 and scale_bits == @as(u32, @bitCast(@as(f32, 0.125)));
    const text_base = key.q_heads == 4 and key.kv_heads == 2 and key.dim == 256 and
        scale_bits == @as(u32, @bitCast(@as(f32, 1.0)));
    const text_plain = text_base and key.local_window_radius == 0 and key.sequence <= 513;
    const text_band = text_base and key.local_window_radius == 512 and key.sequence <= 8192 and version >= 92100;
    return vision or text_plain or text_band;
}

fn scoreLayout(batch: usize, heads: usize, sequence: usize) !Layout {
    const ss = try std.math.mul(usize, sequence, sequence);
    const hss = try std.math.mul(usize, heads, ss);
    inline for (.{ batch, heads, sequence, ss, hss }) |value| if (value > std.math.maxInt(i64)) return error.InvalidShape;
    return .{ .dims = .{ @intCast(batch), @intCast(heads), @intCast(sequence), @intCast(sequence) }, .strides = .{ @intCast(hss), @intCast(ss), @intCast(sequence), 1 } };
}

fn check(status: Status) !void {
    if (status != success) return error.CudnnFailure;
}

fn symbol(lib: *std.DynLib, comptime T: type, name: [:0]const u8) !T {
    return lib.lookup(T, name) orelse error.CudnnSymbolMissing;
}

test "cuDNN BSHD layout is expressed as logical BHSD" {
    const layout = try bshdLayout(2, 12, 2394, 64);
    try std.testing.expectEqual([4]i64{ 2, 12, 2394, 64 }, layout.dims);
    try std.testing.expectEqual([4]i64{ 1_838_592, 64, 768, 1 }, layout.strides);
    try std.testing.expectError(error.InvalidShape, bshdLayout(1, 12, 0, 64));
    try std.testing.expectError(error.Overflow, bshdLayout(1, std.math.maxInt(usize), 2, 2));
}

test "cuDNN keys isolate local windows and enforce qualified shapes" {
    const plain = Key{ .batch = 8, .q_heads = 4, .kv_heads = 2, .sequence = 512, .dim = 256, .scale = 1 };
    var band = plain;
    band.local_window_radius = 512;
    try std.testing.expect(!plain.eql(band));
    try std.testing.expect(keyFingerprint(plain) != keyFingerprint(band));
    try std.testing.expect(eligible(plain, 91300));
    try std.testing.expect(!eligible(band, 92000));
    try std.testing.expect(eligible(band, 92100));
    band.sequence = 8192;
    try std.testing.expect(eligible(band, 92400));
    band.sequence = 8193;
    try std.testing.expect(!eligible(band, 92400));
    try std.testing.expect(eligible(.{ .batch = 1, .q_heads = 12, .kv_heads = 12, .sequence = 2394, .dim = 64, .scale = 0.125 }, 91300));
}

test "cuDNN padding lengths reject masks that cannot be represented by prefixes" {
    var lengths: [2]i32 = undefined;
    try std.testing.expect(rightPaddedLengths(&.{ 1, 1, 1, 1, 1, 0, 0, 0 }, 2, 4, &lengths));
    try std.testing.expectEqual([2]i32{ 4, 1 }, lengths);
    try std.testing.expect(!rightPaddedLengths(&.{ 0, 1, 1, 1, 1, 1, 0, 0 }, 2, 4, &lengths));
    try std.testing.expect(!rightPaddedLengths(&.{ 1, 0, 1, 0, 1, 1, 0, 0 }, 2, 4, &lengths));
    try std.testing.expect(!rightPaddedLengths(&.{ 1, 1, 0, 0, 0, 0, 0, 0 }, 2, 4, &lengths));
    try std.testing.expect(!rightPaddedLengths(&.{ 1, 2, 0, 0, 1, 1, 0, 0 }, 2, 4, &lengths));
    try std.testing.expect(!rightPaddedLengths(&.{ 1, 1, 0, 0, 1, 1, -1, 0 }, 2, 4, &lengths));
}

test "cuDNN padding lengths validate input and output dimensions" {
    var lengths: [2]i32 = undefined;
    try std.testing.expect(!rightPaddedLengths(&.{ 1, 1, 0 }, 2, 2, &lengths));
    try std.testing.expect(!rightPaddedLengths(&.{ 1, 1, 0, 0 }, 2, 2, lengths[0..1]));
    try std.testing.expect(!rightPaddedLengths(&.{}, 0, 0, &lengths));
    try std.testing.expect(!rightPaddedLengths(&.{}, 2, std.math.maxInt(usize), &lengths));
}

test "cuDNN padded and unpadded plans have separate version-qualified keys" {
    const plain = Key{ .batch = 8, .q_heads = 4, .kv_heads = 2, .sequence = 512, .dim = 256, .scale = 1 };
    var padded = plain;
    padded.padding_mask = true;
    try std.testing.expect(!plain.eql(padded));
    try std.testing.expect(keyFingerprint(plain) != keyFingerprint(padded));
    try std.testing.expect(eligible(plain, 91300));
    try std.testing.expect(!eligible(padded, 91400));
    try std.testing.expect(eligible(padded, 91500));
    padded.sequence = 8192;
    padded.local_window_radius = 512;
    try std.testing.expect(!eligible(padded, 92000));
    try std.testing.expect(eligible(padded, 92400));
    try std.testing.expect(!eligible(.{ .batch = 1, .q_heads = 12, .kv_heads = 12, .sequence = 512, .dim = 64, .scale = 0.125, .padding_mask = true }, 92400));
}
