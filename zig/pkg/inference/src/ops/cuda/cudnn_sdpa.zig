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
const platform = @import("antfly_platform");

// Covers the unmerged vision patches of full-resolution OCR pages, including
// square pages. Decoder tokens are reduced by the model's spatial merger.
pub const max_ocr_vision_sequence: usize = 16_384;

const Desc = ?*anyopaque;
const Handle = ?*anyopaque;
const Status = c_int;
const success: Status = 0;

const backend = @import("cudnn_backend.zig");
const DescriptorType = backend.DescriptorType;
const AttributeType = backend.AttributeType;

const Table = struct {
    get_version: *const fn () callconv(.c) usize,
    create: *const fn (*Handle) callconv(.c) Status,
    destroy: *const fn (Handle) callconv(.c) Status,
    set_stream: *const fn (Handle, ?*anyopaque) callconv(.c) Status,
    create_desc: @FieldType(backend.Api, "create_desc"),
    destroy_desc: @FieldType(backend.Api, "destroy_desc"),
    set: @FieldType(backend.Api, "set"),
    get: @FieldType(backend.Api, "get"),
    finalize: @FieldType(backend.Api, "finalize"),
    execute: @FieldType(backend.Api, "execute"),
};

// A failed SDK destruction does not surrender ownership. Checked primitives
// preserve the exact pointer for a caller-owned retry; production void
// destructors fail closed before forgetting it or unloading its library.
fn destroyDescriptorChecked(f: *const Table, owner: *Desc) !void {
    if (owner.* == null) return;
    if (f.destroy_desc(owner.*) != success) return error.CudnnDescriptorDestroyFailed;
    owner.* = null;
}
fn destroyDescriptor(f: *const Table, owner: *Desc) void {
    destroyDescriptorChecked(f, owner) catch platform.inference_process_supervisor.restartWorker();
}
fn destroyHandleChecked(f: *const Table, owner: *Handle) !void {
    if (owner.* == null) return;
    if (f.destroy(owner.*) != success) return error.CudnnHandleDestroyFailed;
    owner.* = null;
}
fn destroyHandle(f: *const Table, owner: *Handle) void {
    destroyHandleChecked(f, owner) catch platform.inference_process_supervisor.restartWorker();
}

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
        self.deinitChecked(f) catch platform.inference_process_supervisor.restartWorker();
    }
    fn deinitChecked(self: *Plan, f: *const Table) !void {
        inline for (.{ "execution", "engine_config", "graph", "operation", "subgraph" }) |field| {
            try destroyDescriptorChecked(f, &@field(self, field));
        }
        for (&self.masks) |*d| {
            try destroyDescriptorChecked(f, d);
        }
        for (&self.scores) |*d| {
            try destroyDescriptorChecked(f, d);
        }
        inline for (.{ "negative_inf", "right_bound", "left_bound", "seq_len_kv", "seq_len_q", "scale", "o", "v", "k", "q" }) |field| {
            try destroyDescriptorChecked(f, &@field(self, field));
        }
        self.* = .{};
    }
};

/// Vendor-default SDPA does not expose instruction quantum, online traversal
/// or row-reduction order. Fixed schedules require a separately proven backend.
pub const NumericalContract = enum { vendor_selected, flash_bf16, efficient_bf16 };
pub fn supportsNumericalContract(contract: NumericalContract) bool {
    return contract == .vendor_selected;
}

pub const Key = struct {
    batch: usize = 0,
    q_heads: usize = 0,
    kv_heads: usize = 0,
    sequence: usize = 0,
    dim: usize = 0,
    scale: f32 = 1,
    local_window_radius: usize = 0,
    padding_mask: bool = false,
    ocr_vision: bool = false,
    florence_vision: bool = false,
    dense_seq2seq: bool = false,
    dtype: enum { bf16, f16 } = .bf16,
    numerical_contract: NumericalContract = .vendor_selected,

    fn eql(a: Key, b: Key) bool {
        return a.numerical_contract == b.numerical_contract and a.batch == b.batch and a.q_heads == b.q_heads and a.kv_heads == b.kv_heads and
            a.sequence == b.sequence and a.dim == b.dim and a.florence_vision == b.florence_vision and a.dense_seq2seq == b.dense_seq2seq and a.dtype == b.dtype and a.ocr_vision == b.ocr_vision and a.local_window_radius == b.local_window_radius and a.padding_mask == b.padding_mask and
            @as(u32, @bitCast(a.scale)) == @as(u32, @bitCast(b.scale));
    }
};

fn keyFingerprint(key: Key) usize {
    var value: usize = key.batch;
    inline for (.{ @as(usize, @backingInt(key.numerical_contract)), key.q_heads, key.kv_heads, key.sequence, key.dim, key.local_window_radius, @as(usize, @intFromBool(key.florence_vision)), @as(usize, @intFromBool(key.dense_seq2seq)), @as(usize, @backingInt(key.dtype)), @as(usize, @intFromBool(key.ocr_vision)), @as(usize, @intFromBool(key.padding_mask)), @as(usize, @intCast(@as(u32, @bitCast(key.scale)))) }) |part|
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
        vision_batched_executes: u64 = 0,
        text_executes: u64 = 0,
        workspace_high_water: usize = 0,
    };
    lib: std.DynLib,
    f: Table,
    handle: Handle,
    plans: [64]Plan = @splat(.{}),
    plan_count: usize = 0,
    declined: [16]usize = @splat(0),
    declined_count: usize = 0,
    stats_: Stats = .{},
    version: usize,

    pub fn stats(self: *const Module) Stats {
        return self.stats_;
    }

    pub fn init(stream: ?*anyopaque) !Module {
        var lib = try @import("libraries.zig").openCudnnLibrary();
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
        errdefer destroyHandle(&f, &handle);
        try check(f.create(&handle));
        try check(f.set_stream(handle, stream));
        return .{ .lib = lib, .f = f, .handle = handle, .version = version };
    }

    /// Library remains loaded until every owned descriptor/handle died.
    /// A failed checked retirement leaves that handle and later owners intact.
    fn retireOwnedChecked(self: *Module) !void {
        for (&self.plans) |*entry| try entry.deinitChecked(&self.f);
        try destroyHandleChecked(&self.f, &self.handle);
        self.plan_count = 0;
    }
    pub fn deinit(self: *Module) void {
        self.retireOwnedChecked() catch platform.inference_process_supervisor.restartWorker();
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

    pub fn executeVision(self: *Module, key: Key, q: usize, k: usize, v: usize, o: usize, workspace: usize) !bool {
        const result = try self.executeFor(key, q, k, v, o, workspace, 0);
        if (result) {
            self.stats_.vision_executes += 1;
            if (key.batch > 1) self.stats_.vision_batched_executes += 1;
        }
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
        defer destroyDescriptor(&self.f, &variant);
        try check(self.f.create_desc(.variant_pack, &variant));
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
        if (!supportsNumericalContract(key.numerical_contract)) {
            self.stats_.plan_declines += 1;
            return null;
        }
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
        const plan_limit: usize = if (key.florence_vision) self.plans.len else if (key.ocr_vision) 64 else 16;
        if (self.plan_count >= plan_limit) {
            self.stats_.plan_declines += 1;
            return null;
        }
        self.stats_.plan_misses += 1;
        var candidate: Plan = .{ .key = key };
        errdefer candidate.deinit(&self.f);
        self.buildPlan(&candidate) catch |err| {
            // An explicitly selected physical policy may decline unsupported
            // metadata, but an SDK allocation/execution failure is not a hint
            // to replay the operation using another schedule.
            if (key.dense_seq2seq and err != error.CudnnUnsupported) return err;
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
        const dtype: c_int = if (p.key.dtype == .f16) 2 else 9;
        try self.tensor(&p.q, 1, q_layout, dtype, false);
        try self.tensor(&p.k, 2, kv_layout, dtype, false);
        try self.tensor(&p.v, 3, kv_layout, dtype, false);
        try self.tensor(&p.o, 4, q_layout, dtype, false);
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
        defer destroyDescriptor(&self.f, &heur);
        try check(self.f.create_desc(.engineheur, &heur));
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
            // Also owns a descriptor published by a failed create before
            // the loop increments created. A selected config moves to Plan.
            for (&configs) |*config| {
                if (config.* != p.engine_config) destroyDescriptor(&self.f, config);
            }
        }
        while (created < requested) : (created += 1) try check(self.f.create_desc(.enginecfg, &configs[created]));
        var count: i64 = 0;
        try check(self.f.get(heur, 202, .backend_descriptor, @intCast(requested), &count, @ptrCast(&configs)));
        if (count <= 0 or count > @as(i64, @intCast(requested))) return error.CudnnUnsupported;
        for (configs[0..@intCast(count)]) |config| {
            var execution: Desc = null;
            defer destroyDescriptor(&self.f, &execution);
            try check(self.f.create_desc(.execution_plan, &execution));
            if (try self.configureExecution(execution, config, p.key.dense_seq2seq)) {
                p.engine_config = config;
                p.execution = execution;
                execution = null;
                break;
            }
        }
        if (p.execution == null) return error.CudnnUnsupported;
        var workspace: i64 = 0;
        count = 0;
        try check(self.f.get(p.execution, 402, .int64, 1, &count, @ptrCast(&workspace)));
        if (count != 1 or workspace < 0) return error.CudnnUnsupported;
        p.workspace_bytes = @intCast(workspace);
        self.stats_.workspace_high_water = @max(self.stats_.workspace_high_water, p.workspace_bytes);
    }

    fn configureExecution(self: *Module, execution: Desc, config: Desc, strict_errors: bool) !bool {
        var ec = config;
        var handle = self.handle;
        const first = self.f.set(execution, 400, .handle, 1, @ptrCast(&handle));
        if (first != success and (first == 9 or !strict_errors)) return false;
        try check(first);
        const second = self.f.set(execution, 401, .backend_descriptor, 1, @ptrCast(&ec));
        if (second != success and (second == 9 or !strict_errors)) return false;
        try check(second);
        const final = self.f.finalize(execution);
        if (final != success and (final == 9 or !strict_errors)) return false;
        try check(final);
        return true;
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
    if (key.batch == 0 or key.sequence == 0 or !std.math.isFinite(key.scale)) return false;
    if (key.dense_seq2seq) {
        return key.dtype == .f16 and !key.florence_vision and !key.ocr_vision and !key.padding_mask and
            key.local_window_radius == 0 and key.batch <= 8 and key.sequence <= 1500 and key.dim == 64 and
            key.q_heads == key.kv_heads and (key.q_heads == 6 or key.q_heads == 8 or key.q_heads == 12 or key.q_heads == 16 or key.q_heads == 20) and key.scale == 0.125;
    }
    if (key.florence_vision) {
        if (key.dtype != .f16 or key.ocr_vision or key.padding_mask or key.local_window_radius != 0 or key.q_heads != key.kv_heads) return false;
        const encoder = key.batch <= 64 and (key.q_heads == 8 or key.q_heads == 12 or key.q_heads == 16) and key.dim == 64 and key.sequence <= 1024;
        const windows = key.batch <= 65535 and (key.q_heads == 4 or key.q_heads == 8 or key.q_heads == 16 or key.q_heads == 32) and key.dim == 32 and key.sequence == 144;
        const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(key.dim)));
        return (encoder or windows) and @as(u32, @bitCast(key.scale)) == @as(u32, @bitCast(scale));
    }
    if (key.batch > 32 or key.dtype != .bf16) return false;
    if (key.padding_mask and version < 91500) return false;
    const scale_bits: u32 = @bitCast(key.scale);
    const vision = !key.padding_mask and key.local_window_radius == 0 and key.batch <= 8 and key.q_heads == 12 and key.kv_heads == 12 and
        key.dim == 64 and key.sequence <= 3136 and scale_bits == @as(u32, @bitCast(@as(f32, 0.125)));
    // Independently qualified LightOn OCR geometry; do not broaden the
    // EmbeddingGemma defaults to unrelated model heads or longer pages.
    const ocr_vision = key.ocr_vision and !key.padding_mask and key.local_window_radius == 0 and
        key.batch <= 16 and (key.q_heads == 12 or key.q_heads == 16) and key.kv_heads == key.q_heads and
        key.dim == 64 and key.sequence <= max_ocr_vision_sequence and scale_bits == @as(u32, @bitCast(@as(f32, 0.125)));
    if (key.ocr_vision) return ocr_vision;
    const text_base = key.q_heads == 4 and key.kv_heads == 2 and key.dim == 256 and
        scale_bits == @as(u32, @bitCast(@as(f32, 1.0)));
    const text_plain = text_base and key.local_window_radius == 0 and key.sequence <= 513;
    const text_band = text_base and key.local_window_radius == 512 and key.sequence <= 8192 and version >= 92100;
    const text_global = key.q_heads == 4 and key.kv_heads == 1 and key.dim == 512 and
        key.local_window_radius == 0 and key.sequence <= 8192 and scale_bits == @as(u32, @bitCast(@as(f32, 1.0)));
    return vision or text_plain or text_band or text_global;
}

fn scoreLayout(batch: usize, heads: usize, sequence: usize) !Layout {
    const ss = try std.math.mul(usize, sequence, sequence);
    const hss = try std.math.mul(usize, heads, ss);
    inline for (.{ batch, heads, sequence, ss, hss }) |value| if (value > std.math.maxInt(i64)) return error.InvalidShape;
    return .{ .dims = .{ @intCast(batch), @intCast(heads), @intCast(sequence), @intCast(sequence) }, .strides = .{ @intCast(hss), @intCast(ss), @intCast(sequence), 1 } };
}

fn check(status: Status) !void {
    if (status == 9) return error.CudnnUnsupported;
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

test "embeddinggemma2 cuDNN batched vision and global text plans retain shape and mask identity" {
    const vision = Key{ .batch = 4, .q_heads = 12, .kv_heads = 12, .sequence = 560, .dim = 64, .scale = 0.125 };
    try std.testing.expect(eligible(vision, 91300));
    var single = vision;
    single.batch = 1;
    try std.testing.expect(!vision.eql(single));
    try std.testing.expect(keyFingerprint(vision) != keyFingerprint(single));
    var global = Key{ .batch = 2, .q_heads = 4, .kv_heads = 1, .sequence = 4546, .dim = 512 };
    try std.testing.expect(eligible(global, 91300));
    global.padding_mask = true;
    try std.testing.expect(!eligible(global, 91400));
    try std.testing.expect(eligible(global, 91500));
    global.local_window_radius = 512;
    try std.testing.expect(!eligible(global, 92400));
}

test "LightOnOCR3 CUDA cuDNN OCR vision geometry remains separate from embedding defaults" {
    var key = Key{ .batch = 4, .q_heads = 16, .kv_heads = 16, .sequence = 4096, .dim = 64, .scale = 0.125 };
    try std.testing.expect(!eligible(key, 92000));
    const original = keyFingerprint(key);
    const plain = key;
    key.ocr_vision = true;
    try std.testing.expect(eligible(key, 92000));
    try std.testing.expect(!key.eql(plain));
    try std.testing.expect(keyFingerprint(key) != original);
    key.batch = 16;
    try std.testing.expect(eligible(key, 92000));
    key.batch = 17;
    try std.testing.expect(!eligible(key, 92000));
    key.batch = 4;
    for ([_]usize{ 4097, 7656, 12996, max_ocr_vision_sequence }) |sequence| {
        key.sequence = sequence;
        try std.testing.expect(eligible(key, 92000));
    }
    key.sequence = max_ocr_vision_sequence + 1;
    try std.testing.expect(!eligible(key, 92000));
    key.sequence = 662;
    key.padding_mask = true;
    try std.testing.expect(!eligible(key, 92000));
}

test "LightOnOCR3 CUDA Florence F16 plans isolate dtype windows and encoder masks" {
    var key = Key{ .batch = 11664, .q_heads = 4, .kv_heads = 4, .sequence = 144, .dim = 32, .scale = 1.0 / @sqrt(@as(f32, 32)), .dtype = .f16, .florence_vision = true };
    try std.testing.expect(eligible(key, 92000));
    var other = key;
    other.dtype = .bf16;
    try std.testing.expect(!eligible(other, 92000));
    try std.testing.expect(!key.eql(other));
    try std.testing.expect(keyFingerprint(key) != keyFingerprint(other));
    other = key;
    other.florence_vision = false;
    try std.testing.expect(!eligible(other, 92000));
    key = .{ .batch = 64, .q_heads = 8, .kv_heads = 8, .sequence = 587, .dim = 64, .scale = 0.125, .dtype = .f16, .florence_vision = true };
    try std.testing.expect(eligible(key, 92000));
    key.padding_mask = true;
    try std.testing.expect(!eligible(key, 92000));
    key.padding_mask = false;
    key.local_window_radius = 512;
    try std.testing.expect(!eligible(key, 92000));
}

test "Whisper cuDNN encoder isolates FP16 plans from vision and rejects unsupported geometry" {
    var key = Key{ .batch = 1, .q_heads = 12, .kv_heads = 12, .sequence = 1500, .dim = 64, .scale = 0.125, .dtype = .f16, .dense_seq2seq = true };
    try std.testing.expect(eligible(key, 91500));
    var other = key;
    other.dense_seq2seq = false;
    try std.testing.expect(!Key.eql(key, other));
    try std.testing.expect(!eligible(other, 91500));
    key.sequence = 1501;
    try std.testing.expect(!eligible(key, 91500));
}

test "cuDNN cold unsupported metadata remains distinct from allocation and execution failure" {
    try std.testing.expectError(error.CudnnUnsupported, check(9));
    try std.testing.expectError(error.CudnnFailure, check(2));
    try std.testing.expectError(error.CudnnFailure, check(8));
}

test "cuDNN dense seq2seq actual cold plan failure destroys descriptors and only caches unsupported" {
    const Fake = struct {
        var failure: Status = 0;
        var live: usize = 0;
        var calls: usize = 0;
        fn create(_: DescriptorType, descriptor: *Desc) callconv(.c) Status {
            live += 1;
            calls += 1;
            descriptor.* = @ptrFromInt(live);
            return 0;
        }
        fn set(_: Desc, _: c_int, _: AttributeType, _: i64, _: ?*const anyopaque) callconv(.c) Status {
            return failure;
        }
        fn destroy(_: Desc) callconv(.c) Status {
            std.debug.assert(live > 0);
            live -= 1;
            return 0;
        }
    };
    const key = Key{ .batch = 1, .q_heads = 6, .kv_heads = 6, .sequence = 2, .dim = 64, .scale = 0.125, .dtype = .f16, .dense_seq2seq = true };
    for ([_]Status{ 9, 2, 8 }) |status| {
        Fake.failure = status;
        Fake.live = 0;
        Fake.calls = 0;
        // No real library/driver or numerical entry is initialized. Only the
        // actual cold descriptor path may be reached by this metadata query.
        var module = Module{ .lib = undefined, .f = undefined, .handle = null, .version = 91500 };
        module.f.create_desc = Fake.create;
        module.f.set = Fake.set;
        module.f.destroy_desc = Fake.destroy;
        if (status == 9) {
            try std.testing.expect((try module.workspaceBytesFor(key)) == null);
            try std.testing.expectEqual(@as(usize, 1), module.declined_count);
            try std.testing.expect((try module.workspaceBytesFor(key)) == null);
            try std.testing.expectEqual(@as(usize, 1), Fake.calls);
        } else {
            try std.testing.expectError(error.CudnnFailure, module.workspaceBytesFor(key));
            try std.testing.expectEqual(@as(usize, 0), module.declined_count);
            try std.testing.expectError(error.CudnnFailure, module.workspaceBytesFor(key));
            try std.testing.expectEqual(@as(usize, 2), Fake.calls);
        }
        try std.testing.expectEqual(@as(usize, 0), Fake.live);
        try std.testing.expectEqual(@as(usize, 0), module.plan_count);
    }
}

const CudnnRetirementFault = struct {
    var alive: [128]bool = @splat(false);
    var kinds: [128]DescriptorType = @splat(.tensor);
    var next: usize = 0;
    var fail_id: usize = 0;
    var fail_kind: ?DescriptorType = null;
    var execution_set_status: Status = 0;
    var execution_sets: usize = 0;
    var handle_failure: bool = false;
    fn reset() void {
        alive = @splat(false);
        kinds = @splat(.tensor);
        next = 0;
        fail_id = 0;
        fail_kind = null;
        execution_set_status = 0;
        execution_sets = 0;
        handle_failure = false;
    }
    fn own(kind: DescriptorType) Desc {
        next += 1;
        std.debug.assert(next < alive.len);
        alive[next] = true;
        kinds[next] = kind;
        return @ptrFromInt(next);
    }
    fn create(kind: DescriptorType, out: *Desc) callconv(.c) Status {
        out.* = own(kind);
        return if (fail_kind == kind) 2 else 0;
    }
    fn destroy(desc: Desc) callconv(.c) Status {
        const id = @intFromPtr(desc.?);
        std.debug.assert(id < alive.len and alive[id]);
        if (id == fail_id) return 9;
        alive[id] = false;
        return 0;
    }
    fn destroyHandle(handle: Handle) callconv(.c) Status {
        return if (handle_failure) 8 else destroy(handle);
    }
    fn set(desc: Desc, attribute: c_int, _: AttributeType, _: i64, _: ?*const anyopaque) callconv(.c) Status {
        if (kinds[@intFromPtr(desc.?)] == .execution_plan and attribute == 400) {
            execution_sets += 1;
            if (execution_sets == 1) return execution_set_status;
        }
        return 0;
    }
    fn finalize(_: Desc) callconv(.c) Status {
        return 0;
    }
    fn get(_: Desc, attribute: c_int, _: AttributeType, requested: i64, count: *i64, value: ?*anyopaque) callconv(.c) Status {
        if (attribute == 202) {
            count.* = 2;
            return 0;
        }
        if (attribute == 402 and requested == 1) {
            count.* = 1;
            const bytes: *i64 = @ptrCast(@alignCast(value.?));
            bytes.* = 512;
            return 0;
        }
        return 3;
    }
    fn table() Table {
        var result: Table = undefined;
        result.create_desc = create;
        result.destroy_desc = destroy;
        result.destroy = @This().destroyHandle;
        result.set = set;
        result.finalize = finalize;
        result.get = get;
        return result;
    }
    fn live() usize {
        var result: usize = 0;
        for (alive) |present| result += @intFromBool(present);
        return result;
    }
    fn plan() Plan {
        var result = Plan{ .key = .{ .batch = 7 }, .workspace_bytes = 512 };
        inline for (@typeInfo(Plan).@"struct".field_names, @typeInfo(Plan).@"struct".field_types) |name, T| {
            if (comptime T == Desc) {
                @field(result, name) = own(.tensor);
            } else if (comptime @typeInfo(T) == .array) {
                if (comptime @typeInfo(T).array.child == Desc) {
                    for (&@field(result, name)) |*item| item.* = own(.tensor);
                }
            }
        }
        return result;
    }
    fn expectOwned(value: *const Plan) !void {
        var count: usize = 0;
        inline for (@typeInfo(Plan).@"struct".field_names, @typeInfo(Plan).@"struct".field_types) |name, T| {
            if (comptime T == Desc) {
                if (@field(value, name)) |pointer| {
                    try std.testing.expect(alive[@intFromPtr(pointer)]);
                    count += 1;
                }
            } else if (comptime @typeInfo(T) == .array) {
                if (comptime @typeInfo(T).array.child == Desc) for (@field(value, name)) |item| {
                    if (item) |pointer| {
                        try std.testing.expect(alive[@intFromPtr(pointer)]);
                        count += 1;
                    }
                };
            }
        }
        try std.testing.expectEqual(live(), count);
    }
};

test "cuDNN checked plan destruction preserves every failed descriptor and supports exact retry" {
    const Fault = CudnnRetirementFault;
    Fault.reset();
    var initial = Fault.plan();
    const descriptor_count = Fault.next;
    const initial_table = Fault.table();
    try initial.deinitChecked(&initial_table);
    try std.testing.expectEqual(@as(usize, 20), descriptor_count);
    for (1..descriptor_count + 1) |failure| {
        Fault.reset();
        var plan = Fault.plan();
        const f = Fault.table();
        Fault.fail_id = failure;
        try std.testing.expectError(error.CudnnDescriptorDestroyFailed, plan.deinitChecked(&f));
        try std.testing.expect(Fault.alive[failure]);
        try std.testing.expectEqual(@as(usize, 7), plan.key.batch);
        try std.testing.expectEqual(@as(usize, 512), plan.workspace_bytes);
        try Fault.expectOwned(&plan);
        Fault.fail_id = 0;
        try plan.deinitChecked(&f);
        try std.testing.expectEqualDeep(Plan{}, plan);
        try std.testing.expectEqual(@as(usize, 0), Fault.live());
        try plan.deinitChecked(&f);
    }
}

test "cuDNN checked module retirement keeps failed handle until successful retry" {
    const Fault = CudnnRetirementFault;
    Fault.reset();
    var module = Module{ .lib = undefined, .f = Fault.table(), .handle = Fault.own(.tensor), .version = 91500 };
    module.plans[0] = .{ .q = Fault.own(.tensor), .execution = Fault.own(.execution_plan) };
    module.plan_count = 1;
    const handle = module.handle;
    Fault.handle_failure = true;
    try std.testing.expectError(error.CudnnHandleDestroyFailed, module.retireOwnedChecked());
    try std.testing.expectEqual(handle, module.handle);
    try std.testing.expectEqual(@as(usize, 1), Fault.live());
    Fault.handle_failure = false;
    try module.retireOwnedChecked();
    try std.testing.expect(module.handle == null and module.plan_count == 0);
    try std.testing.expectEqual(@as(usize, 0), Fault.live());
}

test "cuDNN actual cold builder cleans SDK-published descriptors on failed temporary create" {
    const Fault = CudnnRetirementFault;
    const key = Key{ .batch = 1, .q_heads = 6, .kv_heads = 6, .sequence = 2, .dim = 64, .scale = 0.125, .dtype = .f16, .dense_seq2seq = true };
    for ([_]DescriptorType{ .engineheur, .enginecfg, .execution_plan }) |kind| {
        Fault.reset();
        Fault.fail_kind = kind;
        var module = Module{ .lib = undefined, .f = Fault.table(), .handle = null, .version = 91500 };
        try std.testing.expectError(error.CudnnFailure, module.workspaceBytesFor(key));
        try std.testing.expectEqual(@as(usize, 0), module.plan_count);
        try std.testing.expectEqual(@as(usize, 0), Fault.live());
    }
}

test "cuDNN actual cold candidate only retries unsupported execution metadata and preserves SDK errors" {
    const Fault = CudnnRetirementFault;
    const key = Key{ .batch = 1, .q_heads = 6, .kv_heads = 6, .sequence = 2, .dim = 64, .scale = 0.125, .dtype = .f16, .dense_seq2seq = true };
    for ([_]Status{ 9, 2, 8 }) |status| {
        Fault.reset();
        Fault.execution_set_status = status;
        var module = Module{ .lib = undefined, .f = Fault.table(), .handle = null, .version = 91500 };
        if (status == 9) {
            try std.testing.expectEqual(@as(?usize, 512), try module.workspaceBytesFor(key));
            try std.testing.expectEqual(@as(usize, 2), Fault.execution_sets);
            try std.testing.expectEqual(@as(usize, 1), module.plan_count);
            try module.retireOwnedChecked();
        } else {
            try std.testing.expectError(error.CudnnFailure, module.workspaceBytesFor(key));
            try std.testing.expectEqual(@as(usize, 1), Fault.execution_sets);
            try std.testing.expectEqual(@as(usize, 0), module.plan_count);
            try std.testing.expectEqual(@as(usize, 0), module.declined_count);
        }
        try std.testing.expectEqual(@as(usize, 0), Fault.live());
    }
}

test "cuDNN actual variant create error retains plan and retires SDK-published temporary" {
    const Fault = CudnnRetirementFault;
    Fault.reset();
    const key = Key{ .batch = 1, .q_heads = 6, .kv_heads = 6, .sequence = 2, .dim = 64, .scale = 0.125, .dtype = .f16, .dense_seq2seq = true };
    var module = Module{ .lib = undefined, .f = Fault.table(), .handle = null, .version = 91500 };
    module.plans[0] = .{ .key = key, .execution = Fault.own(.execution_plan) };
    module.plan_count = 1;
    Fault.fail_kind = .variant_pack;
    // Real executeFor/getPlan entrypoints stop before numerical execute; its
    // table slot is deliberately undefined and must never be borrowed.
    try std.testing.expectError(error.CudnnFailure, module.executeVision(key, 8, 16, 24, 32, 0));
    try std.testing.expect(module.plans[0].execution != null);
    try std.testing.expectEqual(@as(usize, 1), Fault.live());
    try module.retireOwnedChecked();
    try std.testing.expectEqual(@as(usize, 0), Fault.live());
}

test "cuDNN legacy candidate retry remains unchanged while explicit dense SDK errors propagate" {
    const Fault = CudnnRetirementFault;
    for ([_]bool{ false, true }) |strict| {
        Fault.reset();
        Fault.execution_set_status = 2;
        const key = Key{ .batch = 1, .q_heads = 12, .kv_heads = 12, .sequence = 2, .dim = 64, .scale = 0.125, .dtype = if (strict) .f16 else .bf16, .dense_seq2seq = strict };
        var module = Module{ .lib = undefined, .f = Fault.table(), .handle = null, .version = 91500 };
        if (strict) {
            try std.testing.expectError(error.CudnnFailure, module.workspaceBytesFor(key));
            try std.testing.expectEqual(@as(usize, 1), Fault.execution_sets);
        } else {
            try std.testing.expectEqual(@as(?usize, 512), try module.workspaceBytesFor(key));
            try std.testing.expectEqual(@as(usize, 2), Fault.execution_sets);
            try module.retireOwnedChecked();
        }
        try std.testing.expectEqual(@as(usize, 0), Fault.live());
    }
}

test "cuDNN SDPA fixed numerical contracts decline before cache or SDK access" {
    try std.testing.expect(supportsNumericalContract(.vendor_selected));
    var module: Module = undefined;
    module.stats_ = .{};
    for ([_]NumericalContract{ .flash_bf16, .efficient_bf16 }) |contract| {
        const key = Key{ .batch = 1, .q_heads = 4, .kv_heads = 2, .sequence = 15, .dim = 256, .numerical_contract = contract };
        try std.testing.expect(!supportsNumericalContract(contract));
        try std.testing.expect((try module.workspaceBytesFor(key)) == null);
        var vendor = key;
        vendor.numerical_contract = .vendor_selected;
        try std.testing.expect(!key.eql(vendor));
        try std.testing.expect(keyFingerprint(key) != keyFingerprint(vendor));
    }
    try std.testing.expectEqual(@as(u64, 2), module.stats_.plan_declines);
}
