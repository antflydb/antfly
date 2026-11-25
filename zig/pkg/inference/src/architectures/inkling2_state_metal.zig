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

//! Request-owned Metal buffers for the hybrid paged decoder.
const std = @import("std");
const kv = @import("../runtime/kv/hybrid_paged.zig");
const storage = @import("../runtime/kv/storage.zig");
const metal = @import("../backends/metal_runtime.zig");
const tensors = @import("../backends/metal_tensor.zig");
const attention = @import("inkling2_attention_metal.zig");
const Tensor = tensors.MetalTensor;

const Slot = struct {
    buffers: [10]Tensor,

    fn init(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, plane: i32, history: i32, residual: i32) !Slot {
        var result: Slot = undefined;
        var initialized: usize = 0;
        errdefer for (result.buffers[0..initialized]) |*buffer| buffer.deinit();
        for (&result.buffers, [_]i32{ plane, plane, history, history, history, history, residual, residual, residual, residual }) |*buffer, count| {
            buffer.* = try Tensor.deviceAllocateTyped(a, runtime, .i16, .private, &.{count});
            initialized += 1;
        }
        return result;
    }

    fn deinit(self: *Slot) void {
        for (&self.buffers) |*buffer| buffer.deinit();
    }
};

const Bank = struct {
    pool: storage.KvStorage,
    slots: []Slot,
    page_ids: Tensor,
    host_ids: []u32,
    staged_first_page: ?usize = null,

    fn init(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, config: kv.BankConfig, group: kv.Group, hidden_width: u32) !Bank {
        const pages = try config.layout.pageCapacity(group);
        const count = config.slots(group);
        if (count == 0) return error.InvalidKvShape;
        const plane = std.math.cast(i32, (try config.planeBytes(group)) / 2) orelse return error.InvalidKvShape;
        const history = std.math.cast(i32, try std.math.mul(usize, (try config.rowBytes()) / 2, 3)) orelse return error.InvalidKvShape;
        const residual = std.math.cast(i32, @as(usize, hidden_width) * 3) orelse return error.InvalidKvShape;
        if (residual == 0) return error.InvalidKvShape;
        var pool = try storage.initHost(a, .{
            .backend = .metal,
            .dtype = .bf16,
            .page_size_tokens = config.layout.page_tokens,
            .num_layers_packed = count,
            .num_kv_heads = config.kv_heads,
            .head_dim = config.head_dim,
            .store_cpu_bytes = false,
        });
        errdefer pool.deinit(a);
        pool.hostPool().?.setTargetMaxBlocks(pages);
        const slots = try a.alloc(Slot, count);
        errdefer a.free(slots);
        var initialized: usize = 0;
        errdefer for (slots[0..initialized]) |*slot| slot.deinit();
        for (slots) |*slot| {
            slot.* = try Slot.init(a, runtime, plane, history, residual);
            initialized += 1;
        }
        const host_ids = try a.alloc(u32, pages);
        errdefer a.free(host_ids);
        const page_ids = try Tensor.deviceAllocateTyped(a, runtime, .i32, .shared, &.{std.math.cast(i32, pages) orelse return error.InvalidKvShape});
        return .{ .pool = pool, .slots = slots, .page_ids = page_ids, .host_ids = host_ids };
    }

    fn deinit(self: *Bank, a: std.mem.Allocator) void {
        self.page_ids.deinit();
        a.free(self.host_ids);
        for (self.slots) |*slot| slot.deinit();
        a.free(self.slots);
        self.pool.deinit(a);
    }

    fn stagePages(self: *Bank, append: *kv.Append, group: kv.Group) !void {
        const range = try append.stagedRange(group);
        const committed = append.sequence.committed_tokens;
        const page_tokens: usize = range.page_tokens;
        const committed_end_page = committed / page_tokens + @intFromBool(committed % page_tokens != 0);
        const prefix = if (self.staged_first_page == range.first_page)
            @min(range.page_count, committed_end_page -| range.first_page)
        else
            0;
        if (prefix == range.page_count) return;

        self.staged_first_page = null;
        const changed = self.host_ids[prefix..range.page_count];
        for (changed, range.first_page + prefix..) |*id, logical|
            id.* = append.pageId(group, logical) orelse return error.InvalidKvRange;

        const bytes = std.mem.sliceAsBytes(changed);
        var patch = try self.page_ids.retainedView(prefix * @sizeOf(u32), bytes.len, &.{@intCast(changed.len)});
        defer patch.deinit();
        try patch.uploadBytes(bytes);
        self.staged_first_page = range.first_page;
    }
};

pub const State = struct {
    runtime: *metal.RawMetalDecodeRuntime,
    sequence: kv.Sequence,
    full: Bank,
    sliding: Bank,
    history_index: usize = 0,

    pub const ResidualHistory = struct {
        committed: ?*const Tensor,
        next: *Tensor,
    };

    /// Stencil used by both hidden residual streams within a decoder visit.
    pub fn convolveResidual(self: *const State, append: *const kv.Append, width: u32, input: *const Tensor, weight: *const Tensor, history: ResidualHistory, output: *Tensor) !void {
        if (!append.active or append.sequence != &self.sequence) return error.KvAppendMissing;
        const rows = std.math.cast(u32, append.next_tokens - self.sequence.committed_tokens) orelse return error.InvalidShape;
        const convolution: metal.CausalDepthwise4Bf16 = .{
            .rows = rows,
            .channels = width,
            .valid_history = @intCast(@min(3, self.sequence.committed_tokens)),
            .output_scale = 1.0 / @sqrt(@as(f32, 5.0)),
            .clip = 0,
            .epsilon = 1,
            .normalize = false,
        };
        try convolution.encode(self.runtime, .{
            .values = input,
            .weight = weight,
            .history = history.committed,
            .dynamic_taps = null,
        }, output, history.next);
    }

    pub const Binding = struct {
        planes: metal.PagedRelativeBf16Attention.Planes,
        history: attention.Layer.History,
        attention_residual: ResidualHistory,
        expert_residual: ResidualHistory,
        page_ids: *const Tensor,
    };

    pub fn deviceBytes(config: kv.BankConfig, width: u32) !usize {
        const full = try config.deviceBytes(.full);
        const sliding = try config.deviceBytes(.sliding);
        const visit_count = @as(usize, config.full_slots) + config.sliding_slots;
        const history = try std.math.mul(usize, 12 * @sizeOf(u16), try std.math.mul(usize, visit_count, @as(usize, width) + (try config.rowBytes()) / @sizeOf(u16)));
        const pages = try std.math.add(usize, try config.layout.pageCapacity(.full), try config.layout.pageCapacity(.sliding));
        const tables = try std.math.mul(usize, pages, @sizeOf(u32));
        return std.math.add(usize, try std.math.add(usize, full, sliding), try std.math.add(usize, history, tables));
    }

    /// Initialize at a stable address. Only the request's frame may use the
    /// returned views, and the commit/abort fence must precede reuse.
    pub fn init(self: *State, a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, config: kv.BankConfig, hidden_width: u32) !void {
        var full = try Bank.init(a, runtime, config, .full, hidden_width);
        errdefer full.deinit(a);
        var sliding = try Bank.init(a, runtime, config, .sliding, hidden_width);
        errdefer sliding.deinit(a);
        self.* = .{ .runtime = runtime, .sequence = undefined, .full = full, .sliding = sliding };
        self.sequence = try kv.Sequence.init(a, config.layout, &self.full.pool, &self.sliding.pool);
    }

    pub fn deinit(self: *State) void {
        const a = self.sequence.allocator;
        self.sequence.deinit();
        self.sliding.deinit(a);
        self.full.deinit(a);
    }

    pub fn prepare(self: *State, rows: usize) !kv.Append {
        if (metal.hasActiveFrame(self.runtime)) return error.MetalFrameAlreadyActive;
        var append = try self.sequence.prepareAppend(rows);
        errdefer append.abort();
        try self.full.stagePages(&append, .full);
        try self.sliding.stagePages(&append, .sliding);
        return append;
    }

    pub fn binding(self: *State, append: *const kv.Append, visit: kv.Visit) !Binding {
        if (!append.active or append.sequence != &self.sequence) return error.KvAppendMissing;
        const bank = if (visit.group == .full) &self.full else &self.sliding;
        if (visit.slot >= bank.slots.len) return error.InvalidKvRange;
        const buffers = &bank.slots[visit.slot].buffers;
        const old = 2 + self.history_index * 2;
        const next = 2 + (1 - self.history_index) * 2;
        return .{
            .planes = .{ .key = &buffers[0], .value = &buffers[1] },
            .history = .{
                .key = if (self.sequence.committed_tokens == 0) null else &buffers[old],
                .value = if (self.sequence.committed_tokens == 0) null else &buffers[old + 1],
                .next_key = &buffers[next],
                .next_value = &buffers[next + 1],
            },
            .attention_residual = .{
                .committed = if (self.sequence.committed_tokens == 0) null else &buffers[old + 4],
                .next = &buffers[next + 4],
            },
            .expert_residual = .{
                .committed = if (self.sequence.committed_tokens == 0) null else &buffers[old + 5],
                .next = &buffers[next + 5],
            },
            .page_ids = &bank.page_ids,
        };
    }

    /// Waits for the submitted visit frame before publishing KV and history.
    pub fn publish(self: *State, append: *kv.Append) !void {
        if (!append.active or append.sequence != &self.sequence) return error.KvAppendMissing;
        try metal.waitFrame(self.runtime);
        append.commit();
        self.history_index = 1 - self.history_index;
    }
};
