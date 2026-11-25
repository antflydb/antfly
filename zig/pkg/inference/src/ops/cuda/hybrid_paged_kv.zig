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

//! Exact BF16 KV for a request with independent full and sliding page tables.
//! Each group has one contiguous plane per visit and K/V operand, indexed by
//! physical page then token. Only the staged suffix may be written.
const std = @import("std");
const buffer = @import("buffer.zig");
const CudaContext = @import("context.zig").CudaContext;
const metadata = @import("../../runtime/kv/hybrid_paged.zig");
const KvStorage = @import("../../runtime/kv/storage.zig");

const DeviceBuffer = buffer.DeviceBuffer;
pub const Group = metadata.Group;
pub const Layout = metadata.Layout;

pub const Config = metadata.BankConfig;
pub const Visit = metadata.Visit;

pub const Planes = struct {
    key: DeviceBuffer,
    value: DeviceBuffer,
};

pub const StagedView = struct {
    planes: Planes,
    first_page: usize,
    page_count: usize,
    physical_pages: usize,
    page_tokens: u16,
    visible_start: usize,
    visible_end: usize,

    pub fn range(self: StagedView) metadata.StagedRange {
        return .{
            .first_page = self.first_page,
            .page_count = self.page_count,
            .physical_pages = self.physical_pages,
            .page_tokens = self.page_tokens,
            .visible_start = self.visible_start,
            .visible_end = self.visible_end,
        };
    }
};

fn view(parent: DeviceBuffer, offset: usize, len: usize) DeviceBuffer {
    std.debug.assert(offset <= parent.len and len <= parent.len - offset);
    return .{ .ptr = parent.ptr + offset, .len = len, .borrowed = true };
}

const Bank = struct {
    storage: KvStorage.KvStorage,
    device: DeviceBuffer,
    written: []bool,
    plane_bytes: usize,

    fn init(allocator: std.mem.Allocator, ctx: *CudaContext, config: Config, group: Group) !Bank {
        const bytes = try config.deviceBytes(group);
        const slot_count = config.slots(group);
        var storage = try KvStorage.initHost(allocator, .{
            .backend = .cuda,
            .dtype = .bf16,
            .page_size_tokens = config.layout.page_tokens,
            .num_layers_packed = slot_count,
            .num_kv_heads = config.kv_heads,
            .head_dim = config.head_dim,
            .store_cpu_bytes = false,
        });
        errdefer storage.deinit(allocator);
        storage.hostPool().?.setTargetMaxBlocks(try config.layout.pageCapacity(group));
        const written = try allocator.alloc(bool, slot_count);
        errdefer allocator.free(written);
        return .{
            .storage = storage,
            .device = try DeviceBuffer.allocWithPurpose(ctx, bytes, .kv_state),
            .written = written,
            .plane_bytes = bytes / (@as(usize, slot_count) * 2),
        };
    }

    fn deinit(self: *Bank, allocator: std.mem.Allocator, ctx: *CudaContext) void {
        self.device.free(ctx);
        self.storage.deinit(allocator);
        allocator.free(self.written);
    }

    fn planes(self: *const Bank, slot: usize) Planes {
        const offset = slot * 2 * self.plane_bytes;
        return .{
            .key = view(self.device, offset, self.plane_bytes),
            .value = view(self.device, offset + self.plane_bytes, self.plane_bytes),
        };
    }
};

const Pending = struct {
    append: metadata.Append,
    remaining_slots: usize,
    submitted: bool = false,
    failed: bool = false,
};

const CopyDirection = enum { to_cache, from_cache };

pub const Usage = struct {
    full_pages: usize,
    sliding_pages: usize,
    device_bytes: usize,
};

/// Initialize in the caller's final storage. All payload work uses ctx's
/// ordered stream. Access, including use of borrowed device views, is serialized
/// by the request owner.
pub const Cache = struct {
    ctx: *CudaContext,
    sequence: metadata.Sequence,
    full: Bank,
    sliding: Bank,
    row_bytes: usize,
    pending: ?Pending = null,

    pub fn init(self: *Cache, allocator: std.mem.Allocator, ctx: *CudaContext, config: Config) !void {
        const row_bytes = try config.rowBytes();
        var full = try Bank.init(allocator, ctx, config, .full);
        errdefer full.deinit(allocator, ctx);
        var sliding = try Bank.init(allocator, ctx, config, .sliding);
        errdefer sliding.deinit(allocator, ctx);
        self.* = .{ .ctx = ctx, .sequence = undefined, .full = full, .sliding = sliding, .row_bytes = row_bytes };
        self.sequence = try metadata.Sequence.init(allocator, config.layout, &self.full.storage, &self.sliding.storage);
    }

    /// A failed drain retains ownership. The caller can retry cleanup without
    /// releasing either bank or making pending destination pages reusable.
    pub fn deinit(self: *Cache) !void {
        try self.ctx.synchronize();
        self.discardPending();
        const allocator = self.sequence.allocator;
        self.sequence.deinit();
        self.sliding.deinit(allocator, self.ctx);
        self.full.deinit(allocator, self.ctx);
    }

    pub fn committedTokens(self: *const Cache) usize {
        return self.sequence.committed_tokens;
    }

    pub fn usage(self: *Cache) Usage {
        return .{
            .full_pages = self.full.storage.hostPool().?.liveBlocks(),
            .sliding_pages = self.sliding.storage.hostPool().?.liveBlocks(),
            .device_bytes = self.full.device.allocation_bytes + self.sliding.device.allocation_bytes,
        };
    }

    fn bank(self: *Cache, group: Group) *Bank {
        return switch (group) {
            .full => &self.full,
            .sliding => &self.sliding,
        };
    }

    pub fn prepareAppend(self: *Cache, rows: usize) !void {
        const append = try self.sequence.prepareAppend(rows);
        @memset(self.full.written, false);
        @memset(self.sliding.written, false);
        self.pending = .{ .append = append, .remaining_slots = self.full.written.len + self.sliding.written.len };
    }

    /// Sources must stay live until the ordered batch is committed or aborted.
    /// Each visit supplies the entire staged suffix once.
    pub fn queueWrite(self: *Cache, visit: Visit, source: Planes) !void {
        const pending = if (self.pending) |*value| value else return error.KvAppendMissing;
        if (pending.failed) return error.KvAppendFailed;
        const selected = self.bank(visit.group);
        if (visit.slot >= selected.written.len) return error.InvalidKvShape;
        if (selected.written[visit.slot]) return error.KvVisitAlreadyWritten;
        const start = self.committedTokens();
        const end = pending.append.next_tokens;
        const bytes = (end - start) * self.row_bytes;
        try buffer.validateRegions(3, .{ source.key, source.value, selected.device }, .{ bytes, bytes, selected.device.len }, .{ 2, 2, 2 }, 2);

        pending.submitted = true;
        errdefer pending.failed = true;
        try self.copyRows(selected.planes(visit.slot), source, visit.group, start, end, &pending.append, .to_cache);
        selected.written[visit.slot] = true;
        pending.remaining_slots -= 1;
    }

    /// Queue a read of committed rows on the same stream. Callers own the
    /// destination until the stream completes.
    pub fn queueRead(self: *Cache, visit: Visit, start: usize, rows: usize, destination: Planes) !void {
        try self.queueReadRange(visit, start, rows, destination, null);
    }

    /// A visit can read its own K/V as soon as its writes are on the stream.
    /// The previous committed window is available throughout the append.
    pub fn queueStagedRead(self: *Cache, visit: Visit, start: usize, rows: usize, destination: Planes) !void {
        const pending = if (self.pending) |*value| value else return error.KvAppendMissing;
        if (pending.failed) return error.KvAppendFailed;
        const written = self.bank(visit.group).written;
        if (visit.slot >= written.len) return error.InvalidKvRange;
        if (!written[visit.slot]) return error.KvVisitNotWritten;
        try self.queueReadRange(visit, start, rows, destination, &pending.append);
    }

    /// Publish the mapping shared by all visits in a group. The pending
    /// append owns these page IDs through the commit or abort fence.
    pub fn stagedPageIds(self: *Cache, group: Group, page_ids: []u32) !usize {
        const pending = if (self.pending) |*value| value else return error.KvAppendMissing;
        if (pending.failed) return error.KvAppendFailed;
        return pending.append.stagedPageIds(group, page_ids);
    }

    /// Borrow a visit's staged planes. The caller uses the page table for the
    /// same group and pending append on this CUDA stream.
    pub fn stagedBinding(self: *Cache, visit: Visit) !StagedView {
        const pending = if (self.pending) |*value| value else return error.KvAppendMissing;
        const selected = self.bank(visit.group);
        if (visit.slot >= selected.written.len) return error.InvalidKvRange;
        if (pending.failed) return error.KvAppendFailed;
        if (!selected.written[visit.slot]) return error.KvVisitNotWritten;
        const range = try pending.append.stagedRange(visit.group);
        return .{
            .planes = selected.planes(visit.slot),
            .first_page = range.first_page,
            .page_count = range.page_count,
            .physical_pages = range.physical_pages,
            .page_tokens = range.page_tokens,
            .visible_start = range.visible_start,
            .visible_end = range.visible_end,
        };
    }

    /// A stable binding for GPU replay while the configured context fits
    /// inside the sliding window. Replay consumes the live page IDs and token
    /// position supplied for each append rather than the maximum visible end.
    pub fn replayBinding(self: *Cache, visit: Visit) !StagedView {
        const layout = self.sequence.layout;
        const pending = self.pending orelse return error.KvAppendMissing;
        if (layout.max_tokens > layout.window_tokens or pending.append.next_tokens != self.committedTokens() + 1)
            return error.InvalidKvShape;
        const selected = self.bank(visit.group);
        if (visit.slot >= selected.written.len) return error.InvalidKvRange;
        return .{
            .planes = selected.planes(visit.slot),
            .first_page = 0,
            .page_count = (layout.max_tokens - 1) / layout.page_tokens + 1,
            .physical_pages = try layout.pageCapacity(visit.group),
            .page_tokens = layout.page_tokens,
            .visible_start = 0,
            .visible_end = layout.max_tokens,
        };
    }

    /// Publish a submitted replay which writes every visit once. The same
    /// stream fence used by eager appends protects the staged pages.
    pub fn markReplaySubmitted(self: *Cache) !void {
        const pending = if (self.pending) |*value| value else return error.KvAppendMissing;
        if (pending.failed or pending.remaining_slots != self.full.written.len + self.sliding.written.len or
            pending.append.next_tokens != self.committedTokens() + 1) return error.InvalidKvShape;
        @memset(self.full.written, true);
        @memset(self.sliding.written, true);
        pending.remaining_slots = 0;
        pending.submitted = true;
    }

    /// Borrow a staged visit and populate its logical-to-physical mapping.
    pub fn stagedView(self: *Cache, visit: Visit, page_ids: []u32) !StagedView {
        const binding = try self.stagedBinding(visit);
        _ = try self.stagedPageIds(visit.group, page_ids);
        return binding;
    }

    fn queueReadRange(self: *Cache, visit: Visit, start: usize, rows: usize, destination: Planes, append: ?*metadata.Append) !void {
        const end = try std.math.add(usize, start, rows);
        const oldest = if (visit.group == .sliding) self.committedTokens() -| self.sequence.layout.window_tokens else 0;
        const available_end = if (append) |staged| staged.next_tokens else self.committedTokens();
        const selected = self.bank(visit.group);
        if (visit.slot >= selected.written.len or start < oldest or end > available_end) return error.InvalidKvRange;
        const bytes = rows * self.row_bytes;
        try buffer.validateRegions(4, .{ self.full.device, self.sliding.device, destination.key, destination.value }, .{ self.full.device.len, self.sliding.device.len, bytes, bytes }, .{ 2, 2, 2, 2 }, 2);
        try self.copyRows(selected.planes(visit.slot), destination, visit.group, start, end, append, .from_cache);
    }

    fn copyRows(self: *Cache, bank_planes: Planes, linear: Planes, group: Group, start: usize, end: usize, append: ?*metadata.Append, direction: CopyDirection) !void {
        const page_tokens = self.sequence.layout.page_tokens;
        var position = start;
        while (position < end) {
            const logical_page = position / page_tokens;
            const within_page = position % page_tokens;
            const physical_page = (if (append) |staged| staged.pageId(group, logical_page) else self.sequence.pageId(group, logical_page)) orelse
                return error.InvalidKvRange;
            const rows = @min(end - position, page_tokens - within_page);
            const bytes = rows * self.row_bytes;
            const offset = (@as(usize, physical_page) * page_tokens + within_page) * self.row_bytes;
            const linear_offset = (position - start) * self.row_bytes;
            const keys = view(bank_planes.key, offset, bytes);
            const values = view(bank_planes.value, offset, bytes);
            const linear_keys = view(linear.key, linear_offset, bytes);
            const linear_values = view(linear.value, linear_offset, bytes);
            if (direction == .to_cache) {
                try keys.copyFromDevice(self.ctx, linear_keys, bytes);
                try values.copyFromDevice(self.ctx, linear_values, bytes);
            } else {
                try linear_keys.copyFromDevice(self.ctx, keys, bytes);
                try linear_values.copyFromDevice(self.ctx, values, bytes);
            }
            position += rows;
        }
    }

    pub fn commit(self: *Cache) !void {
        const pending = self.pending orelse return error.KvAppendMissing;
        try self.commitPrefix(pending.append.next_tokens - self.committedTokens());
    }

    pub fn commitPrefix(self: *Cache, rows: usize) !void {
        const pending = if (self.pending) |*value| value else return error.KvAppendMissing;
        if (pending.failed) return error.KvAppendFailed;
        if (pending.remaining_slots != 0) return error.IncompleteKvWriteSet;
        errdefer pending.failed = true;
        try self.ctx.synchronize();
        try pending.append.commitPrefix(rows);
        self.discardPending();
    }

    pub fn abort(self: *Cache) !void {
        if (self.pending) |*pending| {
            if (pending.submitted) {
                pending.failed = true;
                try self.ctx.synchronize();
            }
            self.discardPending();
        }
    }

    fn discardPending(self: *Cache) void {
        if (self.pending) |*pending| pending.append.abort();
        self.pending = null;
    }
};
