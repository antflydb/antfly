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

//! Bounded immutable input window for amortized native build sessions.
const std = @import("std");
const local = @import("antfly_local_sources");
const Replay = @import("lake_index_build_replay.zig").Replay;
const Provider = @import("lake_index_row_source.zig").Provider;
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Self = @This();
pub const width = 64;
pub const initial_width = 4;
a: A,
parent: *local.serverless_query_lake_serving.ServingSource,
source: local.serverless_query_lake_serving.ServingSource,
provider: Provider,
buffers: [width]Replay = undefined,
initialized: usize = 0,
binding: local.serverless_segment_source_binding.Binding,
budget: local.sql_memory_budget,
disk_limit: u64,
first: usize = 0,
end: usize = 0,
prepared: bool = false,
reported_captures: usize = 0,
pub fn init(self: *Self, a: A, provider: *Provider, binding: local.serverless_segment_source_binding.Binding, memory: usize, disk: u64) !void {
    self.* = .{ .a = a, .parent = provider.source, .source = try provider.source.parallelView(a), .provider = provider.*, .binding = binding, .budget = .{ .backing = a, .limit = memory }, .disk_limit = disk };
    self.provider.source = &self.source;
    self.provider.replay = null;
    self.provider.replay_consumer = null;
    for (&self.buffers, 0..) |*buffer, index| {
        buffer.* = Replay.init(a, &self.provider, binding.column_bindings);
        buffer.budget.limit = memory;
        buffer.budget.backing = self.budget.allocator();
        buffer.max_spill_bytes = disk;
        if (index != 0) self.buffers[index - 1].sibling = buffer;
        self.initialized += 1;
    }
}
pub fn report(self: *Self, counters: *Replay) void {
    var total: usize = 0;
    for (self.buffers[0..self.initialized]) |buffer| total += buffer.captures;
    counters.captures += total - self.reported_captures;
    self.reported_captures = total;
}
pub fn deinit(self: *Self, counters: *Replay) void {
    self.report(counters);
    for (self.buffers[0..self.initialized]) |*buffer| {
        buffer.deinit();
    }
    self.source.closeParallelView(self.parent);
}
pub fn capture(self: *Self, first: usize, end: usize, token: Token) anyerror!bool {
    if (end <= first or end - first > width or end > self.source.inventory.files.len) return error.InvalidNativeLakeFileState;
    self.first = first;
    self.end = end;
    self.prepared = self.binding.column_bindings.len != 0;
    for (&self.buffers) |*buffer| try buffer.reset();
    var disk = self.disk_limit;
    for (self.buffers[0 .. end - first], first..) |*buffer, ordinal| {
        try token.check();
        buffer.max_spill_bytes = disk;
        self.budget.exhausted = false;
        const retained = if (self.prepared) buffer.prepare(self.a, self.binding, ordinal, token) catch |err| declined: {
            if (err != error.OutOfMemory or !self.budget.isLocallyExhausted()) return err;
            try buffer.reset();
            break :declined false;
        } else false;
        if (!retained) {
            // Retain an admitted prefix; an oversized first file keeps the
            // ordinary single-file fallback instead of rescanning good files.
            self.end = if (ordinal == first) first + 1 else ordinal;
            self.prepared = ordinal != first;
            return self.prepared;
        }
        disk -= buffer.manager.live_bytes;
    }
    return self.prepared;
}
/// Session length follows actual retained bytes, independently of file count.
pub fn nextWidth(self: *const Self, requested: usize) usize {
    const captured = self.end - self.first;
    if (!self.prepared or captured < requested) return @max(1, captured);
    var disk: u64 = 0;
    for (self.buffers[0..captured]) |buffer| disk +|= buffer.manager.live_bytes;
    if (self.budget.live <= self.budget.limit / 2 and disk <= self.disk_limit / 2) return @min(width, requested * 2);
    return requested;
}
pub fn replay(self: *Self) *Replay {
    return &self.buffers[0];
}
