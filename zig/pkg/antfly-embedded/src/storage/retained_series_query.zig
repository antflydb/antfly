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

//! Private native status/replay reads. Snapshot pages use the separate retained
//! reader, so a poll never pretends to reconstruct an old visibility cut.
const std = @import("std");
const series = @import("retained_series.zig");
const capture = @import("retained_series_capture.zig");
const control = @import("retained_series_control.zig");
const contract = @import("retained_series_read_contract.zig");
const A = std.mem.Allocator;
pub const Entry = struct { binding: capture.Binding, progress: series.Progress };
pub const Status = struct { root_incarnation: []const u8, entries: []const Entry };
pub const Revocation = struct { root_incarnation: []const u8, fence: control.Fence, revoked: bool, reclaimed: bool = false };
pub const Replay = struct {
    root_incarnation: []const u8,
    cursor: series.Cursor,
    frames: []const series.Frame,
    admitted_ns: i128 = 0,
    latest_sequence: u64 = 0,
    pub fn jsonStringify(self: Replay, writer: anytype) !void {
        if (self.latest_sequence == 0) return @import("../retained_dataset_types.zig").writeFieldsOmitting(self, .{}, .{"latest_sequence"}, writer);
        try @import("../retained_dataset_types.zig").writeFields(self, .{}, writer);
    }
};
pub const max_response_bytes = 6 * 1024 * 1024;

pub fn executeJson(a: A, txn: anytype, root_incarnation: u128, request: contract.Request) ![]u8 {
    return executeJsonControlled(a, txn, root_incarnation, request, .{});
}
pub fn executeJsonControlled(output: A, txn: anytype, root_incarnation: u128, request: contract.Request, operation: contract.Control) ![]u8 {
    var memory: @import("antfly_platform").allocator.BoundedAllocator = .{ .backing = output, .limit = 32 * 1024 * 1024 };
    defer std.debug.assert(memory.live == 0);
    return executeAdmitted(output, &memory, txn, root_incarnation, request, operation) catch |err| {
        return if (err == error.OutOfMemory and memory.denied) error.DatasetPageBudgetExceeded else err;
    };
}
fn executeAdmitted(output: A, memory: *@import("antfly_platform").allocator.BoundedAllocator, txn: anytype, root_incarnation: u128, request: contract.Request, operation: contract.Control) ![]u8 {
    try operation.check();
    const a = memory.allocator();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const root = try std.fmt.allocPrint(temp, "{d}", .{root_incarnation});
    const bindings: []const capture.Binding = if (request == .revocation) &.{} else try capture.load(temp, txn);
    const response = switch (request) {
        .revocation => |fence| result: {
            const revoked = try @import("retained_series_revocation.zig").isRevoked(txn, fence);
            const reclaimed = if (revoked) for (try capture.load(temp, txn)) |binding| {
                if (std.mem.eql(u8, &binding.spec.id, &fence.id) and binding.spec.generation == fence.generation) {
                    try control.requireFence(fence, binding.spec);
                    break false;
                }
            } else true else false;
            break :result try contract.stringifyAlloc(a, Revocation{ .root_incarnation = root, .fence = fence, .revoked = revoked, .reclaimed = reclaimed }, .{}, operation);
        },
        .status => result: {
            const entries = try temp.alloc(Entry, bindings.len);
            for (bindings, entries) |binding, *entry| {
                try operation.check();
                entry.* = .{ .binding = binding, .progress = try (series.Engine{ .alloc = a, .spec = binding.spec, .read_control = operation }).status(temp, txn) };
            }
            break :result try contract.stringifyAlloc(a, Status{ .root_incarnation = root, .entries = entries }, .{}, operation);
        },
        .poll => |poll| result: {
            if (poll.root_incarnation != root_incarnation) return error.RetainedSeriesResetRequired;
            if (poll.limit == 0 or poll.limit > 32) return error.RetainedSeriesBatchTooLarge;
            const spec = for (bindings) |binding| {
                if (std.mem.eql(u8, &binding.spec.id, &poll.fence.id) and binding.spec.generation == poll.fence.generation) break binding.spec;
            } else return error.RetainedSeriesDefinitionMismatch;
            try control.requireFence(poll.fence, spec);
            const engine = series.Engine{ .alloc = a, .spec = spec, .read_control = operation };
            const frames = try engine.poll(temp, txn, poll.cursor, poll.limit);
            const progress = try engine.status(temp, txn);
            var cursor = poll.cursor;
            if (frames.len != 0) cursor.sequence = frames[frames.len - 1].sequence;
            break :result try contract.stringifyAlloc(a, Replay{ .root_incarnation = root, .cursor = cursor, .frames = frames, .admitted_ns = progress.admitted_ns, .latest_sequence = if (poll.include_latest) progress.feed_latest else 0 }, .{}, operation);
        },
    };
    defer a.free(response);
    if (response.len > max_response_bytes) return error.RetainedSeriesBatchTooLarge;
    try operation.check();
    if (response.len > memory.limit - memory.live) return error.DatasetPageBudgetExceeded;
    return output.dupe(u8, response);
}
