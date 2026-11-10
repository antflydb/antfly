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
pub const Replay = struct { root_incarnation: []const u8, cursor: series.Cursor, frames: []const series.Frame };
pub const max_response_bytes = 6 * 1024 * 1024;

pub fn executeJson(a: A, txn: anytype, root_incarnation: u128, request: contract.Request) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const root = try std.fmt.allocPrint(temp, "{d}", .{root_incarnation});
    const bindings = try capture.load(temp, txn);
    const response = switch (request) {
        .status => result: {
            const entries = try temp.alloc(Entry, bindings.len);
            for (bindings, entries) |binding, *entry| entry.* = .{ .binding = binding, .progress = try (series.Engine{ .alloc = a, .spec = binding.spec }).status(temp, txn) };
            break :result try std.json.Stringify.valueAlloc(a, Status{ .root_incarnation = root, .entries = entries }, .{});
        },
        .poll => |poll| result: {
            if (poll.root_incarnation != root_incarnation) return error.RetainedSeriesResetRequired;
            if (poll.limit == 0 or poll.limit > 32) return error.RetainedSeriesBatchTooLarge;
            const spec = for (bindings) |binding| {
                if (std.mem.eql(u8, &binding.spec.id, &poll.fence.id)) break binding.spec;
            } else return error.RetainedSeriesDefinitionMismatch;
            try control.requireFence(poll.fence, spec);
            const frames = try (series.Engine{ .alloc = a, .spec = spec }).poll(temp, txn, poll.cursor, poll.limit);
            var cursor = poll.cursor;
            if (frames.len != 0) cursor.sequence = frames[frames.len - 1].sequence;
            break :result try std.json.Stringify.valueAlloc(a, Replay{ .root_incarnation = root, .cursor = cursor, .frames = frames }, .{});
        },
    };
    errdefer a.free(response);
    if (response.len > max_response_bytes) return error.RetainedSeriesBatchTooLarge;
    return response;
}
