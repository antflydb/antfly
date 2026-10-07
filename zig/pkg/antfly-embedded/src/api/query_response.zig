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

//! Owned JSON response passed across table runtime callback boundaries.

const std = @import("std");
const graph_wire_envelope = @import("graph_wire_envelope.zig");

pub const QueryResponse = struct {
    /// Internal group-query response header used to carry the storage snapshot
    /// selected by the shard. This stays outside the public query JSON contract.
    pub const identity_read_generation_header = "X-Antfly-Identity-Read-Generation";

    json: []u8,
    /// Present only after synchronous delivery to a caller-owned sink. JSON is
    /// empty in that case; internal/group callers continue receiving owned JSON.
    delivered_bytes: ?usize = null,
    identity_read_generation: ?u64 = null,
    /// Dialect admitted at the public graph boundary. This is transport
    /// metadata only: graph execution and storage always use the canonical IR.
    graph_dialect: ?graph_wire_envelope.Dialect = null,

    pub fn deinit(self: *QueryResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

/// Synchronous delivery: the producer retains query state until the final write
/// returns. start runs only after validation and exact byte counting, allowing
/// transports to reject a response before committing headers. Backpressure and
/// disconnects propagate through write; no background producer outlives a query.
pub const Delivery = struct {
    ptr: *anyopaque,
    start_fn: *const fn (*anyopaque, usize) anyerror!void,
    write_fn: *const fn (*anyopaque, []const u8) anyerror!void,
    max_bytes: usize = std.math.maxInt(usize),
    pub const Writer = struct {
        sink: Delivery,
        buffer: [16 * 1024]u8 = undefined,
        writer: std.Io.Writer = undefined,
        failure: ?anyerror = null,
        pub fn init(self: *@This(), sink: Delivery) void {
            self.sink = sink;
            self.writer = .{ .vtable = &.{ .drain = drain }, .buffer = &self.buffer };
        }
        fn send(self: *@This(), bytes: []const u8) std.Io.Writer.Error!void {
            self.sink.write_fn(self.sink.ptr, bytes) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
        }
        fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
            try self.send(w.buffered());
            w.end = 0;
            var count: usize = 0;
            for (data[0 .. data.len - 1]) |bytes| {
                try self.send(bytes);
                count += bytes.len;
            }
            const pattern = data[data.len - 1];
            for (0..splat) |_| try self.send(pattern);
            return count + pattern.len * splat;
        }
    };
};
