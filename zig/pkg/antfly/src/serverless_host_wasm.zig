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

//! Workers kernel for the portable serverless WAL and visibility contracts.
//! No filesystem, threads, native HTTP, model weights, or replay-on-I/O.
const std = @import("std");
const objects = @import("objectstore");
const Wal = @import("serverless/wal/indexed_object_store.zig").IndexedObjectStore;
const wal_types = @import("serverless/wal/types.zig");
const visibility = @import("serverless/visibility.zig");
const a = std.heap.wasm_allocator;

export fn antfly_host_alloc(len: usize) usize {
    const bytes = a.alloc(u8, len) catch return 0;
    return @intFromPtr(bytes.ptr);
}
export fn antfly_host_free(ptr: [*]u8, len: usize) void {
    a.free(ptr[0..len]);
}

const Request = struct {
    op: enum { append, append_batch, read, latest, truncate },
    bucket: []const u8,
    prefix: []const u8,
    /// Authenticated physical incarnation, not a caller-controlled table alias.
    namespace: []const u8,
    timestamp_ns: []const u8 = "0",
    operation_id: ?[]const u8 = null,
    payload: []const u8 = "",
    payloads: []const []const u8 = &.{},
    start_lsn: []const u8 = "1",
    end_lsn: ?[]const u8 = null,
    keep_from_lsn: []const u8 = "1",
    required_visibility: ?[]const u8 = null,
    max_records: usize = 4096,
    max_bytes: usize = 8 * 1024 * 1024,
};
fn number(text: []const u8) !u64 {
    return std.fmt.parseInt(u64, text, 10);
}
fn json(value: anytype) ![]u8 {
    return std.json.Stringify.valueAlloc(a, value, .{});
}
fn execute(raw: []const u8) ![]u8 {
    if (raw.len > 1024 * 1024) return error.RequestTooLarge;
    const operation = try std.json.parseFromSlice(struct { op: []const u8 }, a, raw, .{ .ignore_unknown_fields = true });
    defer operation.deinit();
    if (!std.mem.eql(u8, operation.value.op, "append") and !std.mem.eql(u8, operation.value.op, "append_batch") and !std.mem.eql(u8, operation.value.op, "read") and
        !std.mem.eql(u8, operation.value.op, "latest") and !std.mem.eql(u8, operation.value.op, "truncate"))
    {
        if (comptime @hasDecl(@import("root"), "requestAdditional")) return @import("root").requestAdditional(raw);
        return error.UnsupportedHostedOperation;
    }
    const parsed = try std.json.parseFromSlice(Request, a, raw, .{});
    defer parsed.deinit();
    const req = parsed.value;
    var host = objects.wasm_host.Host{ .alloc = a };
    var wal = try Wal.init(a, host.client(), req.bucket, req.prefix);
    defer wal.deinit();
    switch (req.op) {
        .append => {
            const id = req.operation_id orelse return error.InvalidWalOperationId;
            const lsn = try wal.appendIdempotent(req.namespace, try number(req.timestamp_ns), req.payload, id);
            const token = visibility.Token.init(req.namespace, lsn).encode();
            return json(.{ .visibility = token, .committed = true });
        },
        .append_batch => {
            const id = req.operation_id orelse return error.InvalidWalOperationId;
            const result = try wal.appendBatchIdempotent(req.namespace, try number(req.timestamp_ns), req.payloads, id);
            return json(.{ .start_visibility = visibility.Token.init(req.namespace, result.start_lsn).encode(), .visibility = visibility.Token.init(req.namespace, result.end_lsn).encode(), .committed = true });
        },
        .latest => {
            const latest = try wal.latestLsn(req.namespace);
            return json(.{ .visibility = visibility.Token.init(req.namespace, latest).encode() });
        },
        .truncate => {
            const removed = try wal.truncatePrefix(req.namespace, try number(req.keep_from_lsn));
            var buf: [20]u8 = undefined;
            return json(.{ .removed = try std.fmt.bufPrint(&buf, "{d}", .{removed}) });
        },
        .read => {
            const latest = try wal.latestLsn(req.namespace);
            const required = if (req.required_visibility) |token| try visibility.Token.decode(token) else null;
            if (required) |token| try token.require(req.namespace, latest);
            const cutoff = if (req.end_lsn) |end| try number(end) else latest;
            if (required) |token| try token.require(req.namespace, cutoff);
            if (req.max_records > 4096 or req.max_bytes > 8 * 1024 * 1024) return error.WalReadBudgetExceeded;
            const records = try wal.readRangeAlloc(a, req.namespace, .{
                .start_lsn = try number(req.start_lsn),
                .end_lsn = cutoff,
                .max_records = req.max_records,
                .max_bytes = req.max_bytes,
            });
            defer wal_types.freeRecords(a, records);
            // Sequence numbers remain decimal strings across the JS boundary.
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const out = try arena.allocator().alloc(struct { lsn: []const u8, timestamp_ns: []const u8, payload_base64: []const u8 }, records.len);
            for (records, out) |record, *item| item.* = .{
                .lsn = try std.fmt.allocPrint(arena.allocator(), "{d}", .{record.lsn}),
                .timestamp_ns = try std.fmt.allocPrint(arena.allocator(), "{d}", .{record.timestamp_ns}),
                .payload_base64 = encoded: {
                    const encoder = std.base64.standard.Encoder;
                    const bytes = try arena.allocator().alloc(u8, encoder.calcSize(record.payload.len));
                    break :encoded encoder.encode(bytes, record.payload);
                },
            };
            return json(.{ .visibility = visibility.Token.init(req.namespace, cutoff).encode(), .records = out });
        },
    }
}

/// Packed pointer/length of an owned JSON response. The host frees it once.
export fn antfly_host_request(ptr: [*]const u8, len: usize) u64 {
    const result = execute(ptr[0..len]) catch |err| json(.{ .err = @errorName(err) }) catch return 0;
    return (@as(u64, result.len) << 32) | @as(u64, @intFromPtr(result.ptr));
}
