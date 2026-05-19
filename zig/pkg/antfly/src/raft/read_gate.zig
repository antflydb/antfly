// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");

pub const EnrichmentReadKind = enum {
    search,
    lookup,
    scan,
};

pub const ReadConsistency = enum {
    stale,
    leader_lease,
    read_index,
};

pub const ReadableLeaseRequester = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        request_readable_lease: *const fn (ptr: *anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void,
    };

    pub fn requestReadableLease(self: ReadableLeaseRequester, group_id: u64, request_ctx: []const u8) !void {
        try self.vtable.request_readable_lease(self.ptr, group_id, request_ctx);
    }
};

pub const ReadableLeaseCallback = *const fn (ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void;

pub const CallbackReadableLeaseRequester = struct {
    ctx: ?*anyopaque,
    callback: ReadableLeaseCallback,

    pub fn init(ctx: ?*anyopaque, callback: ReadableLeaseCallback) CallbackReadableLeaseRequester {
        return .{
            .ctx = ctx,
            .callback = callback,
        };
    }

    pub fn requester(self: *const CallbackReadableLeaseRequester) ReadableLeaseRequester {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .request_readable_lease = requestReadableLease,
            },
        };
    }

    fn requestReadableLease(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *const CallbackReadableLeaseRequester = @ptrCast(@alignCast(ptr));
        try self.callback(self.ctx, group_id, request_ctx);
    }
};

pub fn noopReadableLeaseRequester() ReadableLeaseRequester {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .request_readable_lease = requestReadableLeaseNoop,
        },
    };
}

fn requestReadableLeaseNoop(_: *anyopaque, _: u64, _: []const u8) !void {}

pub const EnrichmentReadGate = struct {
    requester: ReadableLeaseRequester,

    pub fn init(requester: ReadableLeaseRequester) EnrichmentReadGate {
        return .{ .requester = requester };
    }

    pub fn prepare(
        self: EnrichmentReadGate,
        group_id: u64,
        kind: EnrichmentReadKind,
        consistency: ReadConsistency,
    ) !void {
        if (consistency == .stale) return;

        var buf: [96]u8 = undefined;
        const request_ctx = try std.fmt.bufPrint(
            &buf,
            "enrichment:{s}:{s}",
            .{ @tagName(kind), @tagName(consistency) },
        );
        try self.requester.requestReadableLease(group_id, request_ctx);
    }

    pub fn prepareSearch(
        self: EnrichmentReadGate,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: ReadConsistency,
    ) !void {
        _ = req;
        try self.prepare(group_id, .search, consistency);
    }

    pub fn prepareLookup(
        self: EnrichmentReadGate,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: ReadConsistency,
    ) !void {
        _ = key;
        _ = opts;
        try self.prepare(group_id, .lookup, consistency);
    }

    pub fn prepareScan(
        self: EnrichmentReadGate,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: ReadConsistency,
    ) !void {
        _ = from_key;
        _ = to_key;
        _ = opts;
        try self.prepare(group_id, .scan, consistency);
    }
};

test "enrichment read gate supports explicit consistency modes" {
    const Recorder = struct {
        group_id: u64 = 0,
        request_ctx: [64]u8 = undefined,
        request_ctx_len: usize = 0,
        request_count: usize = 0,

        fn requester(self: *@This()) ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{
                    .request_readable_lease = requestReadableLease,
                },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.group_id = group_id;
            @memcpy(self.request_ctx[0..request_ctx.len], request_ctx);
            self.request_ctx_len = request_ctx.len;
            self.request_count += 1;
        }
    };

    var recorder = Recorder{};
    const gate = EnrichmentReadGate.init(recorder.requester());
    try gate.prepare(77, .search, .read_index);

    try std.testing.expectEqual(@as(u64, 77), recorder.group_id);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepare(77, .lookup, .leader_lease);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.request_ctx[0..recorder.request_ctx_len]);

    const requests_before_stale = recorder.request_count;
    try gate.prepare(77, .scan, .stale);
    try std.testing.expectEqual(requests_before_stale, recorder.request_count);

    try gate.prepareSearch(77, .{}, .read_index);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepareLookup(77, "doc:a", .{}, .leader_lease);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepareScan(77, "doc:a", "doc:z", .{}, .read_index);
    try std.testing.expectEqualStrings("enrichment:scan:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);
}

test "callback readable lease requester forwards calls" {
    const Recorder = struct {
        group_id: u64 = 0,
        request_ctx: [64]u8 = undefined,
        request_ctx_len: usize = 0,

        fn callback(ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.group_id = group_id;
            @memcpy(self.request_ctx[0..request_ctx.len], request_ctx);
            self.request_ctx_len = request_ctx.len;
        }
    };

    var recorder = Recorder{};
    const callback_requester = CallbackReadableLeaseRequester.init(&recorder, Recorder.callback);
    try callback_requester.requester().requestReadableLease(91, "enrichment:lookup");

    try std.testing.expectEqual(@as(u64, 91), recorder.group_id);
    try std.testing.expectEqualStrings("enrichment:lookup", recorder.request_ctx[0..recorder.request_ctx_len]);
}
