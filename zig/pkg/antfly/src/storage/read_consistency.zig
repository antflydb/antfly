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

//! Borrowed consistency barriers for local read consumers. Quorum tracking
//! and ReadIndex initiation remain server responsibilities.
const std = @import("std");
const db_types = @import("db/types.zig");
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

/// Starts a Raft ReadIndex operation and returns as soon as it has been
/// admitted to the local RawNode. This is deliberately not a read barrier:
/// callers that will inspect state must separately wait for a ReadSafetyBarrier.
pub const ReadIndexRequester = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        request_read_index: *const fn (ptr: *anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void,
    };

    pub fn requestReadIndex(self: ReadIndexRequester, group_id: u64, request_ctx: []const u8) !void {
        try self.vtable.request_read_index(self.ptr, group_id, request_ctx);
    }
};

pub const ReadSafetyBarrier = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        wait_read_safe: *const fn (ptr: *anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void,
        capture_frozen: ?*const fn (*anyopaque, u64) anyerror!FrozenProof = null,
        validate_frozen: ?*const fn (*anyopaque, u64, FrozenProof, ?u64, @import("antfly_cancellation").CancellationToken) anyerror!void = null,
    };

    pub const FrozenProof = struct {
        incarnation: u128,
        term: u64,
        applied_index: u64,

        pub fn validate(self: @This(), incarnation: u128, term: u64, is_leader: bool, quorum_index: u64) !void {
            if (!is_leader or incarnation != self.incarnation or term != self.term) return error.NotLeader;
            if (quorum_index > self.applied_index) return error.ReadUnavailable;
        }
    };

    pub fn waitReadSafe(self: ReadSafetyBarrier, group_id: u64, request_ctx: []const u8) !void {
        try self.vtable.wait_read_safe(self.ptr, group_id, request_ctx);
    }
};

pub const ReadSafetyCallback = *const fn (ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) anyerror!void;

pub const CallbackReadSafetyBarrier = struct {
    ctx: ?*anyopaque,
    callback: ReadSafetyCallback,

    pub fn init(ctx: ?*anyopaque, callback: ReadSafetyCallback) CallbackReadSafetyBarrier {
        return .{
            .ctx = ctx,
            .callback = callback,
        };
    }

    pub fn barrier(self: *const CallbackReadSafetyBarrier) ReadSafetyBarrier {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .wait_read_safe = waitReadSafe,
            },
        };
    }

    fn waitReadSafe(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *const CallbackReadSafetyBarrier = @ptrCast(@alignCast(ptr));
        try self.callback(self.ctx, group_id, request_ctx);
    }
};

/// Use only when the caller owns non-replicated or otherwise already-fenced
/// state. The explicit name makes this proof obligation visible at call sites.
pub fn alreadyReadSafeBarrier() ReadSafetyBarrier {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .wait_read_safe = waitReadSafeNoop,
        },
    };
}

fn waitReadSafeNoop(_: *anyopaque, _: u64, _: []const u8) !void {}

/// Fail-closed placeholder for a replicated read source whose owner has not
/// installed its applied-state barrier yet.
pub fn unavailableReadSafetyBarrier() ReadSafetyBarrier {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .wait_read_safe = waitReadSafeUnavailable,
        },
    };
}

fn waitReadSafeUnavailable(_: *anyopaque, _: u64, _: []const u8) !void {
    return error.ReadSafetyBarrierUnavailable;
}

pub const EnrichmentReadIndexRequester = struct {
    requester: ReadIndexRequester,

    pub fn init(requester: ReadIndexRequester) EnrichmentReadIndexRequester {
        return .{ .requester = requester };
    }

    pub fn request(
        self: EnrichmentReadIndexRequester,
        group_id: u64,
        kind: EnrichmentReadKind,
        consistency: ReadConsistency,
    ) !void {
        if (consistency == .stale) return;
        var buf: [96]u8 = undefined;
        const request_ctx = try enrichmentRequestContext(&buf, kind, consistency);
        try self.requester.requestReadIndex(group_id, request_ctx);
    }

    pub fn requestSearch(self: EnrichmentReadIndexRequester, group_id: u64, consistency: ReadConsistency) !void {
        try self.request(group_id, .search, consistency);
    }

    pub fn requestLookup(self: EnrichmentReadIndexRequester, group_id: u64, consistency: ReadConsistency) !void {
        try self.request(group_id, .lookup, consistency);
    }

    pub fn requestScan(self: EnrichmentReadIndexRequester, group_id: u64, consistency: ReadConsistency) !void {
        try self.request(group_id, .scan, consistency);
    }
};

pub const EnrichmentReadGate = struct {
    barrier: ReadSafetyBarrier,

    pub fn init(barrier: ReadSafetyBarrier) EnrichmentReadGate {
        return .{ .barrier = barrier };
    }

    pub fn prepare(
        self: EnrichmentReadGate,
        group_id: u64,
        kind: EnrichmentReadKind,
        consistency: ReadConsistency,
    ) !void {
        if (consistency == .stale) return;

        var buf: [96]u8 = undefined;
        const request_ctx = try enrichmentRequestContext(&buf, kind, consistency);
        try self.barrier.waitReadSafe(group_id, request_ctx);
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

fn enrichmentRequestContext(
    buf: []u8,
    kind: EnrichmentReadKind,
    consistency: ReadConsistency,
) ![]const u8 {
    return try std.fmt.bufPrint(
        buf,
        "enrichment:{s}:{s}",
        .{ @tagName(kind), @tagName(consistency) },
    );
}

pub const FeatureReads = struct {
    gate: EnrichmentReadGate,

    pub fn init(read_safety_barrier: ReadSafetyBarrier) FeatureReads {
        return .{ .gate = EnrichmentReadGate.init(read_safety_barrier) };
    }

    pub fn prepareSearchWithConsistency(
        self: FeatureReads,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: ReadConsistency,
    ) !void {
        try self.gate.prepareSearch(group_id, req, consistency);
    }

    pub fn prepareSearch(self: FeatureReads, group_id: u64, req: db_types.SearchRequest) !void {
        try self.prepareSearchWithConsistency(group_id, req, .read_index);
    }

    pub fn prepareLookupWithConsistency(
        self: FeatureReads,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: ReadConsistency,
    ) !void {
        try self.gate.prepareLookup(group_id, key, opts, consistency);
    }

    pub fn prepareLookup(self: FeatureReads, group_id: u64, key: []const u8, opts: db_types.LookupOptions) !void {
        try self.prepareLookupWithConsistency(group_id, key, opts, .read_index);
    }

    pub fn prepareScanWithConsistency(
        self: FeatureReads,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: ReadConsistency,
    ) !void {
        try self.gate.prepareScan(group_id, from_key, to_key, opts, consistency);
    }

    pub fn prepareScan(
        self: FeatureReads,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
    ) !void {
        try self.prepareScanWithConsistency(group_id, from_key, to_key, opts, .read_index);
    }
};
