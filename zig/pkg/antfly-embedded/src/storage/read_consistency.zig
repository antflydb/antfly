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
        wait_read_safe_controlled: ?*const fn (*anyopaque, u64, []const u8, Controls) anyerror!void = null,
        capture_frozen: ?*const fn (*anyopaque, u64) anyerror!FrozenProof = null,
        validate_frozen: ?*const fn (*anyopaque, u64, FrozenProof, ?u64, @import("antfly_cancellation").CancellationToken) anyerror!void = null,
        /// Immutable images need a quorum-completed prefix, not a freeze of
        /// today's moving commit tip. Returns the ACTUAL completed boundary.
        certify_completed_prefix: ?*const fn (*anyopaque, u64, u64, u64, ?u64, @import("antfly_cancellation").CancellationToken) anyerror!FrozenProof = null,
    };

    pub const CompletedPrefix = struct {
        incarnation: u128,
        replica_epoch: u64,
        term: u64,
        minimum_index: u64,

        /// Caller supplies a correlated ReadState and the shared installation/
        /// apply completion cursor under the host lock. A raw Raft applied or
        /// commit cursor is NOT a substitute. Unrelated later commits cannot
        /// invalidate an immutable prefix which has actually completed.
        pub fn qualify(self: @This(), incarnation: u128, replica_epoch: ?u64, term: u64, is_leader: bool, quorum_index: ?u64, completed_index: u64) !?FrozenProof {
            if (self.incarnation == 0 or self.replica_epoch == 0 or self.term == 0 or self.minimum_index == 0) return error.InvalidControlReceiptPosition;
            if (!is_leader or incarnation != self.incarnation or replica_epoch != self.replica_epoch or term != self.term) return error.NotLeader;
            const quorum = quorum_index orelse return null;
            if (completed_index < @max(self.minimum_index, quorum)) return null;
            return .{ .incarnation = incarnation, .term = term, .applied_index = completed_index };
        }
    };

    /// Relative budget: the caller and quorum owner may use different clocks.
    pub const Controls = struct {
        timeout_ms: ?u32 = null,
        cancellation: db_types.CancellationToken = .none,

        fn capture(deadline: ?u64, now: u64, cancellation: ?db_types.CancellationToken) Controls {
            return .{
                .timeout_ms = if (deadline) |end| @intCast(@min(std.math.maxInt(u32), std.math.divCeil(u64, end -| now, std.time.ns_per_ms) catch unreachable)) else null,
                .cancellation = cancellation orelse .none,
            };
        }
    };

    pub const FrozenProof = struct {
        incarnation: u128 = 0,
        term: u64 = 0,
        applied_index: u64 = 0,
        native: ?NativeCut = null,

        pub const NativeCut = struct {
            descriptor_fingerprint: [32]u8,
            native_instance: [16]u8,
            primary_sequence: u64,
        };
        pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
            return (comptime std.mem.eql(u8, field, "native")) and self.native == null;
        }
        pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
            if (self.native) |native| return stream.write(.{ .incarnation = self.incarnation, .term = self.term, .applied_index = self.applied_index, .native = native });
            return stream.write(.{ .incarnation = self.incarnation, .term = self.term, .applied_index = self.applied_index });
        }
        pub fn fromNative(descriptor: @import("../common/topology_records.zig").RelationPersistence.Descriptor, native_instance: [16]u8, sequence: u64) !FrozenProof {
            if (std.mem.allEqual(u8, &native_instance, 0)) return error.SqlRelationOwnerChanged;
            return .{ .native = .{ .descriptor_fingerprint = try descriptor.fingerprint(), .native_instance = native_instance, .primary_sequence = sequence } };
        }
        pub fn requireNative(self: FrozenProof, descriptor: @import("../common/topology_records.zig").RelationPersistence.Descriptor, native_instance: [16]u8) !void {
            const native = self.native orelse return error.SqlStatementSnapshotRequired;
            if (self.incarnation != 0 or self.term != 0 or self.applied_index != 0 or
                std.mem.allEqual(u8, &native_instance, 0) or !std.mem.eql(u8, &native.native_instance, &native_instance) or
                !std.mem.eql(u8, &native.descriptor_fingerprint, &try descriptor.fingerprint())) return error.SqlRelationOwnerChanged;
        }

        /// A retained immutable root was certified while capture was frozen.
        /// Later opens require a newly quorum-completed prefix on the same
        /// owner/term, which may include subsequent writes. Never use this to
        /// validate a live capture fence or fabricate completion from Raft tips.
        pub fn validateRetained(self: @This(), completed: FrozenProof) !void {
            if (self.native) |captured| {
                const current = completed.native orelse return error.SqlRelationOwnerChanged;
                if (self.incarnation != 0 or self.term != 0 or self.applied_index != 0 or
                    completed.incarnation != 0 or completed.term != 0 or completed.applied_index != 0 or
                    std.mem.allEqual(u8, &captured.native_instance, 0) or !std.mem.eql(u8, &captured.native_instance, &current.native_instance) or
                    !std.mem.eql(u8, &captured.descriptor_fingerprint, &current.descriptor_fingerprint)) return error.SqlRelationOwnerChanged;
                if (current.primary_sequence < captured.primary_sequence) return error.ReadUnavailable;
                return;
            }
            if (completed.native != null) return error.SqlRelationOwnerChanged;
            if (self.incarnation == 0 or self.term == 0 or self.applied_index == 0) return error.InvalidControlReceiptPosition;
            if (completed.incarnation != self.incarnation or completed.term != self.term) return error.NotLeader;
            if (completed.applied_index < self.applied_index) return error.ReadUnavailable;
        }

        pub fn validate(self: @This(), incarnation: u128, term: u64, is_leader: bool, quorum_index: u64) !void {
            if (self.native != null) return error.SqlRelationOwnerChanged;
            if (!is_leader or incarnation != self.incarnation or term != self.term) return error.NotLeader;
            if (quorum_index > self.applied_index) return error.ReadUnavailable;
        }
    };

    pub fn waitReadSafe(self: ReadSafetyBarrier, group_id: u64, request_ctx: []const u8) !void {
        try self.vtable.wait_read_safe(self.ptr, group_id, request_ctx);
    }
    pub fn waitReadSafeControlled(self: ReadSafetyBarrier, group_id: u64, request_ctx: []const u8, controls: Controls) !void {
        if (controls.cancellation.isCancelled()) return error.Canceled;
        if (controls.timeout_ms == 0) return error.Timeout;
        if (self.vtable.wait_read_safe_controlled) |wait| {
            try wait(self.ptr, group_id, request_ctx, controls);
        } else {
            // Legacy/no-op barriers retain their existing implementation.
            // Production quorum owners expose the controlled capability.
            try self.waitReadSafe(group_id, request_ctx);
        }
        if (controls.cancellation.isCancelled()) return error.Canceled;
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
        return self.prepareControlled(group_id, kind, consistency, .{});
    }

    fn prepareControlled(self: EnrichmentReadGate, group_id: u64, kind: EnrichmentReadKind, consistency: ReadConsistency, controls: ReadSafetyBarrier.Controls) !void {
        if (consistency == .stale) return;

        var buf: [96]u8 = undefined;
        const request_ctx = try enrichmentRequestContext(&buf, kind, consistency);
        try self.barrier.waitReadSafeControlled(group_id, request_ctx, controls);
    }

    pub fn prepareSearch(
        self: EnrichmentReadGate,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: ReadConsistency,
    ) !void {
        const now = if (req.execution_deadline_ns != null) @import("antfly_platform").time.monotonicNs() else 0;
        try self.prepareControlled(group_id, .search, consistency, .capture(req.execution_deadline_ns, now, req.cancellation));
    }

    pub fn prepareLookup(
        self: EnrichmentReadGate,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: ReadConsistency,
    ) !void {
        _ = key;
        const now = if (opts.execution_deadline_ns != null) opts.executionNowNs() else 0;
        try self.prepareControlled(group_id, .lookup, consistency, .capture(opts.execution_deadline_ns, now, opts.cancellation));
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
        const now = if (opts.execution_deadline_ns != null) @import("antfly_platform").time.monotonicNs() else 0;
        try self.prepareControlled(group_id, .scan, consistency, .capture(opts.execution_deadline_ns, now, opts.cancellation));
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

test "native local frozen cut proves actual sequence and owner without inventing raft positions" {
    const descriptor: @import("../common/topology_records.zig").RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 1, .owner_store_id = 2, .generation_instance = @splat(3), .data_generation = 4 };
    const empty = try ReadSafetyBarrier.FrozenProof.fromNative(descriptor, @splat(3), 0);
    try empty.requireNative(descriptor, @splat(3));
    try std.testing.expectEqual(@as(u128, 0), empty.incarnation);
    try std.testing.expectEqual(@as(u64, 0), empty.term);
    try std.testing.expectEqual(@as(u64, 0), empty.applied_index);
    const newer = try ReadSafetyBarrier.FrozenProof.fromNative(descriptor, @splat(3), 7);
    try empty.validateRetained(newer);
    try std.testing.expectError(error.ReadUnavailable, newer.validateRetained(empty));
    // Clean adoption keeps the image/descriptor but invalidates every old live read cut.
    try std.testing.expectError(error.SqlRelationOwnerChanged, empty.requireNative(descriptor, @splat(4)));
    try std.testing.expectError(error.SqlRelationOwnerChanged, empty.validateRetained(try ReadSafetyBarrier.FrozenProof.fromNative(descriptor, @splat(4), 7)));
    var reset = descriptor;
    reset.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, empty.requireNative(reset, @splat(3)));
    try std.testing.expectError(error.SqlRelationOwnerChanged, empty.validateRetained(try ReadSafetyBarrier.FrozenProof.fromNative(reset, @splat(3), 7)));
    const raft: ReadSafetyBarrier.FrozenProof = .{ .incarnation = 1, .term = 2, .applied_index = 3 };
    try std.testing.expectError(error.SqlRelationOwnerChanged, empty.validateRetained(raft));
    try std.testing.expectError(error.SqlRelationOwnerChanged, raft.validateRetained(newer));
    try std.testing.expectError(error.SqlRelationOwnerChanged, empty.validate(1, 2, true, 3));
}

test "SQL relation lifetime ordinary frozen proof preserves HTTP and durable wire" {
    const a = std.testing.allocator;
    const proof: ReadSafetyBarrier.FrozenProof = .{ .incarnation = 1, .term = 2, .applied_index = 3 };
    const http = try std.json.Stringify.valueAlloc(a, proof, .{});
    defer a.free(http);
    try std.testing.expectEqualStrings("{\"incarnation\":1,\"term\":2,\"applied_index\":3}", http);
    const Wire = struct {
        value: ReadSafetyBarrier.FrozenProof,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            return @import("db/relational_integrity_json.zig").write(self.value, stream);
        }
    };
    const durable = try std.json.Stringify.valueAlloc(a, Wire{ .value = proof }, .{});
    defer a.free(durable);
    try std.testing.expectEqualStrings(http, durable);
}
