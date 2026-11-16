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

//! Source delivery's bridge from a metadata-reserved frozen plan to its
//! routed native artifact store. The caller must load the current journal
//! record under source ownership before invoking these methods.
const std = @import("std");
const ops = @import("source_plan_operations.zig");
const wire = ops.wire;
const storage = wire.control.storage;
const ledger = @import("../metadata/source_artifact_control.zig");
const metadata = @import("../metadata/api.zig");
const Context = @import("antfly_local_sources").api_operation.RequestContext;
const A = std.mem.Allocator;
pub const Transfer = struct {
    owner: wire.Owner,
    fence: metadata.CatalogRouteFence,
    table: []const u8,
    spec: storage.Spec,
    controls: ops.ControlPort,
    observations: ops.ObservePort,
    chunks: ops.ChunkPort,

    fn authorize(self: Transfer, record: ledger.Record) !void {
        try (ledger.State{ .next_token = record.spec.token(), .current = record }).validate();
        try self.spec.validate(try wire.entry.rootFor(self.owner));
        const expected: ledger.Spec = .{ .namespace = self.spec.namespace, .binding = self.spec.binding, .domain = self.spec.domain, .attempt = self.spec.attempt, .bytes = self.spec.bytes, .checksum = self.spec.checksum, .plan_digest = self.spec.plan_digest };
        if (std.mem.allEqual(u8, &record.reservation, 0) or !std.meta.eql(expected, record.spec)) return error.SourcePayloadConflict;
    }
    fn query(self: Transfer) !wire.control.Query {
        return .{ .root = try wire.entry.rootFor(self.owner), .spec = self.spec, .minimum_applied_index = 1 };
    }
    fn observe(self: Transfer, a: A, context: Context) !wire.control.Observation {
        const request: wire.ObserveRequest = .{ .version = 1, .source_owner = self.owner, .query = try self.query() };
        const result = try self.observations.read(a, self.fence, self.table, request, context);
        try result.validate(request.query);
        try context.ensureActive();
        return result;
    }
    fn apply(self: Transfer, a: A, input: wire.native.Command, context: Context) !storage.Status {
        const request: wire.ControlRequest = .{ .version = 1, .operation = .submit, .source_owner = self.owner, .command = input };
        try context.ensureActive();
        const digest = try request.digest(a);
        const reply = try self.controls.execute(a, self.fence, self.table, request, context);
        try reply.validate(request, digest);
        try context.ensureActive();
        if (!reply.confirmed()) return error.SourceOutcomeUnresolved;
        return switch (reply.outcome.?) {
            .applied => |status| status,
            .rejected => |reason| reason.asError(),
            .superseded => error.SourceOutcomeUnresolved,
        };
    }
    fn command(self: Transfer, action: @FieldType(wire.native.Command, "action")) !wire.native.Command {
        return .{ .version = 1, .root = try wire.entry.rootFor(self.owner), .spec = self.spec, .action = action };
    }
    /// Resume by inspecting the exact upload. Each mutation is issued once in
    /// this pass; uncertainty stops the pass before any further mutation.
    /// Native begin/chunk/seal are idempotent for this immutable upload identity.
    pub fn seal(self: Transfer, a: A, record: ledger.Record, bytes: []const u8, original: Context) !ledger.Digest {
        var request_scope = try @import("history_publication_operations.zig").Scope.init(self.fence, original);
        const context = request_scope.context();
        try context.ensureActive();
        try self.authorize(record);
        if (record.phase != .reserved) return error.SourceControlConflict;
        if (bytes.len != self.spec.bytes) return error.SourcePayloadConflict;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &checksum, .{});
        if (!std.mem.eql(u8, &checksum, &self.spec.checksum) or !std.mem.eql(u8, &try storage.frozenDigest(bytes), &self.spec.plan_digest)) return error.SourcePayloadConflict;
        for (self.spec.chunk_hashes, 0..) |expected, ordinal| {
            const start = ordinal * storage.chunk_bytes;
            if (!std.mem.eql(u8, &storage.chunkDigest(@intCast(ordinal), bytes[start..@min(bytes.len, start + storage.chunk_bytes)]), &expected)) return error.SourcePayloadConflict;
        }
        const observation = try self.observe(a, context);
        var status = observation.status orelse storage.Status{};
        if (status.fence != null or status.reclamation != null) return error.SourceReceiptExpired;
        if (status.seal) |stamp| return stamp;
        if (!status.present) status = try self.apply(a, try self.command(.begin), context);
        for (self.spec.chunk_hashes, 0..) |_, ordinal| {
            if (status.received[ordinal / 8] & (@as(u8, 1) << @as(u3, @intCast(ordinal % 8))) != 0) continue;
            const start = ordinal * storage.chunk_bytes;
            var encoded = try wire.native.Encoded.init(a, .{ .root = try wire.entry.rootFor(self.owner), .spec = self.spec, .action = .chunk, .ordinal = @intCast(ordinal), .chunk = bytes[start..@min(bytes.len, start + storage.chunk_bytes)] });
            defer encoded.deinit();
            status = try self.apply(a, encoded.command, context);
        }
        status = try self.apply(a, try self.command(.seal), context);
        return status.seal orelse error.SourceOutcomeUnresolved;
    }
    /// Independently owned frozen bytes; no provider rows or routing-dependent
    /// plan reconstruction are needed after the metadata seal is committed.
    pub fn load(self: Transfer, a: A, record: ledger.Record, original: Context) ![]u8 {
        var request_scope = try @import("history_publication_operations.zig").Scope.init(self.fence, original);
        const context = request_scope.context();
        try context.ensureActive();
        try self.authorize(record);
        if (record.phase != .sealed and record.phase != .accepted) return error.SourceControlConflict;
        const stamp = record.stamp orelse return error.InvalidSourceControl;
        const result = try a.alloc(u8, @intCast(self.spec.bytes));
        errdefer a.free(result);
        for (self.spec.chunk_hashes, 0..) |_, ordinal| {
            const read: wire.control.ReadRequest = .{ .query = try self.query(), .seal = stamp, .ordinal = @intCast(ordinal) };
            const chunk = try self.chunks.read(a, self.fence, self.table, .{ .version = 1, .source_owner = self.owner, .request = read }, context);
            defer a.free(chunk.bytes);
            try chunk.observation.validate(read.query);
            const status = chunk.observation.status orelse return error.SourcePayloadConflict;
            if (!status.present or status.fence != null or status.reclamation != null or status.seal == null or !std.mem.eql(u8, &status.seal.?, &stamp) or chunk.bytes.len != try self.spec.chunkLen(@intCast(ordinal)) or !std.mem.eql(u8, &storage.chunkDigest(@intCast(ordinal), chunk.bytes), &self.spec.chunk_hashes[ordinal])) return error.SourcePayloadConflict;
            @memcpy(result[ordinal * storage.chunk_bytes ..][0..chunk.bytes.len], chunk.bytes);
            try context.ensureActive();
        }
        var checksum: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(result, &checksum, .{});
        if (!std.mem.eql(u8, &checksum, &self.spec.checksum) or !std.mem.eql(u8, &try storage.frozenDigest(result), &self.spec.plan_digest)) return error.SourcePayloadConflict;
        try context.ensureActive();
        return result;
    }
    /// One bounded reclamation pass. Null retains the metadata charge and
    /// schedules another pass; only a native proof permits metadata release.
    pub fn reclaim(self: Transfer, a: A, record: ledger.Record, original: Context) !?ledger.Digest {
        var request_scope = try @import("history_publication_operations.zig").Scope.init(self.fence, original);
        const context = request_scope.context();
        try context.ensureActive();
        try self.authorize(record);
        if (record.phase != .garbage or record.retirement == null) return error.SourceControlConflict;
        const observation = try self.observe(a, context);
        var status = observation.status orelse return error.SourceOutcomeUnresolved;
        if (status.reclamation) |proof| return proof;
        if (status.fence == null) status = try self.apply(a, try self.command(.fence), context);
        status = try self.apply(a, try self.command(.reclaim), context);
        return status.reclamation;
    }
};

test "source plan HTTP delivery resumes lost replies from retained bytes and gates reclamation" {
    const a = std.testing.allocator;
    const Fake = struct {
        txn: storage.test_support.Txn = .{},
        index: u64 = 1,
        calls: usize = 0,
        lose: ?@FieldType(wire.native.Command, "action") = null,
        fn position(self: *@This()) [32]u8 {
            var result: [32]u8 = @splat(0);
            std.mem.writeInt(u64, result[0..8], 1, .little);
            std.mem.writeInt(u64, result[8..16], self.index, .little);
            result[31] = 1;
            return result;
        }
        fn observe(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, request: wire.ObserveRequest, _: Context) !wire.control.Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .root = request.query.root, .cut = self.position(), .quota = try storage.readQuota(&self.txn, request.query.root), .status = if (request.query.spec) |spec| try storage.inspect(alloc, &self.txn, request.query.root, spec) else null };
        }
        fn execute(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, request: wire.ControlRequest, _: Context) !wire.ControlReply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.index += 1;
            var decoded = try wire.native.fromCommand(alloc, request.command);
            defer decoded.deinit();
            const status = try wire.control.apply(alloc, &self.txn, decoded.request, self.position());
            var reply: wire.ControlReply = .{ .request_digest = try request.digest(alloc), .receipt = .{ .term = 1, .index = self.index }, .outcome = .{ .applied = status } };
            if (self.lose == request.command.action) {
                self.lose = null;
                reply.outcome = null;
            }
            return reply;
        }
        fn read(ptr: *anyopaque, alloc: A, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.ChunkRequest, context: Context) !wire.control.Read {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .observation = try observe(ptr, alloc, fence, table, .{ .version = 1, .source_owner = request.source_owner, .query = request.request.query }, context), .bytes = try storage.readSealedChunkAlloc(alloc, &self.txn, request.request.query.root, request.request.query.spec.?, request.request.seal, request.request.ordinal) };
        }
    };
    for ([_]@FieldType(wire.native.Command, "action"){ .begin, .chunk, .seal }) |lost| {
        var fixture = try wire.test_support.Fixture.init();
        var fake: Fake = .{ .lose = lost };
        defer fake.txn.deinit();
        const spec = try fixture.spec();
        const transfer: Transfer = .{ .owner = fixture.source_owner, .fence = .{ .metadata_group_id = 1, .metadata_incarnation = fixture.source_owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } }, .table = "docs", .spec = spec, .controls = .{ .ptr = &fake, .execute_fn = Fake.execute }, .observations = .{ .ptr = &fake, .read_fn = Fake.observe }, .chunks = .{ .ptr = &fake, .read_fn = Fake.read } };
        var record: ledger.Record = .{ .reservation = @splat(4), .spec = .{ .namespace = spec.namespace, .binding = spec.binding, .domain = spec.domain, .attempt = spec.attempt, .bytes = spec.bytes, .checksum = spec.checksum, .plan_digest = spec.plan_digest } };
        try std.testing.expectError(error.SourceControlConflict, transfer.reclaim(a, record, .{}));
        var invalid = record;
        invalid.phase = .garbage;
        invalid.retirement = @splat(0);
        try std.testing.expectError(error.InvalidSourceControl, transfer.reclaim(a, invalid, .{}));
        var expired = transfer;
        expired.fence.admission_deadline_ns = 0;
        try std.testing.expectError(error.DeadlineExceeded, expired.seal(a, record, &fixture.frame, .{}));
        try std.testing.expectEqual(@as(usize, 0), fake.calls);
        try std.testing.expectError(error.SourceOutcomeUnresolved, transfer.seal(a, record, &fixture.frame, .{}));
        try std.testing.expectEqual(@as(usize, switch (lost) {
            .begin => 1,
            .chunk => 2,
            .seal => 3,
            else => unreachable,
        }), fake.calls);
        const stamp = try transfer.seal(a, record, &fixture.frame, .{});
        try std.testing.expectEqual(@as(usize, 3), fake.calls);
        try std.testing.expectEqual(stamp, try transfer.seal(a, record, &fixture.frame, .{}));
        try std.testing.expectEqual(@as(usize, 3), fake.calls);
        record.phase = .sealed;
        record.stamp = stamp;
        const owned = try transfer.load(a, record, .{});
        defer a.free(owned);
        try std.testing.expectEqualSlices(u8, &fixture.frame, owned);
        var wrong = record;
        wrong.spec.checksum[0] ^= 1;
        try std.testing.expectError(error.SourcePayloadConflict, transfer.load(a, wrong, .{}));
        try std.testing.expectError(error.SourceControlConflict, transfer.reclaim(a, record, .{}));
        record.phase = .garbage;
        record.retirement = @splat(8);
        try std.testing.expect((try transfer.reclaim(a, record, .{})) != null);
        try std.testing.expectEqualSlices(u8, &fixture.frame, owned);
    }
}
