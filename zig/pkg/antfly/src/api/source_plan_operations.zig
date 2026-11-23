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

//! Authenticated HTTP adapters supply service authority; these operations bind
//! the immutable route, request and evidence across the callback ABI boundary.
const std = @import("std");
const local = @import("antfly_local_sources");
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
pub const wire = @import("../data/source_plan_http_wire.zig");
const A = std.mem.Allocator;
pub const ControlPort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.ControlRequest, operation.RequestContext) anyerror!wire.ControlReply,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { execute: @FieldType(ControlPort, "execute_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn execute(self: ControlPort, a: A, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.ControlRequest, context: operation.RequestContext) !wire.ControlReply {
        return Boundary.call("execute", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, fence, table, request, context });
    }
};
pub const ObservePort = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.ObserveRequest, operation.RequestContext) anyerror!wire.control.Observation,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(ObservePort, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn read(self: ObservePort, a: A, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.ObserveRequest, context: operation.RequestContext) !wire.control.Observation {
        return Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, request, context });
    }
};
pub const ChunkPort = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.ChunkRequest, operation.RequestContext) anyerror!wire.control.Read,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(ChunkPort, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn read(self: ChunkPort, a: A, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.ChunkRequest, context: operation.RequestContext) !wire.control.Read {
        return Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, request, context });
    }
};
const Scope = @import("source_owner_scope.zig").Scope;

pub fn executeControl(port: ?ControlPort, a: A, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !wire.ControlReply {
    try context.ensureActive();
    var parsed = try wire.parse(wire.ControlRequest, a, body);
    defer parsed.deinit();
    const request = parsed.value;
    try request.validate(a);
    var scope = try Scope.init(a, group, table, request.source_owner, context);
    defer scope.deinit();
    const digest = try request.digest(a);
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    var reply = target.execute(a, scope.fence, table, request, context) catch |err| {
        if (err == error.RaftBatchWriteOutcomeUnknown) return .{ .request_digest = digest, .receipt = request.receipt };
        return err;
    };
    reply.validate(request, digest) catch return error.SourceOutcomeUnresolved;
    context.ensureActive() catch {
        reply.outcome = null;
        return reply;
    };
    return reply;
}
pub fn executeObserve(port: ?ObservePort, a: A, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !wire.native.ObservationReply {
    try context.ensureActive();
    var parsed = try wire.parse(wire.ObserveRequest, a, body);
    defer parsed.deinit();
    const request = parsed.value;
    try request.validate();
    var scope = try Scope.init(a, group, table, request.source_owner, context);
    defer scope.deinit();
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    const result = try target.read(a, scope.fence, table, request, context);
    result.validate(request.query) catch return error.SourceAdmissionCorrupt;
    try context.ensureActive();
    return .{ .version = 1, .query_digest = request.query.digest(), .observation = result };
}
pub fn executeChunk(port: ?ChunkPort, a: A, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !wire.native.ChunkReply {
    try context.ensureActive();
    var parsed = try wire.parse(wire.ChunkRequest, a, body);
    defer parsed.deinit();
    const request = parsed.value;
    try request.validate();
    var scope = try Scope.init(a, group, table, request.source_owner, context);
    defer scope.deinit();
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    const result = try target.read(a, scope.fence, table, request, context);
    defer a.free(result.bytes);
    result.observation.validate(request.request.query) catch return error.SourceAdmissionCorrupt;
    const status = result.observation.status orelse return error.SourceAdmissionCorrupt;
    if (status.seal == null or status.fence != null or status.reclamation != null or !std.mem.eql(u8, &status.seal.?, &request.request.seal)) return error.SourceAdmissionCorrupt;
    const spec = request.request.query.spec.?;
    if (result.bytes.len != try spec.chunkLen(request.request.ordinal) or !std.mem.eql(u8, &wire.control.storage.chunkDigest(request.request.ordinal, result.bytes), &spec.chunk_hashes[request.request.ordinal])) return error.SourceAdmissionCorrupt;
    const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(result.bytes.len));
    errdefer a.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, result.bytes);
    try context.ensureActive();
    return .{ .version = 1, .request_digest = request.request.digest(), .observation = result.observation, .chunk_base64 = encoded };
}

test "source plan HTTP ingress fences before dispatch and preserves lost recovery receipts" {
    const a = std.testing.allocator;
    var fixture = try wire.test_support.Fixture.init();
    const Fake = struct {
        calls: usize = 0,
        mode: enum { applied, unknown, malformed, cancel } = .applied,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn run(ptr: *anyopaque, allocator: A, _: metadata.CatalogRouteFence, _: []const u8, input: wire.ControlRequest, _: operation.RequestContext) !wire.ControlReply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.mode == .unknown) return error.RaftBatchWriteOutcomeUnknown;
            if (self.mode == .cancel) self.cancelled.store(true, .release);
            return .{ .request_digest = try input.digest(allocator), .receipt = input.receipt orelse .{ .term = 1, .index = 2 }, .outcome = if (self.mode == .malformed) .{ .applied = .{} } else .{ .applied = .{ .present = true } } };
        }
    };
    var fake: Fake = .{};
    const port: ControlPort = .{ .ptr = &fake, .execute_fn = Fake.run };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = fixture.source_owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence };
    var request = try fixture.begin();
    const body = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(body);
    try std.testing.expectError(error.InvalidCatalogRouteFence, executeControl(port, a, 17, "docs", body, .{}));
    try std.testing.expectError(error.InvalidCatalogRouteFence, executeControl(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    const accepted = try executeControl(port, a, 17, "docs", body, context);
    try std.testing.expect(accepted.confirmed());
    fake.mode = .unknown;
    const unknown = try executeControl(port, a, 17, "docs", body, context);
    try std.testing.expect(!unknown.confirmed() and unknown.receipt == null);
    request.operation = .recover;
    request.receipt = accepted.receipt;
    const recovery = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(recovery);
    const recovered = try executeControl(port, a, 17, "docs", recovery, context);
    try std.testing.expectEqualDeep(request.receipt, recovered.receipt);
    try std.testing.expect(!recovered.confirmed());
    fake.mode = .malformed;
    try std.testing.expectError(error.SourceOutcomeUnresolved, executeControl(port, a, 17, "docs", body, context));
    fake.mode = .cancel;
    var cancelled = context;
    cancelled.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled);
    const after_cancel = try executeControl(port, a, 17, "docs", recovery, cancelled);
    try std.testing.expect(!after_cancel.confirmed());
    const calls = fake.calls;
    try std.testing.expectError(error.Canceled, executeControl(port, a, 17, "docs", recovery, cancelled));
    try std.testing.expectEqual(calls, fake.calls);
    try std.testing.expectError(error.Canceled, executeControl(port, a, 17, "docs", "invalid JSON", cancelled));
    try std.testing.expectError(error.Canceled, executeObserve(null, a, 17, "docs", "invalid JSON", cancelled));
    try std.testing.expectError(error.Canceled, executeChunk(null, a, 17, "docs", "invalid JSON", cancelled));
}

test "source plan HTTP read ingress validates owned bytes before serialization" {
    const a = std.testing.allocator;
    var fixture = try wire.test_support.Fixture.init();
    const Fake = struct {
        fixture: *wire.test_support.Fixture,
        corrupt: bool = false,
        fn observe(ptr: *anyopaque, allocator: A, _: metadata.CatalogRouteFence, _: []const u8, _: wire.ObserveRequest, _: operation.RequestContext) !wire.control.Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.fixture.sealed(allocator);
        }
        fn chunk(ptr: *anyopaque, allocator: A, _: metadata.CatalogRouteFence, _: []const u8, _: wire.ChunkRequest, _: operation.RequestContext) !wire.control.Read {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const observation = try self.fixture.sealed(allocator);
            const bytes = try allocator.dupe(u8, &self.fixture.frame);
            if (self.corrupt) bytes[127] ^= 1;
            return .{ .observation = observation, .bytes = bytes };
        }
    };
    var fake: Fake = .{ .fixture = &fixture };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = fixture.source_owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence };
    const begin = try fixture.begin();
    const query: wire.control.Query = .{ .root = begin.command.root, .spec = begin.command.spec, .minimum_applied_index = 2 };
    const observation = try fixture.sealed(a);
    const request: wire.ObserveRequest = .{ .version = 1, .source_owner = fixture.source_owner, .query = query };
    const body = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(body);
    const reply = try executeObserve(.{ .ptr = &fake, .read_fn = Fake.observe }, a, 17, "docs", body, context);
    try std.testing.expectEqual(query.digest(), reply.query_digest);
    const read: wire.ChunkRequest = .{ .version = 1, .source_owner = fixture.source_owner, .request = .{ .query = query, .seal = observation.status.?.seal.?, .ordinal = 0 } };
    const read_body = try std.json.Stringify.valueAlloc(a, read, .{});
    defer a.free(read_body);
    const read_reply = try executeChunk(.{ .ptr = &fake, .read_fn = Fake.chunk }, a, 17, "docs", read_body, context);
    defer a.free(read_reply.chunk_base64);
    const encoded_reply = try std.json.Stringify.valueAlloc(a, read_reply, .{});
    defer a.free(encoded_reply);
    const owned = try wire.native.decodeChunkReply(a, read.request, encoded_reply);
    defer a.free(owned.bytes);
    try std.testing.expectEqualSlices(u8, &fixture.frame, owned.bytes);
    fake.corrupt = true;
    try std.testing.expectError(error.SourceAdmissionCorrupt, executeChunk(.{ .ptr = &fake, .read_fn = Fake.chunk }, a, 17, "docs", read_body, context));
}
