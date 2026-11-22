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

//! Public-delivery orchestration over durable metadata authority. An adapter
//! must authorize against live policy, project/filter its public payload, and
//! certify global commit/bootstrap completeness before returning Prepared. Raw
//! root pages cannot implement that contract without those qualifications.
//!
//! Delivery receipts precede publication; client ACK precedes native reclamation.
//! Retries replay the original issued interval, even when page limits or the
//! current source head have changed. The cursor never acts as authorization.
const std = @import("std");
const control = @import("changefeed_control.zig");
const A = std.mem.Allocator;
pub const max_page_bytes = 16 * 1024 * 1024;

pub const Limits = struct {
    events: u32 = 256,
    bytes: u32 = 1024 * 1024,
    pub fn validate(self: Limits) !void {
        if (self.events == 0 or self.events > 1024 or self.bytes == 0 or self.bytes > max_page_bytes) return error.InvalidChangefeed;
    }
};
pub const Prepared = struct {
    /// Owned by the supplied attempt arena. Includes only authorized public
    /// data, with no hidden participant, ordinal, policy or source metadata.
    body: []const u8,
    events: u32,
    proof: control.Digest,
    advances: []const control.Advance,
    provider_context: []const u8 = "",
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    cursor: control.Digest,
    sequence: u64,
    body: []const u8,
    replayed: bool,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
fn equal(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn load(svc: anytype, a: A, id: control.Id, request: anytype) !control.Parsed {
    const bytes = try control.readOnService(svc, a, id, request);
    defer a.free(bytes);
    var parsed = try control.parse(a, bytes);
    errdefer parsed.deinit();
    if (parsed.value.phase != .empty and !equal(parsed.value.id, id)) return error.ChangefeedCorrupt;
    return parsed;
}
fn mutate(svc: anytype, a: A, state: control.State, mutation: control.Mutation, request: anytype) !void {
    const definition = state.definition orelse return error.ChangefeedConflict;
    return control.mutateOnService(svc, a, .{
        .id = state.id,
        .authority = state.authority,
        .owner = definition.owner,
        .authorization = definition.authorization,
        .expected_revision = state.revision,
        .expected_epoch = state.epoch,
        .mutation = mutation,
    }, request);
}
fn active(adapter: anytype, state: control.State, request: anytype) !u64 {
    try request.ensureActive();
    // Authorization precedes cursor lookup and phase/expiry disclosure.
    try adapter.authorize(state, request);
    if (state.phase != .streaming and state.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
    const now = try adapter.nowSeconds(request);
    if (now == 0 or now >= state.expires_at or (state.phase == .bootstrap and now >= state.bootstrap_expires_at)) return error.ChangefeedLeaseExpired;
    return now;
}
fn following(state: control.State, cursor: control.Digest) !?control.Delivery {
    if (equal(cursor, state.latest_cursor)) return null;
    for (state.cursors) |receipt| if (equal(receipt.before, cursor)) return receipt;
    return error.ChangefeedCursorExpired;
}
fn receiptProof(state: control.State, page: Prepared) control.Digest {
    if (page.provider_context.len != 0) return control.hash(.{ .domain = "antfly:feed-delivery-proof:v2", .consumer = state.consumerId(), .definition = state.definition.?.identity(), .payload = control.hash(page.body), .events = page.events, .provider = page.proof, .advances = page.advances, .provider_context = page.provider_context });
    return control.hash(.{ .domain = "antfly:feed-delivery-proof:v1", .consumer = state.consumerId(), .definition = state.definition.?.identity(), .payload = control.hash(page.body), .events = page.events, .provider = page.proof, .advances = page.advances });
}
fn validatePage(page: Prepared, limits: Limits) !void {
    if (!control.validProviderContext(page.provider_context)) return error.InvalidChangefeed;
    if (page.body.len > limits.bytes or page.events > limits.events) return error.ChangefeedBackpressure;
    if (std.mem.allEqual(u8, &page.proof, 0) or page.advances.len > control.max_advances) return error.InvalidChangefeed;
}
/// Adapter methods: authorize(state, request), nowSeconds(request),
/// prepare(arena, state, before, limits, request), and
/// replay(arena, state, original_receipt, limits, request). Replay must reproduce
/// the original public payload/proof/advances, not a newly sized source page.
pub fn pull(svc: anytype, adapter: anytype, a: A, id: control.Id, before: control.Digest, limits: Limits, request: anytype) !Result {
    try limits.validate();
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    var parsed = try load(svc, a, id, request);
    defer parsed.deinit();
    const state = parsed.value;
    const now = try active(adapter, state, request);
    const original = try following(state, before);
    if (original == null and state.cursors.len >= state.definition.?.cursor_window) return error.ChangefeedBackpressure;
    const page = if (original) |receipt|
        try adapter.replay(scratch, state, receipt, limits, request)
    else
        try adapter.prepare(scratch, state, before, limits, request);
    try validatePage(page, limits);
    _ = try active(adapter, state, request);
    var receipt: control.Delivery = if (original) |recorded| recorded else .{
        .sequence = std.math.add(u64, state.latest_delivery, 1) catch return error.ChangefeedBackpressure,
        .before = before,
        .cursor = @splat(0),
        .authority = receiptProof(state, page),
        .issued_at = now,
        .advances = page.advances,
        .provider_context = page.provider_context,
    };
    if (original != null) {
        if (!std.mem.eql(u8, receipt.provider_context, page.provider_context)) return error.ChangefeedResynchronizationRequired;
        if (!equal(receipt.authority, receiptProof(state, page)) or !equal(control.hash(receipt.advances), control.hash(page.advances))) return error.ChangefeedResynchronizationRequired;
    } else {
        receipt.cursor = receipt.identity(state.id, state.epoch);
        // A lost proposal/apply reply cannot publish unproven data. The next
        // request discovers the recorded receipt and replays its exact interval.
        try mutate(svc, a, state, .{ .issue = receipt }, request);
    }
    var observed = try load(svc, a, id, request);
    defer observed.deinit();
    _ = try active(adapter, observed.value, request);
    if (observed.value.epoch != state.epoch or !equal(observed.value.authority, state.authority) or !equal(observed.value.definition.?.identity(), state.definition.?.identity())) return error.ChangefeedResynchronizationRequired;
    const retained = (try following(observed.value, before)) orelse return error.ChangefeedCursorExpired;
    if (!equal(retained.cursor, receipt.cursor) or !equal(retained.authority, receipt.authority)) return error.ChangefeedResynchronizationRequired;
    return .{ .arena = arena, .cursor = receipt.cursor, .sequence = receipt.sequence, .body = page.body, .replayed = original != null };
}
/// This commits only the client ACK. A separate owner reconciliation propagates
/// observed metadata progress to roots and performs bounded physical GC.
pub fn acknowledge(svc: anytype, adapter: anytype, a: A, id: control.Id, cursor: control.Digest, request: anytype) !control.Parsed {
    for (0..4) |_| {
        var before = try load(svc, a, id, request);
        defer before.deinit();
        _ = try active(adapter, before.value, request);
        mutate(svc, a, before.value, .{ .acknowledge = cursor }, request) catch |err| switch (err) {
            error.ChangefeedConflict => continue,
            error.MetadataMutationOutcomeUnknown => {},
            else => return err,
        };
        var observed = try load(svc, a, id, request);
        errdefer observed.deinit();
        _ = try active(adapter, observed.value, request);
        if (observed.value.epoch != before.value.epoch or !equal(observed.value.authority, before.value.authority))
            return error.ChangefeedResynchronizationRequired;
        if (equal(observed.value.acknowledged_cursor, cursor)) return observed;
        if (observed.value.acknowledged_delivery > before.value.acknowledged_delivery)
            return error.ChangefeedCursorExpired;
        observed.deinit();
    }
    return error.MetadataMutationOutcomeUnknown;
}

const TestRequest = struct {
    pub fn ensureActive(_: @This()) !void {}
};
const TestService = struct {
    a: A,
    bytes: []u8,
    stamp: control.Stamp,
    metadata_group_id: u64 = 1,
    proposals: u64 = 0,
    lose_ack: bool = false,
    reject: bool = false,
    revoke_after_apply: ?*bool = null,
    pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: TestRequest, version: u16) !void {
        try std.testing.expectEqual(control.protocol_version, version);
    }
    pub fn ensureLinearizableReadWithContext(_: *@This(), _: TestRequest) !void {}
    pub fn projectedStore(self: *@This()) ?*@This() {
        return self;
    }
    pub fn getChangefeedControl(self: *@This(), a: A, _: u64, _: control.Id) ![]u8 {
        return a.dupe(u8, self.bytes);
    }
    pub fn getChangefeedAuthority(self: *@This(), _: A, _: u64) !?control.Stamp {
        return self.stamp;
    }
    pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
        self.proposals += 1;
        if (!self.reject) {
            var write = try control.parseWrite(self.a, command.apply_changefeed_control);
            defer write.deinit();
            var state = try control.parse(self.a, self.bytes);
            defer state.deinit();
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            const next = try write.value.apply(arena.allocator(), state.value, self.stamp.sequence + 1);
            const bytes = try control.encode(self.a, next);
            self.a.free(self.bytes);
            self.bytes = bytes;
            self.stamp.sequence = next.authority_sequence;
        }
        if (self.revoke_after_apply) |denied| denied.* = true;
        return self.proposals;
    }
    pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: TestRequest) !void {
        if (self.lose_ack) return error.TestLostAcknowledgment;
    }
};
const TestAdapter = struct {
    denied: bool = false,
    policy: u64 = 1,
    prepares: usize = 0,
    replays: usize = 0,
    authorizations: usize = 0,
    tamper: bool = false,
    provider_context: []const u8 = "",
    revoke_on_prepare: bool = false,
    now: u64 = 200,
    pub fn authorize(self: *@This(), state: control.State, _: TestRequest) !void {
        self.authorizations += 1;
        if (self.denied) return error.TestDenied;
        if (state.definition.?.tables[0].policy_revision != self.policy) return error.ChangefeedResynchronizationRequired;
    }
    pub fn nowSeconds(self: *@This(), _: TestRequest) !u64 {
        return self.now;
    }
    fn page(self: *@This(), a: A, namespace: control.Namespace, sequence: u64) !Prepared {
        return .{ .body = try std.fmt.allocPrint(a, "{{\"events\":[{{\"id\":{d}}}]}}", .{sequence + @as(u64, if (self.tamper) 1 else 0)}), .events = 1, .provider_context = self.provider_context, .proof = control.hash(.{ .sequence = sequence }), .advances = try a.dupe(control.Advance, &.{.{ .namespace = namespace, .position = .{ .sequence = sequence } }}) };
    }
    pub fn prepare(self: *@This(), a: A, state: control.State, _: control.Digest, _: Limits, _: TestRequest) !Prepared {
        self.prepares += 1;
        if (self.revoke_on_prepare) self.denied = true;
        return self.page(a, state.roots[0].namespace, state.roots[0].delivered.sequence + 1);
    }
    pub fn replay(self: *@This(), a: A, _: control.State, receipt: control.Delivery, _: Limits, _: TestRequest) !Prepared {
        self.replays += 1;
        return self.page(a, receipt.advances[0].namespace, receipt.advances[0].position.sequence);
    }
};
fn testService(a: A) !TestService {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const definition: control.Definition = .{ .owner = @splat(1), .authorization = @splat(2), .tables = &.{.{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_document }}, .cursor_window = 1 };
    var state = try (control.Mutation{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } }).apply(scratch, .{}, .{ .incarnation = @splat(9), .sequence = 1 });
    var namespace: control.Namespace = @splat(0);
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    namespace[23] = 1;
    state = try (control.Mutation{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} }).apply(scratch, state, .{ .incarnation = @splat(9), .sequence = 2 });
    state = try (control.Mutation{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = 2, .consumer_epoch = 1, .start = 0 } }).apply(scratch, state, .{ .incarnation = @splat(9), .sequence = 3 });
    state = try (control.Mutation{ .seal = .{ .cut = @splat(5) } }).apply(scratch, state, .{ .incarnation = @splat(9), .sequence = 4 });
    return .{ .a = a, .bytes = try control.encode(a, state), .stamp = .{ .incarnation = @splat(9), .sequence = 4 } };
}

test "feed delivery recovers lost metadata reply by replaying exact issued payload before ACK" {
    const a = std.testing.allocator;
    var svc = try testService(a);
    defer a.free(svc.bytes);
    var adapter: TestAdapter = .{};
    var initial = try control.parse(a, svc.bytes);
    defer initial.deinit();
    const cursor = initial.value.latest_cursor;
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, pull(&svc, &adapter, a, initial.value.id, cursor, .{}, TestRequest{}));
    try std.testing.expectEqual(@as(usize, 1), adapter.prepares);
    svc.lose_ack = false;
    var recovered = try pull(&svc, &adapter, a, initial.value.id, cursor, .{ .events = 100 }, TestRequest{});
    defer recovered.deinit();
    try std.testing.expect(recovered.replayed);
    try std.testing.expectEqual(@as(usize, 1), adapter.prepares);
    try std.testing.expectEqual(@as(usize, 1), adapter.replays);
    try std.testing.expectEqualStrings("{\"events\":[{\"id\":1}]}", recovered.body);
    try std.testing.expectEqual(@as(u64, 1), svc.proposals);
    try std.testing.expectError(error.ChangefeedBackpressure, pull(&svc, &adapter, a, initial.value.id, recovered.cursor, .{}, TestRequest{}));
    try std.testing.expectEqual(@as(usize, 1), adapter.prepares);
    adapter.tamper = true;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, pull(&svc, &adapter, a, initial.value.id, cursor, .{}, TestRequest{}));
    adapter.tamper = false;
    try std.testing.expectError(error.ChangefeedBackpressure, pull(&svc, &adapter, a, initial.value.id, cursor, .{ .bytes = 1 }, TestRequest{}));
    svc.lose_ack = true;
    var acknowledged = try acknowledge(&svc, &adapter, a, initial.value.id, recovered.cursor, TestRequest{});
    defer acknowledged.deinit();
    svc.lose_ack = false;
    try std.testing.expectEqual(@as(u64, 1), acknowledged.value.roots[0].acknowledged.sequence);
    try std.testing.expectEqual(@as(u64, 0), acknowledged.value.roots[0].reclaimed_through);
    try std.testing.expectError(error.ChangefeedCursorExpired, pull(&svc, &adapter, a, initial.value.id, cursor, .{}, TestRequest{}));
    var next = try pull(&svc, &adapter, a, initial.value.id, recovered.cursor, .{}, TestRequest{});
    defer next.deinit();
    try std.testing.expect(!next.replayed);
    try std.testing.expectEqual(@as(u64, 2), next.sequence);
}

test "feed delivery reauthorizes before proposal and after durable issue without exposing payload" {
    const a = std.testing.allocator;
    var svc = try testService(a);
    defer a.free(svc.bytes);
    var adapter: TestAdapter = .{ .denied = true };
    var initial = try control.parse(a, svc.bytes);
    defer initial.deinit();
    const id = initial.value.id;
    const cursor = initial.value.latest_cursor;
    try std.testing.expectError(error.TestDenied, pull(&svc, &adapter, a, id, @splat(99), .{}, TestRequest{}));
    try std.testing.expectEqual(@as(usize, 0), adapter.prepares);
    adapter.denied = false;
    adapter.revoke_on_prepare = true;
    try std.testing.expectError(error.TestDenied, pull(&svc, &adapter, a, id, cursor, .{}, TestRequest{}));
    try std.testing.expectEqual(@as(u64, 0), svc.proposals);
    adapter.denied = false;
    adapter.revoke_on_prepare = false;
    svc.revoke_after_apply = &adapter.denied;
    try std.testing.expectError(error.TestDenied, pull(&svc, &adapter, a, id, cursor, .{}, TestRequest{}));
    try std.testing.expectEqual(@as(u64, 1), svc.proposals);
    svc.revoke_after_apply = null;
    adapter.denied = false;
    adapter.policy = 2;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, pull(&svc, &adapter, a, id, cursor, .{}, TestRequest{}));
    try std.testing.expectEqual(@as(usize, 0), adapter.replays);
    adapter.policy = 1;
    adapter.now = 1000;
    try std.testing.expectError(error.ChangefeedLeaseExpired, pull(&svc, &adapter, a, id, cursor, .{}, TestRequest{}));
    try std.testing.expectEqual(@as(usize, 0), adapter.replays);
}

test "feed delivery cannot publish a rejected proposal or forge a client ACK" {
    const a = std.testing.allocator;
    var svc = try testService(a);
    defer a.free(svc.bytes);
    var adapter: TestAdapter = .{};
    var initial = try control.parse(a, svc.bytes);
    defer initial.deinit();
    svc.reject = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, pull(&svc, &adapter, a, initial.value.id, initial.value.latest_cursor, .{}, TestRequest{}));
    svc.reject = false;
    try std.testing.expectError(error.ChangefeedCursorExpired, acknowledge(&svc, &adapter, a, initial.value.id, @splat(99), TestRequest{}));
    var observed = try control.parse(a, svc.bytes);
    defer observed.deinit();
    try std.testing.expectEqual(@as(u64, 0), observed.value.latest_delivery);
    try std.testing.expectEqual(@as(u64, 0), observed.value.acknowledged_delivery);
}

test "feed delivery rejects a valid state envelope stored under a different feed identity" {
    const a = std.testing.allocator;
    var svc = try testService(a);
    defer a.free(svc.bytes);
    var adapter: TestAdapter = .{};
    try std.testing.expectError(error.ChangefeedCorrupt, pull(&svc, &adapter, a, @splat(99), @splat(1), .{}, TestRequest{}));
    try std.testing.expectEqual(@as(u64, 0), svc.proposals);
    try std.testing.expectEqual(@as(usize, 0), adapter.prepares);
}

test "feed delivery persists private continuation across replay ACK and context tampering" {
    const a = std.testing.allocator;
    var svc = try testService(a);
    defer a.free(svc.bytes);
    var adapter: TestAdapter = .{ .provider_context = "YQ" };
    var initial = try control.parse(a, svc.bytes);
    defer initial.deinit();
    var issued = try pull(&svc, &adapter, a, initial.value.id, initial.value.latest_cursor, .{}, TestRequest{});
    defer issued.deinit();
    var saved = try control.parse(a, svc.bytes);
    defer saved.deinit();
    try std.testing.expectEqualStrings("YQ", try saved.value.contextAt(issued.cursor));
    try std.testing.expectEqualStrings("", try saved.value.contextAt(initial.value.latest_cursor));
    adapter.provider_context = "Yg";
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, pull(&svc, &adapter, a, initial.value.id, initial.value.latest_cursor, .{}, TestRequest{}));
    adapter.provider_context = "YQ";
    var replayed = try pull(&svc, &adapter, a, initial.value.id, initial.value.latest_cursor, .{}, TestRequest{});
    defer replayed.deinit();
    try std.testing.expectEqualSlices(u8, &issued.cursor, &replayed.cursor);
    var acknowledged = try acknowledge(&svc, &adapter, a, initial.value.id, issued.cursor, TestRequest{});
    defer acknowledged.deinit();
    try std.testing.expectEqualStrings("YQ", try acknowledged.value.contextAt(issued.cursor));
    try std.testing.expectEqual(@as(usize, 0), acknowledged.value.cursors.len);
}
