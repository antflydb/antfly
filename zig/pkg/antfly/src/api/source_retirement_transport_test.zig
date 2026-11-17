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

//! One-attempt transport and original recovery-authority qualification.
const std = @import("std");
const local = @import("antfly_local_sources");
const wire = @import("../data/source_retirement_http_wire.zig");
const http = @import("../raft/transport/http_common.zig");
const metadata = @import("../metadata/api.zig");
const responses = @import("http_route_helpers.zig");
const forwarding = @import("internal_batch_forwarding.zig");
pub fn run(comptime Client: type) !void {
    const a = std.testing.allocator;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    const owner: @FieldType(wire.control.Intent, "owner") = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 17, .topology_epoch = 1 };
    var fixture: @import("../data/source_retirement_test_fixture.zig").Fixture = .{};
    try fixture.init(owner);
    const locator = "table2:00000004:docs:17";
    fixture.members[0].locator = locator;
    fixture.command.intent.admission_digest = fixture.command.observation.decision.?.admission.admission.digest();
    fixture.command.observation.decision.?.admission.digest = fixture.command.intent.admission_digest;
    const input: wire.Request = .{ .version = 1, .request_id = @splat(91), .operation = .submit, .intent = fixture.command.intent, .coordinator_locator = locator };
    const body = try wire.encodeRequest(a, input);
    var parsed_request = try wire.decodeRequest(a, body);
    defer parsed_request.deinit();
    a.free(body);
    try std.testing.expectEqualStrings(locator, parsed_request.value.coordinator_locator.?);
    const Fake = struct {
        command: wire.control.Command,
        mode: enum { good, marker, nonce, missing_ack, duplicate_ack, lost, cut, ambiguous } = .good,
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(std.mem.endsWith(u8, request.uri, "/source-retirement/control-v1"));
            try std.testing.expectEqual(@as(?usize, wire.max_reply_bytes), request.max_response_bytes);
            try std.testing.expect(request.header(@import("internal_service_auth.zig").header_name) != null);
            if (self.mode == .lost) return error.ConnectionResetByPeer;
            var decoded = try wire.decodeRequest(alloc, request.body);
            defer decoded.deinit();
            const query = decoded.value;
            const command = if (query.recovery) |q| q.command else self.command;
            const cut: local.storage_source_scope.Cut = if (query.recovery) |q| q.entry else .{ .term = @as(u64, 1), .index = @as(u64, 20) };
            var reply: wire.Reply = .{ .request_digest = try query.digest(alloc), .command = command, .receipt = cut, .outcome = .{ .rejected = .pending } };
            if (self.mode == .nonce) reply.request_digest[0] ^= 1;
            if (self.mode == .cut) reply.receipt.?.index += 1;
            if (self.mode == .ambiguous) {
                reply.receipt = null;
                reply.outcome = null;
            }
            const bytes = try std.json.Stringify.valueAlloc(alloc, reply, .{});
            defer alloc.free(bytes);
            const Header = @typeInfo(@typeInfo(@TypeOf(responses.textResponseWithHeaders)).@"fn".param_types[3].?).pointer.child;
            var headers: [4]Header = undefined;
            var count: usize = 0;
            if (self.mode != .missing_ack) {
                headers[count] = .{ .name = metadata.catalog_route_fence_ack_header, .value = metadata.catalog_route_fence_ack_value };
                count += 1;
            }
            if (self.mode == .duplicate_ack) {
                headers[count] = headers[0];
                count += 1;
            }
            headers[count] = .{ .name = "Content-Type", .value = "application/json" };
            count += 1;
            if (self.mode == .marker) {
                headers[count] = .{ .name = forwarding.outcome_header, .value = forwarding.outcome_unknown_v1 };
                count += 1;
            }
            return responses.textResponseWithHeaders(alloc, 200, bytes, headers[0..count]);
        }
    };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    var fake: Fake = .{ .command = fixture.command };
    var client = Client.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    try std.testing.expectError(error.Unauthorized, client.fetchGroupSourceRetirementControl("http://node", 17, "docs", input, fence, .{}));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    _ = client.withInternalServiceAuth("source-retirement-client-secret-long-enough", "retirement-test");
    var known = try client.fetchGroupSourceRetirementControl("http://node", 17, "docs", input, fence, .{});
    defer known.deinit();
    try std.testing.expect(known.value.confirmed());
    try std.testing.expectEqualStrings(locator, known.value.command.?.observation.decision.?.admission.admission.participants[0].locator);
    for ([_]@TypeOf(fake.mode){ .marker, .nonce, .missing_ack, .duplicate_ack, .lost, .ambiguous }) |mode| {
        fake.mode = mode;
        const before = fake.calls;
        var result = try client.fetchGroupSourceRetirementControl("http://node", 17, "docs", input, fence, .{});
        defer result.deinit();
        try std.testing.expect(!result.value.confirmed());
        try std.testing.expectEqual(before + 1, fake.calls);
        if (mode == .marker or mode == .ambiguous) try std.testing.expect(result.value.command != null);
    }
    const recovery: wire.Request = .{ .version = 1, .request_id = @splat(92), .operation = .recover, .recovery = .{ .command = fixture.command, .entry = .{ .term = 1, .index = 20 }, .minimum_applied_index = 20 } };
    fake.mode = .good;
    var recovered = try client.fetchGroupSourceRetirementControl("http://node", 17, "docs", recovery, fence, .{});
    defer recovered.deinit();
    try std.testing.expect(recovered.value.confirmed());
    fake.mode = .cut;
    var conflicting = try client.fetchGroupSourceRetirementControl("http://node", 17, "docs", recovery, fence, .{});
    defer conflicting.deinit();
    try std.testing.expect(!conflicting.value.confirmed());
    try std.testing.expectEqualDeep(recovery.recovery.?.entry, conflicting.value.receipt.?);
    var mixed = input;
    mixed.recovery = recovery.recovery;
    try std.testing.expectError(error.InvalidSourceAdmission, mixed.validate());
    const valid_bytes = try wire.encodeRequest(a, input);
    defer a.free(valid_bytes);
    const invalid = try std.fmt.allocPrint(a, "{s},\"observation\":{{}}}}", .{valid_bytes[0 .. valid_bytes.len - 1]});
    defer a.free(invalid);
    try std.testing.expectError(error.InvalidSourceAdmission, wire.decodeRequest(a, invalid));
}
