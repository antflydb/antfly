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
const wire = @import("../data/source_scope_http_wire.zig");
const http = @import("../raft/transport/http_common.zig");
const metadata = @import("../metadata/api.zig");
const responses = @import("http_route_helpers.zig");
const forwarding = @import("internal_batch_forwarding.zig");
pub fn run(comptime Client: type) !void {
    const a = std.testing.allocator;
    const Fake = struct {
        mode: enum { good, marker, nonce, missing_ack, duplicate_ack, oversized, lost, forged_cut } = .good,
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(std.mem.endsWith(u8, request.uri, "/source-scope/control-v1"));
            try std.testing.expectEqual(@as(?usize, wire.max_reply_bytes), request.max_response_bytes);
            if (self.mode == .lost) return error.ConnectionResetByPeer;
            const input = try wire.decodeRequest(alloc, request.body);
            const command: wire.control.Command = if (input.recovery) |r| r.command else .{ .version = 1, .intent = input.intent.?, .metadata_cut = .{ .group_id = input.owner().metadata_group_id, .incarnation = input.owner().metadata_incarnation, .index = input.intent.?.minimum_metadata_index + 1 } };
            const receipt: wire.control.scope.Cut = if (input.recovery) |r| r.entry else .{ .term = 1, .index = 20 };
            var reply: wire.Reply = .{ .request_digest = try input.digest(alloc), .command = command, .receipt = receipt, .outcome = .{ .rejected = .pending } };
            if (self.mode == .nonce) reply.request_digest[0] ^= 1;
            if (self.mode == .forged_cut) reply.outcome = .{ .applied = .{ .version = 1, .owner = input.owner(), .domain = command.intent.spec.domain(), .cut = .{ .term = receipt.term + 1, .index = receipt.index }, .header = .{ .root = try wire.control.scope.Basis.fromOwner(input.owner()), .strict = true, .domains = 1, .metadata_floor = command.metadata_cut.index }, .record = .{ .spec = command.intent.spec, .bound_metadata_index = command.metadata_cut.index } } };
            const body = if (self.mode == .oversized) try alloc.alloc(u8, wire.max_reply_bytes + 1) else try std.json.Stringify.valueAlloc(alloc, reply, .{});
            defer alloc.free(body);
            if (self.mode == .oversized) @memset(body, ' ');
            const Header = @typeInfo(@typeInfo(@TypeOf(responses.textResponseWithHeaders)).@"fn".param_types[3].?).pointer.child;
            var headers: [4]Header = undefined;
            var count: usize = 0;
            if (self.mode != .missing_ack) {
                headers[count] = .{ .name = metadata.catalog_route_fence_ack_header, .value = metadata.catalog_route_fence_ack_value };
                count += 1;
            }
            if (self.mode == .duplicate_ack) {
                headers[count] = .{ .name = metadata.catalog_route_fence_ack_header, .value = metadata.catalog_route_fence_ack_value };
                count += 1;
            }
            headers[count] = .{ .name = "Content-Type", .value = "application/json" };
            count += 1;
            if (self.mode == .marker) {
                headers[count] = .{ .name = forwarding.outcome_header, .value = forwarding.outcome_unknown_v1 };
                count += 1;
            }
            return responses.textResponseWithHeaders(alloc, 200, body, headers[0..count]);
        }
    };
    const input = wire.test_support.fixture();
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = input.owner().metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    var fake: Fake = .{};
    var client = Client.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    try std.testing.expectError(error.Unauthorized, client.fetchGroupSourceScopeControl("http://node:8080", 17, "docs", input, fence, .{}));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    _ = client.withInternalServiceAuth("source-scope-control-client-secret-long-enough", "source-scope-control-test");
    const known = try client.fetchGroupSourceScopeControl("http://node:8080", 17, "docs", input, fence, .{});
    try std.testing.expect(known.confirmed() and known.outcome.? == .rejected);
    fake.mode = .marker;
    const ambiguous = try client.fetchGroupSourceScopeControl("http://node:8080", 17, "docs", input, fence, .{});
    try std.testing.expect(!ambiguous.confirmed() and ambiguous.outcome == null);
    try std.testing.expectEqualDeep(known.command, ambiguous.command);
    try std.testing.expectEqualDeep(known.receipt, ambiguous.receipt);
    for ([_]@TypeOf(fake.mode){ .nonce, .missing_ack, .duplicate_ack, .oversized, .lost, .forged_cut }) |mode| {
        fake.mode = mode;
        const before = fake.calls;
        const unknown = try client.fetchGroupSourceScopeControl("http://node:8080", 17, "docs", input, fence, .{});
        try std.testing.expect(!unknown.confirmed());
        try std.testing.expect(unknown.command == null and unknown.receipt == null);
        try std.testing.expectEqual(before + 1, fake.calls);
    }
    const recovery: wire.Request = .{ .version = 1, .request_id = @splat(10), .operation = .recover, .recovery = .{ .command = known.command.?, .entry = known.receipt.?, .minimum_applied_index = known.receipt.?.index } };
    const unknown_recovery = try client.fetchGroupSourceScopeControl("http://node:8080", 17, "docs", recovery, fence, .{});
    try std.testing.expectEqualDeep(known.command, unknown_recovery.command);
    try std.testing.expectEqualDeep(known.receipt, unknown_recovery.receipt);
    try std.testing.expect(!unknown_recovery.confirmed());
}
