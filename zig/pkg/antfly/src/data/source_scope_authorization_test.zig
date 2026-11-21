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

//! Remote-authorizer transport qualification, not a fabricated metadata proof.
//! Real metadata Raft/ReadIndex capture is covered by the actual host fixture.
const std = @import("std");
const http = @import("runtime_root.zig").common.http;
const scopes = @import("../api/source_scope_metadata.zig");
const control = @import("antfly_local_sources").storage_source_scope_control;
pub fn intent() control.Intent {
    const owner: @FieldType(control.Intent, "owner") = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 601, .topology_epoch = 1 };
    return .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 7, .source_id = @splat(3), .generation = 1, .binding = @splat(4), .checkpoint_owner = control.scope.Basis.fromOwner(owner) catch unreachable }, .minimum_metadata_index = 10, .action = .bind };
}
pub const Fake = struct {
    mode: enum { active, closing, corrupt } = .active,
    next_cut: u64 = 10,
    missing_calls: usize = 0,
    capture_calls: usize = 0,
    pub fn execute(raw: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try std.testing.expect(request.header(@import("../api/internal_service_auth.zig").header_name) != null);
        if (std.mem.startsWith(u8, request.uri, "http://missing")) {
            self.missing_calls += 1;
            return .{ .status = 501, .body = try a.dupe(u8, "unsupported") };
        }
        self.capture_calls += 1;
        const input = try scopes.wire.decodeRequest(a, request.body);
        const spec = intent().spec;
        var reply: scopes.wire.Reply = .{ .version = 1, .request_digest = input.digest(), .snapshot = .{ .cut = .{ .group_id = 1, .incarnation = intent().owner.metadata_incarnation, .index = self.next_cut }, .spec = spec, .phase = if (self.mode == .closing) .closing else .active, .actor_revision = 1, .live_definition = true, .checkpoint_table = "checkpoint" } };
        self.next_cut += 1;
        if (self.mode == .corrupt) reply.request_digest[0] ^= 1;
        const body = try std.json.Stringify.valueAlloc(a, reply, .{});
        errdefer a.free(body);
        const headers = try a.alloc(http.Header, 2);
        errdefer a.free(headers);
        headers[0] = .{ .name = try a.dupe(u8, "Content-Type"), .value = try a.dupe(u8, "application/json") };
        headers[1] = .{ .name = try a.dupe(u8, scopes.ack_header), .value = try a.dupe(u8, scopes.ack_value) };
        return .{ .status = 200, .body = body, .headers = headers };
    }
};
pub fn run(a: std.mem.Allocator, remote: anytype, fake: *Fake) !void {
    const request = intent();
    const first = try remote.authorizeSourceScopeIntent(a, request, .{});
    const second = try remote.authorizeSourceScopeIntent(a, request, .{});
    try std.testing.expectEqual(@as(u64, 10), first.metadata_cut.index);
    try std.testing.expectEqual(@as(u64, 11), second.metadata_cut.index);
    try std.testing.expectEqualDeep(request, second.intent);
    try std.testing.expectEqual(@as(usize, 2), fake.capture_calls);
    try std.testing.expect(fake.missing_calls >= 2);
    fake.mode = .closing;
    try std.testing.expectError(error.SourceBindingConflict, remote.authorizeSourceScopeIntent(a, request, .{}));
    fake.mode = .corrupt;
    try std.testing.expectError(error.SourceControlCorrupt, remote.authorizeSourceScopeIntent(a, request, .{}));
    const before = fake.capture_calls + fake.missing_calls;
    var cancelled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, remote.authorizeSourceScopeIntent(a, request, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) }));
    try std.testing.expectEqual(before, fake.capture_calls + fake.missing_calls);
}
