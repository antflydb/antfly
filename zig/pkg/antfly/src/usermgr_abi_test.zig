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

const std = @import("std");
const auth = @import("antfly_local_sources").usermgr_user_manager;
extern fn usermgr_abi_create() callconv(.c) ?*auth.UserManager;
extern fn usermgr_abi_fail(c_int) callconv(.c) void;
extern fn usermgr_abi_destroy(*auth.UserManager) callconv(.c) void;

test "usermgr archive boundary preserves secure randomness errors and releases mutation leases" {
    const manager = usermgr_abi_create() orelse return error.TestSetupFailed;
    defer usermgr_abi_destroy(manager);
    for ([_]anyerror{ error.Canceled, error.EntropyUnavailable }, 1..) |expected, failure| {
        usermgr_abi_fail(@intCast(failure));
        try std.testing.expectError(expected, manager.createUser("bob", "password", &.{}));
        try std.testing.expectError(expected, manager.updatePassword("alice", "changed"));
        try std.testing.expectError(expected, manager.createApiKey("alice", "test", &.{}, &.{}, null));
        try std.testing.expectEqual(@as(usize, 1), manager.users.count());
        try std.testing.expectEqual(@as(usize, 0), manager.api_keys.count());
        var lease = manager.acquireSeedCaptureLease();
        lease.release();
    }
    // A failed password update cannot replace the old hash.
    var user = try manager.authenticateUser("alice", "password");
    defer user.deinit(manager.alloc);
    usermgr_abi_fail(0);
    var key = try manager.createApiKey("alice", "test", &.{}, &.{}, null);
    defer key.deinit(manager.alloc);
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

test "usermgr archive boundary preserves replication grants and current revocation" {
    const manager = usermgr_abi_create() orelse return error.TestSetupFailed;
    defer usermgr_abi_destroy(manager);
    var permission = try auth.Permission.initOwned(manager.alloc, .table, "docs", .replicate);
    defer permission.deinit(manager.alloc);
    try manager.addPermissionToUser("alice", permission);
    try std.testing.expect(try manager.permissionCurrentlyAllowed("alice", .table, "docs", .replicate));
    try std.testing.expect(!try manager.permissionCurrentlyAllowed("alice", .table, "docs", .read));
    var key = try manager.createApiKey("alice", "replication", &.{permission}, &.{}, null);
    defer key.deinit(manager.alloc);
    var validated = try manager.validateApiKey(key.key.key_id, key.key_secret);
    defer validated.deinit(manager.alloc);
    try std.testing.expectEqual(auth.PermissionType.replicate, validated.permissions[0].type);
    try manager.removePermissionFromUser("alice", "docs", .table);
    var revoked = try manager.validateApiKey(key.key.key_id, key.key_secret);
    defer revoked.deinit(manager.alloc);
    try std.testing.expectEqual(@as(usize, 0), revoked.permissions.len);
}

test "usermgr archive boundary fences credential and permission checks by user incarnation" {
    const manager = usermgr_abi_create() orelse return error.TestSetupFailed;
    defer usermgr_abi_destroy(manager);
    var user = try manager.getUser("alice");
    defer user.deinit(manager.alloc);
    const legacy = try manager.destinationGrantMac("basic:alice", "replication-owner");
    try std.testing.expectEqual(legacy, try manager.destinationGrantMacForUserInstance("basic:alice", "replication-owner", user.instance_id));
    try std.testing.expectError(error.UserNotFound, manager.destinationGrantMacForUserInstance("basic:alice", "replication-owner", @splat(0)));
    try std.testing.expect(try manager.permissionsCurrentlyAllowedWithUserInstance("alice", "basic:alice", &.{}, user.instance_id));
    try manager.deleteUser("alice");
    var replacement = try manager.createUser("alice", "password", &.{});
    defer replacement.deinit(manager.alloc);
    try std.testing.expectError(error.UserNotFound, manager.destinationGrantMacForUserInstance("basic:alice", "replication-owner", user.instance_id));
    try std.testing.expect(!try manager.permissionsCurrentlyAllowedWithUserInstance("alice", "basic:alice", &.{}, user.instance_id));
    try std.testing.expect(try manager.permissionsCurrentlyAllowedWithUserInstance("alice", "basic:alice", &.{}, replacement.instance_id));
}
