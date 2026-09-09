// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const auth = @import("usermgr/user_manager.zig");
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
