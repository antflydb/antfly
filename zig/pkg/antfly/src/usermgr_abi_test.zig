// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
