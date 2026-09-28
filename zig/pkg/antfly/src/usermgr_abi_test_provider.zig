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
const auth = @import("usermgr/user_manager.zig");
const casbin = @import("antfly_casbin");
const alloc = std.heap.c_allocator;
const Owner = struct {
    users: auth.MemoryStore,
    policies: casbin.MemoryAdapter,
    manager: auth.UserManager,
};
var failure: c_int = 0;
fn random(_: ?*anyopaque, bytes: []u8) std.Io.RandomSecureError!void {
    switch (failure) {
        1 => return error.Canceled,
        2 => return error.EntropyUnavailable,
        else => @memset(bytes, 42),
    }
}
const vtable: std.Io.VTable = blk: {
    var result = std.Options.debug_io.vtable.*;
    result.randomSecure = random;
    break :blk result;
};
fn create() !*auth.UserManager {
    failure = 0;
    const owner = try alloc.create(Owner);
    errdefer alloc.destroy(owner);
    owner.users = auth.MemoryStore.init(alloc);
    errdefer owner.users.deinit();
    owner.policies = casbin.MemoryAdapter.init(alloc);
    errdefer owner.policies.deinit();
    owner.manager = try auth.UserManager.initWithIo(alloc, .{
        .userdata = std.Options.debug_io.userdata,
        .vtable = &vtable,
    }, owner.users.iface(), try auth.initDefaultEnforcer(alloc, owner.policies.iface()));
    errdefer owner.manager.deinit();
    var user = try owner.manager.createUser("alice", "password", &.{});
    user.deinit(alloc);
    return &owner.manager;
}
export fn usermgr_abi_create() callconv(.c) ?*auth.UserManager {
    return create() catch null;
}
export fn usermgr_abi_fail(value: c_int) callconv(.c) void {
    failure = value;
}
export fn usermgr_abi_destroy(manager: *auth.UserManager) callconv(.c) void {
    const owner: *Owner = @fieldParentPtr("manager", manager);
    owner.manager.deinit();
    owner.policies.deinit();
    owner.users.deinit();
    alloc.destroy(owner);
}
