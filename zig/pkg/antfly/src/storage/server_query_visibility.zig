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

//! Stable server routing for local visibility observations. The DB callback
//! detachment barrier owns the lifetime of the binding and its borrowed route.
const std = @import("std");
const db = @import("antfly_source_root").antfly_sources.selected_db;
const visibility = @import("antfly_local_sources").storage_db_query_visibility;
pub const Binding = struct {
    pub const Route = struct {
        ptr: *anyopaque,
        table_name: []const u8,
        group_id: u64,
        owner: ?*db.DB,
        notify: *const fn (*anyopaque, []const u8, u64, ?*db.DB, visibility.QueryVisibilityEvent) void,
    };
    mutex: std.atomic.Mutex = .unlocked,
    route: ?Route = null,
    fn lock(self: *Binding) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn bind(self: *Binding, route: Route) visibility.QueryVisibilityHook {
        self.lock();
        self.route = route;
        self.mutex.unlock();
        return .{ .ptr = self, .on_change = changed };
    }
    fn changed(ptr: *anyopaque, event: visibility.QueryVisibilityEvent) void {
        const self: *Binding = @ptrCast(@alignCast(ptr));
        const route = blk: {
            self.lock();
            defer self.mutex.unlock();
            break :blk self.route orelse return;
        };
        route.notify(route.ptr, route.table_name, route.group_id, route.owner, event);
    }
};

test "server visibility binding attaches routing without holding its lock across observation" {
    const Capture = struct {
        binding: *Binding,
        calls: usize = 0,
        fn changed(ptr: *anyopaque, table_name: []const u8, group_id: u64, _: ?*db.DB, event: visibility.QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.binding.mutex.tryLock());
            self.binding.mutex.unlock();
            std.debug.assert(std.mem.eql(u8, table_name, "docs") and group_id == 17 and event.change == .status);
            self.calls += 1;
        }
    };
    var binding: Binding = .{};
    var capture: Capture = .{ .binding = &binding };
    const hook = binding.bind(.{ .ptr = &capture, .table_name = "docs", .group_id = 17, .owner = null, .notify = Capture.changed });
    hook.notify(.{ .change = .status });
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
}
