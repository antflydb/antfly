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

//! Stable private executor. Join before freeing users, release manager last.
const std = @import("std");
const limits = @import("antfly_runtime_fs").threaded_io_limits;
const memory = @import("runtime_memory.zig");
const resources = @import("resource_manager.zig");

pub const Owner = struct {
    allocator: std.mem.Allocator,
    memory: ?*memory.Owner,
    resource_lifetime: ?*resources.SharedResourceManager,
    threaded: std.Io.Threaded,
    joined: bool = false,

    pub fn create(backing: std.mem.Allocator, manager: ?*resources.ResourceManager) !*Owner {
        const lifetime = if (manager) |value| value.retainLifetime() else null;
        errdefer if (lifetime) |value| value.release();
        const funded = if (manager) |value| try memory.Owner.create(backing, value.memoryParentPort()) else null;
        errdefer if (funded) |value| value.destroy();
        const allocator = if (funded) |value| value.allocator() else backing;
        const self = try allocator.create(Owner);
        self.* = .{ .allocator = allocator, .memory = funded, .resource_lifetime = lifetime, .threaded = limits.initOwnedService(allocator, if (funded) |value| value.stackAllocator() else null) };
        return self;
    }
    pub fn io(self: *Owner) std.Io {
        std.debug.assert(!self.joined);
        return self.threaded.io();
    }
    pub fn join(self: *Owner) void {
        if (self.joined) return;
        self.threaded.deinit();
        self.joined = true;
    }
    pub fn destroy(self: *Owner) void {
        self.join();
        const allocator = self.allocator;
        const funded = self.memory;
        const lifetime = self.resource_lifetime;
        allocator.destroy(self);
        if (funded) |value| value.destroy();
        if (lifetime) |value| value.release();
    }
};
