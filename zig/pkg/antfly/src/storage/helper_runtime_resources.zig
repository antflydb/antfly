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

//! A helper executor and its stable memory authority move together.
const std = @import("std");
const local = @import("antfly_local_sources");
const resources = local.storage_resource_manager;
const runtime = local.storage_background_runtime;

pub const Config = struct {
    /// Zero/null selects the system envelope, or the supplied parent's envelope.
    limit_bytes: ?u64 = null,
    /// Borrowed authority, retained by the embedding host through physical joins.
    parent: ?resources.ResourceManager.MemoryParent = null,
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    handle: runtime.BackendRuntimeHandle,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Owner {
        const limit = if (config.limit_bytes) |value| if (value != 0) value else automaticLimit(config.parent) else automaticLimit(config.parent);
        const manager = try resources.SharedResourceManager.create(allocator, .{
            .memory_budget = .{ .hard_limit_bytes = limit },
            .identity_allocator = allocator,
        });
        errdefer manager.releaseOwned(allocator);
        if (config.parent) |parent| try manager.installMemoryParent(parent);
        const parent = manager.memoryParentPort();
        const handle = try runtime.BackendRuntimeHandle.init(allocator, .{
            .memory_parent = parent,
            .shared_sql_memory_parent = parent,
        });
        return .{ .allocator = allocator, .manager = manager, .handle = handle };
    }

    pub fn ptr(self: *Owner) *runtime.BackendRuntime {
        return self.handle.ptr();
    }

    pub fn deinit(self: *Owner) void {
        self.handle.deinit();
        self.manager.releaseOwned(self.allocator);
        self.* = undefined;
    }
};

fn automaticLimit(parent: ?resources.ResourceManager.MemoryParent) u64 {
    return if (parent != null) 0 else @import("antfly_platform").process_memory.systemEnvelope().limit_bytes;
}
