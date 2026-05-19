// Copyright 2026 Antfly, Inc.
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
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const lsm_backend = @import("lsm_backend.zig");
const supports_native_lmdb_backend = builtin.os.tag != .freestanding;
const lmdb_backend = if (supports_native_lmdb_backend) @import("lmdb_backend.zig") else struct {
    pub const Backend = struct {
        pub fn close(_: *@This()) void {}

        pub fn sync(_: *@This(), _: bool) !void {
            return error.UnsupportedPlatform;
        }

        pub fn runtimeNamespaceStore(_: *@This(), _: Allocator) !backend_erased.NamespaceStore {
            return error.UnsupportedPlatform;
        }
    };
};

pub const OpenedBackend = union(enum) {
    lmdb: *lmdb_backend.Backend,
    lsm: lsm_backend.BackendHandle,

    pub fn close(self: *OpenedBackend, alloc: Allocator) void {
        switch (self.*) {
            .lmdb => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.close(),
        }
        self.* = undefined;
    }

    pub fn sync(self: *OpenedBackend, force: bool) !void {
        switch (self.*) {
            .lmdb => |backend| try backend.sync(force),
            .lsm => |*handle| try handle.backend.sync(force),
        }
    }

    pub fn syncReplayState(self: *OpenedBackend) !void {
        switch (self.*) {
            .lmdb => |backend| try backend.sync(false),
            .lsm => |*handle| try handle.backend.syncReplayState(),
        }
    }

    pub fn runtimeNamespaceStore(self: OpenedBackend, allocator: Allocator) !backend_erased.NamespaceStore {
        return switch (self) {
            .lmdb => |backend| try backend.runtimeNamespaceStore(allocator),
            .lsm => |handle| try handle.backend.runtimeNamespaceStore(allocator),
        };
    }
};

pub fn openBackend(alloc: Allocator, path: [*:0]const u8, config: anytype) !OpenedBackend {
    return try openBackendWithStorage(alloc, path, config, null);
}

pub fn openBackendWithStorage(alloc: Allocator, path: [*:0]const u8, config: anytype, lsm_storage: ?lsm_backend.Storage) !OpenedBackend {
    return try openBackendWithLsmOptions(alloc, path, config, .{ .storage = lsm_storage });
}

pub const LsmOptions = struct {
    backend_options: lsm_backend.Options = .{},
    storage: ?lsm_backend.Storage = null,
    cache: ?*lsm_backend.Cache = null,
    root_generation: u64 = 0,
};

pub fn openBackendWithLsmOptions(alloc: Allocator, path: [*:0]const u8, config: anytype, lsm_options: LsmOptions) !OpenedBackend {
    return switch (config.storage_backend) {
        .lmdb => blk: {
            if (!supports_native_lmdb_backend) return error.UnsupportedPlatform;
            const backend = try alloc.create(lmdb_backend.Backend);
            errdefer alloc.destroy(backend);
            backend.* = try lmdb_backend.Backend.open(alloc, path, .{
                .env = .{
                    .max_dbs = 5,
                    .map_size = config.map_size,
                    .no_sync = config.no_sync,
                    .no_meta_sync = config.no_meta_sync,
                    .no_tls = true,
                    .defer_page_mutation = config.defer_page_mutation,
                },
            });
            errdefer backend.close();
            break :blk .{ .lmdb = backend };
        },
        .lsm => blk: {
            var backend_options = lsm_options.backend_options;
            backend_options.backend.durability = if (config.no_sync) .none else backend_options.backend.durability;
            backend_options.storage = lsm_options.storage orelse backend_options.storage;
            backend_options.cache = lsm_options.cache orelse backend_options.cache;
            if (lsm_options.root_generation != 0 and backend_options.root_generation == 0) {
                backend_options.root_generation = lsm_options.root_generation;
            }
            var handle = try lsm_backend.BackendHandle.open(alloc, std.mem.span(path), backend_options);
            errdefer handle.close();
            break :blk .{ .lsm = handle };
        },
    };
}
