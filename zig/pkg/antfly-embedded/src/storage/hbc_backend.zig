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
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const lsm_backend = @import("lsm_backend.zig");
const posting_segment_store = @import("posting_segment_store.zig");
pub const OpenedBackend = union(enum) {
    lsm: lsm_backend.BackendHandle,
    native: *NativeBackend,

    pub fn close(self: *OpenedBackend, alloc: Allocator) void {
        switch (self.*) {
            .lsm => |*handle| handle.close(),
            .native => |backend| backend.close(alloc),
        }
        self.* = undefined;
    }

    pub fn abandonAfterCrash(self: *OpenedBackend, alloc: Allocator) void {
        switch (self.*) {
            .lsm => |*handle| handle.abandonAfterCrash(),
            .native => |backend| backend.close(alloc),
        }
        self.* = undefined;
    }

    pub fn sync(self: *OpenedBackend, force: bool) !void {
        switch (self.*) {
            .lsm => |*handle| try handle.backend.sync(force),
            // HBC WAL appends and CURRENT replacement sync their own files and
            // parent namespace at the publication boundary.
            .native => {},
        }
    }

    pub fn syncReplayState(self: *OpenedBackend) !void {
        switch (self.*) {
            .lsm => |*handle| try handle.backend.syncReplayState(),
            .native => {},
        }
    }

    pub fn runtimeNamespaceStore(self: OpenedBackend, allocator: Allocator) !backend_erased.NamespaceStore {
        return switch (self) {
            .lsm => |handle| try handle.backend.runtimeNamespaceStore(allocator),
            .native => return error.HbcNativeGenerationRequired,
        };
    }
};

pub const NativeBackend = struct {
    read_only: bool = false,
    storage: lsm_backend.Storage,
    storage_lease: ?lsm_backend.NativeStorageLease,
    storage_owner: ?*lsm_backend.NativeStorage,
    root_dir: []u8,

    pub fn openIfAuthoritative(alloc: Allocator, root_dir: []const u8, options: LsmOptions) !?*NativeBackend {
        const owned_root = try alloc.dupe(u8, root_dir);
        errdefer alloc.free(owned_root);
        var storage_owner: ?*lsm_backend.NativeStorage = null;
        errdefer if (storage_owner) |owner| {
            owner.deinit();
            alloc.destroy(owner);
        };
        const storage = options.storage orelse blk: {
            const owner = try alloc.create(lsm_backend.NativeStorage);
            errdefer alloc.destroy(owner);
            owner.* = try lsm_backend.NativeStorage.initWithPool(
                alloc,
                options.backend_options.io_runtime,
                options.backend_options.native_storage_pool,
            );
            storage_owner = owner;
            break :blk owner.storage();
        };
        const authority_path = try std.fs.path.join(alloc, &.{ root_dir, "posting-segments", posting_segment_store.authority_name });
        defer alloc.free(authority_path);
        const authority = storage.readFileAlloc(alloc, authority_path, posting_segment_store.authority_value.len + 1) catch |err| switch (err) {
            error.FileNotFound => {
                if (storage_owner) |owner| {
                    owner.deinit();
                    alloc.destroy(owner);
                    storage_owner = null;
                }
                alloc.free(owned_root);
                return null;
            },
            else => return err,
        };
        defer alloc.free(authority);
        if (!std.mem.eql(u8, authority, posting_segment_store.authority_value)) return error.InvalidHbcNativeAuthority;

        const native = try alloc.create(NativeBackend);
        native.* = .{
            .read_only = options.backend_options.backend.read_only,
            .storage = storage,
            .storage_lease = null,
            .storage_owner = storage_owner,
            .root_dir = owned_root,
        };
        storage_owner = null;
        return native;
    }

    pub fn detachFromLsm(alloc: Allocator, handle: *lsm_backend.BackendHandle) !*NativeBackend {
        const backend = handle.backend;
        const root = try alloc.dupe(u8, backend.root_dir orelse return error.MissingStorageRoot);
        errdefer alloc.free(root);
        var lease = try backend.acquireNativeStorageLease();
        errdefer if (lease) |*owned| owned.deinit();
        const storage = if (lease) |*owned| owned.storage() else backend.storage orelse return error.MissingStorageBackend;
        const native = try alloc.create(NativeBackend);
        errdefer alloc.destroy(native);
        native.* = .{
            .storage = storage,
            .storage_lease = lease,
            .storage_owner = null,
            .root_dir = root,
        };
        lease = null;
        return native;
    }

    pub fn close(self: *NativeBackend, alloc: Allocator) void {
        if (self.storage_lease) |*lease| lease.deinit();
        if (self.storage_owner) |owner| {
            owner.deinit();
            alloc.destroy(owner);
        }
        alloc.free(self.root_dir);
        alloc.destroy(self);
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
        .lsm => blk: {
            if (try NativeBackend.openIfAuthoritative(alloc, std.mem.span(path), lsm_options)) |native| {
                break :blk .{ .native = native };
            }
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
