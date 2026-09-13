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
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const lsm_backend = @import("lsm_backend.zig");
const posting_segment_store = @import("posting_segment_store.zig");
const supports_native_lmdb_backend = builtin.os.tag != .freestanding and build_options.lmdb_enabled;
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
    native: *NativeBackend,

    pub fn close(self: *OpenedBackend, alloc: Allocator) void {
        switch (self.*) {
            .lmdb => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.close(),
            .native => |backend| backend.close(alloc),
        }
        self.* = undefined;
    }

    pub fn abandonAfterCrash(self: *OpenedBackend, alloc: Allocator) void {
        switch (self.*) {
            // LMDB has no modeled unclean-close hook. Closing releases process
            // resources without adding an HBC publication transition.
            .lmdb => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.abandonAfterCrash(),
            .native => |backend| backend.close(alloc),
        }
        self.* = undefined;
    }

    pub fn sync(self: *OpenedBackend, force: bool) !void {
        switch (self.*) {
            .lmdb => |backend| try backend.sync(force),
            .lsm => |*handle| try handle.backend.sync(force),
            // HBC WAL appends and CURRENT replacement sync their own files and
            // parent namespace at the publication boundary.
            .native => {},
        }
    }

    pub fn syncReplayState(self: *OpenedBackend) !void {
        switch (self.*) {
            .lmdb => |backend| try backend.sync(false),
            .lsm => |*handle| try handle.backend.syncReplayState(),
            .native => {},
        }
    }

    pub fn runtimeNamespaceStore(self: OpenedBackend, allocator: Allocator) !backend_erased.NamespaceStore {
        return switch (self) {
            .lmdb => |backend| try backend.runtimeNamespaceStore(allocator),
            .lsm => |handle| try handle.backend.runtimeNamespaceStore(allocator),
            .native => return error.HbcNativeGenerationRequired,
        };
    }
};

pub const NativeBackend = struct {
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
