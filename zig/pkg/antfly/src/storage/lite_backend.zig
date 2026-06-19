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
const db_mod = @import("db/db.zig");
const lsm_backend = @import("lsm_backend/mod.zig");

const Allocator = std.mem.Allocator;

pub const CheckReport = lsm_backend.AfliteContainerStorage.CheckReport;
pub const VacuumReport = lsm_backend.AfliteContainerStorage.VacuumReport;

pub const EngineKind = enum {
    /// Temporary bridge implementation. The public Lite API should depend on
    /// this module, not directly on the container, so the native backend can
    /// replace it without changing callers.
    bridge_lsm_container,
};

pub const OpenOptions = struct {
    engine: EngineKind = .bridge_lsm_container,
    read_only: bool = false,
};

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    return try lsm_backend.AfliteContainerStorage.checkFile(allocator, path);
}

pub const Handle = struct {
    allocator: Allocator,
    engine: EngineKind,
    bridge_storage: ?*lsm_backend.AfliteContainerStorage = null,

    pub fn open(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
        return switch (opts.engine) {
            .bridge_lsm_container => try openBridgeLsmContainer(allocator, path, opts),
        };
    }

    pub fn deinit(self: *Handle) void {
        switch (self.engine) {
            .bridge_lsm_container => {
                if (self.bridge_storage) |storage| {
                    storage.deinit();
                    self.allocator.destroy(storage);
                    self.bridge_storage = null;
                }
            },
        }
        self.* = undefined;
    }

    pub fn configureDbOpenOptions(self: *Handle, opts: *db_mod.OpenOptions) void {
        switch (self.engine) {
            .bridge_lsm_container => {
                const storage = self.bridge_storage.?.storage();
                opts.storage = storage;
                opts.index_backends.text_lsm_storage = storage;
                opts.index_backends.dense_lsm_storage = storage;
                opts.index_backends.sparse_lsm_storage = storage;
                opts.index_backends.graph_lsm_storage = storage;
                opts.external_derived_checkpoints = false;
            },
        }
    }

    pub fn check(self: *Handle) !CheckReport {
        return switch (self.engine) {
            .bridge_lsm_container => try self.bridge_storage.?.check(),
        };
    }

    pub fn vacuum(self: *Handle) !VacuumReport {
        return switch (self.engine) {
            .bridge_lsm_container => try self.bridge_storage.?.vacuum(),
        };
    }
};

fn openBridgeLsmContainer(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    var storage = try allocator.create(lsm_backend.AfliteContainerStorage);
    errdefer allocator.destroy(storage);

    storage.* = try lsm_backend.AfliteContainerStorage.openWithOptions(allocator, path, .{
        .read_only = opts.read_only,
    });
    errdefer storage.deinit();

    return .{
        .allocator = allocator,
        .engine = .bridge_lsm_container,
        .bridge_storage = storage,
    };
}
