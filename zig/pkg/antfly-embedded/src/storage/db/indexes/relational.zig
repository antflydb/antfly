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
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;
const Indexes = @import("mod.zig").Indexes;

/// Relational column index state owned by `DB.indexes`: column maintenance and the rewrite program and tail caches.
pub const RelationalIndexes = struct {
    rewrite_program_cache: @import("../../rewrite_program_cache.zig").Cache = .{},
    rewrite_tail_cache: @import("../../rewrite_tail_spool.zig").Cache = .{},
    columns_building: std.atomic.Value(bool) = .init(false),
    columns_rebuild_requested: std.atomic.Value(bool) = .init(false),
    column_maintenance: db_mod.relational_columns.Maintenance = .{},

    fn dbPtr(self: *RelationalIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("relational", self));
        return parent.dbPtr();
    }


    pub fn runColumnMaintenancePass(self: *RelationalIndexes) !usize {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("runRelationalColumnMaintenancePass", .{db});
        const started = db.independentMaintenanceNowNs();
        // Artifact repair can keep the shared worker on its active cadence;
        // enforce columnar backoff independently of that worker's sleep.
        if (started < self.column_maintenance.retry_after_ns.load(.acquire)) return 0;
        var completed: usize = 0;
        while (completed < 8) : (completed += 1) {
            const changed = self.rebuildColumnsWithPolicy(true) catch |err| {
                // Resource pressure backs off to the idle cadence; it must not
                // create a 100 ms retry storm or masquerade as a clean table.
                self.column_maintenance.notePending(true);
                self.column_maintenance.backing_off.store(true, .release);
                self.column_maintenance.retry_after_ns.store(db.independentMaintenanceNowNs() +| DB.artifact_repair_metadata_poll_ns, .release);
                return err;
            };
            if (!changed) break;
            if (db.independentMaintenanceNowNs() -| started >= 50 * std.time.ns_per_ms) return completed + 1;
        }
        return completed;
    }

    /// Build disposable table-owned column blocks without holding the apply
    /// lock. The store's atomic primary-mutation fence governs publication.
    pub fn rebuildColumns(self: *RelationalIndexes) !bool {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("rebuildRelationalColumns", .{db});
        return self.rebuildColumnsWithPolicy(false);
    }

    fn rebuildColumnsWithPolicy(self: *RelationalIndexes, adaptive: bool) !bool {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return false;
        if (db.independent_maintenance.stopping.load(.acquire)) return error.Canceled;
        var view = db.core.acquireSchemaView() orelse return false;
        defer view.release();
        if (view.storageMode() != .relational) return false;
        if (self.columns_building.swap(true, .acq_rel)) return false;
        defer self.columns_building.store(false, .release);
        const started = db.independentMaintenanceNowNs();
        _ = self.column_maintenance.passes.fetchAdd(1, .monotonic);
        defer self.column_maintenance.last_pass_ns.store(db.independentMaintenanceNowNs() -| started, .monotonic);
        errdefer _ = self.column_maintenance.failures.fetchAdd(1, .monotonic);
        var preparation: db_mod.RequestPreparationContext = undefined;
        preparation.init(db);
        defer preparation.deinit();
        const force = self.columns_rebuild_requested.swap(false, .acq_rel);
        errdefer if (force) self.columns_rebuild_requested.store(true, .release);
        const changed = db_mod.relational_columns.rebuild(db, preparation.guard.allocator(), force, adaptive) catch |err| {
            switch (err) {
                error.InvalidColumnSegment, error.InvalidData, error.NotFound => self.columns_rebuild_requested.store(true, .release),
                else => {},
            }
            return preparation.mapError(err);
        };
        self.column_maintenance.notePending(changed or self.column_maintenance.waiting_until_ns.load(.acquire) != 0);
        self.column_maintenance.backing_off.store(false, .release);
        self.column_maintenance.retry_after_ns.store(0, .release);
        return changed;
    }
};
