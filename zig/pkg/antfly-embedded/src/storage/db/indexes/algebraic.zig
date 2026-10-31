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

/// Algebraic index state owned by `DB.indexes`: the HyperLogLog maintenance lane owner.
pub const AlgebraicIndexes = struct {
    hll_owner_id: u64 = 0,

    fn dbPtr(self: *AlgebraicIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("algebraic", self));
        return parent.dbPtr();
    }

    pub fn attachHllMaintenanceLane(self: *AlgebraicIndexes) !void {
        const db = self.dbPtr();
        const owner_id = try db.backend_runtime.allocOwnerId();
        self.hll_owner_id = owner_id;
        db.core.index_manager.attachHllMaintenance(db.backend_runtime.durable_jobs, owner_id);
    }
};

test "db open borrows shared backend runtime" {
    const alloc = std.testing.allocator;

    var runtime = try db_mod.background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer runtime.deinit();

    var first_path_tmp = try db_mod.TestDirectory.init("db");
    defer first_path_tmp.cleanup();
    const first_path = first_path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(first_path);

    var second_path_tmp = try db_mod.TestDirectory.init("db");
    defer second_path_tmp.cleanup();
    const second_path = second_path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(second_path);

    var first = try DB.open(alloc, std.mem.span(first_path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
    });
    defer first.close();

    var second = try DB.open(alloc, std.mem.span(second_path), .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
    });
    defer second.close();

    try std.testing.expectEqual(runtime.ptr(), first.backend_runtime);
    try std.testing.expectEqual(runtime.ptr(), second.backend_runtime);
    try std.testing.expect(first.owned_backend_runtime == null);
    try std.testing.expect(second.owned_backend_runtime == null);
    try std.testing.expect(first.backend_owner_id != 0);
    try std.testing.expect(second.backend_owner_id != 0);
    try std.testing.expect(first.backend_owner_id != second.backend_owner_id);
    // Each DB owns a backend lane, a repair/cleanup lane, and an algebraic HLL
    // maintenance lane. Generation retirement uses the runtime-wide cleanup
    // owner instead.
    try std.testing.expectEqual(
        second.indexes.algebraic.hll_owner_id + 1,
        try runtime.ptr().allocOwnerId(),
    );
}
