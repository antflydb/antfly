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
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations.

const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const server = @import("http_server.zig");

/// Observe actual owner inventory, not only desired metadata. SQL keyword
/// mappings must not manufacture an implicit full-text artifact behind {}.
pub fn expectNoSearchArtifacts(ctx: *fixture.Context, table_id: u64) !void {
    const source = server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    var snapshot = (try source.adminSnapshot()) orelse return error.MissingOwnerCatalog;
    defer source.freeAdminSnapshot(&snapshot);
    const table = for (snapshot.tables) |table| {
        if (table.table_id == table_id) break table;
    } else return error.MissingOwnerTable;
    const owner = ctx.data.kernel_owner_source orelse return error.MissingStorageOwner;
    var statuses = (try owner.writeSource().localRuntimeStatuses(ctx.alloc, table.name)) orelse return error.MissingOwnerStatus;
    defer statuses.deinit(ctx.alloc);
    try std.testing.expect(statuses.items.len != 0);
    for (statuses.items) |status| {
        // The kernel produces these from runtimeStatusStatsConsistent on the
        // actual DB, not the control facade's desired index configuration.
        try std.testing.expectEqual(@as(usize, 0), status.stats.indexes.len);
    }
}
