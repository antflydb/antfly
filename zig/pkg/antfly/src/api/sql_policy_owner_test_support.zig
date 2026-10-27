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
    // Runtime inventory is deliberately best-effort/resident-only. A busy
    // owner or a control projection refresh can yield no observation even
    // after a successful SQL read. Wait for a real sample, never treat absence
    // (or desired configuration) as proof that no artifacts were created.
    const deadline = std.Io.Clock.now(.awake, ctx.io).nanoseconds + 5 * std.time.ns_per_s;
    var statuses = observed: {
        while (std.Io.Clock.now(.awake, ctx.io).nanoseconds < deadline) {
            try ctx.checkDrivers();
            if (try owner.writeSource().localRuntimeStatuses(ctx.alloc, table.name)) |value| break :observed value;
            try ctx.io.sleep(.fromMilliseconds(10), .awake);
        }
        return error.MissingOwnerStatus;
    };
    defer statuses.deinit(ctx.alloc);
    try std.testing.expect(statuses.items.len != 0);
    for (statuses.items) |status| {
        try std.testing.expect(status.metadata.source != .synthetic_config);
        // The kernel produces these from runtimeStatusStatsConsistent on the
        // actual DB, not the control facade's desired index configuration.
        try std.testing.expectEqual(@as(usize, 0), status.stats.indexes.len);
    }
}
