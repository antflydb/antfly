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

//! Recovery observes one request-owned evaluation; it never recreates a producer.
const std = @import("std");
const population = @import("antfly_local_sources").storage_db_relational_population_contract;
const owners = @import("restore_owner.zig");

pub fn requireSealed(capture: population.Capture, group: u64, result: owners.Response, now_ms: u64) !void {
    try capture.validate();
    if (result.phase == .reserved or result.phase == .importing) {
        if (now_ms >= capture.expires_at_ms) return error.SourceSnapshotIncomplete;
        return error.RestoreStagingWait;
    }
    const cursor = result.population orelse return error.RestoreStagingScopeChanged;
    cursor.validate() catch return error.RestoreStagingScopeChanged;
    const binding = capture.forPartition(group);
    if (result.phase != .imported or !cursor.sealed or result.rows != cursor.rows or
        !std.mem.eql(u8, &cursor.binding, &(try binding.digest()))) return error.RestoreStagingScopeChanged;
}

test "restore owner SQL population recovery requires sealed scoped output and expires only incomplete captures" {
    const capture: population.Capture = .{ .capture_id = @splat(1), .definition = @splat(2), .source_definition = @splat(3), .target_schema = @splat(4), .principal_instance = @splat(5), .catalog_revision = 6, .write_version = 7, .expires_at_ms = 100 };
    for ([_]@import("antfly_local_sources").storage_db_restore_staging.Phase{ .reserved, .importing }) |phase| {
        const response: owners.Response = .{ .phase = phase, .rows = 0, .receipt = @splat(1) };
        try std.testing.expectError(error.RestoreStagingWait, requireSealed(capture, 8, response, 99));
        try std.testing.expectError(error.SourceSnapshotIncomplete, requireSealed(capture, 8, response, 100));
    }
    var cursor = try population.Cursor.init(capture.forPartition(8));
    const unsealed: owners.Response = .{ .phase = .imported, .rows = 0, .receipt = @splat(1), .population = cursor };
    try std.testing.expectError(error.RestoreStagingScopeChanged, requireSealed(capture, 8, unsealed, 99));
    cursor = try cursor.seal(cursor.completionForSource(0));
    var sealed: owners.Response = .{ .phase = .imported, .rows = 0, .receipt = @splat(1), .population = cursor };
    try requireSealed(capture, 8, sealed, 1000);
    try std.testing.expectError(error.RestoreStagingScopeChanged, requireSealed(capture, 9, sealed, 99));
    sealed.rows = 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, requireSealed(capture, 8, sealed, 99));
}
