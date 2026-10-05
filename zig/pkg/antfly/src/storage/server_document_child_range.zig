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

const std = @import("std");
const types = @import("antfly_local_sources").storage_db_types;

/// Server placement policy for a committed manifest. Live routing validation
/// remains in the delivery callback, after the local apply fence is released.
pub fn selectPersistedDestination(_: *anyopaque, range: types.DocumentArtifactChildRange) ?u64 {
    if (!std.mem.eql(u8, range.route_status orelse "local_committed", "remote_committed")) return null;
    const owner = range.owner_group_id orelse return null;
    return if (owner == 0) null else owner;
}

test "server child range selection requires a committed nonzero remote destination" {
    var range: types.DocumentArtifactChildRange = .{
        .range_id = @constCast("r"),
        .range_kind = @constCast("unit"),
        .artifact_name = @constCast("units"),
        .split_boundary = @constCast("unit"),
        .placement = @constCast("remote"),
        .start_key = @constCast("a"),
        .end_key_exclusive = @constCast("z"),
        .last_key = @constCast("a"),
        .owner_group_id = 7,
    };
    try std.testing.expect(selectPersistedDestination(&range, range) == null);
    range.route_status = @constCast("remote_pending");
    try std.testing.expect(selectPersistedDestination(&range, range) == null);
    range.route_status = @constCast("remote_committed");
    try std.testing.expectEqual(@as(?u64, 7), selectPersistedDestination(&range, range));
    range.owner_group_id = 0;
    try std.testing.expect(selectPersistedDestination(&range, range) == null);
}
