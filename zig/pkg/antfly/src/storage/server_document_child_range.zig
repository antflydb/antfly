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
