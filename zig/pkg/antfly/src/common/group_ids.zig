// Copyright 2026 Antfly, Inc.
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

pub const system_group_bit: u64 = @as(u64, 1) << 63;
pub const data_group_mask: u64 = system_group_bit - 1;

pub const main_metadata_group_id: u64 = system_group_bit | 1;

pub fn isSystemGroupId(group_id: u64) bool {
    return (group_id & system_group_bit) != 0;
}

pub fn isDataGroupId(group_id: u64) bool {
    return group_id != 0 and !isSystemGroupId(group_id);
}

pub fn dataGroupIdFromHash(raw: u64) u64 {
    const group_id = raw & data_group_mask;
    return if (group_id == 0) 1 else group_id;
}

pub fn requireDataGroupId(group_id: u64) !void {
    if (!isDataGroupId(group_id)) return error.ReservedGroupId;
}

test "group id namespaces are disjoint" {
    try std.testing.expect(isSystemGroupId(main_metadata_group_id));
    try std.testing.expect(!isDataGroupId(main_metadata_group_id));
    try std.testing.expectEqual(@as(u64, 1), dataGroupIdFromHash(system_group_bit | 1));
}
