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

//! Point-read exclusion shared by dependency and lifecycle admission. Presence
//! is sufficient to refuse a new writer; it is never proof of child authority.
//! Recovery independently verifies the lease's full body and reverse membership.
const std = @import("std");

pub fn memberKey(buffer: []u8, group: u64, kind: []const u8, id: u64) ![]const u8 {
    if (group == 0 or id == 0) return error.InvalidScopeRetirement;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:scope-retirement-member:{s}:{x:0>16}", .{ group, kind, id });
}

pub fn occupied(reader: anytype, group: u64, kind: []const u8, id: u64) !bool {
    var buffer: [256]u8 = undefined;
    _ = reader.get(try memberKey(&buffer, group, kind, id)) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return true;
}

pub fn requireTableWritable(reader: anytype, group: u64, table: u64) !void {
    if (try occupied(reader, group, "table", table)) return error.GenerationPublicationChanged;
}
