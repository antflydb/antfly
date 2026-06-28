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

const relational_rows = @import("relational_rows.zig");

pub const TestPrimaryResolver = struct {
    row_json: []const u8,
    version: u64,
    exists: bool = true,
    resolved_key: []const u8 = "test-existing-primary",

    pub fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolve,
            .resolve_temporal_overlap = resolveTemporalOverlap,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolve(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8 {
        _ = table_name;
        _ = constraint_name;
        _ = encoded_value;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.exists) return null;
        return try alloc.dupe(u8, self.resolved_key);
    }

    fn resolveTemporalOverlap(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
    ) anyerror!?[]u8 {
        _ = table_name;
        _ = constraint_name;
        _ = encoded_value;
        if (encoded_start.len == 0 or encoded_end.len == 0) return error.TestUnexpectedResult;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.exists) return null;
        return try alloc.dupe(u8, self.resolved_key);
    }

    fn primaryExists(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!bool {
        _ = alloc;
        _ = table_name;
        _ = physical_key;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.exists;
    }

    fn lookupPrimary(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!?relational_rows.ResolvedPrimaryRow {
        _ = table_name;
        if (physical_key.len == 0) return null;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .json = try alloc.dupe(u8, self.row_json),
            .version = self.version,
        };
    }
};
