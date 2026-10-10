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
const extension_domain = @import("mod.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const json_helpers = @import("antfly_local_sources").api_json_helpers;

pub fn memberTableName(member: extension_domain.ExtensionMember) ?[]const u8 {
    if (member.table_name.len != 0) return member.table_name;
    if (member.scope.kind == .table) return member.scope.table_name;
    return null;
}

pub fn ownsIndex(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8, index_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        if (member.object_kind != .index) continue;
        const member_table = memberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name) and std.mem.eql(u8, member.object_name, index_name)) return true;
    }
    return false;
}

pub fn ownsEnrichment(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8, enrichment_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        if (member.object_kind != .enrichment) continue;
        const member_table = memberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name) and std.mem.eql(u8, member.object_name, enrichment_name)) return true;
    }
    return false;
}

pub fn ownsTableShape(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        const member_table = memberTableName(member) orelse continue;
        if (!std.mem.eql(u8, member_table, table_name)) continue;
        if (member.object_kind == .table_schema) return true;
        if (member.object_kind != .data_shape) continue;
        const shape_kind = member.shape_kind orelse continue;
        if (shape_kind == .document or shape_kind == .row) return true;
    }
    return false;
}

fn collectEnrichmentByName(root: std.json.Value, name: []const u8, found: *?std.json.Value) !void {
    switch (root) {
        .object => |object| {
            if (object.get("enrichments")) |enrichments| {
                if (enrichments != .array) return error.InvalidTableIndexMetadata;
                for (enrichments.array.items) |enrichment| {
                    if (enrichment != .object) return error.InvalidTableIndexMetadata;
                    const value_name = enrichment.object.get("name") orelse continue;
                    if (value_name != .string or !std.mem.eql(u8, value_name.string, name)) continue;
                    if (found.*) |previous| {
                        if (!json_helpers.jsonValuesEqual(previous, enrichment))
                            return error.InvalidTableIndexMetadata;
                    } else {
                        found.* = enrichment;
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try collectEnrichmentByName(entry.value_ptr.*, name, found);
            }
        },
        .array => |array| for (array.items) |item| try collectEnrichmentByName(item, name, found),
        else => {},
    }
}

fn enrichmentByName(root: std.json.Value, name: []const u8) !?std.json.Value {
    var found: ?std.json.Value = null;
    try collectEnrichmentByName(root, name, &found);
    return found;
}

fn optionalJsonValuesEqual(lhs: ?std.json.Value, rhs: ?std.json.Value) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return json_helpers.jsonValuesEqual(lhs.?, rhs.?);
}

/// Returns whether an exact table-definition replacement changes state owned
/// by an extension. Index-only replacements are intentionally permitted on a
/// table whose schema or document shape is extension-owned, provided every
/// extension-owned index and enrichment remains semantically unchanged.
pub fn definitionMutationTouchesOwnedState(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    expected: metadata_table_manager.TableRecord,
    replacement: metadata_table_manager.TableRecord,
) !bool {
    var replacement_without_indexes = replacement;
    replacement_without_indexes.indexes_json = expected.indexes_json;
    if (!metadata_table_manager.tableDefinitionsEqual(expected, replacement_without_indexes) and
        ownsTableShape(snapshot, replacement.name))
    {
        return true;
    }
    if (std.mem.eql(u8, expected.indexes_json, replacement.indexes_json)) return false;

    var guard = try DefinitionMutationGuard.init(alloc, expected, replacement);
    defer guard.deinit();
    for (snapshot.extension_members) |member| if (try guard.touches(member)) return true;
    return false;
}

/// Streaming equivalent of snapshot ownership admission. Index metadata is
/// parsed once; the caller may visit only this table's durable owner index.
pub const DefinitionMutationGuard = struct {
    table_name: []const u8,
    shape_changed: bool,
    expected_indexes: ?std.json.Parsed(std.json.Value) = null,
    replacement_indexes: ?std.json.Parsed(std.json.Value) = null,

    pub fn init(alloc: std.mem.Allocator, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) !@This() {
        var without_indexes = replacement;
        without_indexes.indexes_json = expected.indexes_json;
        var result: @This() = .{ .table_name = replacement.name, .shape_changed = !metadata_table_manager.tableDefinitionsEqual(expected, without_indexes) };
        errdefer result.deinit();
        if (!std.mem.eql(u8, expected.indexes_json, replacement.indexes_json)) {
            result.expected_indexes = std.json.parseFromSlice(std.json.Value, alloc, expected.indexes_json, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidTableIndexMetadata;
            result.replacement_indexes = std.json.parseFromSlice(std.json.Value, alloc, replacement.indexes_json, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidTableIndexMetadata;
            if (result.expected_indexes.?.value != .object or result.replacement_indexes.?.value != .object) return error.InvalidTableIndexMetadata;
        }
        return result;
    }

    pub fn deinit(self: *@This()) void {
        if (self.expected_indexes) |*parsed| parsed.deinit();
        if (self.replacement_indexes) |*parsed| parsed.deinit();
        self.* = undefined;
    }

    pub fn touches(self: *const @This(), member: extension_domain.ExtensionMember) !bool {
        const name = memberTableName(member) orelse return false;
        if (!std.mem.eql(u8, name, self.table_name)) return false;
        if (self.shape_changed) switch (member.object_kind) {
            .table_schema => return true,
            .data_shape => if (member.shape_kind) |kind| if (kind == .document or kind == .row) return true,
            else => {},
        };
        const before = if (self.expected_indexes) |parsed| parsed.value else return false;
        const after = self.replacement_indexes.?.value;
        return switch (member.object_kind) {
            .index => !optionalJsonValuesEqual(before.object.get(member.object_name), after.object.get(member.object_name)),
            .enrichment => !optionalJsonValuesEqual(try enrichmentByName(before, member.object_name), try enrichmentByName(after, member.object_name)),
            else => false,
        };
    }
};

test "relation mutation ownership streams semantic index checks and preserves shape ownership" {
    const Fixture = struct {
        const before: metadata_table_manager.TableRecord = .{ .table_id = 7, .name = "rows", .indexes_json = "{\"owned\":{\"type\":\"text\",\"enabled\":true},\"other\":{\"type\":\"text\"}}" };
        const after: metadata_table_manager.TableRecord = .{ .table_id = 7, .name = "rows", .indexes_json = "{\"other\":{\"type\":\"text\",\"enabled\":false},\"owned\":{\"enabled\":true,\"type\":\"text\"}}" };
        const member: extension_domain.ExtensionMember = .{ .extension_name = "owner", .scope = .{ .kind = .table, .table_name = "rows" }, .table_name = "rows", .object_kind = .index, .object_name = "owned" };
        fn run(alloc: std.mem.Allocator) !void {
            var guard = try DefinitionMutationGuard.init(alloc, before, after);
            defer guard.deinit();
            try std.testing.expect(!try guard.touches(member));
            var other = member;
            other.object_name = "other";
            try std.testing.expect(try guard.touches(other));
            other.table_name = "different";
            try std.testing.expect(!try guard.touches(other));
            var shape = member;
            shape.object_kind = .table_schema;
            try std.testing.expect(!try guard.touches(shape));
            var changed = after;
            changed.description = "changed shape metadata";
            var shape_guard = try DefinitionMutationGuard.init(alloc, before, changed);
            defer shape_guard.deinit();
            try std.testing.expect(try shape_guard.touches(shape));
            shape.object_kind = .data_shape;
            shape.shape_kind = .row;
            try std.testing.expect(try shape_guard.touches(shape));
        }
    };
    try Fixture.run(std.testing.allocator);
    // Optional in-place arena growth must not change the numbered fault
    // schedule according to backing-heap placement between invocations.
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{});
    var invalid = Fixture.after;
    invalid.indexes_json = "[]";
    try std.testing.expectError(error.InvalidTableIndexMetadata, DefinitionMutationGuard.init(std.testing.allocator, Fixture.before, invalid));
}
