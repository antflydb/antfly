// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const extension_domain = @import("mod.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const json_helpers = @import("../api/json_helpers.zig");

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

    var expected_indexes = std.json.parseFromSlice(std.json.Value, alloc, expected.indexes_json, .{}) catch
        return error.InvalidTableIndexMetadata;
    defer expected_indexes.deinit();
    var replacement_indexes = std.json.parseFromSlice(std.json.Value, alloc, replacement.indexes_json, .{}) catch
        return error.InvalidTableIndexMetadata;
    defer replacement_indexes.deinit();
    if (expected_indexes.value != .object or replacement_indexes.value != .object)
        return error.InvalidTableIndexMetadata;

    for (snapshot.extension_members) |member| {
        const member_table = memberTableName(member) orelse continue;
        if (!std.mem.eql(u8, member_table, replacement.name)) continue;
        const unchanged = switch (member.object_kind) {
            .index => optionalJsonValuesEqual(
                expected_indexes.value.object.get(member.object_name),
                replacement_indexes.value.object.get(member.object_name),
            ),
            .enrichment => optionalJsonValuesEqual(
                try enrichmentByName(expected_indexes.value, member.object_name),
                try enrichmentByName(replacement_indexes.value, member.object_name),
            ),
            else => true,
        };
        if (!unchanged) return true;
    }
    return false;
}
