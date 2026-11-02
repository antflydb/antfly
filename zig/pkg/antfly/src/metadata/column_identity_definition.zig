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

//! One pure definition-to-attribute boundary for native admission and staged
//! generation validation. Persisting these values remains the native owner's job.
const std = @import("std");
const metadata = @import("domain.zig");
const column_identities = @import("antfly_local_sources").system_catalog_column_identities;

pub fn prepare(alloc: std.mem.Allocator, record: metadata.TableRecord, previous: ?column_identities.State, renames: []const column_identities.Rename) ![]u8 {
    if (record.schema_json.len > column_identities.max_encoded_bytes * 4) return error.ColumnIdentityLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const sources = @import("antfly_local_sources");
    const parsed = try sources.schema_mod.parseValidatedTableSchema(a, if (record.schema_json.len == 0) "{}" else record.schema_json);
    const columns = if (parsed.storage_mode == .document)
        try sources.sql_document_row.deriveColumns(a, parsed)
    else relational: {
        const native = try sources.schema_mod.deriveRuntimeTableSchema(a, parsed);
        break :relational try sources.sql_document_row.deriveRelationalColumnsLeaky(a, a, parsed, native);
    };
    const table: sources.sql_catalog.Table = .{ .id = record.table_id, .physical_name = record.name, .schema_version = parsed.version, .storage_mode = if (parsed.storage_mode == .document) .document else .relational, .columns = columns };
    const virtual = table.virtualColumns();
    if (columns.len + virtual.len > column_identities.max_columns) return error.ColumnIdentityLimitExceeded;
    const declarations = try a.alloc(column_identities.Declaration, columns.len + virtual.len);
    for (columns, declarations[0..columns.len]) |column, *out| out.* = .{ .name = column.name, .signature = try column.identitySignature(a) };
    for (virtual, declarations[columns.len..]) |column, *out| out.* = .{ .name = column.name, .signature = try column.identitySignature(a) };
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(record.schema_json, &digest, .{});
    var prepared = try column_identities.prepare(alloc, record.table_id, previous, .{ .version = parsed.version, .digest = digest }, declarations, renames);
    defer prepared.deinit();
    return prepared.value.encode(alloc);
}

pub fn initial(alloc: std.mem.Allocator, record: metadata.TableRecord) !std.json.Parsed(column_identities.State) {
    const encoded = try prepare(alloc, record, null, &.{});
    defer alloc.free(encoded);
    return column_identities.State.decode(alloc, encoded);
}

/// An explicit, bijective logical identity mapping. Row assignment expressions
/// alone cannot rename catalog attributes or adopt their annotations/ownership.
pub const Handoff = struct {
    before: std.StringHashMapUnmanaged(column_identities.Column) = .empty,
    after: std.StringHashMapUnmanaged(column_identities.Column) = .empty,
    destinations: std.StringHashMapUnmanaged([]const u8) = .empty,
    sources: std.StringHashMapUnmanaged([]const u8) = .empty,

    pub fn destination(self: Handoff, name: []const u8) []const u8 {
        return self.destinations.get(name) orelse name;
    }
};

pub fn prepareHandoffLeaky(a: std.mem.Allocator, before: column_identities.State, next: column_identities.State, renames: []const column_identities.Rename, transforms: []const @import("antfly_local_sources").storage_db_relational_rewrite_contract.ColumnTransform) !Handoff {
    if (renames.len > column_identities.max_columns or transforms.len > column_identities.max_columns) return error.ColumnIdentityLimitExceeded;
    try next.validate(a);
    try before.validate(a);
    var result: Handoff = .{};
    for (before.columns) |column| try result.before.put(a, column.name, column);
    for (next.columns) |column| try result.after.put(a, column.name, column);
    var assignments: std.StringHashMapUnmanaged([]const u8) = .empty;
    for (transforms) |transform| {
        const entry = try assignments.getOrPut(a, transform.column);
        if (entry.found_existing) return error.InvalidRestoreStaging;
        entry.value_ptr.* = transform.expression_json;
    }
    for (renames) |rename| {
        const source = result.before.get(rename.from) orelse return error.InvalidRestoreStaging;
        const target = result.after.get(rename.to) orelse return error.InvalidRestoreStaging;
        if (result.before.contains(rename.to) or result.after.contains(rename.from) or !std.mem.eql(u8, &source.signature, &target.signature)) return error.InvalidRestoreStaging;
        const forward = try result.destinations.getOrPut(a, rename.from);
        if (forward.found_existing) return error.InvalidRestoreStaging;
        forward.value_ptr.* = rename.to;
        const reverse = try result.sources.getOrPut(a, rename.to);
        if (reverse.found_existing) return error.InvalidRestoreStaging;
        reverse.value_ptr.* = rename.from;
        const expression = try std.json.parseFromSliceLeaky(std.json.Value, a, assignments.get(rename.to) orelse return error.InvalidRestoreStaging, .{});
        if (expression != .object or expression.object.count() != 2) return error.InvalidRestoreStaging;
        const op = expression.object.get("op") orelse return error.InvalidRestoreStaging;
        const column = expression.object.get("column") orelse return error.InvalidRestoreStaging;
        if (op != .string or column != .string or !std.mem.eql(u8, op.string, "column") or !std.mem.eql(u8, column.string, rename.from)) return error.InvalidRestoreStaging;
    }
    return result;
}

/// All returned bytes belong to the caller's publication region. Retained
/// columns preserve comments through type changes; derived additions do not
/// acquire another column's description merely by reading its old value.
pub fn inheritDescriptionsLeaky(a: std.mem.Allocator, next: column_identities.State, before: column_identities.State, renames: []const column_identities.Rename, transforms: []const @import("antfly_local_sources").storage_db_relational_rewrite_contract.ColumnTransform) !column_identities.State {
    const handoff = try prepareHandoffLeaky(a, before, next, renames, transforms);
    const columns = try a.dupe(column_identities.Column, next.columns);
    for (columns) |*column| {
        const source_name = handoff.sources.get(column.name) orelse column.name;
        const old = handoff.before.get(source_name) orelse continue;
        column.description = if (old.description) |description| try a.dupe(u8, description) else null;
    }
    var result = next;
    result.columns = columns;
    return result;
}

test "system catalog creation sources column annotations require explicit verified rename identity and survive retained type changes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const before: column_identities.State = .{ .format = 1, .table_id = 17, .epoch = .{ .version = 1, .digest = @splat(1) }, .next_id = 3, .columns = &.{ .{ .id = 1, .name = "status", .signature = @splat(2), .description = "status description" }, .{ .id = 2, .name = "n", .signature = @splat(3), .description = "number description" } } };
    const next: column_identities.State = .{ .format = 1, .table_id = 18, .epoch = .{ .version = 2, .digest = @splat(4) }, .next_id = 4, .columns = &.{ .{ .id = 1, .name = "state", .signature = @splat(2) }, .{ .id = 2, .name = "n", .signature = @splat(5) }, .{ .id = 3, .name = "derived", .signature = @splat(3) } } };
    const transforms = [_]@import("antfly_local_sources").storage_db_relational_rewrite_contract.ColumnTransform{ .{ .column = "state", .expression_json = "{\"op\":\"column\",\"column\":\"status\"}" }, .{ .column = "derived", .expression_json = "{\"op\":\"column\",\"column\":\"n\"}" } };
    const inherited = try inheritDescriptionsLeaky(a, next, before, &.{.{ .from = "status", .to = "state" }}, &transforms);
    try std.testing.expectEqualStrings("status description", inherited.columns[0].description.?);
    try std.testing.expectEqualStrings("number description", inherited.columns[1].description.?);
    try std.testing.expect(inherited.columns[2].description == null);
    const unclaimed = try inheritDescriptionsLeaky(a, next, before, &.{}, &transforms);
    try std.testing.expect(unclaimed.columns[0].description == null);
    try std.testing.expectError(error.InvalidRestoreStaging, prepareHandoffLeaky(a, before, next, &.{.{ .from = "n", .to = "state" }}, &transforms));
    try std.testing.expectError(error.InvalidRestoreStaging, prepareHandoffLeaky(a, before, next, &.{.{ .from = "status", .to = "state" }}, &.{}));
    try std.testing.expectError(error.InvalidRestoreStaging, prepareHandoffLeaky(a, before, next, &.{ .{ .from = "status", .to = "state" }, .{ .from = "status", .to = "state" } }, &transforms));
    try std.testing.expectEqual(next.next_id, inherited.next_id);
    try std.testing.expectEqualDeep(next.epoch, inherited.epoch);
}
