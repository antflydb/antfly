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

//! Pure object projection from the existing table schema and shared index map.
//! SQL and REST annotations use these same declarations. No catalog lookup,
//! native mutation, object allocation or public-name resolution occurs here.
const std = @import("std");
const objects = @import("object_annotations.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const max_definition_bytes = 8 * 1024 * 1024;
pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    digest: [32]u8,
    declarations: []const objects.Declaration,
    names: std.StringHashMapUnmanaged(usize),
    schema: Json,

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn byKey(self: Owned, key: objects.Key) ?objects.Declaration {
        if (key.name.len > 256) return null;
        var buffer: [272]u8 = undefined;
        const name = std.fmt.bufPrint(&buffer, "{s}:{s}", .{ @tagName(key.kind), key.name }) catch return null;
        const index = self.names.get(name) orelse return null;
        return self.declarations[index];
    }
    /// Resolve only an actual final declaration slot. Native naming may change
    /// its label, while declaration order remains the containing schema's own.
    pub fn copyDestination(self: Owned, copy: objects.Copy) !objects.Key {
        try copy.validate();
        const entries = self.schema.object.get(@tagName(copy.destination_family)) orelse return error.InvalidCatalogMutation;
        if (entries != .array or copy.destination_ordinal >= entries.array.items.len) return error.InvalidCatalogMutation;
        const entry = entries.array.items[copy.destination_ordinal];
        const name = try declarationName(entry);
        const key: objects.Key = .{ .kind = copy.source.kind, .name = name };
        if (self.byKey(key) == null) return error.InvalidCatalogMutation;
        if (copy.destination_family == .unique_constraints and copy.source.kind == .constraint and try indexOwned(entry)) return error.InvalidCatalogMutation;
        return key;
    }
};

/// Only object declarations contribute. Table version changes and opaque
/// resolver/enrichment maintenance must not rewrite the identity directory.
fn declarationDigest(a: A, declarations: []const objects.Declaration) ![32]u8 {
    const order = try a.alloc(usize, declarations.len);
    for (order, 0..) |*entry, i| entry.* = i;
    std.mem.sort(usize, order, declarations, struct {
        fn less(entries: []const objects.Declaration, left: usize, right: usize) bool {
            const lhs = entries[left].key;
            const rhs = entries[right].key;
            if (lhs.kind != rhs.kind) return @backingInt(lhs.kind) < @backingInt(rhs.kind);
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/catalog-object-declarations/v1\x00");
    for (order) |position| {
        const declaration = declarations[position];
        hash.update(&.{@backingInt(declaration.key.kind)});
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, declaration.key.name.len, .big);
        hash.update(&length);
        hash.update(declaration.key.name);
        hash.update(&declaration.signature);
        hash.update(&objects.descriptionDigest(declaration.declared_description));
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn declarationName(value: Json) ![]const u8 {
    if (value != .object) return error.InvalidCatalogRecord;
    const name = value.object.get("name") orelse return error.InvalidCatalogRecord;
    if (name != .string) return error.InvalidCatalogRecord;
    try (objects.Key{ .kind = .index, .name = name.string }).validate();
    return name.string;
}
fn indexOwned(value: Json) !bool {
    const origin = value.object.get("origin") orelse return false;
    if (origin == .null) return false;
    if (origin != .string) return error.InvalidCatalogRecord;
    if (std.mem.eql(u8, origin.string, "index")) return true;
    if (std.mem.eql(u8, origin.string, "constraint")) return false;
    return error.InvalidCatalogRecord;
}
fn description(value: Json) !?[]const u8 {
    const cell = value.object.get("description") orelse return null;
    if (cell == .null) return null;
    if (cell != .string) return error.InvalidCatalogRecord;
    try objects.validateDescription(cell.string);
    return objects.normalized(cell.string);
}
fn signature(a: A, kind: objects.Kind, family: []const u8, value: Json) ![32]u8 {
    var projected: std.json.ObjectMap = .empty;
    for (value.object.keys(), value.object.values()) |key, cell| {
        if (std.mem.eql(u8, key, "name") or std.mem.eql(u8, key, "description")) continue;
        try projected.put(a, key, cell);
    }
    const bytes = try std.json.Stringify.valueAlloc(a, Json{ .object = projected }, .{});
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/catalog-object-signature/v1\x00");
    hash.update(@tagName(kind));
    hash.update("\x00");
    hash.update(family);
    hash.update("\x00");
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn init(backing: A, schema_json: []const u8, indexes_json: []const u8) !Owned {
    if (schema_json.len > max_definition_bytes or indexes_json.len > max_definition_bytes - schema_json.len) return error.CatalogCommandTooLarge;
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(Json, a, if (schema_json.len == 0) "{}" else schema_json, .{ .allocate = .alloc_always });
    const indexes = try std.json.parseFromSliceLeaky(Json, a, if (indexes_json.len == 0) "{}" else indexes_json, .{ .allocate = .alloc_always });
    if (schema != .object or indexes != .object) return error.InvalidCatalogRecord;
    const Builder = struct {
        alloc: A,
        entries: std.ArrayList(objects.Declaration) = .empty,
        names: std.StringHashMapUnmanaged(usize) = .empty,

        fn append(self: *@This(), kind: objects.Kind, name: []const u8, family: []const u8, value: Json, related_unique: bool) !void {
            try (objects.Key{ .kind = kind, .name = name }).validate();
            if (value != .object) return error.InvalidCatalogRecord;
            const key = try std.fmt.allocPrint(self.alloc, "{s}:{s}", .{ @tagName(kind), name });
            const entry = try self.names.getOrPut(self.alloc, key);
            const current_signature = try signature(self.alloc, kind, family, value);
            if (entry.found_existing) {
                // An ordered unique index may also have its index-owned
                // uniqueness declaration. They describe one index identity.
                if (!related_unique or kind != .index) return error.InvalidCatalogRecord;
                const existing = &self.entries.items[entry.value_ptr.*];
                var hash = std.crypto.hash.Blake3.init(.{});
                hash.update("antfly/catalog-related-unique-index/v1\x00");
                hash.update(&existing.signature);
                hash.update(&current_signature);
                hash.final(&existing.signature);
                return;
            }
            if (self.entries.items.len == objects.max_objects) return error.CatalogCommandTooLarge;
            entry.value_ptr.* = self.entries.items.len;
            try self.entries.append(self.alloc, .{ .key = .{ .kind = kind, .name = name }, .signature = current_signature, .declared_description = try description(value) });
        }
    };
    var builder: Builder = .{ .alloc = a };
    for ([_][]const u8{ "relational_indexes", "unique_constraints", "checks", "foreign_keys" }) |family| {
        const entries = schema.object.get(family) orelse continue;
        if (entries != .array or entries.array.items.len > objects.max_objects) return error.InvalidCatalogRecord;
        for (entries.array.items) |entry| {
            const name = try declarationName(entry);
            if (std.mem.eql(u8, family, "relational_indexes")) {
                try builder.append(.index, name, family, entry, false);
            } else if (std.mem.eql(u8, family, "unique_constraints")) {
                if (!try indexOwned(entry)) try builder.append(.constraint, name, family, entry, false);
                try builder.append(.index, name, family, entry, true);
            } else try builder.append(.constraint, name, family, entry, false);
        }
    }
    for (indexes.object.keys(), indexes.object.values()) |name, config| {
        if (@import("../api/index_metadata.zig").isReservedEntry(name)) continue;
        if (config != .object) return error.InvalidCatalogRecord;
        if (config.object.get("name")) |declared| {
            if (declared != .string or !std.mem.eql(u8, declared.string, name)) return error.InvalidCatalogRecord;
        }
        try builder.append(.index, name, "indexes", config, false);
    }
    return .{ .arena = arena, .digest = try declarationDigest(a, builder.entries.items), .declarations = try builder.entries.toOwnedSlice(a), .names = builder.names, .schema = schema };
}

test "catalog object definitions share relational and document index descriptions while separating constraint ownership" {
    const a = std.testing.allocator;
    const schema = "{\"unique_constraints\":[{\"name\":\"primary\",\"columns\":[\"id\"],\"primary\":true},{\"name\":\"unique_index\",\"columns\":[\"label\"],\"origin\":\"index\"}],\"checks\":[{\"name\":\"status_check\",\"column\":\"status\",\"op\":\"is_not_null\"}],\"relational_indexes\":[{\"name\":\"ordered\",\"description\":\"ordered lookup\",\"keys\":[{\"column\":\"label\"}]}]}";
    var value = try init(a, schema, "{\"graph\":{\"type\":\"graph\",\"description\":\"graph lookup\"}}");
    defer value.deinit();
    try std.testing.expectEqual(@as(usize, 6), value.declarations.len);
    try std.testing.expect(value.byKey(.{ .kind = .constraint, .name = "unique_index" }) == null);
    try std.testing.expect(value.byKey(.{ .kind = .index, .name = "unique_index" }) != null);
    try std.testing.expectEqualStrings("ordered lookup", value.byKey(.{ .kind = .index, .name = "ordered" }).?.declared_description.?);
    try std.testing.expectEqualStrings("graph lookup", value.byKey(.{ .kind = .index, .name = "graph" }).?.declared_description.?);
    const destination = try value.copyDestination(.{ .source_table_id = 7, .source = .{ .kind = .index, .name = "source_index" }, .destination_family = .relational_indexes, .destination_ordinal = 0 });
    try std.testing.expectEqualStrings("ordered", destination.name);
    try std.testing.expectError(error.InvalidCatalogMutation, value.copyDestination(.{ .source_table_id = 7, .source = .{ .kind = .constraint, .name = "source_constraint" }, .destination_family = .unique_constraints, .destination_ordinal = 1 }));
}

test "catalog object definitions copy signatures exclude only their own name and description and retain expression literal data" {
    const a = std.testing.allocator;
    var source = try init(a, "{\"checks\":[{\"name\":\"source\",\"expression\":{\"op\":\"literal\",\"value\":{\"name\":\"payload\",\"description\":\"data\"}}}]}", "{}");
    defer source.deinit();
    var destination = try init(a, "{\"checks\":[{\"name\":\"destination\",\"expression\":{\"op\":\"literal\",\"value\":{\"name\":\"payload\",\"description\":\"data\"}}}]}", "{}");
    defer destination.deinit();
    try std.testing.expectEqualDeep(source.declarations[0].signature, destination.declarations[0].signature);
    try std.testing.expect(!std.mem.eql(u8, &source.digest, &destination.digest));
    var changed = try init(a, "{\"checks\":[{\"name\":\"destination\",\"expression\":{\"op\":\"literal\",\"value\":{\"name\":\"payload\",\"description\":\"different\"}}}]}", "{}");
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &source.declarations[0].signature, &changed.declarations[0].signature));
    try std.testing.expectError(error.InvalidCatalogRecord, init(a, "{\"checks\":[{\"name\":\"same\"},{\"name\":\"same\"}]}", "{}"));
}

test "catalog object definitions exclude shared enrichment resolver and opaque control metadata before examining payloads" {
    var value = try init(std.testing.allocator, "{}", "{\"resolvers\":[],\"enrichments\":[],\"_resolver_record_store\":null,\"_resolver_record_owners\":false,\"_resolver_curation\":{\"name\":\"fake index\"},\"_resolver_namespaces\":{},\"_curation_proposals\":[],\"search\":{\"type\":\"full_text\"}}");
    defer value.deinit();
    try std.testing.expectEqual(@as(usize, 1), value.declarations.len);
    try std.testing.expectEqualStrings("search", value.declarations[0].key.name);
}

test "catalog object definitions ignore table epochs and opaque maintenance churn while retaining object configuration changes" {
    const a = std.testing.allocator;
    var before = try init(a, "{\"version\":1}", "{\"search\":{\"type\":\"full_text\"},\"_resolver_curation\":{\"revision\":1}}");
    defer before.deinit();
    var after = try init(a, "{\"version\":2,\"description\":\"table metadata\"}", "{\"_resolver_curation\":{\"revision\":2},\"enrichments\":[],\"search\":{\"type\":\"full_text\"}}");
    defer after.deinit();
    try std.testing.expectEqualDeep(before.digest, after.digest);
    var changed = try init(a, "{}", "{\"search\":{\"type\":\"full_text\",\"description\":\"new inline\"}}");
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &before.digest, &changed.digest));
}

/// Validate explicit identity retention from actual before/after declarations.
/// No similar-looking replacement is ever adopted as a rename. A UNIQUE or
/// PRIMARY rename must retain its backing-index identity in the same intent.
pub fn validateRenameDefinitions(backing: A, before_schema: []const u8, before_indexes: []const u8, after_schema: []const u8, after_indexes: []const u8, renames: []const objects.Rename) !void {
    try objects.validateSingleRename(renames);
    if (renames.len == 0) return;
    var before = try init(backing, before_schema, before_indexes);
    defer before.deinit();
    var after = init(backing, after_schema, after_indexes) catch |err| switch (err) {
        error.OutOfMemory, error.CatalogCommandTooLarge => return err,
        else => return error.InvalidCatalogMutation,
    };
    defer after.deinit();
    var backed_constraint = false;
    for (renames) |rename| if (rename.kind == .constraint) {
        if (before.schema.object.get("unique_constraints")) |rules| {
            for (rules.array.items) |rule| if (std.mem.eql(u8, try declarationName(rule), rename.from)) {
                if (try indexOwned(rule)) return error.InvalidCatalogMutation;
                backed_constraint = true;
                const paired = for (renames) |candidate| {
                    if (candidate.kind == .index and std.mem.eql(u8, candidate.from, rename.from) and std.mem.eql(u8, candidate.to, rename.to)) break true;
                } else false;
                if (!paired) return error.InvalidCatalogMutation;
            };
        }
    };
    if (renames.len == 2 and !backed_constraint) return error.InvalidCatalogMutation;
    var source = try objects.prepare(backing, 1, null, before.digest, before.declarations);
    defer source.deinit();
    var target = try objects.prepareWithRenames(backing, 1, source.state, after.digest, after.declarations, renames);
    defer target.deinit();
}

test "catalog object rename definitions require complete unique ownership and preserve declaration families" {
    const a = std.testing.allocator;
    const unique_before = "{\"unique_constraints\":[{\"name\":\"old\",\"columns\":[\"id\"],\"primary\":true}]}";
    const unique_after = "{\"unique_constraints\":[{\"name\":\"new\",\"columns\":[\"id\"],\"primary\":true}]}";
    const constraint: objects.Rename = .{ .kind = .constraint, .from = "old", .to = "new" };
    const index: objects.Rename = .{ .kind = .index, .from = "old", .to = "new" };
    try validateRenameDefinitions(a, unique_before, "{}", unique_after, "{}", &.{ constraint, index });
    try std.testing.expectError(error.InvalidCatalogMutation, validateRenameDefinitions(a, unique_before, "{}", unique_after, "{}", &.{constraint}));
    try validateRenameDefinitions(a, "{\"checks\":[{\"name\":\"old\"}]}", "{}", "{\"checks\":[{\"name\":\"new\"}]}", "{}", &.{constraint});
    try std.testing.expectError(error.InvalidCatalogMutation, validateRenameDefinitions(a, "{\"checks\":[{\"name\":\"old\"}]}", "{}", "{\"foreign_keys\":[{\"name\":\"new\"}]}", "{}", &.{constraint}));
}

/// Apply one exact declaration rename. Callers still bind the predecessor and
/// successor versions and reject any unrelated schema change independently.
pub fn renameSchemaValue(a: A, schema: *Json, renames: []const objects.Rename) !void {
    try objects.validateSingleRename(renames);
    if (renames.len == 0 or schema.* != .object) return error.InvalidCatalogMutation;
    for (renames) |rename| {
        var found: usize = 0;
        for ([_][]const u8{ "checks", "foreign_keys", "unique_constraints", "relational_indexes" }) |family| {
            const is_index = std.mem.eql(u8, family, "relational_indexes");
            if (is_index != (rename.kind == .index)) continue;
            if (schema.object.getPtr(family)) |entries| {
                if (entries.* != .array) return error.InvalidCatalogMutation;
                for (entries.array.items) |*entry| {
                    const name = try declarationName(entry.*);
                    if (std.mem.eql(u8, name, rename.to)) return error.InvalidCatalogMutation;
                    if (std.mem.eql(u8, name, rename.from)) {
                        entry.object = try entry.object.clone(a);
                        try entry.object.put(a, "name", .{ .string = try a.dupe(u8, rename.to) });
                        found += 1;
                    }
                }
            }
        }
        // A UNIQUE's backing object can be implicit in unique_constraints.
        if (found == 0 and rename.kind == .index) {
            if (schema.object.get("unique_constraints")) |entries| for (entries.array.items) |entry| {
                if (std.mem.eql(u8, try declarationName(entry), rename.to)) {
                    found = 1;
                    break;
                }
            };
        }
        if (found != 1) return error.InvalidCatalogMutation;
    }
}

pub fn renameFingerprint(renames: []const objects.Rename) ![32]u8 {
    try objects.validateSingleRename(renames);
    if (renames.len == 0) return error.InvalidCatalogMutation;
    var state = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
    try state.writer.writeAll("antfly/catalog-object-rename/v1\x00");
    try std.json.Stringify.value(renames, .{}, &state.writer);
    var result: [32]u8 = undefined;
    state.hasher.final(&result);
    return result;
}

pub fn verifySchemaRename(a: A, before: []const u8, after: []const u8, renames: []const objects.Rename) !void {
    try validateRenameDefinitions(a, before, "{}", after, "{}", renames);
    var old = try std.json.parseFromSliceLeaky(Json, a, before, .{ .parse_numbers = false });
    var next = try std.json.parseFromSliceLeaky(Json, a, after, .{ .parse_numbers = false });
    try renameSchemaValue(a, &old, renames);
    _ = old.object.orderedRemove("version");
    _ = next.object.orderedRemove("version");
    if (!jsonEqual(old, next)) return error.InvalidCatalogMutation;
}

fn jsonEqual(left: Json, right: Json) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .null => true,
        .bool => |value| value == right.bool,
        .integer => |value| value == right.integer,
        .float => |value| value == right.float,
        .number_string => |value| std.mem.eql(u8, value, right.number_string),
        .string => |value| std.mem.eql(u8, value, right.string),
        .array => |items| blk: {
            if (items.items.len != right.array.items.len) break :blk false;
            for (items.items, right.array.items) |lhs, rhs| if (!jsonEqual(lhs, rhs)) break :blk false;
            break :blk true;
        },
        .object => |fields| blk: {
            if (fields.count() != right.object.count()) break :blk false;
            for (fields.keys(), fields.values()) |key, value| if (!jsonEqual(value, right.object.get(key) orelse break :blk false)) break :blk false;
            break :blk true;
        },
    };
}

test "catalog object rename schema proof permits only complete naming changes without rewriting nested literals" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const arena = region.allocator();
    const hints = [_]objects.Rename{ .{ .kind = .constraint, .from = "old", .to = "new" }, .{ .kind = .index, .from = "old", .to = "new" } };
    const before = "{\"version\":1,\"unique_constraints\":[{\"name\":\"old\",\"columns\":[\"id\"]}],\"other\":{\"name\":\"old\"}}";
    const after = "{\"other\":{\"name\":\"old\"},\"unique_constraints\":[{\"columns\":[\"id\"],\"name\":\"new\"}],\"version\":2}";
    // JSON object member order does not change this logical rename proof.
    const canonical_after = "{\"other\":{\"name\":\"old\"},\"unique_constraints\":[{\"name\":\"new\",\"columns\":[\"id\"]}],\"version\":2}";
    try verifySchemaRename(arena, before, canonical_after, &hints);
    try verifySchemaRename(arena, before, after, &hints);
    const rewritten = "{\"version\":2,\"unique_constraints\":[{\"name\":\"new\",\"columns\":[\"id\"]}],\"other\":{\"name\":\"new\"}}";
    try std.testing.expectError(error.InvalidCatalogMutation, verifySchemaRename(arena, before, rewritten, &hints));
}
