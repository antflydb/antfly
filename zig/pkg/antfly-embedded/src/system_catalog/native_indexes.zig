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

//! SQL namespace ownership over existing native index configurations. Native
//! table-local indexes acquire no SQL name unless explicitly declared here.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_names = 256;
pub const max_name_bytes = 256;
pub const max_bytes = 64 * 1024;
pub const GinOperatorClass = enum { jsonb_path_ops, array_ops };
/// SQL definition metadata belongs to namespace custody, not runtime config.
pub const GinBinding = struct {
    name: []const u8,
    field: []const u8,
    operator_class: GinOperatorClass,
};
pub const Binding = struct { name: []const u8, incarnation: u64, gin: ?GinBinding = null };
const Record = struct { version: u8 = 1, names: []const []const u8 = &.{}, gin: ?[]const GinBinding = null };

pub const Bindings = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const Binding,
    pub fn deinit(self: *Bindings) void {
        self.arena.deinit();
    }
};
fn validName(name: []const u8) bool {
    return @import("index_metadata.zig").isValidIndexName(name);
}
fn load(a: A, encoded: []const u8) !Record {
    if (encoded.len == 0) return .{};
    if (encoded.len > max_bytes) return error.InvalidCatalogRecord;
    const record = std.json.parseFromSliceLeaky(Record, a, encoded, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogRecord;
    const gin = record.gin orelse &.{};
    if (record.names.len > max_names or gin.len > record.names.len or
        record.version != (if (gin.len == 0) @as(u8, 1) else 2)) return error.InvalidCatalogRecord;
    for (record.names, 0..) |name, i| {
        if (!validName(name) or (i != 0 and std.mem.order(u8, record.names[i - 1], name) != .lt)) return error.InvalidCatalogRecord;
    }
    var selected_position: usize = 0;
    for (gin, 0..) |binding, i| {
        if (!validName(binding.name) or binding.field.len == 0 or binding.field.len > 256 or
            !std.unicode.utf8ValidateSlice(binding.field) or std.mem.indexOfScalar(u8, binding.field, 0) != null or
            (i != 0 and std.mem.order(u8, gin[i - 1].name, binding.name) != .lt)) return error.InvalidCatalogRecord;
        while (selected_position < record.names.len and std.mem.order(u8, record.names[selected_position], binding.name) == .lt) : (selected_position += 1) {}
        if (selected_position == record.names.len or !std.mem.eql(u8, record.names[selected_position], binding.name)) return error.InvalidCatalogRecord;
    }
    return record;
}
fn names(a: A, encoded: []const u8) ![]const []const u8 {
    return (try load(a, encoded)).names;
}
fn ginFor(record: Record, name: []const u8) ?GinBinding {
    const gin = record.gin orelse return null;
    var lo: usize = 0;
    var hi = gin.len;
    while (lo < hi) {
        const middle = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, gin[middle].name, name)) {
            .lt => lo = middle + 1,
            .gt => hi = middle,
            .eq => return gin[middle],
        }
    }
    return null;
}

pub fn validateNames(a: A, encoded: []const u8) !void {
    var scratch: std.heap.ArenaAllocator = .init(a);
    defer scratch.deinit();
    _ = try names(scratch.allocator(), encoded);
}
/// Project only selected incarnation headers. Unrelated configurations and
/// their opaque evidence are skipped without building a configuration DOM.
pub fn read(a: A, encoded: []const u8, configs: []const u8) !Bindings {
    return readMode(a, encoded, configs, false);
}
fn readMode(a: A, encoded: []const u8, configs: []const u8, prune_missing: bool) !Bindings {
    var arena: std.heap.ArenaAllocator = .init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const record = try load(owned, encoded);
    const selected = record.names;
    if (selected.len == 0) return .{ .arena = arena, .entries = &.{} };
    if (configs.len > 16 * 1024 * 1024) return error.CatalogCommandTooLarge;
    var positions: std.StringHashMapUnmanaged(usize) = .empty;
    defer positions.deinit(owned);
    for (selected, 0..) |name, i| try positions.put(owned, name, i);
    const generations = try owned.alloc(u64, selected.len);
    @memset(generations, 0);
    var scanner = std.json.Scanner.initCompleteInput(owned, configs);
    defer scanner.deinit();
    if (try scanner.next() != .object_begin) return error.InvalidCatalogRecord;
    while (true) {
        const token = try scanner.nextAllocMax(owned, .alloc_if_needed, 16384);
        if (token == .object_end) break;
        const name = switch (token) {
            .string, .allocated_string => |value| value,
            else => return error.InvalidCatalogRecord,
        };
        if (positions.get(name)) |index| {
            if (generations[index] != 0) return error.InvalidCatalogRecord;
            const Header = struct { type: []const u8, _index_incarnation: u64 };
            const header = std.json.innerParse(Header, owned, &scanner, .{ .ignore_unknown_fields = true, .allocate = .alloc_always, .max_value_len = 16384 }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogRecord;
            if (header._index_incarnation == 0 or header._index_incarnation > std.math.maxInt(i64) or
                !(std.mem.eql(u8, header.type, "full_text") or std.mem.eql(u8, header.type, "graph") or std.mem.eql(u8, header.type, "embeddings") or std.mem.eql(u8, header.type, "algebraic"))) return error.InvalidCatalogRecord;
            if (ginFor(record, name) != null and !std.mem.eql(u8, header.type, "algebraic")) return error.InvalidCatalogRecord;
            generations[index] = header._index_incarnation;
        } else try scanner.skipValue();
    }
    if (try scanner.next() != .end_of_document) return error.InvalidCatalogRecord;
    const entries = try owned.alloc(Binding, selected.len);
    var count: usize = 0;
    for (selected, generations) |name, generation| {
        if (generation == 0) {
            if (!prune_missing) return error.InvalidCatalogRecord;
            continue;
        }
        entries[count] = .{ .name = name, .incarnation = generation, .gin = ginFor(record, name) };
        count += 1;
    }
    return .{ .arena = arena, .entries = entries[0..count] };
}
fn encode(a: A, selected: []const []const u8, gin: []const GinBinding) ![]u8 {
    if (selected.len == 0) {
        if (gin.len != 0) return error.InvalidCatalogRecord;
        return a.dupe(u8, "");
    }
    const bytes = try std.json.Stringify.valueAlloc(a, Record{ .version = if (gin.len == 0) 1 else 2, .names = selected, .gin = if (gin.len == 0) null else gin }, .{ .emit_null_optional_fields = false });
    if (bytes.len > max_bytes) {
        a.free(bytes);
        return error.CatalogCommandTooLarge;
    }
    return bytes;
}
pub fn add(a: A, encoded: []const u8, name: []const u8) ![]u8 {
    return addWithGin(a, encoded, name, null);
}
pub fn addGin(a: A, encoded: []const u8, binding: GinBinding) ![]u8 {
    return addWithGin(a, encoded, binding.name, binding);
}
fn addWithGin(a: A, encoded: []const u8, name: []const u8, binding: ?GinBinding) ![]u8 {
    if (!validName(name)) return error.InvalidCatalogMutation;
    var scratch: std.heap.ArenaAllocator = .init(a);
    defer scratch.deinit();
    const record = try load(scratch.allocator(), encoded);
    const selected = record.names;
    if (selected.len == max_names) return error.CatalogCommandTooLarge;
    const result = try scratch.allocator().alloc([]const u8, selected.len + 1);
    @memcpy(result[0..selected.len], selected);
    for (selected) |prior| if (std.mem.eql(u8, prior, name)) return error.SqlIndexAlreadyExists;
    result[selected.len] = name;
    std.mem.sort([]const u8, result, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.less);
    var gin: std.ArrayList(GinBinding) = .empty;
    try gin.appendSlice(scratch.allocator(), record.gin orelse &.{});
    if (binding) |item| {
        if (item.field.len == 0 or item.field.len > 256 or !std.unicode.utf8ValidateSlice(item.field) or std.mem.indexOfScalar(u8, item.field, 0) != null) return error.InvalidCatalogMutation;
        try gin.append(scratch.allocator(), item);
    }
    std.mem.sort(GinBinding, gin.items, {}, struct {
        fn less(_: void, left: GinBinding, right: GinBinding) bool {
            return std.mem.order(u8, left.name, right.name) == .lt;
        }
    }.less);
    return encode(a, result, gin.items);
}
/// Generic native index deletion withdraws its SQL name and operator class in
/// the same table-definition CAS. Reconfiguration retains exact incarnations.
pub fn prune(a: A, encoded: []const u8, configs: []const u8) ![]u8 {
    var bindings = try readMode(a, encoded, configs, true);
    defer bindings.deinit();
    const selected = try bindings.arena.allocator().alloc([]const u8, bindings.entries.len);
    var gin: std.ArrayList(GinBinding) = .empty;
    for (bindings.entries, selected) |entry, *name| {
        name.* = entry.name;
        if (entry.gin) |item| try gin.append(bindings.arena.allocator(), item);
    }
    return encode(a, selected, gin.items);
}

const identity_magic = "AFSQLIDX";
pub fn identity(incarnation: u64) [16]u8 {
    std.debug.assert(incarnation != 0);
    var result: [16]u8 = undefined;
    @memcpy(result[0..8], identity_magic);
    std.mem.writeInt(u64, result[8..16], incarnation, .big);
    return result;
}
pub fn isNativeIdentity(value: [16]u8) bool {
    return std.mem.eql(u8, value[0..8], identity_magic) and std.mem.readInt(u64, value[8..16], .big) != 0;
}

test "native index namespace bindings preserve incarnation and prune only missing names" {
    const a = std.testing.allocator;
    const one = try add(a, "", "status_fts");
    defer a.free(one);
    const both = try add(a, one, "body_fts");
    defer a.free(both);
    const configs = "{\"body_fts\":{\"type\":\"full_text\",\"_index_incarnation\":17,\"opaque\":{\"large\":[1,2]}},\"status_fts\":{\"type\":\"full_text\",\"_index_incarnation\":19},\"local_only\":{\"anything\":[null]}}";
    var result = try read(a, both, configs);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.entries.len);
    try std.testing.expectEqualStrings("body_fts", result.entries[0].name);
    try std.testing.expectEqual(@as(u64, 17), result.entries[0].incarnation);
    const pruned = try prune(a, both, "{\"status_fts\":{\"type\":\"full_text\",\"_index_incarnation\":21}}");
    defer a.free(pruned);
    try std.testing.expectEqualStrings(one, pruned);
    try std.testing.expect(isNativeIdentity(identity(21)));
    try std.testing.expect(!std.mem.eql(u8, &identity(19), &identity(21)));
    try std.testing.expectError(error.SqlIndexAlreadyExists, add(a, both, "status_fts"));
}

test "native index namespace bindings reject corruption and have a zero allocation absent path" {
    const a = std.testing.allocator;
    var no_memory: std.heap.FixedBufferAllocator = .init(&.{});
    var absent = try read(no_memory.allocator(), "", "not read for tables without SQL native indexes");
    defer absent.deinit();
    try std.testing.expectEqual(@as(usize, 0), absent.entries.len);
    const selected = "{\"version\":1,\"names\":[\"fts\"]}";
    for ([_][]const u8{
        "{}",
        "{\"fts\":{\"type\":\"full_text\",\"_index_incarnation\":0}}",
        "{\"fts\":{\"type\":\"unknown\",\"_index_incarnation\":1}}",
        "{\"fts\":{\"type\":\"full_text\",\"_index_incarnation\":1},\"fts\":{\"type\":\"full_text\",\"_index_incarnation\":2}}",
    }) |bad| try std.testing.expectError(error.InvalidCatalogRecord, read(a, selected, bad));
    try std.testing.expectError(error.InvalidCatalogRecord, read(a, "{\"version\":1,\"names\":[\"z\",\"a\"]}", "{}"));
    try std.testing.expectError(error.InvalidCatalogRecord, read(a, "{\"version\":2,\"names\":[]}", "{}"));
}

test "SQL GIN namespace custody retains typed definitions across additions pruning and allocation failures" {
    const a = std.testing.allocator;
    const Probe = struct {
        fn run(alloc: A) !void {
            const one = try addGin(alloc, "", .{ .name = "gin", .field = "metadata", .operator_class = .jsonb_path_ops });
            defer alloc.free(one);
            const both = try add(alloc, one, "text");
            defer alloc.free(both);
            const configs = "{\"gin\":{\"type\":\"algebraic\",\"_index_incarnation\":17},\"text\":{\"type\":\"full_text\",\"_index_incarnation\":18}}";
            var current = try read(alloc, both, configs);
            defer current.deinit();
            try std.testing.expectEqual(@as(usize, 2), current.entries.len);
            try std.testing.expectEqualStrings("metadata", current.entries[0].gin.?.field);
            try std.testing.expectEqual(.jsonb_path_ops, current.entries[0].gin.?.operator_class);
            const pruned = try prune(alloc, both, "{\"text\":{\"type\":\"full_text\",\"_index_incarnation\":18}}");
            defer alloc.free(pruned);
            var remaining = try read(alloc, pruned, "{\"text\":{\"type\":\"full_text\",\"_index_incarnation\":18}}");
            defer remaining.deinit();
            try std.testing.expectEqual(@as(usize, 1), remaining.entries.len);
            try std.testing.expect(remaining.entries[0].gin == null);
        }
    };
    try Probe.run(a);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Probe.run, .{});
    const encoded = try addGin(a, "", .{ .name = "gin", .field = "metadata", .operator_class = .jsonb_path_ops });
    defer a.free(encoded);
    try std.testing.expectError(error.InvalidCatalogRecord, read(a, encoded, "{\"gin\":{\"type\":\"full_text\",\"_index_incarnation\":17}}"));
}
