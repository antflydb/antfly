// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Semantic graph declaration identity shared by metadata plans and owners.
//! This does not by itself authorize retirement: an owner must additionally
//! seal its workers and persist a fence-bound receipt.
const std = @import("std");
const types = @import("types.zig");

pub const Digest = [32]u8;
const Entry = struct { name: []const u8, generation: u64, config: std.json.Value };

pub fn fromMetadata(alloc: std.mem.Allocator, indexes_json: []const u8) !?Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableIndexMetadata;
    var entries: std.ArrayList(Entry) = .empty;
    defer entries.deinit(alloc);
    var it = parsed.value.object.iterator();
    while (it.next()) |item| {
        const config = item.value_ptr.*;
        if (config != .object) return error.InvalidTableIndexMetadata;
        const kind = config.object.get("type") orelse return error.InvalidTableIndexMetadata;
        if (kind != .string) return error.InvalidTableIndexMetadata;
        if (!std.mem.eql(u8, kind.string, "graph")) continue;
        const generation: u64 = if (config.object.get("_index_incarnation") orelse config.object.get("_coverage_incarnation")) |value| blk: {
            if (value != .integer or value.integer <= 0) return error.InvalidTableIndexMetadata;
            break :blk @intCast(value.integer);
        } else blk: {
            // Match table_provisioner's legacy fallback, which hashes the
            // exact runtime config extracted in catalog field order.
            const runtime_json = try runtimeConfigJson(alloc, config);
            defer alloc.free(runtime_json);
            break :blk @import("../internal_keys.zig").derivedCoverageGeneration(runtime_json);
        };
        try entries.append(alloc, .{ .name = item.key_ptr.*, .generation = generation, .config = config });
    }
    return digestEntries(alloc, entries.items, true);
}

pub fn fromLoaded(alloc: std.mem.Allocator, configs: []const types.IndexConfig) !?Digest {
    var entries: std.ArrayList(Entry) = .empty;
    defer entries.deinit(alloc);
    var parsed_configs: std.ArrayList(std.json.Parsed(std.json.Value)) = .empty;
    defer {
        for (parsed_configs.items) |*parsed| parsed.deinit();
        parsed_configs.deinit(alloc);
    }
    for (configs) |config| {
        if (config.kind != .graph) continue;
        if (config.coverage_generation == 0) return error.GraphRetirementIncarnationMissing;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, config.config_json, .{});
        if (parsed.value != .object) {
            var owned = parsed;
            owned.deinit();
            return error.InvalidTableIndexMetadata;
        }
        parsed_configs.append(alloc, parsed) catch |err| {
            parsed.deinit();
            return err;
        };
        try entries.append(alloc, .{ .name = config.name, .generation = config.coverage_generation, .config = parsed.value });
    }
    return digestEntries(alloc, entries.items, false);
}

pub fn retirementDigest(source_table_id: u64, target_table_id: u64, config_digest: Digest) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-empty-generation-graph-retirement-v2\x00");
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, source_table_id, .little);
    hash.update(&number);
    std.mem.writeInt(u64, &number, target_table_id, .little);
    hash.update(&number);
    hash.update(&config_digest);
    var out: Digest = undefined;
    hash.final(&out);
    return out;
}

fn digestEntries(alloc: std.mem.Allocator, entries: []Entry, metadata: bool) !?Digest {
    if (entries.len == 0) return null;
    std.mem.sort(Entry, entries, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-graph-config-v1\x00");
    hashInt(&hash, entries.len);
    for (entries, 0..) |entry, index| {
        if (index != 0 and std.mem.eql(u8, entries[index - 1].name, entry.name)) return error.InvalidTableIndexMetadata;
        hashBytes(&hash, entry.name);
        hashInt(&hash, entry.generation);
        try hashValue(alloc, &hash, entry.config, metadata);
    }
    var out: Digest = undefined;
    hash.final(&out);
    return out;
}

fn hashInt(hash: *std.crypto.hash.Blake3, number: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, number, .little);
    hash.update(&bytes);
}

fn hashBytes(hash: *std.crypto.hash.Blake3, bytes: []const u8) void {
    hashInt(hash, bytes.len);
    hash.update(bytes);
}

fn catalogField(name: []const u8) bool {
    // Keep in sync with table_index_config.isCatalogMetadataField(.graph).
    for ([_][]const u8{ "type", "name", "description", "validation", "enrichments", "derive_from_schema", "_index_incarnation", "_coverage_incarnation", "version" }) |field| {
        if (std.mem.eql(u8, name, field)) return true;
    }
    return false;
}

fn runtimeConfigJson(alloc: std.mem.Allocator, config: std.json.Value) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = config.object.iterator();
    while (it.next()) |item| {
        if (catalogField(item.key_ptr.*)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        const name = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(item.key_ptr.*, .{})});
        defer alloc.free(name);
        const value = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(item.value_ptr.*, .{})});
        defer alloc.free(value);
        try out.appendSlice(alloc, name);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, value);
    }
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

fn hashValue(alloc: std.mem.Allocator, hash: *std.crypto.hash.Blake3, value: std.json.Value, strip_catalog_fields: bool) !void {
    switch (value) {
        .null => hash.update("n"),
        .bool => |flag| hash.update(if (flag) "t" else "f"),
        .integer => |number| {
            hash.update("i");
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(i64, &bytes, number, .little);
            hash.update(&bytes);
        },
        .float => |number| {
            if (!std.math.isFinite(number)) return error.InvalidTableIndexMetadata;
            hash.update("d");
            hashInt(hash, @bitCast(number));
        },
        .number_string => |number| {
            hash.update("q");
            hashBytes(hash, number);
        },
        .string => |string| {
            hash.update("s");
            hashBytes(hash, string);
        },
        .array => |array| {
            hash.update("a");
            hashInt(hash, array.items.len);
            for (array.items) |item| try hashValue(alloc, hash, item, false);
        },
        .object => |object| {
            hash.update("o");
            const names = try alloc.alloc([]const u8, object.count());
            defer alloc.free(names);
            var count: usize = 0;
            var it = object.iterator();
            while (it.next()) |item| {
                if (strip_catalog_fields and catalogField(item.key_ptr.*)) continue;
                names[count] = item.key_ptr.*;
                count += 1;
            }
            std.mem.sort([]const u8, names[0..count], {}, struct {
                fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.lessThan(u8, a, b);
                }
            }.lessThan);
            hashInt(hash, count);
            for (names[0..count]) |name| {
                hashBytes(hash, name);
                try hashValue(alloc, hash, object.get(name).?, false);
            }
        },
    }
}

test "graph retirement digest agrees across metadata and loaded owner serialization" {
    const alloc = std.testing.allocator;
    const metadata =
        \\{"other":{"type":"full_text"},"links":{"type":"graph","_index_incarnation":7,"edge_types":[{"name":"knows","weight":1.5}],"settings":{"b":2,"a":1}}}
    ;
    const loaded = [_]types.IndexConfig{.{ .name = "links", .kind = .graph, .coverage_generation = 7, .config_json = "{\"settings\":{\"a\":1,\"b\":2},\"edge_types\":[{\"weight\":1.5,\"name\":\"knows\"}]}" }};
    const expected = (try fromMetadata(alloc, metadata)).?;
    try std.testing.expectEqualDeep(expected, (try fromLoaded(alloc, &loaded)).?);
    var changed = loaded;
    changed[0].coverage_generation = 8;
    const changed_generation = (try fromLoaded(alloc, &changed)).?;
    try std.testing.expect(!std.mem.eql(u8, &expected, &changed_generation));
    changed = loaded;
    changed[0].config_json = "{\"settings\":{\"a\":1,\"b\":3},\"edge_types\":[{\"weight\":1.5,\"name\":\"knows\"}]}";
    const changed_config = (try fromLoaded(alloc, &changed)).?;
    try std.testing.expect(!std.mem.eql(u8, &expected, &changed_config));
    const legacy = "{\"links\":{\"type\":\"graph\",\"settings\":{\"a\":1}}}";
    const legacy_config = "{\"settings\":{\"a\":1}}";
    const legacy_loaded = [_]types.IndexConfig{.{ .name = "links", .kind = .graph, .coverage_generation = @import("../internal_keys.zig").derivedCoverageGeneration(legacy_config), .config_json = legacy_config }};
    try std.testing.expectEqualDeep((try fromMetadata(alloc, legacy)).?, (try fromLoaded(alloc, &legacy_loaded)).?);
}

test "graph retirement digest sorts indexes, ignores unrelated declarations, and binds both table IDs" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidTableIndexMetadata, fromMetadata(alloc, "{\"untyped\":{\"settings\":{}}}"));
    const first =
        \\{"z":{"type":"graph","_index_incarnation":11,"edge_types":[{"name":"z"}]},"text":{"type":"full_text"},"a":{"type":"graph","_index_incarnation":12,"edge_types":[{"name":"a"}]}}
    ;
    const reordered =
        \\{ "a" : { "edge_types" : [{ "name" : "a" }], "_index_incarnation" : 12, "type" : "graph" }, "z" : { "edge_types" : [{ "name" : "z" }], "type" : "graph", "_index_incarnation" : 11 } }
    ;
    const metadata_digest = (try fromMetadata(alloc, first)).?;
    try std.testing.expectEqualDeep(metadata_digest, (try fromMetadata(alloc, reordered)).?);
    // A status-only or failed-to-open owner has no loaded graph definition:
    // it cannot satisfy a metadata graph declaration with a null digest.
    try std.testing.expect((try fromLoaded(alloc, &.{})) == null);
    const loaded = [_]types.IndexConfig{
        .{ .name = "a", .kind = .graph, .coverage_generation = 12, .config_json = "{\"edge_types\":[{\"name\":\"a\"}]}" },
        .{ .name = "z", .kind = .graph, .coverage_generation = 11, .config_json = "{\"edge_types\":[{\"name\":\"z\"}]}" },
    };
    try std.testing.expectEqualDeep(metadata_digest, (try fromLoaded(alloc, &loaded)).?);
    const intended = retirementDigest(100, 200, metadata_digest);
    const other_source = retirementDigest(101, 200, metadata_digest);
    const other_target = retirementDigest(100, 201, metadata_digest);
    try std.testing.expect(!std.mem.eql(u8, &intended, &other_source));
    try std.testing.expect(!std.mem.eql(u8, &intended, &other_target));
    var missing = loaded;
    missing[1].kind = .full_text;
    const missing_digest = (try fromLoaded(alloc, &missing)).?;
    try std.testing.expect(!std.mem.eql(u8, &metadata_digest, &missing_digest));
}
