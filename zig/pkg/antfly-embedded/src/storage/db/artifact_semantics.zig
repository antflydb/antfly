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

//! Logical compatibility for artifact definitions. Eligibility is checked
//! separately against the negotiated effect protocol and producer readiness.
const std = @import("std");
const Hash = std.crypto.hash.sha2.Sha256;

fn frame(hash: *Hash, bytes: []const u8) void {
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, bytes.len, .little);
    hash.update(&size);
    hash.update(bytes);
}
fn number(data: []const u8, offset: *usize) !u32 {
    if (offset.* > data.len or data.len - offset.* < 4) return error.InvalidArtifactCatalogCommand;
    const result = std.mem.readInt(u32, data[offset.*..][0..4], .little);
    offset.* += 4;
    return result;
}
fn string(data: []const u8, offset: *usize) ![]const u8 {
    const len = try number(data, offset);
    if (len > data.len - offset.*) return error.InvalidArtifactCatalogCommand;
    const result = data[offset.*..][0..len];
    offset.* += len;
    return result;
}
fn canonical(hash: *Hash, alloc: std.mem.Allocator, value: std.json.Value) anyerror!void {
    frame(hash, @tagName(value));
    switch (value) {
        .object => |object| {
            const keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            for (object.keys(), 0..) |key, i| keys[i] = key;
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.lessThan(u8, a, b);
                }
            }.less);
            for (keys) |key| {
                frame(hash, key);
                try canonical(hash, alloc, object.get(key).?);
            }
        },
        .array => |array| for (array.items) |item| {
            try canonical(hash, alloc, item);
        },
        else => {
            const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(encoded);
            frame(hash, encoded);
        },
    }
    frame(hash, "end");
}
pub fn digest(alloc: std.mem.Allocator, indexes: []const u8, enrichments: []const u8, resolvers: []const u8) ![32]u8 {
    var hash = Hash.init(.{});
    frame(&hash, "antfly-row-derived-definitions-v1");
    if (indexes.len == 0) {
        try producerDefinitions(&hash, alloc, enrichments, resolvers);
        return hash.finalResult();
    }
    if (indexes.len < 12 or !std.mem.eql(u8, indexes[0..4], "AIDX")) return error.InvalidArtifactCatalogCommand;
    var offset: usize = 4;
    const version = try number(indexes, &offset);
    if (version != 1 and version != 2) return error.InvalidArtifactCatalogCommand;
    const count = try number(indexes, &offset);
    if (count > (indexes.len - offset) / 9) return error.InvalidArtifactCatalogCommand;
    const Entry = struct { name: []const u8, digest: [32]u8 };
    const entries = try alloc.alloc(Entry, count);
    defer alloc.free(entries);
    for (entries) |*entry| {
        const name = try string(indexes, &offset);
        if (offset >= indexes.len) return error.InvalidArtifactCatalogCommand;
        const kind = indexes[offset];
        offset += 1;
        const config = try string(indexes, &offset);
        if (version == 2) {
            if (indexes.len - offset < 8) return error.InvalidArtifactCatalogCommand;
            offset += 8; // Physical generation is deliberately owner-local.
        }
        var definition = Hash.init(.{});
        frame(&definition, name);
        frame(&definition, &.{kind});
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, config, .{});
        defer parsed.deinit();
        try canonical(&definition, alloc, parsed.value);
        entry.* = .{ .name = name, .digest = definition.finalResult() };
    }
    if (offset != indexes.len) return error.InvalidArtifactCatalogCommand;
    std.mem.sort(Entry, entries, {}, struct {
        fn less(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    for (entries, 0..) |entry, i| {
        if (i != 0 and std.mem.eql(u8, entries[i - 1].name, entry.name)) return error.InvalidArtifactCatalogCommand;
        frame(&hash, &entry.digest);
    }
    try producerDefinitions(&hash, alloc, enrichments, resolvers);
    return hash.finalResult();
}

fn producerDefinitions(hash: *Hash, alloc: std.mem.Allocator, enrichments: []const u8, resolvers: []const u8) !void {
    for ([_][]const u8{ enrichments, resolvers }, [_][]const u8{ "enrichments", "resolvers" }) |raw, family| {
        if (raw.len == 0) continue;
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidArtifactCatalogCommand;
        if (parsed.value.array.items.len == 0) continue; // Preserve main/v14 empty-family identity.
        const Entry = struct { name: []const u8, value: std.json.Value };
        const entries = try alloc.alloc(Entry, parsed.value.array.items.len);
        defer alloc.free(entries);
        for (parsed.value.array.items, entries) |value, *entry| {
            if (value != .object) return error.InvalidArtifactCatalogCommand;
            const name = value.object.get("name") orelse return error.InvalidArtifactCatalogCommand;
            if (name != .string or name.string.len == 0) return error.InvalidArtifactCatalogCommand;
            entry.* = .{ .name = name.string, .value = value };
        }
        std.mem.sort(Entry, entries, {}, struct {
            fn less(_: void, a: Entry, b: Entry) bool {
                return std.mem.lessThan(u8, a.name, b.name);
            }
        }.less);
        frame(hash, family);
        for (entries, 0..) |entry, index| {
            if (index != 0 and std.mem.eql(u8, entries[index - 1].name, entry.name)) return error.InvalidArtifactCatalogCommand;
            // Resolver config_generation is an explicit re-resolution input,
            // unlike owner-local index coverage generations; retain it.
            try canonical(hash, alloc, entry.value);
        }
        frame(hash, "end-producer-family");
    }
}

test "ordered artifact inventory semantic producer catalogs ignore order but bind generation and definition" {
    const alloc = std.testing.allocator;
    const first = "[{\"name\":\"b\",\"source_field\":\"body\"},{\"name\":\"a\",\"source_field\":\"title\"}]";
    const reordered = "[{\"source_field\":\"title\",\"name\":\"a\"},{\"source_field\":\"body\",\"name\":\"b\"}]";
    try std.testing.expectEqualDeep(try digest(alloc, "", first, ""), try digest(alloc, "", reordered, "[]"));
    try std.testing.expectEqualDeep(try digest(alloc, "", "", ""), try digest(alloc, "", "[]", "[]"));
    const one = try digest(alloc, "", "", "[{\"name\":\"resolver\",\"config_generation\":1}]");
    const two = try digest(alloc, "", "", "[{\"name\":\"resolver\",\"config_generation\":2}]");
    try std.testing.expect(!std.mem.eql(u8, &one, &two));
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, digest(alloc, "", "[{\"name\":\"a\"},{\"name\":\"a\"}]", ""));
}
