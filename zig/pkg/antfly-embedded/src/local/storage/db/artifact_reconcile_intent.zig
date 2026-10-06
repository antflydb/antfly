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

const std = @import("std");
const inventory = @import("artifact_inventory.zig");
pub const key = "__metadata__:artifact_reconcile_intent";
pub const resolver_cursor_key = "__metadata__:artifact_reconcile_resolver_cursor";
pub const ResolverCursor = struct { token: [32]u8, name: []const u8, after: []const u8, complete: bool };
pub const Context = struct { token: [32]u8 };
pub const Intent = struct { token: [32]u8, command: inventory.Command, applied_index: u64 };

pub fn load(alloc: std.mem.Allocator, txn: anytype) !?std.json.Parsed(Intent) {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const value = std.json.parseFromSlice(Intent, alloc, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ArtifactCatalogCorrupt,
    };
    errdefer value.deinit();
    value.value.command.validate() catch return error.ArtifactCatalogCorrupt;
    if (value.value.applied_index == 0) return error.ArtifactCatalogCorrupt;
    return value;
}
pub fn requireAbsent(txn: anytype) !void {
    _ = txn.get(key) catch |err| {
        if (err == error.NotFound) return;
        return err;
    };
    return error.IntegrityTopologyBusy;
}
pub fn requireContext(alloc: std.mem.Allocator, txn: anytype, context: Context) !void {
    const value = try load(alloc, txn) orelse return error.ArtifactCatalogEpochChanged;
    defer value.deinit();
    if (!std.mem.eql(u8, &context.token, &value.value.token)) return error.ArtifactCatalogEpochChanged;
}
pub fn stage(alloc: std.mem.Allocator, txn: anytype, req: anytype, entry: anytype) !Context {
    const bytes = try std.json.Stringify.valueAlloc(alloc, .{ .request = req, .entry = .{ .term = entry.term, .index = entry.index } }, .{});
    defer alloc.free(bytes);
    var token: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &token, .{});
    const old = try load(alloc, txn);
    defer if (old) |value| value.deinit();
    if (old) |value| {
        if (!std.mem.eql(u8, &value.value.token, &token)) return error.ArtifactCatalogEpochChanged;
    } else {
        const encoded = try std.json.Stringify.valueAlloc(alloc, Intent{ .token = token, .command = req.artifact_catalog.?, .applied_index = entry.index }, .{});
        defer alloc.free(encoded);
        try txn.put(key, encoded);
    }
    return .{ .token = token };
}
pub fn clear(alloc: std.mem.Allocator, txn: anytype, command: inventory.Command, applied_index: u64) !void {
    if (!try validateCompletion(alloc, txn, command, applied_index)) return;
    try txn.delete(key);
    txn.delete(resolver_cursor_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}
pub fn validateCompletion(alloc: std.mem.Allocator, txn: anytype, command: inventory.Command, applied_index: u64) !bool {
    const value = try load(alloc, txn) orelse return false;
    defer value.deinit();
    if (value.value.applied_index != applied_index or !std.meta.eql(value.value.command.binding, command.binding)) return error.ArtifactCatalogEpochChanged;
    return true;
}

// The low-level catalog persistence seam has no ambient bypass authority.
// Under an intent it permits only strictly convergent changes: exact desired
// additions, unchanged entries, and removal/replacement of obsolete entries.
pub fn permitCatalog(alloc: std.mem.Allocator, txn: anytype, catalog_key: []const u8, candidate: []const u8) !bool {
    const value = try load(alloc, txn) orelse return false;
    defer value.deinit();
    const old = txn.get(catalog_key) catch |err| switch (err) {
        error.NotFound => "",
        else => return err,
    };
    if (std.mem.eql(u8, old, candidate)) return true;
    if (std.mem.eql(u8, catalog_key, inventory.index_key)) {
        const parser = @import("catalog/index_manager.zig");
        const types = @import("types.zig");
        const before = if (old.len == 0) try alloc.alloc(types.IndexConfig, 0) else try parser.deserializeCatalog(alloc, old);
        defer types.freeIndexConfigs(alloc, before);
        const after = if (candidate.len == 0) try alloc.alloc(types.IndexConfig, 0) else try parser.deserializeCatalog(alloc, candidate);
        defer types.freeIndexConfigs(alloc, after);
        const desired = if (value.value.command.catalogs.indexes.len == 0) try alloc.alloc(types.IndexConfig, 0) else try parser.deserializeCatalog(alloc, value.value.command.catalogs.indexes);
        defer types.freeIndexConfigs(alloc, desired);
        return converges(alloc, before, after, desired);
    }
    // Producer definitions use the same exact, monotonic permit as indexes.
    // In particular an ordinary caller cannot remove an already desired
    // producer while an admission is reconciling its dependent indexes.
    if (std.mem.eql(u8, catalog_key, inventory.enrichment_key)) {
        const parser = @import("catalog/enrichment_catalog.zig");
        const before = if (old.len == 0) try alloc.alloc(parser.EnrichmentConfig, 0) else try parser.deserializeCatalog(alloc, old);
        defer {
            for (before) |*entry| entry.deinit(alloc);
            alloc.free(before);
        }
        const after = if (candidate.len == 0) try alloc.alloc(parser.EnrichmentConfig, 0) else try parser.deserializeCatalog(alloc, candidate);
        defer {
            for (after) |*entry| entry.deinit(alloc);
            alloc.free(after);
        }
        const desired_bytes = value.value.command.catalogs.enrichments;
        const desired = if (desired_bytes.len == 0) try alloc.alloc(parser.EnrichmentConfig, 0) else try parser.deserializeCatalog(alloc, desired_bytes);
        defer {
            for (desired) |*entry| entry.deinit(alloc);
            alloc.free(desired);
        }
        return converges(alloc, before, after, desired);
    }
    if (std.mem.eql(u8, catalog_key, inventory.resolver_key)) {
        const parser = @import("catalog/resolver_catalog.zig");
        const before = try parser.deserializeCatalog(alloc, old);
        defer {
            for (before) |*entry| entry.deinit(alloc);
            alloc.free(before);
        }
        const after = try parser.deserializeCatalog(alloc, candidate);
        defer {
            for (after) |*entry| entry.deinit(alloc);
            alloc.free(after);
        }
        return converges(alloc, before, after, &.{});
    }
    return error.IntegrityTopologyBusy;
}
const DigestMap = std.StringHashMap([32]u8);
fn digests(alloc: std.mem.Allocator, entries: anytype) !DigestMap {
    var result = DigestMap.init(alloc);
    errdefer result.deinit();
    try result.ensureTotalCapacity(@intCast(entries.len));
    for (entries) |entry| {
        const bytes = try std.json.Stringify.valueAlloc(alloc, entry, .{});
        defer alloc.free(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const slot = try result.getOrPut(entry.name);
        if (slot.found_existing) return error.IntegrityTopologyBusy;
        slot.value_ptr.* = digest;
    }
    return result;
}
fn matches(map: *const DigestMap, name: []const u8, digest: [32]u8) bool {
    return if (map.get(name)) |found| std.mem.eql(u8, &found, &digest) else false;
}
fn converges(alloc: std.mem.Allocator, before: anytype, after: anytype, desired: []const std.meta.Child(@TypeOf(before))) !bool {
    var old = try digests(alloc, before);
    defer old.deinit();
    var next = try digests(alloc, after);
    defer next.deinit();
    var expected = try digests(alloc, desired);
    defer expected.deinit();
    var additions = next.iterator();
    while (additions.next()) |entry| if (!matches(&old, entry.key_ptr.*, entry.value_ptr.*) and !matches(&expected, entry.key_ptr.*, entry.value_ptr.*)) return error.IntegrityTopologyBusy;
    var removals = old.iterator();
    while (removals.next()) |entry| if (matches(&expected, entry.key_ptr.*, entry.value_ptr.*) and !matches(&next, entry.key_ptr.*, entry.value_ptr.*)) return error.IntegrityTopologyBusy;
    return true;
}

test "ordered artifact inventory producer permit preserves desired definitions and rejects unrelated replacements" {
    const Config = @import("catalog/enrichment_catalog.zig").EnrichmentConfig;
    const before = [_]Config{.{ .name = "model", .kind = .embedding, .source_field = "old" }};
    const desired = [_]Config{.{ .name = "model", .kind = .embedding, .source_field = "new" }};
    const unrelated = [_]Config{.{ .name = "model", .kind = .embedding, .source_field = "unrelated" }};
    try std.testing.expect(try converges(std.testing.allocator, @as([]const Config, &before), @as([]const Config, &desired), &desired));
    try std.testing.expectError(error.IntegrityTopologyBusy, converges(std.testing.allocator, @as([]const Config, &before), @as([]const Config, &unrelated), &desired));
    const empty: []const Config = &.{};
    try std.testing.expectError(error.IntegrityTopologyBusy, converges(std.testing.allocator, @as([]const Config, &desired), empty, &desired));
}
