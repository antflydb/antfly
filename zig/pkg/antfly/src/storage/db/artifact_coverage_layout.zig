// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Prepared point-addressed coverage layout for an ordered producer epoch.
//! Old replica-local marker generations are never scanned into distributed
//! authority. An epoch starts empty and its ordered publications establish
//! coverage while the baseline cursor establishes the required obligations.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const inventory = @import("artifact_inventory.zig");
const epoch = @import("artifact_coverage_epoch.zig");

pub const Layout = struct {
    arena: std.heap.ArenaAllocator,
    entries: []Entry,
    const Entry = struct { prefix: []const u8, counters: [3][]const u8 };
    const outcomes = [_][]const u8{ "produced", "skipped", "terminal_failed" };

    pub fn prepare(alloc: std.mem.Allocator, catalogs: inventory.Catalogs, authority: @import("artifact_publication.zig").Authority) !Layout {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        if (catalogs.indexes.len == 0) return .{ .arena = arena, .entries = &.{} };
        const configs = try @import("catalog/index_manager.zig").deserializeCatalog(owned, catalogs.indexes);
        var entries: std.ArrayList(Entry) = .empty;
        for (configs) |config| {
            if (config.kind != .dense_vector and config.kind != .sparse_vector and config.kind != .graph and config.kind != .full_text) continue;
            const generation = keys.derivedCoverageGenerationForConfig(config.coverage_generation, config.config_json);
            var entry: Entry = .{ .prefix = try epoch.markerPrefix(owned, authority, config.name, generation), .counters = undefined };
            for (outcomes, 0..) |outcome, i| entry.counters[i] = try epoch.counter(owned, authority, config.name, generation, outcome);
            try entries.append(owned, entry);
        }
        return .{ .arena = arena, .entries = entries.items };
    }

    pub fn deinit(self: *Layout) void {
        self.arena.deinit();
    }

    pub fn initializeEmpty(self: *const Layout, txn: anytype) !bool {
        var complete = true;
        for (self.entries) |entry| {
            var counts: [3]?u64 = @splat(null);
            for (entry.counters, 0..) |key, i| {
                const raw = txn.get(key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                counts[i] = try keys.decodeDerivedCoverageOutcomeCount(raw);
            }
            if (counts[0] != null and counts[1] != null and counts[2] != null) continue;
            var cursor = try txn.openPhysicalCursorAdapter();
            const first = cursor.seekAtOrAfter(entry.prefix) catch |err| {
                cursor.close();
                return err;
            };
            const populated = if (first) |item| std.mem.startsWith(u8, item.key, entry.prefix) else false;
            cursor.close();
            if (populated) {
                complete = false;
                continue;
            }
            // Partial preexisting counters must agree with the empty range.
            for (counts) |count| if (count != null and count.? != 0) return error.InvalidDerivedCoverageCounter;
            const zero: [8]u8 = @splat(0);
            for (entry.counters, counts) |key, count| if (count == null) {
                try txn.put(key, &zero);
            };
        }
        return complete;
    }
};
