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

//! Lazy necessary-predicate probes over only text-competitive serving packs.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const predicates = @import("lake_index_text_predicate.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Self = @This();
budget: local.sql_memory_budget,
arena: std.heap.ArenaAllocator = undefined,
predicate: @import("lake_index_sql_rows.zig").Predicate = undefined,
root: corpus.Root,
resolver: predicates.PhysicalResolver,
cached: artifacts.CachedRead,
token: @import("antfly_cancellation").CancellationToken,
live: predicates.LiveRowsCache = .{},
probes: usize = 0,
rejected: usize = 0,
unknown: usize = 0,
pub fn create(root: corpus.Root, cached: artifacts.CachedRead, resolver: predicates.PhysicalResolver, json: []const u8, token: @import("antfly_cancellation").CancellationToken) !?*Self {
    if (root.source_routes_version != 1) return null;
    const self = try std.heap.page_allocator.create(Self);
    self.* = .{ .budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), .limit = 16 * 1024 * 1024 }, .root = root, .resolver = resolver, .cached = cached, .token = token };
    errdefer std.heap.page_allocator.destroy(self);
    self.arena = .init(self.budget.allocator());
    errdefer self.arena.deinit();
    var predicate = (try resolver.selectivePredicate(self.budget.allocator(), json, std.math.maxInt(usize))) orelse {
        self.arena.deinit();
        std.heap.page_allocator.destroy(self);
        return null;
    };
    errdefer predicate.deinit();
    // Tiny exact sets keep the complete compressed routing proof. Broader
    // sets never need a census before a selective text query can produce hits.
    if (predicate.estimatedRows() <= 4096 or !predicate.hasMembership()) {
        predicate.deinit();
        self.arena.deinit();
        std.heap.page_allocator.destroy(self);
        return null;
    }
    self.predicate = predicate;
    self.live.backing = self.budget.allocator();
    return self;
}
pub fn deinit(self: *Self) void {
    self.live.deinit(self.budget.allocator());
    self.predicate.deinit();
    self.arena.deinit();
    std.debug.assert(self.budget.live == 0);
    std.heap.page_allocator.destroy(self);
}
pub fn allows(raw: *anyopaque, id: [32]u8) !bool {
    const self: *Self = @ptrCast(@alignCast(raw));
    return self.testPack(id) catch |err| switch (err) {
        error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.QueryCandidateBudgetExceeded => {
            self.unknown += 1;
            return true;
        },
        else => return err,
    };
}
fn testPack(self: *Self, id: [32]u8) !bool {
    try self.cached.context.ensureActive();
    try self.token.check();
    self.probes += 1;
    _ = self.arena.reset(.free_all);
    const a = self.arena.allocator();
    const selected = try corpus.text_directory.loadSelected(a, a, self.root, self.resolver.store, self.cached, self.token, &.{id});
    for (selected.file_groups) |group| {
        if (group.sources.len == 0) {
            self.unknown += 1;
            return true;
        }
        for (group.sources) |span| {
            const first = try predicates.Identities.sourcePhysicalRow(a, &self.live, self.resolver.store, self.cached, span, span.file_lower, self.resolver.source.inventory.source_id, self.resolver.source.inventory.snapshot_id);
            const last = try predicates.Identities.sourcePhysicalRow(a, &self.live, self.resolver.store, self.cached, span, span.file_lower + span.count - 1, self.resolver.source.inventory.source_id, self.resolver.source.inventory.snapshot_id);
            const possible = (try self.predicate.intersectsPhysical(first, last)) orelse {
                self.unknown += 1;
                return true;
            };
            if (possible) return true;
        }
    }
    if (selected.file_groups.len == 0) return error.InvalidNativeLakeTextCorpus;
    self.rejected += 1;
    return false;
}
