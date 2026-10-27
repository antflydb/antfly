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

//! Request-local native corpus selection. All selected index snapshots
//! are acquired under one catalog cut, before any source-dependent shared filter.
//! This pointer is synchronous process-local state, never a wire credential.
const std = @import("std");
const types = @import("../types.zig");
const search = @import("search_exec.zig");
const catalog = @import("../catalog/index_manager.zig");
const A = std.mem.Allocator;
pub const Context = struct {
    ptr: *anyopaque,
    acquire: *const fn (*anyopaque, ?[]const u8) anyerror!?search.PinnedTextSource,
};
fn defaultScored(req: types.SearchRequest) bool {
    if (req.full_text_queries.len != 0) return false;
    if (req.full_text != null) return true;
    if (req.dense != null or req.sparse != null or req.dense_queries.len != 0 or req.sparse_queries.len != 0 or req.graph_queries.len != 0 or req.graph_metric_queries.len != 0) return false;
    return switch (req.query) {
        .dense_knn, .sparse_knn, .graph => false,
        else => true,
    };
}
fn count(req: types.SearchRequest) usize {
    return req.full_text_queries.len + @intFromBool(defaultScored(req)) + @intFromBool(req.filter_text != null or req.exclusion_text != null);
}
fn nameAt(req: types.SearchRequest, ordinal: usize) ?[]const u8 {
    if (ordinal < req.full_text_queries.len) return req.full_text_queries[ordinal].index_name;
    if (ordinal == req.full_text_queries.len and defaultScored(req)) return req.index_name;
    return req.primary_text_index_name orelse req.index_name;
}
pub const Plan = struct {
    allocator: A,
    sources: std.ArrayListUnmanaged(Selected) = .empty,
    default_name: ?[]const u8 = null,
    context: Context,
    const Selected = struct { entry: *catalog.IndexManager.TextIndex, name: []const u8, revision: u64, snapshot: *@import("../../../index.zig").IndexSnapshot };

    pub fn create(a: A, manager: *catalog.IndexManager, req: types.SearchRequest) !?*Plan {
        const selected_count = count(req);
        if (selected_count == 0 or !manager.source_ownership_possible.load(.acquire)) return null;
        manager.catalog_mutex.lockShared();
        defer manager.catalog_mutex.unlockShared();
        // Preserve the allocation-free legacy path. Reservation is monotone;
        // a first-publication race is rejected by the actual acquired corpus.
        const needed = for (0..selected_count) |i| {
            const entry = manager.textIndexEntry(nameAt(req, i)) orelse continue;
            if (entry.persistent.source_maintenance.load(.acquire) != null or (entry.persistent.read_only and entry.persistent.source_roots_present)) break true;
        } else false;
        if (!needed) return null;
        const self = try a.create(Plan);
        self.* = .{ .allocator = a, .context = .{ .ptr = self, .acquire = acquire } };
        errdefer self.deinit();
        try self.sources.ensureTotalCapacity(a, selected_count);
        for (0..selected_count) |i| {
            const selected_name = nameAt(req, i);
            const entry = manager.textIndexEntry(selected_name) orelse continue;
            if (selected_name == null) self.default_name = entry.config.name;
            const duplicate = for (self.sources.items) |source| {
                if (std.mem.eql(u8, source.name, entry.config.name)) break true;
            } else false;
            if (duplicate) continue;
            entry.lockAnalysisShared();
            defer entry.unlockAnalysisShared();
            self.sources.appendAssumeCapacity(.{ .entry = entry, .name = entry.config.name, .revision = entry.projection_revision, .snapshot = entry.persistent.acquireSnapshot() });
        }
        return self;
    }
    fn releaseEntry(raw: *anyopaque) void {
        const entry: *catalog.IndexManager.TextIndex = @ptrCast(@alignCast(raw));
        entry.unlockAnalysisShared();
    }
    fn acquire(raw: *anyopaque, name: ?[]const u8) !?search.PinnedTextSource {
        const self: *Plan = @ptrCast(@alignCast(raw));
        const selected = name orelse self.default_name orelse return null;
        for (self.sources.items) |source| if (std.mem.eql(u8, selected, source.name)) {
            const entry = source.entry;
            entry.lockAnalysisShared();
            errdefer entry.unlockAnalysisShared();
            // Non-empty declared configurations are immutable. A schema refresh
            // of an empty generation invalidates this cut; additive observed
            // analyzer discovery stays protected by the short ranking lease.
            if (entry.projection_revision != source.revision) return error.IndexRebuilding;
            return .{ .snapshot = source.snapshot.retain(), .name = source.name, .text_analysis = entry.text_analysis, .runtime_schema = entry.runtime_schema, .owner = entry, .release_owner = releaseEntry };
        };
        return null;
    }
    pub fn finishRanking(self: *Plan) void {
        for (self.sources.items) |source| source.snapshot.release();
        self.sources.clearRetainingCapacity();
        self.default_name = null;
    }
    pub fn deinit(self: *Plan) void {
        self.finishRanking();
        self.sources.deinit(self.allocator);
        self.allocator.destroy(self);
    }
};
