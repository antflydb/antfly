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

//! One request-owned query program, frozen postings cut and bounded page of
//! pending candidates. Document sessions retain primary visibility and RLS.
const std = @import("std");
const Budget = @import("../../sql/memory_budget.zig");
const Cursor = @import("../../search/filter_cursor.zig").Cursor;
const query = @import("../../search/query.zig");
const index_manager = @import("catalog/index_manager.zig");
const public = @import("../../api/public_query_string.zig");
const search = @import("query/search_exec.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Owner = struct {
    backing: A,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    index_name: []const u8 = "",
    config_hash: u64 = 0,
    instance_id: u64 = 0,
    projection_revision: u64 = 0,
    query_digest: [32]u8 = @splat(0),
    filter: query.Filter = .match_none,
    cursor: ?*Cursor = null,
    pending: [64]u32 = undefined,
    count: usize = 0,
    position: usize = 0,
    exhausted: bool = false,
    page_sequence: u64 = 0,

    pub fn init(a: A, input: types.RelationalRowQuery.TextFilter) !*Owner {
        if (input.query.len == 0 or input.query.len > 64 * 1024 or input.memory_bytes == 0 or input.memory_bytes > 32 * 1024 * 1024) return error.InvalidRelationalRowsRequest;
        const self = try a.create(Owner);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = input.memory_bytes }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        return self;
    }
    /// The caller pins analysis while preparing; no primary/apply fence is
    /// needed for parsing, analyzer work or immutable query allocation.
    pub fn prepare(self: *Owner, entry: *index_manager.IndexManager.TextIndex, input: types.RelationalRowQuery.TextFilter) !void {
        if (!entry.coversPrimaryDocuments()) return error.UnsupportedSqlExecution;
        const a = self.arena.allocator();
        std.crypto.hash.sha2.Sha256.hash(input.query, &self.query_digest, .{});
        self.index_name = try a.dupe(u8, entry.config.name);
        self.config_hash = types.indexConfigHash(entry.config);
        self.instance_id = entry.instance_id;
        self.projection_revision = entry.projection_revision;
        if (input.config_hash) |expected| if (expected != self.config_hash) return error.PreparedGenerationChanged;
        var parsed = try public.parseFilterAlloc(a, input.query);
        defer parsed.deinit(a);
        const native = try public.filterToStatefulTextQueryAlloc(a, parsed.filter, 1);
        const normalized = try search.textQueryToSearchQuery(a, native, entry.text_analysis, entry.runtime_schema);
        const lowered = try @import("../../search/search.zig").searchQueryToFilterArena(a, normalized);
        self.filter = try public.cloneFilterAlloc(a, lowered);
    }
    pub fn matches(self: *const Owner, input: types.RelationalRowQuery.TextFilter) bool {
        if (self.cursor == null or self.budget.limit != input.memory_bytes) return false;
        if (input.index) |name| if (!std.mem.eql(u8, name, self.index_name)) return false;
        if (input.config_hash) |hash| if (hash != self.config_hash) return false;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(input.query, &digest, .{});
        return std.mem.eql(u8, &digest, &self.query_digest);
    }
    pub fn allocationError(self: *Owner, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.budget.isExhausted()) error.QueryCandidateBudgetExceeded else err;
    }
    pub fn deinit(self: *Owner) void {
        if (self.cursor) |cursor| cursor.deinit();
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
};
