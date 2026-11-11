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

//! Native retained-relation snapshot. One immutable source cut covers every
//! page and the initial replay cursor. A caller must bound session lifetime and
//! retain its native owner lease until close; no session pins source TTL.
const std = @import("std");
const series = @import("retained_series.zig");
const capture = @import("retained_series_capture.zig");
const control = @import("retained_series_control.zig");
const store = @import("docstore.zig");
const A = std.mem.Allocator;
pub const Query = @import("retained_series_read_contract.zig").Query;
pub const Request = @import("retained_series_read_contract.zig").Request;
pub const Session = struct {
    alloc: A,
    definition_arena: std.heap.ArenaAllocator,
    page_arena: std.heap.ArenaAllocator,
    txn: store.DocStore.Txn,
    spec: series.Spec,
    query: Query,
    root_incarnation: u128,
    expires_at_ns: u64,
    cursor: series.Cursor,
    after: ?[]u8 = null,
    done: bool = false,
    row_policy_lease: ?@import("db/row_policy_gate.zig").Gate.Lease = null,

    /// Ownership of txn transfers on invocation, including failure paths.
    pub fn open(a: A, txn_owned: store.DocStore.Txn, query: Query, root_incarnation: u128) !*Session {
        var txn = txn_owned;
        errdefer txn.abort();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const temp = arena.allocator();
        var retained_query = query;
        if (query.dimensions_json) |dimensions| retained_query.dimensions_json = try temp.dupe(u8, dimensions);
        const bindings = try capture.load(temp, &txn);
        const spec = for (bindings) |binding| {
            if (std.mem.eql(u8, &binding.spec.id, &query.fence.id) and binding.spec.generation == query.fence.generation) break binding.spec;
        } else return error.RetainedSeriesDefinitionMismatch;
        try control.requireFence(query.fence, spec);
        const engine = series.Engine{ .alloc = a, .spec = spec };
        const cursor = try engine.snapshotCursor(&txn);
        // Validate coverage, readiness and logical expiry even for empty reads.
        if (query.archive) |archive| {
            _ = try engine.scanArchive(temp, &txn, archive, null, 1);
        } else {
            _ = try engine.scanWhere(temp, &txn, query.start_ns, query.end_ns, query.admitted_ns, null, 1, query.dimensions_json, query.after);
        }
        const self = try a.create(Session);
        self.* = .{ .alloc = a, .definition_arena = arena, .page_arena = std.heap.ArenaAllocator.init(a), .txn = txn, .spec = spec, .query = retained_query, .root_incarnation = root_incarnation, .expires_at_ns = @import("antfly_platform").time.monotonicNs() +| (60 * std.time.ns_per_s), .cursor = cursor };
        return self;
    }
    /// Rows borrow the page arena until the next call. Errors close admission
    /// at the host; callers must not retry an ambiguously delivered page.
    pub fn next(self: *Session, limit: usize) !series.Page {
        if (@import("antfly_platform").time.monotonicNs() >= self.expires_at_ns) return error.RetainedSeriesSnapshotExpired;
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        if (limit == 0 or limit > series.max_changes) return error.RetainedSeriesBatchTooLarge;
        _ = self.page_arena.reset(.retain_capacity);
        if (self.done) return .{ .rows = &.{}, .after = null, .cursor = self.cursor };
        const engine = series.Engine{ .alloc = self.alloc, .spec = self.spec };
        const page = if (self.query.archive) |archive|
            try engine.scanArchive(self.page_arena.allocator(), &self.txn, archive, self.after, limit)
        else
            try engine.scanWhere(self.page_arena.allocator(), &self.txn, self.query.start_ns, self.query.end_ns, self.query.admitted_ns, self.after, limit, self.query.dimensions_json, self.query.after);
        if (!std.meta.eql(page.cursor, self.cursor)) return error.RetainedSeriesResetRequired;
        const after = if (page.after) |value| try self.alloc.dupe(u8, value) else null;
        if (self.after) |value| self.alloc.free(value);
        self.after = after;
        self.done = page.after == null;
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        return page;
    }
    pub fn close(self: *Session) void {
        const a = self.alloc;
        self.txn.abort();
        if (self.row_policy_lease) |*lease| lease.release();
        if (self.after) |value| a.free(value);
        self.page_arena.deinit();
        self.definition_arena.deinit();
        a.destroy(self);
    }
};
