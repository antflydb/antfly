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

//! Lazy scalar source pinned to the query's native read transaction. Metadata
//! is an optimization only: missing/old certificates fall back to source reads.
const std = @import("std");
const projection = @import("../../api/field_projection.zig");
const sql = @import("../../sql/scalar.zig");
const logical = @import("../logical_source.zig");
const conditional = @import("../conditional_source.zig");
const query = @import("document_query.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const OperandPlans = struct {
    const View = @import("query/json_view.zig").View;
    const Selector = @import("query/json_projection_plan.zig");
    const Operand = struct { parts: []const []const u8, selector: *const Selector.Selector };
    paths: std.StringHashMapUnmanaged(Operand) = .empty,
    selector: *const Selector.Selector,
    read_set: []const []const u8 = &.{},
    full_read: bool = false,
    pub fn init(a: A, plan: *const projection.Plan) !OperandPlans {
        var paths: std.ArrayList(Selector.Path) = .empty;
        var self: OperandPlans = .{ .selector = undefined };
        var fields = plan.required_fields.keyIterator();
        while (fields.next()) |path| {
            var parts: std.ArrayList([]const u8) = .empty;
            var tokens = std.mem.splitScalar(u8, path.*, '.');
            while (tokens.next()) |part| try parts.append(a, part);
            try paths.append(a, .{ .parts = parts.items });
            try self.paths.put(a, path.*, .{ .parts = parts.items, .selector = try Selector.compile(a, &.{.{ .parts = parts.items }}) });
        }
        self.selector = try Selector.compile(a, paths.items);
        var read_set: std.ArrayList([]const u8) = .empty;
        for (plan.source_fields) |path| {
            if (!projection.validPath(path)) self.full_read = true;
            if (!hasPath(read_set.items, path)) try read_set.append(a, path);
        }
        for (plan.unconditional_fields) |path| if (!hasPath(read_set.items, path)) try read_set.append(a, path);
        self.read_set = read_set.items;
        return self;
    }
};
fn hasPath(paths: []const []const u8, path: []const u8) bool {
    for (paths) |value| if (std.mem.eql(u8, value, path)) return true;
    return false;
}
fn selectiveRoots(roots: []const []const u8) bool {
    return roots.len != 0 and roots.len <= 64 and for (roots) |path| {
        if (!projection.validPath(path)) break false;
    } else true;
}
/// Selected rows only. Ownership is bounded by a byte window, not page count;
/// unknown/large afterimages retain ordinary lazy per-row reads.
pub const Batch = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    rows: []logical.BorrowedRow = &.{},
    scope: ?@import("../backend_erased.zig").ReadScope = null,
    start: usize = 0,
    end: usize = 0,
    profile: bool = false,
    attempts: usize = 0,
    admitted: usize = 0,
    admitted_rows: usize = 0,
    planned_rows: usize = 0,
    preflight_rejects: usize = 0,
    pub fn init(a: A) Batch {
        return .{ .a = a, .arena = .init(a), .profile = @import("antfly_platform").env.getenvBool("ANTFLY_BENCH_SOURCE_BATCH") };
    }
    pub fn deinit(self: *Batch) void {
        if (self.profile) std.log.info("antfly_bench_source_batch attempts={d} admitted={d} rows={d} planned_rows={d} preflight_rejects={d}", .{ self.attempts, self.admitted, self.admitted_rows, self.planned_rows, self.preflight_rejects });
        if (self.scope) |*scope| scope.close();
        self.arena.deinit();
    }
    pub fn reset(self: *Batch) void {
        if (self.scope) |*scope| scope.close();
        self.scope = null;
        _ = self.arena.reset(.free_all);
        self.rows = &.{};
        self.start = 0;
        self.end = 0;
    }
    fn physicalWorkBytes(authority: conditional.Authority, full: bool, paths: []const []const u8) ?u64 {
        const proof = authority.source.proof orelse return null;
        if (!full) if (authority.source.directory) |directory| {
            const covered = for (paths) |path| {
                const field = directory.resolve(path) orelse {
                    if (directory.version < 2) break false;
                    continue;
                };
                if (directory.covered(field) == null) break false;
            } else true;
            if (covered) return 0;
        };
        return proof.bytes;
    }
    fn pairFits(authorities: []const conditional.Authority, full: bool, paths: []const []const u8) bool {
        var remaining: u64 = 1024 * 1024;
        for (authorities) |authority| {
            const bytes = physicalWorkBytes(authority, full, paths) orelse return false;
            if (bytes > remaining) return false;
            remaining -= bytes;
        }
        return true;
    }
    pub fn at(self: *Batch, txn: *@import("../docstore.zig").DocStore.Txn, index: usize, physical: []const []const u8, ids: []const []const u8, authorities: []const conditional.Authority, req: @import("types.zig").SearchRequest, operands: *const OperandPlans) !?logical.BorrowedRow {
        if (!req.include_all_fields and operands.read_set.len == 0) return null;
        if (index < self.end) return self.rows[index - self.start];
        self.reset();
        const full = req.include_all_fields or operands.full_read or operands.read_set.len > 64;
        const capacity = @min(ids.len - index, 64);
        if (capacity < 2) return null;
        if (self.profile) self.attempts += 1;
        const first_bytes = physicalWorkBytes(authorities[index], full, operands.read_set) orelse return null;
        // Capability metadata is available before opening a scope. Unsupported
        // and whole-value adapters keep the original cheap per-row fallback.
        if (first_bytes > 1024 * 1024 and
            (full or (if (txn.readScopeRangeMode()) |hint| hint != .selected_read else false)))
        {
            if (self.profile) self.preflight_rejects += 1;
            return null;
        }
        // A single admissible copied value cannot form a batch. Reject the
        // first two physical values before opening a scope or planning spans.
        // Unknown capabilities must defer this decision: selected_read may
        // legitimately admit small spans from much larger physical values.
        if (full or (if (txn.readScopeRangeMode()) |hint| hint != .selected_read else false)) {
            if (!pairFits(authorities[index..][0..2], full, operands.read_set)) {
                if (self.profile) self.preflight_rejects += 1;
                return null;
            }
        }
        const a = self.arena.allocator();
        var scope = try txn.openReadScope(a);
        var retained_scope = false;
        defer if (!retained_scope) scope.close();
        // Unsupported adapters may retain entire values behind range-shaped
        // APIs. Keep whole-document admission unless ownership is explicit.
        const mode = scope.rangeReadMode();
        if ((full or mode != .selected_read) and !pairFits(authorities[index..][0..2], full, operands.read_set)) {
            if (self.profile) self.preflight_rejects += 1;
            return null;
        }
        // A copied span still requires a whole-value read. Reject oversized
        // copy-backed batches before allocating/planning every remaining row.
        if (mode != .selected_read) {
            if (physicalWorkBytes(authorities[index], full, operands.read_set).? > 1024 * 1024) return null;
        }
        // Used synchronously by loadPrepared; plans/returned rows live in the
        // arena, while these bounded headers need no allocation or teardown.
        var selected: [64]logical.Authority = undefined;
        const selective = !full and mode != .full_value;
        var end = index;
        var bytes: usize = 0;
        var physical_bytes: usize = 0;
        var ranges: usize = 0;
        while (end - index < capacity) : (end += 1) {
            try @import("query/search_exec.zig").checkSearchRequestDeadline(req);
            var authority = authorities[end].source;
            const proof = authority.proof orelse break;
            const original_bytes = std.math.cast(usize, proof.bytes) orelse break;
            const physical_work = std.math.cast(usize, physicalWorkBytes(authorities[end], full, operands.read_set).?) orelse break;
            if (mode != .selected_read and physical_work > 1024 * 1024 -| physical_bytes) break;
            var count = original_bytes;
            var range_count: usize = 1;
            if (full) {
                authority.directory = null;
            } else if (selective) {
                if (authority.directory) |directory| {
                    if (proof.kind == @import("../internal_keys.zig").primary_kind and proof.bytes >= @import("../document_projection.zig").minimum_bytes) {
                        if (self.profile) self.planned_rows += 1;
                        const planned = try logical.Selection.init(a, directory, proof, operands.read_set);
                        if (planned) |selection| {
                            const cached = try a.create(logical.Selection);
                            cached.* = selection;
                            authority.selection = .{ .selected = cached };
                            count = selection.work_bytes;
                            range_count = selection.ranges.len;
                        } else authority.selection = .full;
                    }
                }
            }
            if (count > 1024 * 1024 -| bytes or range_count > 256 -| ranges) break;
            bytes += count;
            physical_bytes +|= physical_work;
            ranges += range_count;
            selected[end - index] = authority;
        }
        if (end - index < 2) return null;
        var checked: CheckedScope = .{ .upstream = &scope, .mode = mode, .request = &req };
        self.rows = try logical.loadPreparedViews(a, a, &checked, physical[index..end], ids[index..end], if (full) &.{} else operands.read_set, selected[0 .. end - index]);
        self.scope = scope;
        retained_scope = true;
        self.start = index;
        self.end = end;
        if (self.profile) {
            self.admitted += 1;
            self.admitted_rows += end - index;
        }
        return self.rows[0];
    }
};
// Full-value cursor reads can be arbitrarily larger than the copied spans.
// Check between those reads; native selected I/O keeps its bounded batch walk.
const CheckedScope = struct {
    upstream: *@import("../backend_erased.zig").ReadScope,
    mode: @import("../backend_erased.zig").RangeReadMode,
    request: *const @import("types.zig").SearchRequest,
    pub fn getManySorted(self: *@This(), keys: []const []const u8, output: []?[]const u8) !void {
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.request.*);
        try self.upstream.getManySorted(keys, output);
    }
    pub fn getRangesMany(self: *@This(), requests: []const @import("../value_ranges.zig").Request) !void {
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.request.*);
        const Poll = struct {
            fn check(raw: ?*const anyopaque) callconv(.c) u8 {
                const req: *const @import("types.zig").SearchRequest = @ptrCast(@alignCast(raw orelse return 3));
                @import("query/search_exec.zig").checkSearchRequestDeadline(req.*) catch |err| return switch (err) {
                    error.Cancelled => 1,
                    error.Timeout => 2,
                };
                return 0;
            }
        };
        return self.upstream.getRangesManyControlled(requests, .{ .ptr = self.request, .poll = Poll.check });
    }
};

/// Opt-in aggregate attribution; never includes document identifiers or data.
pub const NativeViewProfile = struct {
    enabled: bool = false,
    rows: usize = 0,
    requested: usize = 0,
    authority: usize = 0,
    full: usize = 0,
    bytes_match: usize = 0,
    selected: usize = 0,
    members: usize = 0,
    witnesses: usize = 0,
    pub fn observe(self: *@This(), row: Row, receipt: ?*const projection.Receipt) void {
        if (!self.enabled) return;
        self.rows += 1;
        self.requested += @intFromBool(row.req.field_projection_value_views);
        self.authority += @intFromBool(row.authority.source.directory != null);
        self.full += @intFromBool(row.full_bytes != null);
        self.selected += @intFromBool(row.selected_view != null);
        if (row.full_bytes) |bytes| if (row.authority.source.proof) |proof| {
            self.bytes_match += @intFromBool(bytes.len == proof.bytes);
        };
        if (receipt) |owned| {
            self.members += owned.members.len;
            for (owned.members) |member| if (member.proof != null) {
                self.witnesses += 1;
            };
        }
    }
    pub fn finish(self: @This()) void {
        if (self.enabled) std.log.info("antfly_bench_field_view rows={d} requested={d} authority={d} full={d} bytes_match={d} selected={d} members={d} witnesses={d}", .{ self.rows, self.requested, self.authority, self.full, self.bytes_match, self.selected, self.members, self.witnesses });
    }
};
pub const Row = struct {
    a: A,
    txn: ?*@import("../docstore.zig").DocStore.Txn,
    physical: []const u8,
    id: []const u8,
    authority: conditional.Authority,
    req: @import("types.zig").SearchRequest,
    public_plan: ?*const query.PublicPlan = null,
    operands: ?*const OperandPlans = null,
    raw_sources: std.StringHashMapUnmanaged([]const u8) = .empty,
    raw_operands: std.StringHashMapUnmanaged([]const u8) = .empty,
    selected_view: ?logical.SelectedView = null,
    full_view: ?OperandPlans.View = null,
    scope: ?@import("../backend_erased.zig").ReadScope = null,
    prefetched: ?[]const u8 = null,
    base_prefetched: bool = false,
    prefetched_view: ?OperandPlans.View = null,
    prefetched_root: ?V = null,
    values: std.StringHashMapUnmanaged(V) = .empty,
    /// Exact authenticated operands, distinct from containing-object roots.
    /// Repeated value/identity expressions share one parse per row.
    token_values: std.StringHashMapUnmanaged(V) = .empty,
    identities: std.StringHashMapUnmanaged(sql.Datum) = .empty,
    metadata_hits: usize = 0,
    certificate_hits: usize = 0,
    value_reads: usize = 0,
    full_bytes: ?[]const u8 = null,
    full_root: ?V = null,
    pub fn source(self: *Row) projection.Source {
        return .{ .context = self, .read = read, .read_raw = readRaw, .read_normalized = readNormalized, .value_proof = valueProof, .prefetch = prefetch };
    }
    pub fn deinit(self: *Row) void {
        if (self.scope) |*scope| scope.close();
        self.scope = null;
    }
    fn prefetchedPath(self: *Row, path: []const u8) bool {
        const bytes = self.raw_sources.get(path) orelse return false;
        const cached = self.prefetched orelse return false;
        return bytes.ptr == cached.ptr and bytes.len == cached.len;
    }
    pub fn attach(self: *Row, prepared: logical.BorrowedRow) void {
        if (prepared.selected) |view| {
            self.selected_view = view;
            return;
        }
        if (!prepared.projected) self.full_bytes = prepared.bytes else {
            self.prefetched = prepared.bytes;
            self.base_prefetched = true;
        }
    }
    fn prefetch(raw: *anyopaque, paths: []const []const u8) !void {
        const self: *Row = @ptrCast(@alignCast(raw));
        if (self.full_bytes != null or self.selected_view != null) return;
        const read_set = if (self.operands) |operands| operands.read_set else paths;
        if (!self.req.include_all_fields and read_set.len == 0) return;
        const full = self.req.include_all_fields or (if (self.operands) |operands| operands.full_read else false);
        if (self.prefetched == null) {
            // Acquire an authenticated selection or a full-value loan. A
            // prefetch must not index/parse a full fallback just to discard the
            // first token; decoded consumers need one ordinary root parse.
            try self.acquire(if (full) &.{} else read_set);
            return;
        }
        const bytes = self.prefetched.?;
        if (self.full_bytes != null) return;
        self.prefetched = bytes;
        self.base_prefetched = self.operands != null;
        for (read_set) |path| try self.raw_sources.put(self.a, path, bytes);
    }
    fn load(self: *Row, roots: []const []const u8) ![]const u8 {
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (self.full_bytes) |bytes| return bytes;
        if (selectiveRoots(roots)) if (self.selected_view) |view| if (view.serves(roots)) {
            return view.materialize(self.a);
        };
        try self.acquire(roots);
        if (self.full_bytes) |bytes| return bytes;
        if (self.selected_view) |view| return view.materialize(self.a);
        return error.DocumentNotFound;
    }
    /// Populate a selected view/full loan without constructing JSON or an
    /// operand index. The eventual consumer decides whether bytes or Datum is
    /// needed; full decoded values must not pay for both representations.
    fn acquire(self: *Row, roots: []const []const u8) !void {
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (self.full_bytes != null) return;
        const selective = selectiveRoots(roots);
        if (selective) if (self.selected_view) |view| if (view.serves(roots)) return;
        if (self.scope == null) self.scope = try (self.txn orelse return error.DocumentNotFound).openReadScope(self.a);
        var authority = self.authority.source;
        if (!selective) authority.directory = null;
        const rows = try logical.loadPreparedViews(self.a, self.a, &self.scope.?, &.{self.physical}, &.{self.id}, if (selective) roots else &.{}, &.{authority});
        self.value_reads += 1;
        if (rows[0].selected) |view| {
            self.selected_view = view;
            return;
        }
        const bytes = rows[0].bytes orelse return error.DocumentNotFound;
        if (!rows[0].projected) self.full_bytes = bytes;
        if (rows[0].projected) {
            self.prefetched = bytes;
            self.base_prefetched = self.operands != null;
        }
    }
    /// Exact selected tokens can be consumed directly. A full fallback is
    /// deliberately left unindexed here: typed reads parse its root once.
    fn selectedToken(self: *Row, path: []const u8) !?[]const u8 {
        return self.selectedTokenMode(path, true);
    }
    fn selectedTokenMode(self: *Row, path: []const u8, leaf_only: bool) !?[]const u8 {
        if (self.raw_operands.get(path)) |value| return value;
        if (!std.mem.eql(u8, path, "_id") and projection.validPath(path)) {
            if (self.full_bytes == null and self.prefetched == null and self.txn != null and (if (self.selected_view) |view| !view.serves(&.{path}) else true)) {
                const operands = self.operands;
                const read_set: []const []const u8 = if (self.selected_view == null and operands != null and operands.?.read_set.len != 0) operands.?.read_set else &.{path};
                if (selectiveRoots(read_set)) {
                    try self.acquire(read_set);
                }
            }
            if (self.selected_view) |view| if (view.raw(path)) |bytes| {
                try self.raw_operands.put(self.a, path, bytes);
                return bytes;
            };
            if (try self.fullToken(path, leaf_only)) |bytes| return bytes;
        }
        return null;
    }
    /// Only a native snapshot and its directory-bound canonical certificate
    /// qualify extents without another payload scan. Typed leaf consumers may
    /// avoid unrelated roots; raw structured output scans just its own token
    /// in the ordinary normalizer, rather than indexing the complete source.
    fn fullToken(self: *Row, path: []const u8, leaf_only: bool) !?[]const u8 {
        if (std.mem.eql(u8, path, "_id") or !projection.validPath(path)) return null;
        const full = self.full_bytes orelse return null;
        const proof = self.authority.source.proof orelse return null;
        const directory = self.authority.source.directory orelse return null;
        const certificate = self.authority.certificate orelse return null;
        if (proof.kind != @import("../internal_keys.zig").primary_kind or full.len != proof.bytes) return null;
        const field = directory.resolveExact(path) orelse return null;
        if (leaf_only) {
            if (field.kind != .leaf or certificate.find(field.index) == null) return null;
            // Structured ancestor operands share the containing-root parse.
            if (self.operands) |operands| {
                var end = path.len;
                while (std.mem.lastIndexOfScalar(u8, path[0..end], '.')) |dot| {
                    if (operands.paths.contains(path[0..dot])) return null;
                    end = dot;
                }
            }
        }
        const offset = std.math.cast(usize, field.offset) orelse return null;
        const len = std.math.cast(usize, field.length) orelse return null;
        if (offset > full.len or len > full.len - offset) return null;
        const bytes = full[offset..][0..len];
        try self.raw_operands.put(self.a, path, bytes);
        return bytes;
    }
    fn readRaw(raw: *anyopaque, _: A, path: []const u8) !?[]const u8 {
        const self: *Row = @ptrCast(@alignCast(raw));
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        // Raw output needs canonical bytes, not the typed reader's shared
        // containing-root decision. Acquire and qualify its token once.
        if (try self.selectedTokenMode(path, false)) |bytes| return bytes;
        // A prior operator may already have materialized this source. Reuse
        // that value rather than building a second index for small/mixed rows.
        if (self.full_root != null or self.values.contains(path) or (self.prefetched_root != null and self.prefetchedPath(path))) return null;
        const operands = self.operands orelse return null;
        const operand = operands.paths.get(path) orelse return null;
        if (std.mem.eql(u8, path, "_id")) return null;
        const bytes = self.raw_sources.get(path) orelse loaded: {
            const bytes = try self.load(&.{path});
            try self.raw_sources.put(self.a, path, bytes);
            break :loaded bytes;
        };
        var partial = OperandPlans.View.init(self.a, bytes);
        const view = if (self.full_bytes != null) full: {
            if (self.full_view == null) {
                var prepared = OperandPlans.View.init(self.a, self.full_bytes.?);
                try prepared.prepareProjectionWithOptions(operands.selector, .all, .object_only, .{ .defer_fanout = true });
                self.full_view = prepared;
            }
            break :full &self.full_view.?;
        } else if (self.prefetchedPath(path)) prefetched: {
            if (self.prefetched_view == null) {
                var prepared = OperandPlans.View.init(self.a, self.prefetched.?);
                try prepared.prepareProjectionWithOptions(operands.selector, .all, .object_only, .{ .defer_fanout = true });
                self.prefetched_view = prepared;
            }
            break :prefetched &self.prefetched_view.?;
        } else projected: {
            try partial.prepareProjectionWithOptions(operand.selector, .all, .object_only, .{ .defer_fanout = true });
            break :projected &partial;
        };
        var node = &view.root;
        for (operand.parts) |part| {
            node = switch (node.kind()) {
                '{' => (try view.field(node, part)) orelse return null,
                '[' => if (OperandPlans.Selector.index(part)) |index| (try view.element(node, index)) orelse return null else return null,
                else => return null,
            };
        }
        try self.raw_operands.put(self.a, path, node.raw);
        return node.raw;
    }
    fn valueProof(raw: *anyopaque, path: []const u8, bytes: []const u8) ?projection.ValueProof {
        const self: *Row = @ptrCast(@alignCast(raw));
        if (self.selected_view) |view| if (view.proof(path, bytes)) |proven| {
            return .{ .digest = proven.digest, .encoded_length = proven.encoded_length };
        };
        // A full value and its metadata come from the same authenticated,
        // pinned transaction. Bind the actual strict node extent, not a
        // caller's identity claim or an independently fetched directory.
        const full = self.full_bytes orelse return null;
        const authority = self.authority.source;
        const proof = authority.proof orelse return null;
        if (full.len != proof.bytes) return null;
        const directory = authority.directory orelse return null;
        const field = directory.resolveExact(path) orelse return null;
        const offset = std.math.cast(usize, field.offset) orelse return null;
        const len = std.math.cast(usize, field.length) orelse return null;
        if (offset > full.len or len > full.len - offset) return null;
        const actual = full[offset..][0..len];
        if (actual.ptr != bytes.ptr or actual.len != bytes.len) return null;
        return .{ .digest = field.digest, .encoded_length = bytes.len };
    }
    fn readNormalized(raw: *anyopaque, path: []const u8) !?sql.NormalizedOutput {
        const self: *Row = @ptrCast(@alignCast(raw));
        // readRaw establishes strict immutable node extents with the row's
        // authenticated view. Strings contain no numeric descendants and the
        // output contract preserves their escape spelling, so no second scan
        // is needed. Numeric/composite values retain the streaming normalizer.
        const bytes = (try readRaw(self, self.a, path)) orelse return null;
        if (bytes.len == 0) return null;
        if (bytes[0] == '"' or std.mem.eql(u8, bytes, "true") or std.mem.eql(u8, bytes, "false") or std.mem.eql(u8, bytes, "null"))
            return .{ .bytes = bytes, .original = bytes, .nodes = 1, .height = 1 };
        return null;
    }
    fn read(raw: *anyopaque, a: A, path: []const u8, kind: sql.ColumnRead) anyerror!sql.Datum {
        const self: *Row = @ptrCast(@alignCast(raw));
        try @import("query/search_exec.zig").checkSearchRequestDeadline(self.req);
        if (std.mem.eql(u8, path, "_id")) {
            var source_value: projection.JsonSource = .{ .id = self.id, .root = .null };
            return source_value.source().read(&source_value, a, path, kind);
        }
        if (kind == .identity) if (self.identities.get(path)) |identity| return identity;
        if (kind != .value) if (self.authority.source.directory) |directory| {
            if (directory.resolveExact(path)) |field| {
                // Opaque subtrees do not prove that a descendant exists.
                {
                    if (kind == .missing) {
                        self.metadata_hits += 1;
                        return sql.Datum.json(.{ .bool = false });
                    }
                    if (field.kind != .object) if (self.authority.certificate) |certificate| if (certificate.find(field.index)) |digest| {
                        const value = sql.Datum.json(.{ .string = try std.fmt.allocPrint(a, "sv1-{s}", .{std.fmt.bytesToHex(digest, .lower)}) });
                        try self.identities.put(self.a, path, value);
                        self.metadata_hits += 1;
                        self.certificate_hits += 1;
                        return value;
                    };
                }
            } else if (directory.version >= 2 and directory.resolve(path) == null) {
                self.metadata_hits += 1;
                return if (kind == .missing) sql.Datum.json(.{ .bool = true }) else .{};
            }
        };
        if (kind == .missing) if (try readRaw(self, a, path)) |_| return sql.Datum.json(.{ .bool = false });
        if (kind != .missing and self.full_root == null and !self.values.contains(path) and !(self.prefetched_root != null and self.prefetchedPath(path))) if (try self.selectedToken(path)) |bytes| {
            const value = self.token_values.get(path) orelse parsed: {
                const value = try @import("antfly-json").parseFromSliceLeaky(V, self.a, bytes, .{ .allocate = .alloc_if_needed });
                try self.token_values.put(self.a, path, value);
                break :parsed value;
            };
            if (kind == .identity) {
                const identity = sql.Datum.json(.{ .string = try self.a.dupe(u8, &(try @import("../../common/value_identity.zig").identity(value))) });
                try self.identities.put(self.a, path, identity);
                return identity;
            }
            return sql.Datum.json(value);
        };
        const root = self.full_root orelse self.values.get(path) orelse (if (self.prefetchedPath(path)) self.prefetched_root else null) orelse loaded: {
            const bytes = self.raw_sources.get(path) orelse try self.load(&.{path});
            // The authenticated range loader preserves opaque array positions.
            // Parse that immutable row view directly: re-masking compacts array
            // indexes, and copying/encoding it adds work proportional to payload.
            const value = try @import("antfly-json").parseFromSliceLeaky(V, self.a, bytes, .{ .allocate = .alloc_if_needed });
            if (self.full_bytes != null) self.full_root = value else if (self.prefetchedPath(path)) self.prefetched_root = value else try self.values.put(self.a, path, value);
            break :loaded value;
        };
        var source_value: projection.JsonSource = .{ .id = self.id, .root = root };
        return source_value.source().read(&source_value, a, path, kind);
    }
    /// Public output can reuse a small checked mask even after scalar
    /// evaluation needed the complete source. Keep this separate from load():
    /// decoded consumers must distinguish their whole-root cache from masks.
    fn publicSourceBytes(self: *Row) ![]const u8 {
        const roots = self.req.fields;
        if (selectiveRoots(roots)) {
            const prepared: ?logical.SelectedView = if (self.selected_view) |view| if (view.serves(roots)) view else null else null;
            if (prepared) |view| {
                var bytes: usize = 0;
                for (view.values) |value| bytes +|= value.len;
                if (bytes <= @import("../document_projection.zig").covering_max_bytes) return view.materializeMasked(self.a, roots);
            }
            if (self.authority.source.proof) |proof| if (self.authority.source.directory) |directory| {
                if (directory.version >= 3 and proof.kind == @import("../internal_keys.zig").primary_kind) covered: {
                    for (roots) |path| {
                        const field = directory.resolve(path) orelse continue;
                        if (directory.covered(field) == null) break :covered;
                    }
                    const selection = (try logical.Selection.init(self.a, directory, proof, roots)) orelse break :covered;
                    defer self.a.free(selection.fields);
                    defer self.a.free(selection.ranges);
                    if (selection.ranges.len != 0) break :covered;
                    const values = try self.a.alloc([]const u8, selection.fields.len);
                    defer self.a.free(values);
                    for (selection.fields, values) |field, *value| {
                        value.* = directory.covered(field) orelse return error.InvalidDocumentContentAuthority;
                        var digest: [32]u8 = undefined;
                        std.crypto.hash.sha2.Sha256.hash(value.*, &digest, .{});
                        if (!std.mem.eql(u8, &digest, &field.digest)) return error.InvalidDocumentContentAuthority;
                    }
                    const view: logical.SelectedView = .{ .directory = directory, .fields = selection.fields, .values = values };
                    // This selection was built solely for the public roots;
                    // filtering it again only repeats planning/allocations.
                    return view.materialize(self.a);
                }
            };
            // Covering can avoid copying a large selected scalar that the
            // public mask does not need. Otherwise reuse the existing view.
            if (prepared) |view| return view.materializeMasked(self.a, roots);
        }
        if (self.base_prefetched) return self.prefetched.?;
        return self.load(if (self.req.include_all_fields) &.{} else self.req.fields);
    }
    pub fn base(self: *Row, output: A) ![]u8 {
        if (!self.req.include_all_fields and self.req.fields.len == 0) return output.dupe(u8, "{}");
        const bytes = try self.publicSourceBytes();
        if (self.public_plan) |plan| return (try plan.apply(self.a, output, bytes)).json;
        var plan = try query.PublicPlan.init(self.a, .{ .fields = self.req.fields, .include_all_fields = self.req.include_all_fields });
        defer plan.deinit();
        return (try plan.apply(self.a, output, bytes)).json;
    }
};

test "api query contract native lazy source uses nested leaf identities without opening payloads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const directory = @import("../document_projection.zig");
    const fields = [_]directory.Field{
        .{ .name = "body", .kind = .object, .offset = 0, .length = 0, .digest = @splat(0), .index = 0, .first_child = 1, .child_count = 1 },
        .{ .name = "image", .kind = .leaf, .offset = 0, .length = 0, .digest = @splat(0), .index = 1, .parent = 0 },
    };
    const identity = try @import("../../common/value_identity.zig").digestValue(.{ .string = "image" });
    var record: [34]u8 = undefined;
    std.mem.writeInt(u16, record[0..2], 1, .little);
    record[2..].* = identity;
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{
        .source = .{ .directory = .{ .fields = &fields, .root_count = 1, .version = 2 } },
        .certificate = .{ .records = &record },
    } };
    const token = try row.source().read(&row, a, "body.image", .identity);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(a, "sv1-{s}", .{std.fmt.bytesToHex(identity, .lower)}), token.value.string);
    const present = try row.source().read(&row, a, "body.image", .missing);
    try std.testing.expect(!present.value.bool);
    const absent = try row.source().read(&row, a, "body.absent", .missing);
    try std.testing.expect(absent.value.bool);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
}

test "api query contract native full source fallback retains hidden null and sibling operands" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .full_bytes = "{\"body\":{\"image\":\"large\",\"null\":null,\"integer\":9007199254740993}}" };
    _ = try row.source().read(&row, a, "body.image", .identity);
    const token = try row.source().read(&row, a, "body.null", .identity);
    try std.testing.expectEqualStrings(&(try @import("../../common/value_identity.zig").identity(.null)), token.value.string);
    const number = try row.source().read(&row, a, "body.integer", .value);
    try std.testing.expectEqual(@as(i64, 9007199254740993), number.value.integer);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
}

test "api query contract native scalar base preserves indexed array selection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "{\"arr\":[{\"n\":1},{\"n\":2}]}",
        "{\"arr\":[{\"n\":1},{\"n\":2}],\"metadata\":{\"_type\":\"image\"},\"padding\":\"small\"}",
    }) |bytes| {
        var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{ .fields = &.{"arr.1.n"}, .include_all_fields = false }, .authority = .{}, .full_bytes = bytes };
        try std.testing.expectEqualStrings("{\"arr\":[{\"n\":2}]}", try row.base(a));
    }
}

test "api query contract opaque identity proves exact path without certifying descendants" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const directory = @import("../document_projection.zig");
    const fields = [_]directory.Field{.{ .name = "items", .kind = .subtree, .offset = 0, .length = 0, .digest = @splat(0), .index = 0 }};
    const digest = try @import("../../common/value_identity.zig").digestValue(.null);
    var record: [34]u8 = undefined;
    std.mem.writeInt(u16, record[0..2], 0, .little);
    record[2..].* = digest;
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{
        .source = .{ .directory = .{ .fields = &fields, .root_count = 1, .version = 2 } },
        .certificate = .{ .records = &record },
    } };
    _ = try row.source().read(&row, a, "items", .identity);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
    try std.testing.expectError(error.DocumentNotFound, row.source().read(&row, a, "items.child", .missing));
}

test "api query contract raw full fallback shares one row view and memoized operands" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"first","expression":{"field":"body.first"}},
        \\  {"name":"second","expression":{"field":"body.second"}} ]
    , .{});
    var plan = try projection.Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const operands = try OperandPlans.init(a, &plan);
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .operands = &operands, .full_bytes = "{\"body\":{\"first\":[1,null],\"second\":\"two\"},\"unused\":{\"large\":[0,1,2]}}" };
    const source = row.source();
    const first = (try source.read_raw.?(source.context, a, "body.first")).?;
    try std.testing.expectEqualStrings("[1,null]", first);
    try std.testing.expectEqualStrings("\"two\"", (try source.read_raw.?(source.context, a, "body.second")).?);
    const again = (try source.read_raw.?(source.context, a, "body.first")).?;
    try std.testing.expect(first.ptr == again.ptr);
    try std.testing.expectEqual(@as(usize, 2), row.raw_operands.count());
    try std.testing.expectEqual(@as(usize, 0), row.full_view.?.materialized_values);
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
    var typed: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .operands = &operands, .full_root = try std.json.parseFromSliceLeaky(V, a, row.full_bytes.?, .{}) };
    const typed_source = typed.source();
    try std.testing.expect(try typed_source.read_raw.?(typed_source.context, a, "body.first") == null);
    try std.testing.expectEqual(@as(usize, 2), (try typed_source.read(typed_source.context, a, "body.first", .value)).value.array.items.len);
    try std.testing.expect(typed.full_view == null);
}

test "api query contract native scalar receipts require strict nodes and retain numeric normalization" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a,
        \\[ {"name":"s","expression":{"field":"s"}}, {"name":"n","expression":{"field":"n"}}, {"name":"o","expression":{"field":"o"}} ]
    , .{});
    var plan = try projection.Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    const operands = try OperandPlans.init(a, &plan);
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .authority = .{}, .req = .{}, .operands = &operands, .full_bytes = "{\"s\":\"café λ \\uD83D\\uDE80\\n\",\"n\":1e-400,\"o\":{\"x\":1e-400}}" };
    const source = row.source();
    const value = (try source.read_normalized.?(source.context, "s")).?;
    try std.testing.expectEqual(@as(usize, 1), value.nodes);
    try std.testing.expectEqual(@as(usize, 1), value.height);
    try std.testing.expect(value.bytes.ptr == (try source.read_raw.?(source.context, a, "s")).?.ptr);
    try std.testing.expect(try source.read_normalized.?(source.context, "n") == null);
    try std.testing.expect(try source.read_normalized.?(source.context, "o") == null);
    for ([_][]const u8{ "{\"s\":\"unterminated}", "{\"s\":\"a\",\"s\":\"b\"}" }) |bad| {
        var invalid: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .authority = .{}, .req = .{}, .operands = &operands, .full_bytes = bad };
        const invalid_source = invalid.source();
        const proof = invalid_source.read_normalized.?(invalid_source.context, "s") catch null;
        try std.testing.expect(proof == null);
    }
}

test "api query contract selected copy windows cancel between whole value reads" {
    const backend = @import("../backend_erased.zig");
    const Fake = struct {
        signal: *std.atomic.Value(bool),
        calls: *usize,
        count: *usize,
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.UnexpectedFullRead;
        }
        pub fn getRangesMany(self: *@This(), requests: []const @import("../value_ranges.zig").Request) !void {
            self.calls.* += 1;
            self.count.* = requests.len;
            self.signal.store(true, .release);
        }
        pub fn close(_: *@This()) void {}
    };
    var signal = std.atomic.Value(bool).init(false);
    var calls: usize = 0;
    var count: usize = 0;
    var upstream = try backend.readScopeFrom(std.testing.allocator, Fake{ .signal = &signal, .calls = &calls, .count = &count });
    defer upstream.close();
    try std.testing.expectEqual(backend.RangeReadMode.full_value, upstream.rangeReadMode());
    const request: @import("types.zig").SearchRequest = .{ .cancellation = @import("types.zig").CancellationToken.fromAtomic(&signal) };
    var checked: CheckedScope = .{ .upstream = &upstream, .mode = .selected_copy, .request = &request };
    const requests = [_]@import("../value_ranges.zig").Request{
        .{ .key = "a", .ranges = &.{}, .output = &.{} },
        .{ .key = "b", .ranges = &.{}, .output = &.{} },
    };
    try std.testing.expectError(error.Cancelled, checked.getRangesMany(&requests));
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqual(@as(usize, 1), count);
    signal.store(false, .release);
    calls = 0;
    checked.mode = .selected_read;
    var selected_vtable = upstream.vtable.*;
    selected_vtable.range_read_mode = .selected_read;
    upstream.vtable = &selected_vtable;
    try checked.getRangesMany(&requests);
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "api query contract oversized copy batches use capability metadata without opening scopes" {
    const backend = @import("../backend_erased.zig");
    const Fake = struct {
        fn abort(_: A, _: *anyopaque) void {}
        fn get(_: *anyopaque, _: []const u8) ![]const u8 {
            return error.UnexpectedGet;
        }
        fn cursor(_: A, _: *anyopaque) !backend.Cursor {
            return error.UnexpectedCursor;
        }
        fn scope(_: A, raw: *anyopaque) !backend.ReadScope {
            const opens: *usize = @ptrCast(@alignCast(raw));
            opens.* += 1;
            return error.UnexpectedScope;
        }
    };
    const a = std.testing.allocator;
    var opens: usize = 0;
    var vtable: backend.ReadTxn.VTable = .{
        .read_scope_range_mode = .selected_copy,
        .abort = Fake.abort,
        .get = Fake.get,
        .open_cursor = Fake.cursor,
        .open_read_scope = Fake.scope,
    };
    var txn: @import("../docstore.zig").DocStore.Txn = .{ .alloc = a, .read = .{ .allocator = a, .ptr = &opens, .vtable = &vtable } };
    var batch = Batch.init(a);
    defer batch.deinit();
    var operands: OperandPlans = .{ .selector = undefined, .read_set = &.{"metadata"} };
    const authority: conditional.Authority = .{ .source = .{ .proof = .{ .revision = 1, .bytes = 2 * 1024 * 1024, .kind = @import("../internal_keys.zig").primary_kind, .digest = @splat(0) } } };
    const authorities = [_]conditional.Authority{ authority, authority };
    const physical = [_][]const u8{ "a", "b" };
    const request: @import("types.zig").SearchRequest = .{ .include_all_fields = false };
    try std.testing.expect(try batch.at(&txn, 0, &physical, &physical, &authorities, request, &operands) == null);
    try std.testing.expectEqual(@as(usize, 0), opens);
    // Neither individually fits together, so no scope or Selection is needed.
    var medium = authority;
    medium.source.proof.?.bytes = 750 * 1024;
    const pair = [_]conditional.Authority{ medium, medium };
    try std.testing.expect(try batch.at(&txn, 0, &physical, &physical, &pair, request, &operands) == null);
    try std.testing.expectEqual(@as(usize, 0), opens);
    var small = authority;
    small.source.proof.?.bytes = 32;
    const mixed = [_]conditional.Authority{ medium, authority, small };
    const three = [_][]const u8{ "a", "b", "c" };
    try std.testing.expect(try batch.at(&txn, 0, &three, &three, &mixed, request, &operands) == null);
    try std.testing.expectEqual(@as(usize, 0), opens);
    // Unknown capability may be genuinely selective; inspect the actual scope.
    vtable.read_scope_range_mode = null;
    try std.testing.expectError(error.UnexpectedScope, batch.at(&txn, 0, &physical, &physical, &pair, request, &operands));
    try std.testing.expectEqual(@as(usize, 1), opens);
    opens = 0;
    // Unknown adapters defer eligibility until their real capability is known.
    vtable.read_scope_range_mode = null;
    try std.testing.expectError(error.UnexpectedScope, batch.at(&txn, 0, &physical, &physical, &authorities, request, &operands));
    try std.testing.expectEqual(@as(usize, 1), opens);
    opens = 0;
    const uncertified = [_]conditional.Authority{ .{}, .{} };
    try std.testing.expect(try batch.at(&txn, 0, &physical, &physical, &uncertified, request, &operands) == null);
    try std.testing.expectEqual(@as(usize, 0), opens);
    // Actual selected I/O remains eligible, rather than inheriting a copy cap.
    vtable.read_scope_range_mode = .selected_read;
    try std.testing.expectError(error.UnexpectedScope, batch.at(&txn, 0, &physical, &physical, &authorities, request, &operands));
    try std.testing.expectEqual(@as(usize, 1), opens);
}

test "api query contract controlled copy batch retains one owner and stops between keys" {
    const backend = @import("../backend_erased.zig");
    const Fake = struct {
        signal: *std.atomic.Value(bool),
        calls: *usize,
        rows: *usize,
        pub const range_read_mode = backend.RangeReadMode.selected_copy;
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.UnexpectedFullRead;
        }
        pub fn getRangesManyControlled(self: *@This(), requests: []const backend.RangeRequest, control: backend.ReadControl) !void {
            self.calls.* += 1;
            for (requests) |_| {
                try control.check();
                self.rows.* += 1;
                self.signal.store(true, .release);
            }
        }
        pub fn close(_: *@This()) void {}
    };
    var signal = std.atomic.Value(bool).init(false);
    var calls: usize = 0;
    var rows: usize = 0;
    var upstream = try backend.readScopeFrom(std.testing.allocator, Fake{ .signal = &signal, .calls = &calls, .rows = &rows });
    defer upstream.close();
    const request: @import("types.zig").SearchRequest = .{ .cancellation = @import("types.zig").CancellationToken.fromAtomic(&signal) };
    var checked: CheckedScope = .{ .upstream = &upstream, .mode = .selected_copy, .request = &request };
    const requests = [_]backend.RangeRequest{
        .{ .key = "a", .ranges = &.{}, .output = &.{} },
        .{ .key = "b", .ranges = &.{}, .output = &.{} },
    };
    try std.testing.expectError(error.Cancelled, checked.getRangesMany(&requests));
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqual(@as(usize, 1), rows);
    // Admission and private error containment happen before engine invocation.
    const Poll = struct {
        fn check(raw: ?*const anyopaque) callconv(.c) u8 {
            return @as(*const u8, @ptrCast(@alignCast(raw.?))).*;
        }
    };
    for ([_]u8{ 2, 255 }) |code| {
        const expected = if (code == 2) error.Timeout else error.RuntimeBoundaryFailure;
        try std.testing.expectError(expected, upstream.getRangesManyControlled(&requests, .{ .ptr = &code, .poll = Poll.check }));
        try std.testing.expectEqual(@as(usize, 1), calls);
    }
}

test "api query contract direct scalar spans preserve proof without reconstructing containing JSON" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const token = "\"data:image/png;base64,YQ==\"";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(token, &digest, .{});
    const fields = [_]@import("../document_projection.zig").Field{.{ .name = "image", .offset = 10, .length = token.len, .digest = digest }};
    const values = [_][]const u8{token};
    var row: Row = .{ .a = a, .txn = null, .physical = "key", .id = "row", .req = .{}, .authority = .{}, .selected_view = .{ .directory = .{ .fields = &fields, .root_count = 1, .version = 2 }, .fields = &fields, .values = &values } };
    const source = row.source();
    const raw = (try source.read_normalized.?(source.context, "image")).?;
    try std.testing.expectEqual(token.ptr, raw.bytes.ptr);
    try std.testing.expect(source.value_proof.?(source.context, "image", raw.bytes) != null);
    try std.testing.expect(row.prefetched == null);
    try std.testing.expect(row.scope == null);
    const datum = try source.read(source.context, a, "image", .value);
    try std.testing.expectEqualStrings(token[1 .. token.len - 1], datum.value.string);
}

test "api query contract borrowed projection windows keep scopes until reset" {
    const backend = @import("../backend_erased.zig");
    const Fake = struct {
        const Scope = struct {
            pub const range_read_mode = backend.RangeReadMode.selected_copy;
            closes: *usize,
            pub fn get(_: *@This(), _: []const u8) ![]const u8 {
                return error.UnexpectedPrimaryRead;
            }
            pub fn close(self: *@This()) void {
                self.closes.* += 1;
            }
        };
        fn abort(_: A, _: *anyopaque) void {}
        fn get(_: *anyopaque, _: []const u8) ![]const u8 {
            return error.UnexpectedPrimaryRead;
        }
        fn cursor(_: A, _: *anyopaque) !backend.Cursor {
            return error.UnexpectedCursor;
        }
        fn open(a: A, raw: *anyopaque) !backend.ReadScope {
            return backend.readScopeFrom(a, Scope{ .closes = @ptrCast(@alignCast(raw)) });
        }
    };
    const a = std.testing.allocator;
    var closes: usize = 0;
    const vtable: backend.ReadTxn.VTable = .{ .read_scope_range_mode = .selected_copy, .abort = Fake.abort, .get = Fake.get, .open_cursor = Fake.cursor, .open_read_scope = Fake.open };
    var txn: @import("../docstore.zig").DocStore.Txn = .{ .alloc = a, .read = .{ .allocator = a, .ptr = &closes, .vtable = &vtable } };
    const metadata = "{\"revision\":\"r1\"}";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(metadata, &digest, .{});
    const fields = [_]@import("../document_projection.zig").Field{.{ .name = "metadata", .offset = 1, .length = metadata.len, .digest = digest, .covering_offset = 1 }};
    const authority: conditional.Authority = .{ .source = .{ .proof = .{ .revision = 1, .bytes = 2 * 1024 * 1024, .kind = @import("../internal_keys.zig").primary_kind, .digest = @splat(0) }, .directory = .{ .fields = &fields, .root_count = 1, .version = 3, .encoded = "x" ++ metadata } } };
    var operands: OperandPlans = .{ .selector = undefined, .read_set = &.{"metadata"} };
    var batch = Batch.init(a);
    defer batch.deinit();
    const authorities = [_]conditional.Authority{ authority, authority };
    const keys = [_][]const u8{ try @import("../internal_keys.zig").documentKeyAlloc(a, "a"), try @import("../internal_keys.zig").documentKeyAlloc(a, "b") };
    defer for (keys) |key| a.free(key);
    const request: @import("types.zig").SearchRequest = .{ .include_all_fields = false };
    const first = (try batch.at(&txn, 0, &keys, &keys, &authorities, request, &operands)).?;
    try std.testing.expectEqual(@as(usize, 0), closes);
    try std.testing.expectEqualStrings(metadata, first.selected.?.raw("metadata").?);
    _ = (try batch.at(&txn, 1, &keys, &keys, &authorities, request, &operands)).?;
    try std.testing.expectEqual(@as(usize, 0), closes);
    batch.reset();
    try std.testing.expectEqual(@as(usize, 1), closes);
}

test "api query contract nested full source fields retain native output witnesses" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const image = try temp.alloc(u8, 64 * 1024);
    @memset(image, 'A');
    const bytes = try std.fmt.allocPrint(temp, "{{\"images\":{{\"preview\":\"data:image/png;base64,{s}\"}},\"revision\":\"r1\",\"permissions\":{{\"public\":true}}}}", .{image});
    const directory_mod = @import("../document_projection.zig");
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const kind = @import("../internal_keys.zig").primary_kind;
    const proof: @import("../document_mutation_revision.zig").Content = .{ .revision = 1, .bytes = bytes.len, .kind = kind, .digest = digest };
    const encoded = (try directory_mod.buildAuthority(temp, bytes, kind, digest)).?;
    const directory = (try directory_mod.Directory.open(temp, encoded, proof)).?;
    const field = directory.resolveExact("images.preview").?;
    const fields = try std.json.parseFromSliceLeaky([]const V, temp,
        \\[{"name":"v0","expression":{"field":"images.preview"}},
        \\ {"name":"v1","expression":{"field":"revision"}},
        \\ {"name":"v2","expression":{"field":"permissions"}},
        \\ {"name":"m0","expression":{"call":"is_missing","args":[{"field":"images.preview"}]}}]
    , .{});
    var plan = try projection.Plan.init(a, .{ .fields = fields });
    defer plan.deinit();
    var operands = try OperandPlans.init(temp, &plan);
    var row: Row = .{ .a = temp, .txn = null, .physical = "row", .id = "row", .req = .{ .include_all_fields = false }, .authority = .{ .source = .{ .proof = proof, .directory = directory } }, .full_bytes = bytes, .operands = &operands };
    const result = try plan.evaluateRetained(temp, a, row.source(), null, .{}, true);
    defer a.free(result.bytes);
    defer result.receipt.?.deinit(a);
    const view: projection.ComputedView = .{ .plan = &plan, .receipt = result.receipt.?, .bytes = result.bytes };
    try std.testing.expect(view.proof("v0") != null);
    try std.testing.expectEqualSlices(u8, &field.digest, &view.proof("v0").?.digest);
    try std.testing.expectEqual(field.length, view.proof("v0").?.encoded_length);
    const object = try view.object(temp);
    try std.testing.expectEqual(image.len + "data:image/png;base64,".len, object.object.get("v0").?.string.len);
    try std.testing.expect(!object.object.get("m0").?.bool);
}

test "api query contract native repeated token values and identities share one parse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{} };
    try row.raw_operands.put(a, "body.image", "\"escaped\\u0020text\"");
    const source_value = row.source();
    const first = try source_value.read(&row, a, "body.image", .value);
    const second = try source_value.read(&row, a, "body.image", .value);
    try std.testing.expectEqualStrings("escaped text", first.value.string);
    try std.testing.expect(first.value.string.ptr == second.value.string.ptr);
    const identity = try source_value.read(&row, a, "body.image", .identity);
    try std.testing.expectEqualStrings(&(try @import("../../common/value_identity.zig").identity(first.value)), identity.value.string);
    const again = try source_value.read(&row, a, "body.image", .identity);
    try std.testing.expect(identity.value.string.ptr == again.value.string.ptr);
    try std.testing.expectEqual(@as(usize, 1), row.token_values.count());
    try std.testing.expectEqual(@as(usize, 0), row.values.count());
    try std.testing.expectEqual(@as(usize, 0), row.value_reads);
    try std.testing.expect(row.full_root == null and row.full_view == null);
}

test "api query contract decoded full values do not build a second raw index" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a, "[{\"name\":\"value\",\"expression\":{\"field\":\"body.image\"}}]", .{});
    var plan = try projection.Plan.init(std.testing.allocator, .{ .fields = fields });
    defer plan.deinit();
    const operands = try OperandPlans.init(a, &plan);
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .operands = &operands, .full_bytes = "{\"body\":{\"image\":\"escaped\\u0020text\"}}" };
    const first = try row.source().read(&row, a, "body.image", .value);
    const again = try row.source().read(&row, a, "body.image", .value);
    try std.testing.expectEqualStrings("escaped text", first.value.string);
    try std.testing.expect(first.value.string.ptr == again.value.string.ptr);
    try std.testing.expect(row.full_root != null);
    try std.testing.expect(row.full_view == null);
    try std.testing.expectEqual(@as(usize, 0), row.token_values.count());
}

/// Diagnostic first/repeated decoded-field access, excluding I/O, SQL VM,
/// identity hashing and output. Called only by the optimized preparation target.
pub fn benchmarkDecodedAccess() !void {
    var corpus = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer corpus.deinit();
    const a = corpus.allocator();
    const fields = try std.json.parseFromSliceLeaky([]const V, a, "[{\"name\":\"value\",\"expression\":{\"field\":\"body.image\"}}]", .{});
    var plan = try projection.Plan.init(std.testing.allocator, .{ .fields = fields });
    defer plan.deinit();
    const operands = try OperandPlans.init(a, &plan);
    for ([_]usize{ 16384, 262144, 1048576 }) |size| {
        const payload = try a.alloc(u8, size);
        @memset(payload, 'x');
        const body = try std.fmt.allocPrint(a, "{{\"body\":{{\"image\":\"{s}\"}},\"metadata\":{{\"revision\":\"r1\"}}}}", .{payload});
        for (0..8) |round| for (0..2) |slot| {
            const native = (if (round % 2 == 0) slot else 1 - slot) == 1;
            var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (0..32) |_| {
                var scratch = std.heap.ArenaAllocator.init(measured.allocator());
                defer scratch.deinit();
                const temp = scratch.allocator();
                var row: Row = .{ .a = temp, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{}, .operands = &operands, .full_bytes = body };
                defer row.deinit();
                var reference: projection.JsonSource = .{ .id = "a", .root = .null };
                if (!native) reference.root = try @import("antfly-json").parseFromSliceLeaky(V, temp, body, .{ .allocate = .alloc_if_needed });
                const source = if (native) row.source() else reference.source();
                const first = try source.read(source.context, temp, "body.image", .value);
                const repeated = try source.read(source.context, temp, "body.image", .value);
                try std.testing.expectEqual(size, first.value.string.len);
                try std.testing.expect(first.value.string.ptr == repeated.value.string.ptr);
                std.mem.doNotOptimizeAway(first.value.string);
                std.mem.doNotOptimizeAway(repeated.value.string);
            }
            const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
            try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
            std.debug.print("source_decoded value_bytes={d} round={d} mode={s} iterations=32 ns={d} allocated_bytes={d}\n", .{ size, round, if (native) "native" else "whole", elapsed, measured.allocated_bytes });
        };
    }
}

test "api query contract public masks reuse checked selections after a full fallback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const padding = try a.alloc(u8, 65536);
    @memset(padding, 'x');
    const metadata = "{\"revision\":\"r1\",\"allowed\":true}";
    const bytes = try std.fmt.allocPrint(a, "{{\"metadata\":{s},\"payload\":{{\"large\":\"{s}\"}},\"padding\":\"{s}{s}\"}}", .{ metadata, padding, padding, padding });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const directory_module = @import("../document_projection.zig");
    const kind = @import("../internal_keys.zig").primary_kind;
    const encoded = (try directory_module.build(a, bytes, kind, digest)).?;
    const directory = (try directory_module.Directory.open(a, encoded, .{ .bytes = @as(u64, bytes.len), .kind = kind, .digest = digest })).?;
    const field = directory.resolveExact("metadata").?;
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{ .fields = &.{ "metadata.revision", "metadata.allowed" }, .include_all_fields = false }, .authority = .{}, .full_bytes = bytes, .selected_view = .{ .directory = directory, .fields = &.{field}, .values = &.{metadata} } };
    const prepared = try row.publicSourceBytes();
    const output = try row.base(a);
    for ([_][]const u8{ prepared, output }) |candidate| {
        const value = try @import("antfly-json").parseFromSliceLeaky(V, a, candidate, .{});
        try std.testing.expectEqual(@as(usize, 1), value.object.count());
        const result_metadata = value.object.get("metadata").?.object;
        try std.testing.expectEqual(@as(usize, 2), result_metadata.count());
        try std.testing.expectEqualStrings("r1", result_metadata.get("revision").?.string);
        try std.testing.expect(result_metadata.get("allowed").?.bool);
    }
    // Whole-source and wildcard masks still require the complete document.
    try std.testing.expect((try row.load(&.{})).ptr == bytes.ptr);
    try std.testing.expect((try row.load(&.{"metadata.*"})).ptr == bytes.ptr);
    const covering = (try directory_module.buildAuthorityObserved(a, bytes, kind, digest, null)).?;
    const proof = @import("../document_mutation_revision.zig").Content{ .revision = 1, .bytes = bytes.len, .kind = kind, .digest = digest };
    row.authority.source = .{ .proof = proof, .directory = (try directory_module.Directory.open(a, covering, proof)).? };
    row.selected_view = null;
    const from_covering = try row.publicSourceBytes();
    try std.testing.expect(from_covering.len < 128);
    const parsed = try std.json.parseFromSliceLeaky(V, a, from_covering, .{});
    const result_metadata = parsed.object.get("metadata").?.object;
    try std.testing.expectEqualStrings("r1", result_metadata.get("revision").?.string);
    try std.testing.expect(result_metadata.get("allowed").?.bool);
    const payload_field = row.authority.source.directory.?.resolveExact("payload.large").?;
    row.full_bytes = null;
    row.selected_view = .{ .directory = row.authority.source.directory.?, .fields = &.{ field, payload_field }, .values = &.{ metadata, bytes[@intCast(payload_field.offset)..][0..@intCast(payload_field.length)] } };
    try std.testing.expect((try row.publicSourceBytes()).len < 128);
    row.full_bytes = bytes;
    // A projected public mask must never become the decoded whole-root cache.
    const whole = try row.source().read(&row, a, "payload.large", .value);
    try std.testing.expectEqual(padding.len, whole.value.string.len);
}

test "api query contract public masks restrict uncovered prepared values before materializing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const large = try a.alloc(u8, 65536);
    @memset(large, 'p');
    const permissions = try a.alloc(u8, 8192);
    @memset(permissions, 'a');
    const bytes = try std.fmt.allocPrint(a, "{{\"metadata\":{{\"revision\":\"r1\",\"allowed\":\"{s}\"}},\"image\":\"{s}\",\"padding\":\"{s}{s}\"}}", .{ permissions, large, large, large });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const module = @import("../document_projection.zig");
    const kind = @import("../internal_keys.zig").primary_kind;
    const proof = @import("../document_mutation_revision.zig").Content{ .revision = 1, .bytes = bytes.len, .kind = kind, .digest = digest };
    for (0..2) |modern| {
        const encoded = (if (modern == 1) try module.buildAuthority(a, bytes, kind, digest) else try module.build(a, bytes, kind, digest)).?;
        const directory = (try module.Directory.open(a, encoded, proof)).?;
        const metadata = directory.find("metadata").?;
        const image = directory.find("image").?;
        var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{ .fields = &.{"metadata"}, .include_all_fields = false }, .authority = .{ .source = .{ .proof = proof, .directory = directory } }, .selected_view = .{ .directory = directory, .fields = &.{ metadata, image }, .values = &.{ bytes[@intCast(metadata.offset)..][0..@intCast(metadata.length)], bytes[@intCast(image.offset)..][0..@intCast(image.length)] } } };
        const masked = try row.publicSourceBytes();
        try std.testing.expect(masked.len < permissions.len + 128);
        const value = try @import("antfly-json").parseFromSliceLeaky(V, a, try row.base(a), .{});
        try std.testing.expectEqual(@as(usize, 1), value.object.count());
        try std.testing.expectEqualStrings(permissions, value.object.get("metadata").?.object.get("allowed").?.string);
        // Masking borrows values without narrowing the scalar source itself.
        const retained = try row.source().read(&row, a, "image", .value);
        try std.testing.expectEqualStrings(large, retained.value.string);
    }
}

test "api query contract authenticated full loans parse leaf operands without unrelated roots" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const padding = try a.alloc(u8, 65536);
    @memset(padding, 'p');
    const bytes = try std.fmt.allocPrint(a, "{{\"body\":{{\"image\":\"{s}\",\"other\":true}},\"padding\":\"{s}\"}}", .{ padding, padding });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const module = @import("../document_projection.zig");
    const kind = @import("../internal_keys.zig").primary_kind;
    const proof = @import("../document_mutation_revision.zig").Content{ .revision = 1, .bytes = bytes.len, .kind = kind, .digest = digest };
    const encoded = (try module.buildAuthority(a, bytes, kind, digest)).?;
    const directory = (try module.Directory.open(a, encoded, proof)).?;
    const identities = @import("../source_value_identity.zig");
    const certificate_bytes = (try identities.build(a, bytes, kind, digest, encoded)).?;
    const certificate = identities.Certificate.open(certificate_bytes, encoded, proof).?;
    var row: Row = .{ .a = a, .txn = null, .physical = "a", .id = "a", .req = .{}, .authority = .{ .source = .{ .proof = proof, .directory = directory }, .certificate = certificate }, .full_bytes = bytes };
    const value = try row.source().read(&row, a, "body.image", .value);
    try std.testing.expectEqualStrings(padding, value.value.string);
    try std.testing.expect(row.full_root == null and row.full_view == null);
    const repeated = try row.source().read(&row, a, "body.image", .value);
    try std.testing.expect(value.value.string.ptr == repeated.value.string.ptr);
    const raw_parent = (try row.source().read_raw.?(&row, a, "body")).?;
    try std.testing.expect(raw_parent.len < bytes.len);
    try std.testing.expect(row.full_view == null and row.full_root == null);
    // Clear the raw cache to exercise the ordinary typed structured fallback.
    row.raw_operands = .empty;
    // Structured consumers retain the existing one-root fallback.
    const parent = try row.source().read(&row, a, "body", .value);
    try std.testing.expect(parent.value.object.get("other").?.bool);
    try std.testing.expect(row.full_root != null);
    // Without a directory-bound certificate, read actual source normally.
    row.authority.certificate = null;
    row.full_root = null;
    row.raw_operands = .empty;
    row.token_values = .empty;
    _ = try row.source().read(&row, a, "body.image", .value);
    try std.testing.expect(row.full_root != null);
}
