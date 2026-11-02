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

//! Bounded exact-vector scans. The embedding host authenticates continuation
//! state before calling this private kernel; every step reopens a pinned version.
const std = @import("std");
const A = std.mem.Allocator;
const indexed = @import("indexed_reader.zig");
const runtime = @import("runtime.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const common = @import("../paged_projection.zig");
const paged = @import("../vector_segment/paged.zig");
const vectors = @import("antfly_vector").vector;
const fact_reader = @import("document_facts_reader.zig");
const search_sources = @import("../search_sources.zig");
const platform_time = @import("antfly_platform").time;
pub const max_state_bytes = 128 * 1024;
pub const State = struct {
    format: u8 = 1,
    generation: []const u8,
    cutoff: []const u8,
    query: [32]u8,
    after: []const u8 = "",
    scanned: []const u8 = "0",
    top: []const indexed.ScoredDoc = &.{},
    pending: []const []const u8 = &.{},
    refined_records: []const u8 = "0",
    pruned_records: []const u8 = "0",
};
pub const Step = struct {
    state: State,
    complete: bool,
    step_records: usize,
    page_reads: usize,
    read_bytes: u64,
    refined_records: usize = 0,
    pruned_records: usize = 0,
    residual_fetches: usize = 0,
    body_refinements: usize = 0,
    pub fn deinit(self: *Step, a: A) void {
        a.free(self.state.generation);
        a.free(self.state.cutoff);
        a.free(self.state.after);
        a.free(self.state.scanned);
        a.free(self.state.refined_records);
        a.free(self.state.pruned_records);
        for (self.state.top) |row| a.free(row.doc_id);
        a.free(self.state.top);
        for (self.state.pending) |value| a.free(value);
        a.free(self.state.pending);
    }
};
// Unlike borrowed TopDocs, this owns at most k IDs and deduplicates multiple
// projections of one document, retaining its best exact distance across steps.
const Selection = struct {
    rows: std.ArrayList(indexed.ScoredDoc) = .empty,
    positions: std.StringHashMapUnmanaged(usize) = .empty,
    capacity: usize,
    bytes: usize = 0,
    fn deinit(self: *@This(), a: A) void {
        for (self.rows.items) |row| a.free(row.doc_id);
        self.rows.deinit(a);
        self.positions.deinit(a);
    }
    fn swap(self: *@This(), l: usize, r: usize) void {
        std.mem.swap(indexed.ScoredDoc, &self.rows.items[l], &self.rows.items[r]);
        self.positions.getPtr(self.rows.items[l].doc_id).?.* = l;
        self.positions.getPtr(self.rows.items[r].doc_id).?.* = r;
    }
    fn down(self: *@This(), start: usize) void {
        var i = start;
        while (i * 2 + 1 < self.rows.items.len) {
            var child = i * 2 + 1;
            if (child + 1 < self.rows.items.len and indexed.lessScoredDoc({}, self.rows.items[child], self.rows.items[child + 1])) child += 1;
            if (!indexed.lessScoredDoc({}, self.rows.items[i], self.rows.items[child])) break;
            self.swap(i, child);
            i = child;
        }
    }
    fn add(self: *@This(), a: A, value: indexed.ScoredDoc) !void {
        if (self.positions.get(value.doc_id)) |i| {
            if (indexed.lessScoredDoc({}, value, self.rows.items[i])) {
                self.rows.items[i].score = value.score;
                self.rows.items[i].distance = value.distance;
                self.down(i);
            }
            return;
        }
        const full = self.rows.items.len == self.capacity;
        if (full and !indexed.lessScoredDoc({}, value, self.rows.items[0])) return;
        const removed = if (full) self.rows.items[0].doc_id.len else 0;
        if (self.bytes - removed + value.doc_id.len > max_state_bytes / 2) return error.ExactScanStateTooLarge;
        try self.positions.ensureUnusedCapacity(a, 1);
        if (!full) try self.rows.ensureUnusedCapacity(a, 1);
        const id = try a.dupe(u8, value.doc_id);
        const row = indexed.ScoredDoc{ .doc_id = id, .score = value.score, .distance = value.distance };
        if (full) {
            const old = self.rows.items[0];
            _ = self.positions.remove(old.doc_id);
            self.bytes -= old.doc_id.len;
            a.free(old.doc_id);
            self.rows.items[0] = row;
            self.positions.putAssumeCapacity(id, 0);
            self.down(0);
        } else {
            var i = self.rows.items.len;
            self.rows.appendAssumeCapacity(row);
            self.positions.putAssumeCapacity(id, i);
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!indexed.lessScoredDoc({}, self.rows.items[parent], self.rows.items[i])) break;
                self.swap(parent, i);
                i = parent;
            }
        }
        self.bytes += id.len;
    }
};
// A request-local ambiguity window. Its kth upper bound is a certificate:
// k distinct prefix documents have an exact distance no greater than it. Only
// intervals strictly above that bound may be dropped. Every remaining interval
// is either resolved in this step or carried as a bounded authenticated
// control. Unverified approximate scores never enter the verified top-k heap.
const Refinement = struct {
    a: A,
    pages: *PageStore,
    meter: *Meter,
    reader: *paged.blocks.Reader,
    selected: *Selection,
    upper: Selection,
    query: []const f32,
    measure: f32,
    query_norm: f32,
    metric: vectors.DistanceMetric,
    dims: u32,
    session: *runtime.QuerySession,
    source: ?search_sources.VectorSourceDescriptor,
    body_reader: ?*fact_reader.Reader = null,
    body_meter: ?Meter = null,
    body_disabled: bool = false,
    allow_bodies: bool = false,
    body_refinements: usize = 0,
    body_obligation_bytes: u64 = 0,
    body_obligation_reads: usize = 0,
    pending: std.ArrayList(Item) = .empty,
    bytes: usize = 0,
    reserved_bytes: u64 = 0,
    reserved_reads: usize = 0,
    refined: usize = 0,
    pruned: usize = 0,
    const Item = struct { value: []u8, blob: common.Blob, lower: f32 };
    fn threshold(self: *@This()) f32 {
        return if (self.upper.rows.items.len == self.upper.capacity) self.upper.rows.items[0].distance.? else std.math.inf(f32);
    }
    fn has(self: *@This(), blob: common.Blob) bool {
        for (self.pending.items) |item| if (std.meta.eql(item.blob, blob)) return true;
        return false;
    }
    fn release(self: *@This()) void {
        self.pages.remaining_read_bytes.* += self.reserved_bytes;
        self.meter.reads -= self.reserved_reads;
        self.reserved_bytes = 0;
        self.reserved_reads = 0;
    }
    fn clear(self: *@This()) void {
        for (self.pending.items) |item| self.a.free(item.value);
        self.pending.clearRetainingCapacity();
        self.bytes = 0;
    }
    fn deinit(self: *@This()) void {
        self.release();
        self.clear();
        self.pending.deinit(self.a);
        self.upper.deinit(self.a);
        if (self.body_reader) |reader| reader.destroy();
    }
    fn append(self: *@This(), value: []const u8, c: paged.blocks.Control, lower: f32) !bool {
        if (self.pending.items.len == 128 or value.len > 64 * 1024 - self.bytes) return false;
        const fresh = !self.has(c.residual);
        if (fresh and (c.residual.bytes > self.pages.remaining_read_bytes.* or self.meter.reads == 128)) return false;
        const owned = try self.a.dupe(u8, value);
        errdefer self.a.free(owned);
        try self.pending.append(self.a, .{ .value = owned, .blob = c.residual, .lower = lower });
        self.bytes += value.len;
        if (fresh) {
            self.pages.remaining_read_bytes.* -= c.residual.bytes;
            self.reserved_bytes += c.residual.bytes;
            self.meter.reads += 1;
            self.reserved_reads += 1;
        }
        return true;
    }
    fn trim(self: *@This()) void {
        const cutoff = self.threshold();
        var i: usize = 0;
        while (i < self.pending.items.len) {
            if (self.pending.items[i].lower <= cutoff) {
                i += 1;
                continue;
            }
            const item = self.pending.swapRemove(i);
            self.bytes -= item.value.len;
            self.a.free(item.value);
            self.pruned += 1;
            if (!self.has(item.blob)) {
                self.pages.remaining_read_bytes.* += item.blob.bytes;
                self.reserved_bytes -= item.blob.bytes;
                self.meter.reads -= 1;
                self.reserved_reads -= 1;
            }
        }
    }
    fn refine(self: *@This(), value: []const u8) !void {
        var entry = (try self.tryBody(value)) orelse try self.reader.read(value, self.dims);
        defer entry.deinit(self.a);
        const similarity = indexed.similarityForQuery(self.query, self.measure, entry.vector, self.metric);
        const distance = vectors.distanceToQuery(self.query, self.measure, entry.vector, self.metric);
        if (!std.math.isFinite(similarity) or !std.math.isFinite(distance)) return error.InvalidVectorQuery;
        if (similarity > 0) try self.selected.add(self.a, .{ .doc_id = entry.doc_id, .score = indexed.scoreFromSimilarity(similarity), .distance = distance });
        try self.upper.add(self.a, .{ .doc_id = entry.doc_id, .score = 0, .distance = distance });
        self.refined += 1;
    }
    fn flush(self: *@This()) !void {
        self.body_obligation_bytes = self.reserved_bytes;
        self.body_obligation_reads = self.reserved_reads;
        self.release();
        defer self.clear();
        // Group residual references so each reserved immutable pack needs at
        // most one load, independently of the bounded reader cache's eviction.
        std.mem.sort(Item, self.pending.items, {}, struct {
            fn less(_: void, x: Item, y: Item) bool {
                const digest = std.mem.order(u8, &x.blob.digest, &y.blob.digest);
                if (digest != .eq) return digest == .lt;
                return std.mem.lessThan(u8, &x.blob.attempt, &y.blob.attempt);
            }
        }.less);
        for (self.pending.items) |item| {
            try self.pages.cancellation.check();
            if (item.lower > self.threshold()) {
                self.pruned += 1;
                continue;
            }
            try self.refine(item.value);
        }
    }
    fn tryBody(self: *@This(), value: []const u8) !?@import("../vector_segment/mod.zig").Entry {
        if (!self.allow_bodies or self.body_disabled or self.source == null or !paged.blocks.isCompact(value)) return null;
        const c = try paged.blocks.control(value, self.dims);
        if (c.source_lsn == 0 or self.pages.remaining_read_bytes.* <= self.body_obligation_bytes) return null;
        const limit = 128 - self.body_obligation_reads;
        if (self.meter.reads >= limit) return null;
        const original = self.pages.remaining_read_bytes.*;
        var spare = original - self.body_obligation_bytes;
        const available = spare;
        defer self.pages.remaining_read_bytes.* -= available - spare;
        if (self.body_reader == null) {
            self.meter.reads += 1; // The authenticated facts root.
            self.body_reader = fact_reader.Reader.create(self.a, self.session, &spare) catch |err| switch (err) {
                error.QueryCandidateBudgetExceeded => {
                    self.body_disabled = true;
                    return null;
                },
                else => return err,
            };
            if (self.body_reader == null) {
                self.body_disabled = true;
                return null;
            }
            self.body_meter = .{ .backing = self.body_reader.?.pages.store(), .counter = &self.meter.reads, .limit = limit };
            self.body_reader.?.cache.underlying = self.body_meter.?.store();
        }
        const reader = self.body_reader.?;
        reader.pages.remaining_read_bytes = &spare;
        defer reader.pages.remaining_read_bytes = self.pages.remaining_read_bytes;
        self.body_meter.?.limit = limit;
        const fact = (reader.lookup(c.id) catch |err| switch (err) {
            error.QueryCandidateBudgetExceeded, error.ExactScanYield => return null,
            else => return err,
        }) orelse return null;
        if (fact.last_lsn != c.source_lsn) return null;
        const physical_bytes = if (fact.body.pack) |pack_| pack_.bytes else fact.body.bytes;
        if (physical_bytes > 2 * c.residual.bytes or physical_bytes > spare or self.meter.reads >= limit) return null;
        self.meter.reads += 1;
        const body = reader.readBodyAlloc(fact) catch |err| switch (err) {
            error.QueryCandidateBudgetExceeded => return null,
            else => return err,
        };
        defer self.a.free(body);
        var projection = try @import("../document_projection.zig").parseAlloc(self.a, body);
        defer projection.deinit(self.a);
        const selected = switch (search_sources.selectVectorSource(&projection, self.source.?)) {
            .none => return null,
            .top_level => |vector| if (c.source_ordinal == 0) vector else return null,
            .chunk_embeddings => |chunks| if (c.source_ordinal < chunks.len) chunks[c.source_ordinal].embedding else return null,
        };
        if (selected.len != self.dims) return null;
        const id = try self.a.dupe(u8, c.id);
        errdefer self.a.free(id);
        const vector = try self.a.dupe(f32, selected);
        self.body_refinements += 1;
        return .{ .doc_id = id, .vector = vector, .source_lsn = c.source_lsn, .source_ordinal = c.source_ordinal };
    }
    fn stateFits(self: *@This()) !bool {
        var buffer: [512]u8 = undefined;
        var sink = std.Io.Writer.Discarding.init(&buffer);
        try std.json.Stringify.value(self.selected.rows.items, .{}, &sink.writer);
        // All other native fields have fixed names and bounded decimal/hex
        // representations. Leave a further 4 KiB for the app MAC envelope.
        var bytes = sink.fullCount() + 1024;
        for (self.pending.items) |item| bytes += std.base64.standard.Encoder.calcSize(item.value.len) + 3;
        return bytes <= max_state_bytes - 4096;
    }
    fn consider(self: *@This(), value: []const u8) !void {
        if (!paged.blocks.isCompact(value)) return self.refine(value);
        const c = try paged.blocks.control(value, self.dims);
        const interval = try paged.blocks.distanceInterval(value, self.dims, self.query, self.measure, self.query_norm, self.metric);
        if (interval.lower > self.threshold()) {
            self.pruned += 1;
            return;
        }
        if (!try self.append(value, c, interval.lower)) {
            try self.flush();
            if (interval.lower > self.threshold()) {
                self.pruned += 1;
                return;
            }
            if (!try self.append(value, c, interval.lower)) return self.refine(value);
        }
        try self.upper.add(self.a, .{ .doc_id = c.id, .score = 0, .distance = interval.upper });
        self.trim();
    }
};
const Meter = struct {
    backing: tree.Store,
    reads: usize = 0,
    counter: ?*usize = null,
    limit: usize = 128,
    fn get(ptr: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const count = self.counter orelse &self.reads;
        if (count.* >= self.limit) return error.ExactScanYield;
        count.* += 1;
        return self.backing.get(self.backing.ptr, a, ref);
    }
    fn many(ptr: *anyopaque, a: A, refs_: []const tree.Ref, output: []tree.NavigationPage) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const count = self.counter orelse &self.reads;
        if (refs_.len > self.limit -| count.*) return error.ExactScanYield;
        count.* += refs_.len;
        return self.backing.read_many.?.read(self.backing.read_many.?.ptr, a, refs_, output);
    }
    fn put(_: *anyopaque, _: tree.Ref, _: []const u8) anyerror!void {
        return error.ExactScanWriteForbidden;
    }
    fn check(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.backing.check(self.backing.ptr);
    }
    fn store(self: *@This()) tree.Store {
        return .{ .domain = self.backing.domain, .ptr = self, .get = get, .put = put, .check = check, .read_many = if (self.backing.read_many) |_| .{ .ptr = self, .read = many } else null };
    }
};
fn decimal(text: []const u8) !u64 {
    if (text.len == 0 or text.len > 20 or (text.len > 1 and text[0] == '0')) return error.InvalidExactScanState;
    for (text) |c| if (!std.ascii.isDigit(c)) return error.InvalidExactScanState;
    return std.fmt.parseInt(u64, text, 10) catch error.InvalidExactScanState;
}
pub fn generation(state: State) !u64 {
    return decimal(state.generation);
}
fn queryBinding(a: A, session: *runtime.QuerySession, checksum: []const u8, query: []const f32, limit: usize) ![32]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ .format = @as(u8, 1), .namespace = session.manifest.namespace, .source = checksum, .query = query, .limit = limit }, .{});
    defer a.free(identity);
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(identity, &result, .{});
    return result;
}
pub fn scan(a: A, session: *runtime.QuerySession, query: []const f32, limit: usize, previous: ?State, maximum: usize) !Step {
    if (limit == 0 or limit > 100 or maximum == 0 or maximum > 512) return error.QueryCandidateBudgetExceeded;
    try session.checkCancellation();
    const index = try indexed.resolveVectorArtifactIndex(session, .{ .text = &.{} });
    const source = session.manifest.artifacts[index];
    if (source.metadata_version != common.metadata_version) return error.ExactScanCapabilityUnsupported;
    var remaining: u64 = 4 * 1024 * 1024;
    var writes: u64 = 0;
    var pages = PageStore{ .domain = PageStore.namespaceDomain(session.manifest.namespace), .artifacts = session.artifacts, .cancellation = session.readCancellation(), .remaining_read_bytes = &remaining, .remaining_write_bytes = &writes, .coalesce_reads = true, .read_cache = session.graphAdjacencyCache() };
    if (source.byte_len > remaining) return error.ArtifactReadBudgetExceeded;
    remaining -= source.byte_len;
    const parsed = try common.load(a, &pages, source);
    defer parsed.deinit();
    const root = parsed.value;
    if (query.len != root.dims and paged.runs.records(root) != 0) return error.VectorDimsMismatch;
    const binding = try queryBinding(a, session, source.checksum, query, limit);
    const total = paged.runs.records(root);
    var scanned: u64 = 0;
    var previous_refined: u64 = 0;
    var previous_pruned: u64 = 0;
    var after: [36]u8 = undefined;
    var has_after = false;
    var selection = Selection{ .capacity = limit };
    defer selection.deinit(a);
    if (previous) |state| {
        if ((state.format != 1 and state.format != 2) or !std.mem.eql(u8, &state.query, &binding) or try generation(state) != session.version() or try decimal(state.cutoff) != session.manifest.wal_end_lsn or state.top.len > limit) return error.ExactScanBindingMismatch;
        scanned = try decimal(state.scanned);
        if (state.pending.len > 128 or (state.format == 1 and state.pending.len != 0)) return error.InvalidExactScanState;
        previous_refined = if (state.format == 1) scanned else try decimal(state.refined_records);
        previous_pruned = if (state.format == 1) 0 else try decimal(state.pruned_records);
        const resolved = std.math.add(u64, previous_refined, previous_pruned) catch return error.InvalidExactScanState;
        if (resolved > scanned or scanned - resolved != state.pending.len) return error.InvalidExactScanState;
        if (scanned > total or (scanned == 0) != (state.after.len == 0)) return error.InvalidExactScanState;
        if (state.after.len != 0) {
            if (state.after.len != 72) return error.InvalidExactScanState;
            _ = std.fmt.hexToBytes(&after, state.after) catch return error.InvalidExactScanState;
            has_after = true;
        }
        for (state.top) |row| {
            if (row.doc_id.len == 0 or row.distance == null or !std.math.isFinite(row.distance.?)) return error.InvalidExactScanState;
            try selection.add(a, row);
        }
        if (selection.rows.items.len != state.top.len) return error.InvalidExactScanState;
    }
    const normalized = if (total != 0) try indexed.normalizedCosineQueryAlloc(a, root.metric, query) else query;
    defer if (total != 0 and root.metric == .cosine) a.free(@constCast(normalized));
    const measure = indexed.vectorQueryMeasure(normalized, root.metric);
    var meter = Meter{ .backing = pages.store() };
    var source_blocks = paged.blocks.Reader{ .a = a, .pages = &pages, .read_count = &meter.reads };
    defer source_blocks.deinit();
    const query_norm = if (root.metric == .inner_product) vectors.norm(normalized) else measure;
    const sources = try search_sources.listVectorSourcesAlloc(a, session.manifest.stats.published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(a, sources);
    const body_source = for (sources) |descriptor| {
        if (std.mem.eql(u8, descriptor.index_name, root.index_name)) {
            const expected = try common.fingerprint(a, .{ .source = descriptor, .metric = root.metric });
            if (std.mem.eql(u8, &expected, &root.source_fingerprint)) break descriptor;
        }
    } else null;
    var refinement = Refinement{ .a = a, .pages = &pages, .meter = &meter, .reader = &source_blocks, .selected = &selection, .session = session, .source = body_source, .upper = .{ .capacity = limit }, .query = normalized, .measure = measure, .query_norm = query_norm, .metric = root.metric, .dims = root.dims };
    defer refinement.deinit();
    for (selection.rows.items) |row| try refinement.upper.add(a, .{ .doc_id = row.doc_id, .score = 0, .distance = row.distance });
    var cache = tree.Cache{ .alloc = a, .underlying = meter.store(), .max_bytes = 512 * 1024, .decoded = true };
    defer cache.deinit();
    const lower = if (has_after) try common.upperBound(a, &after) else null;
    defer if (lower) |key| a.free(key);
    var cursor = try paged.runs.Cursor.init(a, cache.store(), root, lower orelse "", null);
    defer cursor.deinit();
    if (previous) |state| {
        var retained_bytes: usize = 0;
        for (state.pending) |encoded| {
            const len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidExactScanState;
            if (len > 32 * 1024 - retained_bytes) return error.ExactScanStateTooLarge;
            retained_bytes += len;
            const value = try a.alloc(u8, len);
            defer a.free(value);
            std.base64.standard.Decoder.decode(value, encoded) catch return error.InvalidExactScanState;
            try refinement.consider(value);
        }
    }
    const started = platform_time.monotonicNs();
    const count_limit = @min(maximum, @max(@as(usize, 1), (1024 * 1024) / (@as(usize, root.dims) * 4 + 128)));
    var count: usize = 0;
    while (scanned < total and count < count_limit) {
        if (count != 0 and platform_time.monotonicNs() -| started >= std.time.ns_per_s) break;
        cursor.readAhead(count_limit - count, @intCast(@min(remaining, 128 * 1024))) catch |err| switch (err) {
            // Optional look-ahead admission must not prevent a smaller useful
            // single-page read at the end of a bounded step.
            error.ArtifactReadBudgetExceeded, error.ExactScanYield => {},
            else => return err,
        };
        const record = (cursor.next() catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded, error.ExactScanYield => if (count != 0) break else return err,
            else => return err,
        }) orelse return error.InvalidExactScanState;
        if (has_after and std.mem.order(u8, record.key, &after) != .gt) return error.InvalidExactScanState;
        if (record.key.len != 36) return error.InvalidVectorValue;
        try session.checkCancellation();
        refinement.consider(record.value) catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded, error.GraphMetricBuildBudgetExceeded, error.ExactScanYield => if (count != 0) break else return err,
            else => return err,
        };
        @memcpy(&after, record.key);
        has_after = true;
        count += 1;
        scanned += 1;
    }
    // Preserve a small ambiguity set across bounded steps. It is authenticated
    // as private native state and never appears as public hits. Large windows
    // fall back to resolving before yielding; complete responses always resolve.
    refinement.allow_bodies = scanned == total;
    if (scanned == total or refinement.bytes > 32 * 1024 or !try refinement.stateFits()) try refinement.flush() else refinement.release();
    if (!try refinement.stateFits()) return error.ExactScanStateTooLarge;
    const deferred = try a.alloc([]const u8, refinement.pending.items.len);
    var deferred_count: usize = 0;
    errdefer {
        for (deferred[0..deferred_count]) |value| a.free(value);
        a.free(deferred);
    }
    for (refinement.pending.items, deferred) |item, *encoded| {
        const bytes = try a.alloc(u8, std.base64.standard.Encoder.calcSize(item.value.len));
        _ = std.base64.standard.Encoder.encode(bytes, item.value);
        encoded.* = bytes;
        deferred_count += 1;
    }
    const refined_total = try std.math.add(u64, previous_refined, refinement.refined);
    const pruned_total = try std.math.add(u64, previous_pruned, refinement.pruned);
    const accounted = try std.math.add(u64, refined_total, pruned_total);
    if (accounted > scanned or scanned - accounted != deferred.len) return error.InvalidExactScanState;
    const refined_text = try std.fmt.allocPrint(a, "{d}", .{refined_total});
    errdefer a.free(refined_text);
    const pruned_text = try std.fmt.allocPrint(a, "{d}", .{pruned_total});
    errdefer a.free(pruned_text);
    const gen = try std.fmt.allocPrint(a, "{d}", .{session.version()});
    errdefer a.free(gen);
    const cutoff = try std.fmt.allocPrint(a, "{d}", .{session.manifest.wal_end_lsn});
    errdefer a.free(cutoff);
    const position = try a.dupe(u8, if (has_after) &std.fmt.bytesToHex(after, .lower) else "");
    errdefer a.free(position);
    const visited = try std.fmt.allocPrint(a, "{d}", .{scanned});
    errdefer a.free(visited);
    const top = try selection.rows.toOwnedSlice(a);
    std.mem.sort(indexed.ScoredDoc, top, {}, indexed.lessScoredDoc);
    return .{ .state = .{ .format = 2, .pending = deferred, .refined_records = refined_text, .pruned_records = pruned_text, .generation = gen, .cutoff = cutoff, .query = binding, .after = position, .scanned = visited, .top = top }, .complete = scanned == total, .step_records = count, .page_reads = meter.reads, .read_bytes = 4 * 1024 * 1024 - remaining, .refined_records = refinement.refined, .pruned_records = refinement.pruned, .residual_fetches = source_blocks.fetches, .body_refinements = refinement.body_refinements };
}

pub fn materialize(a: A, session: *runtime.QuerySession, rows: []const indexed.ScoredDoc) ![]@import("request.zig").SearchHit {
    var reads: u64 = 4 * 1024 * 1024;
    const reader = try @import("document_facts_reader.zig").Reader.create(a, session, &reads) orelse return error.ExactScanCapabilityUnsupported;
    defer reader.destroy();
    const Hit = @import("request.zig").SearchHit;
    const hits = try a.alloc(Hit, rows.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            a.free(hit.doc_id);
            a.free(hit.body);
        }
        a.free(hits);
    }
    var bytes: u64 = 0;
    for (rows, hits) |row, *hit| {
        const fact = try reader.lookup(row.doc_id) orelse return error.DocumentBodyNotFound;
        bytes += fact.body.bytes;
        if (bytes > 1024 * 1024) return error.QueryCandidateBudgetExceeded;
        const id = try a.dupe(u8, row.doc_id);
        errdefer a.free(id);
        hit.* = .{ .doc_id = id, .body = try reader.readBodyAlloc(fact), .score = row.score, .distance = row.distance };
        initialized += 1;
    }
    return hits;
}

// Independent source-vector oracle: fully materialize/sort all distinct
// documents, without the scan heap, quantization or serving representation.
fn oracleTop(a: A, entries: []const @import("../vector_segment/mod.zig").Entry, query: []const f32, limit: usize) ![]indexed.ScoredDoc {
    var all: std.ArrayList(indexed.ScoredDoc) = .empty;
    defer all.deinit(a);
    errdefer for (all.items) |row| a.free(row.doc_id);
    const measure = vectors.norm(query);
    for (entries) |entry| {
        const distance = vectors.distanceToQuery(query, measure, entry.vector, .cosine);
        const similarity = 1 - distance;
        if (similarity <= 0) continue;
        const found = for (all.items, 0..) |row, i| {
            if (std.mem.eql(u8, row.doc_id, entry.doc_id)) break i;
        } else null;
        if (found) |i| {
            if (distance < all.items[i].distance.?) {
                all.items[i].distance = distance;
                all.items[i].score = indexed.scoreFromSimilarity(similarity);
            }
        } else {
            const id = try a.dupe(u8, entry.doc_id);
            errdefer a.free(id);
            try all.append(a, .{ .doc_id = id, .score = indexed.scoreFromSimilarity(similarity), .distance = distance });
        }
    }
    std.mem.sort(indexed.ScoredDoc, all.items, {}, indexed.lessScoredDoc);
    const n = @min(limit, all.items.len);
    for (all.items[n..]) |row| a.free(row.doc_id);
    all.shrinkRetainingCapacity(n);
    return all.toOwnedSlice(a);
}

test "serverless exact scan resumes bounded immutable ranges and merges multi-projection ties" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/exact", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var store = fs.artifactStore();
    defer store.deinit();
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var ids: [770][16]u8 = undefined;
    var values: [770][32]f32 = undefined;
    var entries: [770]@import("../vector_segment/mod.zig").Entry = undefined;
    var seed: u32 = 71;
    for (&ids, &values, &entries, 0..) |*id, *vector, *entry, i| {
        for (vector) |*v| {
            seed = seed *% 1664525 +% 1013904223;
            v.* = @as(f32, @floatFromInt(seed >> 8)) / 8388608.0 - 1;
        }
        entry.* = .{ .doc_id = try std.fmt.bufPrint(id, "doc/{d}", .{if (i < 768) i else 0}), .vector = vector };
    }
    const ref = try paged.publishAlloc(a, &pages, null, &entries, &.{}, "dense", @splat(1), .cosine, null);
    defer @import("../build/builder.zig").freeArtifactRef(a, ref);
    var artifacts = [_]@import("../manifest/mod.zig").ArtifactRef{ref};
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "docs", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 770, .stats = .{ .document_count = 768 }, .artifacts = &artifacts } };
    var query: [32]f32 = @splat(0);
    query[0] = 1;
    const oracle = try oracleTop(a, &entries, &query, 10);
    defer indexed.freeScoredDocs(a, oracle);
    const cache_path = try std.fmt.allocPrint(a, "{s}/cache", .{path});
    defer a.free(cache_path);
    var shared = try @import("cache.zig").QueryCache.init(a, cache_path);
    defer shared.deinit();
    session.cache = &shared;
    // Cold duplicate claims, failed-fill cleanup, then a zero-I/O warm batch.
    const projection = try common.load(a, &pages, ref);
    defer projection.deinit();
    var branch = try tree.readPage(a, pages.store(), projection.value.docs.?);
    defer branch.deinit(a);
    try std.testing.expectEqual(@as(u8, 1), branch.ref.height);
    try std.testing.expect(branch.entries[0].child.?.pack != null and branch.entries[1].child.?.pack != null);
    const refs_ = [_]tree.Ref{ branch.entries[0].child.?.*, branch.entries[0].child.?.*, branch.entries[1].child.?.* };
    const hook = session.graphAdjacencyCache().?;
    var encoded: [3][]u8 = undefined;
    var allowance: u64 = 0;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, hook.read_many.?(hook.ptr, a, &store, pages.domain, &refs_, &encoded, .none, &allowance));
    try std.testing.expectEqual(@as(usize, 0), shared.graph_metric_blocks.snapshot().entries);
    allowance = 4 * 1024 * 1024;
    try hook.read_many.?(hook.ptr, a, &store, pages.domain, &refs_, &encoded, .none, &allowance);
    for (encoded) |bytes| a.free(bytes);
    try std.testing.expect(allowance < 4 * 1024 * 1024);
    allowance = 0;
    try hook.read_many.?(hook.ptr, a, &store, pages.domain, &refs_, &encoded, .none, &allowance);
    for (encoded) |bytes| a.free(bytes);
    var current: ?Step = null;
    defer if (current) |*step| step.deinit(a);
    var steps: usize = 0;
    while (true) {
        const next = try scan(a, &session, &query, 10, if (current) |step| step.state else null, 13);
        if (current) |*step| step.deinit(a);
        current = next;
        steps += 1;
        try std.testing.expect(next.step_records <= 13 and next.page_reads <= 128 and next.read_bytes <= 4 * 1024 * 1024);
        try std.testing.expectEqual(try decimal(next.state.scanned), (try decimal(next.state.refined_records)) + (try decimal(next.state.pruned_records)) + next.state.pending.len);
        if (next.complete) try std.testing.expectEqual(@as(usize, 0), next.state.pending.len);
        if (steps == 1) {
            try std.testing.expectError(error.ExactScanBindingMismatch, scan(a, &session, &query, 9, next.state, 13));
            session.manifest.version = 2;
            try std.testing.expectError(error.ExactScanBindingMismatch, scan(a, &session, &query, 10, next.state, 13));
            session.manifest.version = 1;
            var stale = next.state;
            stale.scanned = "99999";
            try std.testing.expectError(error.InvalidExactScanState, scan(a, &session, &query, 10, stale, 13));
        }
        if (next.complete) break;
        try std.testing.expect(steps < 100);
    }
    try std.testing.expect(steps > 1);
    try std.testing.expectEqualStrings("770", current.?.state.scanned);
    try std.testing.expectEqual(oracle.len, current.?.state.top.len);
    for (oracle, current.?.state.top) |l, r| {
        try std.testing.expectEqualStrings(l.doc_id, r.doc_id);
        try std.testing.expectEqual(l.distance.?, r.distance.?);
    }
}

test "serverless exact top-k deduplicates improvements and releases failed allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectionFailure, .{});
}
fn selectionFailure(a: A) !void {
    var selection = Selection{ .capacity = 3 };
    defer selection.deinit(a);
    try selection.add(a, .{ .doc_id = "a", .score = 1, .distance = 0.9 });
    try selection.add(a, .{ .doc_id = "b", .score = 2, .distance = 0.8 });
    try selection.add(a, .{ .doc_id = "c", .score = 3, .distance = 0.7 });
    try selection.add(a, .{ .doc_id = "a", .score = 5, .distance = 0.5 });
    try selection.add(a, .{ .doc_id = "d", .score = 4, .distance = 0.6 });
    std.mem.sort(indexed.ScoredDoc, selection.rows.items, {}, indexed.lessScoredDoc);
    try std.testing.expectEqualStrings("a", selection.rows.items[0].doc_id);
    try std.testing.expectEqualStrings("d", selection.rows.items[1].doc_id);
    try std.testing.expectEqualStrings("c", selection.rows.items[2].doc_id);
}
