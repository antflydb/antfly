// Copyright 2026 Antfly, Inc.
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

//! Cross-shard candidate blocking for entity resolution (see zig/RESOLUTION.md).
//!
//! The storage resolution worker resolves a document's mentions against an
//! entity table that may live on other shards. It does its blocking through the
//! `db_mod.CandidateSource` seam, which is local-only by default (the worker's
//! own store). `DistributedCandidateSource` implements that seam over the api
//! layer's routing-aware `TableReadSource`, so blocking queries (exact key,
//! label-prefix scan, vector nearest) fan out to whichever shard owns the
//! entity and resolve either locally or via HTTP — reusing all the existing
//! group routing instead of re-deriving it.
//!
//! It is injected at DB construction by the serving layer when a resolver
//! declares a `candidate_search` mode; storage never imports the api layer.

const std = @import("std");
const db_mod = @import("../storage/db/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const table_reads = @import("table_read_source.zig");

const CandidateSource = db_mod.CandidateSource;

/// Adapts the api layer's `TableReadSource` (routing-aware lookup/scan/query) to
/// the storage worker's `CandidateSource` seam. Holds only borrowed handles, so
/// it must not outlive the read source it wraps.
pub const DistributedCandidateSource = struct {
    reads: table_reads.TableReadSource,
    catalog_binding: ?@import("../system_catalog/domain.zig").BindingSource = null,
    /// Read consistency for blocking queries. Resolution runs leader-only, so
    /// `read_index` keeps candidates consistent with committed writes; callers
    /// can relax this to `stale` to trade freshness for latency.
    consistency: raft_mod.ReadConsistency = .read_index,

    pub fn candidateSource(self: *DistributedCandidateSource) CandidateSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = CandidateSource.VTable{
        .begin_batch = beginBatch,
        .get = getFn,
        .scan_prefix = scanPrefixFn,
        .nearest = nearestFn,
    };

    const BoundBatch = struct {
        arena: std.heap.ArenaAllocator,
        raw: DistributedCandidateSource,
        binding: @import("../system_catalog/domain.zig").BindingSource,
        names: []const []const u8,
        physical: ?[][]u8 = null,

        fn resolve(self: *@This(), table: []const u8) !?[]const u8 {
            // Curated endpoints outside this resolver's declared target retain
            // their independent promotion binding; never guess physical names.
            for (self.names, 0..) |name, i| if (std.mem.eql(u8, name, table)) {
                if (self.physical == null) self.physical = try self.binding.bind(self.arena.allocator(), self.names);
                if (self.physical.?.len != self.names.len) return error.InvalidCatalogRecord;
                return self.physical.?[i];
            };
            return null;
        }
        fn boundTable(ptr: *anyopaque, table: []const u8) anyerror!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.resolve(table);
        }
        fn get(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return getFn(&self.raw, alloc, (try self.resolve(table)) orelse return error.TableNotFound, key);
        }
        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: CandidateSource.ScanOptions, ctx: *anyopaque, consume: CandidateSource.Consume) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return scanPrefixFn(&self.raw, alloc, (try self.resolve(table)) orelse return error.TableNotFound, prefix, opts, ctx, consume);
        }
        fn nearest(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, query: CandidateSource.NearestQuery, ctx: *anyopaque, consume: CandidateSource.Consume) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return nearestFn(&self.raw, alloc, (try self.resolve(table)) orelse return error.TableNotFound, query, ctx, consume);
        }
        fn release(ptr: *anyopaque, alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.arena.deinit();
            alloc.destroy(self);
        }
        const vtable: CandidateSource.VTable = .{ .get = get, .scan_prefix = scan, .nearest = nearest, .bound_table = boundTable };
    };

    fn beginBatch(ptr: *anyopaque, alloc: std.mem.Allocator, names: []const []const u8) anyerror!CandidateSource.Batch {
        const self: *DistributedCandidateSource = @ptrCast(@alignCast(ptr));
        const binding = self.catalog_binding orelse return .{ .source = self.candidateSource() };
        const batch = try alloc.create(BoundBatch);
        errdefer alloc.destroy(batch);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = try arena.allocator().alloc([]const u8, names.len);
        for (names, owned) |name, *copy| copy.* = try arena.allocator().dupe(u8, name);
        batch.* = .{ .arena = arena, .raw = .{ .reads = self.reads, .consistency = self.consistency }, .binding = binding, .names = owned };
        return .{ .source = .{ .ptr = batch, .vtable = &BoundBatch.vtable }, .release = BoundBatch.release };
    }

    fn getFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table: []const u8,
        key: []const u8,
    ) anyerror!?[]u8 {
        const self: *DistributedCandidateSource = @ptrCast(@alignCast(ptr));
        const physical = if (self.catalog_binding) |binding| try binding.bindOne(allocator, table) else table;
        defer if (self.catalog_binding != null) allocator.free(physical);
        var resp = (try self.reads.lookup(allocator, physical, key, .{}, self.consistency)) orelse return null;
        defer resp.deinit(allocator);
        return try allocator.dupe(u8, resp.json);
    }

    fn scanPrefixFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table: []const u8,
        prefix: []const u8,
        opts: CandidateSource.ScanOptions,
        ctx: *anyopaque,
        consume: CandidateSource.Consume,
    ) anyerror!void {
        const self: *DistributedCandidateSource = @ptrCast(@alignCast(ptr));
        const physical = if (self.catalog_binding) |binding| try binding.bindOne(allocator, table) else table;
        defer if (self.catalog_binding != null) allocator.free(physical);
        // [prefix, prefixUpperBound) covers exactly the keys under `prefix`.
        const upper = (try prefixUpperBoundAlloc(allocator, prefix)) orelse return;
        defer allocator.free(upper);

        var resp = (try self.reads.scan(allocator, physical, prefix, upper, .{
            .inclusive_from = true,
            .exclusive_to = true,
            .include_documents = true,
            .limit = @intCast(@min(opts.limit, std.math.maxInt(u32))),
        }, self.consistency)) orelse return;
        defer resp.deinit(allocator);

        var lines = std.mem.splitScalar(u8, resp.ndjson, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            try consumeScanLine(allocator, line, ctx, consume);
        }
    }

    fn nearestFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table: []const u8,
        query: CandidateSource.NearestQuery,
        ctx: *anyopaque,
        consume: CandidateSource.Consume,
    ) anyerror!void {
        const self: *DistributedCandidateSource = @ptrCast(@alignCast(ptr));
        const physical = if (self.catalog_binding) |binding| try binding.bindOne(allocator, table) else table;
        defer if (self.catalog_binding != null) allocator.free(physical);
        const limit: u32 = @intCast(@min(query.k, std.math.maxInt(u32)));
        const dense_query = db_mod.types.DenseKnnQuery{ .vector = query.embedding, .k = limit };
        const named_queries = [_]db_mod.types.NamedDenseQuery{.{
            .name = "resolver_candidates",
            .index_name = query.index_name,
            .query = dense_query,
        }};
        const req = db_mod.types.SearchRequest{
            .dense = if (query.index_name.len == 0) dense_query else null,
            .dense_queries = if (query.index_name.len == 0) &.{} else named_queries[0..],
            .limit = limit,
            .include_stored = true,
            .include_all_fields = true,
        };
        var resp = (try self.reads.query(allocator, physical, req, self.consistency)) orelse return;
        defer resp.deinit(allocator);
        try consumeQueryHits(allocator, resp.json, ctx, consume);
    }
};

/// Smallest key strictly greater than every key sharing `prefix`, obtained by
/// incrementing the last non-`0xff` byte and truncating after it. Returns null
/// when `prefix` is empty or all `0xff` (an unbounded range), in which case the
/// caller skips the scan rather than reading the whole table.
fn prefixUpperBoundAlloc(allocator: std.mem.Allocator, prefix: []const u8) !?[]u8 {
    var end = prefix.len;
    while (end > 0 and prefix[end - 1] == 0xff) end -= 1;
    if (end == 0) return null;
    const out = try allocator.alloc(u8, end);
    @memcpy(out, prefix[0..end]);
    out[end - 1] += 1;
    return out;
}

/// A scan ndjson row is the entity document object with an extra `"key"` field
/// (see `table_reads.appendScanLine`). Pull the key out and hand the row to the
/// worker as the candidate value; the spurious `"key"` field is harmless because
/// the matcher only reads the fields its comparisons name.
fn consumeScanLine(
    allocator: std.mem.Allocator,
    line: []const u8,
    ctx: *anyopaque,
    consume: CandidateSource.Consume,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const key = switch (parsed.value.object.get("key") orelse return) {
        .string => |s| s,
        else => return,
    };
    try consume(ctx, key, line);
}

/// The public query envelope is `{"responses":[{"hits":{"hits":[{"_id","_source"}]}}]}`
/// (see `query_contract`). Re-serialize each hit's `_source` document and hand
/// it to the worker keyed by `_id`.
fn consumeQueryHits(
    allocator: std.mem.Allocator,
    body: []const u8,
    ctx: *anyopaque,
    consume: CandidateSource.Consume,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const responses = switch (parsed.value.object.get("responses") orelse return) {
        .array => |a| a,
        else => return,
    };
    if (responses.items.len == 0) return;
    const first = responses.items[0];
    if (first != .object) return;
    const hits_obj = switch (first.object.get("hits") orelse return) {
        .object => |o| o,
        else => return,
    };
    const hits = switch (hits_obj.get("hits") orelse return) {
        .array => |a| a,
        else => return,
    };
    for (hits.items) |hit| {
        if (hit != .object) continue;
        const id = switch (hit.object.get("_id") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const source = hit.object.get("_source") orelse continue;
        if (source == .null) continue;
        const value = try std.json.Stringify.valueAlloc(allocator, source, .{});
        defer allocator.free(value);
        try consume(ctx, id, value);
    }
}

const testing = std.testing;

/// Fake routing-aware read source: serves a single table from in-memory maps so
/// the adapter's three blocking modes can be exercised without a live cluster.
const FakeTableReadSource = struct {
    alloc: std.mem.Allocator,
    table: []const u8,
    docs: std.StringHashMapUnmanaged([]const u8) = .empty,
    /// Canned query envelope returned by `query` (the vector path).
    query_body: []const u8 = "",
    last_query_k: u32 = 0,
    last_query_index: []const u8 = "",

    fn source(self: *FakeTableReadSource) table_reads.TableReadSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = table_reads.TableReadSource.VTable{
        .lookup = lookup,
        .scan = scan,
        .query = query,
    };

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?table_reads.LookupResponse {
        _ = opts;
        _ = consistency;
        const self: *FakeTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table)) return null;
        const doc = self.docs.get(key) orelse return null;
        return .{ .json = try alloc.dupe(u8, doc), .version = 1 };
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?table_reads.ScanResponse {
        _ = opts;
        _ = consistency;
        const self: *FakeTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table)) return null;

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        var it = self.docs.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            if (std.mem.order(u8, key, from_key) == .lt) continue;
            if (std.mem.order(u8, key, to_key) != .lt) continue;
            // Emit {"key":"<key>", <doc fields>} like the real scan encoder.
            const doc = e.value_ptr.*;
            const escaped_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
            defer alloc.free(escaped_key);
            try out.appendSlice(alloc, "{\"key\":");
            try out.appendSlice(alloc, escaped_key);
            if (doc.len > 2) {
                try out.append(alloc, ',');
                try out.appendSlice(alloc, doc[1..]);
            } else try out.append(alloc, '}');
            try out.append(alloc, '\n');
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?@import("query.zig").QueryResponse {
        _ = consistency;
        const self: *FakeTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table)) return null;
        if (req.dense_queries.len > 0) {
            self.last_query_k = req.dense_queries[0].query.k;
            self.last_query_index = req.dense_queries[0].index_name;
        } else {
            self.last_query_k = if (req.dense) |d| d.k else 0;
            self.last_query_index = "";
        }
        return .{ .json = try alloc.dupe(u8, self.query_body) };
    }
};

const CollectCtx = struct {
    alloc: std.mem.Allocator,
    keys: std.ArrayListUnmanaged([]u8) = .empty,
    values: std.ArrayListUnmanaged([]u8) = .empty,

    fn deinit(self: *CollectCtx) void {
        for (self.keys.items) |k| self.alloc.free(k);
        for (self.values.items) |v| self.alloc.free(v);
        self.keys.deinit(self.alloc);
        self.values.deinit(self.alloc);
    }

    fn consume(ptr: *anyopaque, entity_key: []const u8, value: []const u8) anyerror!void {
        const self: *CollectCtx = @ptrCast(@alignCast(ptr));
        try self.keys.append(self.alloc, try self.alloc.dupe(u8, entity_key));
        try self.values.append(self.alloc, try self.alloc.dupe(u8, value));
    }
};

test "prefixUpperBoundAlloc increments the last byte and handles 0xff tails" {
    const alloc = testing.allocator;
    {
        const ub = (try prefixUpperBoundAlloc(alloc, "person/")).?;
        defer alloc.free(ub);
        try testing.expectEqualStrings("person0", ub); // '/' (0x2f) -> '0' (0x30)
    }
    {
        const ub = (try prefixUpperBoundAlloc(alloc, &.{ 'a', 0xff, 0xff })).?;
        defer alloc.free(ub);
        try testing.expectEqualSlices(u8, &.{'b'}, ub);
    }
    try testing.expect((try prefixUpperBoundAlloc(alloc, "")) == null);
    try testing.expect((try prefixUpperBoundAlloc(alloc, &.{ 0xff, 0xff })) == null);
}

test "DistributedCandidateSource get fetches an entity document across the read source" {
    const alloc = testing.allocator;
    var fake = FakeTableReadSource{ .alloc = alloc, .table = "entities" };
    defer fake.docs.deinit(alloc);
    try fake.docs.put(alloc, "person/ada_lovelace",
        \\{"canonical_name":"Ada Lovelace","label":"person"}
    );

    var dcs = DistributedCandidateSource{ .reads = fake.source() };
    const src = dcs.candidateSource();

    const got = (try src.get(alloc, "entities", "person/ada_lovelace")).?;
    defer alloc.free(got);
    try testing.expect(std.mem.indexOf(u8, got, "Ada Lovelace") != null);

    try testing.expect((try src.get(alloc, "entities", "person/missing")) == null);
}

test "DistributedCandidateSource scan_prefix returns only keys under the prefix" {
    const alloc = testing.allocator;
    var fake = FakeTableReadSource{ .alloc = alloc, .table = "entities" };
    defer fake.docs.deinit(alloc);
    try fake.docs.put(alloc, "person/ada_lovelace",
        \\{"canonical_name":"Ada Lovelace","label":"person"}
    );
    try fake.docs.put(alloc, "person/alan_turing",
        \\{"canonical_name":"Alan Turing","label":"person"}
    );
    try fake.docs.put(alloc, "org/antfly",
        \\{"canonical_name":"Antfly","label":"org"}
    );

    var dcs = DistributedCandidateSource{ .reads = fake.source() };
    const src = dcs.candidateSource();

    var ctx = CollectCtx{ .alloc = alloc };
    defer ctx.deinit();
    try src.scanPrefix(alloc, "entities", "person/", .{}, &ctx, CollectCtx.consume);

    try testing.expectEqual(@as(usize, 2), ctx.keys.items.len);
    for (ctx.keys.items) |k| try testing.expect(std.mem.startsWith(u8, k, "person/"));
    // The value carries the document fields the matcher reads.
    for (ctx.values.items) |v| try testing.expect(std.mem.indexOf(u8, v, "canonical_name") != null);
}

test "DistributedCandidateSource nearest parses query hits into candidates" {
    const alloc = testing.allocator;
    var fake = FakeTableReadSource{ .alloc = alloc, .table = "entities" };
    defer fake.docs.deinit(alloc);
    fake.query_body =
        \\{"responses":[{"hits":{"total":{"value":1,"relation":"exact"},"hits":[
        \\  {"_id":"person/ada_lovelace","_score":0.98,"_source":{"canonical_name":"Ada Lovelace","label":"person"}}
        \\]}}]}
    ;

    var dcs = DistributedCandidateSource{ .reads = fake.source() };
    const src = dcs.candidateSource();

    var ctx = CollectCtx{ .alloc = alloc };
    defer ctx.deinit();
    const embedding = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    try src.nearest(alloc, "entities", .{
        .index_name = "name_embedding",
        .embedding = &embedding,
        .k = 25,
    }, &ctx, CollectCtx.consume);

    try testing.expectEqual(@as(u32, 25), fake.last_query_k);
    try testing.expectEqualStrings("name_embedding", fake.last_query_index);
    try testing.expectEqual(@as(usize, 1), ctx.keys.items.len);
    try testing.expectEqualStrings("person/ada_lovelace", ctx.keys.items[0]);
    try testing.expect(std.mem.indexOf(u8, ctx.values.items[0], "Ada Lovelace") != null);
}

test "DistributedCandidateSource system catalog batch binds once and retains the old destination" {
    const alloc = testing.allocator;
    const Binding = struct {
        calls: usize = 0,
        physical: []const u8 = "table:old",
        fn bind(ptr: *anyopaque, a: std.mem.Allocator, names: []const []const u8) anyerror![][]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try testing.expectEqual(@as(usize, 1), names.len);
            try testing.expectEqualStrings("entities", names[0]);
            const result = try a.alloc([]u8, 1);
            errdefer a.free(result);
            result[0] = try a.dupe(u8, self.physical);
            return result;
        }
    };
    var binding: Binding = .{};
    var fake = FakeTableReadSource{ .alloc = alloc, .table = "table:old" };
    defer fake.docs.deinit(alloc);
    try fake.docs.put(alloc, "person/ada", "{\"canonical_name\":\"Ada\"}");
    var adapter = DistributedCandidateSource{ .reads = fake.source(), .catalog_binding = .{ .ptr = &binding, .bind_fn = Binding.bind } };
    const batch = try adapter.candidateSource().beginBatch(alloc, &.{"entities"});
    defer batch.deinit(alloc);
    // Beginning a batch with no source artifact performs no metadata I/O.
    try testing.expectEqual(@as(usize, 0), binding.calls);
    for (0..100) |_| {
        const doc = (try batch.source.get(alloc, "entities", "person/ada")).?;
        alloc.free(doc);
        binding.physical = "table:replacement";
    }
    try testing.expectEqual(@as(usize, 1), binding.calls);
    try testing.expectEqualStrings("table:old", (try batch.source.boundTable("entities")).?);
    const next = try adapter.candidateSource().beginBatch(alloc, &.{"entities"});
    defer next.deinit(alloc);
    try testing.expectEqualStrings("table:replacement", (try next.source.boundTable("entities")).?);
    try testing.expectEqual(@as(usize, 2), binding.calls);
}
