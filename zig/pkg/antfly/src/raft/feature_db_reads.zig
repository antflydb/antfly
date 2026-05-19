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

const std = @import("std");
const db_mod = @import("../storage/db/mod.zig");
const db_query_search = @import("../storage/db/query/search_exec.zig");
const feature_reads = @import("feature_reads.zig");
const read_gate = @import("read_gate.zig");

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

pub const FeatureDBReads = struct {
    group_id: u64,
    reads: feature_reads.FeatureReads,

    pub fn init(group_id: u64, requester: read_gate.ReadableLeaseRequester) FeatureDBReads {
        return .{
            .group_id = group_id,
            .reads = feature_reads.FeatureReads.init(requester),
        };
    }

    pub fn initCallback(
        group_id: u64,
        callback_requester: *const read_gate.CallbackReadableLeaseRequester,
    ) FeatureDBReads {
        return init(group_id, callback_requester.requester());
    }

    pub fn lookup(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
    ) !?db_mod.types.LookupResult {
        return try self.lookupWithConsistency(alloc, db, key, opts, .read_index);
    }

    pub fn lookupWithConsistency(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?db_mod.types.LookupResult {
        try self.reads.prepareLookupWithConsistency(self.group_id, key, opts, consistency);
        return try db.lookup(alloc, key, opts);
    }

    pub fn search(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        req: db_mod.types.SearchRequest,
    ) !db_mod.types.SearchResult {
        return try self.searchWithConsistency(alloc, db, req, .read_index);
    }

    pub fn searchWithConsistency(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        req: db_mod.types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !db_mod.types.SearchResult {
        try self.reads.prepareSearchWithConsistency(self.group_id, req, consistency);
        return try db.search(alloc, req);
    }

    pub fn searchDenseProfiledWithConsistency(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        req: db_mod.types.SearchRequest,
        dense: db_mod.types.DenseKnnQuery,
        consistency: read_gate.ReadConsistency,
    ) !db_query_search.ProfiledDenseSearchResult {
        try self.reads.prepareSearchWithConsistency(self.group_id, req, consistency);
        return try db.searchDenseProfiled(alloc, req, dense);
    }

    pub fn scan(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
    ) !db_mod.types.ScanResult {
        return try self.scanWithConsistency(alloc, db, from_key, to_key, opts, .read_index);
    }

    pub fn scanWithConsistency(
        self: FeatureDBReads,
        alloc: std.mem.Allocator,
        db: *db_mod.DB,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !db_mod.types.ScanResult {
        try self.reads.prepareScanWithConsistency(self.group_id, from_key, to_key, opts, consistency);
        return try db.scan(alloc, from_key, to_key, opts);
    }
};

test "feature db reads honor per-read consistency" {
    const Recorder = struct {
        group_ids: [3]u64 = .{ 0, 0, 0 },
        contexts: [3][32]u8 = [_][32]u8{[_]u8{0} ** 32} ** 3,
        context_lens: [3]usize = .{ 0, 0, 0 },
        count: usize = 0,

        fn requester(self: *@This()) read_gate.ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{
                    .request_readable_lease = requestReadableLease,
                },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.count >= self.contexts.len or request_ctx.len > self.contexts[self.count].len) return error.TestUnexpectedResult;
            self.group_ids[self.count] = group_id;
            @memcpy(self.contexts[self.count][0..request_ctx.len], request_ctx);
            self.context_lens[self.count] = request_ctx.len;
            self.count += 1;
        }
    };

    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-feature-db-reads";
    cleanupTestDir(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        cleanupTestDir(path);
    }

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
            },
        },
        .sync_level = .full_index,
    });

    var recorder = Recorder{};
    const reads = FeatureDBReads.init(77, recorder.requester());

    var lookup = (try reads.lookupWithConsistency(alloc, &db, "doc:a", .{}, .leader_lease)).?;
    defer lookup.deinit(alloc);

    var scan = try reads.scanWithConsistency(alloc, &db, "doc:a", "doc:z", .{
        .include_documents = false,
        .limit = 10,
    }, .stale);
    defer scan.deinit(alloc);

    var search = try reads.searchWithConsistency(alloc, &db, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0 },
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
    }, .read_index);
    defer search.deinit();

    try std.testing.expectEqual(@as(usize, 2), recorder.count);
    try std.testing.expectEqual(@as(u64, 77), recorder.group_ids[0]);
    try std.testing.expectEqual(@as(u64, 77), recorder.group_ids[1]);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.contexts[0][0..recorder.context_lens[0]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[1][0..recorder.context_lens[1]]);
    try std.testing.expectEqual(@as(u32, 1), search.total_hits);
}

test "feature db reads can use callback requester wrapper" {
    const Recorder = struct {
        count: usize = 0,

        fn callback(ctx: ?*anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.count += 1;
        }
    };

    var recorder = Recorder{};
    const callback_requester = read_gate.CallbackReadableLeaseRequester.init(&recorder, Recorder.callback);
    const reads = FeatureDBReads.initCallback(9, &callback_requester);
    try reads.reads.prepareLookup(9, "doc:a", .{});
    try std.testing.expectEqual(@as(usize, 1), recorder.count);
}
