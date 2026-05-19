// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly-zig");
const platform_time = antfly.platform_time;

const db_mod = antfly.db;
const embedder_mod = db_mod.embedder;
const raft_mod = antfly.raft;
const schema_mod = antfly.schema;

const CompatReadableLeaseObserver = struct {
    request_count: usize = 0,
    last_group_id: u64 = 0,
    last_request_ctx: [64]u8 = undefined,
    last_request_ctx_len: usize = 0,

    fn callback(ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *CompatReadableLeaseObserver = @ptrCast(@alignCast(ctx.?));
        self.request_count += 1;
        self.last_group_id = group_id;
        if (request_ctx.len > self.last_request_ctx.len) return error.InvalidArgument;
        @memcpy(self.last_request_ctx[0..request_ctx.len], request_ctx);
        self.last_request_ctx_len = request_ctx.len;
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    _ = args.next() orelse return error.InvalidArguments;
    const input_path = args.next() orelse {
        std.debug.print("usage: compat_runner <case_dir_or_cases_root>\n", .{});
        return error.InvalidArguments;
    };
    if (args.next() != null) {
        std.debug.print("usage: compat_runner <case_dir_or_cases_root>\n", .{});
        return error.InvalidArguments;
    }

    const executed = try runPath(init.io, alloc, input_path);
    std.debug.print("PASS {d} compat case(s)\n", .{executed});
}

fn runPath(io: std.Io, alloc: std.mem.Allocator, input_path: []const u8) !usize {
    if (try isCaseDir(io, input_path)) {
        try runCase(alloc, input_path);
        return 1;
    }

    var root_dir = try std.Io.Dir.cwd().openDir(io, input_path, .{ .iterate = true });
    defer root_dir.close(io);

    var walker = try root_dir.walk(alloc);
    defer walker.deinit();

    var cases = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (cases.items) |case_path| alloc.free(case_path);
        cases.deinit(alloc);
    }

    while (try walker.next(io)) |entry| {
        if (entry.kind != .directory) continue;
        const case_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ input_path, entry.path });
        errdefer alloc.free(case_path);
        if (try isCaseDir(io, case_path)) {
            try cases.append(alloc, case_path);
        } else {
            alloc.free(case_path);
        }
    }

    std.mem.sort([]u8, cases.items, {}, struct {
        fn lessThan(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    for (cases.items) |case_path| {
        try runCase(alloc, case_path);
    }

    return cases.items.len;
}

fn runCase(alloc: std.mem.Allocator, case_dir: []const u8) !void {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf);
    defer cleanupTempDir(tmp_path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try db_mod.DB.open(alloc, std.mem.span(tmp_path), .{
        .enrichment = .{
            .owner_id = "compat-runner",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();
    var txn_ids = std.StringHashMapUnmanaged(db_mod.types.TxnId).empty;
    var lease_observer = CompatReadableLeaseObserver{};
    defer {
        var it = txn_ids.iterator();
        while (it.next()) |entry| alloc.free(entry.key_ptr.*);
        txn_ids.deinit(alloc);
    }

    try loadSchema(alloc, &db, case_dir);
    try loadEnrichments(alloc, &db, case_dir);
    try loadIndexes(alloc, &db, case_dir);
    try applyOps(alloc, &db, &txn_ids, std.mem.span(tmp_path), case_dir);
    try runQueriesAndValidate(alloc, &db, txn_ids, case_dir, &lease_observer);

    std.debug.print("PASS {s}\n", .{case_dir});
}

fn isCaseDir(io: std.Io, path: []const u8) !bool {
    const required = [_][]const u8{
        "indexes.json",
        "ops.ndjson",
        "queries.json",
        "expected.json",
    };

    for (required) |file_name| {
        const file_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ path, file_name });
        defer std.heap.page_allocator.free(file_path);
        std.Io.Dir.cwd().access(io, file_path, .{}) catch return false;
    }
    return true;
}

fn loadIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, case_dir: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/indexes.json", .{case_dir});
    defer alloc.free(path);

    const raw = try readFileAlloc(alloc, path);
    defer alloc.free(raw);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    if (parsed.value != .array) return error.InvalidCompatCase;
    for (parsed.value.array.items) |item| {
        if (item != .object) return error.InvalidCompatCase;
        const name = item.object.get("name") orelse return error.InvalidCompatCase;
        const kind = item.object.get("kind") orelse return error.InvalidCompatCase;
        const config_json = if (item.object.get("config")) |config|
            try stringifyJsonValue(alloc, config)
        else
            try alloc.dupe(u8, "{}");
        defer alloc.free(config_json);

        try db.addIndex(.{
            .name = name.string,
            .kind = parseIndexKind(kind.string),
            .config_json = config_json,
        });
    }
}

fn loadEnrichments(alloc: std.mem.Allocator, db: *db_mod.DB, case_dir: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/enrichments.json", .{case_dir});
    defer alloc.free(path);

    if (!fileExists(path)) return;

    const raw = try readFileAlloc(alloc, path);
    defer alloc.free(raw);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    if (parsed.value != .array) return error.InvalidCompatCase;
    for (parsed.value.array.items) |item| {
        if (item != .object) return error.InvalidCompatCase;
        const name = item.object.get("name") orelse return error.InvalidCompatCase;
        const kind = item.object.get("kind") orelse return error.InvalidCompatCase;
        const source_field = item.object.get("source_field") orelse return error.InvalidCompatCase;
        try db.addEnrichment(.{
            .name = name.string,
            .kind = parseEnrichmentKind(kind.string),
            .source_field = source_field.string,
            .source_artifact_name = if (item.object.get("source_artifact_name")) |value| value.string else "",
            .expected_dims = if (item.object.get("expected_dims")) |value| try parseU32JsonValue(value) else 0,
            .chunk_size = if (item.object.get("chunk_size")) |value| try parseU32JsonValue(value) else 0,
            .chunk_overlap = if (item.object.get("chunk_overlap")) |value| try parseU32JsonValue(value) else 0,
        });
    }
}

fn loadSchema(alloc: std.mem.Allocator, db: *db_mod.DB, case_dir: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/schema.json", .{case_dir});
    defer alloc.free(path);

    if (!fileExists(path)) return;

    const raw = try readFileAlloc(alloc, path);
    defer alloc.free(raw);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidCompatCase;

    var schema = schema_mod.TableSchema{};
    if (parsed.value.object.get("version")) |version| schema.version = @intCast(version.integer);
    if (parsed.value.object.get("default_type")) |default_type| schema.default_type = default_type.string;
    if (parsed.value.object.get("ttl_duration_ns")) |ttl_duration_ns| schema.ttl_duration_ns = @intCast(ttl_duration_ns.integer);
    if (parsed.value.object.get("ttl_field")) |ttl_field| schema.ttl_field = ttl_field.string;
    if (parsed.value.object.get("enforce_types")) |enforce_types| schema.enforce_types = enforce_types.bool;

    if (schema.version == 0 and schema.default_type.len == 0 and schema.ttl_duration_ns == 0 and !schema.enforce_types) return;
    try db.setSchema(schema);
}

fn applyOps(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    txn_ids: *std.StringHashMapUnmanaged(db_mod.types.TxnId),
    db_path: []const u8,
    case_dir: []const u8,
) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/ops.ndjson", .{case_dir});
    defer alloc.free(path);

    const raw = try readFileAlloc(alloc, path);
    defer alloc.free(raw);

    var lines = std.mem.tokenizeScalar(u8, raw, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;

        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, line, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidCompatCase;

        const op = parsed.value.object.get("op") orelse return error.InvalidCompatCase;
        if (std.mem.eql(u8, op.string, "reopen")) {
            db.close();
            var deterministic = embedder_mod.DeterministicDenseEmbedder{};
            db.* = try db_mod.DB.open(alloc, db_path, .{
                .enrichment = .{
                    .owner_id = "compat-runner",
                    .dense_embedder = deterministic.interface(),
                },
            });
            continue;
        }
        if (std.mem.eql(u8, op.string, "begin_transaction")) {
            const txn_name = (parsed.value.object.get("txn") orelse return error.InvalidCompatCase).string;
            const timestamp_ns = @as(u64, @intCast((parsed.value.object.get("timestamp_ns") orelse return error.InvalidCompatCase).integer));
            const txn_id = try db.beginTransaction(timestamp_ns);
            const owned_name = try alloc.dupe(u8, txn_name);
            errdefer alloc.free(owned_name);
            try txn_ids.put(alloc, owned_name, txn_id);
            continue;
        }
        if (std.mem.eql(u8, op.string, "write_transaction")) {
            const txn_name = (parsed.value.object.get("txn") orelse return error.InvalidCompatCase).string;
            const txn_id = txn_ids.get(txn_name) orelse return error.InvalidCompatCase;
            const expect_error = if (parsed.value.object.get("expect_error")) |value| value.string else null;

            const writes_val = parsed.value.object.get("writes") orelse return error.InvalidCompatCase;
            const deletes_val = parsed.value.object.get("deletes") orelse return error.InvalidCompatCase;

            var writes = std.ArrayListUnmanaged(db_mod.types.TransactionWrite).empty;
            defer {
                for (writes.items) |write| alloc.free(@constCast(write.value));
                writes.deinit(alloc);
            }
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(alloc);
            var predicates = std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate).empty;
            defer predicates.deinit(alloc);

            if (writes_val != .array or deletes_val != .array) return error.InvalidCompatCase;
            for (writes_val.array.items) |write_item| {
                if (write_item != .object) return error.InvalidCompatCase;
                const key = write_item.object.get("key") orelse return error.InvalidCompatCase;
                const value = write_item.object.get("value") orelse return error.InvalidCompatCase;
                try writes.append(alloc, .{
                    .key = key.string,
                    .value = try stringifyJsonValue(alloc, value),
                });
            }
            for (deletes_val.array.items) |delete_item| {
                try deletes.append(alloc, delete_item.string);
            }
            if (parsed.value.object.get("predicates")) |predicates_val| {
                if (predicates_val != .array) return error.InvalidCompatCase;
                for (predicates_val.array.items) |predicate_item| {
                    if (predicate_item != .object) return error.InvalidCompatCase;
                    try predicates.append(alloc, .{
                        .key = (predicate_item.object.get("key") orelse return error.InvalidCompatCase).string,
                        .expected_version = @intCast((predicate_item.object.get("expected_version") orelse return error.InvalidCompatCase).integer),
                    });
                }
            }

            db.writeTransaction(txn_id, .{
                .writes = writes.items,
                .deletes = deletes.items,
                .predicates = predicates.items,
            }) catch |err| {
                if (expect_error) |expected| {
                    if (matchesCompatTxnError(err, expected)) continue;
                }
                return err;
            };
            if (expect_error != null) return error.ExpectedCompatError;
            continue;
        }
        if (std.mem.eql(u8, op.string, "commit_transaction")) {
            const txn_name = (parsed.value.object.get("txn") orelse return error.InvalidCompatCase).string;
            const txn_id = txn_ids.get(txn_name) orelse return error.InvalidCompatCase;
            const timestamp_ns = @as(u64, @intCast((parsed.value.object.get("timestamp_ns") orelse return error.InvalidCompatCase).integer));
            try db.commitTransaction(txn_id, timestamp_ns);
            continue;
        }
        if (std.mem.eql(u8, op.string, "abort_transaction")) {
            const txn_name = (parsed.value.object.get("txn") orelse return error.InvalidCompatCase).string;
            const txn_id = txn_ids.get(txn_name) orelse return error.InvalidCompatCase;
            const timestamp_ns = @as(u64, @intCast((parsed.value.object.get("timestamp_ns") orelse return error.InvalidCompatCase).integer));
            try db.abortTransaction(txn_id, timestamp_ns);
            continue;
        }
        if (!std.mem.eql(u8, op.string, "batch")) return error.InvalidCompatCase;

        const writes_val = parsed.value.object.get("writes") orelse return error.InvalidCompatCase;
        const deletes_val = parsed.value.object.get("deletes") orelse return error.InvalidCompatCase;
        const sync_level = if (parsed.value.object.get("sync_level")) |sync_level_val|
            parseSyncLevel(sync_level_val.string)
        else
            db_mod.types.SyncLevel.write;
        const timestamp_ns: u64 = if (parsed.value.object.get("timestamp_ns")) |timestamp_val|
            @intCast(timestamp_val.integer)
        else
            0;

        var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
        defer {
            for (writes.items) |write| alloc.free(@constCast(write.value));
            writes.deinit(alloc);
        }

        if (writes_val != .array) return error.InvalidCompatCase;
        for (writes_val.array.items) |write_item| {
            if (write_item != .object) return error.InvalidCompatCase;
            const key = write_item.object.get("key") orelse return error.InvalidCompatCase;
            const value = write_item.object.get("value") orelse return error.InvalidCompatCase;
            const value_json = try stringifyJsonValue(alloc, value);
            try writes.append(alloc, .{
                .key = key.string,
                .value = value_json,
            });
        }

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        var predicates = std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate).empty;
        defer predicates.deinit(alloc);

        if (deletes_val != .array) return error.InvalidCompatCase;
        for (deletes_val.array.items) |delete_item| {
            try deletes.append(alloc, delete_item.string);
        }

        if (parsed.value.object.get("predicates")) |predicates_val| {
            if (predicates_val != .array) return error.InvalidCompatCase;
            for (predicates_val.array.items) |predicate_item| {
                if (predicate_item != .object) return error.InvalidCompatCase;
                try predicates.append(alloc, .{
                    .key = (predicate_item.object.get("key") orelse return error.InvalidCompatCase).string,
                    .expected_version = @intCast((predicate_item.object.get("expected_version") orelse return error.InvalidCompatCase).integer),
                });
            }
        }

        try db.batch(.{
            .writes = writes.items,
            .deletes = deletes.items,
            .predicates = predicates.items,
            .timestamp_ns = timestamp_ns,
            .sync_level = sync_level,
        });
        try waitForDerivedWork(db);
    }
}

fn runQueriesAndValidate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    txn_ids: std.StringHashMapUnmanaged(db_mod.types.TxnId),
    case_dir: []const u8,
    lease_observer: *CompatReadableLeaseObserver,
) !void {
    var lease_requester = raft_mod.CallbackReadableLeaseRequester.init(lease_observer, CompatReadableLeaseObserver.callback);
    const feature_db_reads = raft_mod.FeatureDBReads.initCallback(1, &lease_requester);
    const queries_path = try std.fmt.allocPrint(alloc, "{s}/queries.json", .{case_dir});
    defer alloc.free(queries_path);
    const expected_path = try std.fmt.allocPrint(alloc, "{s}/expected.json", .{case_dir});
    defer alloc.free(expected_path);

    const queries_raw = try readFileAlloc(alloc, queries_path);
    defer alloc.free(queries_raw);
    const expected_raw = try readFileAlloc(alloc, expected_path);
    defer alloc.free(expected_raw);

    const queries = try std.json.parseFromSlice(std.json.Value, alloc, queries_raw, .{});
    defer queries.deinit();
    const expected = try std.json.parseFromSlice(std.json.Value, alloc, expected_raw, .{});
    defer expected.deinit();

    if (queries.value != .array or expected.value != .object) return error.InvalidCompatCase;

    for (queries.value.array.items) |query_item| {
        if (query_item != .object) return error.InvalidCompatCase;
        const name = (query_item.object.get("name") orelse return error.InvalidCompatCase).string;
        const request = query_item.object.get("request") orelse return error.InvalidCompatCase;
        const expected_result = expected.value.object.get(name) orelse return error.InvalidCompatCase;

        var arena_state = std.heap.ArenaAllocator.init(alloc);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        if (request != .object) return error.InvalidCompatCase;
        if (request.object.get("lookup")) |lookup_request| {
            const lookup_options = try parseLookupOptions(arena, lookup_request);
            const key = (lookup_request.object.get("key") orelse return error.InvalidCompatCase).string;
            const maybe_lookup = try feature_db_reads.lookup(alloc, db, key, lookup_options);
            if (maybe_lookup) |lookup_result_const| {
                var lookup_result = lookup_result_const;
                defer lookup_result.deinit(alloc);
                try validateLookupResult(alloc, lookup_result, expected_result);
            } else {
                try validateLookupResult(alloc, null, expected_result);
            }
            continue;
        }
        if (request.object.get("get_timestamp")) |timestamp_request| {
            if (timestamp_request != .object) return error.InvalidCompatCase;
            const key = (timestamp_request.object.get("key") orelse return error.InvalidCompatCase).string;
            const timestamp_ns = try db.getTimestamp(alloc, key);
            try validateTimestampResult(timestamp_ns, expected_result);
            continue;
        }
        if (request.object.get("get_transaction_status")) |status_request| {
            if (status_request != .object) return error.InvalidCompatCase;
            const txn_name = (status_request.object.get("txn") orelse return error.InvalidCompatCase).string;
            const txn_id = txn_ids.get(txn_name) orelse return error.InvalidCompatCase;
            const status = try db.getTransactionStatus(txn_id);
            try validateTransactionStatus(status, expected_result);
            continue;
        }

        const expected_total_hits = blk: {
            if (expected_result != .object) return error.InvalidCompatCase;
            const total_hits = expected_result.object.get("total_hits") orelse return error.InvalidCompatCase;
            break :blk @as(u32, @intCast(total_hits.integer));
        };

        var result = try waitForSearchResult(alloc, feature_db_reads, db, try parseSearchRequest(arena, request), expected_total_hits);
        defer result.deinit();
        try validateResult(alloc, result, expected_result);
    }
}

fn validateTransactionStatus(status: db_mod.types.TxnStatus, expected: std.json.Value) !void {
    if (expected != .object) return error.InvalidCompatCase;
    const status_val = expected.object.get("status") orelse return error.InvalidCompatCase;
    const expected_status = if (status_val == .string) status_val.string else return error.InvalidCompatCase;
    const actual_status = switch (status) {
        .pending => "pending",
        .committed => "committed",
        .aborted => "aborted",
    };
    try std.testing.expectEqualStrings(expected_status, actual_status);
}

fn matchesCompatTxnError(err: anyerror, expected: []const u8) bool {
    if (std.mem.eql(u8, expected, "version_conflict")) {
        return std.mem.eql(u8, @errorName(err), "VersionConflict");
    }
    if (std.mem.eql(u8, expected, "intent_conflict")) {
        return std.mem.eql(u8, @errorName(err), "IntentConflict");
    }
    return false;
}

fn parseLookupOptions(alloc: std.mem.Allocator, request: std.json.Value) !db_mod.types.LookupOptions {
    if (request != .object) return error.InvalidCompatCase;

    var result = db_mod.types.LookupOptions{};
    if (request.object.get("include_all_fields")) |include_all_fields| {
        result.include_all_fields = include_all_fields.bool;
    }
    if (request.object.get("fields")) |fields_val| {
        if (fields_val != .array) return error.InvalidCompatCase;
        const fields = try alloc.alloc([]const u8, fields_val.array.items.len);
        for (fields_val.array.items, 0..) |field, i| fields[i] = field.string;
        result.fields = fields;
    }
    return result;
}

fn validateLookupResult(alloc: std.mem.Allocator, actual: ?db_mod.types.LookupResult, expected: std.json.Value) !void {
    if (expected != .object) return error.InvalidCompatCase;
    const found = if (expected.object.get("found")) |found_val| found_val.bool else true;
    try std.testing.expectEqual(found, actual != null);
    if (!found) return;

    const json_val = expected.object.get("json") orelse return error.InvalidCompatCase;
    const actual_result = actual orelse return error.InvalidCompatCase;
    const actual_json = try std.json.parseFromSlice(std.json.Value, alloc, actual_result.json, .{});
    defer actual_json.deinit();
    try expectJsonEqual(actual_json.value, json_val);
}

fn validateTimestampResult(actual: u64, expected: std.json.Value) !void {
    if (expected != .object) return error.InvalidCompatCase;
    const timestamp_ns = expected.object.get("timestamp_ns") orelse return error.InvalidCompatCase;
    try std.testing.expectEqual(@as(u64, @intCast(timestamp_ns.integer)), actual);
}

fn validateResult(alloc: std.mem.Allocator, result: db_mod.types.SearchResult, expected: std.json.Value) !void {
    if (expected != .object) return error.InvalidCompatCase;
    const total_hits = expected.object.get("total_hits") orelse return error.InvalidCompatCase;
    try std.testing.expectEqual(@as(u32, @intCast(total_hits.integer)), result.total_hits);

    const hits = expected.object.get("hits") orelse return error.InvalidCompatCase;
    if (hits != .array) return error.InvalidCompatCase;
    try std.testing.expectEqual(hits.array.items.len, result.hits.len);

    for (hits.array.items, result.hits) |expected_hit, actual_hit| {
        try validateSearchHit(alloc, actual_hit, expected_hit);
    }

    if (expected.object.get("graph_results")) |graph_results| {
        if (graph_results != .object) return error.InvalidCompatCase;
        try std.testing.expectEqual(graph_results.object.count(), result.graph_results.len);
        var it = graph_results.object.iterator();
        while (it.next()) |entry| {
            const actual = findGraphResult(result.graph_results, entry.key_ptr.*) orelse return error.InvalidCompatCase;
            try validateGraphResult(alloc, actual, entry.value_ptr.*);
        }
    } else {
        try std.testing.expectEqual(@as(usize, 0), result.graph_results.len);
    }
}

fn parseSearchRequest(alloc: std.mem.Allocator, request: std.json.Value) !db_mod.types.SearchRequest {
    if (request != .object) return error.InvalidCompatCase;

    var result = db_mod.types.SearchRequest{};
    if (request.object.get("index_name")) |index_name| {
        result.index_name = index_name.string;
    }
    if (request.object.get("limit")) |limit| result.limit = @intCast(limit.integer);
    if (request.object.get("offset")) |offset| result.offset = @intCast(offset.integer);
    if (request.object.get("include_stored")) |include_stored| result.include_stored = include_stored.bool;
    if (request.object.get("include_all_fields")) |include_all_fields| result.include_all_fields = include_all_fields.bool;
    if (request.object.get("filter_prefix")) |filter_prefix| result.filter_prefix = filter_prefix.string;
    if (request.object.get("distance_over")) |distance_over| result.distance_over = @floatCast(distance_over.float);
    if (request.object.get("distance_under")) |distance_under| result.distance_under = @floatCast(distance_under.float);
    if (request.object.get("return_mode")) |return_mode| result.return_mode = parseReturnMode(return_mode.string);
    if (request.object.get("max_chunks_per_parent")) |max_chunks| result.max_chunks_per_parent = @intCast(max_chunks.integer);
    if (request.object.get("fields")) |fields_val| {
        if (fields_val != .array) return error.InvalidCompatCase;
        const fields = try alloc.alloc([]const u8, fields_val.array.items.len);
        for (fields_val.array.items, 0..) |field, i| fields[i] = field.string;
        result.fields = fields;
    }
    if (request.object.get("filter_ids")) |filter_ids| {
        if (filter_ids != .array) return error.InvalidCompatCase;
        const ids = try alloc.alloc(u64, filter_ids.array.items.len);
        for (filter_ids.array.items, 0..) |id, i| ids[i] = @intCast(id.integer);
        result.filter_ids = ids;
    }
    if (request.object.get("exclude_ids")) |exclude_ids| {
        if (exclude_ids != .array) return error.InvalidCompatCase;
        const ids = try alloc.alloc(u64, exclude_ids.array.items.len);
        for (exclude_ids.array.items, 0..) |id, i| ids[i] = @intCast(id.integer);
        result.exclude_ids = ids;
    }

    if (request.object.get("full_text")) |full_text| {
        result.full_text = try parseTextQuery(full_text);
    }
    if (request.object.get("full_text_queries")) |full_text_queries| {
        result.full_text_queries = try parseNamedFullTextQueries(alloc, full_text_queries, result.index_name);
    }
    if (request.object.get("dense")) |dense| {
        result.dense = try parseDenseQuery(alloc, dense, result.limit);
    }
    if (request.object.get("sparse")) |sparse| {
        result.sparse = try parseSparseQuery(alloc, sparse, result.limit);
    }
    if (request.object.get("dense_queries")) |dense_queries| {
        result.dense_queries = try parseNamedDenseQueries(alloc, dense_queries, result.limit);
    }
    if (request.object.get("sparse_queries")) |sparse_queries| {
        result.sparse_queries = try parseNamedSparseQueries(alloc, sparse_queries, result.limit);
    }
    if (request.object.get("graph_queries")) |graph_queries| {
        result.graph_queries = try parseNamedGraphQueries(alloc, graph_queries);
    }
    if (request.object.get("merge_config")) |merge_config| {
        result.merge_config = try parseMergeConfig(alloc, merge_config);
    }
    if (request.object.get("expand_strategy")) |expand_strategy| {
        result.expand_strategy = parseExpandStrategy(expand_strategy.string);
    }

    if (request.object.get("query")) |query| {
        result.query = try parseLegacyQuery(alloc, query, result.limit);
    }

    return result;
}

fn validateGraphResult(alloc: std.mem.Allocator, result: db_mod.types.GraphSearchResult, expected: std.json.Value) !void {
    if (expected != .object) return error.InvalidCompatCase;
    const total_hits = expected.object.get("total_hits") orelse return error.InvalidCompatCase;
    try std.testing.expectEqual(@as(u32, @intCast(total_hits.integer)), result.total_hits);

    const hits = expected.object.get("hits") orelse return error.InvalidCompatCase;
    if (hits != .array) return error.InvalidCompatCase;
    try std.testing.expectEqual(hits.array.items.len, result.hits.len);

    for (hits.array.items, result.hits) |expected_hit, actual_hit| {
        try validateSearchHit(alloc, actual_hit, expected_hit);
    }
}

fn validateSearchHit(alloc: std.mem.Allocator, actual_hit: db_mod.types.SearchHit, expected_hit: std.json.Value) !void {
    if (expected_hit != .object) return error.InvalidCompatCase;
    const expected_id = expected_hit.object.get("id") orelse return error.InvalidCompatCase;
    try std.testing.expectEqualStrings(expected_id.string, actual_hit.id);

    if (expected_hit.object.get("stored_data")) |expected_stored| {
        const actual_bytes = actual_hit.stored_data orelse return error.InvalidCompatCase;
        const actual_json = try std.json.parseFromSlice(std.json.Value, alloc, actual_bytes, .{});
        defer actual_json.deinit();
        try expectJsonEqual(actual_json.value, expected_stored);
    }

    if (expected_hit.object.get("chunk_hits")) |expected_chunks| {
        if (expected_chunks != .array) return error.InvalidCompatCase;
        try std.testing.expectEqual(expected_chunks.array.items.len, actual_hit.chunk_hits.len);
        for (expected_chunks.array.items, actual_hit.chunk_hits) |expected_chunk, actual_chunk| {
            try validateChunkHit(alloc, actual_chunk, expected_chunk);
        }
    } else {
        try std.testing.expectEqual(@as(usize, 0), actual_hit.chunk_hits.len);
    }
}

fn validateChunkHit(alloc: std.mem.Allocator, actual_hit: db_mod.types.ChunkHit, expected_hit: std.json.Value) !void {
    if (expected_hit != .object) return error.InvalidCompatCase;
    const expected_id = expected_hit.object.get("id") orelse return error.InvalidCompatCase;
    try std.testing.expectEqualStrings(expected_id.string, actual_hit.id);

    if (expected_hit.object.get("stored_data")) |expected_stored| {
        const actual_bytes = actual_hit.stored_data orelse return error.InvalidCompatCase;
        const actual_json = try std.json.parseFromSlice(std.json.Value, alloc, actual_bytes, .{});
        defer actual_json.deinit();
        try expectJsonEqual(actual_json.value, expected_stored);
    }
}

fn findGraphResult(results: []const db_mod.types.GraphSearchResult, name: []const u8) ?db_mod.types.GraphSearchResult {
    for (results) |result| {
        if (std.mem.eql(u8, result.name, name)) return result;
    }
    return null;
}

fn parseLegacyQuery(alloc: std.mem.Allocator, query: std.json.Value, limit: u32) !db_mod.types.Query {
    if (query != .object) return error.InvalidCompatCase;
    if (query.object.get("match_all") != null) {
        return .{ .match_all = {} };
    } else if (query.object.get("term")) |term| {
        if (term != .object) return error.InvalidCompatCase;
        return .{ .term = .{
            .field = (term.object.get("field") orelse return error.InvalidCompatCase).string,
            .term = (term.object.get("term") orelse return error.InvalidCompatCase).string,
        } };
    } else if (query.object.get("match")) |match| {
        if (match != .object) return error.InvalidCompatCase;
        return .{ .match = .{
            .field = (match.object.get("field") orelse return error.InvalidCompatCase).string,
            .text = (match.object.get("text") orelse return error.InvalidCompatCase).string,
        } };
    } else if (query.object.get("dense_knn")) |dense| {
        return .{ .dense_knn = try parseDenseQuery(alloc, dense, limit) };
    } else if (query.object.get("sparse_knn")) |sparse| {
        return .{ .sparse_knn = try parseSparseQuery(alloc, sparse, limit) };
    } else {
        return error.InvalidCompatCase;
    }
}

fn parseTextQuery(value: std.json.Value) !db_mod.types.TextQuery {
    if (value != .object) return error.InvalidCompatCase;
    if (value.object.get("match_all") != null) {
        return .{ .match_all = {} };
    } else if (value.object.get("term")) |term| {
        if (term != .object) return error.InvalidCompatCase;
        return .{ .term = .{
            .field = (term.object.get("field") orelse return error.InvalidCompatCase).string,
            .term = (term.object.get("term") orelse return error.InvalidCompatCase).string,
        } };
    } else if (value.object.get("match")) |match| {
        if (match != .object) return error.InvalidCompatCase;
        return .{ .match = .{
            .field = (match.object.get("field") orelse return error.InvalidCompatCase).string,
            .text = (match.object.get("text") orelse return error.InvalidCompatCase).string,
        } };
    }
    return error.InvalidCompatCase;
}

fn parseDenseQuery(alloc: std.mem.Allocator, value: std.json.Value, default_k: u32) !db_mod.types.DenseKnnQuery {
    if (value != .object) return error.InvalidCompatCase;
    const vector_val = value.object.get("vector") orelse return error.InvalidCompatCase;
    if (vector_val != .array) return error.InvalidCompatCase;
    const vector = try alloc.alloc(f32, vector_val.array.items.len);
    for (vector_val.array.items, 0..) |item, i| vector[i] = try jsonNumberToF32(item);
    return .{
        .vector = vector,
        .k = if (value.object.get("k")) |k| @intCast(k.integer) else default_k,
    };
}

fn parseSparseQuery(alloc: std.mem.Allocator, value: std.json.Value, default_k: u32) !db_mod.types.SparseKnnQuery {
    if (value != .object) return error.InvalidCompatCase;
    const indices_val = value.object.get("indices") orelse return error.InvalidCompatCase;
    const values_val = value.object.get("values") orelse return error.InvalidCompatCase;
    if (indices_val != .array or values_val != .array) return error.InvalidCompatCase;
    if (indices_val.array.items.len != values_val.array.items.len) return error.InvalidCompatCase;

    const indices = try alloc.alloc(u32, indices_val.array.items.len);
    const values = try alloc.alloc(f32, values_val.array.items.len);
    for (indices_val.array.items, 0..) |item, i| indices[i] = try jsonNumberToU32(item);
    for (values_val.array.items, 0..) |item, i| values[i] = try jsonNumberToF32(item);

    return .{
        .indices = indices,
        .values = values,
        .k = if (value.object.get("k")) |k| @intCast(k.integer) else default_k,
    };
}

fn parseNamedDenseQueries(alloc: std.mem.Allocator, value: std.json.Value, default_k: u32) ![]db_mod.types.NamedDenseQuery {
    if (value != .object) return error.InvalidCompatCase;
    var queries = try alloc.alloc(db_mod.types.NamedDenseQuery, value.object.count());
    var index: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        queries[index] = .{
            .name = entry.key_ptr.*,
            .index_name = entry.key_ptr.*,
            .query = try parseDenseQuery(alloc, entry.value_ptr.*, default_k),
        };
        index += 1;
    }
    return queries;
}

fn parseNamedFullTextQueries(alloc: std.mem.Allocator, value: std.json.Value, default_index_name: ?[]const u8) ![]db_mod.types.NamedFullTextQuery {
    if (value != .object) return error.InvalidCompatCase;
    var queries = try alloc.alloc(db_mod.types.NamedFullTextQuery, value.object.count());
    var index: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        var index_name = default_index_name orelse return error.InvalidCompatCase;
        var query_value = entry.value_ptr.*;
        if (query_value == .object) {
            if (query_value.object.get("index_name")) |explicit_index_name| {
                index_name = explicit_index_name.string;
            }
            if (query_value.object.get("query")) |nested_query| {
                query_value = nested_query;
            }
        }
        queries[index] = .{
            .name = entry.key_ptr.*,
            .index_name = index_name,
            .query = try parseTextQuery(query_value),
        };
        index += 1;
    }
    return queries;
}

fn parseNamedSparseQueries(alloc: std.mem.Allocator, value: std.json.Value, default_k: u32) ![]db_mod.types.NamedSparseQuery {
    if (value != .object) return error.InvalidCompatCase;
    var queries = try alloc.alloc(db_mod.types.NamedSparseQuery, value.object.count());
    var index: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        queries[index] = .{
            .name = entry.key_ptr.*,
            .index_name = entry.key_ptr.*,
            .query = try parseSparseQuery(alloc, entry.value_ptr.*, default_k),
        };
        index += 1;
    }
    return queries;
}

fn parseNamedGraphQueries(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.NamedGraphQuery {
    if (value != .object) return error.InvalidCompatCase;
    var queries = try alloc.alloc(db_mod.types.NamedGraphQuery, value.object.count());
    var index: usize = 0;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        queries[index] = .{
            .name = entry.key_ptr.*,
            .query = try parseGraphQuery(alloc, entry.value_ptr.*),
        };
        index += 1;
    }
    return queries;
}

fn parseGraphQuery(alloc: std.mem.Allocator, value: std.json.Value) !antfly.graph_query.GraphQuery {
    if (value != .object) return error.InvalidCompatCase;
    const kind = (value.object.get("type") orelse return error.InvalidCompatCase).string;
    const index_name = (value.object.get("index_name") orelse return error.InvalidCompatCase).string;
    const start_nodes = try parseNodeSelector(alloc, value.object.get("start_nodes") orelse return error.InvalidCompatCase);
    const params = if (value.object.get("params")) |params| try parseGraphQueryParams(alloc, params) else antfly.graph_query.QueryParams{};
    const target_nodes = if (value.object.get("target_nodes")) |target| try parseNodeSelector(alloc, target) else null;
    return .{
        .query_type = parseGraphQueryType(kind),
        .index_name = index_name,
        .start_nodes = start_nodes,
        .params = params,
        .target_nodes = target_nodes,
        .k = blk: {
            if (value.object.get("params")) |params_val| {
                if (params_val == .object) {
                    if (params_val.object.get("k")) |k| break :blk @intCast(k.integer);
                }
            }
            break :blk 1;
        },
    };
}

fn parseNodeSelector(alloc: std.mem.Allocator, value: std.json.Value) !antfly.graph_query.NodeSelector {
    if (value != .object) return error.InvalidCompatCase;
    if (value.object.get("keys")) |keys| {
        if (keys != .array) return error.InvalidCompatCase;
        const owned = try alloc.alloc([]const u8, keys.array.items.len);
        for (keys.array.items, 0..) |item, i| owned[i] = item.string;
        return .{ .keys = owned };
    }
    if (value.object.get("result_ref")) |result_ref| {
        return .{ .result_ref = .{
            .ref = result_ref.string,
            .limit = if (value.object.get("limit")) |limit| @intCast(limit.integer) else 0,
        } };
    }
    return error.InvalidCompatCase;
}

fn parseGraphQueryParams(alloc: std.mem.Allocator, value: std.json.Value) !antfly.graph_query.QueryParams {
    if (value != .object) return error.InvalidCompatCase;
    var params = antfly.graph_query.QueryParams{};
    if (value.object.get("edge_types")) |edge_types| {
        if (edge_types != .array) return error.InvalidCompatCase;
        const owned = try alloc.alloc([]const u8, edge_types.array.items.len);
        for (edge_types.array.items, 0..) |item, i| owned[i] = item.string;
        params.edge_types = owned;
    }
    if (value.object.get("direction")) |direction| params.direction = parseDirection(direction.string);
    if (value.object.get("max_depth")) |max_depth| params.max_depth = @intCast(max_depth.integer);
    if (value.object.get("max_results")) |max_results| params.max_results = @intCast(max_results.integer);
    if (value.object.get("min_weight")) |min_weight| params.min_weight = @floatCast(try jsonNumberToF32(min_weight));
    if (value.object.get("max_weight")) |max_weight| params.max_weight = @floatCast(try jsonNumberToF32(max_weight));
    if (value.object.get("deduplicate_nodes")) |dedup| params.deduplicate = dedup.bool;
    if (value.object.get("include_paths")) |include_paths| params.include_paths = include_paths.bool;
    if (value.object.get("weight_mode")) |weight_mode| params.weight_mode = parseWeightMode(weight_mode.string);
    return params;
}

fn parseMergeConfig(alloc: std.mem.Allocator, value: std.json.Value) !db_mod.types.MergeConfig {
    if (value != .object) return error.InvalidCompatCase;
    var config = db_mod.types.MergeConfig{};
    if (value.object.get("strategy")) |strategy| config.strategy = parseFusionStrategy(strategy.string);
    if (value.object.get("rank_constant")) |rank_constant| config.rank_constant = @floatCast(try jsonNumberToF32(rank_constant));
    if (value.object.get("window_size")) |window_size| config.window_size = @intCast(window_size.integer);
    if (value.object.get("weights")) |weights| {
        if (weights != .object) return error.InvalidCompatCase;
        var named = try alloc.alloc(antfly.fusion.NamedWeight, weights.object.count());
        var index: usize = 0;
        var it = weights.object.iterator();
        while (it.next()) |entry| {
            named[index] = .{
                .name = entry.key_ptr.*,
                .weight = try jsonNumberToF32(entry.value_ptr.*),
            };
            index += 1;
        }
        config.weights = named;
    }
    return config;
}

fn parseGraphQueryType(kind: []const u8) antfly.graph_query.QueryType {
    if (std.mem.eql(u8, kind, "traverse")) return .traverse;
    if (std.mem.eql(u8, kind, "neighbors")) return .neighbors;
    if (std.mem.eql(u8, kind, "shortest_path")) return .shortest_path;
    if (std.mem.eql(u8, kind, "k_shortest_paths")) return .k_shortest_paths;
    unreachable;
}

fn parseDirection(kind: []const u8) antfly.graph.EdgeDirection {
    if (std.mem.eql(u8, kind, "in")) return .in;
    if (std.mem.eql(u8, kind, "both")) return .both;
    return .out;
}

fn parseWeightMode(kind: []const u8) antfly.paths.PathWeightMode {
    if (std.mem.eql(u8, kind, "min_weight")) return .min_weight;
    if (std.mem.eql(u8, kind, "max_weight")) return .max_weight;
    return .min_hops;
}

fn parseFusionStrategy(kind: []const u8) antfly.fusion.FusionStrategy {
    if (std.mem.eql(u8, kind, "rsf")) return .rsf;
    return .rrf;
}

fn parseExpandStrategy(kind: []const u8) antfly.graph_query.ExpandStrategy {
    if (std.mem.eql(u8, kind, "intersection")) return .intersection;
    return .@"union";
}

fn parseReturnMode(kind: []const u8) db_mod.types.ReturnMode {
    if (std.mem.eql(u8, kind, "chunk")) return .chunk;
    if (std.mem.eql(u8, kind, "parent_with_chunks")) return .parent_with_chunks;
    return .parent;
}

fn parseSyncLevel(kind: []const u8) db_mod.types.SyncLevel {
    if (std.mem.eql(u8, kind, "full_index")) return .full_index;
    return .write;
}

fn waitForSearchResult(
    alloc: std.mem.Allocator,
    feature_db_reads: raft_mod.FeatureDBReads,
    db: *db_mod.DB,
    req: db_mod.types.SearchRequest,
    min_hits: u32,
) !db_mod.types.SearchResult {
    var last = try feature_db_reads.search(alloc, db, req);
    var attempts: usize = 0;
    while (last.total_hits < min_hits and attempts < 100) : (attempts += 1) {
        last.deinit();
        sleepNs(10 * std.time.ns_per_ms);
        last = try feature_db_reads.search(alloc, db, req);
    }
    return last;
}

fn waitForDerivedWork(db: *db_mod.DB) !void {
    var sequence = db.core.change_journal.lastSequence();
    if (sequence == 0) return;
    if (db.enrichment_runtime) |runtime| {
        try runtime.waitForApplied(sequence);
        sequence = db.core.change_journal.lastSequence();
        if (sequence == 0) return;
    }
    try db.executor.waitForAll(sequence);
}

test "compat readable lease observer records feature db reads" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-compat-read-observer";
    cleanupTempDir(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        cleanupTempDir(path);
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
    });

    var observer = CompatReadableLeaseObserver{};
    var requester = raft_mod.CallbackReadableLeaseRequester.init(&observer, CompatReadableLeaseObserver.callback);
    const reads = raft_mod.FeatureDBReads.initCallback(1, &requester);

    var lookup = (try reads.lookup(alloc, &db, "doc:a", .{})).?;
    defer lookup.deinit(alloc);

    var result = try reads.search(alloc, &db, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0 },
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), observer.request_count);
    try std.testing.expectEqual(@as(u64, 1), observer.last_group_id);
    try std.testing.expectEqualStrings("enrichment:search:read_index", observer.last_request_ctx[0..observer.last_request_ctx_len]);
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

fn jsonNumberToF32(value: std.json.Value) !f32 {
    return switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| @floatCast(f),
        .number_string => |s| try std.fmt.parseFloat(f32, s),
        else => error.InvalidCompatCase,
    };
}

fn jsonNumberToU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |i| std.math.cast(u32, i) orelse return error.InvalidCompatCase,
        .number_string => |s| try std.fmt.parseInt(u32, s, 10),
        else => error.InvalidCompatCase,
    };
}

fn parseIndexKind(kind: []const u8) db_mod.types.IndexKind {
    if (std.mem.eql(u8, kind, "full_text")) return .full_text;
    if (std.mem.eql(u8, kind, "dense_vector")) return .dense_vector;
    if (std.mem.eql(u8, kind, "sparse_vector")) return .sparse_vector;
    if (std.mem.eql(u8, kind, "graph")) return .graph;
    unreachable;
}

fn parseEnrichmentKind(kind: []const u8) db_mod.types.EnrichmentKind {
    if (std.mem.eql(u8, kind, "chunk")) return .chunk;
    if (std.mem.eql(u8, kind, "summary")) return .summary;
    if (std.mem.eql(u8, kind, "embedding")) return .embedding;
    unreachable;
}

fn parseU32JsonValue(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |i| std.math.cast(u32, i) orelse return error.InvalidCompatCase,
        .number_string => |s| try std.fmt.parseInt(u32, s, 10),
        else => error.InvalidCompatCase,
    };
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJsonValue(alloc, &out, value);
    const owned = try alloc.dupe(u8, out.items);
    out.deinit(alloc);
    return owned;
}

fn appendJsonValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    switch (value) {
        .null => try out.appendSlice(alloc, "null"),
        .bool => |b| try out.appendSlice(alloc, if (b) "true" else "false"),
        .integer => |i| {
            const formatted = try std.fmt.allocPrint(alloc, "{d}", .{i});
            defer alloc.free(formatted);
            try out.appendSlice(alloc, formatted);
        },
        .float => |f| {
            const formatted = try std.fmt.allocPrint(alloc, "{d}", .{f});
            defer alloc.free(formatted);
            try out.appendSlice(alloc, formatted);
        },
        .number_string => |s| try out.appendSlice(alloc, s),
        .string => |s| try appendJsonString(alloc, out, s),
        .array => |arr| {
            try out.append(alloc, '[');
            for (arr.items, 0..) |item, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonValue(alloc, out, item);
            }
            try out.append(alloc, ']');
        },
        .object => |obj| {
            try out.append(alloc, '{');
            var first = true;
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (!first) try out.append(alloc, ',');
                first = false;
                try appendJsonString(alloc, out, entry.key_ptr.*);
                try out.append(alloc, ':');
                try appendJsonValue(alloc, out, entry.value_ptr.*);
            }
            try out.append(alloc, '}');
        },
    }
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    try out.append(alloc, '"');
    for (s) |c| {
        switch (c) {
            '"', '\\' => {
                try out.append(alloc, '\\');
                try out.append(alloc, c);
            },
            '\n' => try out.appendSlice(alloc, "\\n"),
            '\r' => try out.appendSlice(alloc, "\\r"),
            '\t' => try out.appendSlice(alloc, "\\t"),
            else => try out.append(alloc, c),
        }
    }
    try out.append(alloc, '"');
}

fn expectJsonEqual(actual: std.json.Value, expected: std.json.Value) !void {
    if (isNumericJsonValue(actual) and isNumericJsonValue(expected)) {
        try std.testing.expectApproxEqAbs(try jsonNumberToF64(actual), try jsonNumberToF64(expected), 0.00001);
        return;
    }
    switch (expected) {
        .null => try std.testing.expect(actual == .null),
        .bool => |b| try std.testing.expect(actual == .bool and actual.bool == b),
        .integer => |i| try std.testing.expect(actual == .integer and actual.integer == i),
        .float => |f| try std.testing.expect(actual == .float and actual.float == f),
        .number_string => |s| try std.testing.expect(actual == .number_string and std.mem.eql(u8, actual.number_string, s)),
        .string => |s| try std.testing.expect(actual == .string and std.mem.eql(u8, actual.string, s)),
        .array => |arr| {
            try std.testing.expect(actual == .array);
            try std.testing.expectEqual(arr.items.len, actual.array.items.len);
            for (arr.items, actual.array.items) |expected_item, actual_item| {
                try expectJsonEqual(actual_item, expected_item);
            }
        },
        .object => |obj| {
            try std.testing.expect(actual == .object);
            var it = obj.iterator();
            while (it.next()) |entry| {
                const actual_value = actual.object.get(entry.key_ptr.*) orelse return error.InvalidCompatCase;
                try expectJsonEqual(actual_value, entry.value_ptr.*);
            }
        },
    }
}

fn isNumericJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float, .number_string => true,
        else => false,
    };
}

fn jsonNumberToF64(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .number_string => |s| try std.fmt.parseFloat(f64, s),
        else => error.InvalidCompatCase,
    };
}

fn tempPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-compat-";
    const ts = monotonicNs();
    const path = std.fmt.bufPrint(buf, "{s}{d}\x00", .{ base, ts }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn monotonicNs() u64 {
    return platform_time.monotonicNs();
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn sleepNs(ns: u64) void {
    if (ns == 0) return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromNanoseconds(@intCast(ns)),
    }, io_impl.io()) catch {};
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
