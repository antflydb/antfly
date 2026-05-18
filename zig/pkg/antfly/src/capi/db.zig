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
const antfly = @import("antfly-zig");
const capi = @import("types.zig");
const search_wire = @import("search_wire.zig");

const db_mod = antfly.db;
const raft_mod = antfly.raft;
const hbc = antfly.hbc;
const graph_mod = antfly.graph;
const traversal_mod = antfly.traversal;
const paths_mod = antfly.paths;
const graph_query_mod = antfly.graph_query;
const graph_pattern_mod = antfly.graph_pattern;
const transactions_mod = antfly.transactions;
const aggregations_mod = db_mod.aggregations;
const search_agg_mod = antfly.aggregation;
const geo_mod = antfly.geo;
const schema_mod = antfly.schema;
const Allocator = std.mem.Allocator;

fn monotonicNowNs() u64 {
    return antfly.platform_time.monotonicNs();
}

var temp_test_path_nonce: u64 = 0;

fn tempTestPath(alloc: Allocator, label: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &temp_test_path_nonce, .Add, 1, .monotonic);
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-{s}-{d}-{d}", .{
        label,
        antfly.platform_time.monotonicNs(),
        nonce,
    });
    defer alloc.free(path);
    return try alloc.dupeZ(u8, path);
}

const Handle = struct {
    alloc: std.mem.Allocator,
    db: db_mod.DB,
    readable_lease_hook: ?ReadableLeaseHook = null,

    fn prepareSearchRequest(self: *Handle, req: db_mod.types.SearchRequest) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareSearch(hook.group_id, req);
    }

    fn prepareDenseSearchRequest(
        self: *Handle,
        index_name: []const u8,
        vector: []const f32,
        k: u32,
        limit: u32,
        offset: u32,
    ) !void {
        try self.prepareSearchRequest(.{
            .index_name = index_name,
            .query = .{ .dense_knn = .{
                .vector = vector,
                .k = k,
            } },
            .limit = limit,
            .offset = offset,
            .include_stored = false,
        });
    }

    fn prepareLookupRequest(self: *Handle, key: []const u8, opts: db_mod.types.LookupOptions) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareLookup(hook.group_id, key, opts);
    }

    fn prepareScanRequest(
        self: *Handle,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
    ) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareScan(hook.group_id, from_key, to_key, opts);
    }
};

const ReadableLeaseHookFn = *const fn (
    ctx: ?*anyopaque,
    group_id: u64,
    request_ctx_ptr: ?[*]const u8,
    request_ctx_len: usize,
) callconv(.c) capi.ErrorCode;

const ReadableLeaseHook = struct {
    group_id: u64,
    callback_ctx: ?*anyopaque,
    callback: ReadableLeaseHookFn,

    fn requester(self: *const ReadableLeaseHook) raft_mod.ReadableLeaseRequester {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .request_readable_lease = requestReadableLease,
            },
        };
    }

    fn featureReads(self: *const ReadableLeaseHook) raft_mod.FeatureReads {
        return raft_mod.FeatureReads.init(self.requester());
    }

    fn requestReadableLease(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *ReadableLeaseHook = @ptrCast(@alignCast(ptr));
        const code = self.callback(
            self.callback_ctx,
            group_id,
            if (request_ctx.len > 0) request_ctx.ptr else null,
            request_ctx.len,
        );
        switch (code) {
            .ok => {},
            .invalid_argument => return error.InvalidArgument,
            .not_found => return error.NotFound,
            .version_conflict => return error.VersionConflict,
            .intent_conflict => return error.IntentConflict,
            .txn_not_found => return error.TxnNotFound,
            .internal => return error.Internal,
        }
    }
};

fn asHandle(ptr: ?*anyopaque) ?*Handle {
    const raw = ptr orelse return null;
    return @ptrCast(@alignCast(raw));
}

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

fn beginWithIdAndParticipants(
    handle: *Handle,
    txn_id: transactions_mod.TxnId,
    timestamp_ns: u64,
    participants_ptr: ?[*]const capi.Slice,
    participant_count: usize,
) !void {
    const participants = try handle.alloc.alloc([]const u8, participant_count);
    defer handle.alloc.free(participants);
    for (participants, 0..) |*entry, i| {
        entry.* = participants_ptr.?[i].bytes();
    }
    _ = try handle.db.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, participants);
}

fn writeIntentsInternal(
    handle: *Handle,
    txn_id: transactions_mod.TxnId,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
) !void {
    var writes = try handle.alloc.alloc(db_mod.types.TransactionWrite, write_count);
    defer handle.alloc.free(writes);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(handle.alloc);
    var predicates = try handle.alloc.alloc(db_mod.types.TransactionVersionPredicate, predicate_count);
    defer handle.alloc.free(predicates);

    var write_len: usize = 0;
    for (0..write_count) |i| {
        const src = writes_ptr.?[i];
        if (src.is_delete) {
            try deletes.append(handle.alloc, src.key.bytes());
        } else {
            writes[write_len] = .{
                .key = src.key.bytes(),
                .value = src.value.bytes(),
            };
            write_len += 1;
        }
    }
    for (0..predicate_count) |i| {
        predicates[i] = .{
            .key = predicates_ptr.?[i].key.bytes(),
            .expected_version = predicates_ptr.?[i].expected_version,
        };
    }

    try handle.db.writeTransaction(txn_id, .{
        .writes = writes[0..write_len],
        .deletes = deletes.items,
        .predicates = predicates,
    });
}

fn batchInternal(
    handle: *Handle,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
    timestamp_ns: u64,
    sync_level: u8,
) !void {
    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer writes.deinit(handle.alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(handle.alloc);
    var predicates = try handle.alloc.alloc(db_mod.types.TransactionVersionPredicate, predicate_count);
    defer handle.alloc.free(predicates);

    for (0..write_count) |i| {
        const src = writes_ptr.?[i];
        if (src.is_delete) {
            try deletes.append(handle.alloc, src.key.bytes());
        } else {
            try writes.append(handle.alloc, .{
                .key = src.key.bytes(),
                .value = src.value.bytes(),
            });
        }
    }
    for (0..predicate_count) |i| {
        predicates[i] = .{
            .key = predicates_ptr.?[i].key.bytes(),
            .expected_version = predicates_ptr.?[i].expected_version,
        };
    }

    const level: db_mod.types.SyncLevel = switch (sync_level) {
        0 => .write,
        1 => .full_index,
        else => return error.InvalidArgument,
    };

    try handle.db.batch(.{
        .writes = writes.items,
        .deletes = deletes.items,
        .predicates = predicates,
        .timestamp_ns = timestamp_ns,
        .sync_level = level,
    });
}

fn dupBytes(bytes: []const u8) !capi.Buffer {
    if (bytes.len == 0) return .{};
    const out = try std.heap.c_allocator.alloc(u8, bytes.len);
    @memcpy(out, bytes);
    return .{
        .ptr = out.ptr,
        .len = out.len,
    };
}

fn stringifyJson(value: anytype) !capi.Buffer {
    const bytes = try std.fmt.allocPrint(std.heap.c_allocator, "{f}", .{std.json.fmt(value, .{})});
    return .{
        .ptr = bytes.ptr,
        .len = bytes.len,
    };
}

fn dupBase64(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

fn decodeBase64Alloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, encoded);
    return out;
}

fn graphFreeEdges(alloc: Allocator, edges: []graph_mod.Edge) void {
    graph_mod.GraphIndex.freeEdges(alloc, edges);
    alloc.free(edges);
}

fn traversalFreeResults(alloc: Allocator, results: []traversal_mod.TraversalResult) void {
    traversal_mod.freeOwnedResults(alloc, results);
}

const JsonRange = struct {
    start_b64: []u8,
    end_b64: []u8,

    fn init(alloc: Allocator, byte_range: db_mod.types.ByteRange) !JsonRange {
        return .{
            .start_b64 = try dupBase64(alloc, byte_range.start),
            .end_b64 = try dupBase64(alloc, byte_range.end),
        };
    }

    fn deinit(self: *JsonRange, alloc: Allocator) void {
        alloc.free(self.start_b64);
        alloc.free(self.end_b64);
        self.* = undefined;
    }
};

const JsonSplitState = struct {
    phase: u8,
    split_key_b64: []u8,
    new_shard_id: u64,
    started_at: u64,
    original_range_end_b64: []u8,

    fn init(alloc: Allocator, state: db_mod.types.SplitState) !JsonSplitState {
        return .{
            .phase = @intFromEnum(state.phase),
            .split_key_b64 = try dupBase64(alloc, state.split_key),
            .new_shard_id = state.new_shard_id,
            .started_at = state.started_at,
            .original_range_end_b64 = try dupBase64(alloc, state.original_range_end),
        };
    }

    fn deinit(self: *JsonSplitState, alloc: Allocator) void {
        alloc.free(self.split_key_b64);
        alloc.free(self.original_range_end_b64);
        self.* = undefined;
    }
};

const JsonSplitDeltaWrite = struct {
    key_b64: []u8,
    value_b64: []u8,

    fn init(alloc: Allocator, write: db_mod.types.BatchWrite) !JsonSplitDeltaWrite {
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
        };
    }

    fn deinit(self: *JsonSplitDeltaWrite, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        self.* = undefined;
    }
};

const JsonSplitDeltaEntry = struct {
    sequence: u64,
    timestamp: u64,
    writes: []JsonSplitDeltaWrite,
    deletes_b64: [][]u8,

    fn init(alloc: Allocator, entry: db_mod.types.SplitDeltaEntry) !JsonSplitDeltaEntry {
        var writes = try alloc.alloc(JsonSplitDeltaWrite, entry.writes.len);
        errdefer alloc.free(writes);
        var write_count: usize = 0;
        errdefer {
            for (writes[0..write_count]) |*write| write.deinit(alloc);
        }
        for (entry.writes, 0..) |write, i| {
            writes[i] = try JsonSplitDeltaWrite.init(alloc, write);
            write_count += 1;
        }

        var deletes = try alloc.alloc([]u8, entry.deletes.len);
        errdefer alloc.free(deletes);
        var delete_count: usize = 0;
        errdefer {
            for (deletes[0..delete_count]) |item| alloc.free(item);
        }
        for (entry.deletes, 0..) |key, i| {
            deletes[i] = try dupBase64(alloc, key);
            delete_count += 1;
        }

        return .{
            .sequence = entry.sequence,
            .timestamp = entry.timestamp,
            .writes = writes,
            .deletes_b64 = deletes,
        };
    }

    fn deinit(self: *JsonSplitDeltaEntry, alloc: Allocator) void {
        for (self.writes) |*write| write.deinit(alloc);
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes_b64) |item| alloc.free(item);
        if (self.deletes_b64.len > 0) alloc.free(self.deletes_b64);
        self.* = undefined;
    }
};

const JsonIndexConfig = struct {
    name: []const u8,
    kind: []const u8,
    config_json: []const u8,
};

const JsonScanHash = struct {
    id_b64: []u8,
    hash: u64,

    fn init(alloc: Allocator, item: db_mod.types.ScanHash) !JsonScanHash {
        return .{
            .id_b64 = try dupBase64(alloc, item.id),
            .hash = item.hash,
        };
    }

    fn deinit(self: *JsonScanHash, alloc: Allocator) void {
        alloc.free(self.id_b64);
        self.* = undefined;
    }
};

const JsonScanDocument = struct {
    id_b64: []u8,
    json: []const u8,

    fn init(alloc: Allocator, item: db_mod.types.ScanDocument) !JsonScanDocument {
        return .{
            .id_b64 = try dupBase64(alloc, item.id),
            .json = item.json,
        };
    }

    fn deinit(self: *JsonScanDocument, alloc: Allocator) void {
        alloc.free(self.id_b64);
        self.* = undefined;
    }
};

const JsonScanResult = struct {
    hashes: []JsonScanHash,
    documents: []JsonScanDocument,
};

const JsonDBStats = struct {
    doc_count: u64,
    index_count: u32,
    indexes: []JsonDBIndexStats,
    enrichment: JsonEnrichmentStats,
    ttl_cleanup: JsonTTLCleanupStats,
    transaction_recovery: JsonTransactionRecoveryStats,
    text_merge: JsonTextMergeStats,
    term_doc_freq_cache_hits: u64,
    term_doc_freq_cache_misses: u64,
};

const JsonDBIndexStats = struct {
    name: []const u8,
    kind: []const u8,
    doc_count: u64,
    term_count: u64,
    edge_count: u64,
    node_count: u64,
};

const JsonEnrichmentStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    target_sequence: u64,
    applied_sequence: u64,
    processed_requests: u64,
    error_count: u64,
    retryable_error_count: u64,
    fatal_error_count: u64,
    retrying: bool,
    worker_failed: bool,
    skip_by_hash_count: u64,
    codec_decode_failures: u64,
    dense_artifact_bytes_written: u64,
    sparse_artifact_bytes_written: u64,
    chunk_artifact_bytes_written: u64,
    artifact_bytes_written: u64,
};

const JsonTTLCleanupStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    runs: u64,
    scanned_timestamps: u64,
    deleted_docs: u64,
    last_run_ns: u64,
    error_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
};

const JsonTransactionRecoveryStats = struct {
    enabled: bool,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    lease_acquire_failures: u64,
    lost_leases: u64,
    last_acquired_ms: u64,
    runs: u64,
    scanned_records: u64,
    auto_aborted: u64,
    resolved_finalized: u64,
    cleaned_records: u64,
    kept_recent_pending: u64,
    deferred_unresolved: u64,
    notification_attempts: u64,
    notification_successes: u64,
    notification_failures: u64,
    last_run_ns: u64,
    error_count: u64,
};

const JsonTextMergeStats = struct {
    enabled: bool,
    pending_indexes: u64,
    pending_segments: u64,
    pending_bytes: u64,
    in_flight_merges: u64,
    in_flight_segments: u64,
    completed_merges: u64,
    skipped_stale_merges: u64,
    failed_merges: u64,
    quarantined_merges: u64,
    quarantined_segments: u64,
    last_merge_error: []const u8,
    backpressure_events: u64,
    backpressure_ns: u64,
    max_pending_segments: u64,
    max_pending_bytes: u64,
};

const JsonChunkHit = struct {
    id_b64: []u8,
    score: ?f32 = null,
    stored_json: ?[]const u8 = null,
    artifact_ref: ?JsonArtifactRef = null,

    fn init(alloc: Allocator, hit: db_mod.types.ChunkHit) !JsonChunkHit {
        return .{
            .id_b64 = try dupBase64(alloc, hit.id),
            .score = hit.score,
            .stored_json = hit.stored_data,
            .artifact_ref = if (hit.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
        };
    }

    fn deinit(self: *JsonChunkHit, alloc: Allocator) void {
        alloc.free(self.id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

const JsonSearchHit = struct {
    id_b64: []u8,
    score: ?f32 = null,
    stored_json: ?[]const u8 = null,
    artifact_ref: ?JsonArtifactRef = null,
    chunk_hits: []JsonChunkHit = &.{},

    fn init(alloc: Allocator, hit: db_mod.types.SearchHit) !JsonSearchHit {
        var chunk_hits = try alloc.alloc(JsonChunkHit, hit.chunk_hits.len);
        errdefer alloc.free(chunk_hits);
        var count: usize = 0;
        errdefer {
            for (chunk_hits[0..count]) |*item| item.deinit(alloc);
        }
        for (hit.chunk_hits, 0..) |chunk, i| {
            chunk_hits[i] = try JsonChunkHit.init(alloc, chunk);
            count += 1;
        }
        return .{
            .id_b64 = try dupBase64(alloc, hit.id),
            .score = hit.score,
            .stored_json = hit.stored_data,
            .artifact_ref = if (hit.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
            .chunk_hits = chunk_hits,
        };
    }

    fn deinit(self: *JsonSearchHit, alloc: Allocator) void {
        alloc.free(self.id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        for (self.chunk_hits) |*item| item.deinit(alloc);
        if (self.chunk_hits.len > 0) alloc.free(self.chunk_hits);
        self.* = undefined;
    }
};

const JsonSearchResult = struct {
    total_hits: u32,
    hits: []JsonSearchHit,
    graph_results: []JsonGraphSearchResult = &.{},
    aggregations: []JsonSearchAggregationResult = &.{},
};

const JsonAggregateHitsRequest = struct {
    index_name: []const u8 = "",
    hit_ids_b64: []const []const u8 = &.{},
    aggregations: []const JsonSearchAggregationRequest = &.{},
};

const JsonGraphNodeSelectorRequest = struct {
    keys: []const []const u8 = &.{},
    result_ref: []const u8 = "",
    limit: u32 = 0,
};

const JsonGraphQueryRequest = struct {
    name: []const u8,
    type: []const u8,
    index_name: []const u8,
    start_nodes: JsonGraphNodeSelectorRequest,
    target_nodes: ?JsonGraphNodeSelectorRequest = null,
    edge_types: []const []const u8 = &.{},
    direction: []const u8 = "out",
    max_depth: u32 = 3,
    max_results: u32 = 100,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0,
    deduplicate: bool = true,
    include_paths: bool = false,
    weight_mode: []const u8 = "min_hops",
    k: u32 = 1,
};

const JsonNamedGraphInputSetRequest = struct {
    name: []const u8,
    hit_ids_b64: []const []const u8 = &.{},
    total_hits: u32 = 0,
};

const JsonGraphSearchResult = struct {
    name: []u8,
    total_hits: u32,
    nodes: []JsonGraphNode,
    paths: []JsonPath = &.{},
    hits: []JsonSearchHit,

    fn init(alloc: Allocator, result: db_mod.types.GraphSearchResult) !JsonGraphSearchResult {
        var nodes = try alloc.alloc(JsonGraphNode, result.nodes.len);
        errdefer alloc.free(nodes);
        var node_count: usize = 0;
        errdefer {
            for (nodes[0..node_count]) |*item| item.deinit(alloc);
        }
        for (result.nodes, 0..) |node, i| {
            nodes[i] = try JsonGraphNode.init(alloc, node);
            node_count += 1;
        }

        var paths = try alloc.alloc(JsonPath, result.paths.len);
        errdefer alloc.free(paths);
        var path_count: usize = 0;
        errdefer {
            for (paths[0..path_count]) |*item| item.deinit(alloc);
        }
        for (result.paths, 0..) |path, i| {
            paths[i] = try JsonPath.init(alloc, path);
            path_count += 1;
        }

        var hits = try alloc.alloc(JsonSearchHit, result.hits.len);
        errdefer alloc.free(hits);
        var count: usize = 0;
        errdefer {
            for (hits[0..count]) |*item| item.deinit(alloc);
        }
        for (result.hits, 0..) |hit, i| {
            hits[i] = try JsonSearchHit.init(alloc, hit);
            count += 1;
        }
        return .{
            .name = try alloc.dupe(u8, result.name),
            .total_hits = result.total_hits,
            .nodes = nodes,
            .paths = paths,
            .hits = hits,
        };
    }

    fn deinit(self: *JsonGraphSearchResult, alloc: Allocator) void {
        alloc.free(self.name);
        for (self.nodes) |*item| item.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.paths) |*item| item.deinit(alloc);
        if (self.paths.len > 0) alloc.free(self.paths);
        for (self.hits) |*item| item.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

const JsonSearchAggregationRequest = struct {
    name: []const u8,
    type: []const u8,
    field: []const u8,
    size: i64 = 0,
    interval: f64 = 0,
    calendar_interval: []const u8 = "",
    fixed_interval: []const u8 = "",
    min_doc_count: i64 = 0,
    significance_algorithm: []const u8 = "",
    background_query_type: []const u8 = "",
    background_field: []const u8 = "",
    background_text: []const u8 = "",
    bucket_path: []const u8 = "",
    sort_order: []const u8 = "",
    from: i64 = 0,
    window: i64 = 0,
    gap_policy: []const u8 = "",
    term_prefix: []const u8 = "",
    term_pattern: []const u8 = "",
    ranges: []const JsonNumericRangeRequest = &.{},
    date_ranges: []const JsonDateRangeRequest = &.{},
    distance_ranges: []const JsonDistanceRangeRequest = &.{},
    center_lat: f64 = 0,
    center_lon: f64 = 0,
    distance_unit: []const u8 = "",
    geohash_precision: u8 = 0,
    aggregations: []const JsonSearchAggregationRequest = &.{},
};

const JsonNumericRangeRequest = struct {
    name: []const u8 = "",
    start: ?f64 = null,
    end: ?f64 = null,
};

const JsonDateRangeRequest = struct {
    name: []const u8 = "",
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

const JsonDistanceRangeRequest = struct {
    name: []const u8 = "",
    from: ?f64 = null,
    to: ?f64 = null,
};

const JsonSearchAggregationBucket = struct {
    key_json: []const u8,
    count: i64,
    score: ?f64 = null,
    bg_count: ?i64 = null,
    aggregations: []JsonSearchAggregationResult = &.{},

    fn deinit(self: *JsonSearchAggregationBucket, alloc: Allocator) void {
        alloc.free(self.key_json);
        for (self.aggregations) |*agg| agg.deinit(alloc);
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        self.* = undefined;
    }
};

const JsonSearchAggregationResult = struct {
    name: []const u8,
    field: []const u8,
    type: []const u8,
    value_json: ?[]const u8 = null,
    metadata_json: ?[]const u8 = null,
    buckets: []JsonSearchAggregationBucket = &.{},

    fn deinit(self: *JsonSearchAggregationResult, alloc: Allocator) void {
        if (self.value_json) |value_json| alloc.free(value_json);
        if (self.metadata_json) |metadata_json| alloc.free(metadata_json);
        for (self.buckets) |*bucket| bucket.deinit(alloc);
        if (self.buckets.len > 0) alloc.free(self.buckets);
        self.* = undefined;
    }
};

fn toAggregationRequest(
    alloc: Allocator,
    requests: []const JsonSearchAggregationRequest,
) ![]aggregations_mod.SearchAggregationRequest {
    const out = try alloc.alloc(aggregations_mod.SearchAggregationRequest, requests.len);
    errdefer alloc.free(out);
    for (requests, 0..) |request, i| {
        const ranges = try alloc.alloc(aggregations_mod.NumericRangeRequest, request.ranges.len);
        errdefer alloc.free(ranges);
        for (request.ranges, 0..) |item, j| {
            ranges[j] = .{ .name = item.name, .start = item.start, .end = item.end };
        }
        const date_ranges = try alloc.alloc(aggregations_mod.DateRangeRequest, request.date_ranges.len);
        errdefer alloc.free(date_ranges);
        for (request.date_ranges, 0..) |item, j| {
            date_ranges[j] = .{ .name = item.name, .start = item.start, .end = item.end };
        }
        const distance_ranges = try alloc.alloc(aggregations_mod.DistanceRangeRequest, request.distance_ranges.len);
        errdefer alloc.free(distance_ranges);
        for (request.distance_ranges, 0..) |item, j| {
            distance_ranges[j] = .{ .name = item.name, .from = item.from, .to = item.to };
        }
        const nested = try toAggregationRequest(alloc, request.aggregations);
        out[i] = .{
            .name = request.name,
            .type = request.type,
            .field = request.field,
            .size = request.size,
            .interval = request.interval,
            .calendar_interval = request.calendar_interval,
            .fixed_interval = request.fixed_interval,
            .min_doc_count = request.min_doc_count,
            .significance_algorithm = request.significance_algorithm,
            .background_query = if (request.background_query_type.len == 0)
                null
            else if (std.mem.eql(u8, request.background_query_type, "match_all"))
                .{ .match_all = {} }
            else if (std.mem.eql(u8, request.background_query_type, "match"))
                .{ .match = .{
                    .field = request.background_field,
                    .text = request.background_text,
                } }
            else if (std.mem.eql(u8, request.background_query_type, "term"))
                .{ .term = .{
                    .field = request.background_field,
                    .term = request.background_text,
                } }
            else
                return error.InvalidArgument,
            .bucket_path = request.bucket_path,
            .sort_order = request.sort_order,
            .from = request.from,
            .window = request.window,
            .gap_policy = request.gap_policy,
            .term_prefix = request.term_prefix,
            .term_pattern = request.term_pattern,
            .ranges = ranges,
            .date_ranges = date_ranges,
            .distance_ranges = distance_ranges,
            .center_lat = request.center_lat,
            .center_lon = request.center_lon,
            .distance_unit = request.distance_unit,
            .geohash_precision = request.geohash_precision,
            .aggregations = nested,
        };
    }
    return out;
}

fn freeAggregationRequests(alloc: Allocator, requests: []const aggregations_mod.SearchAggregationRequest) void {
    for (requests) |request| {
        if (request.ranges.len > 0) alloc.free(request.ranges);
        if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
        if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
        freeAggregationRequests(alloc, request.aggregations);
    }
    if (requests.len > 0) alloc.free(requests);
}

fn toJsonAggregationResults(
    alloc: Allocator,
    results: []aggregations_mod.SearchAggregationResult,
) ![]JsonSearchAggregationResult {
    const out = try alloc.alloc(JsonSearchAggregationResult, results.len);
    errdefer alloc.free(out);
    for (results, 0..) |result, i| {
        const buckets = try alloc.alloc(JsonSearchAggregationBucket, result.buckets.len);
        errdefer alloc.free(buckets);
        for (result.buckets, 0..) |bucket, j| {
            buckets[j] = .{
                .key_json = try alloc.dupe(u8, bucket.key_json),
                .count = bucket.count,
                .score = bucket.score,
                .bg_count = bucket.bg_count,
                .aggregations = try toJsonAggregationResults(alloc, bucket.aggregations),
            };
        }
        out[i] = .{
            .name = result.name,
            .field = result.field,
            .type = result.type,
            .value_json = if (result.value_json) |value| try alloc.dupe(u8, value) else null,
            .metadata_json = if (result.metadata_json) |value| try alloc.dupe(u8, value) else null,
            .buckets = buckets,
        };
    }
    return out;
}

const JsonWritePair = struct {
    key_b64: []u8,
    value_b64: []u8,

    fn init(alloc: Allocator, write: db_mod.types.BatchWrite) !JsonWritePair {
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
        };
    }

    fn deinit(self: *JsonWritePair, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        self.* = undefined;
    }
};

fn artifactKindLabel(kind: db_mod.types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .summary => "summary",
        .embedding => "embedding",
    };
}

const JsonArtifactSourceRef = struct {
    kind: []const u8,
    name: []const u8,
    chunk_id: ?u32 = null,

    fn init(source: db_mod.types.ArtifactSourceRef) JsonArtifactSourceRef {
        return .{
            .kind = artifactKindLabel(source.kind),
            .name = source.name,
            .chunk_id = source.chunk_id,
        };
    }
};

const JsonArtifactRef = struct {
    document_id_b64: []u8,
    name: []const u8,
    kind: []const u8,
    chunk_id: ?u32 = null,
    source: ?JsonArtifactSourceRef = null,

    fn init(alloc: Allocator, artifact_ref: db_mod.types.ArtifactRef) !JsonArtifactRef {
        return .{
            .document_id_b64 = try dupBase64(alloc, artifact_ref.document_id),
            .name = artifact_ref.name,
            .kind = artifactKindLabel(artifact_ref.kind),
            .chunk_id = artifact_ref.chunk_id,
            .source = if (artifact_ref.source) |source| JsonArtifactSourceRef.init(source) else null,
        };
    }

    fn deinit(self: *JsonArtifactRef, alloc: Allocator) void {
        alloc.free(self.document_id_b64);
        self.* = undefined;
    }
};

const JsonArtifactWrite = struct {
    id_b64: []u8,
    value_b64: []u8,
    artifact_ref: JsonArtifactRef,

    fn init(alloc: Allocator, write: db_mod.types.ArtifactWrite) !JsonArtifactWrite {
        return .{
            .id_b64 = try dupBase64(alloc, write.id),
            .value_b64 = try dupBase64(alloc, write.value),
            .artifact_ref = try JsonArtifactRef.init(alloc, write.artifact_ref),
        };
    }

    fn deinit(self: *JsonArtifactWrite, alloc: Allocator) void {
        alloc.free(self.id_b64);
        alloc.free(self.value_b64);
        self.artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

const JsonDenseEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    artifact_id_b64: ?[]u8 = null,
    artifact_ref: ?JsonArtifactRef = null,
    vector: []const f32,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentDenseEmbeddingWrite) !JsonDenseEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .artifact_id_b64 = if (write.artifact_id) |artifact_id| try dupBase64(alloc, artifact_id) else null,
            .artifact_ref = if (write.artifact_ref) |artifact_ref| try JsonArtifactRef.init(alloc, artifact_ref) else null,
            .vector = write.vector,
        };
    }

    fn deinit(self: *JsonDenseEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        if (self.artifact_id_b64) |artifact_id_b64| alloc.free(artifact_id_b64);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

const JsonSparseEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    indices: []const u32,
    values: []const f32,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentSparseEmbeddingWrite) !JsonSparseEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .indices = write.indices,
            .values = write.values,
        };
    }

    fn deinit(self: *JsonSparseEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        self.* = undefined;
    }
};

const JsonSummaryEnrichmentWrite = struct {
    index_name: []const u8,
    doc_key_b64: []u8,
    text: []const u8,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentSummaryWrite) !JsonSummaryEnrichmentWrite {
        return .{
            .index_name = write.index_name,
            .doc_key_b64 = try dupBase64(alloc, write.doc_key),
            .text = write.text,
        };
    }

    fn deinit(self: *JsonSummaryEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.doc_key_b64);
        self.* = undefined;
    }
};

const JsonGraphWrite = struct {
    index_name: []const u8,
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,

    fn init(alloc: Allocator, write: db_mod.types.GraphEdgeWrite) !JsonGraphWrite {
        return .{
            .index_name = write.index_name,
            .source_b64 = try dupBase64(alloc, write.source),
            .target_b64 = try dupBase64(alloc, write.target),
            .edge_type = write.edge_type,
            .weight = write.weight,
            .created_at = write.created_at,
            .updated_at = write.updated_at,
            .metadata_json = write.metadata_json,
        };
    }

    fn deinit(self: *JsonGraphWrite, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

const JsonDocumentEnrichmentWrite = struct {
    key_b64: []u8,
    value_b64: []u8,
    target_index_names: [][]const u8,

    fn init(alloc: Allocator, write: db_mod.types.EnrichmentDocumentWrite) !JsonDocumentEnrichmentWrite {
        const target_index_names = try alloc.alloc([]const u8, write.target_index_names.len);
        errdefer alloc.free(target_index_names);
        for (write.target_index_names, 0..) |name, i| target_index_names[i] = name;
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
            .target_index_names = target_index_names,
        };
    }

    fn deinit(self: *JsonDocumentEnrichmentWrite, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        if (self.target_index_names.len > 0) alloc.free(self.target_index_names);
        self.* = undefined;
    }
};

const JsonExtractEnrichmentsResult = struct {
    dense_embeddings: []JsonDenseEnrichmentWrite,
    sparse_embeddings: []JsonSparseEnrichmentWrite,
    summaries: []JsonSummaryEnrichmentWrite,
    graph_writes: []JsonGraphWrite,

    fn deinit(self: *JsonExtractEnrichmentsResult, alloc: Allocator) void {
        for (self.dense_embeddings) |*item| item.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.sparse_embeddings) |*item| item.deinit(alloc);
        if (self.sparse_embeddings.len > 0) alloc.free(self.sparse_embeddings);
        for (self.summaries) |*item| item.deinit(alloc);
        if (self.summaries.len > 0) alloc.free(self.summaries);
        for (self.graph_writes) |*item| item.deinit(alloc);
        if (self.graph_writes.len > 0) alloc.free(self.graph_writes);
        self.* = undefined;
    }
};

const JsonComputeEnrichmentsResult = struct {
    artifact_writes: []JsonArtifactWrite,
    documents: []JsonDocumentEnrichmentWrite,
    dense_embeddings: []JsonDenseEnrichmentWrite,
    failed_keys_b64: [][]u8,

    fn deinit(self: *JsonComputeEnrichmentsResult, alloc: Allocator) void {
        for (self.artifact_writes) |*item| item.deinit(alloc);
        if (self.artifact_writes.len > 0) alloc.free(self.artifact_writes);
        for (self.documents) |*item| item.deinit(alloc);
        if (self.documents.len > 0) alloc.free(self.documents);
        for (self.dense_embeddings) |*item| item.deinit(alloc);
        if (self.dense_embeddings.len > 0) alloc.free(self.dense_embeddings);
        for (self.failed_keys_b64) |item| alloc.free(item);
        if (self.failed_keys_b64.len > 0) alloc.free(self.failed_keys_b64);
        self.* = undefined;
    }
};

fn buildJsonExtractEnrichmentsResult(
    alloc: Allocator,
    result: db_mod.types.ExtractEnrichmentsResult,
) !JsonExtractEnrichmentsResult {
    var dense_embeddings = try alloc.alloc(JsonDenseEnrichmentWrite, result.dense_embeddings.len);
    var dense_initialized: usize = 0;
    errdefer {
        for (dense_embeddings[0..dense_initialized]) |*item| item.deinit(alloc);
        alloc.free(dense_embeddings);
    }
    for (result.dense_embeddings, 0..) |item, i| {
        dense_embeddings[i] = try JsonDenseEnrichmentWrite.init(alloc, item);
        dense_initialized += 1;
    }

    var sparse_embeddings = try alloc.alloc(JsonSparseEnrichmentWrite, result.sparse_embeddings.len);
    var sparse_initialized: usize = 0;
    errdefer {
        for (sparse_embeddings[0..sparse_initialized]) |*item| item.deinit(alloc);
        alloc.free(sparse_embeddings);
    }
    for (result.sparse_embeddings, 0..) |item, i| {
        sparse_embeddings[i] = try JsonSparseEnrichmentWrite.init(alloc, item);
        sparse_initialized += 1;
    }

    var summaries = try alloc.alloc(JsonSummaryEnrichmentWrite, result.summaries.len);
    var summaries_initialized: usize = 0;
    errdefer {
        for (summaries[0..summaries_initialized]) |*item| item.deinit(alloc);
        alloc.free(summaries);
    }
    for (result.summaries, 0..) |item, i| {
        summaries[i] = try JsonSummaryEnrichmentWrite.init(alloc, item);
        summaries_initialized += 1;
    }

    var graph_writes = try alloc.alloc(JsonGraphWrite, result.graph_writes.len);
    var graph_initialized: usize = 0;
    errdefer {
        for (graph_writes[0..graph_initialized]) |*item| item.deinit(alloc);
        alloc.free(graph_writes);
    }
    for (result.graph_writes, 0..) |item, i| {
        graph_writes[i] = try JsonGraphWrite.init(alloc, item);
        graph_initialized += 1;
    }

    return .{
        .dense_embeddings = dense_embeddings,
        .sparse_embeddings = sparse_embeddings,
        .summaries = summaries,
        .graph_writes = graph_writes,
    };
}

fn buildJsonComputeEnrichmentsResult(
    alloc: Allocator,
    result: db_mod.types.ComputeEnrichmentsResult,
) !JsonComputeEnrichmentsResult {
    var artifact_writes = try alloc.alloc(JsonArtifactWrite, result.artifact_writes.len);
    var artifact_initialized: usize = 0;
    errdefer {
        for (artifact_writes[0..artifact_initialized]) |*item| item.deinit(alloc);
        alloc.free(artifact_writes);
    }
    for (result.artifact_writes, 0..) |item, i| {
        artifact_writes[i] = try JsonArtifactWrite.init(alloc, item);
        artifact_initialized += 1;
    }

    var documents = try alloc.alloc(JsonDocumentEnrichmentWrite, result.documents.len);
    var documents_initialized: usize = 0;
    errdefer {
        for (documents[0..documents_initialized]) |*item| item.deinit(alloc);
        alloc.free(documents);
    }
    for (result.documents, 0..) |item, i| {
        documents[i] = try JsonDocumentEnrichmentWrite.init(alloc, item);
        documents_initialized += 1;
    }

    var dense_embeddings = try alloc.alloc(JsonDenseEnrichmentWrite, result.dense_embeddings.len);
    var dense_initialized: usize = 0;
    errdefer {
        for (dense_embeddings[0..dense_initialized]) |*item| item.deinit(alloc);
        alloc.free(dense_embeddings);
    }
    for (result.dense_embeddings, 0..) |item, i| {
        dense_embeddings[i] = try JsonDenseEnrichmentWrite.init(alloc, item);
        dense_initialized += 1;
    }

    var failed_keys_b64 = try alloc.alloc([]u8, result.failed_keys.len);
    var failed_initialized: usize = 0;
    errdefer {
        for (failed_keys_b64[0..failed_initialized]) |item| alloc.free(item);
        alloc.free(failed_keys_b64);
    }
    for (result.failed_keys, 0..) |item, i| {
        failed_keys_b64[i] = try dupBase64(alloc, item);
        failed_initialized += 1;
    }

    return .{
        .artifact_writes = artifact_writes,
        .documents = documents,
        .dense_embeddings = dense_embeddings,
        .failed_keys_b64 = failed_keys_b64,
    };
}

fn freeOwnedBatchWrites(alloc: Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn decodeBatchWritesRequest(alloc: Allocator, request_json: []const u8) ![]db_mod.types.BatchWrite {
    const Request = struct {
        writes: []const struct {
            key_b64: []const u8,
            value_b64: []const u8,
        },
    };

    var parsed = try std.json.parseFromSlice(Request, alloc, request_json, .{});
    defer parsed.deinit();

    const writes = try alloc.alloc(db_mod.types.BatchWrite, parsed.value.writes.len);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    for (parsed.value.writes, 0..) |write, i| {
        writes[i] = .{
            .key = try decodeBase64Alloc(alloc, write.key_b64),
            .value = try decodeBase64Alloc(alloc, write.value_b64),
        };
        initialized += 1;
    }

    return writes;
}

const JsonEdge = struct {
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,

    fn init(alloc: Allocator, edge: db_mod.types.GraphEdge) !JsonEdge {
        return .{
            .source_b64 = try dupBase64(alloc, edge.source),
            .target_b64 = try dupBase64(alloc, edge.target),
            .edge_type = edge.edge_type,
            .weight = edge.weight,
            .created_at = edge.created_at,
            .updated_at = edge.updated_at,
            .metadata_json = edge.metadata,
        };
    }

    fn deinit(self: *JsonEdge, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

const JsonTraversalResult = struct {
    key_b64: []u8,
    depth: u32,
    total_weight: f64,
    path_b64: ?[][]u8 = null,

    fn init(alloc: Allocator, item: db_mod.types.GraphTraversalResult) !JsonTraversalResult {
        var path_b64: ?[][]u8 = null;
        if (item.path) |path| {
            var encoded = try alloc.alloc([]u8, path.len);
            errdefer alloc.free(encoded);
            var count: usize = 0;
            errdefer {
                for (encoded[0..count]) |entry| alloc.free(entry);
            }
            for (path, 0..) |entry, i| {
                encoded[i] = try dupBase64(alloc, entry);
                count += 1;
            }
            path_b64 = encoded;
        }
        return .{
            .key_b64 = try dupBase64(alloc, item.key),
            .depth = item.depth,
            .total_weight = item.total_weight,
            .path_b64 = path_b64,
        };
    }

    fn deinit(self: *JsonTraversalResult, alloc: Allocator) void {
        alloc.free(self.key_b64);
        if (self.path_b64) |items| {
            for (items) |entry| alloc.free(entry);
            alloc.free(items);
        }
        self.* = undefined;
    }
};

const JsonPathEdge = struct {
    source_b64: []u8,
    target_b64: []u8,
    edge_type: []const u8,
    weight: f64,

    fn init(alloc: Allocator, edge: paths_mod.PathEdge) !JsonPathEdge {
        return .{
            .source_b64 = try dupBase64(alloc, edge.source),
            .target_b64 = try dupBase64(alloc, edge.target),
            .edge_type = edge.edge_type,
            .weight = edge.weight,
        };
    }

    fn deinit(self: *JsonPathEdge, alloc: Allocator) void {
        alloc.free(self.source_b64);
        alloc.free(self.target_b64);
        self.* = undefined;
    }
};

const JsonPath = struct {
    nodes_b64: [][]u8,
    edges: []JsonPathEdge,
    total_weight: f64,
    length: u32,

    fn init(alloc: Allocator, path: db_mod.types.GraphPath) !JsonPath {
        var nodes = try alloc.alloc([]u8, path.nodes.len);
        errdefer alloc.free(nodes);
        var node_count: usize = 0;
        errdefer {
            for (nodes[0..node_count]) |entry| alloc.free(entry);
        }
        for (path.nodes, 0..) |node, i| {
            nodes[i] = try dupBase64(alloc, node);
            node_count += 1;
        }

        var edges = try alloc.alloc(JsonPathEdge, path.edges.len);
        errdefer alloc.free(edges);
        var edge_count: usize = 0;
        errdefer {
            for (edges[0..edge_count]) |*entry| entry.deinit(alloc);
        }
        for (path.edges, 0..) |edge, i| {
            edges[i] = try JsonPathEdge.init(alloc, edge);
            edge_count += 1;
        }

        return .{
            .nodes_b64 = nodes,
            .edges = edges,
            .total_weight = path.total_weight,
            .length = path.length,
        };
    }

    fn deinit(self: *JsonPath, alloc: Allocator) void {
        for (self.nodes_b64) |entry| alloc.free(entry);
        if (self.nodes_b64.len > 0) alloc.free(self.nodes_b64);
        for (self.edges) |*entry| entry.deinit(alloc);
        if (self.edges.len > 0) alloc.free(self.edges);
        self.* = undefined;
    }
};

const JsonPatternBinding = struct {
    alias: []u8,
    key_b64: []u8,
    depth: u32,

    fn init(alloc: Allocator, binding: graph_pattern_mod.PatternBinding) !JsonPatternBinding {
        return .{
            .alias = try alloc.dupe(u8, binding.alias),
            .key_b64 = try dupBase64(alloc, binding.key),
            .depth = binding.depth,
        };
    }

    fn deinit(self: *JsonPatternBinding, alloc: Allocator) void {
        alloc.free(self.alias);
        alloc.free(self.key_b64);
        self.* = undefined;
    }
};

const JsonPatternMatch = struct {
    bindings: []JsonPatternBinding,
    path: []JsonPathEdge,

    fn init(alloc: Allocator, match: graph_pattern_mod.PatternMatch) !JsonPatternMatch {
        var bindings = try alloc.alloc(JsonPatternBinding, match.bindings.len);
        errdefer alloc.free(bindings);
        var binding_count: usize = 0;
        errdefer {
            for (bindings[0..binding_count]) |*binding| binding.deinit(alloc);
        }
        for (match.bindings, 0..) |binding, i| {
            bindings[i] = try JsonPatternBinding.init(alloc, binding);
            binding_count += 1;
        }

        var path = try alloc.alloc(JsonPathEdge, match.path.len);
        errdefer alloc.free(path);
        var path_count: usize = 0;
        errdefer {
            for (path[0..path_count]) |*entry| entry.deinit(alloc);
        }
        for (match.path, 0..) |edge, i| {
            path[i] = try JsonPathEdge.init(alloc, edge);
            path_count += 1;
        }

        return .{
            .bindings = bindings,
            .path = path,
        };
    }

    fn deinit(self: *JsonPatternMatch, alloc: Allocator) void {
        for (self.bindings) |*binding| binding.deinit(alloc);
        if (self.bindings.len > 0) alloc.free(self.bindings);
        for (self.path) |*entry| entry.deinit(alloc);
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }
};

const JsonGraphNode = struct {
    key_b64: []u8,
    depth: u32,
    distance: f64,
    path_b64: ?[][]u8 = null,
    path_edges: []JsonPathEdge = &.{},

    fn init(alloc: Allocator, node: graph_query_mod.GraphResultNode) !JsonGraphNode {
        var path_b64: ?[][]u8 = null;
        if (node.path) |path| {
            var encoded = try alloc.alloc([]u8, path.len);
            errdefer alloc.free(encoded);
            var count: usize = 0;
            errdefer {
                for (encoded[0..count]) |entry| alloc.free(entry);
            }
            for (path, 0..) |entry, i| {
                encoded[i] = try dupBase64(alloc, entry);
                count += 1;
            }
            path_b64 = encoded;
        }

        var path_edges = try alloc.alloc(JsonPathEdge, if (node.path_edges) |items| items.len else 0);
        errdefer alloc.free(path_edges);
        var edge_count: usize = 0;
        errdefer {
            for (path_edges[0..edge_count]) |*edge| edge.deinit(alloc);
        }
        if (node.path_edges) |items| {
            for (items, 0..) |edge, i| {
                path_edges[i] = .{
                    .source_b64 = try dupBase64(alloc, edge.source),
                    .target_b64 = try dupBase64(alloc, edge.target),
                    .edge_type = try alloc.dupe(u8, edge.edge_type),
                    .weight = edge.weight,
                };
                edge_count += 1;
            }
        }

        return .{
            .key_b64 = try dupBase64(alloc, node.key),
            .depth = node.depth,
            .distance = node.distance,
            .path_b64 = path_b64,
            .path_edges = path_edges,
        };
    }

    fn deinit(self: *JsonGraphNode, alloc: Allocator) void {
        alloc.free(self.key_b64);
        if (self.path_b64) |items| {
            for (items) |entry| alloc.free(entry);
            alloc.free(items);
        }
        for (self.path_edges) |*edge| edge.deinit(alloc);
        if (self.path_edges.len > 0) alloc.free(self.path_edges);
        self.* = undefined;
    }
};

pub export fn antfly_db_open(path: [*:0]const u8, out_handle: *?*anyopaque) capi.ErrorCode {
    const alloc = std.heap.c_allocator;
    const handle = alloc.create(Handle) catch return .internal;
    errdefer alloc.destroy(handle);
    handle.* = .{
        .alloc = alloc,
        .db = db_mod.DB.open(alloc, std.mem.span(path), .{}) catch |err| return capi.mapError(err),
    };
    out_handle.* = handle;
    return .ok;
}

pub export fn antfly_db_close(handle_ptr: ?*anyopaque) void {
    const handle = asHandle(handle_ptr) orelse return;
    handle.db.close();
    handle.alloc.destroy(handle);
}

pub export fn antfly_db_set_readable_lease_hook(
    handle_ptr: ?*anyopaque,
    group_id: u64,
    callback_ctx: ?*anyopaque,
    callback: ?ReadableLeaseHookFn,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    if (callback) |hook| {
        handle.readable_lease_hook = .{
            .group_id = group_id,
            .callback_ctx = callback_ctx,
            .callback = hook,
        };
    } else {
        handle.readable_lease_hook = null;
    }
    return .ok;
}

pub export fn antfly_db_buffer_free(ptr: ?[*]u8, len: usize) void {
    if (ptr == null or len == 0) return;
    std.heap.c_allocator.free(ptr.?[0..len]);
}

pub export fn antfly_db_dense_search_result_free(result: *capi.DenseSearchResult) void {
    if (result.hits_ptr) |hits_ptr| {
        const hits = hits_ptr[0..result.hit_count];
        for (hits) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) {
                std.heap.c_allocator.free(hit.id_ptr.?[0..hit.id_len]);
            }
        }
        std.heap.c_allocator.free(hits);
    }
    result.* = .{};
}

pub export fn antfly_db_packed_dense_search_result_free(result: *capi.PackedDenseSearchResult) void {
    if (result.hits_ptr) |hits_ptr| {
        const hits = hits_ptr[0..result.hit_count];
        std.heap.c_allocator.free(hits);
    }
    if (result.ids_ptr != null and result.ids_len > 0) {
        std.heap.c_allocator.free(result.ids_ptr.?[0..result.ids_len]);
    }
    result.* = .{};
}

fn packDenseHits(
    alloc: Allocator,
    total_hits: u32,
    ids: []const []const u8,
    scores: []const f32,
    out_result: *capi.PackedDenseSearchResult,
) !void {
    std.debug.assert(ids.len == scores.len);

    const hits = try alloc.alloc(capi.PackedDenseSearchHit, ids.len);
    errdefer alloc.free(hits);

    var ids_len: usize = 0;
    for (ids) |id| ids_len += id.len;
    const ids_blob = try alloc.alloc(u8, ids_len);
    errdefer alloc.free(ids_blob);

    var cursor: usize = 0;
    for (ids, scores, 0..) |id, score, i| {
        @memcpy(ids_blob[cursor..][0..id.len], id);
        hits[i] = .{
            .id_offset = cursor,
            .id_len = id.len,
            .score = score,
        };
        cursor += id.len;
    }

    out_result.* = .{
        .hits_ptr = if (hits.len > 0) hits.ptr else null,
        .hit_count = hits.len,
        .total_hits = total_hits,
        .ids_ptr = if (ids_blob.len > 0) ids_blob.ptr else null,
        .ids_len = ids_blob.len,
    };
}

const DenseOwnedResult = struct {
    alloc: Allocator,
    total_hits: u32,
    ids: [][]const u8,
    scores: []f32,

    fn deinit(self: *DenseOwnedResult) void {
        for (self.ids) |id| self.alloc.free(id);
        if (self.ids.len > 0) self.alloc.free(self.ids);
        if (self.scores.len > 0) self.alloc.free(self.scores);
        self.* = undefined;
    }
};

const DenseOwnedProfile = struct {
    result: DenseOwnedResult,
    total_ns: u64 = 0,
    index_lookup_ns: u64 = 0,
    search_ns: u64 = 0,
    hits_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,

    fn deinit(self: *DenseOwnedProfile) void {
        self.result.deinit();
        self.* = undefined;
    }

    fn takeResult(self: *DenseOwnedProfile) DenseOwnedResult {
        const result = self.result;
        self.result = .{
            .alloc = result.alloc,
            .total_hits = 0,
            .ids = &.{},
            .scores = &.{},
        };
        return result;
    }
};

const DenseResolvedHit = struct {
    id: []u8,
    score: f32,
};

const DenseResolvedHits = struct {
    alloc: Allocator,
    total_hits: u32,
    hits: []DenseResolvedHit,

    fn deinit(self: *DenseResolvedHits) void {
        for (self.hits) |hit| self.alloc.free(hit.id);
        if (self.hits.len > 0) self.alloc.free(self.hits);
        self.* = undefined;
    }
};

const DenseWireOwnedProfile = struct {
    out: capi.Buffer = .{},
    total_ns: u64 = 0,
    decode_ns: u64 = 0,
    search_ns: u64 = 0,
    resolve_ns: u64 = 0,
    encode_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,
};

fn resolveDenseHitsFromProfiled(
    alloc: Allocator,
    entry: anytype,
    results: *hbc.ProfiledSearchResults,
    limit: u32,
    offset: u32,
) !DenseResolvedHits {
    const raw_hits = results.results.getHits();
    const start: u32 = @min(offset, @as(u32, @intCast(raw_hits.len)));
    const end: u32 = @min(start + limit, @as(u32, @intCast(raw_hits.len)));
    const sliced_hits = raw_hits[@intCast(start)..@intCast(end)];

    const resolved = try alloc.alloc(DenseResolvedHit, sliced_hits.len);
    errdefer alloc.free(resolved);
    var resolved_count: usize = 0;
    errdefer {
        for (resolved[0..resolved_count]) |hit| alloc.free(hit.id);
    }

    for (sliced_hits, 0..) |hit, i| {
        const result_index: usize = @as(usize, @intCast(start)) + i;
        const id = if (results.results.takeMetadata(result_index)) |metadata|
            metadata
        else
            (try entry.index.getMetadata(hit.vector_id)) orelse return error.Internal;
        resolved[i] = .{
            .id = id,
            .score = hit.distance,
        };
        resolved_count += 1;
    }

    return .{
        .alloc = alloc,
        .total_hits = @intCast(raw_hits.len),
        .hits = resolved,
    };
}

fn packResolvedDenseHits(
    alloc: Allocator,
    resolved: *DenseResolvedHits,
    out_result: *capi.PackedDenseSearchResult,
) !void {
    const hits = try alloc.alloc(capi.PackedDenseSearchHit, resolved.hits.len);
    errdefer alloc.free(hits);

    var ids_len: usize = 0;
    for (resolved.hits) |hit| ids_len += hit.id.len;
    const ids_blob = try alloc.alloc(u8, ids_len);
    errdefer alloc.free(ids_blob);

    var cursor: usize = 0;
    for (resolved.hits, 0..) |hit, i| {
        @memcpy(ids_blob[cursor..][0..hit.id.len], hit.id);
        hits[i] = .{
            .id_offset = cursor,
            .id_len = hit.id.len,
            .score = hit.score,
        };
        cursor += hit.id.len;
    }

    out_result.* = .{
        .hits_ptr = if (hits.len > 0) hits.ptr else null,
        .hit_count = hits.len,
        .total_hits = resolved.total_hits,
        .ids_ptr = if (ids_blob.len > 0) ids_blob.ptr else null,
        .ids_len = ids_blob.len,
    };
}

fn encodeResolvedDenseWireResponse(
    resolved: *DenseResolvedHits,
) !capi.Buffer {
    const header_len: usize = 4 + 2 + 2 + 4 + 4 + 4;
    const hits_len: usize = resolved.hits.len * @sizeOf(search_wire.PackedHit);
    var ids_len: usize = 0;
    for (resolved.hits) |hit| ids_len += hit.id.len;
    const total_len = header_len + hits_len + ids_len;
    const out = try std.heap.c_allocator.alloc(u8, total_len);
    errdefer std.heap.c_allocator.free(out);

    var cursor: usize = 0;
    std.mem.writeInt(u32, out[cursor..][0..4], search_wire.magic, .little);
    cursor += 4;
    std.mem.writeInt(u16, out[cursor..][0..2], search_wire.version, .little);
    cursor += 2;
    std.mem.writeInt(u16, out[cursor..][0..2], @intFromEnum(search_wire.Op.dense_search), .little);
    cursor += 2;
    std.mem.writeInt(u32, out[cursor..][0..4], resolved.total_hits, .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], @intCast(resolved.hits.len), .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], @intCast(ids_len), .little);
    cursor += 4;

    var id_cursor: u32 = 0;
    for (resolved.hits) |hit| {
        std.mem.writeInt(u32, out[cursor..][0..4], id_cursor, .little);
        cursor += 4;
        std.mem.writeInt(u16, out[cursor..][0..2], @intCast(hit.id.len), .little);
        cursor += 2;
        std.mem.writeInt(u16, out[cursor..][0..2], 0, .little);
        cursor += 2;
        std.mem.writeInt(u32, out[cursor..][0..4], @bitCast(hit.score), .little);
        cursor += 4;
        id_cursor += @intCast(hit.id.len);
    }

    for (resolved.hits) |hit| {
        @memcpy(out[cursor..][0..hit.id.len], hit.id);
        cursor += hit.id.len;
    }

    return .{ .ptr = out.ptr, .len = out.len };
}

fn searchDensePackedFast(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
    out_result: *capi.PackedDenseSearchResult,
) !bool {
    if (handle.db.core.schema != null and handle.db.core.schema.?.ttl_duration_ns != 0) return false;
    const entry = handle.db.core.index_manager.denseIndex(index_name) orelse return false;
    if (entry.chunk_name != null) return false;

    var profiled = try entry.index.searchProfiledRequest(.{
        .query = vector,
        .k = k,
    });
    defer profiled.results.deinit();

    var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, limit, offset);
    defer resolved.deinit();
    try packResolvedDenseHits(handle.alloc, &resolved, out_result);
    return true;
}

fn searchDenseWireFast(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !?capi.Buffer {
    if (handle.db.core.schema != null and handle.db.core.schema.?.ttl_duration_ns != 0) return null;
    const entry = handle.db.core.index_manager.denseIndex(index_name) orelse return null;
    if (entry.chunk_name != null) return null;

    var profiled = try entry.index.searchProfiledRequest(.{
        .query = vector,
        .k = k,
    });
    defer profiled.results.deinit();

    var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, limit, offset);
    defer resolved.deinit();
    return try encodeResolvedDenseWireResponse(&resolved);
}

fn searchDenseWireOwnedProfiled(
    handle: *Handle,
    request_buf: []const u8,
) !DenseWireOwnedProfile {
    const total_start = monotonicNowNs();

    const decode_start = monotonicNowNs();
    var req = try search_wire.decodeDenseRequest(handle.alloc, request_buf);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    const decode_end = monotonicNowNs();

    if (handle.db.core.schema == null or handle.db.core.schema.?.ttl_duration_ns == 0) {
        if (handle.db.core.index_manager.denseIndex(req.index_name)) |entry| {
            if (entry.chunk_name == null) {
                const search_start = monotonicNowNs();
                var profiled = try entry.index.searchProfiledRequest(.{
                    .query = req.vector,
                    .k = req.k,
                });
                defer profiled.results.deinit();
                const search_end = monotonicNowNs();

                const resolve_start = monotonicNowNs();
                var resolved = try resolveDenseHitsFromProfiled(handle.alloc, entry, &profiled, req.limit, req.offset);
                defer resolved.deinit();
                const resolve_end = monotonicNowNs();

                const encode_start = monotonicNowNs();
                const out = try encodeResolvedDenseWireResponse(&resolved);
                const encode_end = monotonicNowNs();

                return .{
                    .out = out,
                    .total_ns = @intCast(encode_end - total_start),
                    .decode_ns = @intCast(decode_end - decode_start),
                    .search_ns = @intCast(search_end - search_start),
                    .resolve_ns = @intCast(resolve_end - resolve_start),
                    .encode_ns = @intCast(encode_end - encode_start),
                    .fallback_ns = 0,
                    .hbc_total_ns = profiled.profile.total_ns,
                    .hbc_setup_ns = profiled.profile.setup_ns,
                    .hbc_root_load_ns = profiled.profile.root_load_ns,
                    .hbc_node_cache_miss_ns = profiled.profile.node_cache_miss_ns,
                    .hbc_node_cache_misses = profiled.profile.node_cache_misses,
                    .hbc_quantized_cache_miss_ns = profiled.profile.quantized_cache_miss_ns,
                    .hbc_quantized_cache_misses = profiled.profile.quantized_cache_misses,
                    .hbc_child_expand_ns = profiled.profile.child_expand_ns,
                    .hbc_leaf_score_ns = profiled.profile.leaf_score_ns,
                    .hbc_rerank_ns = profiled.profile.rerank_ns,
                    .hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns,
                    .hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns,
                    .hbc_nodes_visited = profiled.profile.nodes_visited,
                    .hbc_leaves_explored = profiled.profile.leaves_explored,
                    .hbc_reranked_vectors = profiled.profile.reranked_vectors,
                    .hit_count = @intCast(resolved.hits.len),
                    .total_hits = resolved.total_hits,
                    .used_fast_path = true,
                };
            }
        }
    }

    const fallback_start = monotonicNowNs();
    var owned = try searchDenseOwned(handle, req.index_name, req.vector, req.k, req.limit, req.offset);
    defer owned.deinit();
    const fallback_end = monotonicNowNs();

    const encode_start = monotonicNowNs();
    const out = try search_wire.encodeDenseResponse(owned.total_hits, owned.ids, owned.scores);
    const encode_end = monotonicNowNs();

    return .{
        .out = out,
        .total_ns = @intCast(encode_end - total_start),
        .decode_ns = @intCast(decode_end - decode_start),
        .search_ns = 0,
        .resolve_ns = 0,
        .encode_ns = @intCast(encode_end - encode_start),
        .fallback_ns = @intCast(fallback_end - fallback_start),
        .hbc_total_ns = 0,
        .hbc_setup_ns = 0,
        .hbc_root_load_ns = 0,
        .hbc_node_cache_miss_ns = 0,
        .hbc_node_cache_misses = 0,
        .hbc_quantized_cache_miss_ns = 0,
        .hbc_quantized_cache_misses = 0,
        .hbc_child_expand_ns = 0,
        .hbc_leaf_score_ns = 0,
        .hbc_rerank_ns = 0,
        .hbc_rerank_vector_load_ns = 0,
        .hbc_rerank_distance_ns = 0,
        .hbc_nodes_visited = 0,
        .hbc_leaves_explored = 0,
        .hbc_reranked_vectors = 0,
        .hit_count = @intCast(owned.ids.len),
        .total_hits = owned.total_hits,
        .used_fast_path = false,
    };
}

fn searchDenseOwned(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    var profiled = try searchDenseOwnedProfiled(handle, index_name, vector, k, limit, offset);
    defer profiled.deinit();
    return profiled.takeResult();
}

fn searchDenseOwnedProfiled(
    handle: *Handle,
    index_name: []const u8,
    vector: []const f32,
    k: u32,
    limit: u32,
    offset: u32,
) !DenseOwnedProfile {
    if (vector.len == 0) return error.InvalidArgument;

    const total_start = monotonicNowNs();
    const lookup_start = monotonicNowNs();
    if (handle.db.core.schema == null or handle.db.core.schema.?.ttl_duration_ns == 0) {
        if (handle.db.core.index_manager.denseIndex(index_name)) |entry| {
            const lookup_end = monotonicNowNs();
            if (entry.chunk_name == null) {
                const search_start = monotonicNowNs();
                var profiled = try entry.index.searchProfiledRequest(.{
                    .query = vector,
                    .k = k,
                });
                defer profiled.results.deinit();
                const search_end = monotonicNowNs();

                const raw_hits = profiled.results.getHits();
                const start: u32 = @min(offset, @as(u32, @intCast(raw_hits.len)));
                const end: u32 = @min(start + limit, @as(u32, @intCast(raw_hits.len)));
                const sliced_hits = raw_hits[@intCast(start)..@intCast(end)];

                const hits_start = monotonicNowNs();
                const ids = try handle.alloc.alloc([]const u8, sliced_hits.len);
                errdefer handle.alloc.free(ids);
                var id_count: usize = 0;
                errdefer {
                    for (ids[0..id_count]) |id| handle.alloc.free(id);
                }

                const scores = try handle.alloc.alloc(f32, sliced_hits.len);
                errdefer handle.alloc.free(scores);

                for (sliced_hits, 0..) |hit, i| {
                    const result_index: usize = @as(usize, @intCast(start)) + i;
                    const id = if (profiled.results.takeMetadata(result_index)) |metadata|
                        metadata
                    else
                        (try entry.index.getMetadata(hit.vector_id)) orelse return error.Internal;
                    ids[i] = id;
                    id_count += 1;
                    scores[i] = hit.distance;
                }
                const hits_end = monotonicNowNs();

                return .{
                    .result = .{
                        .alloc = handle.alloc,
                        .total_hits = @intCast(raw_hits.len),
                        .ids = ids,
                        .scores = scores,
                    },
                    .total_ns = @intCast(hits_end - total_start),
                    .index_lookup_ns = @intCast(lookup_end - lookup_start),
                    .search_ns = @intCast(search_end - search_start),
                    .hits_ns = @intCast(hits_end - hits_start),
                    .fallback_ns = 0,
                    .hbc_total_ns = profiled.profile.total_ns,
                    .hbc_setup_ns = profiled.profile.setup_ns,
                    .hbc_root_load_ns = profiled.profile.root_load_ns,
                    .hbc_node_cache_miss_ns = profiled.profile.node_cache_miss_ns,
                    .hbc_node_cache_misses = profiled.profile.node_cache_misses,
                    .hbc_quantized_cache_miss_ns = profiled.profile.quantized_cache_miss_ns,
                    .hbc_quantized_cache_misses = profiled.profile.quantized_cache_misses,
                    .hbc_child_expand_ns = profiled.profile.child_expand_ns,
                    .hbc_leaf_score_ns = profiled.profile.leaf_score_ns,
                    .hbc_rerank_ns = profiled.profile.rerank_ns,
                    .hbc_rerank_vector_load_ns = profiled.profile.rerank_vector_load_ns,
                    .hbc_rerank_distance_ns = profiled.profile.rerank_distance_ns,
                    .hbc_nodes_visited = profiled.profile.nodes_visited,
                    .hbc_leaves_explored = profiled.profile.leaves_explored,
                    .hbc_reranked_vectors = profiled.profile.reranked_vectors,
                    .hit_count = @intCast(sliced_hits.len),
                    .total_hits = @intCast(raw_hits.len),
                    .used_fast_path = true,
                };
            }
        }
    }
    const lookup_end = monotonicNowNs();

    const req: db_mod.types.SearchRequest = .{
        .index_name = index_name,
        .query = .{ .dense_knn = .{
            .vector = vector,
            .k = k,
        } },
        .limit = limit,
        .offset = offset,
        .include_stored = false,
    };

    const fallback_start = monotonicNowNs();
    var result = try handle.db.search(handle.alloc, req);
    defer result.deinit();
    const fallback_end = monotonicNowNs();

    const ids = try handle.alloc.alloc([]const u8, result.hits.len);
    errdefer handle.alloc.free(ids);
    var id_count: usize = 0;
    errdefer {
        for (ids[0..id_count]) |id| handle.alloc.free(id);
    }

    const scores = try handle.alloc.alloc(f32, result.hits.len);
    errdefer handle.alloc.free(scores);
    for (result.hits, 0..) |hit, i| {
        ids[i] = try handle.alloc.dupe(u8, hit.id);
        id_count += 1;
        scores[i] = hit.score orelse 0;
    }
    const total_end = monotonicNowNs();
    return .{
        .result = .{
            .alloc = handle.alloc,
            .total_hits = result.total_hits,
            .ids = ids,
            .scores = scores,
        },
        .total_ns = @intCast(total_end - total_start),
        .index_lookup_ns = @intCast(lookup_end - lookup_start),
        .search_ns = 0,
        .hits_ns = 0,
        .fallback_ns = @intCast(fallback_end - fallback_start),
        .hbc_total_ns = 0,
        .hbc_setup_ns = 0,
        .hbc_root_load_ns = 0,
        .hbc_node_cache_miss_ns = 0,
        .hbc_node_cache_misses = 0,
        .hbc_quantized_cache_miss_ns = 0,
        .hbc_quantized_cache_misses = 0,
        .hbc_child_expand_ns = 0,
        .hbc_leaf_score_ns = 0,
        .hbc_rerank_ns = 0,
        .hbc_rerank_vector_load_ns = 0,
        .hbc_rerank_distance_ns = 0,
        .hbc_nodes_visited = 0,
        .hbc_leaves_explored = 0,
        .hbc_reranked_vectors = 0,
        .hit_count = @intCast(result.hits.len),
        .total_hits = result.total_hits,
        .used_fast_path = false,
    };
}

fn searchTextMatchOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or text.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .match = .{
            .field = field,
            .text = text,
            .analyzer = if (analyzer.len > 0) analyzer else null,
            .boost = boost,
        },
    }, limit, offset);
}

fn searchTextTermOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    term: []const u8,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or term.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .term = .{
            .field = field,
            .term = term,
            .boost = boost,
        },
    }, limit, offset);
}

fn searchTextMatchPhraseOwned(
    handle: *Handle,
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    fuzziness: u16,
    auto: bool,
    boost: f32,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (field.len == 0 or text.len == 0) return error.InvalidArgument;

    return searchTextOwned(handle, index_name, .{
        .match_phrase = .{
            .field = field,
            .text = text,
            .analyzer = if (analyzer.len > 0) analyzer else null,
            .max_edits = @intCast(fuzziness),
            .auto_fuzzy = auto,
            .boost = boost,
        },
    }, limit, offset);
}

fn searchTextOwned(
    handle: *Handle,
    index_name: []const u8,
    query: db_mod.types.Query,
    limit: u32,
    offset: u32,
) !DenseOwnedResult {
    if (index_name.len == 0) return error.InvalidArgument;

    const req: db_mod.types.SearchRequest = .{
        .index_name = index_name,
        .query = query,
        .limit = limit,
        .offset = offset,
        .include_stored = false,
    };

    try handle.prepareSearchRequest(req);
    var result = try handle.db.search(handle.alloc, req);
    defer result.deinit();

    const ids = try handle.alloc.alloc([]const u8, result.hits.len);
    errdefer handle.alloc.free(ids);
    var id_count: usize = 0;
    errdefer {
        for (ids[0..id_count]) |id| handle.alloc.free(id);
    }

    const scores = try handle.alloc.alloc(f32, result.hits.len);
    errdefer handle.alloc.free(scores);
    for (result.hits, 0..) |hit, i| {
        ids[i] = try handle.alloc.dupe(u8, hit.id);
        id_count += 1;
        scores[i] = hit.score orelse 0;
    }

    return .{
        .alloc = handle.alloc,
        .total_hits = result.total_hits,
        .ids = ids,
        .scores = scores,
    };
}

pub export fn antfly_db_scan_hash_result_free(result: *capi.ScanHashResult) void {
    if (result.entries_ptr) |entries_ptr| {
        const entries = entries_ptr[0..result.entry_count];
        for (entries) |entry| {
            if (entry.id_ptr != null and entry.id_len > 0) {
                std.heap.c_allocator.free(entry.id_ptr.?[0..entry.id_len]);
            }
        }
        std.heap.c_allocator.free(entries);
    }
    result.* = .{};
}

pub export fn antfly_db_begin_transaction_with_id(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: *const [16]u8,
    timestamp_ns: u64,
    participants_ptr: ?[*]const capi.Slice,
    participant_count: usize,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    beginWithIdAndParticipants(handle, txn_id_ptr.*, timestamp_ns, participants_ptr, participant_count) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_write_transaction(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: *const [16]u8,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    if ((write_count > 0 and writes_ptr == null) or (predicate_count > 0 and predicates_ptr == null)) return .invalid_argument;
    writeIntentsInternal(handle, txn_id_ptr.*, writes_ptr, write_count, predicates_ptr, predicate_count) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_batch(
    handle_ptr: ?*anyopaque,
    writes_ptr: ?[*]const capi.WriteIntent,
    write_count: usize,
    predicates_ptr: ?[*]const capi.VersionPredicate,
    predicate_count: usize,
    timestamp_ns: u64,
    sync_level: u8,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    if ((write_count > 0 and writes_ptr == null) or (predicate_count > 0 and predicates_ptr == null)) return .invalid_argument;
    batchInternal(handle, writes_ptr, write_count, predicates_ptr, predicate_count, timestamp_ns, sync_level) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_resolve_intents(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: *const [16]u8,
    status: u8,
    commit_version: u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const txn_status: transactions_mod.TxnStatus = switch (status) {
        0 => .pending,
        1 => .committed,
        2 => .aborted,
        else => return .invalid_argument,
    };
    handle.db.resolveTransactionIntents(txn_id_ptr.*, txn_status, commit_version) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_transaction_status(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: *const [16]u8,
    out_status: *u8,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const status = handle.db.getTransactionStatus(txn_id_ptr.*) catch |err| return capi.mapError(err);
    out_status.* = @intFromEnum(status);
    return .ok;
}

pub export fn antfly_db_get_commit_version(
    handle_ptr: ?*anyopaque,
    txn_id_ptr: *const [16]u8,
    out_commit_version: *u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    out_commit_version.* = handle.db.getCommitVersion(txn_id_ptr.*) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_timestamp(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_timestamp: *u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    out_timestamp.* = handle.db.getTimestamp(handle.alloc, key.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_lookup_json(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.prepareLookupRequest(key.bytes(), .{}) catch |err| return capi.mapError(err);
    const result = handle.db.getDocument(handle.alloc, key.bytes(), .{}) catch |err| return capi.mapError(err);
    if (result == null) return .not_found;
    out_buf.* = .{
        .ptr = result.?.json.ptr,
        .len = result.?.json.len,
    };
    return .ok;
}

pub export fn antfly_db_get_raw(
    handle_ptr: ?*anyopaque,
    key: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const result = handle.db.get(handle.alloc, key.bytes()) catch |err| return capi.mapError(err);
    if (result == null) return .not_found;
    out_buf.* = .{
        .ptr = result.?.ptr,
        .len = result.?.len,
    };
    return .ok;
}

pub export fn antfly_db_lookup_artifact_json(
    handle_ptr: ?*anyopaque,
    artifact_id_b64: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.prepareLookupRequest(artifact_id_b64.bytes(), .{}) catch |err| return capi.mapError(err);
    const artifact_id = decodeBase64Alloc(handle.alloc, artifact_id_b64.bytes()) catch return .invalid_argument;
    defer handle.alloc.free(artifact_id);

    var record = handle.db.getArtifact(handle.alloc, artifact_id) catch |err| return capi.mapError(err);
    if (record == null) return .not_found;
    defer record.?.deinit(handle.alloc);

    var payload = JsonArtifactWrite.init(handle.alloc, record.?) catch return .internal;
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_decode_artifact_id_json(
    artifact_id_b64: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const alloc = std.heap.c_allocator;
    const artifact_id = decodeBase64Alloc(alloc, artifact_id_b64.bytes()) catch return .invalid_argument;
    defer alloc.free(artifact_id);

    var artifact_ref = (db_mod.artifact_ids.decodeArtifactPublicIdAlloc(alloc, artifact_id) catch return .invalid_argument) orelse return .invalid_argument;
    defer artifact_ref.deinit(alloc);

    var payload = JsonArtifactRef.init(alloc, artifact_ref) catch return .internal;
    defer payload.deinit(alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_set_schema_json(
    handle_ptr: ?*anyopaque,
    schema_json: capi.Slice,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        version: u32 = 0,
        default_type: []const u8 = "_default",
        ttl_duration_ns: u64 = 0,
        ttl_field: []const u8 = "_timestamp",
        enforce_types: bool = false,
        dynamic_templates: []const struct {
            name: []const u8 = "",
            match_pattern: ?[]const u8 = null,
            path_match: ?[]const u8 = null,
            mapping: struct {
                field_type: []const u8 = "text",
                do_index: bool = true,
                store: bool = true,
                doc_values: bool = false,
                include_in_all: bool = false,
                analyzer: []const u8 = "standard",
            } = .{},
        } = &.{},
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, schema_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    var templates = handle.alloc.alloc(schema_mod.DynamicTemplate, parsed.value.dynamic_templates.len) catch return .internal;
    var initialized: usize = 0;
    errdefer {
        for (templates[0..initialized]) |tmpl| {
            handle.alloc.free(tmpl.name);
            if (tmpl.match_pattern) |value| handle.alloc.free(value);
            if (tmpl.path_match) |value| handle.alloc.free(value);
            handle.alloc.free(tmpl.mapping.analyzer);
        }
        handle.alloc.free(templates);
    }

    for (parsed.value.dynamic_templates, 0..) |tmpl, i| {
        const field_type = parseSchemaFieldType(tmpl.mapping.field_type) catch return .invalid_argument;
        templates[i] = .{
            .name = handle.alloc.dupe(u8, tmpl.name) catch return .internal,
            .match_pattern = if (tmpl.match_pattern) |value| handle.alloc.dupe(u8, value) catch return .internal else null,
            .path_match = if (tmpl.path_match) |value| handle.alloc.dupe(u8, value) catch return .internal else null,
            .mapping = .{
                .field_type = field_type,
                .do_index = tmpl.mapping.do_index,
                .store = tmpl.mapping.store,
                .doc_values = tmpl.mapping.doc_values,
                .include_in_all = tmpl.mapping.include_in_all,
                .analyzer = handle.alloc.dupe(u8, tmpl.mapping.analyzer) catch return .internal,
            },
        };
        initialized += 1;
    }

    const table_schema: schema_mod.TableSchema = .{
        .version = parsed.value.version,
        .default_type = handle.alloc.dupe(u8, parsed.value.default_type) catch return .internal,
        .ttl_duration_ns = parsed.value.ttl_duration_ns,
        .ttl_field = handle.alloc.dupe(u8, parsed.value.ttl_field) catch return .internal,
        .enforce_types = parsed.value.enforce_types,
        .dynamic_templates = templates,
    };
    defer schema_mod.freeSchema(handle.alloc, table_schema);

    handle.db.setSchema(table_schema) catch |err| return capi.mapError(err);
    return .ok;
}

fn parseSchemaFieldType(name: []const u8) !schema_mod.AntflyType {
    if (std.mem.eql(u8, name, "text")) return .text;
    if (std.mem.eql(u8, name, "keyword")) return .keyword;
    if (std.mem.eql(u8, name, "numeric")) return .numeric;
    if (std.mem.eql(u8, name, "embedding")) return .embedding;
    if (std.mem.eql(u8, name, "link")) return .link;
    if (std.mem.eql(u8, name, "boolean")) return .boolean;
    if (std.mem.eql(u8, name, "datetime")) return .datetime;
    if (std.mem.eql(u8, name, "geopoint")) return .geopoint;
    if (std.mem.eql(u8, name, "geoshape")) return .geoshape;
    if (std.mem.eql(u8, name, "blob")) return .blob;
    if (std.mem.eql(u8, name, "html")) return .html;
    if (std.mem.eql(u8, name, "search_as_you_type")) return .search_as_you_type;
    return error.InvalidArgument;
}

fn antflyDbExtractEnrichmentsJson(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const writes = decodeBatchWritesRequest(handle.alloc, request_json.bytes()) catch return .invalid_argument;
    defer freeOwnedBatchWrites(handle.alloc, writes);

    var result = handle.db.extractEnrichments(handle.alloc, writes) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var payload = buildJsonExtractEnrichmentsResult(handle.alloc, result) catch return .internal;
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

fn antflyDbComputeEnrichmentsJson(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) callconv(.c) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const writes = decodeBatchWritesRequest(handle.alloc, request_json.bytes()) catch return .invalid_argument;
    defer freeOwnedBatchWrites(handle.alloc, writes);

    var result = handle.db.computeEnrichments(handle.alloc, writes) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var payload = buildJsonComputeEnrichmentsResult(handle.alloc, result) catch return .internal;
    defer payload.deinit(handle.alloc);

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

comptime {
    @export(&antflyDbExtractEnrichmentsJson, .{
        .name = "antfly_db_extract_enrichments_json",
        .linkage = .strong,
    });
    @export(&antflyDbComputeEnrichmentsJson, .{
        .name = "antfly_db_compute_enrichments_json",
        .linkage = .strong,
    });
}

pub export fn antfly_db_update_range(
    handle_ptr: ?*anyopaque,
    start: capi.Slice,
    end: capi.Slice,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.updateRange(.{
        .start = start.bytes(),
        .end = end.bytes(),
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_range_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    var payload = JsonRange.init(handle.alloc, handle.db.getRange()) catch return .internal;
    defer payload.deinit(handle.alloc);
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_get_split_state_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const state = handle.db.getSplitState(handle.alloc) catch |err| return capi.mapError(err);
    if (state == null) return .not_found;
    var payload = JsonSplitState.init(handle.alloc, state.?) catch return .internal;
    defer payload.deinit(handle.alloc);
    defer db_mod.types.freeSplitState(handle.alloc, state);
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_set_split_state_json(
    handle_ptr: ?*anyopaque,
    state_json: capi.Slice,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const ParsedState = struct {
        phase: u8,
        split_key_b64: []const u8,
        new_shard_id: u64,
        started_at: u64,
        original_range_end_b64: []const u8,
    };
    var parsed = std.json.parseFromSlice(ParsedState, handle.alloc, state_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const split_key = decodeBase64Alloc(handle.alloc, parsed.value.split_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(split_key);
    const original_range_end = decodeBase64Alloc(handle.alloc, parsed.value.original_range_end_b64) catch return .invalid_argument;
    defer handle.alloc.free(original_range_end);
    const phase: db_mod.types.SplitPhase = switch (parsed.value.phase) {
        0 => .none,
        1 => .prepare,
        2 => .splitting,
        3 => .finalizing,
        4 => .rolling_back,
        else => return .invalid_argument,
    };
    handle.db.setSplitState(.{
        .phase = phase,
        .split_key = split_key,
        .new_shard_id = parsed.value.new_shard_id,
        .started_at = parsed.value.started_at,
        .original_range_end = original_range_end,
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_clear_split_state(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.clearSplitState() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_split_delta_seq(
    handle_ptr: ?*anyopaque,
    out_seq: *u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    out_seq.* = handle.db.getSplitDeltaSeq();
    return .ok;
}

pub export fn antfly_db_get_split_delta_final_seq(
    handle_ptr: ?*anyopaque,
    out_seq: *u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    out_seq.* = handle.db.getSplitDeltaFinalSeq(handle.alloc) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_set_split_delta_final_seq(
    handle_ptr: ?*anyopaque,
    seq: u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.setSplitDeltaFinalSeq(seq) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_clear_split_delta_final_seq(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.clearSplitDeltaFinalSeq() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_list_split_delta_entries_after_json(
    handle_ptr: ?*anyopaque,
    after_seq: u64,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const entries = handle.db.listSplitDeltaEntriesAfter(handle.alloc, after_seq) catch |err| return capi.mapError(err);
    defer db_mod.types.freeSplitDeltaEntries(handle.alloc, entries);

    var payload = handle.alloc.alloc(JsonSplitDeltaEntry, entries.len) catch return .internal;
    var payload_count: usize = 0;
    defer {
        for (payload[0..payload_count]) |*entry| entry.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (entries, 0..) |entry, i| {
        payload[i] = JsonSplitDeltaEntry.init(handle.alloc, entry) catch return .internal;
        payload_count += 1;
    }

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_clear_split_delta_entries(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.clearSplitDeltaEntries() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_list_indexes_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const configs = handle.db.listIndexes(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeIndexConfigs(handle.alloc, configs);

    var payload = handle.alloc.alloc(JsonIndexConfig, configs.len) catch return .internal;
    defer handle.alloc.free(payload);
    for (configs, 0..) |cfg, i| {
        payload[i] = .{
            .name = cfg.name,
            .kind = @tagName(cfg.kind),
            .config_json = cfg.config_json,
        };
    }

    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_scan_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        from_key_b64: []const u8 = "",
        to_key_b64: []const u8 = "",
        inclusive_from: bool = false,
        exclusive_to: bool = false,
        include_documents: bool = false,
        limit: u32 = 0,
        fields: []const []const u8 = &.{},
        include_all_fields: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .ignore_unknown_fields = true }) catch return .invalid_argument;
    defer parsed.deinit();

    const from_key = decodeBase64Alloc(handle.alloc, parsed.value.from_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(from_key);
    const to_key = decodeBase64Alloc(handle.alloc, parsed.value.to_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(to_key);
    const opts: db_mod.types.ScanOptions = .{
        .inclusive_from = parsed.value.inclusive_from,
        .exclusive_to = parsed.value.exclusive_to,
        .include_documents = parsed.value.include_documents,
        .limit = parsed.value.limit,
        .fields = parsed.value.fields,
        .include_all_fields = parsed.value.include_all_fields,
    };
    handle.prepareScanRequest(from_key, to_key, opts) catch |err| return capi.mapError(err);
    var result = handle.db.scan(handle.alloc, from_key, to_key, opts) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    var hashes = handle.alloc.alloc(JsonScanHash, result.hashes.len) catch return .internal;
    var hash_count: usize = 0;
    defer {
        for (hashes[0..hash_count]) |*item| item.deinit(handle.alloc);
        if (hashes.len > 0) handle.alloc.free(hashes);
    }
    for (result.hashes, 0..) |item, i| {
        hashes[i] = JsonScanHash.init(handle.alloc, item) catch return .internal;
        hash_count += 1;
    }

    var documents = handle.alloc.alloc(JsonScanDocument, result.documents.len) catch return .internal;
    var document_count: usize = 0;
    defer {
        for (documents[0..document_count]) |*item| item.deinit(handle.alloc);
        if (documents.len > 0) handle.alloc.free(documents);
    }
    for (result.documents, 0..) |item, i| {
        documents[i] = JsonScanDocument.init(handle.alloc, item) catch return .internal;
        document_count += 1;
    }

    out_buf.* = stringifyJson(JsonScanResult{
        .hashes = hashes,
        .documents = documents,
    }) catch return .internal;
    return .ok;
}

pub export fn antfly_db_scan_hashes(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_result: *capi.ScanHashResult,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        from_key_b64: []const u8 = "",
        to_key_b64: []const u8 = "",
        inclusive_from: bool = false,
        exclusive_to: bool = false,
        limit: u32 = 0,
        fields: []const []const u8 = &.{},
        include_all_fields: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{ .ignore_unknown_fields = true }) catch return .invalid_argument;
    defer parsed.deinit();

    const from_key = decodeBase64Alloc(handle.alloc, parsed.value.from_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(from_key);
    const to_key = decodeBase64Alloc(handle.alloc, parsed.value.to_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(to_key);
    const opts: db_mod.types.ScanOptions = .{
        .inclusive_from = parsed.value.inclusive_from,
        .exclusive_to = parsed.value.exclusive_to,
        .include_documents = false,
        .limit = parsed.value.limit,
        .fields = parsed.value.fields,
        .include_all_fields = parsed.value.include_all_fields,
    };
    handle.prepareScanRequest(from_key, to_key, opts) catch |err| return capi.mapError(err);
    var result = handle.db.scan(handle.alloc, from_key, to_key, opts) catch |err| return capi.mapError(err);
    defer result.deinit(handle.alloc);

    const entries = std.heap.c_allocator.alloc(capi.ScanHashEntry, result.hashes.len) catch return .internal;
    errdefer std.heap.c_allocator.free(entries);
    for (result.hashes, 0..) |item, i| {
        const id = std.heap.c_allocator.alloc(u8, item.id.len) catch {
            for (entries[0..i]) |entry| {
                if (entry.id_ptr != null and entry.id_len > 0) {
                    std.heap.c_allocator.free(entry.id_ptr.?[0..entry.id_len]);
                }
            }
            return .internal;
        };
        @memcpy(id, item.id);
        entries[i] = .{
            .id_ptr = id.ptr,
            .id_len = id.len,
            .hash = item.hash,
        };
    }

    out_result.* = .{
        .entries_ptr = entries.ptr,
        .entry_count = entries.len,
    };
    return .ok;
}

pub export fn antfly_db_stats_json(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const stats = handle.db.stats(handle.alloc) catch |err| return capi.mapError(err);
    defer db_mod.types.freeDBStats(handle.alloc, stats);

    var indexes = handle.alloc.alloc(JsonDBIndexStats, stats.indexes.len) catch return .internal;
    defer if (indexes.len > 0) handle.alloc.free(indexes);
    for (stats.indexes, 0..) |item, i| {
        indexes[i] = .{
            .name = item.name,
            .kind = @tagName(item.kind),
            .doc_count = item.doc_count,
            .term_count = item.term_count,
            .edge_count = item.edge_count,
            .node_count = item.node_count,
        };
    }

    out_buf.* = stringifyJson(JsonDBStats{
        .doc_count = stats.doc_count,
        .index_count = stats.index_count,
        .indexes = indexes,
        .enrichment = .{
            .enabled = stats.enrichment.enabled,
            .lease_owned = stats.enrichment.lease_owned,
            .has_lease = stats.enrichment.has_lease,
            .acquisition_count = stats.enrichment.acquisition_count,
            .lease_acquire_failures = stats.enrichment.lease_acquire_failures,
            .lost_leases = stats.enrichment.lost_leases,
            .last_acquired_ms = stats.enrichment.last_acquired_ms,
            .target_sequence = stats.enrichment.target_sequence,
            .applied_sequence = stats.enrichment.applied_sequence,
            .processed_requests = stats.enrichment.processed_requests,
            .error_count = stats.enrichment.error_count,
            .retryable_error_count = stats.enrichment.retryable_error_count,
            .fatal_error_count = stats.enrichment.fatal_error_count,
            .retrying = stats.enrichment.retrying,
            .worker_failed = stats.enrichment.worker_failed,
            .skip_by_hash_count = stats.enrichment.skip_by_hash_count,
            .codec_decode_failures = stats.enrichment.codec_decode_failures,
            .dense_artifact_bytes_written = stats.enrichment.dense_artifact_bytes_written,
            .sparse_artifact_bytes_written = stats.enrichment.sparse_artifact_bytes_written,
            .chunk_artifact_bytes_written = stats.enrichment.chunk_artifact_bytes_written,
            .artifact_bytes_written = stats.enrichment.artifact_bytes_written,
        },
        .ttl_cleanup = .{
            .enabled = stats.ttl_cleanup.enabled,
            .lease_owned = stats.ttl_cleanup.lease_owned,
            .has_lease = stats.ttl_cleanup.has_lease,
            .acquisition_count = stats.ttl_cleanup.acquisition_count,
            .runs = stats.ttl_cleanup.runs,
            .scanned_timestamps = stats.ttl_cleanup.scanned_timestamps,
            .deleted_docs = stats.ttl_cleanup.deleted_docs,
            .last_run_ns = stats.ttl_cleanup.last_run_ns,
            .error_count = stats.ttl_cleanup.error_count,
            .lease_acquire_failures = stats.ttl_cleanup.lease_acquire_failures,
            .lost_leases = stats.ttl_cleanup.lost_leases,
            .last_acquired_ms = stats.ttl_cleanup.last_acquired_ms,
        },
        .transaction_recovery = .{
            .enabled = stats.transaction_recovery.enabled,
            .lease_owned = stats.transaction_recovery.lease_owned,
            .has_lease = stats.transaction_recovery.has_lease,
            .acquisition_count = stats.transaction_recovery.acquisition_count,
            .lease_acquire_failures = stats.transaction_recovery.lease_acquire_failures,
            .lost_leases = stats.transaction_recovery.lost_leases,
            .last_acquired_ms = stats.transaction_recovery.last_acquired_ms,
            .runs = stats.transaction_recovery.runs,
            .scanned_records = stats.transaction_recovery.scanned_records,
            .auto_aborted = stats.transaction_recovery.auto_aborted,
            .resolved_finalized = stats.transaction_recovery.resolved_finalized,
            .cleaned_records = stats.transaction_recovery.cleaned_records,
            .kept_recent_pending = stats.transaction_recovery.kept_recent_pending,
            .deferred_unresolved = stats.transaction_recovery.deferred_unresolved,
            .notification_attempts = stats.transaction_recovery.notification_attempts,
            .notification_successes = stats.transaction_recovery.notification_successes,
            .notification_failures = stats.transaction_recovery.notification_failures,
            .last_run_ns = stats.transaction_recovery.last_run_ns,
            .error_count = stats.transaction_recovery.error_count,
        },
        .text_merge = .{
            .enabled = stats.text_merge.enabled,
            .pending_indexes = stats.text_merge.pending_indexes,
            .pending_segments = stats.text_merge.pending_segments,
            .pending_bytes = stats.text_merge.pending_bytes,
            .in_flight_merges = stats.text_merge.in_flight_merges,
            .in_flight_segments = stats.text_merge.in_flight_segments,
            .completed_merges = stats.text_merge.completed_merges,
            .skipped_stale_merges = stats.text_merge.skipped_stale_merges,
            .failed_merges = stats.text_merge.failed_merges,
            .quarantined_merges = stats.text_merge.quarantined_merges,
            .quarantined_segments = stats.text_merge.quarantined_segments,
            .last_merge_error = stats.text_merge.last_merge_error,
            .backpressure_events = stats.text_merge.backpressure_events,
            .backpressure_ns = stats.text_merge.backpressure_ns,
            .max_pending_segments = stats.text_merge.max_pending_segments,
            .max_pending_bytes = stats.text_merge.max_pending_bytes,
        },
        .term_doc_freq_cache_hits = stats.term_doc_freq_cache_hits,
        .term_doc_freq_cache_misses = stats.term_doc_freq_cache_misses,
    }) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        mode: []const u8,
        index_name: []const u8 = "",
        text_query_type: []const u8 = "",
        text_query_json: []const u8 = "",
        field: []const u8 = "",
        text: []const u8 = "",
        vector: []const f32 = &.{},
        indices: []const u32 = &.{},
        values: []const f32 = &.{},
        k: u32 = 10,
        return_mode: []const u8 = "parent",
        max_chunks_per_parent: u32 = 0,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = true,
        filter_prefix: []const u8 = "",
        distance_over: ?f32 = null,
        distance_under: ?f32 = null,
        filter_ids: []const u64 = &.{},
        exclude_ids: []const u64 = &.{},
        aggregations: []const JsonSearchAggregationRequest = &.{},
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch |err| {
        std.debug.print("pattern parse error={s}\n", .{@errorName(err)});
        return .invalid_argument;
    };
    defer parsed.deinit();

    var query_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer query_arena.deinit();
    const query_alloc = query_arena.allocator();

    const return_mode: db_mod.types.ReturnMode = if (std.mem.eql(u8, parsed.value.return_mode, "chunk"))
        .chunk
    else if (std.mem.eql(u8, parsed.value.return_mode, "parent_with_chunks"))
        .parent_with_chunks
    else
        .parent;

    var req: db_mod.types.SearchRequest = .{
        .index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .return_mode = return_mode,
        .max_chunks_per_parent = parsed.value.max_chunks_per_parent,
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = parsed.value.include_stored,
        .filter_prefix = parsed.value.filter_prefix,
        .distance_over = parsed.value.distance_over,
        .distance_under = parsed.value.distance_under,
        .filter_ids = parsed.value.filter_ids,
        .exclude_ids = parsed.value.exclude_ids,
    };

    if (std.mem.eql(u8, parsed.value.mode, "full_text")) {
        if (parsed.value.text_query_json.len > 0) {
            var parsed_query = std.json.parseFromSlice(std.json.Value, query_alloc, parsed.value.text_query_json, .{}) catch return .invalid_argument;
            defer parsed_query.deinit();
            req.full_text = parseTextQueryJson(query_alloc, parsed_query.value) catch return .invalid_argument;
        } else {
            req.full_text = if (std.mem.eql(u8, parsed.value.text_query_type, "term"))
                .{ .term = .{ .field = parsed.value.field, .term = parsed.value.text } }
            else if (std.mem.eql(u8, parsed.value.text_query_type, "match"))
                .{ .match = .{ .field = parsed.value.field, .text = parsed.value.text } }
            else
                .{ .match_all = {} };
        }
    } else if (std.mem.eql(u8, parsed.value.mode, "dense")) {
        req.dense = .{
            .vector = parsed.value.vector,
            .k = parsed.value.k,
        };
    } else if (std.mem.eql(u8, parsed.value.mode, "sparse")) {
        req.sparse = .{
            .indices = parsed.value.indices,
            .values = parsed.value.values,
            .k = parsed.value.k,
        };
    } else {
        return .invalid_argument;
    }

    handle.prepareSearchRequest(req) catch |err| return capi.mapError(err);
    var result = handle.db.search(handle.alloc, req) catch |err| return capi.mapError(err);
    defer result.deinit();

    var aggregation_results: []JsonSearchAggregationResult = &.{};
    if (parsed.value.aggregations.len > 0) {
        var agg_source_is_full = result.hits.len == result.total_hits;
        var full_result: ?db_mod.types.SearchResult = null;
        defer {
            if (full_result) |*value| value.deinit();
        }
        if (!agg_source_is_full) {
            var agg_req = req;
            agg_req.offset = 0;
            agg_req.limit = if (result.total_hits == 0) 1 else result.total_hits;
            agg_req.include_stored = true;
            full_result = handle.db.search(handle.alloc, agg_req) catch |err| return capi.mapError(err);
            agg_source_is_full = true;
        }
        const source = if (full_result) |*value| value else &result;
        const requests = toAggregationRequest(handle.alloc, parsed.value.aggregations) catch return .internal;
        defer freeAggregationRequests(handle.alloc, requests);
        const backend_results = aggregations_mod.computeSearchAggregations(handle.alloc, requests, source.*, .{
            .index_manager = handle.db.core.index_manager,
            .full_text_index_name = req.index_name,
        }) catch |err| return capi.mapError(err);
        defer aggregations_mod.deinitResults(handle.alloc, backend_results);
        aggregation_results = toJsonAggregationResults(handle.alloc, backend_results) catch return .internal;
    }
    defer {
        for (aggregation_results) |*item| item.deinit(handle.alloc);
        if (aggregation_results.len > 0) handle.alloc.free(aggregation_results);
    }

    var hits = handle.alloc.alloc(JsonSearchHit, result.hits.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (hits[0..count]) |*item| item.deinit(handle.alloc);
        if (hits.len > 0) handle.alloc.free(hits);
    }
    for (result.hits, 0..) |hit, i| {
        hits[i] = JsonSearchHit.init(handle.alloc, hit) catch return .internal;
        count += 1;
    }
    var graph_results = handle.alloc.alloc(JsonGraphSearchResult, result.graph_results.len) catch return .internal;
    var graph_count: usize = 0;
    defer {
        for (graph_results[0..graph_count]) |*item| item.deinit(handle.alloc);
        if (graph_results.len > 0) handle.alloc.free(graph_results);
    }
    for (result.graph_results, 0..) |graph_result, i| {
        graph_results[i] = JsonGraphSearchResult.init(handle.alloc, graph_result) catch return .internal;
        graph_count += 1;
    }
    out_buf.* = stringifyJson(JsonSearchResult{
        .total_hits = result.total_hits,
        .hits = hits,
        .graph_results = graph_results,
        .aggregations = aggregation_results,
    }) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    vector_ptr: ?[*]const f32,
    vector_len: usize,
    k: u32,
    limit: u32,
    offset: u32,
    out_result: *capi.PackedDenseSearchResult,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    if (vector_ptr == null or vector_len == 0) return .invalid_argument;
    handle.prepareDenseSearchRequest(index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);

    const fast = searchDensePackedFast(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset, out_result) catch |err| return capi.mapError(err);
    if (fast) return .ok;

    var owned = searchDenseOwned(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    packDenseHits(handle.alloc, owned.total_hits, owned.ids, owned.scores, out_result) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense_profile(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    vector_ptr: ?[*]const f32,
    vector_len: usize,
    k: u32,
    limit: u32,
    offset: u32,
    out_profile: *capi.DenseSearchProfile,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    if (vector_ptr == null or vector_len == 0) return .invalid_argument;
    handle.prepareDenseSearchRequest(index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);

    var profiled = searchDenseOwnedProfiled(handle, index_name.bytes(), vector_ptr.?[0..vector_len], k, limit, offset) catch |err| return capi.mapError(err);
    defer profiled.deinit();

    out_profile.* = .{
        .total_ns = profiled.total_ns,
        .index_lookup_ns = profiled.index_lookup_ns,
        .search_ns = profiled.search_ns,
        .hits_ns = profiled.hits_ns,
        .fallback_ns = profiled.fallback_ns,
        .hbc_total_ns = profiled.hbc_total_ns,
        .hbc_setup_ns = profiled.hbc_setup_ns,
        .hbc_root_load_ns = profiled.hbc_root_load_ns,
        .hbc_node_cache_miss_ns = profiled.hbc_node_cache_miss_ns,
        .hbc_node_cache_misses = profiled.hbc_node_cache_misses,
        .hbc_quantized_cache_miss_ns = profiled.hbc_quantized_cache_miss_ns,
        .hbc_quantized_cache_misses = profiled.hbc_quantized_cache_misses,
        .hbc_child_expand_ns = profiled.hbc_child_expand_ns,
        .hbc_leaf_score_ns = profiled.hbc_leaf_score_ns,
        .hbc_rerank_ns = profiled.hbc_rerank_ns,
        .hbc_rerank_vector_load_ns = profiled.hbc_rerank_vector_load_ns,
        .hbc_rerank_distance_ns = profiled.hbc_rerank_distance_ns,
        .hbc_nodes_visited = profiled.hbc_nodes_visited,
        .hbc_leaves_explored = profiled.hbc_leaves_explored,
        .hbc_reranked_vectors = profiled.hbc_reranked_vectors,
        .hit_count = profiled.hit_count,
        .total_hits = profiled.total_hits,
        .used_fast_path = profiled.used_fast_path,
    };
    return .ok;
}

pub export fn antfly_db_dense_noop(handle_ptr: ?*anyopaque) capi.ErrorCode {
    _ = asHandle(handle_ptr) orelse return .invalid_argument;
    return .ok;
}

pub export fn antfly_db_dense_fixed_packed_result(
    handle_ptr: ?*anyopaque,
    out_result: *capi.PackedDenseSearchResult,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;

    const ids = [_][]const u8{ "doc-fixed-1", "doc-fixed-2", "doc-fixed-3" };
    const scores = [_]f32{ 0.125, 0.25, 0.5 };
    packDenseHits(handle.alloc, @intCast(ids.len), &ids, &scores, out_result) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;

    var req = search_wire.decodeDenseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    handle.prepareDenseSearchRequest(req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);

    const maybe_fast = searchDenseWireFast(handle, req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);
    if (maybe_fast) |out| {
        out_buf.* = out;
        return .ok;
    }

    var owned = searchDenseOwned(handle, req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponse(owned.total_hits, owned.ids, owned.scores) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_dense_wire_profile(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
    out_profile: *capi.DenseWireSearchProfile,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;

    var req = search_wire.decodeDenseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeDenseRequest(handle.alloc, &req);
    handle.prepareDenseSearchRequest(req.index_name, req.vector, req.k, req.limit, req.offset) catch |err| return capi.mapError(err);

    const profiled = searchDenseWireOwnedProfiled(handle, request_buf.bytes()) catch |err| return capi.mapError(err);
    out_buf.* = profiled.out;
    out_profile.* = .{
        .total_ns = profiled.total_ns,
        .decode_ns = profiled.decode_ns,
        .search_ns = profiled.search_ns,
        .resolve_ns = profiled.resolve_ns,
        .encode_ns = profiled.encode_ns,
        .fallback_ns = profiled.fallback_ns,
        .hbc_total_ns = profiled.hbc_total_ns,
        .hbc_setup_ns = profiled.hbc_setup_ns,
        .hbc_root_load_ns = profiled.hbc_root_load_ns,
        .hbc_node_cache_miss_ns = profiled.hbc_node_cache_miss_ns,
        .hbc_node_cache_misses = profiled.hbc_node_cache_misses,
        .hbc_quantized_cache_miss_ns = profiled.hbc_quantized_cache_miss_ns,
        .hbc_quantized_cache_misses = profiled.hbc_quantized_cache_misses,
        .hbc_child_expand_ns = profiled.hbc_child_expand_ns,
        .hbc_leaf_score_ns = profiled.hbc_leaf_score_ns,
        .hbc_rerank_ns = profiled.hbc_rerank_ns,
        .hbc_rerank_vector_load_ns = profiled.hbc_rerank_vector_load_ns,
        .hbc_rerank_distance_ns = profiled.hbc_rerank_distance_ns,
        .hbc_nodes_visited = profiled.hbc_nodes_visited,
        .hbc_leaves_explored = profiled.hbc_leaves_explored,
        .hbc_reranked_vectors = profiled.hbc_reranked_vectors,
        .hit_count = profiled.hit_count,
        .total_hits = profiled.total_hits,
        .used_fast_path = profiled.used_fast_path,
    };
    return .ok;
}

pub export fn antfly_db_search_text_match(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    field: capi.Slice,
    text: capi.Slice,
    limit: u32,
    offset: u32,
    out_result: *capi.DenseSearchResult,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    var owned = searchTextMatchOwned(handle, index_name.bytes(), field.bytes(), text.bytes(), "", 1.0, limit, offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    const hits = handle.alloc.alloc(capi.DenseSearchHit, owned.ids.len) catch return .internal;
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) handle.alloc.free(hit.id_ptr.?[0..hit.id_len]);
        }
        handle.alloc.free(hits);
    }
    for (owned.ids, owned.scores, 0..) |id, score, i| {
        const duped = handle.alloc.dupe(u8, id) catch return .internal;
        hits[i] = .{
            .id_ptr = duped.ptr,
            .id_len = duped.len,
            .score = score,
        };
        initialized += 1;
    }
    out_result.* = .{
        .hits_ptr = hits.ptr,
        .hit_count = hits.len,
        .total_hits = owned.total_hits,
    };
    return .ok;
}

pub export fn antfly_db_search_text_match_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;

    var req = search_wire.decodeTextMatchRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextMatchRequest(handle.alloc, &req);

    var owned = searchTextMatchOwned(handle, req.index_name, req.field, req.text, req.analyzer, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponse(owned.total_hits, owned.ids, owned.scores) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_text_term_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;

    var req = search_wire.decodeTextTermRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextTermRequest(handle.alloc, &req);

    var owned = searchTextTermOwned(handle, req.index_name, req.field, req.text, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponse(owned.total_hits, owned.ids, owned.scores) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_text_match_phrase_wire(
    handle_ptr: ?*anyopaque,
    request_buf: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;

    var req = search_wire.decodeTextMatchPhraseRequest(handle.alloc, request_buf.bytes()) catch |err| return capi.mapError(err);
    defer search_wire.freeTextMatchPhraseRequest(handle.alloc, &req);

    var owned = searchTextMatchPhraseOwned(handle, req.index_name, req.field, req.text, req.analyzer, req.fuzziness, req.auto, req.boost, req.limit, req.offset) catch |err| return capi.mapError(err);
    defer owned.deinit();

    out_buf.* = search_wire.encodeDenseResponse(owned.total_hits, owned.ids, owned.scores) catch return .internal;
    return .ok;
}

pub export fn antfly_db_search_hits_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_result: *capi.DenseSearchResult,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        mode: []const u8,
        index_name: []const u8 = "",
        text_query_type: []const u8 = "",
        text_query_json: []const u8 = "",
        field: []const u8 = "",
        text: []const u8 = "",
        vector: []const f32 = &.{},
        indices: []const u32 = &.{},
        values: []const f32 = &.{},
        k: u32 = 10,
        return_mode: []const u8 = "parent",
        max_chunks_per_parent: u32 = 0,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = false,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    if (parsed.value.include_stored) return .invalid_argument;
    if (!std.mem.eql(u8, parsed.value.return_mode, "parent")) return .invalid_argument;

    var query_arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer query_arena.deinit();
    const query_alloc = query_arena.allocator();

    var req: db_mod.types.SearchRequest = .{
        .index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
        .return_mode = .parent,
        .max_chunks_per_parent = parsed.value.max_chunks_per_parent,
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = false,
    };

    if (std.mem.eql(u8, parsed.value.mode, "full_text")) {
        if (parsed.value.text_query_json.len > 0) {
            var parsed_query = std.json.parseFromSlice(std.json.Value, query_alloc, parsed.value.text_query_json, .{}) catch return .invalid_argument;
            defer parsed_query.deinit();
            req.full_text = parseTextQueryJson(query_alloc, parsed_query.value) catch return .invalid_argument;
        } else {
            req.full_text = if (std.mem.eql(u8, parsed.value.text_query_type, "term"))
                .{ .term = .{ .field = parsed.value.field, .term = parsed.value.text } }
            else if (std.mem.eql(u8, parsed.value.text_query_type, "match"))
                .{ .match = .{ .field = parsed.value.field, .text = parsed.value.text } }
            else
                .{ .match_all = {} };
        }
    } else if (std.mem.eql(u8, parsed.value.mode, "sparse")) {
        req.sparse = .{
            .indices = parsed.value.indices,
            .values = parsed.value.values,
            .k = parsed.value.k,
        };
    } else {
        return .invalid_argument;
    }

    handle.prepareSearchRequest(req) catch |err| return capi.mapError(err);
    var result = handle.db.search(handle.alloc, req) catch |err| return capi.mapError(err);
    defer result.deinit();
    if (result.graph_results.len > 0) return .invalid_argument;

    const hits = handle.alloc.alloc(capi.DenseSearchHit, result.hits.len) catch return .internal;
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            if (hit.id_ptr != null and hit.id_len > 0) handle.alloc.free(hit.id_ptr.?[0..hit.id_len]);
        }
        handle.alloc.free(hits);
    }
    for (result.hits, 0..) |hit, i| {
        if (hit.stored_data != null or hit.chunk_hits.len > 0) return .invalid_argument;
        const id = handle.alloc.dupe(u8, hit.id) catch return .internal;
        hits[i] = .{
            .id_ptr = id.ptr,
            .id_len = id.len,
            .score = hit.score orelse 0,
        };
        initialized += 1;
    }
    out_result.* = .{
        .hits_ptr = hits.ptr,
        .hit_count = hits.len,
        .total_hits = result.total_hits,
    };
    return .ok;
}

fn parseTextQueryJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror!db_mod.types.TextQuery {
    if (value != .object) return error.InvalidArgument;
    if (value.object.get("match_all") != null) {
        return .{ .match_all = {} };
    }
    if (value.object.get("match_none") != null) {
        return .{ .match_none = {} };
    }
    if (value.object.get("phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .terms = try parseStringArrayJson(alloc, phrase.object.get("terms") orelse return error.InvalidArgument),
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("multi_phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .multi_phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .terms = try parseStringMatrixJson(alloc, phrase.object.get("terms") orelse return error.InvalidArgument),
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("term")) |term| {
        if (term != .object) return error.InvalidArgument;
        return .{ .term = .{
            .field = (term.object.get("field") orelse return error.InvalidArgument).string,
            .term = (term.object.get("term") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(term.object),
        } };
    }
    if (value.object.get("match")) |match| {
        if (match != .object) return error.InvalidArgument;
        return .{ .match = .{
            .field = (match.object.get("field") orelse return error.InvalidArgument).string,
            .text = (match.object.get("text") orelse return error.InvalidArgument).string,
            .analyzer = if (match.object.get("analyzer")) |analyzer| switch (analyzer) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .boost = try parseOptionalBoostJson(match.object),
        } };
    }
    if (value.object.get("match_phrase")) |phrase| {
        if (phrase != .object) return error.InvalidArgument;
        const edits_value = phrase.object.get("max_edits") orelse std.json.Value{ .integer = 0 };
        return .{ .match_phrase = .{
            .field = (phrase.object.get("field") orelse return error.InvalidArgument).string,
            .text = (phrase.object.get("text") orelse return error.InvalidArgument).string,
            .analyzer = if (phrase.object.get("analyzer")) |analyzer| switch (analyzer) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .auto_fuzzy = if (phrase.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(phrase.object),
        } };
    }
    if (value.object.get("fuzzy")) |fuzzy| {
        if (fuzzy != .object) return error.InvalidArgument;
        const edits_value = fuzzy.object.get("max_edits") orelse std.json.Value{ .integer = 1 };
        return .{ .fuzzy = .{
            .field = (fuzzy.object.get("field") orelse return error.InvalidArgument).string,
            .term = (fuzzy.object.get("term") orelse return error.InvalidArgument).string,
            .max_edits = @intCast(switch (edits_value) {
                .integer => |v| v,
                else => return error.InvalidArgument,
            }),
            .prefix_len = if (fuzzy.object.get("prefix_length")) |prefix| switch (prefix) {
                .integer => |v| @intCast(v),
                else => return error.InvalidArgument,
            } else 0,
            .auto_fuzzy = if (fuzzy.object.get("auto_fuzzy")) |auto| switch (auto) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(fuzzy.object),
        } };
    }
    if (value.object.get("numeric_range")) |range_query| {
        if (range_query != .object) return error.InvalidArgument;
        return .{ .numeric_range = .{
            .field = (range_query.object.get("field") orelse return error.InvalidArgument).string,
            .min = if (range_query.object.get("min")) |min| switch (min) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max = if (range_query.object.get("max")) |max| switch (max) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_min = if (range_query.object.get("inclusive_min")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_max = if (range_query.object.get("inclusive_max")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(range_query.object),
        } };
    }
    if (value.object.get("date_range")) |range_query| {
        if (range_query != .object) return error.InvalidArgument;
        return .{ .date_range = .{
            .field = (range_query.object.get("field") orelse return error.InvalidArgument).string,
            .start_ns = if (range_query.object.get("start_ns")) |start| switch (start) {
                .integer => |v| @intCast(v),
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .end_ns = if (range_query.object.get("end_ns")) |end| switch (end) {
                .integer => |v| @intCast(v),
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_start = if (range_query.object.get("inclusive_start")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_end = if (range_query.object.get("inclusive_end")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(range_query.object),
        } };
    }
    if (value.object.get("doc_id")) |doc_id| {
        if (doc_id != .object) return error.InvalidArgument;
        return .{ .doc_id = .{
            .ids = try parseStringArrayJson(alloc, doc_id.object.get("ids") orelse return error.InvalidArgument),
            .boost = try parseOptionalBoostJson(doc_id.object),
        } };
    }
    if (value.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return error.InvalidArgument;
        return .{ .bool_field = .{
            .field = (bool_field.object.get("field") orelse return error.InvalidArgument).string,
            .value = switch (bool_field.object.get("value") orelse return error.InvalidArgument) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(bool_field.object),
        } };
    }
    if (value.object.get("geo_distance")) |geo_distance| {
        if (geo_distance != .object) return error.InvalidArgument;
        return .{ .geo_distance = .{
            .field = (geo_distance.object.get("field") orelse return error.InvalidArgument).string,
            .lon = switch (geo_distance.object.get("lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .lat = switch (geo_distance.object.get("lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .radius_meters = switch (geo_distance.object.get("radius_meters") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(geo_distance.object),
        } };
    }
    if (value.object.get("geo_bbox")) |geo_bbox| {
        if (geo_bbox != .object) return error.InvalidArgument;
        return .{ .geo_bbox = .{
            .field = (geo_bbox.object.get("field") orelse return error.InvalidArgument).string,
            .min_lat = switch (geo_bbox.object.get("min_lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .min_lon = switch (geo_bbox.object.get("min_lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .max_lat = switch (geo_bbox.object.get("max_lat") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .max_lon = switch (geo_bbox.object.get("max_lon") orelse return error.InvalidArgument) {
                .integer => |v| @floatFromInt(v),
                .float => |v| v,
                else => return error.InvalidArgument,
            },
            .boost = try parseOptionalBoostJson(geo_bbox.object),
        } };
    }
    if (value.object.get("prefix")) |prefix| {
        if (prefix != .object) return error.InvalidArgument;
        return .{ .prefix = .{
            .field = (prefix.object.get("field") orelse return error.InvalidArgument).string,
            .prefix = (prefix.object.get("prefix") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(prefix.object),
        } };
    }
    if (value.object.get("wildcard")) |wildcard| {
        if (wildcard != .object) return error.InvalidArgument;
        return .{ .wildcard = .{
            .field = (wildcard.object.get("field") orelse return error.InvalidArgument).string,
            .pattern = (wildcard.object.get("pattern") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(wildcard.object),
        } };
    }
    if (value.object.get("regexp")) |regexp| {
        if (regexp != .object) return error.InvalidArgument;
        return .{ .regexp = .{
            .field = (regexp.object.get("field") orelse return error.InvalidArgument).string,
            .pattern = (regexp.object.get("pattern") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(regexp.object),
        } };
    }
    if (value.object.get("term_range")) |term_range| {
        if (term_range != .object) return error.InvalidArgument;
        return .{ .term_range = .{
            .field = (term_range.object.get("field") orelse return error.InvalidArgument).string,
            .min = if (term_range.object.get("min")) |min| switch (min) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .max = if (term_range.object.get("max")) |max| switch (max) {
                .string => |v| v,
                .null => null,
                else => return error.InvalidArgument,
            } else null,
            .inclusive_min = if (term_range.object.get("inclusive_min")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else true,
            .inclusive_max = if (term_range.object.get("inclusive_max")) |inclusive| switch (inclusive) {
                .bool => |v| v,
                else => return error.InvalidArgument,
            } else false,
            .boost = try parseOptionalBoostJson(term_range.object),
        } };
    }
    if (value.object.get("ip_range")) |ip_range| {
        if (ip_range != .object) return error.InvalidArgument;
        return .{ .ip_range = .{
            .field = (ip_range.object.get("field") orelse return error.InvalidArgument).string,
            .cidr = (ip_range.object.get("cidr") orelse return error.InvalidArgument).string,
            .boost = try parseOptionalBoostJson(ip_range.object),
        } };
    }
    if (value.object.get("geo_shape")) |geo_shape| {
        if (geo_shape != .object) return error.InvalidArgument;
        return .{ .geo_shape = .{
            .field = (geo_shape.object.get("field") orelse return error.InvalidArgument).string,
            .relation = if (geo_shape.object.get("relation")) |relation|
                try parseGeoShapeRelation(relation)
            else
                .intersects,
            .polygons = try parseGeoShapePolygonsJson(alloc, geo_shape),
            .boost = try parseOptionalBoostJson(geo_shape.object),
        } };
    }
    if (value.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;

        const must = if (bool_query.object.get("must")) |must_value|
            try parseTextQueryArrayJson(alloc, must_value)
        else
            &.{};
        const should = if (bool_query.object.get("should")) |should_value|
            try parseTextQueryArrayJson(alloc, should_value)
        else
            &.{};
        const must_not = if (bool_query.object.get("must_not")) |must_not_value|
            try parseTextQueryArrayJson(alloc, must_not_value)
        else
            &.{};
        const min_should = if (bool_query.object.get("min_should")) |min_should_value|
            try parseMinShouldJson(min_should_value)
        else
            0;

        if (must.len == 0 and should.len == 0 and must_not.len == 0) return error.InvalidArgument;
        return .{ .bool_query = .{
            .must = must,
            .should = should,
            .must_not = must_not,
            .min_should = min_should,
            .boost = try parseOptionalBoostJson(bool_query.object),
        } };
    }
    if (value.object.get("conjuncts")) |conjuncts| {
        return .{ .bool_query = .{ .must = try parseTextQueryArrayJson(alloc, conjuncts) } };
    }
    if (value.object.get("disjuncts")) |disjuncts| {
        const min_should = if (value.object.get("min_should")) |min_should_value|
            try parseMinShouldJson(min_should_value)
        else
            0;
        return .{ .bool_query = .{
            .should = try parseTextQueryArrayJson(alloc, disjuncts),
            .min_should = min_should,
        } };
    }
    return error.InvalidArgument;
}

fn parseMinShouldJson(value: std.json.Value) anyerror!u32 {
    return switch (value) {
        .integer => |v| if (v < 0) error.InvalidArgument else @intCast(v),
        .float => |v| blk: {
            if (v < 0 or @floor(v) != v or v > std.math.maxInt(u32)) return error.InvalidArgument;
            break :blk @intFromFloat(v);
        },
        else => error.InvalidArgument,
    };
}

fn parseOptionalBoostJson(object: std.json.ObjectMap) anyerror!f32 {
    if (object.get("boost")) |value| {
        return switch (value) {
            .float => |v| @floatCast(v),
            .integer => |v| @floatFromInt(v),
            else => return error.InvalidArgument,
        };
    }
    return 1.0;
}

fn parseStringArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var items = try alloc.alloc([]const u8, value.array.items.len);
    errdefer alloc.free(items);
    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidArgument;
        items[i] = item.string;
    }
    return items;
}

fn parseStringMatrixJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const []const u8 {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var rows = try alloc.alloc([]const []const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(row);
        alloc.free(rows);
    }
    for (value.array.items, 0..) |item, i| {
        rows[i] = try parseStringArrayJson(alloc, item);
        initialized += 1;
    }
    return rows;
}

fn parseGeoPointJson(value: std.json.Value) anyerror!db_mod.types.GeoPoint {
    if (value != .object) return error.InvalidArgument;
    return .{
        .lon = switch (value.object.get("lon") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
        .lat = switch (value.object.get("lat") orelse return error.InvalidArgument) {
            .integer => |v| @floatFromInt(v),
            .float => |v| v,
            else => return error.InvalidArgument,
        },
    };
}

fn parseGeoPointArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const db_mod.types.GeoPoint {
    if (value != .array or value.array.items.len < 3) return error.InvalidArgument;
    var points = try alloc.alloc(db_mod.types.GeoPoint, value.array.items.len);
    errdefer alloc.free(points);
    for (value.array.items, 0..) |item, i| {
        points[i] = try parseGeoPointJson(item);
    }
    if (!std.meta.eql(points[0], points[points.len - 1])) {
        var closed = try alloc.alloc(db_mod.types.GeoPoint, points.len + 1);
        @memcpy(closed[0..points.len], points);
        closed[points.len] = points[0];
        alloc.free(points);
        return closed;
    }
    return points;
}

fn parseGeoShapePolygonsJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]const []const db_mod.types.GeoPoint {
    if (value.object.get("polygons")) |polygons_value| {
        if (polygons_value != .array or polygons_value.array.items.len == 0) return error.InvalidArgument;
        var polygons = try alloc.alloc([]const db_mod.types.GeoPoint, polygons_value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (polygons[0..initialized]) |polygon| alloc.free(polygon);
            alloc.free(polygons);
        }
        for (polygons_value.array.items, 0..) |item, i| {
            polygons[i] = try parseGeoPointArrayJson(alloc, item);
            initialized += 1;
        }
        return polygons;
    }
    if (value.object.get("polygon")) |polygon_value| {
        var polygons = try alloc.alloc([]const db_mod.types.GeoPoint, 1);
        errdefer alloc.free(polygons);
        polygons[0] = try parseGeoPointArrayJson(alloc, polygon_value);
        return polygons;
    }
    return error.InvalidArgument;
}

fn parseGeoShapeRelation(value: std.json.Value) anyerror!db_mod.types.GeoShapeRelation {
    if (value != .string) return error.InvalidArgument;
    if (std.mem.eql(u8, value.string, "intersects")) return .intersects;
    if (std.mem.eql(u8, value.string, "within")) return .within;
    if (std.mem.eql(u8, value.string, "contains")) return .contains;
    return error.InvalidArgument;
}

fn parseTextQueryArrayJson(alloc: std.mem.Allocator, value: std.json.Value) anyerror![]db_mod.types.TextQuery {
    if (value != .array or value.array.items.len == 0) return error.InvalidArgument;
    var clauses = try alloc.alloc(db_mod.types.TextQuery, value.array.items.len);
    for (value.array.items, 0..) |item, i| {
        clauses[i] = try parseTextQueryJson(alloc, item);
    }
    return clauses;
}

pub export fn antfly_db_execute_graph_queries_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        graph_queries: []const JsonGraphQueryRequest,
        named_sets: []const JsonNamedGraphInputSetRequest,
        limit: u32 = 10,
        offset: u32 = 0,
        include_stored: bool = true,
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    const graph_queries = parseNamedGraphQueries(handle.alloc, parsed.value.graph_queries) catch return .invalid_argument;
    defer freeOwnedNamedGraphQueries(handle.alloc, graph_queries);

    const named_sets = parseNamedGraphInputSets(handle.alloc, parsed.value.named_sets) catch return .invalid_argument;
    defer freeOwnedNamedGraphInputSets(handle.alloc, named_sets);

    const req: db_mod.types.SearchRequest = .{
        .limit = parsed.value.limit,
        .offset = parsed.value.offset,
        .include_stored = parsed.value.include_stored,
    };

    const results = handle.db.executeNamedGraphQueries(handle.alloc, req, graph_queries, named_sets) catch |err| return capi.mapError(err);
    defer {
        for (results) |*result| result.deinit(handle.alloc);
        if (results.len > 0) handle.alloc.free(results);
    }

    var payload = handle.alloc.alloc(JsonGraphSearchResult, results.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |result, i| {
        payload[i] = JsonGraphSearchResult.init(handle.alloc, result) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_aggregate_hits_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    var parsed = std.json.parseFromSlice(JsonAggregateHitsRequest, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    const requests = toAggregationRequest(handle.alloc, parsed.value.aggregations) catch return .internal;
    defer freeAggregationRequests(handle.alloc, requests);

    var hits = handle.alloc.alloc(db_mod.types.SearchHit, parsed.value.hit_ids_b64.len) catch return .internal;
    var hit_count: usize = 0;
    defer {
        for (hits[0..hit_count]) |*hit| hit.deinit(handle.alloc);
        if (hits.len > 0) handle.alloc.free(hits);
    }
    for (parsed.value.hit_ids_b64) |item| {
        const hit_id = decodeBase64Alloc(handle.alloc, item) catch return .invalid_argument;
        errdefer handle.alloc.free(hit_id);
        const stored = handle.db.get(handle.alloc, hit_id) catch |err| {
            handle.alloc.free(hit_id);
            return capi.mapError(err);
        } orelse {
            handle.alloc.free(hit_id);
            continue;
        };
        hits[hit_count] = .{
            .id = hit_id,
            .stored_data = stored,
        };
        hit_count += 1;
    }

    const result = db_mod.types.SearchResult{
        .alloc = handle.alloc,
        .hits = hits[0..hit_count],
        .total_hits = @intCast(hit_count),
    };
    const backend_results = aggregations_mod.computeSearchAggregations(handle.alloc, requests, result, .{
        .index_manager = handle.db.core.index_manager,
        .full_text_index_name = if (parsed.value.index_name.len > 0) parsed.value.index_name else null,
    }) catch |err| return capi.mapError(err);
    defer aggregations_mod.deinitResults(handle.alloc, backend_results);

    const aggregation_results = toJsonAggregationResults(handle.alloc, backend_results) catch return .internal;
    defer {
        for (aggregation_results) |*item| item.deinit(handle.alloc);
        if (aggregation_results.len > 0) handle.alloc.free(aggregation_results);
    }

    out_buf.* = stringifyJson(aggregation_results) catch return .internal;
    return .ok;
}

fn parseNamedGraphQueries(alloc: Allocator, requests: []const JsonGraphQueryRequest) ![]db_mod.types.NamedGraphQuery {
    var queries = try alloc.alloc(db_mod.types.NamedGraphQuery, requests.len);
    errdefer alloc.free(queries);
    var count: usize = 0;
    errdefer {
        for (queries[0..count]) |*query| deinitOwnedNamedGraphQuery(alloc, query);
    }
    for (requests, 0..) |request, i| {
        queries[i] = .{
            .name = try alloc.dupe(u8, request.name),
            .query = try parseGraphQueryRequestOwned(alloc, request),
        };
        count += 1;
    }
    return queries;
}

fn parseNamedGraphInputSets(alloc: Allocator, requests: []const JsonNamedGraphInputSetRequest) ![]db_mod.types.NamedGraphInputSet {
    var sets = try alloc.alloc(db_mod.types.NamedGraphInputSet, requests.len);
    errdefer alloc.free(sets);
    var count: usize = 0;
    errdefer {
        for (sets[0..count]) |*set| deinitOwnedNamedGraphInputSet(alloc, set);
    }
    for (requests, 0..) |request, i| {
        sets[i] = .{
            .name = try alloc.dupe(u8, request.name),
            .hit_ids = try decodeGraphHitIds(alloc, request.hit_ids_b64),
            .total_hits = request.total_hits,
        };
        count += 1;
    }
    return sets;
}

fn parseGraphQueryRequestOwned(alloc: Allocator, request: JsonGraphQueryRequest) !graph_query_mod.GraphQuery {
    return .{
        .query_type = if (std.mem.eql(u8, request.type, "neighbors"))
            .neighbors
        else if (std.mem.eql(u8, request.type, "traverse"))
            .traverse
        else if (std.mem.eql(u8, request.type, "shortest_path"))
            .shortest_path
        else if (std.mem.eql(u8, request.type, "k_shortest_paths"))
            .k_shortest_paths
        else
            return error.InvalidArgument,
        .index_name = try alloc.dupe(u8, request.index_name),
        .start_nodes = try parseGraphNodeSelectorRequestOwned(alloc, request.start_nodes),
        .target_nodes = if (request.target_nodes) |target_nodes| try parseGraphNodeSelectorRequestOwned(alloc, target_nodes) else null,
        .params = .{
            .edge_types = try cloneGraphEdgeTypes(alloc, request.edge_types),
            .direction = parseGraphDirection(request.direction),
            .max_depth = request.max_depth,
            .max_results = request.max_results,
            .min_weight = request.min_weight,
            .max_weight = request.max_weight,
            .deduplicate = request.deduplicate,
            .include_paths = request.include_paths,
            .weight_mode = parseGraphWeightMode(request.weight_mode),
        },
        .k = request.k,
    };
}

fn parseGraphNodeSelectorRequestOwned(alloc: Allocator, selector: JsonGraphNodeSelectorRequest) !graph_query_mod.NodeSelector {
    if (selector.keys.len > 0) return .{ .keys = try decodeGraphKeys(alloc, selector.keys) };
    if (selector.result_ref.len > 0) {
        return .{ .result_ref = .{
            .ref = try alloc.dupe(u8, selector.result_ref),
            .limit = selector.limit,
        } };
    }
    return error.InvalidArgument;
}

fn decodeGraphKeys(alloc: Allocator, keys: []const []const u8) ![]const []const u8 {
    var owned = try alloc.alloc([]const u8, keys.len);
    errdefer alloc.free(owned);
    var count: usize = 0;
    errdefer {
        for (owned[0..count]) |key| alloc.free(@constCast(key));
    }
    for (keys, 0..) |key, i| {
        owned[i] = try decodeBase64Alloc(alloc, key);
        count += 1;
    }
    return owned;
}

fn cloneGraphEdgeTypes(alloc: Allocator, edge_types: []const []const u8) ![]const []const u8 {
    var owned = try alloc.alloc([]const u8, edge_types.len);
    errdefer alloc.free(owned);
    var count: usize = 0;
    errdefer {
        for (owned[0..count]) |item| alloc.free(@constCast(item));
    }
    for (edge_types, 0..) |edge_type, i| {
        owned[i] = try alloc.dupe(u8, edge_type);
        count += 1;
    }
    return owned;
}

fn decodeGraphHitIds(alloc: Allocator, hit_ids_b64: []const []const u8) ![]const []const u8 {
    var hit_ids = try alloc.alloc([]const u8, hit_ids_b64.len);
    errdefer alloc.free(hit_ids);
    var count: usize = 0;
    errdefer {
        for (hit_ids[0..count]) |hit_id| alloc.free(@constCast(hit_id));
    }
    for (hit_ids_b64, 0..) |item, i| {
        hit_ids[i] = try decodeBase64Alloc(alloc, item);
        count += 1;
    }
    return hit_ids;
}

fn deinitOwnedNodeSelector(alloc: Allocator, selector: *graph_query_mod.NodeSelector) void {
    switch (selector.*) {
        .keys => |keys| {
            for (keys) |key| alloc.free(@constCast(key));
            if (keys.len > 0) alloc.free(keys);
        },
        .result_ref => |result_ref| {
            alloc.free(@constCast(result_ref.ref));
        },
    }
    selector.* = undefined;
}

fn deinitOwnedGraphQuery(alloc: Allocator, query: *graph_query_mod.GraphQuery) void {
    alloc.free(@constCast(query.index_name));
    deinitOwnedNodeSelector(alloc, &query.start_nodes);
    if (query.target_nodes) |*target_nodes| deinitOwnedNodeSelector(alloc, target_nodes);
    for (query.params.edge_types) |edge_type| alloc.free(@constCast(edge_type));
    if (query.params.edge_types.len > 0) alloc.free(query.params.edge_types);
    query.* = undefined;
}

fn deinitOwnedNamedGraphQuery(alloc: Allocator, query: *db_mod.types.NamedGraphQuery) void {
    alloc.free(query.name);
    deinitOwnedGraphQuery(alloc, &query.query);
    query.* = undefined;
}

fn freeOwnedNamedGraphQueries(alloc: Allocator, queries: []db_mod.types.NamedGraphQuery) void {
    for (queries) |*query| deinitOwnedNamedGraphQuery(alloc, query);
    if (queries.len > 0) alloc.free(queries);
}

fn deinitOwnedNamedGraphInputSet(alloc: Allocator, set: *db_mod.types.NamedGraphInputSet) void {
    alloc.free(@constCast(set.name));
    for (set.hit_ids) |hit_id| alloc.free(@constCast(hit_id));
    if (set.hit_ids.len > 0) alloc.free(@constCast(set.hit_ids));
    set.* = undefined;
}

fn freeOwnedNamedGraphInputSets(alloc: Allocator, sets: []db_mod.types.NamedGraphInputSet) void {
    for (sets) |*set| deinitOwnedNamedGraphInputSet(alloc, set);
    if (sets.len > 0) alloc.free(sets);
}

fn parseGraphDirection(direction: []const u8) db_mod.types.GraphEdgeDirection {
    if (std.mem.eql(u8, direction, "in")) return .in;
    if (std.mem.eql(u8, direction, "both")) return .both;
    return .out;
}

fn parseGraphWeightMode(mode: []const u8) db_mod.types.GraphPathWeightMode {
    if (std.mem.eql(u8, mode, "min_weight")) return .min_weight;
    if (std.mem.eql(u8, mode, "max_weight")) return .max_weight;
    return .min_hops;
}

fn computeSearchAggregations(
    alloc: Allocator,
    requests: []const JsonSearchAggregationRequest,
    result: db_mod.types.SearchResult,
) anyerror![]JsonSearchAggregationResult {
    var out = try alloc.alloc(JsonSearchAggregationResult, requests.len);
    errdefer alloc.free(out);

    for (requests, 0..) |request, i| {
        out[i] = try computeSingleAggregation(alloc, request, result.hits);
    }
    return out;
}

fn computeSingleAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (std.mem.eql(u8, request.type, "count")) {
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try std.fmt.allocPrint(alloc, "{d}", .{hits.len}),
        };
    }
    if (std.mem.eql(u8, request.type, "sum")) return try computeNumericMetricAggregation(alloc, request, hits, .sum);
    if (std.mem.eql(u8, request.type, "min")) return try computeNumericMetricAggregation(alloc, request, hits, .min);
    if (std.mem.eql(u8, request.type, "max")) return try computeNumericMetricAggregation(alloc, request, hits, .max);
    if (std.mem.eql(u8, request.type, "avg")) return try computeNumericMetricAggregation(alloc, request, hits, .avg);
    if (std.mem.eql(u8, request.type, "stats")) return try computeNumericMetricAggregation(alloc, request, hits, .stats);
    if (std.mem.eql(u8, request.type, "cardinality")) return try computeCardinalityAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "terms")) return try computeTermsAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "histogram")) return try computeHistogramAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "date_histogram")) return try computeDateHistogramAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "range")) return try computeRangeAggregation(alloc, request, hits);
    return error.UnsupportedAggregation;
}

const NumericMetricKind = enum { sum, min, max, avg, stats };

fn computeNumericMetricAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    kind: NumericMetricKind,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var sum: f64 = 0;
    var sum_squares: f64 = 0;
    var count: i64 = 0;
    var min_value: f64 = std.math.inf(f64);
    var max_value: f64 = -std.math.inf(f64);

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        accumulateNumericJsonValue(value, &sum, &sum_squares, &count, &min_value, &max_value);
    }

    const value_json = switch (kind) {
        .sum => try std.fmt.allocPrint(alloc, "{d}", .{sum}),
        .min => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{min_value}),
        .max => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{max_value}),
        .avg => if (count == 0)
            try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0}")
        else
            try std.fmt.allocPrint(alloc, "{{\"count\":{d},\"sum\":{d},\"avg\":{d}}}", .{ count, sum, sum / @as(f64, @floatFromInt(count)) }),
        .stats => blk: {
            if (count == 0) break :blk try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0,\"min\":null,\"max\":null,\"sum_squares\":0,\"variance\":0,\"std_dev\":0}");
            const avg = sum / @as(f64, @floatFromInt(count));
            const variance = (sum_squares / @as(f64, @floatFromInt(count))) - (avg * avg);
            const non_negative_variance = if (variance < 0) 0 else variance;
            break :blk try std.fmt.allocPrint(
                alloc,
                "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d},\"sum_squares\":{d},\"variance\":{d},\"std_dev\":{d}}}",
                .{ count, sum, avg, min_value, max_value, sum_squares, non_negative_variance, @sqrt(non_negative_variance) },
            );
        },
    };
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = value_json,
    };
}

fn computeCardinalityAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var seen = std.StringHashMap(void).init(alloc);
    defer {
        var it = seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try collectCardinalityValues(alloc, &seen, value);
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{seen.count()}),
    };
}

fn computeTermsAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var counts = std.StringHashMap(i64).init(alloc);
    defer {
        var it = counts.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        counts.deinit();
    }
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    if (request.term_pattern.len > 0) return error.UnsupportedAggregation;

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try appendTermAggregationValuesZig(alloc, &counts, &grouped, hit, value);
    }

    var entries = std.ArrayList(struct { key: []const u8, count: i64 }).empty;
    defer entries.deinit(alloc);
    var it = counts.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key, request.term_prefix)) continue;
        if (request.min_doc_count > 0 and count < request.min_doc_count) continue;
        try entries.append(alloc, .{ .key = key, .count = count });
    }
    std.mem.sort(@TypeOf(entries.items[0]), entries.items, {}, struct {
        fn lessThan(_: void, lhs: @TypeOf(entries.items[0]), rhs: @TypeOf(entries.items[0])) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < entries.items.len) @intCast(request.size) else entries.items.len;
    var buckets = try alloc.alloc(JsonSearchAggregationBucket, limit);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (entries.items[0..limit], 0..) |entry, idx| {
        const grouped_hits = grouped.get(entry.key).?.items;
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = grouped_hits,
                .total_hits = @intCast(grouped_hits.len),
            });
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{entry.key}),
            .count = entry.count,
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeHistogramAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0 or request.interval <= 0) return error.InvalidAggregation;

    var bucket_counts = std.AutoHashMap(i64, i64).init(alloc);
    defer bucket_counts.deinit();
    var grouped = std.AutoHashMap(i64, std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        if (jsonValueToF64(value)) |numeric| {
            const bucket_index = @as(i64, @intFromFloat(@floor(numeric / request.interval)));
            const entry = try bucket_counts.getOrPut(bucket_index);
            if (entry.found_existing) entry.value_ptr.* += 1 else entry.value_ptr.* = 1;
            const grouped_entry = try grouped.getOrPut(bucket_index);
            if (!grouped_entry.found_existing) grouped_entry.value_ptr.* = .empty;
            try grouped_entry.value_ptr.append(alloc, hit);
        }
    }

    var present_keys = try alloc.alloc(i64, bucket_counts.count());
    defer if (present_keys.len > 0) alloc.free(present_keys);
    var iter = bucket_counts.iterator();
    var present_count: usize = 0;
    while (iter.next()) |entry| {
        if (request.min_doc_count > 0 and entry.value_ptr.* < request.min_doc_count) continue;
        present_keys[present_count] = entry.key_ptr.*;
        present_count += 1;
    }
    std.mem.sort(i64, present_keys[0..present_count], {}, struct {
        fn lessThan(_: void, lhs: i64, rhs: i64) bool {
            return lhs < rhs;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and present_count > 0)
        try fillHistogramBucketKeys(alloc, present_keys[0], present_keys[present_count - 1])
    else
        try alloc.dupe(i64, present_keys[0..present_count]);
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            if (grouped.get(bucket_index)) |list| {
                break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                });
            }
            break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
        };
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = bucket_counts.get(bucket_index) orelse 0,
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeDateHistogramAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const interval = try parseDateInterval(request);
    var agg = search_agg_mod.DateHistogramAgg.init(alloc, interval);
    defer agg.deinit();
    var grouped = std.AutoHashMap(u64, std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (value) |ns| {
            try agg.collect(ns);
            const bucket_key = search_agg_mod.truncateToInterval(ns, interval);
            const entry = try grouped.getOrPut(bucket_key);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(alloc, hit);
        }
    }

    const present_keys = try agg.sortedKeys(alloc);
    defer if (present_keys.len > 0) alloc.free(present_keys);

    var kept: usize = 0;
    for (present_keys) |key| {
        const count = agg.getCount(key);
        if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
        kept += 1;
    }

    const keys = if (request.min_doc_count == 0 and kept > 0)
        try fillDateHistogramBucketKeys(alloc, present_keys[0], present_keys[present_keys.len - 1], interval)
    else blk: {
        var filtered = try alloc.alloc(u64, kept);
        var idx: usize = 0;
        for (present_keys) |key| {
            const count = agg.getCount(key);
            if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
            filtered[idx] = key;
            idx += 1;
        }
        break :blk filtered;
    };
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, key);
        defer alloc.free(formatted);
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            if (grouped.get(key)) |list| {
                break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                });
            }
            break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{formatted}),
            .count = @intCast(agg.getCount(key)),
            .aggregations = nested,
        };
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeRangeAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const has_numeric = request.ranges.len > 0;
    const has_date = request.date_ranges.len > 0;
    const has_distance = request.distance_ranges.len > 0;
    if ((@intFromBool(has_numeric) + @intFromBool(has_date) + @intFromBool(has_distance)) != 1) return error.InvalidAggregation;

    if (has_numeric) {
        var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            defer matched.deinit(alloc);
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractNumericFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |numeric| {
                    if (matchesNumericRangeValue(numeric, range_spec)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }) else try alloc.alloc(JsonSearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    if (has_date) {
        var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.date_ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.date_ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            defer matched.deinit(alloc);
            const start_ns = if (range_spec.start) |start| try parseRfc3339ToNs(start) else null;
            const end_ns = if (range_spec.end) |end| try parseRfc3339ToNs(end) else null;
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |timestamp| {
                    if (matchesDateRangeValue(timestamp, start_ns, end_ns)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }) else try alloc.alloc(JsonSearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    var bands = try alloc.alloc(search_agg_mod.GeoDistanceRange, request.distance_ranges.len);
    defer alloc.free(bands);
    for (request.distance_ranges, 0..) |range_spec, idx| {
        bands[idx] = .{
            .from = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null,
            .to = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null,
        };
    }

    var agg = try search_agg_mod.GeoDistanceAgg.init(alloc, .{
        .lat = request.center_lat,
        .lon = request.center_lon,
    }, bands);
    defer agg.deinit();

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (point) |geo_point| {
            agg.collect(geo_point);
        }
    }

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.distance_ranges.len);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.distance_ranges, 0..) |range_spec, idx| {
        var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
        defer matched.deinit(alloc);
        const from_meters = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null;
        const to_meters = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null;
        for (hits) |hit| {
            const stored = hit.stored_data orelse continue;
            const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
            if (point) |geo_point| {
                const dist = geo_mod.haversineDistance(.{ .lat = request.center_lat, .lon = request.center_lon }, geo_point);
                if (matchesGeoDistanceValue(dist, from_meters, to_meters)) try matched.append(alloc, hit);
            }
        }
        const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
            .alloc = alloc,
            .hits = matched.items,
            .total_hits = @intCast(matched.items.len),
        }) else try alloc.alloc(JsonSearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = @intCast(agg.bands[idx].count),
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn matchesNumericRangeValue(value: f64, range_spec: JsonNumericRangeRequest) bool {
    if (range_spec.start) |start| {
        if (value < start) return false;
    }
    if (range_spec.end) |end| {
        if (value >= end) return false;
    }
    return true;
}

fn matchesDateRangeValue(value: u64, start_ns: ?u64, end_ns: ?u64) bool {
    if (start_ns) |start| {
        if (value < start) return false;
    }
    if (end_ns) |end| {
        if (value >= end) return false;
    }
    return true;
}

fn matchesGeoDistanceValue(value_meters: f64, from_meters: ?f64, to_meters: ?f64) bool {
    if (from_meters) |from| {
        if (value_meters < from) return false;
    }
    if (to_meters) |to| {
        if (value_meters >= to) return false;
    }
    return true;
}

fn accumulateNumericJsonValue(
    value: std.json.Value,
    sum: *f64,
    sum_squares: *f64,
    count: *i64,
    min_value: *f64,
    max_value: *f64,
) void {
    switch (value) {
        .array => |arr| for (arr.items) |item| {
            accumulateNumericJsonValue(item, sum, sum_squares, count, min_value, max_value);
        },
        else => if (jsonValueToF64(value)) |numeric| {
            sum.* += numeric;
            sum_squares.* += numeric * numeric;
            count.* += 1;
            if (numeric < min_value.*) min_value.* = numeric;
            if (numeric > max_value.*) max_value.* = numeric;
        },
    }
}

fn collectCardinalityValues(alloc: Allocator, seen: *std.StringHashMap(void), value: std.json.Value) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try collectCardinalityValues(alloc, seen, item);
        },
        else => {
            const key = try stringifyJsonValueCompact(alloc, value);
            errdefer alloc.free(key);
            const entry = try seen.getOrPut(key);
            if (entry.found_existing) {
                alloc.free(key);
            } else {
                entry.key_ptr.* = key;
                entry.value_ptr.* = {};
            }
        },
    }
}

fn appendTermAggregationValuesZig(
    alloc: Allocator,
    counts: *std.StringHashMap(i64),
    grouped: *std.StringHashMap(std.ArrayListUnmanaged(db_mod.types.SearchHit)),
    hit: db_mod.types.SearchHit,
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try appendTermAggregationValuesZig(alloc, counts, grouped, hit, item);
        },
        else => {
            const key = try jsonValueToTermKey(alloc, value);
            defer alloc.free(key);

            const count_entry = try counts.getOrPut(key);
            if (count_entry.found_existing) {
                count_entry.value_ptr.* += 1;
            } else {
                count_entry.key_ptr.* = try alloc.dupe(u8, key);
                count_entry.value_ptr.* = 1;
            }

            const group_entry = try grouped.getOrPut(count_entry.key_ptr.*);
            if (!group_entry.found_existing) group_entry.value_ptr.* = .empty;
            try group_entry.value_ptr.append(alloc, hit);
        },
    }
}

fn jsonValueToTermKey(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => try alloc.dupe(u8, value.string),
        .bool => if (value.bool) try alloc.dupe(u8, "true") else try alloc.dupe(u8, "false"),
        .integer => try std.fmt.allocPrint(alloc, "{d}", .{value.integer}),
        .float => try std.fmt.allocPrint(alloc, "{d}", .{value.float}),
        .number_string => try alloc.dupe(u8, value.number_string),
        .null => try alloc.dupe(u8, "null"),
        else => try stringifyJsonValueCompact(alloc, value),
    };
}

fn stringifyJsonValueCompact(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn distanceToMeters(value: f64, unit: []const u8) !f64 {
    if (unit.len == 0 or std.mem.eql(u8, unit, "m") or std.mem.eql(u8, unit, "meter") or std.mem.eql(u8, unit, "meters")) {
        return value;
    }
    if (std.mem.eql(u8, unit, "km") or std.mem.eql(u8, unit, "kilometer") or std.mem.eql(u8, unit, "kilometers")) {
        return value * 1000.0;
    }
    if (std.mem.eql(u8, unit, "mi") or std.mem.eql(u8, unit, "mile") or std.mem.eql(u8, unit, "miles")) {
        return value * 1609.344;
    }
    if (std.mem.eql(u8, unit, "ft") or std.mem.eql(u8, unit, "foot") or std.mem.eql(u8, unit, "feet")) {
        return value * 0.3048;
    }
    return error.UnsupportedAggregation;
}

fn extractGeoPointFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?geo_mod.GeoPoint {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .object => |obj| blk: {
            const lat_value = obj.get("lat") orelse break :blk null;
            const lon_value = obj.get("lon") orelse break :blk null;
            const lat = jsonValueToF64(lat_value) orelse break :blk null;
            const lon = jsonValueToF64(lon_value) orelse break :blk null;
            break :blk .{ .lat = lat, .lon = lon };
        },
        else => null,
    };
}

fn jsonValueToF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

fn fillHistogramBucketKeys(alloc: Allocator, first_key: i64, last_key: i64) ![]i64 {
    if (last_key < first_key) return &.{};
    const len: usize = @intCast(last_key - first_key + 1);
    const keys = try alloc.alloc(i64, len);
    for (keys, 0..) |*slot, idx| {
        slot.* = first_key + @as(i64, @intCast(idx));
    }
    return keys;
}

fn fillDateHistogramBucketKeys(
    alloc: Allocator,
    first_key: u64,
    last_key: u64,
    interval: search_agg_mod.DateInterval,
) ![]u64 {
    var keys: std.ArrayList(u64) = .empty;
    errdefer keys.deinit(alloc);

    var current = first_key;
    while (current <= last_key) {
        try keys.append(alloc, current);
        const next = try nextDateHistogramBucketKey(current, interval);
        if (next <= current) break;
        current = next;
    }
    return keys.toOwnedSlice(alloc);
}

fn nextDateHistogramBucketKey(current: u64, interval: search_agg_mod.DateInterval) !u64 {
    return switch (interval) {
        .minute => current + 60 * std.time.ns_per_s,
        .hour => current + std.time.ns_per_hour,
        .day => current + std.time.ns_per_day,
        .week => current + 7 * std.time.ns_per_day,
        .month => try addCalendarMonths(current, 1),
        .year => try addCalendarYears(current, 1),
    };
}

fn addCalendarMonths(current: u64, delta_months: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    const month_index = (civil.year * 12 + (civil.month - 1)) + delta_months;
    var year = @divFloor(month_index, 12);
    var month = @mod(month_index, 12) + 1;
    if (month <= 0) {
        month += 12;
        year -= 1;
    }
    return civilDateToBucketNs(year, month, 1);
}

fn addCalendarYears(current: u64, delta_years: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    return civilDateToBucketNs(civil.year + delta_years, 1, 1);
}

fn civilDateToBucketNs(year: i64, month: i64, day: i64) !u64 {
    const days = daysFromCivil(year, month, day);
    if (days < 0) return error.InvalidAggregation;
    return @as(u64, @intCast(days)) * std.time.ns_per_day;
}

fn extractNumericFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

fn extractTimestampFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @intCast(value.integer),
        .float => @intFromFloat(value.float),
        .number_string => std.fmt.parseInt(u64, value.number_string, 10) catch null,
        .string => try parseRfc3339ToNs(value.string),
        else => null,
    };
}

fn parseDateInterval(request: JsonSearchAggregationRequest) !search_agg_mod.DateInterval {
    const value = if (request.calendar_interval.len > 0) request.calendar_interval else request.fixed_interval;
    if (std.mem.eql(u8, value, "minute") or std.mem.eql(u8, value, "1m")) return .minute;
    if (std.mem.eql(u8, value, "hour") or std.mem.eql(u8, value, "1h")) return .hour;
    if (std.mem.eql(u8, value, "day") or std.mem.eql(u8, value, "1d")) return .day;
    if (std.mem.eql(u8, value, "week") or std.mem.eql(u8, value, "1w")) return .week;
    if (std.mem.eql(u8, value, "month")) return .month;
    if (std.mem.eql(u8, value, "year")) return .year;
    return error.UnsupportedAggregation;
}

fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn formatRfc3339Bucket(alloc: Allocator, ns: u64) ![]const u8 {
    const total_seconds: u64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const secs_of_day: u64 = total_seconds % 86_400;
    const civil = civilFromDays(days);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    return try std.fmt.allocPrint(alloc, "{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}Z", .{
        @as(u64, @intCast(civil.year)),
        @as(u64, @intCast(civil.month)),
        @as(u64, @intCast(civil.day)),
        hour,
        minute,
        second,
    });
}

fn civilFromDays(days_since_epoch: i64) struct { year: i64, month: i64, day: i64 } {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    y += if (m <= 2) @as(i64, 1) else @as(i64, 0);
    return .{ .year = y, .month = m, .day = d };
}

fn extractValueAtPath(root: std.json.Value, field_path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, field_path, '.');
    while (parts.next()) |part| {
        switch (current) {
            .object => |obj| {
                current = obj.get(part) orelse return null;
            },
            else => return null,
        }
    }
    return current;
}

pub export fn antfly_db_add_index_json(
    handle_ptr: ?*anyopaque,
    config_json: capi.Slice,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        name: []const u8,
        kind: []const u8,
        config_json: []const u8,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, config_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const kind: db_mod.types.IndexKind = if (std.mem.eql(u8, parsed.value.kind, "full_text"))
        .full_text
    else if (std.mem.eql(u8, parsed.value.kind, "graph"))
        .graph
    else if (std.mem.eql(u8, parsed.value.kind, "dense_vector"))
        .dense_vector
    else if (std.mem.eql(u8, parsed.value.kind, "sparse_vector"))
        .sparse_vector
    else if (std.mem.eql(u8, parsed.value.kind, "algebraic"))
        .algebraic
    else
        return .invalid_argument;
    handle.db.addIndex(.{
        .name = parsed.value.name,
        .kind = kind,
        .config_json = parsed.value.config_json,
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_delete_index(
    handle_ptr: ?*anyopaque,
    name: capi.Slice,
    out_deleted: *bool,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    out_deleted.* = handle.db.deleteIndex(name.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_edges_json(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    key: capi.Slice,
    edge_type: capi.Slice,
    direction: u8,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const dir: db_mod.types.GraphEdgeDirection = switch (direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const edges = handle.db.getEdges(handle.alloc, index_name.bytes(), key.bytes(), edge_type.bytes(), dir) catch |err| return capi.mapError(err);
    defer graphFreeEdges(handle.alloc, edges);
    var payload = handle.alloc.alloc(JsonEdge, edges.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (edges, 0..) |edge, i| {
        payload[i] = JsonEdge.init(handle.alloc, edge) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_traverse_edges_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        index_name: []const u8,
        start_key_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        direction: u8 = 0,
        max_depth: u32 = 3,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        max_results: u32 = 100,
        deduplicate_nodes: bool = true,
        include_paths: bool = false,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const start_key = decodeBase64Alloc(handle.alloc, parsed.value.start_key_b64) catch return .invalid_argument;
    defer handle.alloc.free(start_key);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const results = handle.db.traverseEdges(handle.alloc, parsed.value.index_name, start_key, .{
        .edge_types = parsed.value.edge_types,
        .direction = direction,
        .max_depth = parsed.value.max_depth,
        .min_weight = parsed.value.min_weight,
        .max_weight = parsed.value.max_weight,
        .max_results = parsed.value.max_results,
        .deduplicate = parsed.value.deduplicate_nodes,
        .include_paths = parsed.value.include_paths,
    }) catch |err| return capi.mapError(err);
    defer traversalFreeResults(handle.alloc, results);
    var payload = handle.alloc.alloc(JsonTraversalResult, results.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |item, i| {
        payload[i] = JsonTraversalResult.init(handle.alloc, item) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_get_neighbors_json(
    handle_ptr: ?*anyopaque,
    index_name: capi.Slice,
    key: capi.Slice,
    edge_type: capi.Slice,
    direction: u8,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const dir: db_mod.types.GraphEdgeDirection = switch (direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const results = handle.db.getNeighbors(handle.alloc, index_name.bytes(), key.bytes(), edge_type.bytes(), dir) catch |err| return capi.mapError(err);
    defer traversalFreeResults(handle.alloc, results);
    var payload = handle.alloc.alloc(JsonTraversalResult, results.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (results, 0..) |item, i| {
        payload[i] = JsonTraversalResult.init(handle.alloc, item) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_find_shortest_path_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        index_name: []const u8,
        source_b64: []const u8,
        target_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        direction: u8 = 0,
        weight_mode: []const u8 = "min_hops",
        max_depth: u32 = 50,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const source = decodeBase64Alloc(handle.alloc, parsed.value.source_b64) catch return .invalid_argument;
    defer handle.alloc.free(source);
    const target = decodeBase64Alloc(handle.alloc, parsed.value.target_b64) catch return .invalid_argument;
    defer handle.alloc.free(target);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const weight_mode: db_mod.types.GraphPathWeightMode = if (std.mem.eql(u8, parsed.value.weight_mode, "min_weight"))
        .min_weight
    else if (std.mem.eql(u8, parsed.value.weight_mode, "max_weight"))
        .max_weight
    else
        .min_hops;
    const maybe_path = handle.db.findShortestPath(handle.alloc, parsed.value.index_name, source, target, parsed.value.edge_types, direction, weight_mode, parsed.value.max_depth, parsed.value.min_weight, parsed.value.max_weight) catch |err| return capi.mapError(err);
    if (maybe_path == null) return .not_found;
    var payload = JsonPath.init(handle.alloc, maybe_path.?) catch return .internal;
    defer payload.deinit(handle.alloc);
    defer paths_mod.freePath(handle.alloc, maybe_path.?);
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_find_k_shortest_paths_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const Request = struct {
        index_name: []const u8,
        source_b64: []const u8,
        target_b64: []const u8,
        edge_types: []const []const u8 = &.{},
        direction: u8 = 0,
        weight_mode: []const u8 = "min_hops",
        max_depth: u32 = 50,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        k: u32 = 1,
    };
    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();
    const source = decodeBase64Alloc(handle.alloc, parsed.value.source_b64) catch return .invalid_argument;
    defer handle.alloc.free(source);
    const target = decodeBase64Alloc(handle.alloc, parsed.value.target_b64) catch return .invalid_argument;
    defer handle.alloc.free(target);
    const direction: db_mod.types.GraphEdgeDirection = switch (parsed.value.direction) {
        0 => .out,
        1 => .in,
        2 => .both,
        else => return .invalid_argument,
    };
    const weight_mode: db_mod.types.GraphPathWeightMode = if (std.mem.eql(u8, parsed.value.weight_mode, "min_weight"))
        .min_weight
    else if (std.mem.eql(u8, parsed.value.weight_mode, "max_weight"))
        .max_weight
    else
        .min_hops;
    const paths = handle.db.findKShortestPaths(handle.alloc, parsed.value.index_name, source, target, parsed.value.k, parsed.value.edge_types, direction, weight_mode, parsed.value.max_depth, parsed.value.min_weight, parsed.value.max_weight) catch |err| return capi.mapError(err);
    defer paths_mod.freePaths(handle.alloc, paths);
    var payload = handle.alloc.alloc(JsonPath, paths.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (paths, 0..) |path, i| {
        payload[i] = JsonPath.init(handle.alloc, path) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_match_pattern_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const JsonPatternNodeFilter = struct {
        filter_prefix: []const u8 = "",
        query_json: []const u8 = "",
    };
    const JsonPatternEdgeStep = struct {
        direction: u8 = 0,
        min_hops: u32 = 1,
        max_hops: u32 = 1,
        min_weight: f64 = 0.0,
        max_weight: f64 = 0.0,
        types: []const []const u8 = &.{},
    };
    const JsonPatternStep = struct {
        alias: []const u8 = "",
        edge: JsonPatternEdgeStep = .{},
        node_filter: JsonPatternNodeFilter = .{},
    };
    const Request = struct {
        index_name: []const u8,
        start_nodes_b64: []const []const u8,
        pattern: []const JsonPatternStep,
        max_results: u32 = 100,
        return_aliases: []const []const u8 = &.{},
    };

    var parsed = std.json.parseFromSlice(Request, handle.alloc, request_json.bytes(), .{}) catch return .invalid_argument;
    defer parsed.deinit();

    var start_nodes = handle.alloc.alloc([]const u8, parsed.value.start_nodes_b64.len) catch return .internal;
    defer {
        for (start_nodes) |entry| handle.alloc.free(entry);
        if (start_nodes.len > 0) handle.alloc.free(start_nodes);
    }
    var start_count: usize = 0;
    errdefer {
        for (start_nodes[0..start_count]) |entry| handle.alloc.free(entry);
    }
    for (parsed.value.start_nodes_b64, 0..) |item, i| {
        start_nodes[i] = decodeBase64Alloc(handle.alloc, item) catch return .invalid_argument;
        start_count += 1;
    }

    var pattern = handle.alloc.alloc(graph_pattern_mod.PatternStep, parsed.value.pattern.len) catch return .internal;
    defer handle.alloc.free(pattern);
    for (parsed.value.pattern, 0..) |step, i| {
        const direction: db_mod.types.GraphEdgeDirection = switch (step.edge.direction) {
            0 => .out,
            1 => .in,
            2 => .both,
            else => return .invalid_argument,
        };
        pattern[i] = .{
            .alias = step.alias,
            .edge = .{
                .direction = direction,
                .min_hops = step.edge.min_hops,
                .max_hops = step.edge.max_hops,
                .min_weight = step.edge.min_weight,
                .max_weight = step.edge.max_weight,
                .types = step.edge.types,
            },
            .node_filter = .{
                .filter_prefix = step.node_filter.filter_prefix,
                .filter_query_json = if (step.node_filter.query_json.len == 0) null else step.node_filter.query_json,
            },
        };
    }

    const matches = handle.db.matchPattern(handle.alloc, parsed.value.index_name, start_nodes, pattern, parsed.value.max_results, parsed.value.return_aliases) catch |err| return capi.mapError(err);
    defer graph_pattern_mod.freeMatches(handle.alloc, matches);

    var payload = handle.alloc.alloc(JsonPatternMatch, matches.len) catch return .internal;
    var count: usize = 0;
    defer {
        for (payload[0..count]) |*item| item.deinit(handle.alloc);
        if (payload.len > 0) handle.alloc.free(payload);
    }
    for (matches, 0..) |match, i| {
        payload[i] = JsonPatternMatch.init(handle.alloc, match) catch return .internal;
        count += 1;
    }
    out_buf.* = stringifyJson(payload) catch return .internal;
    return .ok;
}

pub export fn antfly_db_create_shadow_index_manager(
    handle_ptr: ?*anyopaque,
    split_key: capi.Slice,
    original_range_end: capi.Slice,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.createShadowIndexManager(split_key.bytes(), original_range_end.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_close_shadow_index_manager(handle_ptr: ?*anyopaque) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.closeShadowIndexManager() catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_get_shadow_index_dir(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const dir = handle.db.getShadowIndexDir();
    if (dir.len == 0) return .not_found;
    out_buf.* = dupBytes(dir) catch return .internal;
    return .ok;
}

pub export fn antfly_db_find_median_key(
    handle_ptr: ?*anyopaque,
    out_buf: *capi.Buffer,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    const key = handle.db.findMedianKey(handle.alloc) catch |err| return capi.mapError(err);
    defer handle.alloc.free(key);
    out_buf.* = dupBytes(key) catch return .internal;
    return .ok;
}

pub export fn antfly_db_split(
    handle_ptr: ?*anyopaque,
    curr_start: capi.Slice,
    curr_end: capi.Slice,
    split_key: capi.Slice,
    dest_dir1: capi.Slice,
    dest_dir2: capi.Slice,
    prepare_only: bool,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.split(
        .{
            .start = curr_start.bytes(),
            .end = curr_end.bytes(),
        },
        split_key.bytes(),
        dest_dir1.bytes(),
        dest_dir2.bytes(),
        prepare_only,
    ) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_finalize_split(
    handle_ptr: ?*anyopaque,
    new_start: capi.Slice,
    new_end: capi.Slice,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    handle.db.finalizeSplit(.{
        .start = new_start.bytes(),
        .end = new_end.bytes(),
    }) catch |err| return capi.mapError(err);
    return .ok;
}

pub export fn antfly_db_snapshot(
    handle_ptr: ?*anyopaque,
    id: capi.Slice,
    out_size: *u64,
) capi.ErrorCode {
    const handle = asHandle(handle_ptr) orelse return .invalid_argument;
    out_size.* = handle.db.snapshot(id.bytes()) catch |err| return capi.mapError(err);
    return .ok;
}

test "capi transaction lifecycle" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "capi-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    defer cleanupTestDir(path);

    const txn_id: [16]u8 = .{ 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_begin_transaction_with_id(handle_ptr, &txn_id, 1_000, null, 0));

    const writes = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi", .len = "doc:capi".len },
            .value = .{ .ptr = "{\"title\":\"ok\"}", .len = "{\"title\":\"ok\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_write_transaction(handle_ptr, &txn_id, &writes, writes.len, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_resolve_intents(handle_ptr, &txn_id, @intFromEnum(transactions_mod.TxnStatus.committed), 2_000));

    var status: u8 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_transaction_status(handle_ptr, &txn_id, &status));
    try std.testing.expectEqual(@as(u8, @intFromEnum(transactions_mod.TxnStatus.committed)), status);

    var commit_version: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_commit_version(handle_ptr, &txn_id, &commit_version));
    try std.testing.expectEqual(@as(u64, 2_000), commit_version);
}

test "capi batch and lookup json" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "capi-batch-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    defer cleanupTestDir(path);

    const writes = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-batch", .len = "doc:capi-batch".len },
            .value = .{ .ptr = "{\"title\":\"ok\"}", .len = "{\"title\":\"ok\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(handle_ptr, &writes, writes.len, null, 0, 1_000, 0));

    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle_ptr, .{
        .ptr = "doc:capi-batch",
        .len = "doc:capi-batch".len,
    }, &out));
    defer antfly_db_buffer_free(out.ptr, out.len);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"title\":\"ok\"") != null);
}

test "capi request paths trigger readable lease hook" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "capi-readable-lease");
    defer alloc.free(path);

    cleanupTestDir(path);

    const Recorder = struct {
        contexts: [9][32]u8 = [_][32]u8{[_]u8{0} ** 32} ** 9,
        context_lens: [9]usize = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        group_ids: [9]u64 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        count: usize = 0,

        fn callback(
            ctx: ?*anyopaque,
            group_id: u64,
            request_ctx_ptr: ?[*]const u8,
            request_ctx_len: usize,
        ) callconv(.c) capi.ErrorCode {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (self.count >= self.contexts.len or request_ctx_len > self.contexts[self.count].len) return .internal;
            self.group_ids[self.count] = group_id;
            if (request_ctx_ptr != null and request_ctx_len > 0) {
                @memcpy(self.contexts[self.count][0..request_ctx_len], request_ctx_ptr.?[0..request_ctx_len]);
            }
            self.context_lens[self.count] = request_ctx_len;
            self.count += 1;
            return .ok;
        }
    };

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
            },
        },
    });

    var recorder = Recorder{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        @ptrCast(&handle),
        42,
        &recorder,
        &Recorder.callback,
    ));

    var lookup_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(
        @ptrCast(&handle),
        .{ .ptr = "doc:a".ptr, .len = "doc:a".len },
        &lookup_out,
    ));
    antfly_db_buffer_free(lookup_out.ptr, lookup_out.len);

    const scan_req = "{\"from_key_b64\":\"\",\"to_key_b64\":\"\",\"include_documents\":false,\"limit\":10}";
    var scan_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_scan_json(
        @ptrCast(&handle),
        .{ .ptr = scan_req.ptr, .len = scan_req.len },
        &scan_out,
    ));
    antfly_db_buffer_free(scan_out.ptr, scan_out.len);

    const search_req =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":1,\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var search_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
        @ptrCast(&handle),
        .{ .ptr = search_req.ptr, .len = search_req.len },
        &search_out,
    ));
    antfly_db_buffer_free(search_out.ptr, search_out.len);

    var packed_result: capi.PackedDenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
        @ptrCast(&handle),
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &packed_result,
    ));
    antfly_db_packed_dense_search_result_free(&packed_result);

    var dense_profile: capi.DenseSearchProfile = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_profile(
        @ptrCast(&handle),
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &dense_profile,
    ));

    const dense_wire_req = [_]u8{
        0x54, 0x46, 0x4E, 0x44,
        0x01, 0x00, 0x01, 0x00,
        0x05, 0x00, 0x02, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        'd',  'v',  '_',  'v',
        '1',  0x00, 0x00, 0x80,
        0x3f, 0x00, 0x00, 0x00,
        0x00,
    };
    var dense_wire_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_wire(
        @ptrCast(&handle),
        .{ .ptr = &dense_wire_req, .len = dense_wire_req.len },
        &dense_wire_out,
    ));
    antfly_db_buffer_free(dense_wire_out.ptr, dense_wire_out.len);

    var dense_wire_profile_out: capi.Buffer = .{};
    var dense_wire_profile: capi.DenseWireSearchProfile = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_wire_profile(
        @ptrCast(&handle),
        .{ .ptr = &dense_wire_req, .len = dense_wire_req.len },
        &dense_wire_profile_out,
        &dense_wire_profile,
    ));
    antfly_db_buffer_free(dense_wire_profile_out.ptr, dense_wire_profile_out.len);

    var text_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
        @ptrCast(&handle),
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        .{ .ptr = "title".ptr, .len = "title".len },
        .{ .ptr = "alpha".ptr, .len = "alpha".len },
        1,
        0,
        &text_result,
    ));
    antfly_db_dense_search_result_free(&text_result);

    const hits_req =
        "{\"mode\":\"full_text\",\"index_name\":\"dv_v1\",\"text_query_type\":\"match\",\"field\":\"title\",\"text\":\"alpha\",\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var hits_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_hits_json(
        @ptrCast(&handle),
        .{ .ptr = hits_req.ptr, .len = hits_req.len },
        &hits_result,
    ));
    antfly_db_dense_search_result_free(&hits_result);

    try std.testing.expectEqual(@as(usize, 9), recorder.count);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[1]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[2]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[3]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[4]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[5]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[6]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[7]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[8]);
    try std.testing.expectEqualStrings("enrichment:lookup:read_index", recorder.contexts[0][0..recorder.context_lens[0]]);
    try std.testing.expectEqualStrings("enrichment:scan:read_index", recorder.contexts[1][0..recorder.context_lens[1]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[2][0..recorder.context_lens[2]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[3][0..recorder.context_lens[3]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[4][0..recorder.context_lens[4]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[5][0..recorder.context_lens[5]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[6][0..recorder.context_lens[6]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[7][0..recorder.context_lens[7]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[8][0..recorder.context_lens[8]]);
}

test "capi artifact decode and lookup json" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "capi-artifact-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    defer cleanupTestDir(path);

    const handle = asHandle(handle_ptr).?;
    var artifact_ref = db_mod.types.ArtifactRef{
        .document_id = try handle.alloc.dupe(u8, "doc:a"),
        .name = try handle.alloc.dupe(u8, "body_chunks_v1"),
        .kind = .chunk,
        .chunk_id = 0,
    };
    defer artifact_ref.deinit(handle.alloc);
    const internal_key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(handle.alloc, artifact_ref);
    defer handle.alloc.free(internal_key);
    try handle.db.store.put(internal_key, "{\"body\":\"abcdefgh\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    const artifact_id = try db_mod.artifact_ids.artifactPublicIdAlloc(handle.alloc, artifact_ref);
    defer handle.alloc.free(artifact_id);
    const artifact_id_b64 = try dupBase64(handle.alloc, artifact_id);
    defer handle.alloc.free(artifact_id_b64);

    var decode_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_decode_artifact_id_json(.{
        .ptr = artifact_id_b64.ptr,
        .len = artifact_id_b64.len,
    }, &decode_out));
    defer antfly_db_buffer_free(decode_out.ptr, decode_out.len);
    try std.testing.expect(std.mem.indexOf(u8, decode_out.ptr.?[0..decode_out.len], "\"kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, decode_out.ptr.?[0..decode_out.len], "\"name\":\"body_chunks_v1\"") != null);

    var lookup_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_artifact_json(handle_ptr, .{
        .ptr = artifact_id_b64.ptr,
        .len = artifact_id_b64.len,
    }, &lookup_out));
    defer antfly_db_buffer_free(lookup_out.ptr, lookup_out.len);
    try std.testing.expect(std.mem.indexOf(u8, lookup_out.ptr.?[0..lookup_out.len], "\"artifact_ref\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, lookup_out.ptr.?[0..lookup_out.len], "\"_chunk_id\":0") != null);
}

test "capi dense search profile breakdown" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "capi-dense-profile");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    const writes = try alloc.alloc(db_mod.types.BatchWrite, 2048);
    defer {
        for (writes) |write| alloc.free(write.value);
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        const x: f32 = if (i % 2 == 0) 1 else 0;
        const y: f32 = if (i % 2 == 0) 0 else 1;
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"embedding\":[{d},{d}],\"title\":\"doc-{d}\"}}", .{ x, y, i }),
        };
    }
    defer for (writes) |write| alloc.free(write.key);

    try handle.db.batch(.{ .writes = writes });

    const req: db_mod.types.SearchRequest = .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0 },
            .k = 10,
        } },
        .limit = 10,
        .include_stored = false,
    };

    const dense_entry = handle.db.core.index_manager.denseIndex("dv_v1").?;

    const reps: usize = 20;

    var hbc_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = std.time.nanoTimestamp();
        var result = try dense_entry.index.search(&.{ 1.0, 0.0 }, 10);
        defer result.deinit();
        hbc_total_ns += @intCast(std.time.nanoTimestamp() - start);
    }

    var db_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = std.time.nanoTimestamp();
        var result = try handle.db.search(alloc, req);
        defer result.deinit();
        db_total_ns += @intCast(std.time.nanoTimestamp() - start);
    }

    const request_json =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":10,\"limit\":10,\"offset\":0,\"include_stored\":false}";
    var capi_total_ns: u64 = 0;
    for (0..reps) |_| {
        var out: capi.Buffer = .{};
        const start = std.time.nanoTimestamp();
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
            @ptrCast(&handle),
            .{ .ptr = request_json.ptr, .len = request_json.len },
            &out,
        ));
        capi_total_ns += @intCast(std.time.nanoTimestamp() - start);
        antfly_db_buffer_free(out.ptr, out.len);
    }

    std.debug.print(
        "dense_profile reps={d} hbc_avg_ns={d} db_avg_ns={d} capi_avg_ns={d}\n",
        .{
            reps,
            @divTrunc(hbc_total_ns, reps),
            @divTrunc(db_total_ns, reps),
            @divTrunc(capi_total_ns, reps),
        },
    );

    var final_result = try handle.db.search(alloc, req);
    defer final_result.deinit();
    try std.testing.expectEqual(@as(u32, 10), final_result.total_hits);
}
