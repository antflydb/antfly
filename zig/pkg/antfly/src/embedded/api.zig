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
const embedded_db = @import("embedded_db_surface");
const support = @import("embedded_support");
const batch_api = support.batch;
const query_api = support.query;
const query_contract = support.query_contract;
const backup_codec = support.backup_codec;

const Allocator = std.mem.Allocator;

pub const OpenOptions = struct {
    db: embedded_db.OpenOptions = .{},
    profile: embedded_db.Profile = .native,
    table_name: []const u8 = "docs",
    semantic_resolver: ?query_contract.SemanticResolver = null,
};

pub const Api = struct {
    allocator: Allocator,
    db: embedded_db.DB,
    table_name: []u8,
    semantic_resolver: ?query_contract.SemanticResolver,

    pub fn open(allocator: Allocator, path: []const u8, opts: OpenOptions) !Api {
        return .{
            .allocator = allocator,
            .db = try embedded_db.DB.openWithProfile(allocator, path, opts.db, opts.profile),
            .table_name = try allocator.dupe(u8, opts.table_name),
            .semantic_resolver = opts.semantic_resolver,
        };
    }

    pub fn openHosted(allocator: Allocator, path: []const u8, opts: OpenOptions) !Api {
        return .{
            .allocator = allocator,
            .db = try embedded_db.DB.openHosted(allocator, path, opts.db),
            .table_name = try allocator.dupe(u8, opts.table_name),
            .semantic_resolver = opts.semantic_resolver,
        };
    }

    pub fn openLite(allocator: Allocator, path: []const u8, opts: OpenOptions) !Api {
        return .{
            .allocator = allocator,
            .db = try embedded_db.DB.openLiteWithProfile(allocator, path, opts.db, opts.profile),
            .table_name = try allocator.dupe(u8, opts.table_name),
            .semantic_resolver = opts.semantic_resolver,
        };
    }

    pub fn createLite(allocator: Allocator, path: []const u8, opts: OpenOptions) !Api {
        return .{
            .allocator = allocator,
            .db = try embedded_db.DB.createLiteWithProfile(allocator, path, opts.db, opts.profile),
            .table_name = try allocator.dupe(u8, opts.table_name),
            .semantic_resolver = opts.semantic_resolver,
        };
    }

    pub fn openLiteHosted(allocator: Allocator, path: []const u8, opts: OpenOptions) !Api {
        return .{
            .allocator = allocator,
            .db = try embedded_db.DB.openLiteHosted(allocator, path, opts.db),
            .table_name = try allocator.dupe(u8, opts.table_name),
            .semantic_resolver = opts.semantic_resolver,
        };
    }

    pub fn createLiteHosted(allocator: Allocator, path: []const u8, opts: OpenOptions) !Api {
        return .{
            .allocator = allocator,
            .db = try embedded_db.DB.createLiteHosted(allocator, path, opts.db),
            .table_name = try allocator.dupe(u8, opts.table_name),
            .semantic_resolver = opts.semantic_resolver,
        };
    }

    pub fn close(self: *Api) void {
        self.db.close();
        self.allocator.free(self.table_name);
        self.* = undefined;
    }

    pub fn batchJson(self: *Api, alloc: Allocator, body: []const u8) ![]u8 {
        var owned = try batch_api.parseBatchRequest(alloc, body);
        defer owned.deinit(alloc);

        try self.db.batch(owned.req);
        return try batch_api.encodeBatchResponse(alloc, owned.result());
    }

    pub fn lookupJson(self: *Api, alloc: Allocator, key: []const u8, body: []const u8) ![]u8 {
        var opts = try parseLookupRequest(alloc, body);
        defer opts.deinit(alloc);

        var result = (try self.db.lookup(alloc, key, opts.lookup_opts)) orelse {
            return try alloc.dupe(u8, "{\"found\":false}");
        };
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        try out.appendSlice(alloc, "{\"found\":true,\"_id\":");
        try appendJsonString(alloc, &out, key);
        try out.appendSlice(alloc, ",\"_source\":");
        try out.appendSlice(alloc, result.json);
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    pub fn scanJson(self: *Api, alloc: Allocator, body: []const u8) ![]u8 {
        var req = try parseScanRequest(alloc, body);
        defer req.deinit(alloc);

        var result = try self.db.scan(alloc, req.from, req.to, req.scan_opts);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        try out.appendSlice(alloc, "{\"hashes\":[");
        for (result.hashes, 0..) |entry, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"_id\":");
            try appendJsonString(alloc, &out, entry.id);
            try out.appendSlice(alloc, ",\"hash\":");
            var hash_buf: [32]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&hash_buf, "{d}", .{entry.hash});
            try out.appendSlice(alloc, rendered);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "],\"documents\":[");
        for (result.documents, 0..) |doc, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"_id\":");
            try appendJsonString(alloc, &out, doc.id);
            try out.appendSlice(alloc, ",\"_source\":");
            try out.appendSlice(alloc, doc.json);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
        return try out.toOwnedSlice(alloc);
    }

    pub fn searchJson(self: *Api, alloc: Allocator, body: []const u8) ![]u8 {
        var owned = try query_api.parsePublicQueryRequest(
            alloc,
            self.semantic_resolver,
            self.table_name,
            body,
        );
        defer owned.deinit(alloc);

        var result = try self.db.search(alloc, owned.req);
        defer result.deinit();

        var response = try query_api.encodeQueryResponses(
            alloc,
            self.table_name,
            owned.req,
            .{},
            result,
        );
        defer response.deinit(alloc);
        return try alloc.dupe(u8, response.json);
    }

    pub fn statsJson(self: *Api, alloc: Allocator) ![]u8 {
        const stats = try self.db.stats(alloc);
        defer embedded_db.types.freeDBStats(alloc, stats);
        return try std.json.Stringify.valueAlloc(alloc, stats, .{});
    }

    pub fn pendingWorkStatsJson(self: *Api, alloc: Allocator) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, self.db.pendingWorkStats(), .{});
    }

    pub fn capabilitiesJson(self: *Api, alloc: Allocator) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, self.db.capabilities(), .{});
    }

    pub fn statusJson(self: *Api, alloc: Allocator) ![]u8 {
        var status = try self.db.liteStatus(alloc);
        defer status.deinit(alloc);
        return try std.json.Stringify.valueAlloc(alloc, status, .{});
    }

    pub fn runUntilIdleJson(self: *Api, alloc: Allocator) ![]u8 {
        try self.db.runUntilIdle();
        return try self.pendingWorkStatsJson(alloc);
    }

    pub fn replayGeneratedEnrichmentsJson(self: *Api, alloc: Allocator) ![]u8 {
        const replayed = try self.db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        return try std.fmt.allocPrint(alloc, "{{\"replayed\":{d}}}", .{replayed});
    }

    pub fn listIndexesJson(self: *Api, alloc: Allocator) ![]u8 {
        const configs = try self.db.listIndexes(alloc);
        defer embedded_db.types.freeIndexConfigs(alloc, configs);
        const public_configs = try embedded_db.types.publicIndexConfigsAlloc(alloc, configs);
        defer alloc.free(public_configs);
        return try std.json.Stringify.valueAlloc(alloc, public_configs, .{});
    }

    pub fn addIndexJson(self: *Api, alloc: Allocator, body: []const u8) ![]u8 {
        var parsed = try std.json.parseFromSlice(embedded_db.types.IndexConfig, alloc, body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        parsed.value.coverage_generation = 0;
        try self.db.addIndex(parsed.value);
        return try namedMutationJson(alloc, "created", parsed.value.name);
    }

    pub fn dropIndexJson(self: *Api, alloc: Allocator, name: []const u8) ![]u8 {
        const removed = try self.db.deleteIndex(name);
        return try namedBoolMutationJson(alloc, "removed", name, removed);
    }

    pub fn listEnrichmentsJson(self: *Api, alloc: Allocator) ![]u8 {
        const configs = try self.db.listEnrichments(alloc);
        defer embedded_db.types.freeEnrichmentConfigs(alloc, configs);
        return try std.json.Stringify.valueAlloc(alloc, configs, .{});
    }

    pub fn addEnrichmentJson(self: *Api, alloc: Allocator, body: []const u8) ![]u8 {
        var parsed = try std.json.parseFromSlice(embedded_db.types.EnrichmentConfig, alloc, body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        try self.db.addEnrichment(parsed.value);
        return try namedMutationJson(alloc, "created", parsed.value.name);
    }

    pub fn dropEnrichmentJson(
        self: *Api,
        alloc: Allocator,
        kind: embedded_db.types.EnrichmentKind,
        name: []const u8,
    ) ![]u8 {
        const removed = try self.db.deleteEnrichment(kind, name);
        return try namedBoolMutationJson(alloc, "removed", name, removed);
    }

    pub fn getSchemaJson(self: *Api, alloc: Allocator) ![]u8 {
        const schema_json = (try self.db.getSchemaJson(alloc)) orelse return try alloc.dupe(u8, "null");
        return schema_json;
    }

    pub fn setSchemaJson(self: *Api, alloc: Allocator, body: []const u8) ![]u8 {
        try self.db.setSchemaJson(alloc, body);
        return try alloc.dupe(u8, "{\"updated\":true}");
    }

    pub fn exportPortable(self: *Api, alloc: Allocator, out: *std.ArrayList(u8)) !void {
        try self.db.exportPortable(alloc, out);
    }

    pub fn importPortable(self: *Api, alloc: Allocator, backup: []const u8) !void {
        try self.db.importPortable(alloc, backup);
    }

    pub fn checkLiteJson(self: *Api, alloc: Allocator) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, try self.db.checkLite(), .{});
    }

    pub fn checkLiteFileJson(alloc: Allocator, path: []const u8) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, try embedded_db.checkLiteFile(alloc, path), .{});
    }

    pub fn copyStableLiteSnapshotJson(self: *Api, alloc: Allocator, dest_path: []const u8, replace: bool) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, try self.db.copyStableLiteSnapshot(dest_path, replace), .{});
    }

    pub fn copyStableLiteSnapshotFileJson(alloc: Allocator, source_path: []const u8, dest_path: []const u8, replace: bool) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, try embedded_db.copyStableLiteSnapshotFile(alloc, source_path, dest_path, replace), .{});
    }

    pub fn vacuumLiteJson(self: *Api, alloc: Allocator) ![]u8 {
        return try std.json.Stringify.valueAlloc(alloc, try self.db.vacuumLite(), .{});
    }
};

pub fn checkLiteFileJson(alloc: Allocator, path: []const u8) ![]u8 {
    return try Api.checkLiteFileJson(alloc, path);
}

pub fn copyStableLiteSnapshotFileJson(alloc: Allocator, source_path: []const u8, dest_path: []const u8, replace: bool) ![]u8 {
    return try Api.copyStableLiteSnapshotFileJson(alloc, source_path, dest_path, replace);
}

const ParsedLookupRequest = struct {
    fields: ?[]const []const u8 = null,
};

const OwnedLookupRequest = struct {
    fields: [][]const u8 = &.{},
    lookup_opts: embedded_db.types.LookupOptions = .{},

    fn deinit(self: *OwnedLookupRequest, alloc: Allocator) void {
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        self.* = undefined;
    }
};

const ParsedScanRequest = struct {
    from: ?[]const u8 = null,
    to: ?[]const u8 = null,
    inclusive_from: ?bool = null,
    exclusive_to: ?bool = null,
    include_documents: ?bool = null,
    fields: ?[]const []const u8 = null,
    limit: ?u32 = null,
};

const OwnedScanRequest = struct {
    from: []const u8 = "",
    to: []const u8 = "",
    fields: [][]const u8 = &.{},
    scan_opts: embedded_db.types.ScanOptions = .{},

    fn deinit(self: *OwnedScanRequest, alloc: Allocator) void {
        if (self.from.len > 0) alloc.free(self.from);
        if (self.to.len > 0) alloc.free(self.to);
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        self.* = undefined;
    }
};

fn parseLookupRequest(alloc: Allocator, body: []const u8) !OwnedLookupRequest {
    if (body.len == 0) return .{};

    var parsed = try std.json.parseFromSlice(ParsedLookupRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const fields: [][]const u8 = if (parsed.value.fields) |raw_fields|
        try cloneFieldList(alloc, raw_fields)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeFieldList(alloc, fields);

    return .{
        .fields = fields,
        .lookup_opts = .{
            .fields = fields,
            .include_all_fields = fields.len == 0,
        },
    };
}

fn parseScanRequest(alloc: Allocator, body: []const u8) !OwnedScanRequest {
    if (body.len == 0) return .{};

    var parsed = try std.json.parseFromSlice(ParsedScanRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const fields: [][]const u8 = if (parsed.value.fields) |raw_fields|
        try cloneFieldList(alloc, raw_fields)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer freeFieldList(alloc, fields);

    const from = if (parsed.value.from) |value| try alloc.dupe(u8, value) else "";
    errdefer if (from.len > 0) alloc.free(from);
    const to = if (parsed.value.to) |value| try alloc.dupe(u8, value) else "";
    errdefer if (to.len > 0) alloc.free(to);

    return .{
        .from = from,
        .to = to,
        .fields = fields,
        .scan_opts = .{
            .inclusive_from = parsed.value.inclusive_from orelse false,
            .exclusive_to = parsed.value.exclusive_to orelse false,
            .include_documents = parsed.value.include_documents orelse false,
            .limit = parsed.value.limit orelse 0,
            .fields = fields,
            .include_all_fields = fields.len == 0,
        },
    };
}

fn cloneFieldList(alloc: Allocator, raw_fields: []const []const u8) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, raw_fields.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (raw_fields, 0..) |field, i| {
        fields[i] = try alloc.dupe(u8, field);
        initialized += 1;
    }
    return fields;
}

fn freeFieldList(alloc: Allocator, fields: [][]const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len > 0) alloc.free(fields);
}

fn appendJsonString(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn namedMutationJson(alloc: Allocator, field_name: []const u8, name: []const u8) ![]u8 {
    return try namedBoolMutationJson(alloc, field_name, name, true);
}

fn namedBoolMutationJson(alloc: Allocator, field_name: []const u8, name: []const u8, value: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.append(alloc, '{');
    try appendJsonString(alloc, &out, field_name);
    try out.append(alloc, ':');
    try out.appendSlice(alloc, if (value) "true" else "false");
    try out.appendSlice(alloc, ",\"name\":");
    try appendJsonString(alloc, &out, name);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

test "embedded api round-trips batch lookup scan and search over memory-backed durable lsm" {
    const lsm_backend = support.lsm_storage;
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var memory_storage = lsm_backend.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    var api = try Api.open(alloc, path, .{
        .table_name = "docs",
        .db = .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .storage = memory_storage.storage(),
        },
    });
    defer api.close();
    try api.db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });

    const batch_json = try api.batchJson(
        alloc,
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"},\"doc:b\":{\"title\":\"beta\"}}}",
    );
    defer alloc.free(batch_json);
    try std.testing.expect(std.mem.indexOf(u8, batch_json, "\"inserted\":2") != null);

    const lookup_json = try api.lookupJson(
        alloc,
        "doc:a",
        "{\"fields\":[\"title\"]}",
    );
    defer alloc.free(lookup_json);
    try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"found\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"alpha\"") != null);

    const scan_json = try api.scanJson(
        alloc,
        "{\"from\":\"doc:a\",\"to\":\"doc:z\",\"include_documents\":true,\"fields\":[\"title\"]}",
    );
    defer alloc.free(scan_json);
    try std.testing.expect(std.mem.indexOf(u8, scan_json, "\"documents\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan_json, "\"beta\"") != null);

    const idle_json = try api.runUntilIdleJson(alloc);
    defer alloc.free(idle_json);
    try std.testing.expect(std.mem.indexOf(u8, idle_json, "\"has_async_indexes\"") != null);

    const query_json = try api.searchJson(
        alloc,
        "{\"full_text_search\":{\"match\":{\"field\":\"title\",\"text\":\"alpha\"}},\"limit\":1}",
    );
    defer alloc.free(query_json);
    try std.testing.expect(std.mem.indexOf(u8, query_json, "\"responses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, query_json, "\"doc:a\"") != null);
    try std.testing.expectError(error.InvalidQueryRequest, api.searchJson(
        alloc,
        "{\"query\":{\"match_all\":{}},\"native_doc_id_constraints\":{\"include_doc_ids\":[\"doc:a\"]}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, api.searchJson(
        alloc,
        "{\"query\":{\"match_all\":{}},\"_identity_read_generation\":1}",
    ));

    const pending_json = try api.pendingWorkStatsJson(alloc);
    defer alloc.free(pending_json);
    try std.testing.expect(std.mem.indexOf(u8, pending_json, "\"derived_target_sequence\"") != null);

    const capabilities_json = try api.capabilitiesJson(alloc);
    defer alloc.free(capabilities_json);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hosted_profile\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"local_template_rendering\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_template_rendering\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_template_host_callbacks\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"caller_supplied_embeddings\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"local_inference_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"text_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hybrid_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"graph_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"raft_replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_placement\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_shard_fanout\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"distributed_transaction_coordination\":false") != null);

    try std.testing.expectError(error.NotLiteDatabase, api.statusJson(alloc));

    const stats_json = try api.statsJson(alloc);
    defer alloc.free(stats_json);
    try std.testing.expect(std.mem.indexOf(u8, stats_json, "\"doc_count\":2") != null);
}

test "embedded api hosted profile drains derived indexing without native runtimes" {
    const lsm_backend = support.lsm_storage;
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var memory_storage = lsm_backend.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    var api = try Api.openHosted(alloc, path, .{
        .table_name = "docs",
        .db = .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .storage = memory_storage.storage(),
        },
    });
    defer api.close();
    try api.db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });

    const batch_json = try api.batchJson(
        alloc,
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha hosted\"},\"doc:b\":{\"title\":\"beta hosted\"}}}",
    );
    defer alloc.free(batch_json);
    try std.testing.expect(std.mem.indexOf(u8, batch_json, "\"inserted\":2") != null);

    const pending_before = try api.pendingWorkStatsJson(alloc);
    defer alloc.free(pending_before);
    try std.testing.expect(std.mem.indexOf(u8, pending_before, "\"has_async_indexes\":true") != null);

    const capabilities_json = try api.capabilitiesJson(alloc);
    defer alloc.free(capabilities_json);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hosted_profile\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"manual_maintenance\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"local_template_rendering\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_template_rendering\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_template_host_callbacks\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"caller_supplied_embeddings\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"local_inference_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"text_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hybrid_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"graph_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"raft_replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_placement\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cross_node_joins\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_shard_fanout\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"distributed_transaction_coordination\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_heartbeat_status_aggregation\":false") != null);

    const idle_json = try api.runUntilIdleJson(alloc);
    defer alloc.free(idle_json);
    try std.testing.expect(std.mem.indexOf(u8, idle_json, "\"derived_target_sequence\"") != null);

    const query_json = try api.searchJson(
        alloc,
        "{\"full_text_search\":{\"match\":{\"field\":\"title\",\"text\":\"alpha hosted\"}},\"limit\":1}",
    );
    defer alloc.free(query_json);
    try std.testing.expect(std.mem.indexOf(u8, query_json, "\"doc:a\"") != null);
}

test "embedded api hosted profile persists text index across reopen over storage" {
    const lsm_backend = support.lsm_storage;
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var memory_storage = lsm_backend.MemoryStorage.init(alloc);
    defer memory_storage.deinit();

    {
        var api = try Api.openHosted(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
                .storage = memory_storage.storage(),
            },
        });
        defer api.close();

        try api.db.addIndex(.{
            .name = "full_text_index_v0",
            .kind = .full_text,
            .config_json = "{}",
        });

        const batch_json = try api.batchJson(
            alloc,
            "{\"inserts\":{\"doc:a\":{\"title\":\"alpha hosted reopen\"},\"doc:b\":{\"title\":\"beta hosted reopen\"}}}",
        );
        defer alloc.free(batch_json);
        try std.testing.expect(std.mem.indexOf(u8, batch_json, "\"inserted\":2") != null);

        const idle_json = try api.runUntilIdleJson(alloc);
        defer alloc.free(idle_json);
        try std.testing.expect(std.mem.indexOf(u8, idle_json, "\"derived_target_sequence\"") != null);
    }

    {
        var reopened = try Api.openHosted(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
                .storage = memory_storage.storage(),
            },
        });
        defer reopened.close();

        const query_json = try reopened.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"title\",\"text\":\"alpha hosted reopen\"}},\"limit\":1}",
        );
        defer alloc.free(query_json);
        try std.testing.expect(std.mem.indexOf(u8, query_json, "\"doc:a\"") != null);

        const lookup_json = try reopened.lookupJson(
            alloc,
            "doc:a",
            "{\"fields\":[\"title\"]}",
        );
        defer alloc.free(lookup_json);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"alpha hosted reopen\"") != null);
    }
}

test "embedded api openLite round-trips batch lookup over aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var api = try Api.createLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer api.close();

        const batch_json = try api.batchJson(
            alloc,
            "{\"inserts\":{\"doc:a\":{\"title\":\"alpha lite api\"}}}",
        );
        defer alloc.free(batch_json);
        try std.testing.expect(std.mem.indexOf(u8, batch_json, "\"inserted\":1") != null);
    }

    {
        var reopened = try Api.openLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer reopened.close();

        const lookup_json = try reopened.lookupJson(
            alloc,
            "doc:a",
            "{\"fields\":[\"title\"]}",
        );
        defer alloc.free(lookup_json);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"found\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"alpha lite api\"") != null);
    }
}

test "embedded api openLite manages index and enrichment definitions over aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-management.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var api = try Api.createLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer api.close();

        const created_index = try api.addIndexJson(
            alloc,
            "{\"name\":\"full_text_index_v0\",\"kind\":\"full_text\",\"config_json\":\"{}\",\"coverage_generation\":12345}",
        );
        defer alloc.free(created_index);
        try std.testing.expect(std.mem.indexOf(u8, created_index, "\"created\":true") != null);

        const created_enrichment = try api.addEnrichmentJson(
            alloc,
            "{\"name\":\"body_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":8,\"chunk_overlap\":2}",
        );
        defer alloc.free(created_enrichment);
        try std.testing.expect(std.mem.indexOf(u8, created_enrichment, "\"created\":true") != null);
    }

    {
        var reopened = try Api.openLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer reopened.close();

        const indexes = try reopened.listIndexesJson(alloc);
        defer alloc.free(indexes);
        try std.testing.expect(std.mem.indexOf(u8, indexes, "\"full_text_index_v0\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes, "coverage_generation") == null);

        const enrichments = try reopened.listEnrichmentsJson(alloc);
        defer alloc.free(enrichments);
        try std.testing.expect(std.mem.indexOf(u8, enrichments, "\"body_chunks_v1\"") != null);

        const removed_index = try reopened.dropIndexJson(alloc, "full_text_index_v0");
        defer alloc.free(removed_index);
        try std.testing.expect(std.mem.indexOf(u8, removed_index, "\"removed\":true") != null);

        const removed_enrichment = try reopened.dropEnrichmentJson(alloc, .chunk, "body_chunks_v1");
        defer alloc.free(removed_enrichment);
        try std.testing.expect(std.mem.indexOf(u8, removed_enrichment, "\"removed\":true") != null);
    }
}

test "embedded api openLite resumes generated enrichment after hosted maintenance pause" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-enrichment-resume.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var hosted = try Api.createLiteHosted(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer hosted.close();

        const caps = try hosted.capabilitiesJson(alloc);
        defer alloc.free(caps);
        try std.testing.expect(std.mem.indexOf(u8, caps, "\"manual_maintenance\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, caps, "\"background_enrichment_runtime\":false") != null);

        const created_enrichment = try hosted.addEnrichmentJson(
            alloc,
            "{\"name\":\"body_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":24,\"chunk_overlap\":0}",
        );
        defer alloc.free(created_enrichment);
        try std.testing.expect(std.mem.indexOf(u8, created_enrichment, "\"created\":true") != null);

        const created_index = try hosted.addIndexJson(
            alloc,
            "{\"name\":\"ft_body_chunks\",\"kind\":\"full_text\",\"config_json\":\"{\\\"chunk_name\\\":\\\"body_chunks_v1\\\"}\"}",
        );
        defer alloc.free(created_index);
        try std.testing.expect(std.mem.indexOf(u8, created_index, "\"created\":true") != null);

        const written = try hosted.batchJson(
            alloc,
            "{\"inserts\":{\"doc:paused\":{\"title\":\"paused\",\"body\":\"manual maintenance pause resume phrase\"}},\"sync_level\":\"write\"}",
        );
        defer alloc.free(written);
        try std.testing.expect(std.mem.indexOf(u8, written, "\"inserted\":1") != null);

        const pending = try hosted.pendingWorkStatsJson(alloc);
        defer alloc.free(pending);
        try std.testing.expect(std.mem.indexOf(u8, pending, "\"has_async_indexes\":true") != null);
    }

    {
        var resumed = try Api.openLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
                .enrichment = .{ .enable_without_producers = true },
            },
        });
        defer resumed.close();

        const replayed = try resumed.replayGeneratedEnrichmentsJson(alloc);
        defer alloc.free(replayed);
        try std.testing.expect(std.mem.indexOf(u8, replayed, "\"replayed\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, replayed, "\"replayed\":0") == null);

        const idle = try resumed.runUntilIdleJson(alloc);
        defer alloc.free(idle);
        try std.testing.expect(std.mem.indexOf(u8, idle, "\"has_async_indexes\":true") != null);

        const query_json = try resumed.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"resume phrase\"}},\"limit\":1}",
        );
        defer alloc.free(query_json);
        try std.testing.expect(std.mem.indexOf(u8, query_json, "\"doc:paused\"") != null);
    }
}

test "embedded api openLite keeps full text index inside native aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-native-index.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    const sidecar_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{path});
    defer alloc.free(sidecar_path);
    const applied_sequence_checkpoint_path = try support.db.apply_state.checkpointPathAlloc(alloc, path);
    defer alloc.free(applied_sequence_checkpoint_path);

    {
        var api = try Api.createLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer api.close();

        const created_index = try api.addIndexJson(
            alloc,
            "{\"name\":\"ft_body\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
        );
        defer alloc.free(created_index);
        try std.testing.expect(std.mem.indexOf(u8, created_index, "\"created\":true") != null);

        const batch_json = try api.batchJson(
            alloc,
            "{\"inserts\":{\"doc:a\":{\"body\":\"native lite full text\"}},\"sync_level\":\"full_index\"}",
        );
        defer alloc.free(batch_json);
        try std.testing.expect(std.mem.indexOf(u8, batch_json, "\"inserted\":1") != null);

        const query_json = try api.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"native lite\"}},\"limit\":1}",
        );
        defer alloc.free(query_json);
        try std.testing.expect(std.mem.indexOf(u8, query_json, "\"doc:a\"") != null);
    }

    {
        var reopened = try Api.openLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer reopened.close();

        const query_json = try reopened.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"full text\"}},\"limit\":1}",
        );
        defer alloc.free(query_json);
        try std.testing.expect(std.mem.indexOf(u8, query_json, "\"doc:a\"") != null);
    }

    const sidecar_missing = blk: {
        var dir = std.Io.Dir.cwd().openDir(std.testing.io, sidecar_path, .{}) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => break :blk true,
            else => return err,
        };
        dir.close(std.testing.io);
        break :blk false;
    };
    try std.testing.expect(sidecar_missing);
    const applied_sequence_checkpoint_missing = blk: {
        std.Io.Dir.cwd().access(std.testing.io, applied_sequence_checkpoint_path, .{}) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => break :blk true,
            else => return err,
        };
        break :blk false;
    };
    try std.testing.expect(applied_sequence_checkpoint_missing);
}

test "embedded api openLite persists schema json over aflite file" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-schema.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true,"x-antfly-dynamic-indexing":{"mode":"infer_types"}}}}}
    ;

    {
        var api = try Api.createLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer api.close();

        const updated = try api.setSchemaJson(alloc, schema_json);
        defer alloc.free(updated);
        try std.testing.expect(std.mem.indexOf(u8, updated, "\"updated\":true") != null);
    }

    {
        var reopened = try Api.openLite(alloc, path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer reopened.close();

        const persisted = try reopened.getSchemaJson(alloc);
        defer alloc.free(persisted);
        try std.testing.expectEqualStrings(schema_json, persisted);
    }
}

test "embedded api openLite exports imports checks and vacuums portable backup" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-portable-src.aflite", .{tmp.sub_path});
    defer alloc.free(src_path);
    const dst_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-portable-dst.aflite", .{tmp.sub_path});
    defer alloc.free(dst_path);
    const malformed_dst_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-portable-malformed-dst.aflite", .{tmp.sub_path});
    defer alloc.free(malformed_dst_path);
    const malformed_file_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-malformed-file.aflite", .{tmp.sub_path});
    defer alloc.free(malformed_file_path);
    const roundtrip_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-portable-roundtrip.aflite", .{tmp.sub_path});
    defer alloc.free(roundtrip_path);
    const snapshot_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/embedded-api-lite-snapshot.aflite", .{tmp.sub_path});
    defer alloc.free(snapshot_path);

    var backup = std.ArrayList(u8).empty;
    defer backup.deinit(alloc);
    var roundtrip_backup = std.ArrayList(u8).empty;
    defer roundtrip_backup.deinit(alloc);

    {
        var malformed_file = try std.Io.Dir.cwd().createFile(std.testing.io, malformed_file_path, .{});
        defer malformed_file.close(std.testing.io);
        try malformed_file.writePositionalAll(std.testing.io, "short embedded lite header", 0);
    }
    const malformed_check_json = try Api.checkLiteFileJson(alloc, malformed_file_path);
    defer alloc.free(malformed_check_json);
    try std.testing.expect(std.mem.indexOf(u8, malformed_check_json, "\"valid\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, malformed_check_json, "\"issue\":\"truncated_header\"") != null);

    {
        var api = try Api.createLite(alloc, src_path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer api.close();

        const schema_json =
            \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
        ;
        const schema_updated = try api.setSchemaJson(alloc, schema_json);
        defer alloc.free(schema_updated);
        try std.testing.expect(std.mem.indexOf(u8, schema_updated, "\"updated\":true") != null);

        const enrichment_created = try api.addEnrichmentJson(
            alloc,
            "{\"name\":\"body_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128,\"chunk_overlap\":16}",
        );
        defer alloc.free(enrichment_created);
        try std.testing.expect(std.mem.indexOf(u8, enrichment_created, "\"created\":true") != null);

        const index_created = try api.addIndexJson(
            alloc,
            "{\"name\":\"ft_body\",\"kind\":\"full_text\",\"config_json\":\"{\\\"chunk_name\\\":\\\"body_chunks_v1\\\"}\"}",
        );
        defer alloc.free(index_created);
        try std.testing.expect(std.mem.indexOf(u8, index_created, "\"created\":true") != null);

        const dense_index_created = try api.addIndexJson(
            alloc,
            "{\"name\":\"dv_v1\",\"kind\":\"dense_vector\",\"config_json\":\"{\\\"field\\\":\\\"embedding\\\",\\\"dims\\\":3,\\\"metric\\\":\\\"l2_squared\\\",\\\"external\\\":true}\"}",
        );
        defer alloc.free(dense_index_created);
        try std.testing.expect(std.mem.indexOf(u8, dense_index_created, "\"created\":true") != null);

        const sparse_index_created = try api.addIndexJson(
            alloc,
            "{\"name\":\"sv_v1\",\"kind\":\"sparse_vector\",\"config_json\":\"{\\\"field\\\":\\\"sparse_embedding\\\",\\\"external\\\":true}\"}",
        );
        defer alloc.free(sparse_index_created);
        try std.testing.expect(std.mem.indexOf(u8, sparse_index_created, "\"created\":true") != null);

        const graph_index_created = try api.addIndexJson(
            alloc,
            "{\"name\":\"gr_v1\",\"kind\":\"graph\",\"config_json\":\"{}\"}",
        );
        defer alloc.free(graph_index_created);
        try std.testing.expect(std.mem.indexOf(u8, graph_index_created, "\"created\":true") != null);

        const batch_a = try api.batchJson(
            alloc,
            "{\"inserts\":{\"doc:a\":{\"title\":\"first\"},\"doc:gone\":{\"title\":\"remove\"}}}",
        );
        defer alloc.free(batch_a);
        const batch_b = try api.batchJson(
            alloc,
            "{\"inserts\":{\"doc:a\":{\"title\":\"second\"}},\"deletes\":[\"doc:gone\"]}",
        );
        defer alloc.free(batch_b);

        const vector_batch = try api.batchJson(
            alloc,
            "{\"inserts\":{\"doc:vec:a\":{\"title\":\"vector alpha\",\"body\":\"native lite hybrid alpha\",\"_embeddings\":{\"dv_v1\":[1,0,0],\"sv_v1\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:vec:c\",\"weight\":1.0}]}}},\"doc:vec:b\":{\"title\":\"vector beta\",\"body\":\"native lite hybrid beta\",\"_embeddings\":{\"dv_v1\":[0,1,0],\"sv_v1\":{\"indices\":[99],\"values\":[2.0]}}},\"doc:vec:c\":{\"title\":\"graph target\"}},\"sync_level\":\"full_index\"}",
        );
        defer alloc.free(vector_batch);
        try std.testing.expect(std.mem.indexOf(u8, vector_batch, "\"inserted\":3") != null);

        const dense_before = try api.searchJson(
            alloc,
            "{\"embeddings\":{\"dv_v1\":[1,0,0]},\"indexes\":[\"dv_v1\"],\"limit\":1}",
        );
        defer alloc.free(dense_before);
        try std.testing.expect(std.mem.indexOf(u8, dense_before, "\"doc:vec:a\"") != null);

        const sparse_before = try api.searchJson(
            alloc,
            "{\"embeddings\":{\"sv_v1\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"sv_v1\"],\"limit\":1}",
        );
        defer alloc.free(sparse_before);
        try std.testing.expect(std.mem.indexOf(u8, sparse_before, "\"doc:vec:a\"") != null);

        const graph_before = try api.searchJson(
            alloc,
            "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"gr_v1\",\"start_nodes\":{\"keys\":[\"doc:vec:a\"]},\"params\":{\"edge_types\":[\"links\"]}}},\"limit\":10}",
        );
        defer alloc.free(graph_before);
        try std.testing.expect(std.mem.indexOf(u8, graph_before, "\"doc:vec:c\"") != null);

        const hybrid_before = try api.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"dv_v1\":[1,0,0]},\"indexes\":[\"dv_v1\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer alloc.free(hybrid_before);
        try std.testing.expect(std.mem.indexOf(u8, hybrid_before, "\"doc:vec:a\"") != null);

        const check_before = try api.checkLiteJson(alloc);
        defer alloc.free(check_before);
        try std.testing.expect(std.mem.indexOf(u8, check_before, "\"valid\":true") != null);

        const status_json = try api.statusJson(alloc);
        defer alloc.free(status_json);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"storage\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"format\":\"aflite\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"engine\":\"native_single_file\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"primary_layout\":\"native_document_pages\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"replay_layout\":\"native_replay_lanes_in_document_catalog\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_layout\":\"native_index_catalog_pages\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_layout\":\"lsm") == null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_namespace\":\"__antfly_lite\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"format_version\":1") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"page_size\":4096") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"active_checkpoint\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"stats\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"pending_work\":") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"capabilities\":") != null);

        const vacuumed = try api.vacuumLiteJson(alloc);
        defer alloc.free(vacuumed);
        try std.testing.expect(std.mem.indexOf(u8, vacuumed, "\"reclaimed_bytes\":") != null);

        const check_after = try api.checkLiteJson(alloc);
        defer alloc.free(check_after);
        try std.testing.expect(std.mem.indexOf(u8, check_after, "\"valid\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, check_after, "\"reclaimable_bytes\":0") != null);

        const snapshot_report = try api.copyStableLiteSnapshotJson(alloc, snapshot_path, false);
        defer alloc.free(snapshot_report);
        try std.testing.expect(std.mem.indexOf(u8, snapshot_report, "\"tail_bytes\":0") != null);
        try std.testing.expect(std.mem.indexOf(u8, snapshot_report, "\"snapshot_size\":") != null);

        try api.exportPortable(alloc, &backup);
        try std.testing.expect(backup.items.len > 0);
    }

    {
        var snapshot = try Api.openLite(alloc, snapshot_path, .{
            .table_name = "docs",
            .db = .{
                .open_mode = .query_readonly,
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer snapshot.close();

        const lookup_json = try snapshot.lookupJson(alloc, "doc:a", "{}");
        defer alloc.free(lookup_json);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"second\"") != null);
    }

    {
        var malformed_target = try Api.createLite(alloc, malformed_dst_path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer malformed_target.close();

        const target_batch = try malformed_target.batchJson(
            alloc,
            "{\"inserts\":{\"doc:malformed-target\":{\"title\":\"target survives malformed embedded import\"}}}",
        );
        defer alloc.free(target_batch);

        var malformed = std.ArrayList(u8).empty;
        defer malformed.deinit(alloc);
        try backup_codec.writeHeader(&malformed, alloc, .{
            .format_version = backup_codec.format_version,
            .flags = 0,
            .created_at_ns = 0,
            .backup_id = @as([16]u8, @splat(0)),
            .table_count = 1,
            .shard_count = 1,
        });
        const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
        try backup_codec.writeBlock(&malformed, alloc, .document_batch, &malformed_doc_payload);

        try std.testing.expectError(error.Truncated, malformed_target.importPortable(alloc, malformed.items));

        const target_lookup = try malformed_target.lookupJson(alloc, "doc:malformed-target", "{}");
        defer alloc.free(target_lookup);
        try std.testing.expect(std.mem.indexOf(u8, target_lookup, "\"target survives malformed embedded import\"") != null);
    }

    {
        var restored = try Api.createLite(alloc, dst_path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer restored.close();

        try restored.importPortable(alloc, backup.items);

        const schema_json = try restored.getSchemaJson(alloc);
        defer alloc.free(schema_json);
        try std.testing.expect(std.mem.indexOf(u8, schema_json, "\"default_type\":\"doc\"") != null);

        const indexes_json = try restored.listIndexesJson(alloc);
        defer alloc.free(indexes_json);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"ft_body\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "body_chunks_v1") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"dv_v1\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"sv_v1\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"gr_v1\"") != null);

        const enrichments_json = try restored.listEnrichmentsJson(alloc);
        defer alloc.free(enrichments_json);
        try std.testing.expect(std.mem.indexOf(u8, enrichments_json, "\"body_chunks_v1\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, enrichments_json, "\"chunk_size\":128") != null);

        const lookup_json = try restored.lookupJson(
            alloc,
            "doc:a",
            "{\"fields\":[\"title\"]}",
        );
        defer alloc.free(lookup_json);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"found\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"second\"") != null);

        const deleted_json = try restored.lookupJson(alloc, "doc:gone", "");
        defer alloc.free(deleted_json);
        try std.testing.expect(std.mem.indexOf(u8, deleted_json, "\"found\":false") != null);

        const dense_after = try restored.searchJson(
            alloc,
            "{\"embeddings\":{\"dv_v1\":[1,0,0]},\"indexes\":[\"dv_v1\"],\"limit\":1}",
        );
        defer alloc.free(dense_after);
        try std.testing.expect(std.mem.indexOf(u8, dense_after, "\"doc:vec:a\"") != null);

        const sparse_after = try restored.searchJson(
            alloc,
            "{\"embeddings\":{\"sv_v1\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"sv_v1\"],\"limit\":1}",
        );
        defer alloc.free(sparse_after);
        try std.testing.expect(std.mem.indexOf(u8, sparse_after, "\"doc:vec:a\"") != null);

        const graph_after = try restored.searchJson(
            alloc,
            "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"gr_v1\",\"start_nodes\":{\"keys\":[\"doc:vec:a\"]},\"params\":{\"edge_types\":[\"links\"]}}},\"limit\":10}",
        );
        defer alloc.free(graph_after);
        try std.testing.expect(std.mem.indexOf(u8, graph_after, "\"doc:vec:c\"") != null);

        const hybrid_after = try restored.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"dv_v1\":[1,0,0]},\"indexes\":[\"dv_v1\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer alloc.free(hybrid_after);
        try std.testing.expect(std.mem.indexOf(u8, hybrid_after, "\"doc:vec:a\"") != null);

        try restored.exportPortable(alloc, &roundtrip_backup);
        try std.testing.expect(roundtrip_backup.items.len > 0);
    }

    {
        var roundtrip = try Api.createLite(alloc, roundtrip_path, .{
            .table_name = "docs",
            .db = .{
                .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            },
        });
        defer roundtrip.close();

        try roundtrip.importPortable(alloc, roundtrip_backup.items);

        const schema_json = try roundtrip.getSchemaJson(alloc);
        defer alloc.free(schema_json);
        try std.testing.expect(std.mem.indexOf(u8, schema_json, "\"default_type\":\"doc\"") != null);

        const indexes_json = try roundtrip.listIndexesJson(alloc);
        defer alloc.free(indexes_json);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"ft_body\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"dv_v1\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"sv_v1\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"gr_v1\"") != null);

        const lookup_json = try roundtrip.lookupJson(
            alloc,
            "doc:a",
            "{\"fields\":[\"title\"]}",
        );
        defer alloc.free(lookup_json);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"found\":true") != null);
        try std.testing.expect(std.mem.indexOf(u8, lookup_json, "\"second\"") != null);

        const dense_after = try roundtrip.searchJson(
            alloc,
            "{\"embeddings\":{\"dv_v1\":[1,0,0]},\"indexes\":[\"dv_v1\"],\"limit\":1}",
        );
        defer alloc.free(dense_after);
        try std.testing.expect(std.mem.indexOf(u8, dense_after, "\"doc:vec:a\"") != null);

        const sparse_after = try roundtrip.searchJson(
            alloc,
            "{\"embeddings\":{\"sv_v1\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"sv_v1\"],\"limit\":1}",
        );
        defer alloc.free(sparse_after);
        try std.testing.expect(std.mem.indexOf(u8, sparse_after, "\"doc:vec:a\"") != null);

        const graph_after = try roundtrip.searchJson(
            alloc,
            "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"gr_v1\",\"start_nodes\":{\"keys\":[\"doc:vec:a\"]},\"params\":{\"edge_types\":[\"links\"]}}},\"limit\":10}",
        );
        defer alloc.free(graph_after);
        try std.testing.expect(std.mem.indexOf(u8, graph_after, "\"doc:vec:c\"") != null);

        const hybrid_after = try roundtrip.searchJson(
            alloc,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"dv_v1\":[1,0,0]},\"indexes\":[\"dv_v1\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer alloc.free(hybrid_after);
        try std.testing.expect(std.mem.indexOf(u8, hybrid_after, "\"doc:vec:a\"") != null);
    }
}
