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

    pub fn runUntilIdleJson(self: *Api, alloc: Allocator) ![]u8 {
        try self.db.runUntilIdle();
        return try self.pendingWorkStatsJson(alloc);
    }

    pub fn listIndexesJson(self: *Api, alloc: Allocator) ![]u8 {
        const configs = try self.db.listIndexes(alloc);
        defer embedded_db.types.freeIndexConfigs(alloc, configs);
        return try std.json.Stringify.valueAlloc(alloc, configs, .{});
    }

    pub fn listEnrichmentsJson(self: *Api, alloc: Allocator) ![]u8 {
        const configs = try self.db.listEnrichments(alloc);
        defer embedded_db.types.freeEnrichmentConfigs(alloc, configs);
        return try std.json.Stringify.valueAlloc(alloc, configs, .{});
    }
};

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

    const pending_json = try api.pendingWorkStatsJson(alloc);
    defer alloc.free(pending_json);
    try std.testing.expect(std.mem.indexOf(u8, pending_json, "\"derived_target_sequence\"") != null);

    const capabilities_json = try api.capabilitiesJson(alloc);
    defer alloc.free(capabilities_json);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hosted_profile\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"local_template_rendering\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_template_rendering\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_template_host_callbacks\":false") != null);

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
