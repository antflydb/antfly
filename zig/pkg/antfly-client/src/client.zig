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
const openapi = @import("antfly_client_openapi");
const httpx = @import("httpx");

const retrieval_agent_timeout_ms = 300_000;

pub const ApiError = struct {
    status_code: u16,
    message: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ApiError) void {
        self.allocator.free(self.message);
    }

    pub fn format(self: ApiError, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("API error {d}: {s}", .{ self.status_code, self.message });
    }
};

pub const AntflyClient = struct {
    inner: openapi.Client,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, http: *httpx.Client, base_url: []const u8) !AntflyClient {
        // Store the server root URL. The generated public client owns the
        // /db/v1, /auth/v1, and /ai/v1 route prefixes from the public contract.
        // The generated openapi.Client stores a pointer to this string,
        // so we must keep it alive until deinit.
        const url = try normalizeBaseUrl(allocator, base_url);

        const inner = openapi.Client.init(allocator, http, url);
        return .{
            .inner = inner,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *AntflyClient) void {
        self.allocator.free(self.inner.base_url);
        self.inner.deinit();
    }

    pub fn setBaseUrl(self: *AntflyClient, base_url: []const u8) !void {
        const url = try normalizeBaseUrl(self.allocator, base_url);
        self.allocator.free(self.inner.base_url);
        self.inner.base_url = url;
    }

    // --- Auth ---

    pub fn setBearer(self: *AntflyClient, token: []const u8) !void {
        try self.inner.setBearer(token);
    }

    pub fn setBasicAuth(self: *AntflyClient, username: []const u8, password: []const u8) !void {
        self.inner.freeAuth();
        const cred = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ username, password });
        defer self.allocator.free(cred);
        const encoded = try base64Encode(self.allocator, cred);
        const header_val = try std.fmt.allocPrint(self.allocator, "Basic {s}", .{encoded});
        defer self.allocator.free(encoded);
        self.inner.auth_header = .{ "Authorization", header_val };
    }

    pub fn setApiKey(self: *AntflyClient, key_id: []const u8, key_secret: []const u8) !void {
        self.inner.freeAuth();
        const cred = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ key_id, key_secret });
        defer self.allocator.free(cred);
        const encoded = try base64Encode(self.allocator, cred);
        const header_val = try std.fmt.allocPrint(self.allocator, "ApiKey {s}", .{encoded});
        defer self.allocator.free(encoded);
        self.inner.auth_header = .{ "Authorization", header_val };
    }

    // --- Table operations ---

    pub fn createTable(self: *AntflyClient, table_name: []const u8, body: openapi.types.CreateTableRequest) !void {
        var resp = try self.inner.createTable(table_name, body);
        defer resp.deinit();
        if (resp.status_code >= 300) return self.apiErrorFromResponse(&resp);
    }

    pub fn dropTable(self: *AntflyClient, table_name: []const u8) !void {
        var resp = try self.inner.dropTable(table_name);
        defer resp.deinit();
        if (resp.status_code >= 300) return self.apiErrorFromResponse(&resp);
    }

    pub fn getTable(self: *AntflyClient, table_name: []const u8) !openapi.ApiResponse(openapi.types.TableStatus) {
        var resp = try self.inner.getTable(table_name);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn listTables(self: *AntflyClient) !openapi.ApiResponse([]const openapi.types.TableStatus) {
        var resp = try self.inner.listTables(.{});
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // --- Index operations ---

    pub const IndexConfig = @TypeOf(@as(openapi.types.IndexStatus, undefined).config);
    pub const CreateIndexRequest = openapi.types.CreateIndexRequest;
    pub const CreatedIndex = openapi.types.CreatedIndex;

    pub fn createIndex(self: *AntflyClient, table_name: []const u8, index_name: []const u8, body: CreateIndexRequest) !openapi.ApiResponse(CreatedIndex) {
        var resp = try self.inner.createIndex(table_name, index_name, body);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn dropIndex(self: *AntflyClient, table_name: []const u8, index_name: []const u8) !void {
        var resp = try self.inner.dropIndex(table_name, index_name);
        defer resp.deinit();
        if (resp.status_code >= 300) return self.apiErrorFromResponse(&resp);
    }

    pub fn getIndex(self: *AntflyClient, table_name: []const u8, index_name: []const u8) !openapi.ApiResponse(openapi.types.IndexStatus) {
        var resp = try self.getIndexResponse(table_name, index_name);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    /// Returns the typed HTTP response without converting non-2xx statuses to
    /// `error.ApiError`. Long-running readiness commands use this to classify
    /// retryable status codes without weakening normal one-shot API behavior.
    pub fn getIndexResponse(self: *AntflyClient, table_name: []const u8, index_name: []const u8) !openapi.ApiResponse(openapi.types.IndexStatus) {
        return self.inner.getIndex(table_name, index_name);
    }

    /// Equivalent to `getIndexResponse`, but bounds the complete HTTP request.
    /// Polling callers should pass their remaining operation budget so a
    /// socket-level timeout cannot outlive the user-visible deadline.
    pub fn getIndexResponseWithTimeout(
        self: *AntflyClient,
        table_name: []const u8,
        index_name: []const u8,
        timeout_ms: u64,
    ) !openapi.ApiResponse(openapi.types.IndexStatus) {
        const encoded_table_name = try httpx.PercentEncoding.encode(self.allocator, table_name);
        defer self.allocator.free(encoded_table_name);
        const encoded_index_name = try httpx.PercentEncoding.encode(self.allocator, index_name);
        defer self.allocator.free(encoded_index_name);
        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/db/v1/tables/{s}/indexes/{s}",
            .{ self.inner.base_url, encoded_table_name, encoded_index_name },
        );
        defer self.allocator.free(url);
        const headers: ?[]const [2][]const u8 = if (self.inner.auth_header) |*header|
            @as(*const [1][2][]const u8, header)
        else
            null;
        var resp = try self.inner.http.get(url, .{
            .headers = headers,
            .timeout_ms = @max(timeout_ms, 1),
        });
        return openapi.ApiResponse(openapi.types.IndexStatus).fromResponse(self.allocator, &resp);
    }

    pub fn listIndexes(self: *AntflyClient, table_name: []const u8) !openapi.ApiResponse([]const openapi.types.IndexStatus) {
        var resp = try self.inner.listIndexes(table_name);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    /// Returns index statuses without converting non-2xx responses while
    /// bounding the complete request. This is intended for best-effort CLI
    /// diagnostics that must never dominate the latency of the real action.
    pub fn listIndexesResponseWithTimeout(
        self: *AntflyClient,
        table_name: []const u8,
        timeout_ms: u64,
    ) !openapi.ApiResponse([]const openapi.types.IndexStatus) {
        const encoded_table_name = try httpx.PercentEncoding.encode(self.allocator, table_name);
        defer self.allocator.free(encoded_table_name);
        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/db/v1/tables/{s}/indexes",
            .{ self.inner.base_url, encoded_table_name },
        );
        defer self.allocator.free(url);
        const headers: ?[]const [2][]const u8 = if (self.inner.auth_header) |*header|
            @as(*const [1][2][]const u8, header)
        else
            null;
        var resp = try self.inner.http.get(url, .{
            .headers = headers,
            .timeout_ms = @max(timeout_ms, 1),
        });
        return openapi.ApiResponse([]const openapi.types.IndexStatus).fromResponse(self.allocator, &resp);
    }

    // --- Query operations ---

    pub fn query(self: *AntflyClient, body: openapi.types.QueryRequest) !openapi.ApiResponse(openapi.types.QueryResponses) {
        var resp = try self.queryCanonicalPath("/db/v1/query", body);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn queryTable(self: *AntflyClient, table_name: []const u8, body: openapi.types.QueryRequest) !openapi.ApiResponse(openapi.types.QueryResponses) {
        const encoded_table_name = try httpx.PercentEncoding.encode(self.allocator, table_name);
        defer self.allocator.free(encoded_table_name);
        const path = try std.fmt.allocPrint(self.allocator, "/db/v1/tables/{s}/query", .{encoded_table_name});
        defer self.allocator.free(path);
        var resp = try self.queryCanonicalPath(path, body);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // The public stateful transport accepts a deprecated request/response
    // union, but new SDK entry points intentionally expose only the canonical
    // contract. Encoding and decoding the narrow models here makes that
    // boundary executable instead of relying on a lossy struct conversion.
    fn queryCanonicalPath(
        self: *AntflyClient,
        path: []const u8,
        body: openapi.types.QueryRequest,
    ) !openapi.ApiResponse(openapi.types.QueryResponses) {
        const url = try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.inner.base_url, path });
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringifyRequest(self.allocator, body);
        defer self.allocator.free(json_body);
        const headers: ?[]const [2][]const u8 = if (self.inner.auth_header) |*header|
            @as(*const [1][2][]const u8, header)
        else
            null;
        var response = try self.inner.http.post(url, .{ .json = json_body, .headers = headers });
        return openapi.ApiResponse(openapi.types.QueryResponses).fromResponse(self.allocator, &response);
    }

    pub fn lookupKey(self: *AntflyClient, table_name: []const u8, key: []const u8, params: openapi.client.LookupKeyParams) !openapi.ApiResponse(std.json.Value) {
        var resp = try self.inner.lookupKey(table_name, key, params);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // --- Artifact operations ---

    pub fn listDocumentArtifactManifests(
        self: *AntflyClient,
        table_name: []const u8,
        key: []const u8,
        params: openapi.client.ListDocumentArtifactManifestsParams,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactManifestList) {
        return try self.inner.listDocumentArtifactManifests(table_name, key, params);
    }

    pub fn getDocumentArtifactManifest(
        self: *AntflyClient,
        table_name: []const u8,
        key: []const u8,
        artifact_name: []const u8,
        params: openapi.client.GetDocumentArtifactManifestParams,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactManifest) {
        return try self.inner.getDocumentArtifactManifest(table_name, key, artifact_name, params);
    }

    pub fn listArtifactEnrichments(
        self: *AntflyClient,
        table_name: []const u8,
    ) !openapi.ApiResponse(openapi.types.TableArtifactEnrichmentList) {
        return try self.inner.listArtifactEnrichments(table_name);
    }

    pub fn putArtifactEnrichment(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
        body: openapi.types.EnrichmentConfig,
    ) !openapi.ApiResponse(std.json.Value) {
        return try self.inner.putArtifactEnrichment(table_name, artifact_name, body);
    }

    pub fn deleteArtifactEnrichment(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
    ) !openapi.ApiResponse(std.json.Value) {
        return try self.inner.deleteArtifactEnrichment(table_name, artifact_name);
    }

    pub fn reprocessDocumentArtifact(
        self: *AntflyClient,
        table_name: []const u8,
        key: []const u8,
        artifact_name: []const u8,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactReprocessResponse) {
        return try self.inner.reprocessDocumentArtifact(table_name, key, artifact_name);
    }

    pub fn reprocessDocumentArtifactRange(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
        body: openapi.types.DocumentArtifactTableReprocessRequest,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactTableReprocessResponse) {
        return try self.inner.reprocessDocumentArtifactRange(table_name, artifact_name, body);
    }

    pub fn startDocumentArtifactReprocessJob(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
        body: openapi.types.DocumentArtifactReprocessJobStartRequest,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactReprocessJob) {
        return try self.inner.startDocumentArtifactReprocessJob(table_name, artifact_name, body);
    }

    pub fn getDocumentArtifactReprocessJob(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
        job_id: []const u8,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactReprocessJob) {
        return try self.inner.getDocumentArtifactReprocessJob(table_name, artifact_name, job_id);
    }

    pub fn advanceDocumentArtifactReprocessJob(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
        job_id: []const u8,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactReprocessJob) {
        return try self.inner.advanceDocumentArtifactReprocessJob(table_name, artifact_name, job_id);
    }

    pub fn cancelDocumentArtifactReprocessJob(
        self: *AntflyClient,
        table_name: []const u8,
        artifact_name: []const u8,
        job_id: []const u8,
    ) !openapi.ApiResponse(openapi.types.DocumentArtifactReprocessJob) {
        return try self.inner.cancelDocumentArtifactReprocessJob(table_name, artifact_name, job_id);
    }

    // --- Batch operations ---

    pub fn batch(self: *AntflyClient, table_name: []const u8, body: openapi.types.BatchRequest) !openapi.ApiResponse(openapi.types.BatchResponse) {
        const url = try std.fmt.allocPrint(self.allocator, "{s}/db/v1/tables/{s}/batch", .{ self.inner.base_url, table_name });
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, BatchRequestWire{ .inner = body });
        defer self.allocator.free(json_body);
        const headers: ?[]const [2][]const u8 = if (self.inner.auth_header) |header| &.{header} else null;
        var raw_resp = try self.inner.http.post(url, .{ .json = json_body, .headers = headers });
        var resp = openapi.ApiResponse(openapi.types.BatchResponse).fromResponse(self.allocator, &raw_resp);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // --- Backup / Restore ---

    pub const RestoreOptions = struct {
        /// Stable key for safely retrying job creation after a timeout or lost
        /// response. Reusing a key with a different request returns conflict.
        idempotency_key: ?[]const u8 = null,
    };

    pub fn backupTable(self: *AntflyClient, table_name: []const u8, body: openapi.types.BackupRequest) !void {
        var resp = try self.inner.backupTable(table_name, body);
        defer resp.deinit();
        if (resp.status_code >= 300) return self.apiErrorFromResponse(&resp);
    }

    pub fn restoreTable(self: *AntflyClient, table_name: []const u8, body: openapi.types.RestoreRequest) !openapi.ApiResponse(openapi.types.RestoreJob) {
        return self.restoreTableWithOptions(table_name, body, .{});
    }

    pub fn restoreTableWithOptions(self: *AntflyClient, table_name: []const u8, body: openapi.types.RestoreRequest, options: RestoreOptions) !openapi.ApiResponse(openapi.types.RestoreJob) {
        var resp = try self.inner.restoreTable(table_name, body, options.idempotency_key);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn clusterBackup(self: *AntflyClient, body: openapi.types.ClusterBackupRequest) !openapi.ApiResponse(openapi.types.ClusterBackupResponse) {
        var resp = try self.inner.backup(body);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn clusterRestore(self: *AntflyClient, body: openapi.types.ClusterRestoreRequest) !openapi.ApiResponse(openapi.types.RestoreJob) {
        return self.clusterRestoreWithOptions(body, .{});
    }

    pub fn clusterRestoreWithOptions(self: *AntflyClient, body: openapi.types.ClusterRestoreRequest, options: RestoreOptions) !openapi.ApiResponse(openapi.types.RestoreJob) {
        var resp = try self.inner.restore(body, options.idempotency_key);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn getRestoreJob(self: *AntflyClient, job_id: []const u8) !openapi.ApiResponse(openapi.types.RestoreJob) {
        var resp = try self.getRestoreJobResponse(job_id);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    /// Returns the restore status response without converting non-success
    /// statuses so callers can honor the endpoint's retry contract.
    pub fn getRestoreJobResponse(self: *AntflyClient, job_id: []const u8) !openapi.ApiResponse(openapi.types.RestoreJob) {
        return self.inner.getRestoreJob(job_id);
    }

    pub fn cancelRestoreJob(self: *AntflyClient, job_id: []const u8) !openapi.ApiResponse(openapi.types.RestoreJob) {
        var resp = try self.inner.cancelRestoreJob(job_id);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    pub fn listBackups(self: *AntflyClient, params: openapi.client.ListBackupsParams) !openapi.ApiResponse(openapi.types.BackupListResponse) {
        var resp = try self.inner.listBackups(params);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // --- Agents ---

    pub fn retrievalAgent(self: *AntflyClient, body: openapi.types.RetrievalAgentRequest) !openapi.client.RawResponse {
        const url = try std.fmt.allocPrint(self.allocator, "{s}/db/v1/agents/retrieval", .{self.inner.base_url});
        defer self.allocator.free(url);
        const json_body = try httpx.json.Json.stringify(self.allocator, body);
        defer self.allocator.free(json_body);
        var auth_headers: ?[1][2][]const u8 = null;
        if (self.inner.auth_header) |h| auth_headers = .{h};

        var resp = try self.inner.http.post(url, .{
            .json = json_body,
            .headers = if (auth_headers) |*h| h[0..] else null,
            .timeout_ms = retrieval_agent_timeout_ms,
        });
        defer resp.deinit();
        return .{
            .status_code = resp.status.code,
            .body = if (resp.body) |b| (self.allocator.dupe(u8, b) catch null) else null,
            .content_type = if (resp.contentType()) |ct| (self.allocator.dupe(u8, ct) catch null) else null,
            .allocator = self.allocator,
        };
    }

    pub fn queryBuilder(self: *AntflyClient, body: openapi.types.QueryBuilderRequest) !openapi.ApiResponse(openapi.types.QueryBuilderResult) {
        var resp = try self.inner.queryBuilderAgent(body);
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // --- Internal / Admin ---

    pub fn getStatus(self: *AntflyClient) !openapi.ApiResponse(openapi.types.ClusterStatus) {
        var resp = try self.inner.getStatus();
        if (resp.status_code >= 300) {
            defer resp.deinit();
            return self.apiErrorFromResponse(&resp);
        }
        return resp;
    }

    // --- Helpers ---

    fn apiErrorFromResponse(self: *AntflyClient, resp: anytype) error{ ApiError, OutOfMemory } {
        _ = self;
        const msg = resp.err_body orelse "unknown error";
        std.debug.print("API error {d}: {s}\n", .{ resp.status_code, msg });
        return error.ApiError;
    }
};

pub fn normalizeBaseUrl(allocator: std.mem.Allocator, base_url: []const u8) ![]const u8 {
    const trimmed = trimRightSlash(base_url);
    if (std.mem.endsWith(u8, trimmed, "/db/v1")) {
        return try allocator.dupe(u8, trimmed[0 .. trimmed.len - "/db/v1".len]);
    }
    if (std.mem.endsWith(u8, trimmed, "/auth/v1")) {
        return try allocator.dupe(u8, trimmed[0 .. trimmed.len - "/auth/v1".len]);
    }
    if (std.mem.endsWith(u8, trimmed, "/ai/v1")) {
        return try allocator.dupe(u8, trimmed[0 .. trimmed.len - "/ai/v1".len]);
    }
    return try allocator.dupe(u8, trimmed);
}

const BatchRequestWire = struct {
    inner: openapi.types.BatchRequest,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.beginObject();
        if (self.inner.inserts) |inserts| {
            try jw.objectField("inserts");
            try jw.write(inserts);
        }
        if (self.inner.deletes) |deletes| {
            try jw.objectField("deletes");
            try jw.write(deletes);
        }
        if (self.inner.transforms) |transforms| {
            try jw.objectField("transforms");
            try jw.write(transforms);
        }
        if (self.inner.sync_level) |sync_level| {
            try jw.objectField("sync_level");
            try jw.write(sync_level);
        }
        try jw.endObject();
    }
};

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn base64Encode(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    const encoder = std.base64.standard.Encoder;
    const len = encoder.calcSize(input.len);
    const buf = try allocator.alloc(u8, len);
    _ = encoder.encode(buf, input);
    return buf;
}

test "AntflyClient compiles" {
    _ = AntflyClient;
    _ = ApiError;
}

test "normalizeBaseUrl accepts local and CloudAF URLs" {
    const alloc = std.testing.allocator;

    const local_root = try normalizeBaseUrl(alloc, "http://localhost:8080");
    defer alloc.free(local_root);
    try std.testing.expectEqualStrings("http://localhost:8080", local_root);

    const local_api = try normalizeBaseUrl(alloc, "http://localhost:8080/db/v1/");
    defer alloc.free(local_api);
    try std.testing.expectEqualStrings("http://localhost:8080", local_api);

    const local_auth = try normalizeBaseUrl(alloc, "http://localhost:8080/auth/v1");
    defer alloc.free(local_auth);
    try std.testing.expectEqualStrings("http://localhost:8080", local_auth);

    const local_ai = try normalizeBaseUrl(alloc, "http://localhost:8080/ai/v1");
    defer alloc.free(local_ai);
    try std.testing.expectEqualStrings("http://localhost:8080", local_ai);

    const cloud_root = try normalizeBaseUrl(alloc, "https://platform.antfly.io/cloud/v1/instance");
    defer alloc.free(cloud_root);
    try std.testing.expectEqualStrings("https://platform.antfly.io/cloud/v1/instance", cloud_root);

    const cloud_api = try normalizeBaseUrl(alloc, "https://platform.antfly.io/cloud/v1/instance/db/v1");
    defer alloc.free(cloud_api);
    try std.testing.expectEqualStrings("https://platform.antfly.io/cloud/v1/instance", cloud_api);
}
