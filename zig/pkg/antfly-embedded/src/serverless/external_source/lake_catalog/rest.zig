// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Iceberg REST client. The external service writes metadata. Requests never
//! follow redirects or replay automatically; a remote intent fences retries.
const std = @import("std");
const httpx = @import("httpx");
const storage = @import("objectstore");
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const A = std.mem.Allocator;
const V = std.json.Value;

pub const Response = struct {
    status: u16,
    body: []u8,
    pub fn deinit(self: *Response, a: A) void {
        a.free(self.body);
        self.* = undefined;
    }
};
pub const Transport = struct {
    ptr: *anyopaque,
    request_fn: *const fn (*anyopaque, A, httpx.Method, []const u8, ?[]const u8, ?[]const u8, types.Context) anyerror!Response,
    pub fn request(self: Transport, a: A, method: httpx.Method, uri: []const u8, body: ?[]const u8, id: ?[]const u8, context: types.Context) !Response {
        try context.ensureActive();
        return self.request_fn(self.ptr, a, method, uri, body, id, context);
    }
};
pub const HttpTransport = struct {
    client: *httpx.Client,
    /// Credentials resolved from a named host-policy connection for this call.
    headers: []const [2][]const u8 = &.{},
    pub fn transport(self: *HttpTransport) Transport {
        return .{ .ptr = self, .request_fn = request };
    }
    fn canceled(raw: *const anyopaque) bool {
        const context: *const types.Context = @ptrCast(@alignCast(raw));
        context.ensureActive() catch return true;
        return false;
    }
    fn request(raw: *anyopaque, a: A, method: httpx.Method, uri: []const u8, body: ?[]const u8, id: ?[]const u8, context: types.Context) !Response {
        const self: *HttpTransport = @ptrCast(@alignCast(raw));
        var headers: std.ArrayList([2][]const u8) = .empty;
        defer headers.deinit(a);
        try headers.appendSlice(a, self.headers);
        try headers.append(a, .{ "Accept", "application/json" });
        if (body != null) try headers.append(a, .{ "Content-Type", "application/json" });
        if (id) |value| try headers.append(a, .{ "Idempotency-Key", value });
        const timeout_ms: u64 = if (context.deadline_ns) |deadline| @min(@as(u64, 30_000), (deadline -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms) else 30_000;
        if (timeout_ms == 0) return error.DeadlineExceeded;
        var response = self.client.request(method, uri, .{ .headers = headers.items, .borrowed_body = body, .follow_redirects = false, .max_retries = 0, .cookies_enabled = false, .max_response_size = types.max_metadata_bytes, .timeout_ms = timeout_ms, .cancellation = .{ .ptr = &context, .is_cancelled_fn = canceled } }) catch |err| {
            try context.ensureActive();
            return err;
        };
        defer response.deinit();
        return .{ .status = response.status.code, .body = try a.dupe(u8, response.body orelse "") };
    }
};

pub const Journal = struct {
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
};
const Intent = struct {
    format: u8 = 1,
    id: []const u8,
    request_hash: []const u8,
    body: []const u8,
    created_ms: i64,
    endpoint: ?[]const u8 = null,
    operation: enum { create, commit },
};
pub const Rest = struct {
    config: types.Config,
    transport: Transport,
    context: types.Context = .{},
    journal: ?Journal = null,
    /// Wall clock supplied by host for expiry checks. Monotonic context handles
    /// request deadlines; a restart never renews the remote idempotency window.
    now_ms: i64 = 0,

    const Negotiated = struct {
        arena: std.heap.ArenaAllocator,
        prefix: []const u8,
        endpoints: ?[]const V,
        idempotency_ms: ?u64,
        fn deinit(self: *Negotiated) void {
            self.arena.deinit();
        }
        fn supports(self: Negotiated, resource: []const u8) !bool {
            const endpoints = self.endpoints orelse return true;
            for (endpoints) |value| if (std.mem.eql(u8, try metadata.str(value), resource)) return true;
            return false;
        }
    };
    fn negotiate(self: *const Rest, a: A) !Negotiated {
        try self.config.validate();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        const uri = if (self.config.warehouse) |warehouse| try std.fmt.allocPrint(scratch, "{s}/v1/config?warehouse={s}", .{ std.mem.trimEnd(u8, self.config.uri.?, "/"), try encode(scratch, warehouse) }) else try std.fmt.allocPrint(scratch, "{s}/v1/config", .{std.mem.trimEnd(u8, self.config.uri.?, "/")});
        var response = try self.transport.request(a, .GET, uri, null, null, self.context);
        defer response.deinit(a);
        try status(response.status, false);
        const root = try metadata.parse(scratch, response.body);
        if (root != .object) return error.InvalidLakeCatalog;
        var prefix: []const u8 = "";
        inline for (.{ "defaults", "overrides" }) |key| {
            if (root.object.get(key)) |settings| {
                if (settings != .object) return error.InvalidLakeCatalog;
                if (settings.object.get("prefix")) |value| prefix = switch (value) {
                    .string => |string| string,
                    else => return error.InvalidLakeCatalog,
                };
            }
        }
        const endpoints: ?[]const V = if (root.object.get("endpoints")) |value| blk: {
            if (value != .array) return error.InvalidLakeCatalog;
            break :blk value.array.items;
        } else null;
        const lifetime = if (root.object.get("idempotency-key-lifetime")) |value| try durationMs(try metadata.str(value)) else null;
        return .{ .arena = arena, .prefix = prefix, .endpoints = endpoints, .idempotency_ms = lifetime };
    }
    fn endpoint(self: Rest, a: A, n: Negotiated, creating: bool) ![]u8 {
        var namespace: std.ArrayList(u8) = .empty;
        defer namespace.deinit(a);
        for (self.config.namespace, 0..) |part, i| {
            if (i != 0) try namespace.append(a, 0x1f);
            try namespace.appendSlice(a, part);
        }
        const ns = try encode(a, namespace.items);
        defer a.free(ns);
        const prefix = try encode(a, n.prefix);
        defer a.free(prefix);
        const name = try encode(a, self.config.name.?);
        defer a.free(name);
        const base = std.mem.trimEnd(u8, self.config.uri.?, "/");
        const path = if (prefix.len == 0) "" else "/";
        return if (creating) std.fmt.allocPrint(a, "{s}/v1{s}{s}/namespaces/{s}/tables", .{ base, path, prefix, ns }) else std.fmt.allocPrint(a, "{s}/v1{s}{s}/namespaces/{s}/tables/{s}", .{ base, path, prefix, ns, name });
    }
    pub fn load(self: *const Rest, a: A) !types.Table {
        var n = try self.negotiate(a);
        defer n.deinit();
        if (!try n.supports("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}")) return error.UnsupportedLakeCatalogCapability;
        const uri = try self.endpoint(a, n, false);
        defer a.free(uri);
        var response = try self.transport.request(a, .GET, uri, null, null, self.context);
        defer response.deinit(a);
        try status(response.status, false);
        return tableResponse(a, response.body);
    }
    pub fn create(self: *const Rest, a: A, id: []const u8, request: []const u8, timestamp: i64) !types.Table {
        const c: types.Commit = .{ .id = id, .body = request, .timestamp_ms = timestamp, .expected_metadata_location = "<create>" };
        try c.validate();
        return self.mutate(a, c, .create);
    }
    pub fn commit(self: *const Rest, a: A, c: types.Commit) !types.Table {
        try c.validate();
        return self.mutate(a, c, .commit);
    }
    fn mutate(self: *const Rest, a: A, c: types.Commit, operation: @FieldType(Intent, "operation")) !types.Table {
        const journal = self.journal orelse return error.LakeCommitJournalRequired;
        var n = try self.negotiate(a);
        defer n.deinit();
        const endpoint_name = if (operation == .create) "POST /v1/{prefix}/namespaces/{namespace}/tables" else "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}";
        if (!try n.supports(endpoint_name)) return error.UnsupportedLakeCatalogCapability;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const uri = try self.endpoint(scratch, n, operation == .create);
        const intent_key = try journalKey(scratch, journal, "intents", c.id);
        const receipt_key = try journalKey(scratch, journal, "receipts", c.id);
        var client = journal.client;
        client.allocator = a;
        const cancellation = self.token();
        var existing = client.getObject(journal.bucket, intent_key, .{ .max_response_bytes = types.max_commit_bytes * 2, .cancellation = cancellation }) catch |err| switch (err) {
            error.FileNotFound, error.ObjectNotFound => null,
            else => return err,
        };
        const replay = existing != null;
        defer if (existing) |*value| value.deinit(a);
        var intent: Intent = undefined;
        if (existing) |value| {
            intent = (try std.json.parseFromSlice(Intent, scratch, value.body, .{ .allocate = .alloc_always })).value;
            if (intent.format != 1 or intent.operation != operation or !std.mem.eql(u8, intent.id, c.id) or !std.mem.eql(u8, intent.request_hash, &types.commitHash(c))) return error.LakeCommitIdReused;
            if (try getReceipt(a, &client, journal.bucket, receipt_key, cancellation)) |table| return table;
            const outcome = try self.resolve(a, c.id, &types.commitHash(c));
            if (outcome == .committed) return self.load(a);
            if (outcome == .not_committed) return error.LakeCommitConflict;
            if (intent.endpoint == null or !std.mem.eql(u8, intent.endpoint.?, uri)) return error.LakeCommitOutcomeUnknown;
            const lifetime = n.idempotency_ms orelse return error.LakeCommitOutcomeUnknown;
            if (self.now_ms < intent.created_ms or @as(u64, @intCast(self.now_ms - intent.created_ms)) >= lifetime) return error.LakeCommitOutcomeUnknown;
        } else {
            var guarded = c;
            if (operation == .commit) {
                var table = try self.load(a);
                defer table.deinit(a);
                if (!std.mem.eql(u8, table.metadata_location, c.expected_metadata_location)) return error.LakeCommitConflict;
                const current = try metadata.parse(scratch, table.metadata_json);
                const request = try metadata.parse(scratch, c.body);
                try metadata.requirements(scratch, current, request);
                guarded.body = try guardedRequest(scratch, current, request);
            }
            const body = try markedRequest(scratch, guarded, operation, self.config.name.?);
            // Commit markers identify the caller payload, not the generated guards.
            var marked = try metadata.parse(scratch, body);
            const original_hash = types.commitHash(c);
            const generated = marked.object.getPtr("updates");
            if (generated) |list| {
                for (list.array.items) |*update| {
                    if (update.object.getPtr("updates")) |props| {
                        if (props.* == .object and props.object.contains("antfly.commit-hash")) try props.object.put(scratch, "antfly.commit-hash", .{ .string = try scratch.dupe(u8, &original_hash) });
                    }
                    if (update.object.getPtr("snapshot")) |snapshot| {
                        if (snapshot.object.getPtr("summary")) |summary| try summary.object.put(scratch, "antfly.commit-hash", .{ .string = try scratch.dupe(u8, &original_hash) });
                    }
                }
            }
            const persisted_body = try std.json.Stringify.valueAlloc(scratch, marked, .{});
            intent = .{ .id = c.id, .request_hash = &types.commitHash(c), .body = persisted_body, .created_ms = self.now_ms, .endpoint = uri, .operation = operation };
            const bytes = try std.json.Stringify.valueAlloc(scratch, intent, .{});
            var written = client.putObject(journal.bucket, intent_key, bytes, .{ .if_none_match = true, .content_type = "application/json", .cancellation = cancellation }) catch |err| {
                if (err == error.PreconditionFailed) return error.LakeCommitInProgress;
                return error.LakeCommitOutcomeUnknown;
            };
            written.deinit(a);
        }
        // This ID is unique to the table, operation, and immutable request, and
        // is reused only within the server-advertised retention window.
        const scoped_key = types.digestHex(try std.fmt.allocPrint(scratch, "{s}:{s}", .{ uri, intent.request_hash }));
        const idempotency: ?[]const u8 = if (n.idempotency_ms != null) &scoped_key else null;
        var response = self.transport.request(a, .POST, uri, intent.body, idempotency, self.context) catch {
            try self.context.ensureActive();
            if (try self.resolve(a, c.id, &types.commitHash(c)) == .committed) return self.load(a);
            return error.LakeCommitOutcomeUnknown;
        };
        defer response.deinit(a);
        if (response.status != 200) {
            if (try self.resolve(a, c.id, &types.commitHash(c)) == .committed) return self.load(a);
            if (replay) return error.LakeCommitOutcomeUnknown;
            if (response.status == 409) {
                // Only an original, definitive rejection proves non-commit.
                // Persist that proof before exposing a rebase-safe conflict.
                const key = try journalKey(scratch, journal, "rejections", c.id);
                const bytes = try std.json.Stringify.valueAlloc(scratch, Rejection{ .id = c.id, .request_hash = &types.commitHash(c) }, .{});
                var rejected = client.putObject(journal.bucket, key, bytes, .{ .if_none_match = true, .content_type = "application/json", .cancellation = cancellation }) catch return error.LakeCommitOutcomeUnknown;
                rejected.deinit(a);
            }
            try status(response.status, true);
        }
        var table = tableResponse(a, response.body) catch return error.LakeCommitOutcomeUnknown;
        errdefer table.deinit(a);
        const confirmed = try metadata.parse(scratch, table.metadata_json);
        if (!try marker(try metadata.get(confirmed, "properties"), c.id, &types.commitHash(c))) return error.LakeCommitOutcomeUnknown;
        // Receipt content binds to the request; the immutable intent remains
        // available if the caller never receives this acknowledgement.
        var receipt = client.putObject(journal.bucket, receipt_key, response.body, .{ .if_none_match = true, .content_type = "application/json", .cancellation = cancellation }) catch |err| {
            if (err != error.PreconditionFailed) return error.LakeCommitOutcomeUnknown;
            var prior = (try getReceipt(a, &client, journal.bucket, receipt_key, cancellation)) orelse return error.LakeCommitOutcomeUnknown;
            prior.deinit(a);
            return table;
        };
        receipt.deinit(a);
        return table;
    }
    fn token(self: *const Rest) storage.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled };
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Rest = @ptrCast(@alignCast(raw));
        self.context.ensureActive() catch return true;
        return false;
    }
    pub fn resolve(self: *const Rest, a: A, id: []const u8, hash: []const u8) !types.Outcome {
        if (self.journal) |journal| {
            const intent_key = try journalKey(a, journal, "intents", id);
            defer a.free(intent_key);
            var client = journal.client;
            client.allocator = a;
            var stored = client.getObject(journal.bucket, intent_key, .{ .max_response_bytes = types.max_commit_bytes * 2, .cancellation = self.token() }) catch |err| switch (err) {
                error.FileNotFound, error.ObjectNotFound => null,
                else => return err,
            };
            defer if (stored) |*value| value.deinit(a);
            if (stored) |value| {
                var parsed = try std.json.parseFromSlice(Intent, a, value.body, .{});
                defer parsed.deinit();
                if (!std.mem.eql(u8, parsed.value.id, id) or !std.mem.eql(u8, parsed.value.request_hash, hash)) return error.LakeCommitIdReused;
                const key = try journalKey(a, journal, "receipts", id);
                defer a.free(key);
                if (try getReceipt(a, &client, journal.bucket, key, self.token())) |receipt_value| {
                    var receipt = receipt_value;
                    defer receipt.deinit(a);
                    return .committed;
                }
                const rejected_key = try journalKey(a, journal, "rejections", id);
                defer a.free(rejected_key);
                var rejected = client.getObject(journal.bucket, rejected_key, .{ .max_response_bytes = 4096, .cancellation = self.token() }) catch |err| switch (err) {
                    error.FileNotFound, error.ObjectNotFound => null,
                    else => return err,
                };
                defer if (rejected) |*proof| proof.deinit(a);
                if (rejected) |proof| {
                    var rejection = try std.json.parseFromSlice(Rejection, a, proof.body, .{});
                    defer rejection.deinit();
                    if (!std.mem.eql(u8, rejection.value.id, id) or !std.mem.eql(u8, rejection.value.request_hash, hash)) return error.LakeCommitIdReused;
                    return .not_committed;
                }
            }
        }
        var table = self.load(a) catch |err| switch (err) {
            error.LakeTableNotFound => return .unknown,
            else => return err,
        };
        defer table.deinit(a);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const root = try metadata.parse(arena.allocator(), table.metadata_json);
        if (root.object.get("properties")) |properties| if (try marker(properties, id, hash)) return .committed;
        if (root.object.get("snapshots")) |snapshots| {
            if (snapshots != .array) return error.InvalidLakeMetadata;
            for (snapshots.array.items) |snapshot| if (snapshot.object.get("summary")) |summary| if (try marker(summary, id, hash)) return .committed;
        }
        // Absence is not failure: another writer may have advanced metadata,
        // expired snapshots, or removed properties after our successful commit.
        return .unknown;
    }
};

const Rejection = struct { id: []const u8, request_hash: []const u8 };

fn marker(value: V, id: []const u8, hash: []const u8) !bool {
    if (value != .object) return error.InvalidLakeMetadata;
    const commit_id = value.object.get("antfly.commit-id") orelse return false;
    if (!std.mem.eql(u8, try metadata.str(commit_id), id)) return false;
    const digest = value.object.get("antfly.commit-hash") orelse return error.InvalidLakeMetadata;
    if (!std.mem.eql(u8, try metadata.str(digest), hash)) return error.LakeCommitIdReused;
    return true;
}
fn getReceipt(a: A, client: *storage.Client, bucket: []const u8, key: []const u8, token: storage.CancellationToken) !?types.Table {
    var value = client.getObject(bucket, key, .{ .max_response_bytes = types.max_metadata_bytes, .cancellation = token }) catch |err| switch (err) {
        error.FileNotFound, error.ObjectNotFound => return null,
        else => return err,
    };
    defer value.deinit(a);
    return try tableResponse(a, value.body);
}
fn journalKey(a: A, journal: Journal, kind: []const u8, id: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/lake-rest/{s}/{s}.json", .{ std.mem.trimEnd(u8, journal.prefix, "/"), kind, types.digestHex(id) });
}
fn tableResponse(a: A, bytes: []const u8) !types.Table {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const root = try metadata.parse(arena.allocator(), bytes);
    const location = try a.dupe(u8, try metadata.str(try metadata.get(root, "metadata-location")));
    errdefer a.free(location);
    const meta = try metadata.get(root, "metadata");
    if (meta != .object) return error.InvalidLakeMetadata;
    _ = try metadata.str(try metadata.get(meta, "table-uuid"));
    const version = try metadata.int(try metadata.get(meta, "format-version"));
    if (version != 1 and version != 2) return error.UnsupportedLakeFormatVersion;
    return .{ .metadata_location = location, .metadata_json = try std.json.Stringify.valueAlloc(a, meta, .{}) };
}
// Pin the read used to prepare the commit at the external authority. Caller
// requirements alone need not cover every metadata update they submit.
fn guardedRequest(a: A, current: V, request_value: V) ![]u8 {
    var request = request_value;
    const list = request.object.getPtr("requirements") orelse return error.InvalidLakeCommit;
    if (list.* != .array) return error.InvalidLakeCommit;
    inline for (.{ .{ "assert-table-uuid", "uuid", "table-uuid" }, .{ "assert-current-schema-id", "current-schema-id", "current-schema-id" }, .{ "assert-last-assigned-field-id", "last-assigned-field-id", "last-column-id" }, .{ "assert-last-assigned-partition-id", "last-assigned-partition-id", "last-partition-id" }, .{ "assert-default-spec-id", "default-spec-id", "default-spec-id" }, .{ "assert-default-sort-order-id", "default-sort-order-id", "default-sort-order-id" } }) |fields| {
        var guard: V = .{ .object = .empty };
        try guard.object.put(a, "type", .{ .string = fields[0] });
        try guard.object.put(a, fields[1], try metadata.get(current, fields[2]));
        try list.array.append(guard);
    }
    var guard: V = .{ .object = .empty };
    try guard.object.put(a, "type", .{ .string = "assert-ref-snapshot-id" });
    try guard.object.put(a, "ref", .{ .string = "main" });
    try guard.object.put(a, "snapshot-id", current.object.get("current-snapshot-id") orelse .null);
    try list.array.append(guard);
    return std.json.Stringify.valueAlloc(a, request, .{});
}
fn markedRequest(a: A, c: types.Commit, operation: @FieldType(Intent, "operation"), name: []const u8) ![]u8 {
    var root = try metadata.parse(a, c.body);
    if (root != .object) return error.InvalidLakeCommit;
    const hash = types.commitHash(c);
    if (operation == .create) {
        try root.object.put(a, "name", .{ .string = name });
        try root.object.put(a, "stage-create", .{ .bool = false });
        var properties = root.object.get("properties") orelse V{ .object = .empty };
        if (properties != .object) return error.InvalidLakeCommit;
        try properties.object.put(a, "antfly.commit-id", .{ .string = c.id });
        try properties.object.put(a, "antfly.commit-hash", .{ .string = try a.dupe(u8, &hash) });
        try root.object.put(a, "properties", properties);
    } else {
        const updates = root.object.getPtr("updates") orelse return error.InvalidLakeCommit;
        if (updates.* != .array) return error.InvalidLakeCommit;
        for (updates.array.items) |*update| {
            if (std.mem.eql(u8, try metadata.str(try metadata.get(update.*, "action")), "add-snapshot")) {
                const snapshot = update.object.getPtr("snapshot") orelse return error.InvalidLakeCommit;
                if (snapshot.* != .object) return error.InvalidLakeCommit;
                var summary = snapshot.object.get("summary") orelse V{ .object = .empty };
                if (summary != .object) return error.InvalidLakeCommit;
                try summary.object.put(a, "antfly.commit-id", .{ .string = c.id });
                try summary.object.put(a, "antfly.commit-hash", .{ .string = try a.dupe(u8, &hash) });
                try snapshot.object.put(a, "summary", summary);
            }
        }
        const tagged = try std.fmt.allocPrint(a, "{{\"action\":\"set-properties\",\"updates\":{{}}}}", .{});
        var update = try metadata.parse(a, tagged);
        const props = update.object.getPtr("updates").?;
        try props.object.put(a, "antfly.commit-id", .{ .string = c.id });
        try props.object.put(a, "antfly.commit-hash", .{ .string = try a.dupe(u8, &hash) });
        try updates.array.append(update);
    }
    return std.json.Stringify.valueAlloc(a, root, .{});
}
fn status(code: u16, mutation: bool) !void {
    return switch (code) {
        200 => {},
        401, 403 => error.LakeCatalogForbidden,
        404 => error.LakeTableNotFound,
        409 => error.LakeCommitConflict,
        400, 422 => error.InvalidLakeCommit,
        429, 500, 502, 503, 504 => if (mutation) error.LakeCommitOutcomeUnknown else error.LakeCatalogUnavailable,
        else => if (mutation) error.LakeCommitOutcomeUnknown else error.InvalidLakeCatalog,
    };
}
pub fn encode(a: A, value: []const u8) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(a);
    for (value) |byte| {
        if (std.ascii.isAlphanumeric(byte) or std.mem.indexOfScalar(u8, "-._~", byte) != null) {
            try result.append(a, byte);
        } else {
            const hex = "0123456789ABCDEF";
            try result.appendSlice(a, &.{ '%', hex[byte >> 4], hex[byte & 15] });
        }
    }
    return result.toOwnedSlice(a);
}
fn durationMs(value: []const u8) !u64 {
    // ISO-8601 fixed durations; calendar months/years are not retry windows.
    if (!std.mem.startsWith(u8, value, "P")) return error.InvalidLakeCatalog;
    var total: u64 = 0;
    var start: usize = 1;
    var time = false;
    for (value[1..], 1..) |byte, i| {
        if (byte == 'T') {
            if (i != start or time) return error.InvalidLakeCatalog;
            time = true;
            start = i + 1;
            continue;
        }
        if (std.ascii.isDigit(byte)) continue;
        const n = std.fmt.parseUnsigned(u64, value[start..i], 10) catch return error.InvalidLakeCatalog;
        const multiplier: u64 = switch (byte) {
            'D' => if (!time) 86_400_000 else return error.InvalidLakeCatalog,
            'H' => if (time) 3_600_000 else return error.InvalidLakeCatalog,
            'M' => if (time) 60_000 else return error.InvalidLakeCatalog,
            'S' => if (time) 1000 else return error.InvalidLakeCatalog,
            else => return error.InvalidLakeCatalog,
        };
        total = std.math.add(u64, total, std.math.mul(u64, n, multiplier) catch return error.InvalidLakeCatalog) catch return error.InvalidLakeCatalog;
        start = i + 1;
    }
    if (start != value.len or total == 0) return error.InvalidLakeCatalog;
    return total;
}
