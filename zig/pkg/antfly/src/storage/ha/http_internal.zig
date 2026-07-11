// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HTTP adapter for the hot-standby internal replication protocol.
//!
//! The durable replication protocol lives in `replication_api.zig`. This adapter
//! binds that transport-agnostic contract to the `/internal/v1/ha/replication`
//! routes used for primary-to-standby traffic inside a trusted deployment.

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_sync = @import("antfly_platform").sync;
const http_common = @import("../../common/http/http_common.zig");
const internal_api = @import("../../internal/mod.zig");
const primary_mod = @import("primary.zig");
const replication_api = @import("replication_api.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Server = struct {
    alloc: Allocator,
    primary: ?*primary_mod.Primary = null,
    state_mutex: ?*std.atomic.Mutex = null,

    pub const Options = struct {
        state_mutex: ?*std.atomic.Mutex = null,
    };

    pub fn init(alloc: Allocator, primary: ?*primary_mod.Primary) Server {
        return initWithOptions(alloc, primary, .{});
    }

    pub fn initWithOptions(alloc: Allocator, primary: ?*primary_mod.Primary, options: Options) Server {
        return .{
            .alloc = alloc,
            .primary = primary,
            .state_mutex = options.state_mutex,
        };
    }

    pub fn executor(self: *Server) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn handle(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (self.state_mutex) |mutex| {
            platform_sync.lockYielding(mutex);
            defer mutex.unlock();
        }
        const path = requestPath(req.uri);
        switch (req.method) {
            .GET => {
                if (std.mem.eql(u8, path, internal_api.routes.ha_replication_identify)) {
                    return try self.handleIdentify();
                }
                if (knownFixedRoute(path)) return try textResponse(self.alloc, 405, "method not allowed");
                return try textResponse(self.alloc, 404, "not found");
            },
            .POST => {
                if (std.mem.eql(u8, path, internal_api.routes.ha_replication_slots)) {
                    return try self.handleCreateReplicationSlot(req);
                }
                if (std.mem.eql(u8, path, internal_api.routes.ha_replication_start)) {
                    return try self.handleStartReplication(req);
                }
                if (std.mem.eql(u8, path, internal_api.routes.ha_replication_status)) {
                    return try self.handleStandbyStatusUpdate(req);
                }
                if (knownFixedRoute(path)) return try textResponse(self.alloc, 405, "method not allowed");
                return try textResponse(self.alloc, 404, "not found");
            },
            .PUT, .DELETE => {
                if (knownFixedRoute(path)) return try textResponse(self.alloc, 405, "method not allowed");
                return try textResponse(self.alloc, 404, "not found");
            },
        }
    }

    fn handleIdentify(self: *Server) !http_common.HttpResponse {
        const primary = self.primary orelse return try textResponse(self.alloc, 409, "primary unavailable");
        return try jsonResponse(self.alloc, replication_api.identifySystem(primary));
    }

    fn handleCreateReplicationSlot(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.primary orelse return try textResponse(self.alloc, 409, "primary unavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA replication slot request");
        var parsed = internal_api.openapi.server.parseCreateHAReplicationStreamingSlotBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA replication slot request");
        defer parsed.deinit();

        const initial_lsn = if (parsed.value.initial_lsn) |value|
            uint64FromJson(value) catch return try textResponse(self.alloc, 400, "invalid HA replication slot request")
        else
            null;

        const response = replication_api.createReplicationSlot(primary, .{
            .slot_name = parsed.value.slot_name,
            .initial_lsn = initial_lsn,
        }) catch |err| return try commandErrorResponse(self.alloc, err);
        return try jsonResponse(self.alloc, response);
    }

    fn handleStartReplication(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.primary orelse return try textResponse(self.alloc, 409, "primary unavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA start replication request");
        var parsed = internal_api.openapi.server.parseStartHAReplicationBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA start replication request");
        defer parsed.deinit();

        const from_lsn = positiveUint64FromJson(parsed.value.from_lsn) catch
            return try textResponse(self.alloc, 400, "invalid HA start replication request");
        const max_records = if (parsed.value.max_records) |value|
            usizeFromJson(value) catch return try textResponse(self.alloc, 400, "invalid HA start replication request")
        else
            0;
        const max_encoded_bytes = if (parsed.value.max_encoded_bytes) |value|
            usizeFromJson(value) catch return try textResponse(self.alloc, 400, "invalid HA start replication request")
        else
            0;

        var response = replication_api.startReplication(self.alloc, primary, .{
            .slot_name = parsed.value.slot_name,
            .from_lsn = from_lsn,
            .max_records = max_records,
            .max_encoded_bytes = max_encoded_bytes,
        }) catch |err| return try commandErrorResponse(self.alloc, err);
        defer response.deinit(self.alloc);

        var document = try startReplicationDocument(self.alloc, response);
        defer document.deinit(self.alloc);
        return try jsonResponse(self.alloc, document);
    }

    fn handleStandbyStatusUpdate(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.primary orelse return try textResponse(self.alloc, 409, "primary unavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA standby status update request");
        var parsed = internal_api.openapi.server.parseUpdateHAStandbyStatusBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA standby status update request");
        defer parsed.deinit();

        const timeline_id = positiveUint64FromJson(parsed.value.timeline_id) catch
            return try textResponse(self.alloc, 400, "invalid HA standby status update request");
        const received_lsn = uint64FromJson(parsed.value.received_lsn) catch
            return try textResponse(self.alloc, 400, "invalid HA standby status update request");
        const applied_lsn = uint64FromJson(parsed.value.applied_lsn) catch
            return try textResponse(self.alloc, 400, "invalid HA standby status update request");
        const safe_read_lsn = if (parsed.value.safe_read_lsn) |value|
            uint64FromJson(value) catch return try textResponse(self.alloc, 400, "invalid HA standby status update request")
        else
            null;

        const response = replication_api.standbyStatusUpdate(primary, .{
            .slot_name = parsed.value.slot_name,
            .timeline_id = timeline_id,
            .received_lsn = received_lsn,
            .applied_lsn = applied_lsn,
            .safe_read_lsn = safe_read_lsn,
        }) catch |err| return try commandErrorResponse(self.alloc, err);
        return try jsonResponse(self.alloc, response);
    }

    fn execute(ptr: *anyopaque, _: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *Server = @ptrCast(@alignCast(ptr));
        return self.handle(req);
    }
};

const StartReplicationDocument = struct {
    slot_name: []const u8,
    identity: standby_mod.Identity,
    record_format_version: u16,
    timeline_id: u64,
    from_lsn: u64,
    current_lsn: u64,
    last_sent_lsn: u64,
    next_lsn: u64,
    end_of_wal: bool,
    encoded_bytes: usize,
    records: []ReplicationFrameDocument,

    fn deinit(self: *StartReplicationDocument, alloc: Allocator) void {
        for (self.records) |*record| record.deinit(alloc);
        alloc.free(self.records);
        self.* = undefined;
    }
};

const ReplicationFrameDocument = struct {
    lsn: u64,
    kind: []const u8,
    payload_codec: []const u8,
    encoded: []const u8,

    fn deinit(self: *ReplicationFrameDocument, alloc: Allocator) void {
        alloc.free(self.encoded);
        self.* = undefined;
    }
};

fn startReplicationDocument(alloc: Allocator, response: replication_api.StartReplicationResponse) !StartReplicationDocument {
    const records = try alloc.alloc(ReplicationFrameDocument, response.records.len);
    errdefer alloc.free(records);
    var filled: usize = 0;
    errdefer for (records[0..filled]) |*record| record.deinit(alloc);

    for (response.records, 0..) |record, idx| {
        const encoded = try base64Alloc(alloc, record.encoded);
        errdefer alloc.free(encoded);
        records[idx] = .{
            .lsn = record.lsn,
            .kind = recordKindName(record.kind),
            .payload_codec = payloadCodecName(record.payload_codec),
            .encoded = encoded,
        };
        filled += 1;
    }

    return .{
        .slot_name = response.slot_name,
        .identity = response.identity,
        .record_format_version = response.record_format_version,
        .timeline_id = response.timeline_id,
        .from_lsn = response.from_lsn,
        .current_lsn = response.current_lsn,
        .last_sent_lsn = response.last_sent_lsn,
        .next_lsn = response.next_lsn,
        .end_of_wal = response.end_of_wal,
        .encoded_bytes = response.encoded_bytes,
        .records = records,
    };
}

fn recordKindName(kind: replication_record.RecordKind) []const u8 {
    return switch (kind) {
        .batch_mutation => "batch-mutation",
        .metadata_mutation => "metadata-mutation",
        .derived_effect => "derived-effect",
        .backup_start => "backup-start",
        .backup_end => "backup-end",
        .checkpoint => "checkpoint",
        .manifest => "manifest",
        .truncate => "truncate",
        .timeline_switch => "timeline-switch",
        else => "unknown",
    };
}

fn payloadCodecName(codec: replication_record.PayloadCodec) []const u8 {
    return switch (codec) {
        .raw => "raw",
        .json => "json",
        .binary => "binary",
        else => "unknown",
    };
}

fn requestPath(uri: []const u8) []const u8 {
    const path_with_query = if (std.mem.indexOf(u8, uri, "://")) |scheme_index| blk: {
        const authority_start = scheme_index + 3;
        const path_index = std.mem.indexOfScalarPos(u8, uri, authority_start, '/') orelse return "/";
        break :blk uri[path_index..];
    } else uri;
    const query_index = std.mem.indexOfScalar(u8, path_with_query, '?') orelse return path_with_query;
    return path_with_query[0..query_index];
}

fn knownFixedRoute(path: []const u8) bool {
    return std.mem.eql(u8, path, internal_api.routes.ha_replication_identify) or
        std.mem.eql(u8, path, internal_api.routes.ha_replication_slots) or
        std.mem.eql(u8, path, internal_api.routes.ha_replication_start) or
        std.mem.eql(u8, path, internal_api.routes.ha_replication_status);
}

fn uint64FromJson(value: i64) !u64 {
    if (value < 0) return error.InvalidInternalRequest;
    return @intCast(value);
}

fn positiveUint64FromJson(value: i64) !u64 {
    const parsed = try uint64FromJson(value);
    if (parsed == 0) return error.InvalidInternalRequest;
    return parsed;
}

fn usizeFromJson(value: i64) !usize {
    const parsed = try uint64FromJson(value);
    if (parsed > std.math.maxInt(usize)) return error.InvalidInternalRequest;
    return @intCast(parsed);
}

fn base64Alloc(alloc: Allocator, raw: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(raw.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, raw);
    return out;
}

fn jsonResponse(alloc: Allocator, value: anytype) !http_common.HttpResponse {
    return .{
        .status = 200,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn commandErrorResponse(alloc: Allocator, err: anyerror) !http_common.HttpResponse {
    return try textResponse(alloc, commandErrorStatus(err), @errorName(err));
}

fn commandErrorStatus(err: anyerror) u16 {
    return switch (err) {
        error.PrimaryUnavailable,
        error.SlotAlreadyExists,
        error.SlotInactive,
        error.SlotRequiresReseed,
        error.WalNoLongerRetained,
        => 409,
        error.SlotNotFound => 404,
        error.InvalidInternalRequest,
        error.InvalidSlotName,
        error.InvalidSlotProgress,
        error.InvalidReplicationStartLsn,
        error.ReplicationStartAheadOfPrimary,
        error.InitialLsnAheadOfPrimary,
        error.StandbyAheadOfPrimary,
        error.WrongTimeline,
        => 400,
        else => 500,
    };
}

fn textResponse(alloc: Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-http-internal-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn expectContains(body: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, body, needle) != null);
}

test "storage.ha internal http adapter handles every generated HA replication route method" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, null);

    for (internal_api.openapi.server.routes) |route| {
        if (!std.mem.startsWith(u8, route.path, "/ha/replication/")) continue;

        const path = try generatedRoutePathAlloc(alloc, route.path);
        defer alloc.free(path);

        const method = try methodFromText(route.method);
        var response = try server.handle(.{ .method = method, .uri = path });
        defer response.deinit(alloc);

        if (response.status == 404 or response.status == 405) {
            std.debug.print(
                "generated internal OpenAPI HA replication route {s} {s} ({s}) was not dispatched by storage HA internal server\n",
                .{ route.method, route.path, route.operation_id },
            );
            return error.TestExpectedGeneratedRouteDispatched;
        }
    }
}

test "storage.ha internal http adapter returns method errors for generated HA replication routes" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, null);

    for (internal_api.openapi.server.routes) |route| {
        if (!std.mem.startsWith(u8, route.path, "/ha/replication/")) continue;

        const path = try generatedRoutePathAlloc(alloc, route.path);
        defer alloc.free(path);

        const method = generatedRouteUnsupportedMethod(route.method) orelse return error.TestExpectedUnsupportedMethod;
        var response = try server.handle(.{ .method = method, .uri = path });
        defer response.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 405), response.status);
    }
}

test "storage.ha internal http adapter serves replication pull and status update" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "replication-round-trip");
    defer paths.deinit(alloc);

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    var server = Server.init(alloc, &primary);

    var identify = try server.handle(.{
        .method = .GET,
        .uri = internal_api.routes.ha_replication_identify,
    });
    defer identify.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), identify.status);
    try std.testing.expectEqualStrings("application/json", identify.content_type.?);
    try expectContains(identify.body, "\"current_lsn\":2");
    try expectContains(identify.body, "\"record_format_version\":1");

    var create = try server.handle(.{
        .method = .POST,
        .uri = internal_api.routes.ha_replication_slots,
        .body = "{\"slot_name\":\"standby-a\",\"initial_lsn\":0}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), create.status);
    try expectContains(create.body, "\"slot_name\":\"standby-a\"");
    try expectContains(create.body, "\"restart_lsn\":0");

    var start = try server.handle(.{
        .method = .POST,
        .uri = internal_api.routes.ha_replication_start,
        .body = "{\"slot_name\":\"standby-a\",\"from_lsn\":1,\"max_records\":1}",
    });
    defer start.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), start.status);
    try expectContains(start.body, "\"identity\"");
    try expectContains(start.body, "\"record_format_version\":1");
    try expectContains(start.body, "\"timeline_id\":1");
    try expectContains(start.body, "\"last_sent_lsn\":1");
    try expectContains(start.body, "\"next_lsn\":2");
    try expectContains(start.body, "\"end_of_wal\":false");
    try expectContains(start.body, "\"kind\":\"batch-mutation\"");
    try expectContains(start.body, "\"encoded\":\"");

    var status = try server.handle(.{
        .method = .POST,
        .uri = internal_api.routes.ha_replication_status,
        .body = "{\"slot_name\":\"standby-a\",\"timeline_id\":1,\"received_lsn\":1,\"applied_lsn\":1,\"safe_read_lsn\":1}",
    });
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), status.status);
    try expectContains(status.body, "\"received_lsn\":1");
    try expectContains(status.body, "\"applied_lsn\":1");

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
}

fn generatedRoutePathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    if (std.mem.indexOfScalar(u8, path, '{') == null) {
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ internal_api.routes.base, path });
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, internal_api.routes.base);

    var idx: usize = 0;
    while (idx < path.len) {
        if (path[idx] != '{') {
            try out.append(alloc, path[idx]);
            idx += 1;
            continue;
        }
        const end = std.mem.indexOfScalarPos(u8, path, idx, '}') orelse return error.InvalidGeneratedRoutePath;
        try out.appendSlice(alloc, "test");
        idx = end + 1;
    }

    return try out.toOwnedSlice(alloc);
}

fn methodFromText(method: []const u8) !http_common.Method {
    if (std.mem.eql(u8, method, "GET")) return .GET;
    if (std.mem.eql(u8, method, "POST")) return .POST;
    if (std.mem.eql(u8, method, "PUT")) return .PUT;
    if (std.mem.eql(u8, method, "DELETE")) return .DELETE;
    return error.UnsupportedGeneratedMethod;
}

fn generatedRouteUnsupportedMethod(method: []const u8) ?http_common.Method {
    if (std.mem.eql(u8, method, "GET")) return .POST;
    return .GET;
}
