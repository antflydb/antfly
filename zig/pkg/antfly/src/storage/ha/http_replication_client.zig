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

//! Client for the `/internal/v1/ha/replication` hot-standby protocol.
//!
//! This is the HTTP equivalent of `session.zig`: pull durable replication
//! envelopes from the primary, receive them into a standby, apply available
//! records, and acknowledge receive/apply progress back to the primary slot.

const std = @import("std");
const Allocator = std.mem.Allocator;
const http_common = @import("../../common/http/http_common.zig");
const internal_api = @import("../../internal/mod.zig");
const routes = @import("../../raft/transport/routes.zig");
const http_internal = @import("http_internal.zig");
const primary_mod = @import("primary.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const ReplicateOptions = struct {
    max_records: usize = 0,
    max_encoded_bytes: usize = 0,
};

pub const Result = struct {
    received_count: usize,
    applied_count: usize,
    progress: standby_mod.Progress,
};

pub const Client = struct {
    alloc: Allocator,
    executor: http_common.RequestExecutor,

    pub fn init(alloc: Allocator, executor: http_common.RequestExecutor) Client {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn identifySystem(self: *Client, base_uri: []const u8) !internal_api.HAIdentifySystemResponse {
        const uri = try join(self.alloc, base_uri, internal_api.routes.ha_replication_identify);
        errdefer self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .GET,
            .uri = uri,
        });
        defer self.alloc.free(resp.request_uri);
        defer resp.response.deinit(self.alloc);
        try mapStatus(resp.response.status);

        var parsed = try std.json.parseFromSlice(
            internal_api.HAIdentifySystemResponse,
            self.alloc,
            resp.response.body,
            .{ .ignore_unknown_fields = true },
        );
        defer parsed.deinit();
        return parsed.value;
    }

    pub fn createReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        initial_lsn: ?u64,
    ) !void {
        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            struct {
                slot_name: []const u8,
                initial_lsn: ?u64 = null,
            }{
                .slot_name = slot_name,
                .initial_lsn = initial_lsn,
            },
            .{},
        );
        defer self.alloc.free(body);

        const uri = try join(self.alloc, base_uri, internal_api.routes.ha_replication_slots);
        errdefer self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer self.alloc.free(resp.request_uri);
        defer resp.response.deinit(self.alloc);
        try mapStatus(resp.response.status);
    }

    pub fn replicateAvailable(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        standby: *standby_mod.Standby,
        apply_ctx: *anyopaque,
        apply_fn: standby_mod.ApplyFn,
        options: ReplicateOptions,
    ) !Result {
        var response = try self.startReplication(base_uri, slot_name, standby.nextReceiveLsn(), options);
        defer response.deinit();

        var received_count: usize = 0;
        for (response.parsed.value.records) |frame| {
            const encoded = try decodeFrame(self.alloc, frame);
            defer self.alloc.free(encoded);
            const record = try replication_record.decode(encoded);
            if (record.lsn != @as(u64, @intCast(frame.lsn))) return error.ReplicationFrameLsnMismatch;
            _ = standby.receive(record) catch |err| {
                _ = self.updateStandbyStatus(base_uri, slot_name, standby) catch {};
                return err;
            };
            received_count += 1;
        }

        const applied_count = standby.applyAvailable(apply_ctx, apply_fn) catch |err| {
            try self.updateStandbyStatus(base_uri, slot_name, standby);
            return err;
        };
        try self.updateStandbyStatus(base_uri, slot_name, standby);
        return .{
            .received_count = received_count,
            .applied_count = applied_count,
            .progress = standby.currentProgress(),
        };
    }

    fn startReplication(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        from_lsn: u64,
        options: ReplicateOptions,
    ) !ParsedResponse(internal_api.HAStartReplicationResponse) {
        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            struct {
                slot_name: []const u8,
                from_lsn: u64,
                max_records: u64,
                max_encoded_bytes: u64,
            }{
                .slot_name = slot_name,
                .from_lsn = from_lsn,
                .max_records = @intCast(options.max_records),
                .max_encoded_bytes = @intCast(options.max_encoded_bytes),
            },
            .{},
        );
        defer self.alloc.free(body);

        const uri = try join(self.alloc, base_uri, internal_api.routes.ha_replication_start);
        errdefer self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        errdefer {
            self.alloc.free(resp.request_uri);
            resp.response.deinit(self.alloc);
        }
        try mapStatus(resp.response.status);

        const parsed = try std.json.parseFromSlice(
            internal_api.HAStartReplicationResponse,
            self.alloc,
            resp.response.body,
            .{ .ignore_unknown_fields = true },
        );
        return .{
            .alloc = self.alloc,
            .request_uri = resp.request_uri,
            .response = resp.response,
            .parsed = parsed,
        };
    }

    fn updateStandbyStatus(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        standby: *const standby_mod.Standby,
    ) !void {
        const progress = standby.currentProgress();
        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            struct {
                slot_name: []const u8,
                timeline_id: u64,
                received_lsn: u64,
                applied_lsn: u64,
                safe_read_lsn: u64,
            }{
                .slot_name = slot_name,
                .timeline_id = standby.identity.timeline_id,
                .received_lsn = progress.received_lsn,
                .applied_lsn = progress.applied_lsn,
                .safe_read_lsn = progress.safe_read_lsn,
            },
            .{},
        );
        defer self.alloc.free(body);

        const uri = try join(self.alloc, base_uri, internal_api.routes.ha_replication_status);
        errdefer self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer self.alloc.free(resp.request_uri);
        defer resp.response.deinit(self.alloc);
        try mapStatus(resp.response.status);
    }

    fn execute(self: *Client, req: http_common.HttpRequest) !OwnedResponse {
        var attempt: usize = 0;
        while (true) {
            return .{
                .request_uri = req.uri,
                .response = self.executor.execute(self.alloc, req) catch |err| switch (err) {
                    error.HttpConnectionClosing,
                    error.ConnectionResetByPeer,
                    error.ConnectionRefused,
                    error.BrokenPipe,
                    error.EndOfStream,
                    => {
                        if (attempt >= 1) return err;
                        attempt += 1;
                        continue;
                    },
                    else => return err,
                },
            };
        }
    }
};

const OwnedResponse = struct {
    request_uri: []const u8,
    response: http_common.HttpResponse,
};

fn ParsedResponse(comptime T: type) type {
    return struct {
        alloc: Allocator,
        request_uri: []const u8,
        response: http_common.HttpResponse,
        parsed: std.json.Parsed(T),

        fn deinit(self: *@This()) void {
            self.parsed.deinit();
            self.response.deinit(self.alloc);
            self.alloc.free(self.request_uri);
            self.* = undefined;
        }
    };
}

fn decodeFrame(alloc: Allocator, frame: internal_api.openapi.types.HAReplicationFrame) ![]u8 {
    if (frame.lsn <= 0) return error.InvalidReplicationFrame;
    const size = try std.base64.standard.Decoder.calcSizeForSlice(frame.encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, frame.encoded);
    return out;
}

fn join(alloc: Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    return try routes.Routes.join(alloc, base_uri, path);
}

fn mapStatus(status: u16) !void {
    if (status >= 200 and status < 300) return;
    if (status == 400) return error.InvalidInternalReplicationRequest;
    if (status == 404) return error.InternalReplicationEndpointNotFound;
    if (status == 405) return error.UnsupportedOperation;
    if (status == 409) return error.InternalReplicationConflict;
    if (status == 503) return error.InternalReplicationEndpointNotReady;
    return error.UnexpectedHttpStatus;
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-http-replication-client-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

const ApplyCapture = struct {
    alloc: Allocator,
    payloads: std.ArrayListUnmanaged([]u8) = .empty,
    fail_at_lsn: u64 = 0,

    fn deinit(self: *ApplyCapture) void {
        for (self.payloads.items) |payload| self.alloc.free(payload);
        self.payloads.deinit(self.alloc);
        self.* = undefined;
    }

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *ApplyCapture = @ptrCast(@alignCast(ctx));
        if (record.lsn == self.fail_at_lsn) return error.IntentionalApplyFailure;
        const owned = try self.alloc.dupe(u8, record.payload);
        errdefer self.alloc.free(owned);
        try self.payloads.append(self.alloc, owned);
    }
};

test "storage.ha http replication client pulls applies and acknowledges standby progress" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "replicate");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = http_internal.Server.init(alloc, &primary);
    var client = Client.init(alloc, server.executor());

    const identified = try client.identifySystem("http://primary.internal.test");
    try std.testing.expectEqual(@as(i64, 100), identified.identity.cluster_id);
    try std.testing.expectEqual(@as(i64, 1), identified.record_format_version);

    try client.createReplicationSlot("http://primary.internal.test", "standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const result = try client.replicateAvailable(
        "http://primary.internal.test",
        "standby-a",
        &standby,
        &capture,
        ApplyCapture.apply,
        .{},
    );
    try std.testing.expectEqual(@as(usize, 2), result.received_count);
    try std.testing.expectEqual(@as(usize, 2), result.applied_count);
    try std.testing.expectEqual(@as(u64, 2), result.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.progress.applied_lsn);
    try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
    try std.testing.expectEqualStrings("two", capture.payloads.items[1]);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);

    const names = [_][]const u8{"standby-a"};
    const decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
}

test "storage.ha http replication client reports durable receive progress when apply fails" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "apply-fail");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = http_internal.Server.init(alloc, &primary);
    var client = Client.init(alloc, server.executor());

    try client.createReplicationSlot("http://primary.internal.test", "standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    var capture = ApplyCapture{ .alloc = alloc, .fail_at_lsn = 2 };
    defer capture.deinit();
    try std.testing.expectError(
        error.IntentionalApplyFailure,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{},
        ),
    );

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
}
