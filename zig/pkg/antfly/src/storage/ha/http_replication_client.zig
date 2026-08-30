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
const validation = @import("validation.zig");

var test_path_counter: u64 = 0;

pub const ReplicateOptions = struct {
    max_records: usize = 0,
    max_encoded_bytes: usize = 0,
    verify_upstream: bool = true,
};

pub const Result = struct {
    received_count: usize,
    applied_count: usize,
    progress: standby_mod.Progress,
    current_lsn: u64,
    last_sent_lsn: u64,
    next_lsn: u64,
    end_of_wal: bool,
};

pub const LoopResult = struct {
    iterations: usize,
    received_count: usize,
    applied_count: usize,
    progress: standby_mod.Progress,
    current_lsn: u64,
    last_sent_lsn: u64,
    next_lsn: u64,
};

pub const FetchedBatch = struct {
    alloc: Allocator,
    identity: standby_mod.Identity,
    requested_lsn: u64,
    frames: []VerifiedFrame,
    current_lsn: u64,
    last_sent_lsn: u64,
    next_lsn: u64,
    end_of_wal: bool,

    pub fn deinit(self: *FetchedBatch) void {
        freeVerifiedFrames(self.alloc, self.frames);
        self.* = undefined;
    }
};

pub const AppliedBatch = struct {
    received_count: usize,
    applied_count: usize,
    identity: standby_mod.Identity,
    progress: standby_mod.Progress,
};

pub const AuthOptions = struct {
    bearer_token: ?[]const u8 = null,
};

pub const Client = struct {
    alloc: Allocator,
    executor: http_common.RequestExecutor,
    auth: AuthOptions = .{},

    pub fn init(alloc: Allocator, executor: http_common.RequestExecutor) Client {
        return initWithOptions(alloc, executor, .{});
    }

    pub fn initWithOptions(alloc: Allocator, executor: http_common.RequestExecutor, auth: AuthOptions) Client {
        return .{
            .alloc = alloc,
            .executor = executor,
            .auth = auth,
        };
    }

    pub fn identifySystem(self: *Client, base_uri: []const u8) !internal_api.HAIdentifySystemResponse {
        const uri = try join(self.alloc, base_uri, internal_api.routes.ha_replication_identify);
        var free_uri_on_error = true;
        errdefer if (free_uri_on_error) self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .GET,
            .uri = uri,
        });
        free_uri_on_error = false;
        defer self.alloc.free(resp.request_uri);
        defer resp.response.deinit(self.alloc);
        try mapStatus(resp.response.status, resp.response.body);

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
        try validateSlotName(slot_name);
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
        var free_uri_on_error = true;
        errdefer if (free_uri_on_error) self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        free_uri_on_error = false;
        defer self.alloc.free(resp.request_uri);
        defer resp.response.deinit(self.alloc);
        try mapStatus(resp.response.status, resp.response.body);
    }

    pub fn createReplicationSlotForStandby(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        initial_lsn: ?u64,
        standby: *const standby_mod.Standby,
    ) !void {
        try validateSlotName(slot_name);
        try self.verifyCompatibleUpstream(base_uri, standby);
        try self.createReplicationSlot(base_uri, slot_name, initial_lsn);
    }

    pub fn verifyCompatibleUpstream(
        self: *Client,
        base_uri: []const u8,
        standby: *const standby_mod.Standby,
    ) !void {
        return try self.verifyCompatibleUpstreamIdentity(base_uri, standby.identitySnapshot());
    }

    pub fn verifyCompatibleUpstreamIdentity(
        self: *Client,
        base_uri: []const u8,
        identity: standby_mod.Identity,
    ) !void {
        const identified = try self.identifySystem(base_uri);
        try verifyIdentity(identified.identity, identity);
        const format_version = try positiveUint64FromJson(identified.record_format_version);
        if (format_version != replication_record.format_version) return error.UnsupportedReplicationFormat;
    }

    pub fn fetchAvailable(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        identity: standby_mod.Identity,
        requested_lsn: u64,
        options: ReplicateOptions,
    ) !FetchedBatch {
        try validateSlotName(slot_name);
        if (options.verify_upstream) {
            try self.verifyCompatibleUpstreamIdentity(base_uri, identity);
        }
        var response = try self.startReplication(base_uri, slot_name, requested_lsn, options);
        defer response.deinit();
        try verifyStartReplicationResponse(response.parsed.value, slot_name, identity);

        const current_lsn = try uint64FromJson(response.parsed.value.current_lsn);
        const last_sent_lsn = try uint64FromJson(response.parsed.value.last_sent_lsn);
        const next_lsn = try positiveUint64FromJson(response.parsed.value.next_lsn);
        const frames = try decodeAndValidateFrames(
            self.alloc,
            response.parsed.value,
            identity,
            requested_lsn,
            current_lsn,
            last_sent_lsn,
            next_lsn,
            response.parsed.value.end_of_wal,
        );

        return .{
            .alloc = self.alloc,
            .identity = identity,
            .requested_lsn = requested_lsn,
            .frames = frames,
            .current_lsn = current_lsn,
            .last_sent_lsn = last_sent_lsn,
            .next_lsn = next_lsn,
            .end_of_wal = response.parsed.value.end_of_wal,
        };
    }

    pub fn applyFetched(
        self: *Client,
        batch: *const FetchedBatch,
        standby: *standby_mod.Standby,
        apply_ctx: *anyopaque,
        apply_fn: standby_mod.ApplyFn,
    ) !AppliedBatch {
        _ = self;
        try standby.lockExclusive();
        defer standby.unlockExclusive();
        const before = standby.snapshotLocked();
        if (!std.meta.eql(before.identity, batch.identity) or
            before.progress.nextReceiveLsn() != batch.requested_lsn)
        {
            return error.HAStandbyStateChanged;
        }

        for (batch.frames) |frame| {
            _ = try standby.receiveLocked(frame.record);
        }
        const applied_count = try standby.applyAvailableLocked(apply_ctx, apply_fn);
        const after = standby.snapshotLocked();
        return .{
            .received_count = batch.frames.len,
            .applied_count = applied_count,
            .identity = after.identity,
            .progress = after.progress,
        };
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
        const before = standby.snapshot();
        const requested_lsn = before.progress.nextReceiveLsn();
        const identity = before.identity;
        var batch = try self.fetchAvailable(base_uri, slot_name, identity, requested_lsn, options);
        defer batch.deinit();
        const applied = self.applyFetched(&batch, standby, apply_ctx, apply_fn) catch |err| {
            const failed = standby.snapshot();
            _ = self.updateStandbyStatusSnapshot(base_uri, slot_name, failed.identity, failed.progress) catch {};
            return err;
        };
        try self.updateStandbyStatusSnapshot(base_uri, slot_name, applied.identity, applied.progress);
        return .{
            .received_count = applied.received_count,
            .applied_count = applied.applied_count,
            .progress = applied.progress,
            .current_lsn = batch.current_lsn,
            .last_sent_lsn = batch.last_sent_lsn,
            .next_lsn = batch.next_lsn,
            .end_of_wal = batch.end_of_wal,
        };
    }

    pub fn replicateUntilCaughtUp(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        standby: *standby_mod.Standby,
        apply_ctx: *anyopaque,
        apply_fn: standby_mod.ApplyFn,
        options: ReplicateOptions,
    ) !LoopResult {
        try validateSlotName(slot_name);
        var iterations: usize = 0;
        var received_count: usize = 0;
        var applied_count: usize = 0;
        var progress = standby.currentProgress();
        var current_lsn: u64 = progress.received_lsn;
        var last_sent_lsn: u64 = progress.received_lsn;
        var next_lsn: u64 = standby.nextReceiveLsn();
        var batch_options = options;
        if (batch_options.verify_upstream) {
            try self.verifyCompatibleUpstream(base_uri, standby);
            batch_options.verify_upstream = false;
        }

        while (true) {
            const result = try self.replicateAvailable(
                base_uri,
                slot_name,
                standby,
                apply_ctx,
                apply_fn,
                batch_options,
            );
            iterations += 1;
            received_count += result.received_count;
            applied_count += result.applied_count;
            progress = result.progress;
            current_lsn = result.current_lsn;
            last_sent_lsn = result.last_sent_lsn;
            next_lsn = result.next_lsn;

            if (result.end_of_wal) {
                return .{
                    .iterations = iterations,
                    .received_count = received_count,
                    .applied_count = applied_count,
                    .progress = progress,
                    .current_lsn = current_lsn,
                    .last_sent_lsn = last_sent_lsn,
                    .next_lsn = next_lsn,
                };
            }

            if (result.received_count == 0 and result.applied_count == 0) {
                return error.InternalReplicationDidNotAdvance;
            }
        }
    }

    fn startReplication(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        from_lsn: u64,
        options: ReplicateOptions,
    ) !ParsedResponse(internal_api.HAStartReplicationResponse) {
        try validateSlotName(slot_name);
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
        var free_uri_on_error = true;
        errdefer if (free_uri_on_error) self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        free_uri_on_error = false;
        errdefer {
            self.alloc.free(resp.request_uri);
            resp.response.deinit(self.alloc);
        }
        try mapStatus(resp.response.status, resp.response.body);

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

    pub fn updateStandbyStatusSnapshot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        identity: standby_mod.Identity,
        progress: standby_mod.Progress,
    ) !void {
        try validateSlotName(slot_name);
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
                .timeline_id = identity.timeline_id,
                .received_lsn = progress.received_lsn,
                .applied_lsn = progress.applied_lsn,
                .safe_read_lsn = progress.safe_read_lsn,
            },
            .{},
        );
        defer self.alloc.free(body);

        const uri = try join(self.alloc, base_uri, internal_api.routes.ha_replication_status);
        var free_uri_on_error = true;
        errdefer if (free_uri_on_error) self.alloc.free(uri);
        var resp = try self.execute(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        free_uri_on_error = false;
        defer self.alloc.free(resp.request_uri);
        defer resp.response.deinit(self.alloc);
        try mapStatus(resp.response.status, resp.response.body);

        var parsed = try std.json.parseFromSlice(
            internal_api.HAStandbyStatusUpdateResponse,
            self.alloc,
            resp.response.body,
            .{ .ignore_unknown_fields = true },
        );
        defer parsed.deinit();
        try verifyStandbyStatusUpdateResponse(parsed.value, slot_name, identity, progress);
    }

    fn execute(self: *Client, raw_request: http_common.HttpRequest) !OwnedResponse {
        var request = raw_request;
        var authorization: ?[]u8 = null;
        defer if (authorization) |value| self.alloc.free(value);
        if (self.auth.bearer_token) |token| {
            try validateBearerToken(token);
            authorization = try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{token});
            request.authorization = authorization.?;
        }

        var attempt: usize = 0;
        while (true) {
            return .{
                .request_uri = request.uri,
                .response = self.executor.execute(self.alloc, request) catch |err| switch (err) {
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

const VerifiedFrame = struct {
    encoded: []u8,
    record: replication_record.RecordView,

    fn deinit(self: *VerifiedFrame, alloc: Allocator) void {
        alloc.free(self.encoded);
        self.* = undefined;
    }
};

fn decodeAndValidateFrames(
    alloc: Allocator,
    response: internal_api.HAStartReplicationResponse,
    expected_identity: standby_mod.Identity,
    requested_lsn: u64,
    current_lsn: u64,
    last_sent_lsn: u64,
    next_lsn: u64,
    end_of_wal: bool,
) ![]VerifiedFrame {
    const response_from_lsn = try positiveUint64FromJson(response.from_lsn);
    if (response_from_lsn != requested_lsn) return error.ReplicationResponseLsnMismatch;
    if (last_sent_lsn > current_lsn) return error.ReplicationResponseLsnMismatch;
    if (next_lsn != try std.math.add(u64, last_sent_lsn, 1)) return error.ReplicationResponseLsnMismatch;
    if (next_lsn > try std.math.add(u64, current_lsn, 1)) return error.ReplicationResponseLsnMismatch;
    if (end_of_wal != (next_lsn > current_lsn)) return error.ReplicationResponseEndOfWalMismatch;

    const encoded_bytes = try uint64FromJson(response.encoded_bytes);
    var actual_encoded_bytes: u64 = 0;
    var expected_lsn = requested_lsn;

    const frames = try alloc.alloc(VerifiedFrame, response.records.len);
    var filled: usize = 0;
    errdefer {
        for (frames[0..filled]) |*frame| frame.deinit(alloc);
        alloc.free(frames);
    }

    for (response.records, 0..) |wire_frame, idx| {
        var encoded = try decodeFrame(alloc, wire_frame);
        errdefer if (encoded.len > 0) alloc.free(encoded);
        const record = try replication_record.decode(encoded);
        try validateFrameMetadata(wire_frame, record);
        try validateRecordIdentity(record, expected_identity);

        const frame_lsn = try positiveUint64FromJson(wire_frame.lsn);
        if (frame_lsn != expected_lsn or record.lsn != expected_lsn) {
            return error.ReplicationFrameLsnMismatch;
        }
        if (record.previous_lsn != expected_lsn - 1) return error.UnexpectedPreviousLsn;
        actual_encoded_bytes = try std.math.add(u64, actual_encoded_bytes, encoded.len);

        frames[idx] = .{
            .encoded = encoded,
            .record = record,
        };
        filled += 1;
        encoded = &.{};
        expected_lsn = try std.math.add(u64, expected_lsn, 1);
    }

    const expected_last_sent_lsn = expected_lsn - 1;
    if (last_sent_lsn != expected_last_sent_lsn) return error.ReplicationResponseLsnMismatch;
    if (encoded_bytes != actual_encoded_bytes) return error.ReplicationResponseEncodedBytesMismatch;

    return frames;
}

fn freeVerifiedFrames(alloc: Allocator, frames: []VerifiedFrame) void {
    for (frames) |*frame| frame.deinit(alloc);
    alloc.free(frames);
}

fn decodeFrame(alloc: Allocator, frame: internal_api.openapi.types.HAReplicationFrame) ![]u8 {
    if (frame.lsn <= 0) return error.InvalidReplicationFrame;
    const size = try std.base64.standard.Decoder.calcSizeForSlice(frame.encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, frame.encoded);
    return out;
}

fn validateFrameMetadata(frame: internal_api.openapi.types.HAReplicationFrame, record: replication_record.RecordView) !void {
    if (record.kind != replicationRecordKind(frame.kind)) return error.ReplicationFrameKindMismatch;
    if (record.payload_codec != replicationPayloadCodec(frame.payload_codec)) return error.ReplicationFramePayloadCodecMismatch;
}

fn validateRecordIdentity(record: replication_record.RecordView, expected: standby_mod.Identity) !void {
    if (record.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (expected.shard_id != 0 and record.shard_id != expected.shard_id) return error.WrongShard;
    if (expected.table_id != 0 and record.table_id != expected.table_id) return error.WrongTable;
    if (record.kind == .timeline_switch) {
        if (record.timeline_id <= expected.timeline_id) return error.InvalidTimelineSwitch;
        if (record.epoch <= expected.epoch) return error.InvalidTimelineSwitch;
        return;
    }
    if (record.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (record.epoch != expected.epoch) return error.WrongEpoch;
}

fn replicationRecordKind(kind: internal_api.openapi.types.HARecordKind) replication_record.RecordKind {
    return switch (kind) {
        .batch_mutation => .batch_mutation,
        .metadata_mutation => .metadata_mutation,
        .derived_effect => .derived_effect,
        .backup_start => .backup_start,
        .backup_end => .backup_end,
        .checkpoint => .checkpoint,
        .manifest => .manifest,
        .truncate => .truncate,
        .timeline_switch => .timeline_switch,
    };
}

fn replicationPayloadCodec(codec: internal_api.openapi.types.HAPayloadCodec) replication_record.PayloadCodec {
    return switch (codec) {
        .raw => .raw,
        .json => .json,
        .binary => .binary,
    };
}

fn verifyIdentity(actual: anytype, expected: standby_mod.Identity) !void {
    if (try positiveUint64FromJson(actual.cluster_id) != expected.cluster_id) return error.WrongCluster;
    if (try uint64FromJson(actual.shard_id) != expected.shard_id) return error.WrongShard;
    if (try uint64FromJson(actual.table_id) != expected.table_id) return error.WrongTable;
    if (try positiveUint64FromJson(actual.timeline_id) != expected.timeline_id) return error.WrongTimeline;
    if (try positiveUint64FromJson(actual.epoch) != expected.epoch) return error.WrongEpoch;
}

fn verifyStartReplicationResponse(response: anytype, expected_slot_name: []const u8, expected: standby_mod.Identity) !void {
    if (!std.mem.eql(u8, response.slot_name, expected_slot_name)) return error.ReplicationSlotMismatch;
    try verifyIdentity(response.identity, expected);
    const format_version = try positiveUint64FromJson(response.record_format_version);
    if (format_version != replication_record.format_version) return error.UnsupportedReplicationFormat;
    const timeline_id = try positiveUint64FromJson(response.timeline_id);
    if (timeline_id != expected.timeline_id) return error.WrongTimeline;
}

fn verifyStandbyStatusUpdateResponse(
    response: internal_api.HAStandbyStatusUpdateResponse,
    expected_slot_name: []const u8,
    expected_identity: standby_mod.Identity,
    expected_progress: standby_mod.Progress,
) !void {
    if (!std.mem.eql(u8, response.slot_name, expected_slot_name)) return error.ReplicationSlotMismatch;
    const timeline_id = try positiveUint64FromJson(response.timeline_id);
    if (timeline_id != expected_identity.timeline_id) return error.WrongTimeline;
    const received_lsn = try uint64FromJson(response.received_lsn);
    const applied_lsn = try uint64FromJson(response.applied_lsn);
    const safe_read_lsn = try uint64FromJson(response.safe_read_lsn);
    if (received_lsn < expected_progress.received_lsn) return error.ReplicationStatusAckMismatch;
    if (applied_lsn < expected_progress.applied_lsn) return error.ReplicationStatusAckMismatch;
    if (safe_read_lsn < expected_progress.safe_read_lsn) return error.ReplicationStatusAckMismatch;
    if (applied_lsn > received_lsn) return error.ReplicationStatusAckMismatch;
    if (safe_read_lsn > applied_lsn) return error.ReplicationStatusAckMismatch;
}

fn uint64FromJson(value: i64) !u64 {
    if (value < 0) return error.InvalidInternalReplicationResponse;
    return @intCast(value);
}

fn positiveUint64FromJson(value: i64) !u64 {
    if (value <= 0) return error.InvalidInternalReplicationResponse;
    return @intCast(value);
}

fn validateSlotName(slot_name: []const u8) !void {
    if (!validation.isIdentifier(slot_name)) return error.InvalidSlotName;
}

fn validateBearerToken(token: []const u8) !void {
    if (token.len == 0) return error.InvalidInternalReplicationBearerToken;
    for (token) |byte| {
        if (byte <= 0x20 or byte == 0x7f) return error.InvalidInternalReplicationBearerToken;
    }
}

fn validateBaseURI(base_uri: []const u8) !void {
    if (!validation.isHTTPURLWithHostNoHiddenWhitespace(base_uri)) return error.InvalidInternalReplicationURL;
}

fn join(alloc: Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    try validateBaseURI(base_uri);
    return try routes.Routes.join(alloc, base_uri, path);
}

fn mapStatus(status: u16, body: []const u8) !void {
    if (status >= 200 and status < 300) return;
    if (status == 400) return error.InvalidInternalReplicationRequest;
    // Fixed internal routes use the error name as their command-error body.
    // Preserve resource absence separately from an incompatible/missing route
    // so the standby exposes an actionable degraded-state reason.
    if (status == 404) {
        if (std.mem.eql(u8, std.mem.trim(u8, body, " \t\r\n"), "SlotNotFound")) return error.SlotNotFound;
        return error.InternalReplicationEndpointNotFound;
    }
    if (status == 405) return error.UnsupportedOperation;
    if (status == 409) return error.InternalReplicationConflict;
    if (status == 503) return error.InternalReplicationEndpointNotReady;
    return error.UnexpectedHttpStatus;
}

test "http replication status distinguishes missing slots from missing routes" {
    try std.testing.expectError(error.SlotNotFound, mapStatus(404, "SlotNotFound"));
    try std.testing.expectError(error.InternalReplicationEndpointNotFound, mapStatus(404, "not found"));
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
        .primary_log = try alloc.dupeSentinel(u8, primary_log, 0),
        .primary_slots = try alloc.dupeSentinel(u8, primary_slots, 0),
        .standby_log = try alloc.dupeSentinel(u8, standby_log, 0),
        .standby_progress = try alloc.dupeSentinel(u8, standby_progress, 0),
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

const NoCallExecutor = struct {
    fn executor(self: *NoCallExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        _ = ptr;
        _ = alloc;
        _ = req;
        return error.TestExecutorShouldNotRun;
    }
};

const NotFoundExecutor = struct {
    fn executor(self: *NotFoundExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{ .execute = execute },
        };
    }

    fn execute(_: *anyopaque, alloc: Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
        return .{
            .status = 404,
            .content_type = try alloc.dupe(u8, "text/plain"),
            .body = try alloc.dupe(u8, "not found"),
        };
    }
};

const CorruptFrameExecutor = struct {
    identity: standby_mod.Identity,

    fn executor(self: *CorruptFrameExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *CorruptFrameExecutor = @ptrCast(@alignCast(ptr));
        if (std.mem.endsWith(u8, req.uri, internal_api.routes.ha_replication_start)) {
            return try self.startResponse(alloc);
        }
        if (std.mem.endsWith(u8, req.uri, internal_api.routes.ha_replication_status)) {
            return try jsonTestResponse(alloc, .{ .unexpected_status_update = true });
        }
        return .{
            .status = 404,
            .body = try alloc.dupe(u8, "not found"),
        };
    }

    fn startResponse(self: *CorruptFrameExecutor, alloc: Allocator) !http_common.HttpResponse {
        const encoded = try replication_record.encodeAlloc(alloc, .{
            .kind = .batch_mutation,
            .payload_codec = .raw,
            .cluster_id = self.identity.cluster_id,
            .shard_id = self.identity.shard_id,
            .table_id = self.identity.table_id,
            .timeline_id = self.identity.timeline_id,
            .epoch = self.identity.epoch,
            .lsn = 1,
            .previous_lsn = 0,
            .payload = "one",
        });
        defer alloc.free(encoded);

        const encoded_frame = try base64TestAlloc(alloc, encoded);
        defer alloc.free(encoded_frame);
        const records = [_]struct {
            lsn: u64,
            kind: []const u8,
            payload_codec: []const u8,
            encoded: []const u8,
        }{.{
            .lsn = 1,
            .kind = "metadata-mutation",
            .payload_codec = "raw",
            .encoded = encoded_frame,
        }};

        return try jsonTestResponse(alloc, .{
            .slot_name = "standby-a",
            .identity = self.identity,
            .record_format_version = replication_record.format_version,
            .timeline_id = self.identity.timeline_id,
            .from_lsn = 1,
            .current_lsn = 1,
            .last_sent_lsn = 1,
            .next_lsn = 2,
            .end_of_wal = true,
            .encoded_bytes = encoded.len,
            .records = &records,
        });
    }
};

const WrongIdentityBatchExecutor = struct {
    const Mismatch = enum { epoch, previous_lsn, slot, premature_end_of_wal };

    identity: standby_mod.Identity,
    mismatch: Mismatch = .epoch,

    fn executor(self: *WrongIdentityBatchExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *WrongIdentityBatchExecutor = @ptrCast(@alignCast(ptr));
        if (std.mem.endsWith(u8, req.uri, internal_api.routes.ha_replication_start)) {
            return try self.startResponse(alloc);
        }
        if (std.mem.endsWith(u8, req.uri, internal_api.routes.ha_replication_status)) {
            return try jsonTestResponse(alloc, .{ .unexpected_status_update = true });
        }
        return .{
            .status = 404,
            .body = try alloc.dupe(u8, "not found"),
        };
    }

    fn startResponse(self: *WrongIdentityBatchExecutor, alloc: Allocator) !http_common.HttpResponse {
        const first = try replication_record.encodeAlloc(alloc, .{
            .kind = .batch_mutation,
            .payload_codec = .raw,
            .cluster_id = self.identity.cluster_id,
            .shard_id = self.identity.shard_id,
            .table_id = self.identity.table_id,
            .timeline_id = self.identity.timeline_id,
            .epoch = self.identity.epoch,
            .lsn = 1,
            .previous_lsn = 0,
            .payload = "one",
        });
        defer alloc.free(first);
        const second = try replication_record.encodeAlloc(alloc, .{
            .kind = .batch_mutation,
            .payload_codec = .raw,
            .cluster_id = self.identity.cluster_id,
            .shard_id = self.identity.shard_id,
            .table_id = self.identity.table_id,
            .timeline_id = self.identity.timeline_id,
            .epoch = if (self.mismatch == .epoch) self.identity.epoch + 1 else self.identity.epoch,
            .lsn = 2,
            .previous_lsn = if (self.mismatch == .previous_lsn) 0 else 1,
            .payload = if (self.mismatch == .previous_lsn) "wrong-previous" else "wrong-epoch",
        });
        defer alloc.free(second);

        const first_frame = try base64TestAlloc(alloc, first);
        defer alloc.free(first_frame);
        const second_frame = try base64TestAlloc(alloc, second);
        defer alloc.free(second_frame);
        const first_record = struct {
            lsn: u64,
            kind: []const u8,
            payload_codec: []const u8,
            encoded: []const u8,
        }{
            .lsn = 1,
            .kind = "batch-mutation",
            .payload_codec = "raw",
            .encoded = first_frame,
        };
        const records = [_]@TypeOf(first_record){
            first_record,
            .{
                .lsn = 2,
                .kind = "batch-mutation",
                .payload_codec = "raw",
                .encoded = second_frame,
            },
        };
        const response_records = if (self.mismatch == .premature_end_of_wal) records[0..1] else records[0..];
        const response_last_sent_lsn: u64 = if (self.mismatch == .premature_end_of_wal) 1 else 2;
        const response_next_lsn: u64 = if (self.mismatch == .premature_end_of_wal) 2 else 3;
        const response_encoded_bytes: usize = if (self.mismatch == .premature_end_of_wal) first.len else first.len + second.len;

        return try jsonTestResponse(alloc, .{
            .slot_name = if (self.mismatch == .slot) "standby-other" else "standby-a",
            .identity = self.identity,
            .record_format_version = replication_record.format_version,
            .timeline_id = self.identity.timeline_id,
            .from_lsn = 1,
            .current_lsn = 2,
            .last_sent_lsn = response_last_sent_lsn,
            .next_lsn = response_next_lsn,
            .end_of_wal = true,
            .encoded_bytes = response_encoded_bytes,
            .records = response_records,
        });
    }
};

const StatusAckMismatchExecutor = struct {
    const Mismatch = enum { slot, timeline, received_lsn, applied_lsn, safe_read_lsn };

    identity: standby_mod.Identity,
    mismatch: Mismatch = .slot,

    fn executor(self: *StatusAckMismatchExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *StatusAckMismatchExecutor = @ptrCast(@alignCast(ptr));
        if (std.mem.endsWith(u8, req.uri, internal_api.routes.ha_replication_start)) {
            return try self.startResponse(alloc);
        }
        if (std.mem.endsWith(u8, req.uri, internal_api.routes.ha_replication_status)) {
            return try self.statusResponse(alloc);
        }
        return .{
            .status = 404,
            .body = try alloc.dupe(u8, "not found"),
        };
    }

    fn startResponse(self: *StatusAckMismatchExecutor, alloc: Allocator) !http_common.HttpResponse {
        const encoded = try replication_record.encodeAlloc(alloc, .{
            .kind = .batch_mutation,
            .payload_codec = .raw,
            .cluster_id = self.identity.cluster_id,
            .shard_id = self.identity.shard_id,
            .table_id = self.identity.table_id,
            .timeline_id = self.identity.timeline_id,
            .epoch = self.identity.epoch,
            .lsn = 1,
            .previous_lsn = 0,
            .payload = "one",
        });
        defer alloc.free(encoded);

        const encoded_frame = try base64TestAlloc(alloc, encoded);
        defer alloc.free(encoded_frame);
        const records = [_]struct {
            lsn: u64,
            kind: []const u8,
            payload_codec: []const u8,
            encoded: []const u8,
        }{.{
            .lsn = 1,
            .kind = "batch-mutation",
            .payload_codec = "raw",
            .encoded = encoded_frame,
        }};

        return try jsonTestResponse(alloc, .{
            .slot_name = "standby-a",
            .identity = self.identity,
            .record_format_version = replication_record.format_version,
            .timeline_id = self.identity.timeline_id,
            .from_lsn = 1,
            .current_lsn = 1,
            .last_sent_lsn = 1,
            .next_lsn = 2,
            .end_of_wal = true,
            .encoded_bytes = encoded.len,
            .records = &records,
        });
    }

    fn statusResponse(self: *StatusAckMismatchExecutor, alloc: Allocator) !http_common.HttpResponse {
        const timeline_id: u64 = if (self.mismatch == .timeline) self.identity.timeline_id + 1 else self.identity.timeline_id;
        const received_lsn: u64 = if (self.mismatch == .received_lsn) 0 else 1;
        const applied_lsn: u64 = if (self.mismatch == .applied_lsn) 0 else 1;
        const safe_read_lsn: u64 = if (self.mismatch == .safe_read_lsn) 0 else 1;
        return try jsonTestResponse(alloc, .{
            .slot_name = if (self.mismatch == .slot) "standby-other" else "standby-a",
            .timeline_id = timeline_id,
            .restart_lsn = 1,
            .received_lsn = received_lsn,
            .applied_lsn = applied_lsn,
            .safe_read_lsn = safe_read_lsn,
            .active = true,
            .reseed_required = false,
            .current_lsn = 1,
        });
    }
};

fn base64TestAlloc(alloc: Allocator, raw: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(raw.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, raw);
    return out;
}

fn jsonTestResponse(alloc: Allocator, value: anytype) !http_common.HttpResponse {
    return .{
        .status = 200,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

test "storage.ha http replication client rejects invalid local inputs before execution" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-local-inputs");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var executor = NoCallExecutor{};
    var client = Client.init(alloc, executor.executor());
    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();

    try std.testing.expectError(
        error.InvalidInternalReplicationURL,
        client.identifySystem(" http://primary.internal.test"),
    );
    try std.testing.expectError(
        error.InvalidInternalReplicationURL,
        client.createReplicationSlot("ftp://primary.internal.test", "standby-a", 0),
    );
    try std.testing.expectError(
        error.InvalidInternalReplicationURL,
        client.replicateUntilCaughtUp(
            "http://primary internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{},
        ),
    );

    try std.testing.expectError(
        error.InvalidSlotName,
        client.createReplicationSlot("http://primary.internal.test", " standby-a", 0),
    );
    try std.testing.expectError(
        error.InvalidSlotName,
        client.createReplicationSlotForStandby("http://primary.internal.test", "standby/a", 0, &standby),
    );
    try std.testing.expectError(
        error.InvalidSlotName,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
    try std.testing.expectError(
        error.InvalidSlotName,
        client.replicateUntilCaughtUp(
            "http://primary.internal.test",
            "standby-a\n",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
}

test "storage.ha http replication client releases transferred request URIs once on status errors" {
    const alloc = std.testing.allocator;
    var executor = NotFoundExecutor{};
    var client = Client.init(alloc, executor.executor());

    try std.testing.expectError(
        error.InternalReplicationEndpointNotFound,
        client.identifySystem("http://primary.internal.test"),
    );
    try std.testing.expectError(
        error.InternalReplicationEndpointNotFound,
        client.createReplicationSlot("http://primary.internal.test", "standby-a", 0),
    );
    try std.testing.expectError(
        error.InternalReplicationEndpointNotFound,
        client.startReplication("http://primary.internal.test", "standby-a", 1, .{}),
    );
}

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

    try client.createReplicationSlotForStandby("http://primary.internal.test", "standby-a", 0, &standby);
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
    try std.testing.expectEqual(@as(u64, 2), result.current_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.last_sent_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.next_lsn);
    try std.testing.expect(result.end_of_wal);
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

test "storage.ha http replication client replicates mixed tables for whole instance identity" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "replicate-whole-instance");
    defer paths.deinit(alloc);
    const identity = standby_mod.Identity{
        .cluster_id = 100,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    };

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});

    var server = http_internal.Server.init(alloc, &primary);
    var client = Client.init(alloc, server.executor());
    try client.createReplicationSlotForStandby("http://primary.internal.test", "standby-a", 0, &standby);
    _ = try primary.append(.{ .shard_id = 10, .table_id = 20, .payload = "one" });
    _ = try primary.append(.{ .shard_id = 11, .table_id = 21, .payload = "two" });

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
    standby.close();

    var reopened = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer reopened.close();
    try std.testing.expectEqual(identity, reopened.identitySnapshot());
    try std.testing.expectEqual(@as(u64, 2), reopened.currentProgress().safe_read_lsn);
}

test "storage.ha http replication client verifies upstream identity before streaming" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "identity-preflight");
    defer paths.deinit(alloc);
    const standby_identity = testIdentity();
    var primary_identity = standby_identity;
    primary_identity.cluster_id += 1;

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, primary_identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, standby_identity, .{});
    defer standby.close();

    var server = http_internal.Server.init(alloc, &primary);
    var client = Client.init(alloc, server.executor());

    try std.testing.expectError(
        error.WrongCluster,
        client.createReplicationSlotForStandby("http://primary.internal.test", "standby-a", 0, &standby),
    );

    try client.createReplicationSlot("http://primary.internal.test", "standby-a", 0);
    _ = try primary.append(.{ .payload = "wrong-cluster" });

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(
        error.WrongCluster,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{},
        ),
    );
    try std.testing.expectError(
        error.WrongCluster,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );

    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(usize, 0), capture.payloads.items.len);
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);
}

test "storage.ha http replication client rejects frame metadata mismatch before receive" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "frame-metadata");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var executor = CorruptFrameExecutor{ .identity = identity };
    var client = Client.init(alloc, executor.executor());

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(
        error.ReplicationFrameKindMismatch,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(usize, 0), capture.payloads.items.len);
}

test "storage.ha http replication client rejects record identity mismatch before partial receive" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "frame-identity");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var executor = WrongIdentityBatchExecutor{ .identity = identity };
    var client = Client.init(alloc, executor.executor());

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(
        error.WrongEpoch,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(usize, 0), capture.payloads.items.len);
}

test "storage.ha http replication client rejects previous lsn mismatch before partial receive" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "frame-previous-lsn");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var executor = WrongIdentityBatchExecutor{ .identity = identity, .mismatch = .previous_lsn };
    var client = Client.init(alloc, executor.executor());

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(
        error.UnexpectedPreviousLsn,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(usize, 0), capture.payloads.items.len);
}

test "storage.ha http replication client rejects slot mismatch before receive" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream-slot-mismatch");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var executor = WrongIdentityBatchExecutor{ .identity = identity, .mismatch = .slot };
    var client = Client.init(alloc, executor.executor());

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(
        error.ReplicationSlotMismatch,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(usize, 0), capture.payloads.items.len);
}

test "storage.ha http replication client rejects premature end-of-wal before receive" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "premature-end-of-wal");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var executor = WrongIdentityBatchExecutor{ .identity = identity, .mismatch = .premature_end_of_wal };
    var client = Client.init(alloc, executor.executor());

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectError(
        error.ReplicationResponseEndOfWalMismatch,
        client.replicateAvailable(
            "http://primary.internal.test",
            "standby-a",
            &standby,
            &capture,
            ApplyCapture.apply,
            .{ .verify_upstream = false },
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(usize, 0), capture.payloads.items.len);
}

test "storage.ha http replication client verifies typed standby status acknowledgement" {
    const cases = [_]struct {
        name: []const u8,
        mismatch: StatusAckMismatchExecutor.Mismatch,
        expected_error: anyerror,
    }{
        .{
            .name = "slot",
            .mismatch = .slot,
            .expected_error = error.ReplicationSlotMismatch,
        },
        .{
            .name = "timeline",
            .mismatch = .timeline,
            .expected_error = error.WrongTimeline,
        },
        .{
            .name = "received_lsn",
            .mismatch = .received_lsn,
            .expected_error = error.ReplicationStatusAckMismatch,
        },
        .{
            .name = "applied_lsn",
            .mismatch = .applied_lsn,
            .expected_error = error.ReplicationStatusAckMismatch,
        },
        .{
            .name = "safe_read_lsn",
            .mismatch = .safe_read_lsn,
            .expected_error = error.ReplicationStatusAckMismatch,
        },
    };

    const alloc = std.testing.allocator;
    const identity = testIdentity();

    for (cases) |tt| {
        const paths = try testPaths(alloc, "status-ack-mismatch");
        defer paths.deinit(alloc);

        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();

        var executor = StatusAckMismatchExecutor{ .identity = identity, .mismatch = tt.mismatch };
        var client = Client.init(alloc, executor.executor());

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectError(
            tt.expected_error,
            client.replicateAvailable(
                "http://primary.internal.test",
                "standby-a",
                &standby,
                &capture,
                ApplyCapture.apply,
                .{ .verify_upstream = false },
            ),
        );
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(usize, 1), capture.payloads.items.len);
        try std.testing.expect(tt.name.len > 0);
    }
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

test "storage.ha http replication client catches up over bounded batches" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "catch-up");
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
    _ = try primary.append(.{ .payload = "three" });

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const result = try client.replicateUntilCaughtUp(
        "http://primary.internal.test",
        "standby-a",
        &standby,
        &capture,
        ApplyCapture.apply,
        .{ .max_records = 1 },
    );

    try std.testing.expectEqual(@as(usize, 3), result.iterations);
    try std.testing.expectEqual(@as(usize, 3), result.received_count);
    try std.testing.expectEqual(@as(usize, 3), result.applied_count);
    try std.testing.expectEqual(@as(u64, 3), result.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.progress.applied_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.current_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.last_sent_lsn);
    try std.testing.expectEqual(@as(u64, 4), result.next_lsn);
    try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
    try std.testing.expectEqualStrings("two", capture.payloads.items[1]);
    try std.testing.expectEqualStrings("three", capture.payloads.items[2]);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 3), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), slot.applied_lsn);
}
