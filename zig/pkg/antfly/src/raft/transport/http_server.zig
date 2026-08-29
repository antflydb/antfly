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
const raft_engine = @import("raft_engine");
const common_http = @import("../../common/http/mod.zig");
const data_raft_protocol = @import("../../common/data_raft_protocol.zig");
const common = @import("http_common.zig");
const http_snapshot = @import("http_snapshot.zig");
const routes = @import("routes.zig");
const snapshot_transfer = @import("snapshot_transfer.zig");

pub const HttpServerConfig = struct {
    max_request_bytes: usize = common_http.default_max_request_bytes,
};

pub const BatchHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        handle_peer_batch: *const fn (ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) anyerror!void,
    };

    pub fn handlePeerBatch(self: BatchHandler, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
        return try self.vtable.handle_peer_batch(self.ptr, batch);
    }
};

pub const SnapshotStore = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        put_snapshot: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) anyerror!void,
        get_snapshot: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) anyerror![]u8,
        release_snapshot: ?*const fn (ptr: *anyopaque, snapshot_id: []const u8) anyerror!void = null,
        begin_chunked_snapshot: ?*const fn (ptr: *anyopaque, manifest: snapshot_transfer.Manifest, snapshot_id: []const u8) anyerror!void = null,
        put_snapshot_chunk: ?*const fn (ptr: *anyopaque, snapshot_id: []const u8, offset: u64, body: []const u8) anyerror!void = null,
        commit_chunked_snapshot: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            snapshot_id: []const u8,
            materialize: bool,
        ) anyerror!?raft_engine.core.types.Snapshot = null,
        get_snapshot_upload_manifest: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) anyerror!snapshot_transfer.Manifest = null,
        get_snapshot_manifest: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) anyerror!snapshot_transfer.Manifest = null,
        get_snapshot_chunk: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, offset: u64, max_len: usize) anyerror![]u8 = null,
        abort_chunked_snapshot: ?*const fn (ptr: *anyopaque, snapshot_id: []const u8) anyerror!void = null,
        release_chunked_snapshot: ?*const fn (ptr: *anyopaque, snapshot_id: []const u8) anyerror!void = null,
    };

    pub fn putSnapshot(self: SnapshotStore, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
        return try self.vtable.put_snapshot(self.ptr, alloc, snapshot_id, body);
    }

    pub fn getSnapshot(self: SnapshotStore, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        return try self.vtable.get_snapshot(self.ptr, alloc, snapshot_id);
    }

    pub fn supportsChunkedTransfer(self: SnapshotStore) bool {
        return self.vtable.begin_chunked_snapshot != null and self.vtable.put_snapshot_chunk != null and
            self.vtable.commit_chunked_snapshot != null and self.vtable.get_snapshot_manifest != null and
            self.vtable.get_snapshot_upload_manifest != null and self.vtable.get_snapshot_chunk != null and
            self.vtable.abort_chunked_snapshot != null and
            self.vtable.release_chunked_snapshot != null;
    }
};

pub const SnapshotUpload = struct {
    group_id: u64,
    from: u64,
    to: u64,
    term: u64,
    snapshot: raft_engine.core.types.Snapshot,
    admission_reserved: bool = false,
};

pub const SnapshotUploadAdmission = struct {
    group_id: u64,
    to: u64,
    data_len: u64,
};

pub const SnapshotUploadHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Optional byte-weighted preflight. When present, cancel must also be
        /// present. The reservation transfers to handle_snapshot_upload,
        /// which owns releasing it whether handling succeeds or fails.
        admit_snapshot_upload: ?*const fn (ptr: *anyopaque, admission: SnapshotUploadAdmission) anyerror!void = null,
        cancel_snapshot_upload: ?*const fn (ptr: *anyopaque, admission: SnapshotUploadAdmission) void = null,
        /// Takes ownership of `upload.snapshot` whether it succeeds or fails.
        handle_snapshot_upload: *const fn (ptr: *anyopaque, upload: SnapshotUpload) anyerror!void,
    };

    pub fn handleSnapshotUpload(self: SnapshotUploadHandler, upload: SnapshotUpload) !void {
        return try self.vtable.handle_snapshot_upload(self.ptr, upload);
    }

    pub fn admitSnapshotUpload(self: SnapshotUploadHandler, admission: SnapshotUploadAdmission) !bool {
        const admit = self.vtable.admit_snapshot_upload orelse return false;
        if (self.vtable.cancel_snapshot_upload == null)
            return error.InvalidSnapshotAdmissionHandler;
        try admit(self.ptr, admission);
        return true;
    }

    pub fn cancelSnapshotUpload(self: SnapshotUploadHandler, admission: SnapshotUploadAdmission) void {
        if (self.vtable.cancel_snapshot_upload) |cancel| cancel(self.ptr, admission);
    }
};

pub const HttpServer = struct {
    const snapshot_protocol_header = "x-antfly-raft-snapshot-protocol";
    const snapshot_operation_header = "x-antfly-raft-snapshot-operation";
    const snapshot_offset_header = "x-antfly-raft-snapshot-offset";
    const snapshot_chunk_length_header = "x-antfly-raft-snapshot-chunk-length";

    alloc: std.mem.Allocator,
    cfg: HttpServerConfig,
    codec: raft_engine.runtime.MessageCodec,
    batch_handler: BatchHandler,
    snapshot_store: ?SnapshotStore = null,
    snapshot_upload_handler: ?SnapshotUploadHandler = null,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpServerConfig,
        codec: raft_engine.runtime.MessageCodec,
        batch_handler: BatchHandler,
        snapshot_store: ?SnapshotStore,
        snapshot_upload_handler: ?SnapshotUploadHandler,
    ) HttpServer {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .codec = codec,
            .batch_handler = batch_handler,
            .snapshot_store = snapshot_store,
            .snapshot_upload_handler = snapshot_upload_handler,
        };
    }

    pub fn start(self: *HttpServer) !void {
        _ = self;
    }

    pub fn executor(self: *HttpServer) common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn handle(self: *HttpServer, req: common.HttpRequest) !common.HttpResponse {
        if (routes.Routes.matchSnapshotUploadV2(req.uri) != null or
            routes.Routes.matchSnapshotFetchV2(req.uri) != null)
        {
            const version = req.header(snapshot_protocol_header) orelse
                return error.MissingSnapshotTransferHeader;
            if (!std.mem.eql(u8, version, "2"))
                return error.UnsupportedSnapshotTransferProtocol;
            return (try self.handleChunkedSnapshot(req)) orelse
                return error.InvalidSnapshotTransferOperation;
        }
        // V1 routes never interpret a framed transfer operation. Keeping the
        // namespaces disjoint prevents a mismatched client from decoding a v2
        // manifest as a legacy snapshot body.
        if (req.header(snapshot_protocol_header) != null)
            return error.UnsupportedSnapshotTransferRoute;
        if (std.mem.eql(u8, req.uri, routes.Routes.health) and req.method == .GET) {
            return .{
                .status = 200,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "ok"),
            };
        }
        if (std.mem.eql(u8, req.uri, routes.Routes.capabilities) and req.method == .GET) {
            const content_type = try self.alloc.dupe(u8, "application/json");
            errdefer self.alloc.free(content_type);
            return .{
                .status = 200,
                .content_type = content_type,
                .body = try std.fmt.allocPrint(
                    self.alloc,
                    "{{\"data_raft_batch_protocol_version\":{d},\"snapshot_transfer_protocol_version\":{d},\"snapshot_transfer_route_version\":{d}}}",
                    .{
                        data_raft_protocol.batch_protocol_version,
                        if (self.snapshot_store) |store|
                            if (store.supportsChunkedTransfer()) snapshot_transfer.protocol_version else 0
                        else
                            0,
                        if (self.snapshot_store) |store|
                            if (store.supportsChunkedTransfer()) snapshot_transfer.http_route_version else 0
                        else
                            0,
                    },
                ),
            };
        }
        if (std.mem.eql(u8, req.uri, routes.Routes.raft_batch) and req.method == .POST) {
            if (req.body.len > self.cfg.max_request_bytes) return error.RequestTooLarge;
            const decoded = try self.codec.decodeFrame(self.alloc, .{
                .bytes = @constCast(req.body),
                .media_type = req.content_type orelse "application/octet-stream",
            });
            defer self.codec.freeDecoded(self.alloc, decoded);
            switch (decoded) {
                .raft_peer_batch => |batch| try self.batch_handler.handlePeerBatch(batch),
                else => return error.UnsupportedFrame,
            }
            return .{
                .status = 202,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "accepted"),
            };
        }
        if (req.method == .POST) {
            if (routes.Routes.matchSnapshotUpload(req.uri)) |snapshot_id| {
                const header_state = snapshotUploadHeaderState(req);
                if (self.snapshot_upload_handler != null and
                    (header_state == .partial or (header_state == .absent and self.snapshot_store == null)))
                {
                    return error.InvalidSnapshotUploadHeaders;
                }

                if (self.snapshot_upload_handler) |handler| {
                    if (header_state == .complete) {
                        var live_upload = (try parseSnapshotUpload(self.alloc, req)) orelse unreachable;
                        errdefer live_upload.snapshot.deinit(self.alloc);
                        const upload = live_upload;
                        live_upload.snapshot = .{};
                        try handler.handleSnapshotUpload(upload);
                    } else if (self.snapshot_store) |store| {
                        try store.putSnapshot(self.alloc, snapshot_id, req.body);
                    }
                } else if (self.snapshot_store) |store| {
                    try store.putSnapshot(self.alloc, snapshot_id, req.body);
                } else {
                    return error.MissingSnapshotStore;
                }
                return .{
                    .status = 201,
                    .content_type = try self.alloc.dupe(u8, "text/plain"),
                    .body = try self.alloc.dupe(u8, "stored"),
                };
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchSnapshotFetch(req.uri)) |snapshot_id| {
                const store = self.snapshot_store orelse return error.MissingSnapshotStore;
                const body = store.getSnapshot(self.alloc, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => return try self.textResponse(404, "snapshot artifact not found"),
                    else => return err,
                };
                errdefer self.alloc.free(body);
                return .{
                    .status = 200,
                    .content_type = try self.alloc.dupe(u8, "application/x-antflydb-raft-snapshot"),
                    .body = body,
                };
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchSnapshotFetch(req.uri)) |snapshot_id| {
                const store = self.snapshot_store orelse return error.MissingSnapshotStore;
                const release = store.vtable.release_snapshot orelse
                    return error.UnsupportedSnapshotRelease;
                release(store.ptr, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                return try self.textResponse(204, "");
            }
        }
        return .{
            .status = 404,
            .content_type = try self.alloc.dupe(u8, "text/plain"),
            .body = try self.alloc.dupe(u8, "not found"),
        };
    }

    fn handleChunkedSnapshot(self: *HttpServer, req: common.HttpRequest) !?common.HttpResponse {
        const store = self.snapshot_store orelse return error.MissingSnapshotStore;
        // A v1 store may receive a v2 manifest probe during a rolling upgrade.
        // Fall through to its ordinary fetch response so the client can detect
        // the legacy content type without treating capability discovery as an
        // application failure.
        if (!store.supportsChunkedTransfer()) return null;
        const operation = req.header(snapshot_operation_header) orelse return error.InvalidSnapshotTransferOperation;

        if (routes.Routes.matchSnapshotUploadV2(req.uri)) |snapshot_id| {
            if (std.mem.eql(u8, operation, "begin") and req.method == .POST) {
                if (req.body.len > self.cfg.max_request_bytes) return error.RequestTooLarge;
                var manifest = try snapshot_transfer.decode(self.alloc, req.body);
                defer manifest.deinit(self.alloc);
                try validateManifestRequest(req, manifest);
                try store.vtable.begin_chunked_snapshot.?(store.ptr, manifest, snapshot_id);
                return try self.textResponse(201, "snapshot upload initialized");
            }
            if (std.mem.eql(u8, operation, "chunk") and req.method == .PUT) {
                if (req.body.len > self.cfg.max_request_bytes) return error.RequestTooLarge;
                const offset = try parseRequiredU64Header(req, snapshot_offset_header);
                var manifest = store.vtable.get_snapshot_upload_manifest.?(store.ptr, self.alloc, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => return try self.textResponse(409, "snapshot upload not initialized"),
                    else => return err,
                };
                defer manifest.deinit(self.alloc);
                try validateManifestRequest(req, manifest);
                try store.vtable.put_snapshot_chunk.?(store.ptr, snapshot_id, offset, req.body);
                return try self.textResponse(202, "snapshot chunk accepted");
            }
            if (std.mem.eql(u8, operation, "commit") and req.method == .POST) {
                var manifest = store.vtable.get_snapshot_upload_manifest.?(store.ptr, self.alloc, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => return try self.textResponse(409, "snapshot upload not initialized"),
                    else => return err,
                };
                defer manifest.deinit(self.alloc);
                try validateManifestRequest(req, manifest);
                // Artifact publication and live Raft delivery share the v2
                // upload machinery but have different commit semantics. An
                // artifact must remain committed for a later bootstrap fetch;
                // only a live delivery is materialized into the local host.
                const handler = self.commitHandlerForManifest(manifest);
                const admission: SnapshotUploadAdmission = .{
                    .group_id = manifest.group_id,
                    .to = manifest.to,
                    .data_len = manifest.data_len,
                };
                var admitted = if (handler) |live_handler|
                    try live_handler.admitSnapshotUpload(admission)
                else
                    false;
                errdefer if (admitted) handler.?.cancelSnapshotUpload(admission);
                var snapshot = try store.vtable.commit_chunked_snapshot.?(
                    store.ptr,
                    self.alloc,
                    snapshot_id,
                    handler != null,
                );
                errdefer if (snapshot) |*owned| owned.deinit(self.alloc);
                if (handler) |live_handler| {
                    const upload: SnapshotUpload = .{
                        .group_id = manifest.group_id,
                        .from = manifest.from,
                        .to = manifest.to,
                        .term = manifest.request_term,
                        .snapshot = snapshot orelse return error.SnapshotMaterializationMissing,
                        .admission_reserved = admitted,
                    };
                    snapshot = null;
                    admitted = false;
                    try live_handler.handleSnapshotUpload(upload);
                    store.vtable.release_chunked_snapshot.?(store.ptr, snapshot_id) catch |err| {
                        std.log.warn("committed live snapshot cleanup deferred snapshot_id={s} err={s}", .{
                            snapshot_id,
                            @errorName(err),
                        });
                    };
                } else if (snapshot) |*unexpected| {
                    // Defensive ownership handling for custom stores that
                    // materialize despite the explicit artifact-only request.
                    unexpected.deinit(self.alloc);
                    snapshot = null;
                }
                return try self.textResponse(201, "snapshot committed");
            }
            if (std.mem.eql(u8, operation, "abort") and req.method == .DELETE) {
                var manifest = store.vtable.get_snapshot_upload_manifest.?(store.ptr, self.alloc, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => return try self.textResponse(204, ""),
                    else => return err,
                };
                defer manifest.deinit(self.alloc);
                try validateManifestRequest(req, manifest);
                if (store.vtable.abort_chunked_snapshot) |abort| try abort(store.ptr, snapshot_id);
                return try self.textResponse(204, "");
            }
            return error.InvalidSnapshotTransferOperation;
        }

        if (routes.Routes.matchSnapshotFetchV2(req.uri)) |snapshot_id| {
            if (std.mem.eql(u8, operation, "manifest") and req.method == .GET) {
                var manifest = store.vtable.get_snapshot_manifest.?(store.ptr, self.alloc, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => return try self.textResponse(404, "snapshot artifact not found"),
                    else => return err,
                };
                defer manifest.deinit(self.alloc);
                const body = try snapshot_transfer.encode(self.alloc, manifest);
                errdefer self.alloc.free(body);
                return .{
                    .status = 200,
                    .content_type = try self.alloc.dupe(u8, "application/x-antflydb-raft-snapshot-manifest-v2"),
                    .body = body,
                };
            }
            if (std.mem.eql(u8, operation, "chunk") and req.method == .GET) {
                const offset = try parseRequiredU64Header(req, snapshot_offset_header);
                const requested_len = try parseRequiredU64Header(req, snapshot_chunk_length_header);
                const max_len = std.math.cast(usize, requested_len) orelse return error.InvalidSnapshotChunkLength;
                if (max_len == 0 or max_len > snapshot_transfer.max_chunk_bytes)
                    return error.InvalidSnapshotChunkLength;
                const body = store.vtable.get_snapshot_chunk.?(store.ptr, self.alloc, snapshot_id, offset, max_len) catch |err| switch (err) {
                    error.FileNotFound => return try self.textResponse(404, "snapshot artifact not found"),
                    else => return err,
                };
                errdefer self.alloc.free(body);
                return .{
                    .status = 200,
                    .content_type = try self.alloc.dupe(u8, "application/x-antflydb-raft-snapshot-chunk-v2"),
                    .body = body,
                };
            }
            if (std.mem.eql(u8, operation, "release") and req.method == .DELETE) {
                store.vtable.release_chunked_snapshot.?(store.ptr, snapshot_id) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                return try self.textResponse(204, "");
            }
            return error.InvalidSnapshotTransferOperation;
        }
        return null;
    }

    fn textResponse(self: *HttpServer, status: u16, body: []const u8) !common.HttpResponse {
        const content_type = try self.alloc.dupe(u8, "text/plain");
        errdefer self.alloc.free(content_type);
        return .{
            .status = status,
            .content_type = content_type,
            .body = try self.alloc.dupe(u8, body),
        };
    }

    fn commitHandlerForManifest(
        self: *const HttpServer,
        manifest: snapshot_transfer.Manifest,
    ) ?SnapshotUploadHandler {
        return switch (manifest.purpose()) {
            .bootstrap_artifact => null,
            .live_install => self.snapshot_upload_handler,
        };
    }

    fn parseRequiredU64Header(req: common.HttpRequest, name: []const u8) !u64 {
        const encoded = req.header(name) orelse return error.MissingSnapshotTransferHeader;
        return std.fmt.parseInt(u64, encoded, 10) catch return error.InvalidSnapshotTransferHeader;
    }

    fn validateManifestRequest(req: common.HttpRequest, manifest: snapshot_transfer.Manifest) !void {
        if (try parseRequiredU64Header(req, "x-antfly-raft-group-id") != manifest.group_id or
            try parseRequiredU64Header(req, "x-antfly-raft-from-node-id") != manifest.from or
            try parseRequiredU64Header(req, "x-antfly-raft-to-node-id") != manifest.to or
            try parseRequiredU64Header(req, "x-antfly-raft-term") != manifest.request_term)
        {
            return error.SnapshotTransferIdentityMismatch;
        }
    }

    fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const self: *HttpServer = @ptrCast(@alignCast(ptr));
        var response = self.handle(req) catch |err| switch (err) {
            error.SnapshotAdmissionBackpressure => try self.textResponse(503, "snapshot admission temporarily unavailable"),
            error.SnapshotArtifactQuotaExceeded => try self.textResponse(507, "snapshot artifact quota exceeded"),
            error.RequestTooLarge, error.SnapshotTooLarge => try self.textResponse(413, "snapshot request too large"),
            else => return err,
        };
        if (response.owner_allocator == null) response.owner_allocator = self.alloc;
        return response;
    }
};

const SnapshotUploadHeaderState = enum {
    absent,
    partial,
    complete,
};

fn snapshotUploadHeaderState(req: common.HttpRequest) SnapshotUploadHeaderState {
    const group_present = req.header("x-antfly-raft-group-id") != null;
    const from_present = req.header("x-antfly-raft-from-node-id") != null;
    const to_present = req.header("x-antfly-raft-to-node-id") != null;
    const term_present = req.header("x-antfly-raft-term") != null;
    var present_count: usize = 0;
    if (group_present) present_count += 1;
    if (from_present) present_count += 1;
    if (to_present) present_count += 1;
    if (term_present) present_count += 1;
    if (present_count == 0) return .absent;
    if (present_count == 4) return .complete;
    return .partial;
}

fn parseSnapshotUpload(alloc: std.mem.Allocator, req: common.HttpRequest) !?SnapshotUpload {
    const group_header = req.header("x-antfly-raft-group-id") orelse return null;
    const from_header = req.header("x-antfly-raft-from-node-id") orelse return null;
    const to_header = req.header("x-antfly-raft-to-node-id") orelse return null;
    const term_header = req.header("x-antfly-raft-term") orelse return null;
    return .{
        .group_id = try std.fmt.parseInt(u64, group_header, 10),
        .from = try std.fmt.parseInt(u64, from_header, 10),
        .to = try std.fmt.parseInt(u64, to_header, 10),
        .term = try std.fmt.parseInt(u64, term_header, 10),
        .snapshot = try http_snapshot.HttpSnapshotTransport.decodeSnapshotEnvelope(alloc, req.body),
    };
}

test "http server module compiles" {
    _ = HttpServerConfig;
    _ = BatchHandler;
    _ = SnapshotStore;
    _ = HttpServer;
}

test "http server exposes health and data raft protocol capabilities" {
    const Handler = struct {
        fn iface(_: *@This()) BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    var handler = Handler{};
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), handler.iface(), null, null);
    const executor = server.executor();
    var resp = try executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = routes.Routes.health,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);

    var capabilities = try executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = routes.Routes.capabilities,
    });
    defer capabilities.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), capabilities.status);
    const Parsed = struct {
        data_raft_batch_protocol_version: u16,
        snapshot_transfer_protocol_version: u32,
        snapshot_transfer_route_version: u32,
    };
    const parsed = try std.json.parseFromSlice(Parsed, std.testing.allocator, capabilities.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(
        data_raft_protocol.batch_protocol_version,
        parsed.value.data_raft_batch_protocol_version,
    );
    try std.testing.expectEqual(@as(u32, 0), parsed.value.snapshot_transfer_protocol_version);
    try std.testing.expectEqual(@as(u32, 0), parsed.value.snapshot_transfer_route_version);
}

test "http server advertises isolated snapshot routes only for complete stores" {
    const Handler = struct {
        fn handlePeerBatch(_: *anyopaque, _: raft_engine.runtime.transport_iface.PeerBatch) !void {}
    };
    const Store = struct {
        fn iface(self: *@This()) SnapshotStore {
            return .{ .ptr = self, .vtable = &.{
                .put_snapshot = put,
                .get_snapshot = get,
                .begin_chunked_snapshot = begin,
                .put_snapshot_chunk = putChunk,
                .commit_chunked_snapshot = commit,
                .get_snapshot_upload_manifest = getManifest,
                .get_snapshot_manifest = getManifest,
                .get_snapshot_chunk = getChunk,
                .abort_chunked_snapshot = discard,
                .release_chunked_snapshot = discard,
            } };
        }
        fn put(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {}
        fn get(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8) ![]u8 {
            return try alloc.alloc(u8, 0);
        }
        fn begin(_: *anyopaque, _: snapshot_transfer.Manifest, _: []const u8) !void {}
        fn putChunk(_: *anyopaque, _: []const u8, _: u64, _: []const u8) !void {}
        fn commit(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: bool) !?raft_engine.core.types.Snapshot {
            return null;
        }
        fn getManifest(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !snapshot_transfer.Manifest {
            return error.NotUsed;
        }
        fn getChunk(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return try alloc.alloc(u8, 0);
        }
        fn discard(_: *anyopaque, _: []const u8) !void {}
    };

    var handler_context: u8 = 0;
    const handler: BatchHandler = .{ .ptr = &handler_context, .vtable = &.{ .handle_peer_batch = Handler.handlePeerBatch } };
    var store = Store{};
    var server = HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        handler,
        store.iface(),
        null,
    );
    var capabilities = try server.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = routes.Routes.capabilities,
    });
    defer capabilities.deinit(std.testing.allocator);
    const Parsed = struct {
        snapshot_transfer_protocol_version: u32,
        snapshot_transfer_route_version: u32,
    };
    const parsed = try std.json.parseFromSlice(Parsed, std.testing.allocator, capabilities.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(
        snapshot_transfer.protocol_version,
        parsed.value.snapshot_transfer_protocol_version,
    );
    try std.testing.expectEqual(
        snapshot_transfer.http_route_version,
        parsed.value.snapshot_transfer_route_version,
    );

    const UploadHandler = struct {
        fn handle(_: *anyopaque, _: SnapshotUpload) !void {}
    };
    server.snapshot_upload_handler = .{
        .ptr = &handler_context,
        .vtable = &.{ .handle_snapshot_upload = UploadHandler.handle },
    };
    const artifact_manifest: snapshot_transfer.Manifest = .{
        .group_id = 1,
        .from = 0,
        .to = 7,
        .request_term = 0,
        .metadata = .{},
        .data_len = 0,
        .digest = snapshot_transfer.digest(""),
    };
    try std.testing.expect(server.commitHandlerForManifest(artifact_manifest) == null);
    var live_manifest = artifact_manifest;
    live_manifest.from = 6;
    try std.testing.expect(server.commitHandlerForManifest(live_manifest) != null);
}

test "http server decodes raft batch requests and dispatches them" {
    const Handler = struct {
        seen: usize = 0,

        fn iface(self: *@This()) BatchHandler {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += batch.groups.len;
        }
    };

    var handler = Handler{};
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), handler.iface(), null, null);

    const msg = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 3,
    };
    const batch = raft_engine.runtime.transport_iface.PeerBatch{
        .peer_id = 2,
        .groups = (&[_]raft_engine.runtime.transport_iface.GroupMessageBatch{
            .{
                .group_id = 55,
                .messages = (&[_]raft_engine.core.Message{msg})[0..],
            },
        })[0..],
    };
    const frame = try raft_engine.runtime.BinaryCodec.codec().encodePeerBatch(std.testing.allocator, batch);
    defer raft_engine.runtime.BinaryCodec.codec().freeFrame(std.testing.allocator, frame);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.raft_batch,
        .content_type = frame.media_type,
        .body = frame.bytes,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expectEqual(@as(usize, 1), handler.seen);
}

test "http server stores and fetches snapshot bodies by route" {
    const Store = struct {
        body: ?[]u8 = null,

        fn iface(self: *@This()) SnapshotStore {
            return .{
                .ptr = self,
                .vtable = &.{
                    .put_snapshot = putSnapshot,
                    .get_snapshot = getSnapshot,
                    .release_snapshot = releaseSnapshot,
                },
            };
        }

        fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body) |existing| alloc.free(existing);
            self.body = try alloc.dupe(u8, body);
        }

        fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try alloc.dupe(u8, self.body orelse return error.FileNotFound);
        }

        fn releaseSnapshot(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body) |body| std.testing.allocator.free(body);
            self.body = null;
        }
    };

    const Noop = struct {
        fn iface(_: *@This()) BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    var store = Store{};
    defer if (store.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), noop.iface(), store.iface(), null);

    const missing_path = try routes.Routes.snapshotFetchPath(std.testing.allocator, "missing");
    defer std.testing.allocator.free(missing_path);
    var missing = try server.handle(.{ .method = .GET, .uri = missing_path });
    defer missing.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), missing.status);

    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-7");
    defer std.testing.allocator.free(upload_path);
    var upload = try server.handle(.{
        .method = .POST,
        .uri = upload_path,
        .body = "snapshot-body",
    });
    defer upload.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), upload.status);

    const fetch_path = try routes.Routes.snapshotFetchPath(std.testing.allocator, "snap-7");
    defer std.testing.allocator.free(fetch_path);
    var fetch = try server.handle(.{
        .method = .GET,
        .uri = fetch_path,
    });
    defer fetch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), fetch.status);
    try std.testing.expectEqualStrings("snapshot-body", fetch.body);

    var release = try server.handle(.{ .method = .DELETE, .uri = fetch_path });
    defer release.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), release.status);
    var released = try server.handle(.{ .method = .GET, .uri = fetch_path });
    defer released.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), released.status);
}

test "http server dispatches live snapshot uploads to handler" {
    const Noop = struct {
        fn iface(_: *@This()) BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    const Handler = struct {
        seen: usize = 0,
        group_id: u64 = 0,
        from: u64 = 0,
        to: u64 = 0,
        term: u64 = 0,
        index: u64 = 0,
        data: ?[]u8 = null,

        fn iface(self: *@This()) SnapshotUploadHandler {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_snapshot_upload = handleSnapshotUpload,
                },
            };
        }

        fn deinit(self: *@This()) void {
            if (self.data) |data| std.testing.allocator.free(data);
            self.* = undefined;
        }

        fn handleSnapshotUpload(ptr: *anyopaque, upload: SnapshotUpload) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = upload;
            defer owned.snapshot.deinit(std.testing.allocator);
            self.seen += 1;
            self.group_id = upload.group_id;
            self.from = upload.from;
            self.to = upload.to;
            self.term = upload.term;
            self.index = upload.snapshot.metadata.index;
            if (self.data) |data| std.testing.allocator.free(data);
            self.data = try std.testing.allocator.dupe(u8, upload.snapshot.data);
        }
    };

    const Store = struct {
        put_calls: usize = 0,

        fn iface(self: *@This()) SnapshotStore {
            return .{
                .ptr = self,
                .vtable = &.{
                    .put_snapshot = putSnapshot,
                    .get_snapshot = getSnapshot,
                },
            };
        }

        fn putSnapshot(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.put_calls += 1;
        }

        fn getSnapshot(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedFetch;
        }
    };

    var noop = Noop{};
    var store = Store{};
    var handler = Handler{};
    defer handler.deinit();
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), noop.iface(), store.iface(), handler.iface());

    var voters = [_]u64{ 1, 2, 3 };
    const snapshot_body = try http_snapshot.HttpSnapshotTransport.encodeSnapshotEnvelope(std.testing.allocator, .{
        .metadata = .{
            .index = 42,
            .term = 7,
            .conf_state = .{ .voters = voters[0..] },
        },
        .data = @constCast("live-snapshot"),
    });
    defer std.testing.allocator.free(snapshot_body);

    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-live");
    defer std.testing.allocator.free(upload_path);
    const headers = [_]common.RequestHeader{
        .{ .name = "x-antfly-raft-group-id", .value = "91" },
        .{ .name = "x-antfly-raft-from-node-id", .value = "1" },
        .{ .name = "x-antfly-raft-to-node-id", .value = "2" },
        .{ .name = "x-antfly-raft-term", .value = "8" },
    };
    var resp = try server.handle(.{
        .method = .POST,
        .uri = upload_path,
        .headers = &headers,
        .body = snapshot_body,
    });
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expectEqual(@as(usize, 1), handler.seen);
    try std.testing.expectEqual(@as(u64, 91), handler.group_id);
    try std.testing.expectEqual(@as(u64, 1), handler.from);
    try std.testing.expectEqual(@as(u64, 2), handler.to);
    try std.testing.expectEqual(@as(u64, 8), handler.term);
    try std.testing.expectEqual(@as(u64, 42), handler.index);
    try std.testing.expectEqualStrings("live-snapshot", handler.data.?);
    try std.testing.expectEqual(@as(usize, 0), store.put_calls);
}

test "http server rejects malformed live snapshot upload metadata" {
    const Noop = struct {
        fn iface(_: *@This()) BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    const Handler = struct {
        seen: usize = 0,

        fn iface(self: *@This()) SnapshotUploadHandler {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_snapshot_upload = handleSnapshotUpload,
                },
            };
        }

        fn handleSnapshotUpload(ptr: *anyopaque, upload: SnapshotUpload) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = upload;
            defer owned.snapshot.deinit(std.testing.allocator);
            self.seen += 1;
        }
    };

    const Store = struct {
        put_calls: usize = 0,

        fn iface(self: *@This()) SnapshotStore {
            return .{
                .ptr = self,
                .vtable = &.{
                    .put_snapshot = putSnapshot,
                    .get_snapshot = getSnapshot,
                },
            };
        }

        fn putSnapshot(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.put_calls += 1;
        }

        fn getSnapshot(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedFetch;
        }
    };

    var noop = Noop{};
    var handler = Handler{};
    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-live");
    defer std.testing.allocator.free(upload_path);

    var handler_only_server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), noop.iface(), null, handler.iface());
    try std.testing.expectError(error.InvalidSnapshotUploadHeaders, handler_only_server.handle(.{
        .method = .POST,
        .uri = upload_path,
        .body = "snapshot-body",
    }));
    try std.testing.expectEqual(@as(usize, 0), handler.seen);

    var store = Store{};
    var store_and_handler_server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), noop.iface(), store.iface(), handler.iface());
    const partial_headers = [_]common.RequestHeader{
        .{ .name = "x-antfly-raft-group-id", .value = "91" },
    };
    try std.testing.expectError(error.InvalidSnapshotUploadHeaders, store_and_handler_server.handle(.{
        .method = .POST,
        .uri = upload_path,
        .headers = &partial_headers,
        .body = "snapshot-body",
    }));
    try std.testing.expectEqual(@as(usize, 0), store.put_calls);
    try std.testing.expectEqual(@as(usize, 0), handler.seen);

    const v2_headers = [_]common.RequestHeader{
        .{ .name = HttpServer.snapshot_protocol_header, .value = "2" },
        .{ .name = HttpServer.snapshot_operation_header, .value = "begin" },
    };
    try std.testing.expectError(error.UnsupportedSnapshotTransferRoute, store_and_handler_server.handle(.{
        .method = .POST,
        .uri = upload_path,
        .headers = &v2_headers,
        .body = "snapshot-body",
    }));
    try std.testing.expectEqual(@as(usize, 0), store.put_calls);
}
