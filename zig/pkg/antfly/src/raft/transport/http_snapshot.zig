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
const builtin = @import("builtin");
const raft_engine = @import("raft_engine");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const common_http = @import("../../common/http/mod.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const common = @import("http_common.zig");
const http_server = @import("http_server.zig");
const routes = @import("routes.zig");
const snapshot_transfer = @import("snapshot_transfer.zig");

pub const HttpSnapshotConfig = struct {
    root_dir: []const u8,
    chunk_size: usize = snapshot_transfer.max_chunk_bytes,
    /// Bounded request fan-out for v2 payload chunks. Executors that do not
    /// explicitly advertise concurrent safety remain serialized.
    max_parallel_chunks: usize = 8,
    /// Per-request idle/transport ceiling. The complete transfer is also
    /// fenced by transfer_timeout_ms so successful chunks cannot reset the
    /// operation budget indefinitely.
    request_timeout_ms: u32 = 30_000,
    transfer_timeout_ms: u32 = 5 * 60_000,
    legacy_max_snapshot_bytes: usize = 8 * 1024 * 1024,
    /// Common request/response ceiling for the non-streaming v1 envelope. The
    /// default matches StdHttpExecutorConfig.max_response_bytes so a snapshot
    /// accepted by a default host is fetchable by another default host.
    legacy_fallback_max_request_bytes: usize = 4 * 1024 * 1024,
    max_snapshot_bytes: usize = 1 << 30,
    /// Aggregate logical bytes reserved by local download artifacts. This is
    /// separate from Raft's retained-snapshot admission because a verified
    /// read-only mapping continues to consume filesystem capacity until its
    /// final Raft reference is released.
    max_staging_bytes: usize = 2 << 30,
};

pub const max_parallel_chunk_workers: usize = 32;

var snapshot_fetch_sequence = std.atomic.Value(u64).init(1);
const snapshot_fetch_staging_prefix = ".antfly-snapshot-fetch-";
const snapshot_fetch_staging_suffix = ".part";

/// The snapshot root is an exclusive host resource. On construction no local
/// transfer can still own one of these private artifacts, so every matching
/// file is crash residue and may be removed before new admissions begin.
fn scavengeFetchArtifacts(
    io: std.Io,
    root_dir: []const u8,
) !void {
    try fs_paths.createDirPathPortable(io, root_dir);
    var dir = try std.Io.Dir.cwd().openDir(io, root_dir, .{ .iterate = true });
    defer dir.close(io);
    var deleted = false;
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .file or
            !std.mem.startsWith(u8, entry.name, snapshot_fetch_staging_prefix) or
            !std.mem.endsWith(u8, entry.name, snapshot_fetch_staging_suffix))
            continue;
        dir.deleteFile(io, entry.name) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        deleted = true;
    }
    if (deleted) try fs_paths.syncDirPortable(io, root_dir);
}

const MappedFetchOwner = struct {
    alloc: std.mem.Allocator,
    mapped: []align(std.heap.page_size_min) u8,
    staging_budget: *SnapshotStagingBudget,
    reserved_bytes: usize,

    fn release(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        std.posix.munmap(self.mapped);
        self.staging_budget.release(self.reserved_bytes);
        self.staging_budget.releaseRef();
        const alloc = self.alloc;
        alloc.destroy(self);
    }
};

const SnapshotStagingBudget = struct {
    alloc: std.mem.Allocator,
    limit: usize,
    references: std.atomic.Value(usize) = .init(1),
    reserved: std.atomic.Value(usize) = .init(0),

    fn retain(self: *@This()) void {
        const previous = self.references.fetchAdd(1, .monotonic);
        if (previous == std.math.maxInt(usize))
            @panic("snapshot staging budget reference count overflow");
    }

    fn releaseRef(self: *@This()) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        std.debug.assert(self.reserved.load(.acquire) == 0);
        self.alloc.destroy(self);
    }

    fn reserve(self: *@This(), bytes: usize) !void {
        if (bytes > self.limit) return error.SnapshotStagingBackpressure;
        var current = self.reserved.load(.acquire);
        while (true) {
            if (current > self.limit - bytes)
                return error.SnapshotStagingBackpressure;
            if (self.reserved.cmpxchgWeak(
                current,
                current + bytes,
                .acq_rel,
                .acquire,
            )) |observed| {
                current = observed;
                continue;
            }
            return;
        }
    }

    fn release(self: *@This(), bytes: usize) void {
        if (bytes == 0) return;
        const previous = self.reserved.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
    }
};

pub const SnapshotTargetResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_upload_uri: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            node_id: u64,
            snapshot_id: []const u8,
        ) anyerror![]u8,
        resolve_base_uri: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            node_id: u64,
        ) anyerror![]u8 = null,
    };

    pub fn resolveUploadUri(
        self: SnapshotTargetResolver,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
        snapshot_id: []const u8,
    ) ![]u8 {
        return try self.vtable.resolve_upload_uri(self.ptr, alloc, group_id, node_id, snapshot_id);
    }

    pub fn resolveBaseUri(
        self: SnapshotTargetResolver,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
    ) ![]u8 {
        const resolve = self.vtable.resolve_base_uri orelse return error.SnapshotCapabilityEndpointUnavailable;
        return try resolve(self.ptr, alloc, group_id, node_id);
    }
};

pub const SnapshotFetch = struct {
    group_id: u64,
    snapshot_id: []const u8,
    uri: []const u8,
};

pub const HttpSnapshotTransport = struct {
    alloc: std.mem.Allocator,
    cfg: HttpSnapshotConfig,
    executor: common.RequestExecutor,
    resolver: ?SnapshotTargetResolver = null,
    artifact_io: std.Io,
    owned_artifact_io: ?*std.Io.Threaded = null,
    staging_budget: *SnapshotStagingBudget,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpSnapshotConfig,
        executor: common.RequestExecutor,
        resolver: ?SnapshotTargetResolver,
    ) !HttpSnapshotTransport {
        try validateConfig(cfg);
        const io_impl = try alloc.create(std.Io.Threaded);
        errdefer alloc.destroy(io_impl);
        io_impl.* = threaded_io_limits.initService(alloc);
        errdefer io_impl.deinit();
        var snapshot_transport = try initShared(alloc, cfg, executor, resolver, io_impl.io());
        snapshot_transport.owned_artifact_io = io_impl;
        return snapshot_transport;
    }

    pub fn initShared(
        alloc: std.mem.Allocator,
        cfg: HttpSnapshotConfig,
        executor: common.RequestExecutor,
        resolver: ?SnapshotTargetResolver,
        artifact_io: std.Io,
    ) !HttpSnapshotTransport {
        try validateConfig(cfg);
        const staging_budget = try alloc.create(SnapshotStagingBudget);
        errdefer alloc.destroy(staging_budget);
        staging_budget.* = .{ .alloc = alloc, .limit = cfg.max_staging_bytes };
        try scavengeFetchArtifacts(artifact_io, cfg.root_dir);
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .executor = executor,
            .resolver = resolver,
            .artifact_io = artifact_io,
            .staging_budget = staging_budget,
        };
    }

    pub fn deinit(self: *HttpSnapshotTransport) void {
        // Mapped snapshots may outlive a standalone transport. Their owner
        // retains this control block until the final mapping and reservation
        // are released, avoiding a shutdown-order UAF in embedders.
        self.staging_budget.releaseRef();
        if (self.owned_artifact_io) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.* = undefined;
    }

    pub fn validateConfig(cfg: HttpSnapshotConfig) !void {
        if (cfg.root_dir.len == 0 or
            cfg.chunk_size < snapshot_transfer.min_chunk_bytes or
            cfg.chunk_size > snapshot_transfer.max_chunk_bytes or
            cfg.max_parallel_chunks == 0 or
            cfg.max_parallel_chunks > max_parallel_chunk_workers or
            cfg.request_timeout_ms == 0 or
            cfg.transfer_timeout_ms < cfg.request_timeout_ms or
            cfg.max_snapshot_bytes == 0 or
            cfg.max_staging_bytes < cfg.max_snapshot_bytes or
            cfg.legacy_fallback_max_request_bytes == 0)
            return error.InvalidSnapshotTransferLimits;
    }

    fn transferDeadlineNs(self: *const HttpSnapshotTransport) u64 {
        return platform_time.monotonicNs() +|
            @as(u64, self.cfg.transfer_timeout_ms) * std.time.ns_per_ms;
    }

    fn requestTimeoutUntil(self: *const HttpSnapshotTransport, deadline_ns: u64) !u32 {
        const now_ns = platform_time.monotonicNs();
        if (now_ns >= deadline_ns) return error.SnapshotTransferTimeout;
        const remaining_ns = deadline_ns - now_ns;
        const remaining_ms_u64 = @max(
            @as(u64, 1),
            @divTrunc(remaining_ns +| (std.time.ns_per_ms - 1), std.time.ns_per_ms),
        );
        const remaining_ms = std.math.cast(u32, remaining_ms_u64) orelse std.math.maxInt(u32);
        return @min(self.cfg.request_timeout_ms, remaining_ms);
    }

    pub fn transport(self: *HttpSnapshotTransport) raft_engine.runtime.SnapshotTransport {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_snapshot = sendSnapshot,
                .fetch_snapshot = fetchSnapshot,
            },
        };
    }

    pub fn fetch(self: *HttpSnapshotTransport, req: SnapshotFetch) ![]u8 {
        var resp = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = req.uri,
            .timeout_ms = self.cfg.request_timeout_ms,
        });
        errdefer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        const body = resp.body;
        resp.body = &.{};
        resp.deinit(self.alloc);
        return body;
    }

    fn sendSnapshot(ptr: *anyopaque, req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest) !void {
        const self: *HttpSnapshotTransport = @ptrCast(@alignCast(ptr));
        if (req.snapshot.data.len > self.cfg.max_snapshot_bytes) return error.SnapshotTooLarge;
        const snapshot_id = if (req.locator) |locator|
            try self.alloc.dupe(u8, locator.snapshot_id)
        else
            try std.fmt.allocPrint(self.alloc, "{d}-{d}-{d}-{d}-{d}", .{
                req.group_id,
                req.from,
                req.to,
                req.snapshot.metadata.index,
                req.snapshot.metadata.term,
            });
        defer self.alloc.free(snapshot_id);

        const uri = try self.resolveUploadUri(req, snapshot_id);
        defer self.alloc.free(uri);

        var group_id_buf: [32]u8 = undefined;
        var from_buf: [32]u8 = undefined;
        var to_buf: [32]u8 = undefined;
        var term_buf: [32]u8 = undefined;
        const group_id = try std.fmt.bufPrint(&group_id_buf, "{d}", .{req.group_id});
        const from = try std.fmt.bufPrint(&from_buf, "{d}", .{req.from});
        const to = try std.fmt.bufPrint(&to_buf, "{d}", .{req.to});
        const term = try std.fmt.bufPrint(&term_buf, "{d}", .{req.term});
        const live_headers = [_]common.RequestHeader{
            .{ .name = "x-antfly-raft-group-id", .value = group_id },
            .{ .name = "x-antfly-raft-from-node-id", .value = from },
            .{ .name = "x-antfly-raft-to-node-id", .value = to },
            .{ .name = "x-antfly-raft-term", .value = term },
        };
        const headers: []const common.RequestHeader = if (req.from == 0) &.{} else live_headers[0..];

        const requested_format = if (req.locator) |locator|
            effectiveLocatorFormat(locator, routes.Routes.snapshot_upload_v2)
        else
            .unknown;
        const legacy_len = try snapshotEnvelopeEncodedLen(req.snapshot);
        const legacy_fits = legacy_len <= self.cfg.legacy_fallback_max_request_bytes;
        const prefer_v2 = req.snapshot.data.len > self.cfg.legacy_max_snapshot_bytes or !legacy_fits;
        if (requested_format == .chunked_manifest_v2 or
            (requested_format == .unknown and prefer_v2))
        {
            if (try self.resolveV2UploadTarget(req, snapshot_id, uri)) |target| {
                defer target.deinit(self.alloc);
                const capabilities = self.peerSnapshotV2Capabilities(target.capabilities_uri, req.from);
                if (requested_format == .chunked_manifest_v2) {
                    const value = capabilities orelse
                        return error.SnapshotTransferProtocolUpgradeRequired;
                    return try self.sendChunkedSnapshot(req, target.upload_uri, &live_headers, value.max_chunk_bytes);
                }
                if (capabilities) |value|
                    return try self.sendChunkedSnapshot(req, target.upload_uri, &live_headers, value.max_chunk_bytes);
            }
            if (!legacy_fits or requested_format == .chunked_manifest_v2)
                return error.SnapshotTransferProtocolUpgradeRequired;
        }

        if (requested_format == .legacy_envelope_v1 and !legacy_fits)
            return error.SnapshotTooLarge;
        if (!legacy_fits) return error.SnapshotTransferProtocolUpgradeRequired;
        const body = try encodeSnapshotEnvelopeExact(self.alloc, req.snapshot, legacy_len);
        defer self.alloc.free(body);
        return try self.sendLegacySnapshot(req, uri, headers, body);
    }

    const V2UploadTarget = struct {
        upload_uri: []u8,
        capabilities_uri: []u8,

        fn deinit(self: @This(), alloc: std.mem.Allocator) void {
            alloc.free(self.upload_uri);
            alloc.free(self.capabilities_uri);
        }
    };

    fn resolveV2UploadTarget(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
        snapshot_id: []const u8,
        legacy_uri: []const u8,
    ) !?V2UploadTarget {
        if (req.locator) |locator| {
            if (effectiveLocatorFormat(locator, routes.Routes.snapshot_upload_v2) == .chunked_manifest_v2) {
                if (try baseUriFromLegacySnapshotUri(
                    self.alloc,
                    legacy_uri,
                    routes.Routes.snapshot_upload_v2,
                    snapshot_id,
                )) |base| {
                    defer self.alloc.free(base);
                    const upload_uri = try self.alloc.dupe(u8, legacy_uri);
                    errdefer self.alloc.free(upload_uri);
                    return .{
                        .upload_uri = upload_uri,
                        .capabilities_uri = try routes.Routes.join(self.alloc, base, routes.Routes.capabilities),
                    };
                }
            }
        }
        const base_uri: ?[]u8 = if (self.resolver) |resolver|
            resolver.resolveBaseUri(self.alloc, req.group_id, req.to) catch
                try baseUriFromLegacySnapshotUri(self.alloc, legacy_uri, routes.Routes.snapshot_upload, snapshot_id)
        else
            try baseUriFromLegacySnapshotUri(self.alloc, legacy_uri, routes.Routes.snapshot_upload, snapshot_id);
        const base = base_uri orelse return null;
        defer self.alloc.free(base);
        const upload_path = try routes.Routes.snapshotUploadPathV2(self.alloc, snapshot_id);
        defer self.alloc.free(upload_path);
        const upload_uri = try routes.Routes.join(self.alloc, base, upload_path);
        errdefer self.alloc.free(upload_uri);
        return .{
            .upload_uri = upload_uri,
            .capabilities_uri = try routes.Routes.join(self.alloc, base, routes.Routes.capabilities),
        };
    }

    fn baseUriFromLegacySnapshotUri(
        alloc: std.mem.Allocator,
        uri: []const u8,
        route_prefix: []const u8,
        snapshot_id: []const u8,
    ) !?[]u8 {
        const pos = std.mem.lastIndexOf(u8, uri, route_prefix) orelse return null;
        if (!snapshotUriUsesRoute(uri, route_prefix, snapshot_id)) return null;
        return try alloc.dupe(u8, uri[0..pos]);
    }

    fn snapshotUriUsesRoute(
        uri: []const u8,
        route_prefix: []const u8,
        snapshot_id: []const u8,
    ) bool {
        const pos = std.mem.lastIndexOf(u8, uri, route_prefix) orelse return false;
        const suffix = uri[pos + route_prefix.len ..];
        return suffix.len == snapshot_id.len + 1 and suffix[0] == '/' and
            std.mem.eql(u8, suffix[1..], snapshot_id);
    }

    fn effectiveLocatorFormat(
        locator: raft_engine.runtime.snapshot_transport_iface.SnapshotLocator,
        v2_route_prefix: []const u8,
    ) raft_engine.runtime.snapshot_transport_iface.SnapshotArtifactFormat {
        if (locator.format != .unknown) return locator.format;
        // V2 route names were persisted before catalogs gained an explicit
        // format field. The stored route is deterministic artifact metadata,
        // unlike the serving peer's mutable capability response.
        if (snapshotUriUsesRoute(locator.uri, v2_route_prefix, locator.snapshot_id))
            return .chunked_manifest_v2;
        return .unknown;
    }

    const PeerSnapshotV2Capabilities = struct {
        max_chunk_bytes: usize,
    };

    fn peerSnapshotV2Capabilities(
        self: *HttpSnapshotTransport,
        capabilities_uri: []const u8,
        source_node_id: u64,
    ) ?PeerSnapshotV2Capabilities {
        var resp = self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = capabilities_uri,
            .source_node_id = if (source_node_id == 0) null else source_node_id,
            .timeout_ms = self.cfg.request_timeout_ms,
        }) catch return null;
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return null;
        const Capabilities = struct {
            snapshot_transfer_protocol_version: u32 = 0,
            snapshot_transfer_route_version: u32 = 0,
            snapshot_max_chunk_bytes: usize = snapshot_transfer.min_chunk_bytes,
        };
        const parsed = std.json.parseFromSlice(Capabilities, self.alloc, resp.body, .{
            .ignore_unknown_fields = true,
        }) catch return null;
        defer parsed.deinit();
        if (parsed.value.snapshot_transfer_protocol_version < snapshot_transfer.protocol_version or
            parsed.value.snapshot_transfer_route_version < snapshot_transfer.http_route_version or
            parsed.value.snapshot_max_chunk_bytes < snapshot_transfer.min_chunk_bytes or
            parsed.value.snapshot_max_chunk_bytes > snapshot_transfer.max_chunk_bytes)
            return null;
        return .{ .max_chunk_bytes = parsed.value.snapshot_max_chunk_bytes };
    }

    fn sendLegacySnapshot(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
        uri: []const u8,
        headers: []const common.RequestHeader,
        body: []const u8,
    ) !void {
        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .headers = headers,
            .source_node_id = if (req.from == 0) null else req.from,
            .content_type = "application/x-antflydb-raft-snapshot",
            .body = body,
            .timeout_ms = self.cfg.request_timeout_ms,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
    }

    fn sendChunkedSnapshot(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
        uri: []const u8,
        identity_headers: []const common.RequestHeader,
        peer_max_chunk_bytes: usize,
    ) !void {
        if (self.cfg.chunk_size < snapshot_transfer.min_chunk_bytes or
            self.cfg.chunk_size > snapshot_transfer.max_chunk_bytes)
            return error.InvalidSnapshotChunkSize;
        if (peer_max_chunk_bytes < snapshot_transfer.min_chunk_bytes or
            peer_max_chunk_bytes > snapshot_transfer.max_chunk_bytes)
            return error.InvalidSnapshotChunkSize;
        const transfer_chunk_size = @min(self.cfg.chunk_size, peer_max_chunk_bytes);
        const deadline_ns = self.transferDeadlineNs();
        var cancellation: common.RequestCancellation = .{};
        const manifest: snapshot_transfer.Manifest = .{
            .group_id = req.group_id,
            .from = req.from,
            .to = req.to,
            .request_term = req.term,
            .metadata = req.snapshot.metadata,
            .data_len = req.snapshot.data.len,
            .digest = snapshot_transfer.digest(req.snapshot.data),
        };
        const encoded_manifest = try snapshot_transfer.encode(self.alloc, manifest);
        defer self.alloc.free(encoded_manifest);
        const generation = try snapshot_transfer.generationFromEncodedManifest(encoded_manifest);
        const encoded_generation = snapshot_transfer.encodeGeneration(generation);

        var begin_headers: [7]common.RequestHeader = undefined;
        const begin_count = appendTransferHeaders(&begin_headers, identity_headers, &encoded_generation, "begin", null, null);
        try self.executeExpectedSuccess(.{
            .method = .POST,
            .uri = uri,
            .headers = begin_headers[0..begin_count],
            .source_node_id = if (req.from == 0) null else req.from,
            .content_type = "application/x-antflydb-raft-snapshot-manifest-v2",
            .body = encoded_manifest,
            .timeout_ms = try self.requestTimeoutUntil(deadline_ns),
            .cancellation = &cancellation,
        });
        errdefer {
            var abort_headers: [7]common.RequestHeader = undefined;
            const abort_count = appendTransferHeaders(&abort_headers, identity_headers, &encoded_generation, "abort", null, null);
            self.executeExpectedSuccess(.{
                .method = .DELETE,
                .uri = uri,
                .headers = abort_headers[0..abort_count],
                .source_node_id = if (req.from == 0) null else req.from,
                // Cleanup gets its own bounded attempt even when the transfer
                // context was canceled by a failed chunk.
                .timeout_ms = self.cfg.request_timeout_ms,
            }) catch |err| std.log.warn("snapshot upload abort deferred uri={s} err={s}", .{
                uri,
                @errorName(err),
            });
        }

        try self.sendSnapshotChunks(
            req,
            uri,
            identity_headers,
            encoded_generation,
            transfer_chunk_size,
            deadline_ns,
            &cancellation,
        );

        var commit_headers: [7]common.RequestHeader = undefined;
        const commit_count = appendTransferHeaders(&commit_headers, identity_headers, &encoded_generation, "commit", null, null);
        try self.executeExpectedSuccess(.{
            .method = .POST,
            .uri = uri,
            .headers = commit_headers[0..commit_count],
            .source_node_id = if (req.from == 0) null else req.from,
            .timeout_ms = try self.requestTimeoutUntil(deadline_ns),
            .cancellation = &cancellation,
        });
    }

    fn sendSnapshotChunks(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
        uri: []const u8,
        identity_headers: []const common.RequestHeader,
        encoded_generation: [snapshot_transfer.generation_hex_len]u8,
        chunk_size: usize,
        deadline_ns: u64,
        cancellation: *common.RequestCancellation,
    ) !void {
        if (req.snapshot.data.len == 0) return;
        const chunk_count = std.math.divCeil(usize, req.snapshot.data.len, chunk_size) catch unreachable;
        const worker_count = if (self.executor.supportsConcurrentRequests())
            @min(chunk_count, self.cfg.max_parallel_chunks)
        else
            1;
        const Context = struct {
            transport: *HttpSnapshotTransport,
            req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
            uri: []const u8,
            identity_headers: []const common.RequestHeader,
            generation: [snapshot_transfer.generation_hex_len]u8,
            chunk_size: usize,
            deadline_ns: u64,
            cancellation: *common.RequestCancellation,
            next_offset: std.atomic.Value(usize) = .init(0),
            failed: std.atomic.Value(bool) = .init(false),
            error_mutex: std.atomic.Mutex = .unlocked,
            first_error: ?anyerror = null,

            fn recordFailure(ctx: *@This(), err: anyerror) void {
                ctx.failed.store(true, .release);
                platform_sync.lockYielding(&ctx.error_mutex);
                if (ctx.first_error == null) ctx.first_error = err;
                ctx.error_mutex.unlock();
                // Publish the causal error before waking sibling requests;
                // otherwise a canceled sibling can obscure it with Cancelled.
                ctx.cancellation.cancel();
            }

            fn run(ctx: *@This()) void {
                while (!ctx.failed.load(.acquire)) {
                    const offset = ctx.next_offset.fetchAdd(ctx.chunk_size, .monotonic);
                    if (offset >= ctx.req.snapshot.data.len) return;
                    const end = @min(ctx.req.snapshot.data.len, offset + ctx.chunk_size);
                    var offset_buf: [32]u8 = undefined;
                    const encoded_offset = std.fmt.bufPrint(&offset_buf, "{d}", .{offset}) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                    var headers: [8]common.RequestHeader = undefined;
                    const count = appendTransferHeaders(
                        &headers,
                        ctx.identity_headers,
                        &ctx.generation,
                        "chunk",
                        encoded_offset,
                        null,
                    );
                    ctx.transport.executeExpectedSuccessWithAllocator(std.heap.page_allocator, .{
                        .method = .PUT,
                        .uri = ctx.uri,
                        .headers = headers[0..count],
                        .source_node_id = if (ctx.req.from == 0) null else ctx.req.from,
                        .content_type = "application/x-antflydb-raft-snapshot-chunk-v2",
                        .body = ctx.req.snapshot.data[offset..end],
                        .timeout_ms = ctx.transport.requestTimeoutUntil(ctx.deadline_ns) catch |err| {
                            ctx.recordFailure(err);
                            return;
                        },
                        .cancellation = ctx.cancellation,
                    }) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                }
            }
        };
        var ctx = Context{
            .transport = self,
            .req = req,
            .uri = uri,
            .identity_headers = identity_headers,
            .generation = encoded_generation,
            .chunk_size = chunk_size,
            .deadline_ns = deadline_ns,
            .cancellation = cancellation,
        };
        if (worker_count == 1) {
            Context.run(&ctx);
        } else {
            var group: std.Io.Group = .init;
            for (0..worker_count) |_| group.async(self.artifact_io, Context.run, .{&ctx});
            group.await(self.artifact_io) catch |err| ctx.recordFailure(err);
        }
        if (ctx.first_error) |err| return err;
    }

    fn executeExpectedSuccess(self: *HttpSnapshotTransport, req: common.HttpRequest) !void {
        return self.executeExpectedSuccessWithAllocator(self.alloc, req);
    }

    fn executeExpectedSuccessWithAllocator(
        self: *HttpSnapshotTransport,
        alloc: std.mem.Allocator,
        req: common.HttpRequest,
    ) !void {
        var resp = try self.executor.execute(alloc, req);
        defer resp.deinit(alloc);
        if (resp.status == 409) return error.SnapshotArtifactGenerationConflict;
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
    }

    fn fetchSnapshot(
        ptr: *anyopaque,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
        receiver: raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver,
    ) !void {
        const self: *HttpSnapshotTransport = @ptrCast(@alignCast(ptr));
        if (self.cfg.chunk_size < snapshot_transfer.min_chunk_bytes or
            self.cfg.chunk_size > snapshot_transfer.max_chunk_bytes)
            return error.InvalidSnapshotChunkSize;
        switch (effectiveLocatorFormat(req.locator, routes.Routes.snapshot_fetch_v2)) {
            .legacy_envelope_v1 => return try self.fetchSnapshotLegacy(req, receiver),
            .chunked_manifest_v2 => return try self.fetchSnapshotV2Resolved(req, receiver, false),
            .unknown => self.fetchSnapshotLegacy(req, receiver) catch |err| switch (err) {
                // Pre-version locators historically referred to v1. Only a
                // definitive absence permits probing v2; transport failures
                // remain ambiguous and must not silently change protocols.
                error.SnapshotArtifactNotFound => return try self.fetchSnapshotV2Resolved(req, receiver, true),
                else => return err,
            },
        }
    }

    fn fetchSnapshotLegacy(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
        receiver: raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver,
    ) !void {
        var legacy = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = req.locator.uri,
            .timeout_ms = self.cfg.request_timeout_ms,
        });
        defer legacy.deinit(self.alloc);
        try mapSnapshotFetchStatus(legacy.status);
        if (legacy.body.len > self.cfg.legacy_fallback_max_request_bytes)
            return error.SnapshotTooLarge;
        var snapshot = try decodeSnapshotEnvelopeWithLimits(self.alloc, legacy.body, .{
            .max_snapshot_bytes = self.cfg.max_snapshot_bytes,
        });
        var snapshot_owned = true;
        defer if (snapshot_owned) snapshot.deinit(self.alloc);
        const data_len = snapshot.data.len;
        var admission_owned = try receiver.admitSnapshot(req, data_len);
        errdefer if (admission_owned) receiver.cancelSnapshotAdmission(data_len);
        var admitted_req = req;
        admitted_req.admission_reserved = admission_owned;
        admitted_req.admitted_snapshot_bytes = if (admission_owned) data_len else 0;
        admission_owned = false;
        snapshot_owned = false;
        try receiver.receiveSnapshot(admitted_req, snapshot);
        var release = self.executor.execute(self.alloc, .{
            .method = .DELETE,
            .uri = req.locator.uri,
            .timeout_ms = self.cfg.request_timeout_ms,
        }) catch |err| {
            // Release was added after the v1 transfer. Consumption succeeded,
            // so an older server must not turn it into a failed restore; its
            // TTL cleanup remains the compatibility fallback.
            std.log.warn("legacy snapshot fetch artifact release deferred snapshot_id={s} err={s}", .{
                req.locator.snapshot_id,
                @errorName(err),
            });
            return;
        };
        defer release.deinit(self.alloc);
        if (release.status < 200 or release.status >= 300) {
            std.log.warn("legacy snapshot fetch artifact release deferred snapshot_id={s} status={d}", .{
                req.locator.snapshot_id,
                release.status,
            });
        }
    }

    fn fetchSnapshotV2Resolved(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
        receiver: raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver,
        require_capability: bool,
    ) !void {
        const target = (try self.resolveV2FetchTarget(req)) orelse
            return error.SnapshotTransferProtocolUpgradeRequired;
        defer target.deinit(self.alloc);
        const capabilities = self.peerSnapshotV2Capabilities(target.capabilities_uri, 0);
        const value = capabilities orelse return if (require_capability)
            error.SnapshotArtifactNotFound
        else
            error.SnapshotTransferProtocolUpgradeRequired;
        return try self.fetchSnapshotV2(req, receiver, target.fetch_uri, value.max_chunk_bytes);
    }

    fn mapSnapshotFetchStatus(status: u16) !void {
        return switch (status) {
            200...299 => {},
            404 => error.SnapshotArtifactNotFound,
            410 => error.SnapshotArtifactExpired,
            409 => error.SnapshotArtifactGenerationConflict,
            503 => error.SnapshotAdmissionBackpressure,
            else => error.UnexpectedHttpStatus,
        };
    }

    const V2FetchTarget = struct {
        fetch_uri: []u8,
        capabilities_uri: []u8,

        fn deinit(self: @This(), alloc: std.mem.Allocator) void {
            alloc.free(self.fetch_uri);
            alloc.free(self.capabilities_uri);
        }
    };

    fn resolveV2FetchTarget(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
    ) !?V2FetchTarget {
        if (effectiveLocatorFormat(req.locator, routes.Routes.snapshot_fetch_v2) == .chunked_manifest_v2) {
            if (try baseUriFromLegacySnapshotUri(
                self.alloc,
                req.locator.uri,
                routes.Routes.snapshot_fetch_v2,
                req.locator.snapshot_id,
            )) |base| {
                defer self.alloc.free(base);
                const fetch_uri = try self.alloc.dupe(u8, req.locator.uri);
                errdefer self.alloc.free(fetch_uri);
                return .{
                    .fetch_uri = fetch_uri,
                    .capabilities_uri = try routes.Routes.join(self.alloc, base, routes.Routes.capabilities),
                };
            }
        }
        const base_uri: ?[]u8 = if (self.resolver) |resolver|
            resolver.resolveBaseUri(self.alloc, req.group_id, req.from) catch
                try baseUriFromLegacySnapshotUri(
                    self.alloc,
                    req.locator.uri,
                    routes.Routes.snapshot_fetch,
                    req.locator.snapshot_id,
                )
        else
            try baseUriFromLegacySnapshotUri(
                self.alloc,
                req.locator.uri,
                routes.Routes.snapshot_fetch,
                req.locator.snapshot_id,
            );
        const base = base_uri orelse return null;
        defer self.alloc.free(base);
        const fetch_path = try routes.Routes.snapshotFetchPathV2(self.alloc, req.locator.snapshot_id);
        defer self.alloc.free(fetch_path);
        const fetch_uri = try routes.Routes.join(self.alloc, base, fetch_path);
        errdefer self.alloc.free(fetch_uri);
        return .{
            .fetch_uri = fetch_uri,
            .capabilities_uri = try routes.Routes.join(self.alloc, base, routes.Routes.capabilities),
        };
    }

    fn fetchSnapshotV2(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
        receiver: raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver,
        fetch_uri: []const u8,
        peer_max_chunk_bytes: usize,
    ) !void {
        if (peer_max_chunk_bytes < snapshot_transfer.min_chunk_bytes or
            peer_max_chunk_bytes > snapshot_transfer.max_chunk_bytes)
            return error.InvalidSnapshotChunkSize;
        const transfer_chunk_size = @min(self.cfg.chunk_size, peer_max_chunk_bytes);
        const deadline_ns = self.transferDeadlineNs();
        var cancellation: common.RequestCancellation = .{};
        const manifest_headers = [_]common.RequestHeader{
            .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" },
            .{ .name = "x-antfly-raft-snapshot-operation", .value = "manifest" },
        };
        var resp = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = fetch_uri,
            .headers = &manifest_headers,
            .timeout_ms = try self.requestTimeoutUntil(deadline_ns),
            .cancellation = &cancellation,
        });
        defer resp.deinit(self.alloc);
        try mapSnapshotFetchStatus(resp.status);
        if (resp.content_type == null or
            !std.mem.eql(u8, resp.content_type.?, "application/x-antflydb-raft-snapshot-manifest-v2"))
            return error.InvalidSnapshotManifestResponse;

        var manifest = try snapshot_transfer.decode(self.alloc, resp.body);
        defer manifest.deinit(self.alloc);
        const generation = try snapshot_transfer.generationFromEncodedManifest(resp.body);
        const encoded_generation = snapshot_transfer.encodeGeneration(generation);
        if (manifest.group_id != req.group_id)
            return error.SnapshotTransferIdentityMismatch;
        switch (manifest.purpose()) {
            .bootstrap_artifact => |artifact| if (artifact.owner_node_id != req.from)
                return error.SnapshotTransferIdentityMismatch,
            .live_install => return error.SnapshotTransferIdentityMismatch,
        }
        if (manifest.data_len > self.cfg.max_snapshot_bytes) return error.SnapshotTooLarge;
        const data_len = std.math.cast(usize, manifest.data_len) orelse return error.SnapshotTooLarge;
        var admission_owned = try receiver.admitSnapshot(req, data_len);
        errdefer if (admission_owned) receiver.cancelSnapshotAdmission(data_len);
        var snapshot = try self.fetchSnapshotArtifact(
            req,
            fetch_uri,
            encoded_generation,
            transfer_chunk_size,
            data_len,
            manifest.digest,
            deadline_ns,
            &cancellation,
        );
        var snapshot_owned = true;
        defer if (snapshot_owned) snapshot.deinit(self.alloc);
        const metadata = manifest.metadata;
        manifest.metadata = .{};
        snapshot.metadata = metadata;
        // SnapshotReceiver owns both allocations once invoked, even when Raft
        // admission returns an error. Relinquish locally before the call so an
        // error cannot double-free the receiver's message.
        var admitted_req = req;
        admitted_req.admission_reserved = admission_owned;
        admitted_req.admitted_snapshot_bytes = if (admission_owned) data_len else 0;
        admission_owned = false;
        snapshot_owned = false;
        try receiver.receiveSnapshot(admitted_req, snapshot);

        const release_headers = [_]common.RequestHeader{
            .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" },
            .{ .name = "x-antfly-raft-snapshot-operation", .value = "release" },
            .{ .name = "x-antfly-raft-snapshot-generation", .value = &encoded_generation },
        };
        self.executeExpectedSuccess(.{
            .method = .DELETE,
            .uri = fetch_uri,
            .headers = &release_headers,
            .timeout_ms = self.cfg.request_timeout_ms,
        }) catch |err| std.log.warn("snapshot fetch artifact release deferred snapshot_id={s} err={s}", .{
            req.locator.snapshot_id,
            @errorName(err),
        });
    }

    fn fetchSnapshotArtifact(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
        fetch_uri: []const u8,
        encoded_generation: [snapshot_transfer.generation_hex_len]u8,
        chunk_size: usize,
        data_len: usize,
        expected_digest: [snapshot_transfer.digest_len]u8,
        deadline_ns: u64,
        cancellation: *common.RequestCancellation,
    ) !raft_engine.core.types.Snapshot {
        if (data_len == 0) {
            const actual = snapshot_transfer.digest("");
            if (!std.mem.eql(u8, &actual, &expected_digest))
                return error.SnapshotChecksumMismatch;
            return .{};
        }
        try self.staging_budget.reserve(data_len);
        var staging_reservation_owned = true;
        defer if (staging_reservation_owned) self.staging_budget.release(data_len);
        const file_io = self.artifact_io;
        try fs_paths.createDirPathPortable(file_io, self.cfg.root_dir);
        const sequence = snapshot_fetch_sequence.fetchAdd(1, .monotonic);
        const staging_path = try std.fmt.allocPrint(
            self.alloc,
            "{s}/{s}{d}-{d}-{d}{s}",
            .{
                self.cfg.root_dir,
                snapshot_fetch_staging_prefix,
                req.group_id,
                platform_time.monotonicNs(),
                sequence,
                snapshot_fetch_staging_suffix,
            },
        );
        defer self.alloc.free(staging_path);
        var staging_exists = true;
        defer if (staging_exists) std.Io.Dir.cwd().deleteFile(file_io, staging_path) catch {};
        {
            var staging = try fs_paths.createFilePortable(file_io, staging_path, .{ .truncate = true });
            defer staging.close(file_io);
            try staging.setLength(file_io, data_len);
        }

        try self.fetchSnapshotChunksToFile(
            fetch_uri,
            encoded_generation,
            chunk_size,
            data_len,
            staging_path,
            file_io,
            deadline_ns,
            cancellation,
        );

        var staging = try std.Io.Dir.cwd().openFile(file_io, staging_path, .{ .mode = .read_write });
        var staging_open = true;
        defer if (staging_open) staging.close(file_io);
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        var hash_buffer: [256 * 1024]u8 = undefined;
        var hash_offset: u64 = 0;
        while (hash_offset < data_len) {
            const wanted: usize = @intCast(@min(hash_buffer.len, data_len - hash_offset));
            const read = try staging.readPositionalAll(file_io, hash_buffer[0..wanted], hash_offset);
            if (read != wanted) return error.SnapshotArtifactTruncated;
            hasher.update(hash_buffer[0..read]);
            hash_offset += read;
        }
        var actual_digest: [snapshot_transfer.digest_len]u8 = undefined;
        hasher.final(&actual_digest);
        if (!std.mem.eql(u8, &actual_digest, &expected_digest))
            return error.SnapshotChecksumMismatch;
        try staging.sync(file_io);

        var snapshot: raft_engine.core.types.Snapshot = .{};
        if (comptime builtin.os.tag != .windows and builtin.os.tag != .wasi and
            builtin.os.tag != .freestanding)
        {
            const mapped = try std.posix.mmap(
                null,
                data_len,
                .{ .READ = true },
                .{ .TYPE = .SHARED },
                staging.handle,
                0,
            );
            errdefer std.posix.munmap(mapped);
            std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
            staging.close(file_io);
            staging_open = false;
            // POSIX keeps the verified inode alive through the mapping. Finish
            // every fallible namespace operation before transferring mapping
            // ownership into Snapshot, leaving no post-transfer error window.
            try std.Io.Dir.cwd().deleteFile(file_io, staging_path);
            try fs_paths.syncDirPortable(file_io, self.cfg.root_dir);
            staging_exists = false;
            const owner = try self.alloc.create(MappedFetchOwner);
            errdefer self.alloc.destroy(owner);
            self.staging_budget.retain();
            var budget_ref_owned = true;
            errdefer if (budget_ref_owned) self.staging_budget.releaseRef();
            owner.* = .{
                .alloc = self.alloc,
                .mapped = mapped,
                .staging_budget = self.staging_budget,
                .reserved_bytes = data_len,
            };
            snapshot.data = mapped;
            try snapshot.shareExternalData(self.alloc, .{
                .ptr = owner,
                .release = MappedFetchOwner.release,
            }, null);
            budget_ref_owned = false;
            staging_reservation_owned = false;
            return snapshot;
        } else {
            // Compatibility targets without a native mapping API retain a
            // finite copy; supported POSIX production targets remain fully
            // artifact-backed for O(chunk) heap usage.
            snapshot.data = try self.alloc.alloc(u8, data_len);
            errdefer self.alloc.free(snapshot.data);
            const read = try staging.readPositionalAll(file_io, snapshot.data, 0);
            if (read != data_len) return error.SnapshotArtifactTruncated;
            staging.close(file_io);
            staging_open = false;
        }
        try std.Io.Dir.cwd().deleteFile(file_io, staging_path);
        try fs_paths.syncDirPortable(file_io, self.cfg.root_dir);
        staging_exists = false;
        return snapshot;
    }

    fn fetchSnapshotChunksToFile(
        self: *HttpSnapshotTransport,
        fetch_uri: []const u8,
        encoded_generation: [snapshot_transfer.generation_hex_len]u8,
        chunk_size: usize,
        data_len: usize,
        staging_path: []const u8,
        file_io: std.Io,
        deadline_ns: u64,
        cancellation: *common.RequestCancellation,
    ) !void {
        const chunk_count = std.math.divCeil(usize, data_len, chunk_size) catch unreachable;
        const worker_count = if (self.executor.supportsConcurrentRequests())
            @min(chunk_count, self.cfg.max_parallel_chunks)
        else
            1;
        const Context = struct {
            transport: *HttpSnapshotTransport,
            fetch_uri: []const u8,
            generation: [snapshot_transfer.generation_hex_len]u8,
            chunk_size: usize,
            data_len: usize,
            path: []const u8,
            file_io: std.Io,
            deadline_ns: u64,
            cancellation: *common.RequestCancellation,
            next_offset: std.atomic.Value(usize) = .init(0),
            failed: std.atomic.Value(bool) = .init(false),
            error_mutex: std.atomic.Mutex = .unlocked,
            first_error: ?anyerror = null,

            fn recordFailure(ctx: *@This(), err: anyerror) void {
                ctx.failed.store(true, .release);
                platform_sync.lockYielding(&ctx.error_mutex);
                if (ctx.first_error == null) ctx.first_error = err;
                ctx.error_mutex.unlock();
                // Publish the causal error before waking sibling requests;
                // otherwise a canceled sibling can obscure it with Cancelled.
                ctx.cancellation.cancel();
            }

            fn run(ctx: *@This()) void {
                var file = std.Io.Dir.cwd().openFile(ctx.file_io, ctx.path, .{ .mode = .read_write }) catch |err| {
                    ctx.recordFailure(err);
                    return;
                };
                defer file.close(ctx.file_io);
                while (!ctx.failed.load(.acquire)) {
                    const offset = ctx.next_offset.fetchAdd(ctx.chunk_size, .monotonic);
                    if (offset >= ctx.data_len) return;
                    const requested = @min(ctx.chunk_size, ctx.data_len - offset);
                    var offset_buf: [32]u8 = undefined;
                    var length_buf: [32]u8 = undefined;
                    const encoded_offset = std.fmt.bufPrint(&offset_buf, "{d}", .{offset}) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                    const encoded_length = std.fmt.bufPrint(&length_buf, "{d}", .{requested}) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                    const headers = [_]common.RequestHeader{
                        .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" },
                        .{ .name = "x-antfly-raft-snapshot-operation", .value = "chunk" },
                        .{ .name = "x-antfly-raft-snapshot-generation", .value = &ctx.generation },
                        .{ .name = "x-antfly-raft-snapshot-offset", .value = encoded_offset },
                        .{ .name = "x-antfly-raft-snapshot-chunk-length", .value = encoded_length },
                    };
                    const response_alloc = std.heap.page_allocator;
                    var response = ctx.transport.executor.execute(response_alloc, .{
                        .method = .GET,
                        .uri = ctx.fetch_uri,
                        .headers = &headers,
                        .timeout_ms = ctx.transport.requestTimeoutUntil(ctx.deadline_ns) catch |err| {
                            ctx.recordFailure(err);
                            return;
                        },
                        .cancellation = ctx.cancellation,
                    }) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                    defer response.deinit(response_alloc);
                    mapSnapshotFetchStatus(response.status) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                    if (response.content_type == null or
                        !std.mem.eql(u8, response.content_type.?, "application/x-antflydb-raft-snapshot-chunk-v2") or
                        response.body.len != requested)
                    {
                        ctx.recordFailure(error.InvalidSnapshotChunkResponse);
                        return;
                    }
                    file.writePositionalAll(ctx.file_io, response.body, offset) catch |err| {
                        ctx.recordFailure(err);
                        return;
                    };
                }
            }
        };
        var ctx = Context{
            .transport = self,
            .fetch_uri = fetch_uri,
            .generation = encoded_generation,
            .chunk_size = chunk_size,
            .data_len = data_len,
            .path = staging_path,
            .file_io = file_io,
            .deadline_ns = deadline_ns,
            .cancellation = cancellation,
        };
        if (worker_count == 1) {
            Context.run(&ctx);
        } else {
            var group: std.Io.Group = .init;
            for (0..worker_count) |_| group.async(self.artifact_io, Context.run, .{&ctx});
            group.await(self.artifact_io) catch |err| ctx.recordFailure(err);
        }
        if (ctx.first_error) |err| return err;
    }

    fn appendTransferHeaders(
        out: []common.RequestHeader,
        identity: []const common.RequestHeader,
        generation: []const u8,
        operation: []const u8,
        offset: ?[]const u8,
        chunk_length: ?[]const u8,
    ) usize {
        std.debug.assert(out.len >= identity.len + 3 + @intFromBool(offset != null) + @intFromBool(chunk_length != null));
        @memcpy(out[0..identity.len], identity);
        var count = identity.len;
        out[count] = .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" };
        count += 1;
        out[count] = .{ .name = "x-antfly-raft-snapshot-operation", .value = operation };
        count += 1;
        out[count] = .{ .name = "x-antfly-raft-snapshot-generation", .value = generation };
        count += 1;
        if (offset) |value| {
            out[count] = .{ .name = "x-antfly-raft-snapshot-offset", .value = value };
            count += 1;
        }
        if (chunk_length) |value| {
            out[count] = .{ .name = "x-antfly-raft-snapshot-chunk-length", .value = value };
            count += 1;
        }
        return count;
    }

    fn resolveUploadUri(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
        snapshot_id: []const u8,
    ) ![]u8 {
        if (req.locator) |locator| {
            if (locator.uri.len > 0) return try self.alloc.dupe(u8, locator.uri);
        }
        if (self.resolver) |resolver| {
            return try resolver.resolveUploadUri(self.alloc, req.group_id, req.to, snapshot_id);
        }
        return error.MissingSnapshotUploadUri;
    }

    pub fn encodeSnapshotEnvelope(alloc: std.mem.Allocator, snapshot: raft_engine.core.types.Snapshot) ![]u8 {
        return try encodeSnapshotEnvelopeExact(alloc, snapshot, try snapshotEnvelopeEncodedLen(snapshot));
    }

    fn snapshotEnvelopeEncodedLen(snapshot: raft_engine.core.types.Snapshot) !usize {
        if (snapshot.data.len > std.math.maxInt(u32)) return error.SnapshotTooLarge;
        var len: usize = @sizeOf(u64) * 2;
        const member_sets = [_][]const u64{
            snapshot.metadata.conf_state.voters,
            snapshot.metadata.conf_state.voters_outgoing,
            snapshot.metadata.conf_state.learners,
            snapshot.metadata.conf_state.learners_next,
        };
        for (member_sets) |members| {
            if (members.len > snapshot_transfer.max_members_per_set)
                return error.SnapshotMembershipTooLarge;
            const member_bytes = std.math.mul(usize, members.len, @sizeOf(u64)) catch
                return error.SnapshotTooLarge;
            len = std.math.add(usize, len, @sizeOf(u32)) catch return error.SnapshotTooLarge;
            len = std.math.add(usize, len, member_bytes) catch return error.SnapshotTooLarge;
        }
        len = std.math.add(usize, len, 1 + @sizeOf(u32)) catch return error.SnapshotTooLarge;
        return std.math.add(usize, len, snapshot.data.len) catch return error.SnapshotTooLarge;
    }

    fn encodeSnapshotEnvelopeExact(
        alloc: std.mem.Allocator,
        snapshot: raft_engine.core.types.Snapshot,
        encoded_len: usize,
    ) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        try out.ensureTotalCapacityPrecise(alloc, encoded_len);

        try appendInt(u64, alloc, &out, snapshot.metadata.index);
        try appendInt(u64, alloc, &out, snapshot.metadata.term);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.voters);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.voters_outgoing);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.learners);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.learners_next);
        try out.append(alloc, @intFromBool(snapshot.metadata.conf_state.auto_leave));
        try appendBytes(alloc, &out, snapshot.data);
        std.debug.assert(out.items.len == encoded_len);
        return try out.toOwnedSlice(alloc);
    }

    pub const SnapshotEnvelopeLimits = struct {
        max_snapshot_bytes: usize = 1 << 30,
        max_members_per_set: usize = snapshot_transfer.max_members_per_set,
    };

    pub fn decodeSnapshotEnvelope(alloc: std.mem.Allocator, bytes: []const u8) !raft_engine.core.types.Snapshot {
        return decodeSnapshotEnvelopeWithLimits(alloc, bytes, .{});
    }

    pub fn decodeSnapshotEnvelopeWithLimits(
        alloc: std.mem.Allocator,
        bytes: []const u8,
        limits: SnapshotEnvelopeLimits,
    ) !raft_engine.core.types.Snapshot {
        var cursor: usize = 0;
        const index = try readInt(u64, bytes, &cursor);
        const term = try readInt(u64, bytes, &cursor);
        var conf_state: raft_engine.core.types.ConfState = .{};
        errdefer conf_state.deinit(alloc);
        conf_state.voters = try decodeNodeList(alloc, bytes, &cursor, limits.max_members_per_set);
        conf_state.voters_outgoing = try decodeNodeList(alloc, bytes, &cursor, limits.max_members_per_set);
        conf_state.learners = try decodeNodeList(alloc, bytes, &cursor, limits.max_members_per_set);
        conf_state.learners_next = try decodeNodeList(alloc, bytes, &cursor, limits.max_members_per_set);
        conf_state.auto_leave = try readBool(bytes, &cursor);
        const data = try readBytes(alloc, bytes, &cursor, limits.max_snapshot_bytes);
        errdefer alloc.free(data);
        if (cursor != bytes.len) return error.InvalidSnapshotEnvelope;
        return .{
            .metadata = .{
                .index = index,
                .term = term,
                .conf_state = conf_state,
            },
            .data = data,
        };
    }

    fn appendInt(comptime T: type, alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: T) !void {
        var buf: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }

    fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
        if (bytes.len > std.math.maxInt(u32)) return error.SnapshotTooLarge;
        try appendInt(u32, alloc, out, @intCast(bytes.len));
        try out.appendSlice(alloc, bytes);
    }

    fn encodeNodeList(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), nodes: []const u64) !void {
        if (nodes.len > snapshot_transfer.max_members_per_set)
            return error.SnapshotMembershipTooLarge;
        try appendInt(u32, alloc, out, @intCast(nodes.len));
        for (nodes) |node| try appendInt(u64, alloc, out, node);
    }

    fn readInt(comptime T: type, data: []const u8, cursor: *usize) !T {
        if (cursor.* > data.len or data.len - cursor.* < @sizeOf(T))
            return error.InvalidSnapshotEnvelope;
        var buf: [@sizeOf(T)]u8 = undefined;
        @memcpy(&buf, data[cursor.* .. cursor.* + @sizeOf(T)]);
        const value = std.mem.readInt(T, &buf, .little);
        cursor.* += @sizeOf(T);
        return value;
    }

    fn readBool(data: []const u8, cursor: *usize) !bool {
        if (cursor.* >= data.len) return error.InvalidSnapshotEnvelope;
        const raw = data[cursor.*];
        if (raw > 1) return error.InvalidSnapshotEnvelope;
        cursor.* += 1;
        return raw == 1;
    }

    fn readBytes(
        alloc: std.mem.Allocator,
        data: []const u8,
        cursor: *usize,
        max_len: usize,
    ) ![]u8 {
        const encoded_len = try readInt(u32, data, cursor);
        const len = std.math.cast(usize, encoded_len) orelse return error.InvalidSnapshotEnvelope;
        if (len > max_len) return error.SnapshotTooLarge;
        if (cursor.* > data.len or data.len - cursor.* < len)
            return error.InvalidSnapshotEnvelope;
        defer cursor.* += len;
        return try alloc.dupe(u8, data[cursor.* .. cursor.* + len]);
    }

    fn decodeNodeList(
        alloc: std.mem.Allocator,
        data: []const u8,
        cursor: *usize,
        max_len: usize,
    ) ![]u64 {
        const encoded_len = try readInt(u32, data, cursor);
        const len = std.math.cast(usize, encoded_len) orelse return error.InvalidSnapshotEnvelope;
        if (len > max_len) return error.SnapshotMembershipTooLarge;
        const byte_len = std.math.mul(usize, len, @sizeOf(u64)) catch
            return error.InvalidSnapshotEnvelope;
        if (cursor.* > data.len or data.len - cursor.* < byte_len)
            return error.InvalidSnapshotEnvelope;
        const out = try alloc.alloc(u64, len);
        errdefer alloc.free(out);
        for (out) |*node| node.* = try readInt(u64, data, cursor);
        return out;
    }
};

test "http snapshot transport module compiles" {
    _ = HttpSnapshotConfig;
    _ = SnapshotTargetResolver;
    _ = SnapshotFetch;
    _ = HttpSnapshotTransport;
}

test "snapshot transport validates direct-construction resource limits" {
    try std.testing.expectError(
        error.InvalidSnapshotTransferLimits,
        HttpSnapshotTransport.validateConfig(.{ .root_dir = "/tmp", .max_parallel_chunks = 0 }),
    );
    try std.testing.expectError(
        error.InvalidSnapshotTransferLimits,
        HttpSnapshotTransport.validateConfig(.{
            .root_dir = "/tmp",
            .max_parallel_chunks = max_parallel_chunk_workers + 1,
        }),
    );
    try std.testing.expectError(
        error.InvalidSnapshotTransferLimits,
        HttpSnapshotTransport.validateConfig(.{ .root_dir = "/tmp", .request_timeout_ms = 0 }),
    );
    try std.testing.expectError(
        error.InvalidSnapshotTransferLimits,
        HttpSnapshotTransport.validateConfig(.{
            .root_dir = "/tmp",
            .request_timeout_ms = 10,
            .transfer_timeout_ms = 9,
        }),
    );
    try std.testing.expectError(
        error.InvalidSnapshotTransferLimits,
        HttpSnapshotTransport.validateConfig(.{
            .root_dir = "/tmp",
            .max_snapshot_bytes = 1024,
            .max_staging_bytes = 1023,
        }),
    );
}

test "snapshot staging budget rejects aggregate overcommit and is reusable" {
    var budget = SnapshotStagingBudget{ .alloc = std.testing.allocator, .limit = 10 };
    try budget.reserve(6);
    try std.testing.expectError(error.SnapshotStagingBackpressure, budget.reserve(5));
    try std.testing.expectEqual(@as(usize, 6), budget.reserved.load(.acquire));
    budget.release(6);
    try budget.reserve(10);
    budget.release(10);
    try std.testing.expectEqual(@as(usize, 0), budget.reserved.load(.acquire));
}

test "snapshot transport scavenges only private crash artifacts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/snapshot-scavenge", .{tmp.sub_path});
    defer alloc.free(root_dir);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try fs_paths.createDirPathPortable(io, root_dir);
    const orphan_path = try std.fmt.allocPrint(alloc, "{s}/{s}42{s}", .{
        root_dir,
        snapshot_fetch_staging_prefix,
        snapshot_fetch_staging_suffix,
    });
    defer alloc.free(orphan_path);
    const unrelated_path = try std.fmt.allocPrint(alloc, "{s}/retained.snapshot", .{root_dir});
    defer alloc.free(unrelated_path);
    var orphan = try fs_paths.createFilePortable(io, orphan_path, .{});
    orphan.close(io);
    var unrelated = try fs_paths.createFilePortable(io, unrelated_path, .{});
    unrelated.close(io);

    try scavengeFetchArtifacts(io, root_dir);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, orphan_path, .{}));
    try std.Io.Dir.cwd().access(io, unrelated_path, .{});
}

test "legacy snapshot envelope rejects unbounded and non-canonical frames" {
    const snapshot: raft_engine.core.types.Snapshot = .{
        .metadata = .{ .index = 7, .term = 3 },
        .data = @constCast("payload"),
    };
    const encoded = try HttpSnapshotTransport.encodeSnapshotEnvelope(std.testing.allocator, snapshot);
    defer std.testing.allocator.free(encoded);

    const trailing = try std.testing.allocator.alloc(u8, encoded.len + 1);
    defer std.testing.allocator.free(trailing);
    @memcpy(trailing[0..encoded.len], encoded);
    trailing[encoded.len] = 0xff;
    try std.testing.expectError(
        error.InvalidSnapshotEnvelope,
        HttpSnapshotTransport.decodeSnapshotEnvelope(std.testing.allocator, trailing),
    );

    const oversized_membership = try std.testing.allocator.dupe(u8, encoded);
    defer std.testing.allocator.free(oversized_membership);
    std.mem.writeInt(
        u32,
        oversized_membership[16..20],
        @intCast(snapshot_transfer.max_members_per_set + 1),
        .little,
    );
    try std.testing.expectError(
        error.SnapshotMembershipTooLarge,
        HttpSnapshotTransport.decodeSnapshotEnvelope(std.testing.allocator, oversized_membership),
    );

    try std.testing.expectError(
        error.SnapshotTooLarge,
        HttpSnapshotTransport.decodeSnapshotEnvelopeWithLimits(std.testing.allocator, encoded, .{
            .max_snapshot_bytes = snapshot.data.len - 1,
        }),
    );

    const invalid_bool = try std.testing.allocator.dupe(u8, encoded);
    defer std.testing.allocator.free(invalid_bool);
    // Four empty membership counts follow index and term.
    invalid_bool[16 + 4 * @sizeOf(u32)] = 2;
    try std.testing.expectError(
        error.InvalidSnapshotEnvelope,
        HttpSnapshotTransport.decodeSnapshotEnvelope(std.testing.allocator, invalid_bool),
    );
}

test "legacy snapshot envelope preflight exactly sizes one allocation" {
    var voters = [_]u64{ 1, 2, 3 };
    const snapshot: raft_engine.core.types.Snapshot = .{
        .metadata = .{
            .index = 9,
            .term = 4,
            .conf_state = .{ .voters = &voters },
        },
        .data = @constCast("bounded-payload"),
    };
    const expected_len = try HttpSnapshotTransport.snapshotEnvelopeEncodedLen(snapshot);
    const encoded = try HttpSnapshotTransport.encodeSnapshotEnvelope(std.testing.allocator, snapshot);
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqual(expected_len, encoded.len);
}

test "unknown locator fetches legacy first while v2 locator is deterministic" {
    const Executor = struct {
        legacy_requests: usize = 0,
        v2_requests: usize = 0,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.indexOf(u8, req.uri, routes.Routes.snapshot_fetch) != null and
                std.mem.indexOf(u8, req.uri, "/raft/v2/") == null)
            {
                self.legacy_requests += 1;
                return .{ .status = 404, .body = try alloc.dupe(u8, "not found") };
            }
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) {
                return .{
                    .status = 200,
                    .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":2}"),
                };
            }
            self.v2_requests += 1;
            return .{ .status = 404, .body = try alloc.dupe(u8, "not found") };
        }
    };
    const Receiver = struct {
        fn iface() raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{ .ptr = undefined, .vtable = &.{ .receive_snapshot = receive } };
        }
        fn receive(_: *anyopaque, _: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest, _: raft_engine.core.types.Snapshot) !void {
            return error.UnexpectedSnapshot;
        }
    };

    var executor = Executor{};
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
    defer transport.deinit();
    const base_req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest = .{
        .group_id = 7,
        .from = 2,
        .locator = .{
            .snapshot_id = "artifact",
            .uri = "/raft/v1/snapshot/fetch/artifact",
        },
    };
    try std.testing.expectError(error.SnapshotArtifactNotFound, transport.transport().fetchSnapshot(base_req, Receiver.iface()));
    try std.testing.expectEqual(@as(usize, 1), executor.legacy_requests);
    try std.testing.expectEqual(@as(usize, 1), executor.v2_requests);

    var routed_v2_req = base_req;
    routed_v2_req.locator.uri = "/raft/v2/snapshot/fetch/artifact";
    try std.testing.expectError(error.SnapshotArtifactNotFound, transport.transport().fetchSnapshot(routed_v2_req, Receiver.iface()));
    try std.testing.expectEqual(@as(usize, 1), executor.legacy_requests);
    try std.testing.expectEqual(@as(usize, 2), executor.v2_requests);

    var versioned_req = base_req;
    versioned_req.locator.format = .chunked_manifest_v2;
    try std.testing.expectError(error.SnapshotArtifactNotFound, transport.transport().fetchSnapshot(versioned_req, Receiver.iface()));
    try std.testing.expectEqual(@as(usize, 1), executor.legacy_requests);
    try std.testing.expectEqual(@as(usize, 3), executor.v2_requests);
}

test "v2 bootstrap artifact validates its owner instead of a Raft sender" {
    const Executor = struct {
        manifest: []const u8,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":2,\"snapshot_max_chunk_bytes\":65536}"),
            };
            if (std.mem.eql(u8, req.header("x-antfly-raft-snapshot-operation") orelse "", "manifest")) {
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "application/x-antflydb-raft-snapshot-manifest-v2"),
                    .body = try alloc.dupe(u8, self.manifest),
                };
            }
            return .{ .status = 204 };
        }
    };
    const Receiver = struct {
        seen: bool = false,
        fn iface(self: *@This()) raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{ .ptr = self, .vtable = &.{ .receive_snapshot = receive } };
        }
        fn receive(ptr: *anyopaque, _: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest, snapshot: raft_engine.core.types.Snapshot) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = snapshot;
            defer owned.deinit(std.testing.allocator);
            self.seen = true;
        }
    };

    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 91,
        .from = 0,
        .to = 7,
        .request_term = 0,
        .metadata = .{ .index = 12, .term = 4 },
        .data_len = 0,
        .digest = snapshot_transfer.digest(""),
    };
    const encoded = try snapshot_transfer.encode(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded);
    var executor = Executor{ .manifest = encoded };
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
    defer transport.deinit();
    var receiver = Receiver{};
    try transport.transport().fetchSnapshot(.{
        .group_id = 91,
        .from = 7,
        .locator = .{
            .snapshot_id = "artifact",
            .uri = "/raft/v2/snapshot/fetch/artifact",
            .format = .chunked_manifest_v2,
        },
    }, receiver.iface());
    try std.testing.expect(receiver.seen);
}

test "v2 fetch transfers snapshot ownership before receiver errors" {
    const Executor = struct {
        manifest: []const u8,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":2,\"snapshot_max_chunk_bytes\":65536}"),
            };
            const operation = req.header("x-antfly-raft-snapshot-operation") orelse "";
            if (std.mem.eql(u8, operation, "manifest") or std.mem.eql(u8, operation, "chunk")) {
                if (req.timeout_ms == null) return error.TestExpectedTimeout;
                if (req.cancellation == null) return error.TestExpectedCancellation;
            }
            if (std.mem.eql(u8, operation, "manifest")) return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/x-antflydb-raft-snapshot-manifest-v2"),
                .body = try alloc.dupe(u8, self.manifest),
            };
            if (std.mem.eql(u8, operation, "chunk")) return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/x-antflydb-raft-snapshot-chunk-v2"),
                .body = try alloc.dupe(u8, "owned"),
            };
            return .{ .status = 204 };
        }
    };
    const Receiver = struct {
        fn receive(
            _: *anyopaque,
            _: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            input_snapshot: raft_engine.core.types.Snapshot,
        ) !void {
            var snapshot = input_snapshot;
            defer snapshot.deinit(std.testing.allocator);
            return error.TestReceiverRejected;
        }

        fn iface() raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{ .ptr = undefined, .vtable = &.{ .receive_snapshot = receive } };
        }
    };

    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 92,
        .from = 0,
        .to = 8,
        .request_term = 0,
        .metadata = .{ .index = 13, .term = 5 },
        .data_len = "owned".len,
        .digest = snapshot_transfer.digest("owned"),
    };
    const encoded = try snapshot_transfer.encode(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded);
    var executor = Executor{ .manifest = encoded };
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
    defer transport.deinit();
    try std.testing.expectError(
        error.TestReceiverRejected,
        transport.transport().fetchSnapshot(.{
            .group_id = 92,
            .from = 8,
            .locator = .{
                .snapshot_id = "owned-error",
                .uri = "/raft/v2/snapshot/fetch/owned-error",
                .format = .chunked_manifest_v2,
            },
        }, Receiver.iface()),
    );
}

test "v2 fetch applies receiver admission before allocating or requesting chunks" {
    const Executor = struct {
        manifest: []const u8,
        chunk_requests: usize = 0,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":2,\"snapshot_max_chunk_bytes\":65536}"),
            };
            const operation = req.header("x-antfly-raft-snapshot-operation") orelse "";
            if (std.mem.eql(u8, operation, "manifest") or std.mem.eql(u8, operation, "chunk")) {
                if (req.timeout_ms == null) return error.TestExpectedTimeout;
                if (req.cancellation == null) return error.TestExpectedCancellation;
            }
            if (std.mem.eql(u8, operation, "manifest")) return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/x-antflydb-raft-snapshot-manifest-v2"),
                .body = try alloc.dupe(u8, self.manifest),
            };
            if (std.mem.eql(u8, operation, "chunk")) self.chunk_requests += 1;
            return error.TestUnexpectedRequest;
        }
    };
    const Receiver = struct {
        fn admit(
            _: *anyopaque,
            _: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            _: usize,
        ) !void {
            return error.SnapshotAdmissionBackpressure;
        }

        fn receive(
            _: *anyopaque,
            _: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            _: raft_engine.core.types.Snapshot,
        ) !void {
            return error.TestUnexpectedReceive;
        }

        fn cancel(_: *anyopaque, _: usize) void {}

        fn iface() raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admit_snapshot = admit,
                    .cancel_snapshot_admission = cancel,
                    .receive_snapshot = receive,
                },
            };
        }
    };

    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 93,
        .from = 0,
        .to = 8,
        .request_term = 0,
        .metadata = .{ .index = 13, .term = 5 },
        .data_len = snapshot_transfer.min_chunk_bytes,
        .digest = snapshot_transfer.digest("not-used"),
    };
    const encoded = try snapshot_transfer.encode(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded);
    var executor = Executor{ .manifest = encoded };
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
    defer transport.deinit();
    try std.testing.expectError(error.SnapshotAdmissionBackpressure, transport.transport().fetchSnapshot(.{
        .group_id = 93,
        .from = 8,
        .locator = .{
            .snapshot_id = "admission",
            .uri = "/raft/v2/snapshot/fetch/admission",
            .format = .chunked_manifest_v2,
        },
    }, Receiver.iface()));
    try std.testing.expectEqual(@as(usize, 0), executor.chunk_requests);
}

test "v2 fetch uses bounded parallel artifact-backed transfer" {
    const chunk_size = snapshot_transfer.min_chunk_bytes;
    const payload = try std.testing.allocator.alloc(u8, chunk_size * 4);
    defer std.testing.allocator.free(payload);
    for (payload, 0..) |*byte, index| byte.* = @truncate(index *% 31);

    const manifest: snapshot_transfer.Manifest = .{
        .group_id = 94,
        .from = 0,
        .to = 8,
        .request_term = 0,
        .metadata = .{ .index = 14, .term = 6 },
        .data_len = payload.len,
        .digest = snapshot_transfer.digest(payload),
    };
    const encoded_manifest = try snapshot_transfer.encode(std.testing.allocator, manifest);
    defer std.testing.allocator.free(encoded_manifest);

    const Executor = struct {
        manifest: []const u8,
        payload: []const u8,
        in_flight: std.atomic.Value(usize) = .init(0),
        peak_in_flight: std.atomic.Value(usize) = .init(0),

        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{
                .execute = execute,
                .supports_concurrent_requests = supportsConcurrent,
            } };
        }

        fn supportsConcurrent(_: *const anyopaque) bool {
            return true;
        }

        fn observePeak(self: *@This(), candidate: usize) void {
            var current = self.peak_in_flight.load(.acquire);
            while (candidate > current) {
                if (self.peak_in_flight.cmpxchgWeak(current, candidate, .acq_rel, .acquire)) |observed| {
                    current = observed;
                    continue;
                }
                break;
            }
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":2,\"snapshot_max_chunk_bytes\":65536}"),
            };
            const operation = req.header("x-antfly-raft-snapshot-operation") orelse "";
            if (std.mem.eql(u8, operation, "manifest") or std.mem.eql(u8, operation, "chunk")) {
                if (req.timeout_ms == null) return error.TestExpectedTimeout;
                if (req.cancellation == null) return error.TestExpectedCancellation;
            }
            if (std.mem.eql(u8, operation, "manifest")) return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/x-antflydb-raft-snapshot-manifest-v2"),
                .body = try alloc.dupe(u8, self.manifest),
            };
            if (std.mem.eql(u8, operation, "chunk")) {
                const active = self.in_flight.fetchAdd(1, .acq_rel) + 1;
                defer _ = self.in_flight.fetchSub(1, .acq_rel);
                self.observePeak(active);
                // Hold the first requests briefly so the test proves actual
                // overlap instead of merely observing multiple worker threads.
                var spins: usize = 0;
                while (self.in_flight.load(.acquire) < 4 and spins < 100_000) : (spins += 1)
                    std.Thread.yield() catch {};
                const offset = try std.fmt.parseUnsigned(usize, req.header("x-antfly-raft-snapshot-offset") orelse return error.MissingOffset, 10);
                const length = try std.fmt.parseUnsigned(usize, req.header("x-antfly-raft-snapshot-chunk-length") orelse return error.MissingLength, 10);
                if (offset > self.payload.len or length > self.payload.len - offset)
                    return error.InvalidRange;
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "application/x-antflydb-raft-snapshot-chunk-v2"),
                    .body = try alloc.dupe(u8, self.payload[offset .. offset + length]),
                };
            }
            return .{ .status = 204 };
        }
    };
    const Receiver = struct {
        expected: []const u8,
        received: bool = false,
        owned_snapshot: ?raft_engine.core.types.Snapshot = null,

        fn iface(self: *@This()) raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{ .ptr = self, .vtable = &.{ .receive_snapshot = receive } };
        }

        fn receive(
            ptr: *anyopaque,
            _: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            input_snapshot: raft_engine.core.types.Snapshot,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(input_snapshot.shared_data != null);
            try std.testing.expectEqualSlices(u8, self.expected, input_snapshot.data);
            self.owned_snapshot = input_snapshot;
            self.received = true;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-fetch", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);
    var executor = Executor{ .manifest = encoded_manifest, .payload = payload };
    var receiver = Receiver{ .expected = payload };
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = root_dir,
        .chunk_size = chunk_size,
        .max_parallel_chunks = 4,
    }, executor.iface(), null);
    var transport_live = true;
    defer if (transport_live) transport.deinit();
    defer if (receiver.owned_snapshot) |*snapshot| snapshot.deinit(std.testing.allocator);
    try transport.transport().fetchSnapshot(.{
        .group_id = 94,
        .from = 8,
        .locator = .{
            .snapshot_id = "parallel-artifact",
            .uri = "/raft/v2/snapshot/fetch/parallel-artifact",
            .format = .chunked_manifest_v2,
        },
    }, receiver.iface());
    try std.testing.expect(receiver.received);
    try std.testing.expect(executor.peak_in_flight.load(.acquire) > 1);
    try std.testing.expect(executor.peak_in_flight.load(.acquire) <= 4);
    // The mapped snapshot's quota/control lifetime is independent from the
    // standalone transport and remains valid during orderly host teardown.
    transport.deinit();
    transport_live = false;
}

test "http snapshot transport posts and fetches serialized snapshots" {
    const RecordingExecutor = struct {
        server: *http_server.HttpServer,
        v2_requests: usize = 0,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Simulate an earlier PR build: it advertised wire version 2 but
            // did not serve the isolated v2 route generation.
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2}"),
            };
            if (std.mem.indexOf(u8, req.uri, "/raft/v2/snapshot/") != null)
                self.v2_requests += 1;
            return try self.server.handle(req);
        }
    };

    const Receiver = struct {
        seen: usize = 0,
        index: u64 = 0,

        fn iface(self: *@This()) raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .receive_snapshot = receiveSnapshot,
                },
            };
        }

        fn receiveSnapshot(
            ptr: *anyopaque,
            req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            snapshot: raft_engine.core.types.Snapshot,
        ) !void {
            _ = req;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = snapshot;
            defer owned.deinit(std.testing.allocator);
            self.seen += 1;
            self.index = snapshot.metadata.index;
        }
    };

    const Store = struct {
        body: ?[]u8 = null,

        fn iface(self: *@This()) http_server.SnapshotStore {
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
            return try alloc.dupe(u8, self.body.?);
        }

        fn releaseSnapshot(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body) |body| std.testing.allocator.free(body);
            self.body = null;
        }
    };

    const Noop = struct {
        fn iface(_: *@This()) http_server.BatchHandler {
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

    var store_impl = Store{};
    defer if (store_impl.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var server = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        noop.iface(),
        store_impl.iface(),
        null,
    );
    var executor = RecordingExecutor{ .server = &server };
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
        // Exercise rolling-upgrade fallback through a v1-only store.
        .legacy_max_snapshot_bytes = 1,
    }, executor.iface(), null);
    defer transport.deinit();

    var voters = [_]u64{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "snapshot-bytes");
    defer std.testing.allocator.free(snapshot_bytes);

    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{
                .index = 12,
                .term = 4,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "snap-1", .uri = "/raft/v1/snapshot/upload/snap-1" },
    });

    var receiver = Receiver{};
    try transport.transport().fetchSnapshot(.{
        .group_id = 91,
        .from = 2,
        .locator = .{ .snapshot_id = "snap-1", .uri = "/raft/v1/snapshot/fetch/snap-1" },
    }, receiver.iface());
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);
    try std.testing.expectEqual(@as(u64, 12), receiver.index);
    try std.testing.expectEqual(@as(usize, 0), executor.v2_requests);
    try std.testing.expect(store_impl.body == null);
}

test "http snapshot transport resolves upload uri when locator is absent" {
    const Resolver = struct {
        seen: usize = 0,
        group_id: u64 = 0,
        node_id: u64 = 0,
        snapshot_id: ?[]u8 = null,

        fn iface(self: *@This()) SnapshotTargetResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_upload_uri = resolveUploadUri,
                },
            };
        }

        fn deinit(self: *@This()) void {
            if (self.snapshot_id) |snapshot_id| std.testing.allocator.free(snapshot_id);
            self.* = undefined;
        }

        fn resolveUploadUri(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64, snapshot_id: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.group_id = group_id;
            self.node_id = node_id;
            if (self.snapshot_id) |existing| std.testing.allocator.free(existing);
            self.snapshot_id = try std.testing.allocator.dupe(u8, snapshot_id);
            return try alloc.dupe(u8, "/raft/v1/snapshot/upload/resolved");
        }
    };

    const Executor = struct {
        seen: usize = 0,
        uri: ?[]u8 = null,
        group_id: ?[]u8 = null,
        from: ?[]u8 = null,
        to: ?[]u8 = null,
        term: ?[]u8 = null,
        source_node_id: ?u64 = null,
        body_len: usize = 0,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn deinit(self: *@This()) void {
            if (self.uri) |value| std.testing.allocator.free(value);
            if (self.group_id) |value| std.testing.allocator.free(value);
            if (self.from) |value| std.testing.allocator.free(value);
            if (self.to) |value| std.testing.allocator.free(value);
            if (self.term) |value| std.testing.allocator.free(value);
            self.* = undefined;
        }

        fn capture(dst: *?[]u8, value: ?[]const u8) !void {
            if (dst.*) |existing| std.testing.allocator.free(existing);
            dst.* = if (value) |present| try std.testing.allocator.dupe(u8, present) else null;
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            try capture(&self.uri, req.uri);
            try capture(&self.group_id, req.header("x-antfly-raft-group-id"));
            try capture(&self.from, req.header("x-antfly-raft-from-node-id"));
            try capture(&self.to, req.header("x-antfly-raft-to-node-id"));
            try capture(&self.term, req.header("x-antfly-raft-term"));
            self.source_node_id = req.source_node_id;
            self.body_len = req.body.len;
            return .{ .status = 201 };
        }
    };

    var resolver = Resolver{};
    defer resolver.deinit();
    var executor = Executor{};
    defer executor.deinit();
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), resolver.iface());
    defer transport.deinit();

    var voters = [_]u64{ 1, 2, 3 };
    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .from = 1,
        .to = 2,
        .term = 8,
        .snapshot = .{
            .metadata = .{
                .index = 42,
                .term = 7,
                .conf_state = .{ .voters = voters[0..] },
            },
            .data = @constCast("live-snapshot"),
        },
    });

    try std.testing.expectEqual(@as(usize, 1), resolver.seen);
    try std.testing.expectEqual(@as(u64, 91), resolver.group_id);
    try std.testing.expectEqual(@as(u64, 2), resolver.node_id);
    try std.testing.expectEqualStrings("91-1-2-42-7", resolver.snapshot_id.?);
    try std.testing.expectEqual(@as(usize, 1), executor.seen);
    try std.testing.expectEqualStrings("/raft/v1/snapshot/upload/resolved", executor.uri.?);
    try std.testing.expectEqualStrings("91", executor.group_id.?);
    try std.testing.expectEqualStrings("1", executor.from.?);
    try std.testing.expectEqualStrings("2", executor.to.?);
    try std.testing.expectEqualStrings("8", executor.term.?);
    try std.testing.expectEqual(@as(?u64, 1), executor.source_node_id);
    try std.testing.expect(executor.body_len > 0);
}

test "http snapshot transport omits live upload headers for store-only locator uploads" {
    const Executor = struct {
        seen: usize = 0,
        headers_len: usize = 0,
        source_node_id: ?u64 = 99,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
            self.headers_len = req.headers.len;
            self.source_node_id = req.source_node_id;
            try std.testing.expect(req.header("x-antfly-raft-group-id") == null);
            try std.testing.expect(req.body.len > 0);
            return .{ .status = 201 };
        }
    };

    var executor = Executor{};
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), null);
    defer transport.deinit();

    var voters = [_]u64{ 1, 2 };
    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{
                .index = 12,
                .term = 4,
                .conf_state = .{ .voters = voters[0..] },
            },
            .data = @constCast("store-only-snapshot"),
        },
        .locator = .{ .snapshot_id = "snap-1", .uri = "/raft/v1/snapshot/upload/snap-1" },
    });

    try std.testing.expectEqual(@as(usize, 1), executor.seen);
    try std.testing.expectEqual(@as(usize, 0), executor.headers_len);
    try std.testing.expectEqual(@as(?u64, null), executor.source_node_id);
}

test "versioned v2 store-only upload uses its locator and artifact purpose" {
    const Executor = struct {
        transfer_requests: usize = 0,
        saw_artifact_manifest: bool = false,
        max_chunk_body: usize = 0,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, routes.Routes.capabilities)) {
                return .{
                    .status = 200,
                    .body = try std.fmt.allocPrint(
                        alloc,
                        "{{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":2,\"snapshot_max_chunk_bytes\":{d}}}",
                        .{snapshot_transfer.min_chunk_bytes},
                    ),
                };
            }
            try std.testing.expectEqualStrings("/raft/v2/snapshot/upload/snap-v2", req.uri);
            self.transfer_requests += 1;
            if (std.mem.eql(u8, req.header("x-antfly-raft-snapshot-operation") orelse "", "begin")) {
                var manifest = try snapshot_transfer.decode(alloc, req.body);
                defer manifest.deinit(alloc);
                switch (manifest.purpose()) {
                    .bootstrap_artifact => |purpose| {
                        try std.testing.expectEqual(@as(u64, 2), purpose.owner_node_id);
                        self.saw_artifact_manifest = true;
                    },
                    .live_install => return error.TestUnexpectedResult,
                }
            } else if (std.mem.eql(u8, req.header("x-antfly-raft-snapshot-operation") orelse "", "chunk")) {
                self.max_chunk_body = @max(self.max_chunk_body, req.body.len);
            }
            return .{ .status = 201 };
        }
    };

    var executor = Executor{};
    var transport = try HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), null);
    defer transport.deinit();
    const body = try std.testing.allocator.alloc(u8, snapshot_transfer.min_chunk_bytes + 1);
    defer std.testing.allocator.free(body);
    @memset(body, 's');
    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{ .index = 12, .term = 4 },
            .data = body,
        },
        .locator = .{
            .snapshot_id = "snap-v2",
            .uri = "/raft/v2/snapshot/upload/snap-v2",
            .format = .chunked_manifest_v2,
        },
    });
    try std.testing.expect(executor.saw_artifact_manifest);
    try std.testing.expectEqual(@as(usize, 4), executor.transfer_requests);
    try std.testing.expectEqual(snapshot_transfer.min_chunk_bytes, executor.max_chunk_body);

    // Catalogs written before the explicit format field can still carry the
    // v2 route. That persisted route must remain authoritative after upgrade.
    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{ .index = 12, .term = 4 },
            .data = body,
        },
        .locator = .{
            .snapshot_id = "snap-v2",
            .uri = "/raft/v2/snapshot/upload/snap-v2",
        },
    });
    try std.testing.expectEqual(@as(usize, 8), executor.transfer_requests);
}
