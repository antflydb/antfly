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
const common = @import("http_common.zig");
const http_server = @import("http_server.zig");
const routes = @import("routes.zig");
const snapshot_transfer = @import("snapshot_transfer.zig");

pub const HttpSnapshotConfig = struct {
    root_dir: []const u8,
    chunk_size: usize = 1 << 20,
    legacy_max_snapshot_bytes: usize = 8 * 1024 * 1024,
    /// Common request/response ceiling for the non-streaming v1 envelope. The
    /// default matches StdHttpExecutorConfig.max_response_bytes so a snapshot
    /// accepted by a default host is fetchable by another default host.
    legacy_fallback_max_request_bytes: usize = 4 * 1024 * 1024,
    max_snapshot_bytes: usize = 1 << 30,
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

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpSnapshotConfig,
        executor: common.RequestExecutor,
        resolver: ?SnapshotTargetResolver,
    ) HttpSnapshotTransport {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .executor = executor,
            .resolver = resolver,
        };
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
                    const peer_max = if (capabilities) |value|
                        value.max_chunk_bytes
                    else
                        snapshot_transfer.min_chunk_bytes;
                    return try self.sendChunkedSnapshot(req, target.upload_uri, &live_headers, peer_max);
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

        var begin_headers: [6]common.RequestHeader = undefined;
        const begin_count = appendTransferHeaders(&begin_headers, identity_headers, "begin", null, null);
        try self.executeExpectedSuccess(.{
            .method = .POST,
            .uri = uri,
            .headers = begin_headers[0..begin_count],
            .source_node_id = if (req.from == 0) null else req.from,
            .content_type = "application/x-antflydb-raft-snapshot-manifest-v2",
            .body = encoded_manifest,
        });
        errdefer {
            var abort_headers: [6]common.RequestHeader = undefined;
            const abort_count = appendTransferHeaders(&abort_headers, identity_headers, "abort", null, null);
            self.executeExpectedSuccess(.{
                .method = .DELETE,
                .uri = uri,
                .headers = abort_headers[0..abort_count],
                .source_node_id = if (req.from == 0) null else req.from,
            }) catch |err| std.log.warn("snapshot upload abort deferred uri={s} err={s}", .{
                uri,
                @errorName(err),
            });
        }

        var offset: usize = 0;
        while (offset < req.snapshot.data.len) {
            const end = @min(req.snapshot.data.len, offset + transfer_chunk_size);
            var offset_buf: [32]u8 = undefined;
            const encoded_offset = try std.fmt.bufPrint(&offset_buf, "{d}", .{offset});
            var chunk_headers: [7]common.RequestHeader = undefined;
            const chunk_count = appendTransferHeaders(&chunk_headers, identity_headers, "chunk", encoded_offset, null);
            try self.executeExpectedSuccess(.{
                .method = .PUT,
                .uri = uri,
                .headers = chunk_headers[0..chunk_count],
                .source_node_id = if (req.from == 0) null else req.from,
                .content_type = "application/x-antflydb-raft-snapshot-chunk-v2",
                .body = req.snapshot.data[offset..end],
            });
            offset = end;
        }

        var commit_headers: [6]common.RequestHeader = undefined;
        const commit_count = appendTransferHeaders(&commit_headers, identity_headers, "commit", null, null);
        try self.executeExpectedSuccess(.{
            .method = .POST,
            .uri = uri,
            .headers = commit_headers[0..commit_count],
            .source_node_id = if (req.from == 0) null else req.from,
        });
    }

    fn executeExpectedSuccess(self: *HttpSnapshotTransport, req: common.HttpRequest) !void {
        var resp = try self.executor.execute(self.alloc, req);
        defer resp.deinit(self.alloc);
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
        });
        defer legacy.deinit(self.alloc);
        try mapSnapshotFetchStatus(legacy.status);
        if (legacy.body.len > self.cfg.legacy_fallback_max_request_bytes)
            return error.SnapshotTooLarge;
        const snapshot = try decodeSnapshotEnvelopeWithLimits(self.alloc, legacy.body, .{
            .max_snapshot_bytes = self.cfg.max_snapshot_bytes,
        });
        try receiver.receiveSnapshot(req, snapshot);
        var release = self.executor.execute(self.alloc, .{
            .method = .DELETE,
            .uri = req.locator.uri,
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
        if (require_capability and capabilities == null) return error.SnapshotArtifactNotFound;
        const peer_max = if (capabilities) |value|
            value.max_chunk_bytes
        else
            snapshot_transfer.min_chunk_bytes;
        return try self.fetchSnapshotV2(req, receiver, target.fetch_uri, peer_max);
    }

    fn mapSnapshotFetchStatus(status: u16) !void {
        return switch (status) {
            200...299 => {},
            404 => error.SnapshotArtifactNotFound,
            410 => error.SnapshotArtifactExpired,
            409 => error.SnapshotArtifactNotCommitted,
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
        const manifest_headers = [_]common.RequestHeader{
            .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" },
            .{ .name = "x-antfly-raft-snapshot-operation", .value = "manifest" },
        };
        var resp = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = fetch_uri,
            .headers = &manifest_headers,
        });
        defer resp.deinit(self.alloc);
        try mapSnapshotFetchStatus(resp.status);
        if (resp.content_type == null or
            !std.mem.eql(u8, resp.content_type.?, "application/x-antflydb-raft-snapshot-manifest-v2"))
            return error.InvalidSnapshotManifestResponse;

        var manifest = try snapshot_transfer.decode(self.alloc, resp.body);
        defer manifest.deinit(self.alloc);
        if (manifest.group_id != req.group_id)
            return error.SnapshotTransferIdentityMismatch;
        switch (manifest.purpose()) {
            .bootstrap_artifact => |artifact| if (artifact.owner_node_id != req.from)
                return error.SnapshotTransferIdentityMismatch,
            .live_install => return error.SnapshotTransferIdentityMismatch,
        }
        if (manifest.data_len > self.cfg.max_snapshot_bytes) return error.SnapshotTooLarge;
        const data_len = std.math.cast(usize, manifest.data_len) orelse return error.SnapshotTooLarge;
        const data = try self.alloc.alloc(u8, data_len);
        var data_owned = true;
        defer if (data_owned) self.alloc.free(data);
        var offset: usize = 0;
        while (offset < data.len) {
            const requested = @min(transfer_chunk_size, data.len - offset);
            var offset_buf: [32]u8 = undefined;
            var length_buf: [32]u8 = undefined;
            const encoded_offset = try std.fmt.bufPrint(&offset_buf, "{d}", .{offset});
            const encoded_length = try std.fmt.bufPrint(&length_buf, "{d}", .{requested});
            const chunk_headers = [_]common.RequestHeader{
                .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" },
                .{ .name = "x-antfly-raft-snapshot-operation", .value = "chunk" },
                .{ .name = "x-antfly-raft-snapshot-offset", .value = encoded_offset },
                .{ .name = "x-antfly-raft-snapshot-chunk-length", .value = encoded_length },
            };
            var chunk = try self.executor.execute(self.alloc, .{
                .method = .GET,
                .uri = fetch_uri,
                .headers = &chunk_headers,
            });
            errdefer chunk.deinit(self.alloc);
            try mapSnapshotFetchStatus(chunk.status);
            if (chunk.content_type == null or
                !std.mem.eql(u8, chunk.content_type.?, "application/x-antflydb-raft-snapshot-chunk-v2") or
                chunk.body.len != requested)
                return error.InvalidSnapshotChunkResponse;
            @memcpy(data[offset .. offset + requested], chunk.body);
            chunk.deinit(self.alloc);
            offset += requested;
        }
        const actual_digest = snapshot_transfer.digest(data);
        if (!std.mem.eql(u8, &actual_digest, &manifest.digest))
            return error.SnapshotChecksumMismatch;
        const metadata = manifest.metadata;
        manifest.metadata = .{};
        // SnapshotReceiver owns both allocations once invoked, even when Raft
        // admission returns an error. Relinquish locally before the call so an
        // error cannot double-free the receiver's message.
        data_owned = false;
        try receiver.receiveSnapshot(req, .{ .metadata = metadata, .data = data });

        const release_headers = [_]common.RequestHeader{
            .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" },
            .{ .name = "x-antfly-raft-snapshot-operation", .value = "release" },
        };
        self.executeExpectedSuccess(.{
            .method = .DELETE,
            .uri = fetch_uri,
            .headers = &release_headers,
        }) catch |err| std.log.warn("snapshot fetch artifact release deferred snapshot_id={s} err={s}", .{
            req.locator.snapshot_id,
            @errorName(err),
        });
    }

    fn appendTransferHeaders(
        out: []common.RequestHeader,
        identity: []const common.RequestHeader,
        operation: []const u8,
        offset: ?[]const u8,
        chunk_length: ?[]const u8,
    ) usize {
        std.debug.assert(out.len >= identity.len + 2 + @intFromBool(offset != null) + @intFromBool(chunk_length != null));
        @memcpy(out[0..identity.len], identity);
        var count = identity.len;
        out[count] = .{ .name = "x-antfly-raft-snapshot-protocol", .value = "2" };
        count += 1;
        out[count] = .{ .name = "x-antfly-raft-snapshot-operation", .value = operation };
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
                    .body = try alloc.dupe(u8, "{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":1}"),
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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
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
            const operation = req.header("x-antfly-raft-snapshot-operation") orelse "";
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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{ .root_dir = "/tmp" }, executor.iface(), null);
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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
        // Exercise rolling-upgrade fallback through a v1-only store.
        .legacy_max_snapshot_bytes = 1,
    }, executor.iface(), null);

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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), resolver.iface());

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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), null);

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
                        "{{\"snapshot_transfer_protocol_version\":2,\"snapshot_transfer_route_version\":1,\"snapshot_max_chunk_bytes\":{d}}}",
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
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), null);
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
