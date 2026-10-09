// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native authenticated object reads through lib/objectstore. The generic module
//! parameter keeps network/auth dependencies out of portable media consumers.
const std = @import("std");
const remote = @import("remote.zig");
const source = @import("source.zig");

/// Instantiate with @import("objectstore"). Borrow a configured production
/// client, or create an owned S3/GCS client with built-in HTTP and authentication.
/// Single-consumer; bucket/key/version strings and shared I/O must outlive it.
pub fn Adapter(comptime store: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        client: store.Client,
        bucket: []const u8,
        object: remote.ObjectRange,
        owned: union(enum) { none, s3: store.s3.Client, gcs: store.gcs.JsonApiClient } = .none,

        pub fn borrow(allocator: std.mem.Allocator, client: store.Client, bucket: []const u8, key: []const u8, version: []const u8, kind: remote.VersionKind, length: u64) Self {
            return .{ .allocator = allocator, .client = client, .bucket = bucket, .object = .{ .transport = undefined, .object = key, .version = version, .version_kind = kind, .length = length } };
        }
        /// Takes ownership of cfg only on success; callers supply credentials or
        /// a credential provider, never an HTTP/authentication callback.
        pub fn createS3(allocator: std.mem.Allocator, cfg: store.s3.Config, bucket: []const u8, key: []const u8, version: []const u8, kind: remote.VersionKind, length: u64) !*Self {
            if (kind == .gcs_generation) return error.InvalidObjectProvider;
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);
            self.* = borrow(allocator, undefined, bucket, key, version, kind, length);
            _ = try self.range(); // validate immutable identity before owning cfg
            self.owned = .{ .s3 = try store.s3.Client.init(allocator, cfg) };
            self.client = self.owned.s3.client();
            return self;
        }
        pub fn createGcs(allocator: std.mem.Allocator, cfg: store.gcs.JsonApiConfig, bucket: []const u8, key: []const u8, generation: []const u8, length: u64) !*Self {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);
            self.* = borrow(allocator, undefined, bucket, key, generation, .gcs_generation, length);
            _ = try self.range();
            self.owned = .{ .gcs = try store.gcs.JsonApiClient.init(allocator, cfg) };
            self.client = self.owned.gcs.client();
            return self;
        }
        /// Only for adapters returned by createS3/createGcs. Borrowed adapters
        /// neither own nor destroy their configured client.
        pub fn destroy(self: *Self) void {
            switch (self.owned) {
                .none => unreachable,
                .s3 => |*client| client.deinit(),
                .gcs => |*client| client.deinit(),
            }
            const allocator = self.allocator;
            allocator.destroy(self);
        }
        pub fn range(self: *Self) !source.Range {
            self.object.transport = .{ .context = self, .get = get };
            return self.object.range();
        }
        const Cancellation = struct {
            control: source.Control,
            fn cancelled(raw: *const anyopaque) bool {
                const self: *const @This() = @ptrCast(@alignCast(raw));
                self.control.check() catch return true;
                return false;
            }
        };
        fn get(raw: *anyopaque, request: remote.Request, out: []u8, control: source.Control) !remote.Response {
            const self: *Self = @ptrCast(@alignCast(raw));
            try control.check();
            const cancel = Cancellation{ .control = control };
            var client = self.client;
            client.allocator = self.allocator;
            var result = client.getObject(self.bucket, request.object, .{
                .version_id = if (request.version_kind == .strong_etag) null else request.version,
                .if_match_etag = if (request.version_kind == .strong_etag) request.version else null,
                .range = .{ .offset = request.offset, .length = out.len },
                .skip_metadata_probe = true,
                .max_response_bytes = out.len,
                .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancellation.cancelled },
            }) catch |err| {
                try control.check();
                return err;
            };
            defer result.deinit(self.allocator);
            try control.check();
            const wire = result.response_range orelse return error.InvalidRemoteRange;
            if (result.response_status != 206 or wire.offset != request.offset or wire.total != self.object.length or wire.length != result.body.len or result.body.len == 0 or result.body.len > out.len) return error.InvalidRemoteRange;
            switch (request.version_kind) {
                .strong_etag => {
                    // S3 normalizes strong response ETags; GCS retains quotes.
                    if (!result.response_etag_strong) return error.SourceVersionChanged;
                    const etag = result.metadata.etag orelse return error.SourceVersionChanged;
                    const normalized = if (etag.len >= 2 and etag[0] == '"' and etag[etag.len - 1] == '"') etag[1 .. etag.len - 1] else etag;
                    if (!std.mem.eql(u8, normalized, request.version[1 .. request.version.len - 1])) return error.SourceVersionChanged;
                },
                .s3_version_id, .gcs_generation => if (!std.mem.eql(u8, result.metadata.version_id orelse return error.SourceVersionChanged, request.version)) return error.SourceVersionChanged,
            }
            @memcpy(out[0..result.body.len], result.body);
            return .{ .status = 206, .offset = wire.offset, .total_length = wire.total, .bytes_written = result.body.len, .range_length = result.body.len, .version = request.version };
        }
    };
}
