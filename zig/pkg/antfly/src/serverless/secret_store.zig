// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Independent secret namespace; never nested under a table manifest/lifetime.
const std = @import("std");
const collection = @import("../common/secret_collection.zig");
const objects = @import("../storage/object_storage.zig");

pub const Store = collection.Store(Backend);

/// All fields are borrowed. The host must retain the client, bucket and prefix.
/// Explicit opt-in is required: an eventually consistent or unconditional-only
/// client cannot implement the secret contract. This is a provider guarantee,
/// not a capability inferred from a successful individual request.
pub const Backend = struct {
    client: objects.ObjectStorage,
    /// GCS JSON uploads require generation CAS, not ETag headers.
    gcs_client: ?*objects.Gcs.JsonApiClient = null,
    bucket: []const u8,
    prefix: []const u8,
    consistency: enum { unsupported, linearizable_cas } = .unsupported,

    fn validate(self: Backend) !void {
        if (self.consistency != .linearizable_cas) return error.UnsupportedOperation;
        if (self.bucket.len == 0 or (self.prefix.len > 0 and self.prefix[self.prefix.len - 1] == '/')) return error.InvalidArgument;
    }

    pub fn objectKey(self: Backend, alloc: std.mem.Allocator, scope: []const u8) ![]u8 {
        try self.validate();
        return std.fmt.allocPrint(alloc, "{s}{s}secrets/v1/{s}/collection", .{ self.prefix, if (self.prefix.len > 0) "/" else "", collection.scopeDigest(scope) });
    }

    pub fn read(self: *Backend, alloc: std.mem.Allocator, scope: []const u8) !collection.Snapshot {
        const key = try self.objectKey(alloc, scope);
        defer alloc.free(key);
        var client = self.client;
        var result = client.getObject(self.bucket, key, .{ .max_response_bytes = collection.max_bytes, .skip_metadata_probe = true }) catch |err| switch (err) {
            error.FileNotFound => {
                // ObjectStorage represents both missing buckets and missing
                // keys as FileNotFound. Only the latter is authoritative absence.
                if (!try client.bucketExists(self.bucket)) return error.Unavailable;
                return .{};
            },
            else => return err,
        };
        defer result.deinit(client.allocator);
        if (result.body.len > collection.max_bytes) return error.CorruptInput;
        const etag = (if (self.gcs_client != null) result.metadata.version_id else result.metadata.etag) orelse return error.UnsupportedOperation;
        if (etag.len == 0) return error.UnsupportedOperation;
        const body = try alloc.dupe(u8, result.body);
        errdefer alloc.free(body);
        return .{ .bytes = body, .token = try alloc.dupe(u8, etag) };
    }

    pub fn publish(self: *Backend, alloc: std.mem.Allocator, scope: []const u8, previous: collection.Snapshot, bytes: []const u8) !void {
        const key = try self.objectKey(alloc, scope);
        defer alloc.free(key);
        if ((previous.bytes == null) != (previous.token == null)) return error.InvalidArgument;
        var client = self.client;
        // One bounded collection is the head and payload in a single atomic
        // CAS. No second object, torn head publication, or orphan GC is needed.
        if (self.gcs_client) |gcs| {
            gcs.putObjectAtGeneration(alloc, self.bucket, key, bytes, previous.token orelse "0", "application/vnd.antfly.secret-collection.v1") catch |err| switch (err) {
                error.PreconditionFailed => return error.Conflict,
                else => return error.OutcomeUnknown,
            };
            return;
        }
        var result = client.putObject(self.bucket, key, bytes, .{
            .if_none_match = previous.bytes == null,
            .if_match_etag = previous.token,
            .content_type = "application/vnd.antfly.secret-collection.v1",
        }) catch |err| switch (err) {
            error.PreconditionFailed => return error.Conflict,
            error.UnsupportedOperation => return err,
            else => return error.OutcomeUnknown,
        };
        result.deinit(client.allocator);
    }
};
