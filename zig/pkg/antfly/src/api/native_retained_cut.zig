// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Server-written native cursor capabilities; physical owners retain immutable
//! generations. Credentials, incarnation and recipes are checked each page.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Digest = [32]u8;
pub const prefix = "native2:";
pub const Descriptor = struct { version: u16 = 1, expires_ms: u64, table_id: u64, desired: Digest, id: []const u8 };
fn domain(table: u64, identity: Digest) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-retained-query-cuts-v1");
    hash.update(&identity);
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, table, .little);
    hash.update(&bytes);
    var digest: Digest = undefined;
    hash.final(&digest);
    return digest;
}
fn recipe(table: anytype) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    for ([_][]const u8{ table.schema_json, table.read_schema_json, table.indexes_json }) |bytes| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, bytes.len, .little);
        hash.update(&length);
        hash.update(bytes);
    }
    var digest: Digest = undefined;
    hash.final(&digest);
    return digest;
}
pub fn save(a: A, store: *stores.ArtifactStore, identity: Digest, io: std.Io, table: anytype, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) ![]const u8 {
    try collect(store, table.table_id, identity, now, cancellation);
    var nonce: [32]u8 = undefined;
    try io.randomSecure(&nonce);
    const id = std.fmt.bytesToHex(nonce, .lower);
    const descriptor: Descriptor = .{ .expires_ms = now +| @import("lake_retained_cut.zig").ttl_ms, .table_id = table.table_id, .desired = recipe(table), .id = &id };
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(bytes);
    const scope = try stores.UploadScope.forPublication(domain(table.table_id, identity), descriptor.expires_ms, io);
    var metadata = try store.putScoped(scope, bytes, cancellation);
    defer metadata.deinit(store.allocator);
    return std.fmt.allocPrint(a, "{s}{s}:{d}", .{ prefix, metadata.artifact_id, metadata.byte_len });
}
pub fn load(a: A, store: *stores.ArtifactStore, identity: Digest, token: []const u8, table: anytype, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !Descriptor {
    if (!std.mem.startsWith(u8, token, prefix)) return error.InvalidQueryRequest;
    const split = std.mem.lastIndexOfScalar(u8, token, ':') orelse return error.InvalidQueryRequest;
    const artifact = token[prefix.len..split];
    const scope = (stores.uploadScopeFromArtifactId(artifact) catch return error.InvalidQueryRequest) orelse return error.InvalidQueryRequest;
    if (scope.fencingToken() <= now or !std.mem.eql(u8, &scope.domain, &domain(table.table_id, identity))) return error.CatalogGenerationChanged;
    const length = std.fmt.parseInt(usize, token[split + 1 ..], 10) catch return error.InvalidQueryRequest;
    if (length == 0 or length > 4096) return error.InvalidQueryRequest;
    const bytes = store.getVerifiedAllocWithCancellation(artifact, length, try stores.sha256ChecksumFromArtifactId(artifact), cancellation) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound, error.ArtifactIntegrityMismatch => return error.CatalogGenerationChanged,
        else => return err,
    };
    defer store.allocator.free(bytes);
    const descriptor = std.json.parseFromSliceLeaky(Descriptor, a, bytes, .{ .allocate = .alloc_always }) catch return error.CatalogGenerationChanged;
    if (descriptor.version != 1 or descriptor.expires_ms != scope.fencingToken() or descriptor.expires_ms > now +| @import("lake_retained_cut.zig").ttl_ms or descriptor.table_id != table.table_id or !std.mem.eql(u8, &descriptor.desired, &recipe(table)) or descriptor.id.len != 64) return error.CatalogGenerationChanged;
    return descriptor;
}

pub fn collect(store: *stores.ArtifactStore, table: u64, store_identity: Digest, now: u64, cancellation: @import("antfly_cancellation").CancellationToken) !void {
    const Visitor = struct {
        store: *stores.ArtifactStore,
        now: u64,
        deleted: usize = 0,
        fn visit(raw: *anyopaque, scope: stores.UploadScope, artifact_id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (scope.fencingToken() +| 30_000 >= self.now) return;
            if (self.deleted == 128) return error.RetainedCutCollectionBound;
            try self.store.delete(artifact_id);
            self.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .store = store, .now = now };
    store.visitScopedUploads(domain(table, store_identity), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = 128 }, cancellation) catch |err| switch (err) {
        error.RetainedCutCollectionBound => {},
        else => return err,
    };
}
