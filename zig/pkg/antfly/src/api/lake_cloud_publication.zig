// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Generation maps and certificates are catalog authority, never mutable
//! advisory sidecars. Bind once per query owner; nested maintenance views restore
//! the previous binding before borrowed readers can be released.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const maps = @import("../serverless/artifacts/cloud_generations.zig");
const Object = @import("../serverless/artifacts/object_store.zig").ObjectStore;
const stores = @import("../serverless/artifacts/store.zig");
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
fn digestRoot(id: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(id, &result, .{});
    return result;
}
pub fn certificateBinding(a: A, publication: catalog.Publication, certificate: catalog.CloudRetentionCertificate) ![32]u8 {
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .format = "native-lake-cloud-retention-v1", .reader_protocol = publication.reader_protocol, .namespace = publication.namespace, .generation = publication.generation, .token = publication.token, .signature = publication.signature, .store_locator = publication.store_locator, .base_source = publication.base_source, .inventory = publication.inventory, .directory = publication.directory, .map = publication.cloud_generations.?.root, .complete = publication.cloud_generations.?.complete, .until_ms = certificate.until_ms, .roots = certificate.roots }, .{});
    defer a.free(encoded);
    return digestRoot(encoded);
}
pub const Binding = struct {
    a: A,
    object: *Object,
    reader: *maps.Reader,
    roots: []const [32]u8 = &.{},
    previous_reader: ?*maps.Reader,
    previous_roots: []const [32]u8,
    previous_until: u64,
    pub fn initMap(a: A, object: *Object, cloud: catalog.CloudGenerations, token: Token) !Binding {
        try cloud.root.validate();
        try token.check();
        const reader = try a.create(maps.Reader);
        errdefer a.destroy(reader);
        const root = try maps.cloneRef(a, cloud.root);
        errdefer maps.freeRef(a, root);
        reader.* = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .root = root, .complete = cloud.complete };
        const result: Binding = .{ .a = a, .object = object, .reader = reader, .previous_reader = object.cloud_reader, .previous_roots = object.cloud_certified_roots, .previous_until = object.cloud_certificate_until_ms };
        object.cloud_reader = reader;
        object.cloud_certified_roots = &.{};
        object.cloud_certificate_until_ms = 0;
        return result;
    }
    pub fn init(a: A, object: *Object, publication: catalog.Publication, token: Token) !?Binding {
        const cloud = publication.cloud_generations orelse return null;
        try publication.validate();
        var result = try initMap(a, object, cloud, token);
        errdefer result.deinit();
        var until: u64 = 0;
        if (object.cloud_retention_qualified) if (cloud.certificate) |certificate| {
            const binding = try certificateBinding(a, publication, certificate);
            if (!std.mem.eql(u8, &binding, &certificate.binding)) return error.InvalidCloudRetentionCertificate;
            const provider_now = try object.client.providerTime(object.bucket, @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn));
            if (provider_now != null and provider_now.? +| 60_000 < certificate.until_ms) {
                result.roots = try a.dupe([32]u8, certificate.roots);
                until = certificate.until_ms;
            }
        };
        object.cloud_certified_roots = result.roots;
        object.cloud_certificate_until_ms = until;
        return result;
    }
    pub fn deinit(self: *Binding) void {
        self.object.cloud_reader = self.previous_reader;
        self.object.cloud_certified_roots = self.previous_roots;
        self.object.cloud_certificate_until_ms = self.previous_until;
        self.reader.deinit();
        maps.freeRef(self.a, self.reader.root.?);
        self.a.destroy(self.reader);
        if (self.roots.len != 0) self.a.free(self.roots);
    }
};
/// Full migration audit is bounded by the existing closure census limits. If
/// its optional certificate budget is exhausted, keep version-qualified records
/// already observed and retain ordinary required-read/audit semantics.
pub fn finish(a: A, writer: *maps.Writer, object: *Object, publication: *catalog.Publication, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, table_id: u64, identity: [32]u8, context: local.serverless_query_lake_read_context.Context, token: Token, inherited_certificate: bool) !void {
    const fresh_provider_now = if (inherited_certificate) try object.client.providerTime(object.bucket, @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)) else null;
    const inherit = fresh_provider_now != null and fresh_provider_now.? +| 60_000 < object.cloud_certificate_until_ms;
    if (!inherit and !writer.disabled) {
        // Rebase expired/legacy evidence over the exact live closure. Orphan
        // records cannot retain zero/expired evidence indefinitely.
        writer.rebase();
        var collector: @import("lake_index_gc.zig").Collector = .{ .a = std.heap.page_allocator, .table = table_id, .authority = undefined, .store = object.artifactStore(), .identity = identity, .context = context, .token = @splat(1), .namespace = publication.namespace, .audit_only = true, .remaining_reads = 512 * 1024 * 1024 };
        defer {
            var keys = collector.marked.keyIterator();
            while (keys.next()) |key| collector.a.free(key.*);
            collector.marked.deinit(collector.a);
        }
        collector.markPublication(publication.*) catch |err| switch (err) {
            error.LakeIndexGcBudgetExceeded, error.ArtifactReadBudgetExceeded => writer.complete = false,
            else => return err,
        };
    }
    const root = (try writer.finish(token)) orelse return;
    defer maps.freeRef(writer.a, root);
    publication.cloud_generations = .{ .root = try maps.cloneRef(a, root), .complete = writer.complete and !writer.disabled };
    if (!publication.cloud_generations.?.complete or !writer.qualified_retention or root.retention_until_ms == 0) return;
    const provider_now = (try object.client.providerTime(object.bucket, @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn))) orelse return;
    if (provider_now +| 60_000 >= root.retention_until_ms) return;
    var roots: std.ArrayList([32]u8) = .empty;
    defer roots.deinit(a);
    try roots.append(a, digestRoot(publication.inventory.artifact_id));
    if (publication.directory) |directory| try roots.append(a, digestRoot(directory.artifact_id));
    for (declarations) |declaration| try roots.append(a, digestRoot(declaration.artifact.artifact_id));
    if (roots.items.len > 4096) return;
    std.mem.sort([32]u8, roots.items, {}, struct {
        fn less(_: void, l: [32]u8, r: [32]u8) bool {
            return std.mem.order(u8, &l, &r) == .lt;
        }
    }.less);
    var length: usize = 0;
    for (roots.items) |root_id| {
        if (length != 0 and std.mem.eql(u8, &roots.items[length - 1], &root_id)) continue;
        roots.items[length] = root_id;
        length += 1;
    }
    var certificate: catalog.CloudRetentionCertificate = .{ .until_ms = root.retention_until_ms, .roots = try a.dupe([32]u8, roots.items[0..length]), .binding = undefined };
    certificate.binding = try certificateBinding(a, publication.*, certificate);
    publication.cloud_generations.?.certificate = certificate;
}

/// GC can read identical content through any live publication's qualified
/// version. Its name-level live set conservatively retains every version of a
/// live artifact; it never deletes a version needed by a different reader cut.
pub const UnionBinding = struct {
    a: A,
    object: *Object,
    readers: []maps.Reader,
    pointers: []*maps.Reader,
    union_reader: *maps.Reader,
    previous: ?*maps.Reader,
    pub fn init(a: A, object: *Object, publications: []const catalog.Publication, retired: []const u64, namespace: ?[32]u8, identity: [32]u8) !UnionBinding {
        var selected: std.ArrayList(catalog.CloudGenerations) = .empty;
        defer selected.deinit(a);
        for (publications) |publication| {
            if (!std.meta.eql(publication.namespace, namespace) or !std.mem.eql(u8, &publication.signature.store, &identity)) continue;
            const dead = for (retired) |generation| {
                if (generation == publication.generation) break true;
            } else false;
            if (!dead) if (publication.cloud_generations) |cloud| try selected.append(a, cloud);
        }
        const readers = try a.alloc(maps.Reader, selected.items.len);
        errdefer a.free(readers);
        const pointers = try a.alloc(*maps.Reader, readers.len);
        errdefer a.free(pointers);
        for (readers, pointers, selected.items, 0..) |*reader, *pointer, cloud, i| {
            reader.* = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .root = cloud.root, .cache_limit = if (i < 4) 2 * 1024 * 1024 else 0 };
            pointer.* = reader;
        }
        const union_reader = try a.create(maps.Reader);
        union_reader.* = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .alternatives = pointers };
        const previous = object.cloud_reader;
        object.cloud_reader = union_reader;
        return .{ .a = a, .object = object, .readers = readers, .pointers = pointers, .union_reader = union_reader, .previous = previous };
    }
    pub fn deinit(self: *UnionBinding) void {
        self.object.cloud_reader = self.previous;
        for (self.readers) |*reader| reader.deinit();
        self.union_reader.deinit();
        self.a.destroy(self.union_reader);
        self.a.free(self.readers);
        self.a.free(self.pointers);
    }
};

test "external lake global frontier cloud certificates bind publications restart generations expiry and backend identity" {
    const a = std.testing.allocator;
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try Object.initWithClient(a, client, "bucket", "native");
    defer object.deinit();
    object.cloud_retention_qualified = true;
    const table_id: u64 = 7;
    const identity: [32]u8 = @splat(3);
    const namespace: [32]u8 = @splat(4);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table_id, identity, namespace);
    const scope: stores.UploadScope = .{ .domain = domain, .attempt = @splat(2) };
    var writer: maps.Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope, .qualified_retention = true, .complete = true };
    defer writer.deinit();
    object.cloud_writer = &writer;
    defer object.cloud_writer = null;
    var artifacts = object.artifactStore();
    var inventory = try artifacts.putScoped(scope, "inventory", .none);
    defer inventory.deinit(a);
    var publication: catalog.Publication = .{ .reader_protocol = catalog.native_reader_protocol, .namespace = namespace, .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = identity }, .published_at_ms = 101, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://source/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len } };
    try finish(a, &writer, &object, &publication, &.{}, table_id, identity, .{ .io = std.testing.io }, .none, false);
    const cloud = publication.cloud_generations.?;
    defer maps.freeRef(a, cloud.root);
    defer a.free(cloud.certificate.?.roots);
    try std.testing.expect(cloud.complete);
    try std.testing.expectEqual(provider.until_ms, cloud.certificate.?.until_ms);
    object.cloud_writer = null;
    // Fresh owner has neither a verification cache nor decoded map pages.
    var reopened = try Object.initWithClient(a, client, "bucket", "native");
    defer reopened.deinit();
    reopened.cloud_retention_qualified = true;
    var binding = (try Binding.init(a, &reopened, publication, .none)).?;
    defer binding.deinit();
    const suffix = try stores.storageSuffixAlloc(a, inventory.artifact_id);
    defer a.free(suffix);
    const path = try std.fmt.allocPrint(a, "native/{s}", .{suffix});
    defer a.free(path);
    var current = provider.memory.client();
    var replaced = try current.putObject("bucket", path, "corrupt live name", .{});
    replaced.deinit(a);
    var reads = reopened.artifactStore();
    const bytes = try reads.getVerifiedAllocWithCancellation(inventory.artifact_id, inventory.byte_len, inventory.checksum, .none);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("inventory", bytes);
    const before = provider.stats;
    try std.testing.expect(try reads.retainedClosure(inventory.artifact_id, .none));
    try std.testing.expectEqual(before, provider.stats);
    var tampered = publication;
    tampered.cloud_generations.?.certificate.?.binding[0] ^= 1;
    try std.testing.expectError(error.InvalidCloudRetentionCertificate, Binding.init(a, &reopened, tampered, .none));
    var replayed = publication;
    replayed.signature.store[0] ^= 1;
    try std.testing.expectError(error.InvalidCloudRetentionCertificate, Binding.init(a, &reopened, replayed, .none));
    provider.now_ms = provider.until_ms;
    try std.testing.expect(!try reads.retainedClosure(inventory.artifact_id, .none));
}

test "external lake global frontier cloud GC retains pinned generations and does not retain payloads through map records" {
    const a = std.testing.allocator;
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try Object.initWithClient(a, client, "bucket", "native");
    defer object.deinit();
    object.cloud_retention_qualified = true;
    const identity: [32]u8 = @splat(3);
    const namespace: [32]u8 = @splat(4);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(7, identity, namespace);
    const scope: stores.UploadScope = .{ .domain = domain, .attempt = @splat(2) };
    var writer: maps.Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope, .qualified_retention = true, .complete = true };
    defer writer.deinit();
    object.cloud_writer = &writer;
    defer object.cloud_writer = null;
    var artifacts = object.artifactStore();
    var first = try artifacts.putScoped(scope, "first inventory", .none);
    defer first.deinit(a);
    var publication: catalog.Publication = .{ .reader_protocol = catalog.native_reader_protocol, .namespace = namespace, .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = identity }, .published_at_ms = 101, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://source/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = first.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = first.artifact_id, .checksum = first.checksum, .byte_len = first.byte_len } };
    try finish(a, &writer, &object, &publication, &.{}, 7, identity, .{ .io = std.testing.io }, .none, false);
    defer maps.freeRef(a, publication.cloud_generations.?.root);
    defer a.free(publication.cloud_generations.?.certificate.?.roots);
    object.cloud_writer = null;
    var binding = (try Binding.init(a, &object, publication, .none)).?;
    defer binding.deinit();
    binding.reader.complete = false; // private build view; queries stay strict
    var next_scope = scope;
    next_scope.attempt[15] += 1;
    writer.scope = next_scope;
    object.cloud_writer = &writer;
    var second = try artifacts.putScoped(next_scope, "second inventory", .none);
    defer second.deinit(a);
    var next = publication;
    next.cloud_generations = null;
    next.generation = 2;
    next.token = @splat(2);
    next.base_source.external_parquet.file_inventory_artifact = second.artifact_id;
    next.inventory = .{ .kind = .external_base_source, .artifact_id = second.artifact_id, .checksum = second.checksum, .byte_len = second.byte_len };
    try finish(a, &writer, &object, &next, &.{}, 7, identity, .{ .io = std.testing.io }, .none, true);
    defer maps.freeRef(a, next.cloud_generations.?.root);
    defer a.free(next.cloud_generations.?.certificate.?.roots);
    object.cloud_writer = null;
    const Harness = struct {
        fn free(collector: *@import("lake_index_gc.zig").Collector) void {
            var keys = collector.marked.keyIterator();
            while (keys.next()) |key| collector.a.free(key.*);
            collector.marked.deinit(collector.a);
        }
    };
    var pinned: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 7, .authority = undefined, .store = artifacts, .identity = identity, .context = .{ .io = std.testing.io }, .token = @splat(1), .namespace = namespace, .expires_ms = std.math.maxInt(u64), .authority_deadline = std.math.maxInt(u64), .remaining_reads = 64 * 1024 * 1024 };
    defer Harness.free(&pinned);
    try pinned.markPublication(publication);
    try pinned.markPublication(next);
    try std.testing.expect(pinned.marked.contains(first.artifact_id));
    try std.testing.expect(pinned.marked.contains(second.artifact_id));
    var retired = pinned;
    retired.marked = .empty;
    defer Harness.free(&retired);
    try retired.markPublication(next);
    try std.testing.expect(!retired.marked.contains(first.artifact_id));
    try std.testing.expect(retired.marked.contains(next.cloud_generations.?.root.artifact_id));
    try std.testing.expectError(error.ArtifactRetentionActive, artifacts.delete(first.artifact_id));
    provider.now_ms = provider.until_ms;
    try artifacts.delete(first.artifact_id);
    const envelope: @import("lake_generation_envelope.zig").Envelope = .{ .id = first.artifact_id, .bytes = @intCast(first.byte_len), .generation = first.provider_version };
    try std.testing.expectError(error.FileNotFound, @import("lake_generation_envelope.zig").read(&artifacts, envelope, .none));
    var current_binding = (try Binding.init(a, &object, next, .none)).?;
    defer current_binding.deinit();
    const live = try artifacts.getVerifiedAllocWithCancellation(second.artifact_id, second.byte_len, second.checksum, .none);
    defer a.free(live);
    try std.testing.expectEqualStrings("second inventory", live);
}
