// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
    const encoded = if (publication.row_identity != null) try std.json.Stringify.valueAlloc(a, .{ .format = "native-lake-cloud-retention-v3", .reader_protocol = publication.reader_protocol, .namespace = publication.namespace, .generation = publication.generation, .token = publication.token, .signature = publication.signature, .store_locator = publication.store_locator, .base_source = publication.base_source, .inventory = publication.inventory, .source_navigation = publication.source_navigation, .row_identity = publication.row_identity, .directory = publication.directory, .map = publication.cloud_generations.?.root, .complete = publication.cloud_generations.?.complete, .until_ms = certificate.until_ms, .roots = certificate.roots }, .{}) else if (publication.source_navigation == null) try std.json.Stringify.valueAlloc(a, .{ .format = "native-lake-cloud-retention-v1", .reader_protocol = publication.reader_protocol, .namespace = publication.namespace, .generation = publication.generation, .token = publication.token, .signature = publication.signature, .store_locator = publication.store_locator, .base_source = publication.base_source, .inventory = publication.inventory, .directory = publication.directory, .map = publication.cloud_generations.?.root, .complete = publication.cloud_generations.?.complete, .until_ms = certificate.until_ms, .roots = certificate.roots }, .{}) else try std.json.Stringify.valueAlloc(a, .{ .format = "native-lake-cloud-retention-v2", .reader_protocol = publication.reader_protocol, .namespace = publication.namespace, .generation = publication.generation, .token = publication.token, .signature = publication.signature, .store_locator = publication.store_locator, .base_source = publication.base_source, .inventory = publication.inventory, .source_navigation = publication.source_navigation, .directory = publication.directory, .map = publication.cloud_generations.?.root, .complete = publication.cloud_generations.?.complete, .until_ms = certificate.until_ms, .roots = certificate.roots }, .{});
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
        reader.* = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .io = object.cloud_io, .root = root, .complete = cloud.complete, .shared = maps.sharedNavigation(object.cloud_cache_scope) };
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
/// A valid legacy checkpoint can contribute artifacts outside the published
/// map. Its closure must be bootstrapped even when the publication is complete.
pub fn seedComplete(seed: ?catalog.Publication, checkpoint: ?catalog.Publication, signature: catalog.Signature, namespace: ?catalog.Digest, fresh: bool) bool {
    if (checkpoint) |saved| {
        if (std.meta.eql(saved.signature, signature) and std.meta.eql(saved.namespace, namespace) and saved.cloud_generations == null) return false;
    }
    return if (seed) |publication| if (publication.cloud_generations) |cloud| cloud.complete else false else fresh;
}

/// Full migration audit is bounded by the existing closure census limits. If
/// its optional certificate budget is exhausted, keep version-qualified records
/// already observed and retain ordinary required-read/audit semantics.
pub fn finish(a: A, writer: *maps.Writer, object: *Object, publication: *catalog.Publication, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, table_id: u64, identity: [32]u8, context: local.serverless_query_lake_read_context.Context, token: Token, inherited_certificate: bool) !void {
    const fresh_provider_now = if (inherited_certificate) try object.client.providerTime(object.bucket, @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)) else null;
    const inherit = fresh_provider_now != null and fresh_provider_now.? +| 60_000 < object.cloud_certificate_until_ms;
    if ((!writer.complete or (writer.qualified_retention and !inherit)) and !writer.disabled) {
        // Completeness of exact immutable selectors is independent of optional
        // retention evidence. Inherit a complete map without a closure census
        // when certificates are disabled. Bootstrap and proof renewal alone
        // rebase over the live closure; ordinary reuse still checks existence.
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
    var vacuum_progress = writer.vacuum_progress;
    // Completed GC evidence proves absence without per-record provider HEADs.
    if (object.cloud_vacuum) |cut| {
        var vacuum_object = try Object.initWithClientOptions(a, object.client, object.bucket, object.prefix, .{ .read_only = true });
        defer vacuum_object.deinit();
        var vacuum_store = vacuum_object.artifactStore();
        if (try writer.finish(token)) |flushed| maps.freeRef(writer.a, flushed);
        vacuum_progress = writer.vacuum(&vacuum_store, cut, vacuum_progress, token) catch blk: {
            try token.check();
            try context.ensureActive();
            break :blk vacuum_progress;
        };
        writer.vacuum_progress = vacuum_progress;
    }
    const root = (try writer.finish(token)) orelse return;
    defer maps.freeRef(writer.a, root);
    publication.cloud_generations = .{ .root = try maps.cloneRef(a, root), .complete = writer.complete and !writer.disabled, .vacuum = vacuum_progress };
    if (!publication.cloud_generations.?.complete or !writer.qualified_retention or root.retention_until_ms == 0) return;
    const provider_now = (try object.client.providerTime(object.bucket, @import("objectstore").CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn))) orelse return;
    if (provider_now +| 60_000 >= root.retention_until_ms) return;
    var roots: std.ArrayList([32]u8) = .empty;
    defer roots.deinit(a);
    try roots.append(a, digestRoot(publication.inventory.artifact_id));
    if (publication.source_navigation) |navigation| try roots.append(a, digestRoot(navigation.artifact_id));
    if (publication.row_identity) |row_identity| try roots.append(a, digestRoot(row_identity.artifact_id));
    if (publication.directory) |directory| try roots.append(a, digestRoot(directory.artifact_id));
    for (declarations) |declaration| try roots.append(a, digestRoot(declaration.artifact.artifact_id));
    // Keep optional inline proof evidence comfortably inside the catalog and
    // lifecycle envelopes. Larger root sets retain complete maps and auditing.
    if (roots.items.len > 1024) return;
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
        // Complete fallible allocation before acquiring any shared cache pins.
        const union_reader = try a.create(maps.Reader);
        errdefer a.destroy(union_reader);
        for (readers, pointers, selected.items, 0..) |*reader, *pointer, cloud, i| {
            reader.* = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .io = object.cloud_io, .root = cloud.root, .shared = maps.sharedNavigation(object.cloud_cache_scope), .cache_limit = if (i < 4) 2 * 1024 * 1024 else 0 };
            pointer.* = reader;
        }
        union_reader.* = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .io = object.cloud_io, .alternatives = pointers };
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
    // Bind a multi-level packed navigation forest into the same durable
    // publication proof; a fresh reader must resolve the retained pack version.
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    const PageStore = @import("../serverless/graph_segment/page_store.zig").PageStore;
    var page_reads: u64 = 16 * 1024 * 1024;
    var page_writes: u64 = 16 * 1024 * 1024;
    var pages: PageStore = .{ .domain = domain, .attempt = scope.attempt, .artifacts = &artifacts, .remaining_read_bytes = &page_reads, .remaining_write_bytes = &page_writes, .pack_pages = true };
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store(), .target_bytes = 512 };
    defer builder.deinit();
    for (0..32) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try builder.add(&key, &@as([64]u8, @splat(17)));
    }
    const page_root = (try builder.finish()).?;
    var leaf_ref = page_root;
    while (leaf_ref.height > 0) {
        var page = try tree.readPage(a, pages.store(), leaf_ref);
        defer page.deinit(a);
        leaf_ref = page.entries[0].child.?;
    }
    try std.testing.expect(leaf_ref.pack != null);
    const navigation: @import("lake_source_navigation.zig").Root = .{ .domain = domain, .source = "source", .schema = "schema", .snapshot = "snapshot", .count = 32, .files = page_root, .public = page_root, .private = page_root };
    const nav_bytes = try std.json.Stringify.valueAlloc(a, navigation, .{});
    defer a.free(nav_bytes);
    var nav = try artifacts.putScoped(scope, nav_bytes, .none);
    defer nav.deinit(a);
    var publication: catalog.Publication = .{ .reader_protocol = catalog.native_reader_protocol, .namespace = namespace, .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = identity }, .published_at_ms = 101, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://source/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len } };
    publication.source_navigation = .{ .kind = .doc_values, .metadata_version = 2, .artifact_id = nav.artifact_id, .checksum = nav.checksum, .byte_len = nav.byte_len };
    try finish(a, &writer, &object, &publication, &.{}, table_id, identity, .{ .io = std.testing.io }, .none, false);
    const cloud = publication.cloud_generations.?;
    {
        const Failure = struct {
            fn run(alloc: A, object_: *Object, published: catalog.Publication) !void {
                const owner = maps.sharedNavigation(object_.cloud_cache_scope).?;
                defer owner.release();
                const refs = owner.refs.load(.acquire);
                var binding_ = UnionBinding.init(alloc, object_, &.{published}, &.{}, published.namespace, published.signature.store) catch |err| {
                    try std.testing.expectEqual(refs, owner.refs.load(.acquire));
                    return err;
                };
                binding_.deinit();
                try std.testing.expectEqual(refs, owner.refs.load(.acquire));
            }
        };
        object.cloud_cache_scope = @splat(91);
        defer object.cloud_cache_scope = @splat(0);
        try std.testing.checkAllAllocationFailures(a, Failure.run, .{ &object, publication });
    }
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
    const pack_id = try PageStore.identity(domain, leaf_ref);
    const pack_suffix = try stores.storageSuffixAlloc(a, &pack_id);
    defer a.free(pack_suffix);
    const pack_path = try std.fmt.allocPrint(a, "native/{s}", .{pack_suffix});
    defer a.free(pack_path);
    var changed_pack = try current.putObject("bucket", pack_path, "corrupt live pack name", .{});
    changed_pack.deinit(a);
    pages.artifacts = &reads;
    var retained_page = try tree.readPage(a, pages.store(), leaf_ref);
    defer retained_page.deinit(a);
    try std.testing.expectEqual(@as(u8, 17), retained_page.entries[0].value[0]);
    const before = provider.stats;
    try std.testing.expect(try reads.retainedClosure(inventory.artifact_id, .none));
    try std.testing.expectEqual(before, provider.stats);
    var remapped = publication;
    remapped.row_identity = .{ .kind = .doc_values, .metadata_version = 1, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len };
    const before_identity = try certificateBinding(a, publication, cloud.certificate.?);
    const after_identity = try certificateBinding(a, remapped, cloud.certificate.?);
    try std.testing.expect(!std.mem.eql(u8, &before_identity, &after_identity));
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

test "external lake global frontier wrapped publication builds real Parquet packs durable checkpoints and cloud certificates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("cloud-publication-adapter");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var source_client = fs.client();
    try source_client.makeBucket("antfly");
    const parquet = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "first quasar", "second quasar" } }});
    defer a.free(parquet);
    var put = try source_client.putObject("antfly", "part.parquet", parquet, .{});
    put.deinit(a);
    var second_put = try source_client.putObject("antfly", "part-2.parquet", parquet, .{});
    second_put.deinit(a);
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema);
    var base = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer base.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = base }, .{});
    defer source.deinit();
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try Object.initWithClient(a, client, "bucket", "native");
    defer object.deinit();
    object.cloud_retention_qualified = true;
    var reads = @import("lake_index_artifact_recovery.zig").Reads.init(object.artifactStore());
    var store = reads.store();
    try std.testing.expectEqual(&object, store.providerStore().?);
    const publication_api = @import("lake_index_publication.zig");
    const identity: [32]u8 = @splat(4);
    var table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true}}" };
    const pending = try publication_api.begin(a, std.testing.io, table, &source, identity, .{}, 100, 1000);
    defer a.free(pending);
    table.lake_index_catalog_json = pending;
    const Harness = struct {
        table: local.common_topology_records.TableRecord,
        checkpoints: usize = 0,
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
        fn snapshot(raw: *anyopaque, alloc: A) !local.common_topology_records.TableRecord {
            var result = @as(*@This(), @ptrCast(@alignCast(raw))).table;
            result.lake_index_catalog_json = try alloc.dupe(u8, result.lake_index_catalog_json);
            return result;
        }
        fn save(raw: *anyopaque, saved: catalog.Publication) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try saved.validate();
            const cloud = saved.cloud_generations orelse return error.MissingCheckpointGenerationMap;
            try std.testing.expect(cloud.complete and cloud.certificate == null);
            self.checkpoints += 1;
        }
    };
    var harness: Harness = .{ .table = table };
    const encoded = try publication_api.buildWithLease(a, &store, table, &source, identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now }, .{ .ptr = &harness, .snapshot = Harness.snapshot, .save_checkpoint = Harness.save });
    defer a.free(encoded);
    var published = try catalog.parse(a, encoded);
    defer published.deinit();
    const selected = published.value.published.?;
    try std.testing.expect(harness.checkpoints != 0);
    try std.testing.expect(selected.cloud_generations.?.complete);
    try std.testing.expect(selected.cloud_generations.?.certificate != null);
    var reopened = try Object.initWithClient(a, client, "bucket", "native");
    defer reopened.deinit();
    reopened.cloud_retention_qualified = true;
    var binding = (try Binding.init(a, &reopened, selected, .none)).?;
    defer binding.deinit();
    var artifacts = reopened.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var owned = selected;
    try @import("lake_index_directory.zig").hydrate(ca, artifacts, &owned, .none, null);
    const corpus = @import("lake_index_native_text.zig");
    const declaration = for (owned.declarations) |entry| {
        if (entry.artifact.kind == .text_segment) break entry;
    } else return error.MissingTextPublication;
    const root = try corpus.loadServing(ca, artifacts, declaration.artifact, .none, null);
    try std.testing.expectEqual(@as(u32, 1), root.segment_count);
    try std.testing.expect(root.file_groups[0].sources.len != 0);
    var writer = try corpus.loadWriter(a, artifacts, root, .none, null, null);
    defer writer.deinit();
    const hits = try writer.snapshot().search(a, "body", &.{"quasar"}, 10);
    defer a.free(hits.hits);
    try std.testing.expectEqual(@as(u32, 4), hits.total_count);
    try std.testing.expect(try artifacts.retainedClosure(declaration.artifact.artifact_id, .none));
    // Derived map loss after protection expires must rebuild from Parquet,
    // without re-installing the unavailable map in the fresh recovery attempt.
    provider.now_ms = provider.until_ms;
    var raw_artifacts = object.artifactStore();
    // Interrupt an actual Parquet append after its durable text checkpoint,
    // with original canonical postings already reclaimed.
    const maintenance = try corpus.loadRootDirectory(ca, raw_artifacts, declaration.artifact, .none, null);
    const maintenance_files = try corpus.text_directory.fileRecords(ca, maintenance, raw_artifacts, null, .none);
    for (maintenance_files) |record| {
        const group = try corpus.loadFileManifest(ca, raw_artifacts, record.manifest, maintenance.domain, .none, null);
        for (group.segments) |segment| try raw_artifacts.delete(segment.artifact_id);
    }
    var appended_put = try source_client.putObject("antfly", "part-3.parquet", parquet, .{});
    appended_put.deinit(a);
    var append_source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = base }, .{});
    defer append_source.deinit();
    var append_table = table;
    append_table.lake_index_catalog_json = encoded;
    const append_pending = try publication_api.begin(a, std.testing.io, append_table, &append_source, identity, .{}, 101, 1000);
    defer a.free(append_pending);
    append_table.lake_index_catalog_json = append_pending;
    const Resume = struct {
        a: A,
        table: local.common_topology_records.TableRecord,
        store: *stores.ArtifactStore,
        stop: bool = true,
        saw_base: bool = false,
        fn snapshot(raw: *anyopaque, alloc: A) !local.common_topology_records.TableRecord {
            var record = @as(*@This(), @ptrCast(@alignCast(raw))).table;
            record.lake_index_catalog_json = try alloc.dupe(u8, record.lake_index_catalog_json);
            return record;
        }
        fn save(raw: *anyopaque, saved: catalog.Publication) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var current = try catalog.parse(self.a, self.table.lake_index_catalog_json);
            defer current.deinit();
            self.table.lake_index_catalog_json = try catalog.encode(self.a, try current.value.saveCheckpoint(saved, saved.published_at_ms));
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            var hydrated = saved;
            try @import("lake_index_directory.zig").hydrate(scratch.allocator(), self.store.*, &hydrated, .none, null);
            for (hydrated.declarations) |entry| if (entry.artifact.kind == .text_segment) {
                const partial = try corpus.loadRootDirectory(scratch.allocator(), self.store.*, entry.artifact, .none, null);
                if (partial.checkpoint_base != null and (partial.manifests.len != 0 or partial.checkpoint_files != null)) {
                    self.saw_base = true;
                    if (self.stop) return error.TestInterrupted;
                }
            };
        }
    };
    var resumer: Resume = .{ .a = ca, .table = append_table, .store = &store };
    const lease: publication_api.Lease = .{ .ptr = &resumer, .snapshot = Resume.snapshot, .save_checkpoint = Resume.save };
    try std.testing.expectError(error.TestInterrupted, publication_api.buildWithLease(a, &store, append_table, &append_source, identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now }, lease));
    try std.testing.expect(resumer.saw_base);
    resumer.stop = false;
    const resumed = try publication_api.buildWithLease(a, &store, resumer.table, &append_source, identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now }, lease);
    defer a.free(resumed);
    var resumed_catalog = try catalog.parse(a, resumed);
    defer resumed_catalog.deinit();
    var resumed_publication = resumed_catalog.value.published.?;
    {
        var resume_binding = (try Binding.init(a, &object, resumed_publication, .none)).?;
        defer resume_binding.deinit();
        try @import("lake_index_directory.zig").hydrate(ca, store, &resumed_publication, .none, null);
        const resumed_declaration = for (resumed_publication.declarations) |entry| {
            if (std.mem.eql(u8, entry.name, declaration.name)) break entry;
        } else return error.MissingTextPublication;
        const resumed_root = try corpus.loadServing(ca, store, resumed_declaration.artifact, .none, null);
        var resumed_writer = try corpus.loadWriter(a, store, resumed_root, .none, null, null);
        defer resumed_writer.deinit();
        const resumed_hits = try resumed_writer.snapshot().search(a, "body", &.{"quasar"}, 10);
        defer a.free(resumed_hits.hits);
        try std.testing.expectEqual(@as(u32, 6), resumed_hits.total_count);
    }
    try raw_artifacts.delete(selected.cloud_generations.?.root.artifact_id);
    table.lake_index_catalog_json = encoded;
    const rebuilding = try publication_api.begin(a, std.testing.io, table, &source, identity, .{}, 101, 1000);
    defer a.free(rebuilding);
    table.lake_index_catalog_json = rebuilding;
    const recovered = try publication_api.build(a, &store, table, &source, identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now });
    defer a.free(recovered);
    var restored = try catalog.parse(a, recovered);
    defer restored.deinit();
    try std.testing.expectEqual(@as(u64, 2), restored.value.published.?.generation);
    try std.testing.expect(restored.value.published.?.cloud_generations.?.complete);
    // A credential/backend identity rotation starts a fresh map domain, even
    // when the same physical bucket remains reachable through the new identity.
    const rotated_identity: [32]u8 = @splat(7);
    table.lake_index_catalog_json = recovered;
    const rotated_pending = try publication_api.begin(a, std.testing.io, table, &source, rotated_identity, .{}, 101, 1000);
    defer a.free(rotated_pending);
    table.lake_index_catalog_json = rotated_pending;
    const rotated_encoded = try publication_api.build(a, &store, table, &source, rotated_identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now });
    defer a.free(rotated_encoded);
    var rotated = try catalog.parse(a, rotated_encoded);
    defer rotated.deinit();
    const rotated_root = rotated.value.published.?.cloud_generations.?.root;
    const rotated_scope = (try stores.uploadScopeFromArtifactId(rotated_root.artifact_id)).?;
    try std.testing.expectEqual(publication_api.uploadDomainWithNamespace(4, rotated_identity, rotated.value.namespace), rotated_scope.domain);
    try std.testing.expect(rotated.value.published.?.cloud_generations.?.complete);
    // A removal already folded into durable DF deltas must not be subtracted
    // again when the same partial refresh resumes.
    try source_client.deleteObject("antfly", "part-2.parquet", .{});
    var fourth_put = try source_client.putObject("antfly", "part-4.parquet", parquet, .{});
    fourth_put.deinit(a);
    var removal_source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = base }, .{});
    defer removal_source.deinit();
    var removal_table = append_table;
    removal_table.lake_index_catalog_json = resumed;
    const removal_pending = try publication_api.begin(a, std.testing.io, removal_table, &removal_source, identity, .{}, 101, 1000);
    defer a.free(removal_pending);
    removal_table.lake_index_catalog_json = removal_pending;
    resumer.table = removal_table;
    resumer.stop = true;
    resumer.saw_base = false;
    try std.testing.expectError(error.TestInterrupted, publication_api.buildWithLease(a, &store, removal_table, &removal_source, identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now }, lease));
    resumer.stop = false;
    const removal_resumed = try publication_api.buildWithLease(a, &store, resumer.table, &removal_source, identity, .{ .io = std.testing.io }, .none, .{ .ptr = &harness, .now_ms = Harness.now }, lease);
    defer a.free(removal_resumed);
    var removal_catalog = try catalog.parse(a, removal_resumed);
    defer removal_catalog.deinit();
    var removal_publication = removal_catalog.value.published.?;
    var removal_binding = (try Binding.init(a, &object, removal_publication, .none)).?;
    defer removal_binding.deinit();
    try @import("lake_index_directory.zig").hydrate(ca, store, &removal_publication, .none, null);
    const removal_declaration = for (removal_publication.declarations) |entry| {
        if (std.mem.eql(u8, entry.name, declaration.name)) break entry;
    } else return error.MissingTextPublication;
    const removal_root = try corpus.loadServing(ca, store, removal_declaration.artifact, .none, null);
    try std.testing.expectEqual(@as(u32, 6), removal_root.collection.?.documents);
    var removal_writer = try corpus.loadWriter(a, store, removal_root, .none, null, null);
    defer removal_writer.deinit();
    var frequencies: [1]u32 = undefined;
    try removal_writer.snapshot().termDocFreqs(a, "body", &.{"quasar"}, &frequencies);
    try std.testing.expectEqual(@as(u32, 6), frequencies[0]);
}

test "external lake global frontier complete cloud maps refresh without retention certificates or payload audits" {
    const a = std.testing.allocator;
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try Object.initWithClient(a, client, "bucket", "native");
    defer object.deinit();
    const identity: [32]u8 = @splat(3);
    const namespace: [32]u8 = @splat(4);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(7, identity, namespace);
    const scope: stores.UploadScope = .{ .domain = domain, .attempt = @splat(2) };
    var writer: maps.Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope, .complete = true };
    defer writer.deinit();
    object.cloud_writer = &writer;
    defer object.cloud_writer = null;
    var artifacts = object.artifactStore();
    var inventory = try artifacts.putScoped(scope, "first inventory", .none);
    defer inventory.deinit(a);
    var publication: catalog.Publication = .{ .reader_protocol = catalog.native_reader_protocol, .namespace = namespace, .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = identity }, .published_at_ms = 101, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://source/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len } };
    try finish(a, &writer, &object, &publication, &.{}, 7, identity, .{ .io = std.testing.io }, .none, false);
    const first = publication.cloud_generations.?;
    defer maps.freeRef(a, first.root);
    try std.testing.expect(first.complete and first.certificate == null);
    try std.testing.expectEqual(@as(usize, 0), provider.stats);
    var legacy_checkpoint = publication;
    legacy_checkpoint.cloud_generations = null;
    try std.testing.expect(!seedComplete(publication, legacy_checkpoint, publication.signature, namespace, false));
    try std.testing.expect(seedComplete(publication, publication, publication.signature, namespace, false));
    legacy_checkpoint.signature.source[0] ^= 1;
    try std.testing.expect(seedComplete(publication, legacy_checkpoint, publication.signature, namespace, false));
    // Reopen the persistent map into a fresh builder, including a new attempt.
    writer.rebase();
    writer.reader.root = try maps.cloneRef(a, first.root);
    writer.scope.attempt[15] += 1;
    var next = try artifacts.putScoped(writer.scope, "next inventory", .none);
    defer next.deinit(a);
    publication.generation += 1;
    publication.cloud_generations = null;
    publication.inventory = .{ .kind = .external_base_source, .artifact_id = next.artifact_id, .checksum = next.checksum, .byte_len = next.byte_len };
    publication.base_source.external_parquet.file_inventory_artifact = next.artifact_id;
    const before = provider.serial;
    try finish(a, &writer, &object, &publication, &.{}, 7, identity, .{ .io = std.testing.io }, .none, false);
    defer maps.freeRef(a, publication.cloud_generations.?.root);
    try std.testing.expect(publication.cloud_generations.?.complete);
    try std.testing.expect(publication.cloud_generations.?.certificate == null);
    try std.testing.expect(provider.serial - before <= 2);
    try std.testing.expectEqual(@as(usize, 0), provider.stats);
    var retained = (try writer.lookup(a, inventory.artifact_id, .none)).?;
    defer retained.deinit(a);
    try std.testing.expectEqualStrings(inventory.provider_version.?, retained.generation);
    // Optional bootstrap auditing cannot exceed its payload budget before I/O.
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 7, .authority = undefined, .store = artifacts, .identity = identity, .context = .{ .io = std.testing.io }, .token = @splat(1), .namespace = namespace, .audit_only = true, .remaining_reads = 0 };
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, collector.mark(.{ .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len }));
    try std.testing.expectEqual(@as(usize, 0), provider.stats);
}

test "external lake global frontier completed cloud GC publishes durable vacuum evidence" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try Object.initWithClient(a, client, "bucket", "native");
    defer object.deinit();
    var artifacts = object.artifactStore();
    const identity: [32]u8 = @splat(5);
    const namespace: [32]u8 = @splat(8);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(7, identity, namespace);
    const scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    var writer: maps.Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope, .complete = true };
    defer writer.deinit();
    object.cloud_writer = &writer;
    var inventory = try artifacts.putScoped(scope, "live inventory", .none);
    defer inventory.deinit(a);
    var orphan = try artifacts.putScoped(scope, "obsolete publication payload", .none);
    defer orphan.deinit(a);
    var publication: catalog.Publication = .{ .reader_protocol = catalog.native_reader_protocol, .store_locator = .{ .protocol = .s3, .connection = "test", .bucket = "bucket", .prefix = "native", .root = "s3://bucket/native" }, .namespace = namespace, .generation = 1, .token = scope.attempt, .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = identity }, .published_at_ms = 101, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://source/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len } };
    try finish(ca, &writer, &object, &publication, &.{}, 7, identity, .{ .io = std.testing.io }, .none, false);
    object.cloud_writer = null;
    const Harness = struct {
        a: A,
        state: @import("../metadata/lake_index_lifecycle.zig").State,
        fn read(raw: *anyopaque, alloc: A, _: u64, _: local.api_operation.RequestContext) ![]u8 {
            return @import("../metadata/lake_index_lifecycle.zig").encode(alloc, @as(*@This(), @ptrCast(@alignCast(raw))).state);
        }
        fn mutate(raw: *anyopaque, _: u64, revision: u64, change: @import("../metadata/lake_index_lifecycle.zig").Mutation, _: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (revision != self.state.revision) return error.CatalogGenerationChanged;
            self.state = try change.apply(self.a, self.state);
        }
    };
    var harness: Harness = .{ .a = ca, .state = try (@import("../metadata/lake_index_lifecycle.zig").State{}).synchronize(ca, .{ .namespace = namespace, .generation = 1, .published = publication }) };
    const authority: @import("lake_index_reader_lease.zig").Authority = .{ .ptr = &harness, .context = .{}, .read = Harness.read, .mutate = Harness.mutate };
    provider.now_ms = provider.until_ms;
    var completed = false;
    for (0..128) |_| {
        var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 7, .authority = authority, .store = artifacts, .identity = identity, .context = .{ .io = std.testing.io }, .options = .{ .dry_run = false, .max_marked = 2, .max_deleted = 2 }, .token = @splat(7) };
        if ((try collector.run()).complete) {
            completed = true;
            break;
        }
    }
    try std.testing.expect(completed);
    const lifecycle_bytes = try @import("../metadata/lake_index_lifecycle.zig").encode(ca, harness.state);
    const restarted = try @import("../metadata/lake_index_lifecycle.zig").parse(ca, lifecycle_bytes);
    const cut = restarted.stores[0].vacuum orelse return error.MissingVacuumEvidence;
    var removed = try writer.lookup(a, orphan.artifact_id, .none);
    defer if (removed) |*record| record.deinit(a);
    try std.testing.expect(removed != null);
    const progress = try writer.vacuum(&artifacts, cut, .{}, .none);
    try std.testing.expect(progress.done);
    try std.testing.expect((try writer.lookup(a, orphan.artifact_id, .none)) == null);
    var live = (try writer.lookup(a, inventory.artifact_id, .none)).?;
    live.deinit(a);
    // A later collection roots the persisted mark pages, without treating the
    // IDs stored in that evidence as payload roots.
    harness.state = restarted;
    completed = false;
    for (0..128) |_| {
        var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 7, .authority = authority, .store = artifacts, .identity = identity, .context = .{ .io = std.testing.io }, .options = .{ .dry_run = false, .max_marked = 2, .max_deleted = 2 }, .token = @splat(6) };
        if ((try collector.run()).complete) {
            completed = true;
            break;
        }
    }
    try std.testing.expect(completed);
}
