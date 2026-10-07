// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Native metadata authority for immutable external-lake index generations.
//! Builder leases and published coverage are pinned with the table definition.
const std = @import("std");
const A = std.mem.Allocator;
const manifest = @import("../serverless/segment/sidecar_manifest.zig");
const base = @import("../serverless/manifest/base_source.zig");
const artifacts = @import("../serverless/manifest/artifact_ref.zig");
pub const max_json_bytes: usize = 256 * 1024;
pub const max_artifacts: usize = 256;
pub const Digest = [32]u8;
pub const Token = [16]u8;
pub const DirectoryRef = struct { artifact_id: []const u8, checksum: []const u8, byte_len: u64, count: u32 };
pub const max_directory_artifacts: usize = 4096;
pub const max_directory_bytes: usize = 16 * 1024 * 1024;
/// Physical namespace plus a named connection for current credential lookup.
/// Credentials are never copied into publication metadata.
pub const StoreLocator = struct {
    version: u16 = 1,
    protocol: enum { filesystem, s3, gcs },
    connection: ?[]const u8 = null,
    bucket: []const u8,
    prefix: []const u8,
    root: []const u8,
    tls: bool = true,
    pub fn validate(self: StoreLocator) !void {
        if (self.version != 1 or self.bucket.len == 0 or self.root.len == 0 or self.bucket.len > 1024 or self.prefix.len > 8192 or self.root.len > 8192) return error.InvalidLakeIndexCatalog;
        if (self.connection) |connection| if (connection.len == 0 or connection.len > 1024) return error.InvalidLakeIndexCatalog;
        if ((self.protocol == .filesystem) != (self.connection == null)) return error.InvalidLakeIndexCatalog;
        if (self.protocol != .filesystem and std.mem.indexOfAny(u8, self.root, "@?#") != null) return error.InvalidLakeIndexCatalog;
    }
};
pub const Signature = struct {
    desired: Digest,
    source: Digest,
    credentials: Digest,
    store: Digest,
    pub fn validate(self: Signature) !void {
        inline for (@typeInfo(Signature).@"struct".field_names) |field_name| if (std.mem.allEqual(u8, &@field(self, field_name), 0)) return error.InvalidLakeIndexCatalog;
    }
};
pub const Attempt = struct {
    reader_protocol: u16 = 0,
    store_locator: ?StoreLocator = null,
    generation: u64,
    token: Token,
    signature: Signature,
    started_at_ms: u64,
    lease_expires_at_ms: u64,
    fn validate(self: Attempt) !void {
        if (self.reader_protocol != 0 and self.reader_protocol != 24) return error.InvalidLakeIndexCatalog;
        if (self.store_locator) |locator| try locator.validate();
        try self.signature.validate();
        if (self.generation == 0 or std.mem.allEqual(u8, &self.token, 0) or self.lease_expires_at_ms <= self.started_at_ms) return error.InvalidLakeIndexCatalog;
    }
};
pub const FileContribution = struct {
    file: Digest,
    recipe: Digest,
    name: []const u8,
    artifact: artifacts.ArtifactRef,
};
pub const Publication = struct {
    /// Zero denotes legacy consumers without renewable reader authority.
    /// Older strict decoders reject this field and fall back to source scans.
    reader_protocol: u16 = 0,
    store_locator: ?StoreLocator = null,
    namespace: ?Digest = null,
    generation: u64,
    token: Token,
    signature: Signature,
    published_at_ms: u64,
    base_source: base.BaseSourceDescriptor,
    inventory: artifacts.ArtifactRef,
    declarations: []const manifest.DeclaredArtifact = &.{},
    directory: ?DirectoryRef = null,
    file_contributions: []const FileContribution = &.{},
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        if (self.directory) |directory| {
            try writer.write(.{ .reader_protocol = self.reader_protocol, .store_locator = self.store_locator, .namespace = self.namespace, .generation = self.generation, .token = self.token, .signature = self.signature, .published_at_ms = self.published_at_ms, .base_source = self.base_source, .inventory = self.inventory, .directory = directory });
        } else {
            try writer.write(.{ .reader_protocol = self.reader_protocol, .store_locator = self.store_locator, .namespace = self.namespace, .generation = self.generation, .token = self.token, .signature = self.signature, .published_at_ms = self.published_at_ms, .base_source = self.base_source, .inventory = self.inventory, .declarations = self.declarations });
        }
    }
    pub fn validate(self: Publication) !void {
        if (self.reader_protocol != 0 and self.reader_protocol != 24) return error.InvalidLakeIndexCatalog;
        if (self.store_locator) |locator| try locator.validate();
        if (self.namespace) |namespace| if (std.mem.allEqual(u8, &namespace, 0)) return error.InvalidLakeIndexCatalog;
        if (self.generation == 0 or std.mem.allEqual(u8, &self.token, 0) or self.declarations.len > (if (self.directory != null) max_directory_artifacts else max_artifacts)) return error.InvalidLakeIndexCatalog;
        if (self.directory) |directory| {
            if (directory.count > max_directory_artifacts or directory.byte_len > max_directory_bytes or (self.declarations.len != 0 and self.declarations.len != directory.count)) return error.InvalidLakeIndexCatalog;
            try artifactValid(.{ .kind = .doc_values, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len });
        }
        if (self.file_contributions.len > 16384) return error.InvalidLakeIndexCatalog;
        for (self.file_contributions) |contribution| {
            if (contribution.name.len == 0 or contribution.name.len > 128 or std.mem.allEqual(u8, &contribution.file, 0) or std.mem.allEqual(u8, &contribution.recipe, 0) or contribution.artifact.kind != .algebraic_segment) return error.InvalidLakeIndexCatalog;
            try artifactValid(contribution.artifact);
        }
        try self.signature.validate();
        try self.base_source.validate();
        switch (self.base_source) {
            .external_parquet, .external_iceberg => {},
            else => return error.InvalidLakeIndexCatalog,
        }
        try artifactValid(self.inventory);
        if (self.inventory.kind != .external_base_source) return error.InvalidLakeIndexCatalog;
        const source = switch (self.base_source) {
            .external_parquet, .external_iceberg => |v| v,
            else => unreachable,
        };
        if (source.file_inventory_artifact == null or !std.mem.eql(u8, source.file_inventory_artifact.?, self.inventory.artifact_id)) return error.InvalidLakeIndexCatalog;
        try manifest.validateManifestAgainstBaseSource(.{ .artifacts = self.declarations }, self.base_source);
        for (self.declarations) |declaration| {
            if (declaration.name.len > 128) return error.InvalidLakeIndexCatalog;
            try artifactValid(declaration.artifact);
        }
    }
};
pub const Failure = struct {
    generation: u64,
    desired: Digest,
    reason: []const u8,
    retry_at_ms: u64,
};
pub const State = struct {
    version: u16 = 1,
    namespace: ?Digest = null,
    generation: u64 = 0,
    pending: ?Attempt = null,
    published: ?Publication = null,
    failure: ?Failure = null,
    pub fn validate(self: State) !void {
        if (self.version != 1) return error.InvalidLakeIndexCatalog;
        if (self.namespace) |namespace| if (std.mem.allEqual(u8, &namespace, 0)) return error.InvalidLakeIndexCatalog;
        if (self.pending) |attempt| {
            try attempt.validate();
            if (attempt.generation != self.generation or self.failure != null) return error.InvalidLakeIndexCatalog;
        }
        if (self.published) |publication| {
            try publication.validate();
            if (publication.namespace != null and !std.meta.eql(publication.namespace, self.namespace)) return error.InvalidLakeIndexCatalog;
            if (publication.generation > self.generation or (self.pending != null and publication.generation >= self.pending.?.generation)) return error.InvalidLakeIndexCatalog;
        }
        if (self.failure) |failure| {
            if (failure.generation != self.generation or failure.reason.len == 0 or failure.reason.len > 512 or std.mem.allEqual(u8, &failure.desired, 0)) return error.InvalidLakeIndexCatalog;
            if (self.published) |publication| if (publication.generation >= failure.generation) return error.InvalidLakeIndexCatalog;
        }
    }
    /// The returned state borrows an earlier publication. Serialize it before
    /// releasing the parsed old catalog or any builder-owned declaration.
    pub fn begin(self: State, signature: Signature, token: Token, now_ms: u64, lease_ms: u64) !State {
        try self.validate();
        try signature.validate();
        if (self.pending) |attempt| if (attempt.lease_expires_at_ms > now_ms and std.meta.eql(attempt.signature, signature)) return error.LakeIndexBuildInProgress;
        var next = self;
        next.generation = std.math.add(u64, self.generation, 1) catch return error.LakeIndexGenerationExhausted;
        next.pending = .{ .reader_protocol = 24, .generation = next.generation, .token = token, .signature = signature, .started_at_ms = now_ms, .lease_expires_at_ms = std.math.add(u64, now_ms, lease_ms) catch return error.InvalidLakeIndexCatalog };
        next.failure = null;
        try next.validate();
        return next;
    }
    pub fn publish(self: State, publication: Publication, now_ms: u64) !State {
        try self.validate();
        if (!std.meta.eql(self.namespace, publication.namespace)) return error.LakeIndexPublicationFenceChanged;
        const attempt = self.pending orelse return error.LakeIndexPublicationFenceChanged;
        if (attempt.reader_protocol != publication.reader_protocol or !try locatorEqual(std.heap.page_allocator, attempt.store_locator, publication.store_locator)) return error.LakeIndexPublicationFenceChanged;
        if (!std.mem.eql(u8, &attempt.token, &publication.token) or attempt.generation != publication.generation or !std.meta.eql(attempt.signature, publication.signature) or now_ms < attempt.started_at_ms or now_ms >= attempt.lease_expires_at_ms or publication.published_at_ms != now_ms) return error.LakeIndexPublicationFenceChanged;
        var next = self;
        next.pending = null;
        next.published = publication;
        next.failure = null;
        try next.validate();
        return next;
    }
    pub fn fail(self: State, token: Token, reason: []const u8, retry_at_ms: u64) !State {
        try self.validate();
        const attempt = self.pending orelse return error.LakeIndexPublicationFenceChanged;
        if (!std.mem.eql(u8, &attempt.token, &token)) return error.LakeIndexPublicationFenceChanged;
        var next = self;
        next.pending = null;
        next.failure = .{ .generation = attempt.generation, .desired = attempt.signature.desired, .reason = reason, .retry_at_ms = retry_at_ms };
        try next.validate();
        return next;
    }
    pub fn renew(self: State, token: Token, now_ms: u64, lease_ms: u64) !State {
        try self.validate();
        const attempt = self.pending orelse return error.LakeIndexPublicationFenceChanged;
        if (!std.mem.eql(u8, &attempt.token, &token) or now_ms < attempt.started_at_ms or now_ms >= attempt.lease_expires_at_ms) return error.LakeIndexPublicationFenceChanged;
        var next = self;
        next.pending.?.lease_expires_at_ms = @max(attempt.lease_expires_at_ms, std.math.add(u64, now_ms, lease_ms) catch return error.InvalidLakeIndexCatalog);
        try next.validate();
        return next;
    }
    pub fn clear(self: State) !State {
        try self.validate();
        return .{ .namespace = self.namespace, .generation = std.math.add(u64, self.generation, 1) catch return error.LakeIndexGenerationExhausted };
    }
};
fn artifactValid(ref: artifacts.ArtifactRef) !void {
    if (ref.artifact_id.len == 0 or ref.artifact_id.len > 512 or ref.byte_len == 0 or ref.checksum.len != 64) return error.InvalidLakeIndexCatalog;
    for (ref.checksum) |byte| if (!std.ascii.isHex(byte)) return error.InvalidLakeIndexCatalog;
}
pub fn parse(a: A, bytes: []const u8) !std.json.Parsed(State) {
    if (bytes.len > max_json_bytes) return error.InvalidLakeIndexCatalog;
    const parsed = try std.json.parseFromSlice(State, a, if (bytes.len == 0) "{}" else bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
pub fn encode(a: A, state: State) ![]u8 {
    try state.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_json_bytes) return error.InvalidLakeIndexCatalog;
    return bytes;
}
fn part(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, @intCast(bytes.len), .little);
    hash.update(&length);
    hash.update(bytes);
}
/// Only target schema/index semantics participate. The transitional read
/// schema is a serving projection, not a build input: finalizing it must not
/// invalidate an immutable index built for the same target schema.
pub fn desiredFingerprint(table: anytype) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-lake-index-definition-v6");
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, table.table_id, .little);
    hash.update(&id);
    part(&hash, table.name);
    part(&hash, table.schema_json);
    part(&hash, table.indexes_json);
    return hash.finalResult();
}

test "external lake desired publication survives target read schema finalization" {
    var table: @import("../common/topology_records.zig").TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = "target", .read_schema_json = "previous", .indexes_json = "indexes" };
    const expected = desiredFingerprint(table);
    table.read_schema_json = "";
    try std.testing.expectEqual(expected, desiredFingerprint(table));
    table.schema_json = "changed target";
    try std.testing.expect(!std.mem.eql(u8, &expected, &desiredFingerprint(table)));
}

test "metadata.lake index leases fence expiry retries and changed sources" {
    const signature: Signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) };
    const first = try (State{}).begin(signature, @splat(1), 100, 20);
    try std.testing.expectError(error.LakeIndexBuildInProgress, first.begin(signature, @splat(2), 101, 20));
    const retry = try first.begin(signature, @splat(2), 120, 20);
    try std.testing.expectEqual(@as(u64, 2), retry.generation);
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, retry.fail(@splat(1), "late worker", 200));
    const failed = try retry.fail(@splat(2), "source changed", 200);
    const bytes = try encode(std.testing.allocator, failed);
    defer std.testing.allocator.free(bytes);
    var parsed = try parse(std.testing.allocator, bytes);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("source changed", parsed.value.failure.?.reason);
    var changed = signature;
    changed.source = @splat(9);
    const replacement = try first.begin(changed, @splat(3), 101, 20);
    try std.testing.expectEqual(@as(u64, 2), replacement.generation);
    try std.testing.expectError(error.InvalidLakeIndexCatalog, parse(std.testing.allocator, "{\"version\":2}"));
}

pub fn publicationEqual(a: A, left: ?Publication, right: ?Publication) !bool {
    if (left == null or right == null) return left == null and right == null;
    const first = try std.json.Stringify.valueAlloc(a, left.?, .{});
    defer a.free(first);
    const second = try std.json.Stringify.valueAlloc(a, right.?, .{});
    defer a.free(second);
    return std.mem.eql(u8, first, second);
}
/// Replicas enforce structural lease transitions deterministically. The
/// publishing coordinator separately checks wall-clock expiry before append.
pub fn transitionAllowed(a: A, before: anytype, after: anytype) !bool {
    if (std.mem.eql(u8, before.lake_index_catalog_json, after.lake_index_catalog_json)) return true;
    var old = try parse(a, before.lake_index_catalog_json);
    defer old.deinit();
    var next = try parse(a, after.lake_index_catalog_json);
    defer next.deinit();
    const previous = old.value;
    const replacement = next.value;
    if (!std.meta.eql(previous.namespace, replacement.namespace)) {
        if (previous.namespace != null or replacement.namespace == null or replacement.generation != previous.generation +| 1 or replacement.pending == null) return false;
    }
    // Dropping every retained root still advances the durable attempt counter.
    if (replacement.generation == previous.generation +| 1 and replacement.generation > previous.generation) {
        if (replacement.pending) |attempt| {
            return std.mem.eql(u8, &attempt.signature.desired, &desiredFingerprint(after)) and
                replacement.failure == null and try publicationEqual(a, previous.published, replacement.published);
        }
        return replacement.published == null and replacement.failure == null;
    }
    if (replacement.generation != previous.generation) return false;
    const attempt = previous.pending orelse return false;
    if (replacement.pending) |renewed| {
        return renewed.generation == attempt.generation and std.mem.eql(u8, &renewed.token, &attempt.token) and
            renewed.reader_protocol == attempt.reader_protocol and try locatorEqual(a, renewed.store_locator, attempt.store_locator) and
            std.meta.eql(renewed.signature, attempt.signature) and renewed.started_at_ms == attempt.started_at_ms and
            renewed.lease_expires_at_ms >= attempt.lease_expires_at_ms and replacement.failure == null and
            try publicationEqual(a, previous.published, replacement.published);
    }
    if (replacement.failure) |failure| {
        return failure.generation == attempt.generation and std.mem.eql(u8, &failure.desired, &attempt.signature.desired) and
            try publicationEqual(a, previous.published, replacement.published);
    }
    const publication = replacement.published orelse return false;
    return publication.generation == attempt.generation and std.mem.eql(u8, &publication.token, &attempt.token) and
        publication.reader_protocol == attempt.reader_protocol and try locatorEqual(a, publication.store_locator, attempt.store_locator) and
        std.meta.eql(publication.signature, attempt.signature) and std.mem.eql(u8, &publication.signature.desired, &desiredFingerprint(after)) and
        publication.published_at_ms >= attempt.started_at_ms and publication.published_at_ms < attempt.lease_expires_at_ms;
}

fn locatorEqual(a: A, left: ?StoreLocator, right: ?StoreLocator) !bool {
    const before = try std.json.Stringify.valueAlloc(a, left, .{});
    defer a.free(before);
    const after = try std.json.Stringify.valueAlloc(a, right, .{});
    defer a.free(after);
    return std.mem.eql(u8, before, after);
}

test "metadata.lake index publication preserves ready roots and fences changed definitions" {
    const a = std.testing.allocator;
    const Table = @import("../common/topology_records.zig").TableRecord;
    var table: Table = .{ .table_id = 4, .name = "lake", .schema_json = "{}" };
    const signature: Signature = .{ .desired = desiredFingerprint(table), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) };
    const attempt = try (State{}).begin(signature, @splat(5), 100, 20);
    const inventory: artifacts.ArtifactRef = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" };
    const publication: Publication = .{
        .reader_protocol = attempt.pending.?.reader_protocol,
        .generation = attempt.generation,
        .token = attempt.pending.?.token,
        .signature = signature,
        .published_at_ms = 101,
        .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } },
        .inventory = inventory,
        .declarations = &.{},
    };
    const building_bytes = try encode(a, attempt);
    defer a.free(building_bytes);
    var building = table;
    building.lake_index_catalog_json = building_bytes;
    try std.testing.expect(try transitionAllowed(a, table, building));
    const ready = try attempt.publish(publication, 101);
    const ready_bytes = try encode(a, ready);
    defer a.free(ready_bytes);
    var published = building;
    published.lake_index_catalog_json = ready_bytes;
    try std.testing.expect(try transitionAllowed(a, building, published));
    var parsed = try parse(a, ready_bytes);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("inventory", parsed.value.published.?.inventory.artifact_id);
    try std.testing.expectEqualStrings("snapshot", parsed.value.published.?.base_source.external_parquet.snapshot_id);
    var stale = publication;
    stale.reader_protocol = 0;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, attempt.publish(stale, 101));
    stale = publication;
    stale.token = @splat(6);
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, attempt.publish(stale, 101));
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, attempt.publish(publication, 120));
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, attempt.publish(publication, 99));
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, attempt.publish(publication, 102));
    const renewal = try attempt.renew(attempt.pending.?.token, 119, 20);
    try std.testing.expectEqual(@as(u64, 139), renewal.pending.?.lease_expires_at_ms);
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, attempt.renew(attempt.pending.?.token, 120, 20));
    table.indexes_json = "{\"changed\":{}}";
    var changed = published;
    changed.indexes_json = table.indexes_json;
    try std.testing.expect(try transitionAllowed(a, published, changed));
    var invalid_publication = published;
    invalid_publication.indexes_json = table.indexes_json;
    try std.testing.expect(!try transitionAllowed(a, building, invalid_publication));
    var changed_signature = signature;
    changed_signature.desired = desiredFingerprint(table);
    const rebuild = try ready.begin(changed_signature, @splat(7), 200, 20);
    const rebuild_bytes = try encode(a, rebuild);
    defer a.free(rebuild_bytes);
    var rebuilding = changed;
    rebuilding.lake_index_catalog_json = rebuild_bytes;
    try std.testing.expect(try transitionAllowed(a, changed, rebuilding));
    try std.testing.expectEqual(@as(u64, 1), rebuild.published.?.generation);
    const failure = try rebuild.fail(@splat(7), "changed object", 300);
    const failure_bytes = try encode(a, failure);
    defer a.free(failure_bytes);
    var failed = rebuilding;
    failed.lake_index_catalog_json = failure_bytes;
    try std.testing.expect(try transitionAllowed(a, rebuilding, failed));
    try std.testing.expectEqual(@as(u64, 1), failure.published.?.generation);
    const cleared = try failure.clear();
    const cleared_bytes = try encode(a, cleared);
    defer a.free(cleared_bytes);
    var empty = failed;
    empty.lake_index_catalog_json = cleared_bytes;
    try std.testing.expect(try transitionAllowed(a, failed, empty));
    try std.testing.expect(!try transitionAllowed(a, failed, table));
}

fn cloneQueryDefinitionFaultCase(a: A) !void {
    const domain = @import("../system_catalog/domain.zig");
    const table: @import("../common/topology_records.zig").TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = "schema", .read_schema_json = "read schema", .indexes_json = "indexes", .lake_index_catalog_json = "{\"generation\":7}" };
    const copy = try domain.QueryDefinition.fromTable(table).clone(a);
    defer copy.deinit(a);
    try std.testing.expectEqualStrings(table.lake_index_catalog_json, copy.lake_index_catalog_json);
    try std.testing.expect(copy.lake_index_catalog_json.ptr != table.lake_index_catalog_json.ptr);
    const encoded = try std.json.Stringify.valueAlloc(a, copy, .{});
    defer a.free(encoded);
    var decoded = try std.json.parseFromSlice(domain.QueryDefinition, a, encoded, .{ .allocate = .alloc_always });
    defer decoded.deinit();
    try std.testing.expectEqualStrings(table.lake_index_catalog_json, decoded.value.lake_index_catalog_json);
}

test "metadata.lake index query definitions own publication bytes under allocation failures" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, cloneQueryDefinitionFaultCase, .{});
    const a = std.testing.allocator;
    const table: @import("../common/topology_records.zig").TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = "schema" };
    const bytes = try std.json.Stringify.valueAlloc(a, @import("../system_catalog/domain.zig").QueryDefinition.fromTable(table), .{});
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "lake_index_catalog_json") == null);
    const table_bytes = try std.json.Stringify.valueAlloc(a, table, .{});
    defer a.free(table_bytes);
    try std.testing.expect(std.mem.indexOf(u8, table_bytes, "lake_index_catalog_json") == null);
}
