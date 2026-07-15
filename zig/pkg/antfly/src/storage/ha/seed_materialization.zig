// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Offline materialization of a portable HA logical seed into a bootable data
//! directory. Raw transport bytes remain immutable; the returned live tree is
//! a separate generation that the runtime may mutate after ACTIVE publication.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const data_format = @import("../../common/data_format.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const extensions = @import("../../extensions/mod.zig");
const table_manager = @import("../../metadata/table_manager.zig");
const raft_catalog = @import("../../raft/storage/catalog.zig");
const db_mod = @import("../db/db.zig");
const generation_lifecycle = @import("../db/generation_lifecycle.zig");
const validation = @import("validation.zig");

pub const topology_format_version: u16 = 2;
pub const topology_name = "TOPOLOGY.json";
pub const materialized_receipt_name = ".antfly-ha-materialized.json";
pub const max_topology_bytes: usize = 64 * 1024 * 1024;
pub const max_materialized_receipt_bytes: usize = 64 * 1024 * 1024;
pub const max_files: usize = 1_000_000;
pub const max_file_bytes: u64 = 64 * 1024 * 1024 * 1024;

pub const LogicalCatalog = struct {
    epoch: u64,
    tables: []const table_manager.TableRecord,
    ranges: []const table_manager.RangeRecord,
    extension_packages: []const extensions.PackageManifest = &.{},
    installed_extensions: []const extensions.InstalledExtension = &.{},
    extension_members: []const extensions.ExtensionMember = &.{},
    extension_dependencies: []const extensions.ExtensionDependency = &.{},
};

pub const ReplicaSnapshot = struct {
    group_id: u64,
    table_id: u64,
    table_name: []const u8,
    snapshot_path: []const u8,
    logical_sha256: []const u8,
    identity_table_id: u64,
    identity_shard_id: u64,
    identity_range_id: u64,
};

pub const ExtensionArtifact = struct {
    package_name: []const u8,
    package_version: []const u8,
    path: []const u8,
    size_bytes: u64,
    sha256: []const u8,
};

pub const Topology = struct {
    format_version: u16 = topology_format_version,
    generation: []const u8,
    catalog: LogicalCatalog,
    replicas: []const ReplicaSnapshot,
    extension_artifacts: []const ExtensionArtifact = &.{},
};

pub const MaterializeRequest = struct {
    raw_generation_root: []const u8,
    live_installing_root: []const u8,
    generation: []const u8,
    target_local_node_id: u64,
    target_replica_id: u64 = 1,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    raw_manifest_sha256: []const u8,
    raw_aggregate_sha256: []const u8,
};

pub const MaterializedFile = struct {
    path: []const u8,
    size_bytes: u64,
    sha256: []const u8,
};

pub const MaterializedReceipt = struct {
    format_version: u16 = 1,
    generation: []const u8,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    raw_manifest_sha256: []const u8,
    raw_aggregate_sha256: []const u8,
    target_local_node_id: u64,
    target_replica_id: u64,
    topology_sha256: []const u8,
    aggregate_sha256: []const u8,
    file_count: usize,
    total_bytes: u64,
    files: []const MaterializedFile,
};

pub const MaterializeResult = struct {
    receipt_json: []u8,
    receipt_sha256: [Sha256.digest_length * 2]u8,
    aggregate_sha256: [Sha256.digest_length * 2]u8,

    pub fn deinit(self: *MaterializeResult, alloc: Allocator) void {
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub const PublishedEvidence = struct {
    receipt_json: []u8,
    receipt_sha256: [Sha256.digest_length * 2]u8,
    aggregate_sha256: [Sha256.digest_length * 2]u8,

    pub fn deinit(self: *PublishedEvidence, alloc: Allocator) void {
        alloc.free(self.receipt_json);
        self.* = undefined;
    }
};

pub fn materialize(alloc: Allocator, request: MaterializeRequest) !MaterializeResult {
    try validateMaterializeRequest(request);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    if (try pathExists(io, request.live_installing_root)) return error.LiveInstallingRootExists;
    try fs_paths.createDirPathPortable(io, request.live_installing_root);
    errdefer std.Io.Dir.cwd().deleteTree(io, request.live_installing_root) catch {};

    const topology_path = try std.fs.path.join(alloc, &.{ request.raw_generation_root, topology_name });
    defer alloc.free(topology_path);
    const topology_json = try readFileAlloc(io, alloc, topology_path, max_topology_bytes);
    defer alloc.free(topology_json);
    var parsed = std.json.parseFromSlice(Topology, alloc, topology_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidSeedTopology;
    defer parsed.deinit();
    try validateTopology(alloc, io, request.raw_generation_root, request.generation, parsed.value);

    try data_format.ensureCompatible(alloc, request.live_installing_root);
    const data_root = try std.fs.path.join(alloc, &.{ request.live_installing_root, "data" });
    defer alloc.free(data_root);
    const replicas_root = try std.fs.path.join(alloc, &.{ data_root, "replicas" });
    defer alloc.free(replicas_root);
    const metadata_root = try std.fs.path.join(alloc, &.{ request.live_installing_root, "metadata" });
    defer alloc.free(metadata_root);
    const extension_root = try std.fs.path.join(alloc, &.{ request.live_installing_root, "extensions" });
    defer alloc.free(extension_root);
    try fs_paths.createDirPathPortable(io, replicas_root);
    try fs_paths.createDirPathPortable(io, metadata_root);

    const local_catalog_path = try std.fs.path.join(alloc, &.{ metadata_root, "local-metadata.json" });
    defer alloc.free(local_catalog_path);
    const local_catalog_json = try std.json.Stringify.valueAlloc(alloc, parsed.value.catalog, .{ .emit_null_optional_fields = false });
    defer alloc.free(local_catalog_json);
    try writeNewFileDurably(io, local_catalog_path, local_catalog_json);

    const replica_catalog_path = try std.fs.path.join(alloc, &.{ data_root, "catalog.txt" });
    defer alloc.free(replica_catalog_path);
    var catalog = try raft_catalog.FileReplicaCatalog.init(alloc, replica_catalog_path);
    defer catalog.deinit();

    for (parsed.value.replicas) |replica| {
        const snapshot_root = try std.fs.path.join(alloc, &.{ request.raw_generation_root, replica.snapshot_path });
        defer alloc.free(snapshot_root);
        const relative_db_path = try std.fmt.allocPrint(alloc, "group-{d}/table-db", .{replica.group_id});
        defer alloc.free(relative_db_path);
        const db_path = try std.fs.path.join(alloc, &.{ replicas_root, relative_db_path });
        defer alloc.free(db_path);

        var transition = try generation_lifecycle.beginProcessExclusiveWithRuntime(db_path, null);
        defer transition.deinit();
        var staged = try transition.beginStaging();
        defer staged.deinit();
        try db_mod.DB.restoreSnapshotToStagedGeneration(&staged, alloc, snapshot_root, staged.path(), .{
            .identity_namespace = .{
                .table_id = replica.identity_table_id,
                .shard_id = replica.identity_shard_id,
                .range_id = replica.identity_range_id,
            },
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        if (try staged.publish() != .durable) return error.LiveDBPublicationConflict;
        try catalog.catalog().upsertReplica(.{
            .group_id = replica.group_id,
            .replica_id = request.target_replica_id,
            .local_node_id = request.target_local_node_id,
            .bootstrap_mode = .persisted,
            .metadata_version = parsed.value.catalog.epoch,
        });
    }

    for (parsed.value.extension_artifacts) |artifact| {
        const source = try std.fs.path.join(alloc, &.{ request.raw_generation_root, artifact.path });
        defer alloc.free(source);
        const relative = artifact.path["extensions/".len..];
        const destination = try std.fs.path.join(alloc, &.{ extension_root, relative });
        defer alloc.free(destination);
        try copyFileDurably(io, source, destination);
    }

    try fs_paths.syncDirPortable(io, request.live_installing_root);
    const files = try collectMaterializedFiles(alloc, io, request.live_installing_root);
    defer freeMaterializedFiles(alloc, files);
    var total_bytes: u64 = 0;
    for (files) |file| total_bytes = std.math.add(u64, total_bytes, file.size_bytes) catch return error.MaterializedSeedTooLarge;
    const aggregate_sha256 = aggregateFiles(files);
    var topology_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(topology_json, &topology_digest, .{});
    var topology_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&topology_sha256, &topology_digest);

    const receipt_json = try std.json.Stringify.valueAlloc(alloc, MaterializedReceipt{
        .generation = request.generation,
        .seed_receipt_sha256 = request.seed_receipt_sha256,
        .capture_receipt_sha256 = request.capture_receipt_sha256,
        .raw_manifest_sha256 = request.raw_manifest_sha256,
        .raw_aggregate_sha256 = request.raw_aggregate_sha256,
        .target_local_node_id = request.target_local_node_id,
        .target_replica_id = request.target_replica_id,
        .topology_sha256 = &topology_sha256,
        .aggregate_sha256 = &aggregate_sha256,
        .file_count = files.len,
        .total_bytes = total_bytes,
        .files = files,
    }, .{});
    errdefer alloc.free(receipt_json);
    const receipt_path = try std.fs.path.join(alloc, &.{ request.live_installing_root, materialized_receipt_name });
    defer alloc.free(receipt_path);
    try writeNewFileDurably(io, receipt_path, receipt_json);
    try fs_paths.syncDirPortable(io, request.live_installing_root);

    var receipt_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(receipt_json, &receipt_digest, .{});
    var receipt_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&receipt_sha256, &receipt_digest);
    return .{
        .receipt_json = receipt_json,
        .receipt_sha256 = receipt_sha256,
        .aggregate_sha256 = aggregate_sha256,
    };
}

pub fn validatePublishedBeforeRuntime(
    alloc: Allocator,
    live_root: []const u8,
    expected_generation: []const u8,
    expected_receipt_sha256: []const u8,
) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const receipt_path = try std.fs.path.join(alloc, &.{ live_root, materialized_receipt_name });
    defer alloc.free(receipt_path);
    const receipt_json = try readFileAlloc(io_impl.io(), alloc, receipt_path, max_materialized_receipt_bytes);
    defer alloc.free(receipt_json);
    try expectSha256(receipt_json, expected_receipt_sha256, error.MaterializedReceiptDigestMismatch);
    var parsed = std.json.parseFromSlice(MaterializedReceipt, alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidMaterializedReceipt;
    defer parsed.deinit();
    if (parsed.value.format_version != 1 or
        !std.mem.eql(u8, parsed.value.generation, expected_generation) or
        parsed.value.file_count != parsed.value.files.len or
        !isCanonicalSha256(parsed.value.aggregate_sha256)) return error.InvalidMaterializedReceipt;
    try validateMaterializedFiles(alloc, io_impl.io(), live_root, parsed.value);
}

pub fn loadPublishedEvidence(alloc: Allocator, live_root: []const u8, expected_generation: []const u8) !PublishedEvidence {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const receipt_path = try std.fs.path.join(alloc, &.{ live_root, materialized_receipt_name });
    defer alloc.free(receipt_path);
    const receipt_json = try readFileAlloc(io_impl.io(), alloc, receipt_path, max_materialized_receipt_bytes);
    errdefer alloc.free(receipt_json);
    var parsed = std.json.parseFromSlice(MaterializedReceipt, alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidMaterializedReceipt;
    defer parsed.deinit();
    if (parsed.value.format_version != 1 or !std.mem.eql(u8, parsed.value.generation, expected_generation) or
        !isCanonicalSha256(parsed.value.aggregate_sha256)) return error.InvalidMaterializedReceipt;
    var receipt_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(receipt_json, &receipt_digest, .{});
    var receipt_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&receipt_sha256, &receipt_digest);
    var aggregate_sha256: [Sha256.digest_length * 2]u8 = undefined;
    @memcpy(&aggregate_sha256, parsed.value.aggregate_sha256);
    return .{
        .receipt_json = receipt_json,
        .receipt_sha256 = receipt_sha256,
        .aggregate_sha256 = aggregate_sha256,
    };
}

/// Startup validation intentionally does not hash mutable runtime files. It
/// checks the immutable materialization marker and opens every declared DB with
/// its exact logical identity; normal storage recovery validates file framing.
pub fn validateRuntimeIdentity(
    alloc: Allocator,
    raw_generation_root: []const u8,
    live_root: []const u8,
    expected_generation: []const u8,
    expected_receipt_sha256: []const u8,
) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const receipt_path = try std.fs.path.join(alloc, &.{ live_root, materialized_receipt_name });
    defer alloc.free(receipt_path);
    const receipt_json = try readFileAlloc(io_impl.io(), alloc, receipt_path, max_materialized_receipt_bytes);
    defer alloc.free(receipt_json);
    try expectSha256(receipt_json, expected_receipt_sha256, error.MaterializedReceiptDigestMismatch);
    var receipt = std.json.parseFromSlice(MaterializedReceipt, alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidMaterializedReceipt;
    defer receipt.deinit();
    if (receipt.value.format_version != 1 or !std.mem.eql(u8, receipt.value.generation, expected_generation))
        return error.InvalidMaterializedReceipt;

    const topology_path = try std.fs.path.join(alloc, &.{ raw_generation_root, topology_name });
    defer alloc.free(topology_path);
    const topology_json = try readFileAlloc(io_impl.io(), alloc, topology_path, max_topology_bytes);
    defer alloc.free(topology_json);
    var topology_digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(topology_json, &topology_digest, .{});
    var topology_sha256: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&topology_sha256, &topology_digest);
    if (!std.mem.eql(u8, &topology_sha256, receipt.value.topology_sha256)) return error.MaterializedTopologyMismatch;
    var topology = std.json.parseFromSlice(Topology, alloc, topology_json, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidSeedTopology;
    defer topology.deinit();

    const data_catalog_path = try std.fs.path.join(alloc, &.{ live_root, "data/catalog.txt" });
    defer alloc.free(data_catalog_path);
    var catalog = try raft_catalog.FileReplicaCatalog.init(alloc, data_catalog_path);
    defer catalog.deinit();
    const records = try catalog.catalog().listReplicas(alloc);
    defer raft_catalog.freeReplicaRecords(alloc, records);
    if (records.len != topology.value.replicas.len) return error.LiveReplicaCatalogMismatch;

    for (topology.value.replicas) |replica| {
        var found = false;
        for (records) |record| if (record.group_id == replica.group_id) {
            found = true;
            if (record.local_node_id != receipt.value.target_local_node_id or
                record.replica_id != receipt.value.target_replica_id or
                record.bootstrap_mode != .persisted) return error.LiveReplicaCatalogMismatch;
        };
        if (!found) return error.LiveReplicaCatalogMismatch;
        const db_path = try std.fmt.allocPrint(alloc, "{s}/data/replicas/group-{d}/table-db", .{ live_root, replica.group_id });
        defer alloc.free(db_path);
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = .{
                .table_id = replica.identity_table_id,
                .shard_id = replica.identity_shard_id,
                .range_id = replica.identity_range_id,
            },
            .start_index_workers = false,
            .start_optional_runtimes = false,
        });
        db.close();
    }
}

pub fn validateTopology(
    alloc: Allocator,
    io: std.Io,
    raw_root: []const u8,
    expected_generation: []const u8,
    topology: Topology,
) !void {
    if (topology.format_version != topology_format_version or
        !std.mem.eql(u8, topology.generation, expected_generation) or
        topology.catalog.epoch == 0 or topology.catalog.tables.len == 0 or
        topology.catalog.ranges.len == 0 or
        topology.replicas.len != topology.catalog.ranges.len) return error.InvalidSeedTopology;

    for (topology.catalog.tables, 0..) |table, index| {
        if (table.table_id == 0 or !validation.isIdentifier(table.name)) return error.InvalidSeedTopology;
        if (index > 0 and topology.catalog.tables[index - 1].table_id >= table.table_id) return error.NonCanonicalSeedTopology;
        try validateJson(table.schema_json, true);
        try validateJson(table.read_schema_json, true);
        try validateJson(table.indexes_json, false);
        try validateJson(table.replication_sources_json, false);
    }
    for (topology.catalog.ranges, 0..) |range, index| {
        if (range.group_id == 0 or range.table_id == 0) return error.InvalidSeedTopology;
        if (index > 0 and topology.catalog.ranges[index - 1].group_id >= range.group_id) return error.NonCanonicalSeedTopology;
        _ = findTable(topology.catalog.tables, range.table_id) orelse return error.SeedRangeTableMissing;
    }
    var extension_catalog = extensions.ExtensionCatalog.init(alloc);
    defer extension_catalog.deinit();
    try extension_catalog.loadProjectedRows(
        topology.catalog.extension_packages,
        topology.catalog.installed_extensions,
        topology.catalog.extension_members,
        topology.catalog.extension_dependencies,
    );
    for (topology.catalog.extension_packages, 0..) |package, index| {
        package.validate() catch return error.InvalidExtensionSeedCatalog;
        if (index > 0) {
            const prior = topology.catalog.extension_packages[index - 1];
            const name_order = std.mem.order(u8, prior.name, package.name);
            if (name_order == .gt or
                (name_order == .eq and std.mem.order(u8, prior.version, package.version) != .lt))
                return error.NonCanonicalSeedTopology;
        }
        var artifact_count: usize = 0;
        var has_manifest = false;
        for (topology.extension_artifacts) |artifact| {
            if (!std.mem.eql(u8, artifact.package_name, package.name) or
                !std.mem.eql(u8, artifact.package_version, package.version)) continue;
            artifact_count += 1;
            if (std.mem.eql(u8, std.fs.path.basename(artifact.path), extensions.package_manifest_filename))
                has_manifest = true;
        }
        if (artifact_count == 0 or !has_manifest) return error.ExtensionSeedCatalogMismatch;
    }

    for (topology.replicas, 0..) |replica, index| {
        if (index > 0 and topology.replicas[index - 1].group_id >= replica.group_id) return error.NonCanonicalSeedTopology;
        const range = findRange(topology.catalog.ranges, replica.group_id) orelse return error.SeedReplicaRangeMissing;
        const table = findTable(topology.catalog.tables, replica.table_id) orelse return error.SeedReplicaTableMissing;
        const expected_path = try std.fmt.allocPrint(alloc, "replicas/group-{d}", .{replica.group_id});
        defer alloc.free(expected_path);
        if (replica.group_id != range.group_id or replica.table_id != range.table_id or
            !std.mem.eql(u8, replica.table_name, table.name) or
            !std.mem.eql(u8, replica.snapshot_path, expected_path) or
            replica.identity_table_id != table.table_id or
            replica.identity_shard_id != range.doc_identity_shard_id or
            replica.identity_range_id != range.doc_identity_range_id or
            !isCanonicalSha256(replica.logical_sha256)) return error.SeedReplicaIdentityMismatch;
        const store_path = try std.fs.path.join(alloc, &.{ raw_root, replica.snapshot_path, "store.bin" });
        defer alloc.free(store_path);
        try expectFileSha256(io, alloc, store_path, replica.logical_sha256);
    }

    for (topology.extension_artifacts, 0..) |artifact, index| {
        if (!validation.isIdentifier(artifact.package_name) or artifact.package_version.len == 0 or
            !std.mem.startsWith(u8, artifact.path, "extensions/") or
            !isSafeRelativePath(artifact.path) or
            artifact.size_bytes > max_file_bytes or !isCanonicalSha256(artifact.sha256))
            return error.InvalidExtensionSeedArtifact;
        if (index > 0 and std.mem.order(u8, topology.extension_artifacts[index - 1].path, artifact.path) != .lt)
            return error.NonCanonicalSeedTopology;
        const package = findPackage(topology.catalog.extension_packages, artifact.package_name, artifact.package_version) orelse
            return error.ExtensionSeedCatalogMismatch;
        _ = package;
        const path = try std.fs.path.join(alloc, &.{ raw_root, artifact.path });
        defer alloc.free(path);
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size != artifact.size_bytes) return error.ExtensionSeedArtifactMismatch;
        try expectFileSha256(io, alloc, path, artifact.sha256);
    }
}

fn validateMaterializeRequest(request: MaterializeRequest) !void {
    if (!validation.isAbsoluteNormalizedPath(request.raw_generation_root) or
        !validation.isAbsoluteNormalizedPath(request.live_installing_root) or
        !validation.isIdentifier(request.generation) or request.target_local_node_id == 0 or
        request.target_replica_id == 0 or
        !isCanonicalSha256(request.seed_receipt_sha256) or
        !isCanonicalSha256(request.capture_receipt_sha256) or
        !isCanonicalSha256(request.raw_manifest_sha256) or
        !isCanonicalSha256(request.raw_aggregate_sha256)) return error.InvalidMaterializeRequest;
}

fn validateJson(raw: []const u8, allow_empty: bool) !void {
    if (raw.len == 0 and allow_empty) return;
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, raw, .{}) catch return error.InvalidSeedTopology;
    parsed.deinit();
}

fn findTable(tables: []const table_manager.TableRecord, table_id: u64) ?table_manager.TableRecord {
    for (tables) |table| if (table.table_id == table_id) return table;
    return null;
}

fn findRange(ranges: []const table_manager.RangeRecord, group_id: u64) ?table_manager.RangeRecord {
    for (ranges) |range| if (range.group_id == group_id) return range;
    return null;
}

fn findPackage(packages: []const extensions.PackageManifest, name: []const u8, version: []const u8) ?extensions.PackageManifest {
    for (packages) |package| if (std.mem.eql(u8, package.name, name) and std.mem.eql(u8, package.version, version)) return package;
    return null;
}

fn collectMaterializedFiles(alloc: Allocator, io: std.Io, root: []const u8) ![]MaterializedFile {
    var dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var files = std.ArrayListUnmanaged(MaterializedFile).empty;
    errdefer freeMaterializedFiles(alloc, files.items);
    while (try walker.next(io)) |entry| {
        if (entry.kind == .directory) continue;
        if (entry.kind != .file or !isSafeRelativePath(entry.path)) return error.UnsafeMaterializedSeedEntry;
        if (std.mem.eql(u8, entry.path, materialized_receipt_name)) continue;
        if (files.items.len >= max_files) return error.MaterializedSeedTooManyFiles;
        const path = try std.fs.path.join(alloc, &.{ root, entry.path });
        defer alloc.free(path);
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size > max_file_bytes) return error.MaterializedSeedFileTooLarge;
        const digest = try fileSha256HexAlloc(alloc, io, path);
        errdefer alloc.free(digest);
        const owned_path = try alloc.dupe(u8, entry.path);
        errdefer alloc.free(owned_path);
        try files.append(alloc, .{ .path = owned_path, .size_bytes = stat.size, .sha256 = digest });
    }
    std.mem.sort(MaterializedFile, files.items, {}, struct {
        fn lessThan(_: void, left: MaterializedFile, right: MaterializedFile) bool {
            return std.mem.order(u8, left.path, right.path) == .lt;
        }
    }.lessThan);
    return try files.toOwnedSlice(alloc);
}

fn validateMaterializedFiles(alloc: Allocator, io: std.Io, root: []const u8, receipt: MaterializedReceipt) !void {
    var total: u64 = 0;
    for (receipt.files, 0..) |file, index| {
        if (!isSafeRelativePath(file.path) or !isCanonicalSha256(file.sha256)) return error.InvalidMaterializedReceipt;
        if (index > 0 and std.mem.order(u8, receipt.files[index - 1].path, file.path) != .lt) return error.InvalidMaterializedReceipt;
        const path = try std.fs.path.join(alloc, &.{ root, file.path });
        defer alloc.free(path);
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size != file.size_bytes) return error.MaterializedFileMismatch;
        try expectFileSha256(io, alloc, path, file.sha256);
        total = std.math.add(u64, total, file.size_bytes) catch return error.MaterializedSeedTooLarge;
    }
    if (total != receipt.total_bytes) return error.InvalidMaterializedReceipt;
    const aggregate = aggregateFiles(receipt.files);
    if (!std.mem.eql(u8, &aggregate, receipt.aggregate_sha256)) return error.MaterializedAggregateMismatch;
}

fn aggregateFiles(files: []const MaterializedFile) [Sha256.digest_length * 2]u8 {
    var hasher = Sha256.init(.{});
    hasher.update("antfly-ha-materialized-v1\x00");
    for (files) |file| {
        var len_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &len_buf, file.path.len, .big);
        hasher.update(&len_buf);
        hasher.update(file.path);
        std.mem.writeInt(u64, &len_buf, file.size_bytes, .big);
        hasher.update(&len_buf);
        hasher.update(file.sha256);
    }
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    var encoded: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&encoded, &digest);
    return encoded;
}

fn freeMaterializedFiles(alloc: Allocator, files: []MaterializedFile) void {
    for (files) |file| {
        alloc.free(file.path);
        alloc.free(file.sha256);
    }
    if (files.len > 0) alloc.free(files);
}

fn expectFileSha256(io: std.Io, alloc: Allocator, path: []const u8, expected: []const u8) !void {
    const digest = try fileSha256HexAlloc(alloc, io, path);
    defer alloc.free(digest);
    if (!std.mem.eql(u8, digest, expected)) return error.SeedLogicalDigestMismatch;
}

fn fileSha256HexAlloc(alloc: Allocator, io: std.Io, path: []const u8) ![]u8 {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &.{});
    var hasher = Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    while (true) {
        const n = try reader.interface.readSliceShort(&buffer);
        if (n == 0) break;
        hasher.update(buffer[0..n]);
    }
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const encoded = try alloc.alloc(u8, Sha256.digest_length * 2);
    encodeHex(encoded, &digest);
    return encoded;
}

fn copyFileDurably(io: std.Io, source: []const u8, destination: []const u8) !void {
    try std.Io.Dir.copyFile(std.Io.Dir.cwd(), source, std.Io.Dir.cwd(), destination, io, .{
        .make_path = true,
        .replace = false,
    });
    try fs_paths.syncFileAndParentPortable(io, destination);
}

fn writeNewFileDurably(io: std.Io, path: []const u8, body: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return error.InvalidMaterializedPath;
    try fs_paths.createDirPathPortable(io, parent);
    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
    defer file.close(io);
    try file.writeStreamingAll(io, body);
    try file.sync(io);
    try fs_paths.syncDirPortable(io, parent);
}

fn expectSha256(body: []const u8, expected: []const u8, mismatch: anyerror) !void {
    if (!isCanonicalSha256(expected)) return mismatch;
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(body, &digest, .{});
    var encoded: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&encoded, &digest);
    if (!std.mem.eql(u8, &encoded, expected)) return mismatch;
}

fn isCanonicalSha256(value: []const u8) bool {
    if (value.len != Sha256.digest_length * 2) return false;
    for (value) |byte| if ((byte < '0' or byte > '9') and (byte < 'a' or byte > 'f')) return false;
    return true;
}

fn isSafeRelativePath(path: []const u8) bool {
    return !std.fs.path.isAbsolute(path) and validation.isNormalizedPath(path);
}

fn encodeHex(out: []u8, bytes: []const u8) void {
    for (bytes, 0..) |byte, index| {
        out[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
}

fn readFileAlloc(io: std.Io, alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_bytes));
}

fn pathExists(io: std.Io, path: []const u8) !bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
