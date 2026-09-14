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
const topology_records = @import("../../common/topology_records.zig");
const extensions = @import("../../extensions/mod.zig");
const raft_catalog = @import("../../raft/storage/catalog.zig");
const db_mod = @import("../db/db.zig");
const db_core = @import("../db/core.zig");
const generation_lifecycle = @import("../db/generation_lifecycle.zig");
const backend_types = @import("../backend_types.zig");
const lsm_backend = @import("../lsm_backend.zig");
const validation = @import("validation.zig");

pub const topology_format_version: u16 = 3;
pub const topology_name = "TOPOLOGY.json";
pub const private_provisioning_name = "restore-provisioning.json";
pub const standalone_metadata_name = "standalone-metadata.bin";
pub const materialized_receipt_name = ".antfly-ha-materialized.json";
pub const max_topology_bytes: usize = 64 * 1024 * 1024;
pub const max_materialized_receipt_bytes: usize = 64 * 1024 * 1024;
pub const max_files: usize = 1_000_000;
pub const max_file_bytes: u64 = 64 * 1024 * 1024 * 1024;
const portable_auth_seed_format_version: u16 = 1;
const auth_users_namespace: backend_types.Namespace = .{ .name = "usermgr_users" };
const auth_casbin_namespace: backend_types.Namespace = .{ .name = "usermgr_casbin" };

const PortableAuthSeedEntry = struct {
    namespace: []const u8,
    key_base64: []const u8,
    value_base64: []const u8,
};
const PortableAuthSeed = struct {
    format_version: u16,
    generation: []const u8,
    entries: []const PortableAuthSeedEntry,
};

pub const LogicalCatalog = struct {
    epoch: u64,
    tables: []const topology_records.TableRecord,
    ranges: []const topology_records.RangeRecord,
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
    /// Present for the versioned logical snapshot layout. Absence preserves
    /// the released v0.2.0 store.bin/change-journal.bin seed contract.
    snapshot_manifest_sha256: ?[]const u8 = null,
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

pub const AuthArtifact = struct {
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
    auth_enabled: bool = false,
    auth_artifact: ?AuthArtifact = null,
    /// Never serialized into the public local catalog. These immutable job
    /// proofs authorize only the hidden replica roots carried by this seed.
    private_provisioning: ?@import("../../metadata/restore_staging.zig").ProvisioningProjection = null,
    /// HA-authenticated owners created after the previous seed, with no public
    /// or metadata provisioning placement on this standby.
    native_restore_tables: []const @import("restore_owner_registry.zig").OwnerTable = &.{},
    native_restore_owners: []const @import("restore_owner_registry.zig").OwnerRef = &.{},
    /// Streamed fixed-size cancellation proofs; never charged to active owners.
    restore_terminals: ?AuthArtifact = null,
    /// Complete indexed local metadata authority, including private lifecycle
    /// journals and restore jobs. Distributed metadata has its own Raft seed.
    standalone_metadata: ?AuthArtifact = null,
};

pub const MaterializeRequest = struct {
    io: std.Io = std.Options.debug_io,
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
    const io = request.io;

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

    try data_format.ensureCompatible(alloc, io, request.live_installing_root);
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

    if (parsed.value.auth_artifact) |artifact| {
        const source = try std.fs.path.join(alloc, &.{ request.raw_generation_root, artifact.path });
        defer alloc.free(source);
        const artifact_json = try readFileAlloc(io, alloc, source, @intCast(max_file_bytes));
        defer alloc.free(artifact_json);
        const auth_root = try std.fs.path.join(alloc, &.{ metadata_root, "auth" });
        defer alloc.free(auth_root);
        try materializePortableAuthSeedToPath(alloc, auth_root, request.generation, artifact_json);
    }

    const local_catalog_path = try std.fs.path.join(alloc, &.{ metadata_root, "local-metadata.json" });
    defer alloc.free(local_catalog_path);
    const local_catalog_json = try std.json.Stringify.valueAlloc(alloc, parsed.value.catalog, .{ .emit_null_optional_fields = false });
    defer alloc.free(local_catalog_json);
    try writeNewFileDurably(io, local_catalog_path, local_catalog_json);
    if (parsed.value.standalone_metadata) |artifact| {
        const source_path = try std.fs.path.join(alloc, &.{ request.raw_generation_root, artifact.path });
        defer alloc.free(source_path);
        const metadata_store_root = try std.fs.path.join(alloc, &.{ metadata_root, "local-state" });
        defer alloc.free(metadata_store_root);
        var metadata_store = try @import("../../metadata/storage/raft_apply_store.zig").RaftApplyStore.init(alloc, .{ .root_dir = metadata_store_root });
        defer metadata_store.deinit();
        try metadata_store.importHACheckpoint(io, source_path, artifact.size_bytes);
        try verifyStandaloneMetadataTopology(alloc, &metadata_store, parsed.value);
    }
    if (parsed.value.private_provisioning) |projection| {
        const private_path = try std.fs.path.join(alloc, &.{ metadata_root, private_provisioning_name });
        defer alloc.free(private_path);
        const private_json = try std.json.Stringify.valueAlloc(alloc, projection, .{});
        defer alloc.free(private_json);
        try writeNewFileDurably(io, private_path, private_json);
    }
    const native_owners = try @import("restore_owner_registry.zig").expand(alloc, parsed.value.native_restore_tables, parsed.value.native_restore_owners);
    defer alloc.free(native_owners);
    for (native_owners) |owner| try @import("restore_owner_registry.zig").record(alloc, io, metadata_root, owner);
    if (parsed.value.restore_terminals) |artifact| {
        const terminal_path = try std.fs.path.join(alloc, &.{ request.raw_generation_root, artifact.path });
        defer alloc.free(terminal_path);
        var ledger = try @import("restore_terminal_ledger.zig").Ledger.open(alloc, io, metadata_root);
        defer ledger.deinit();
        try ledger.importFile(io, terminal_path, artifact.size_bytes);
        var terminals = try ledger.snapshot();
        defer terminals.deinit();
        for (native_owners) |owner| if (try terminals.get(owner.scope.target_namespace.shard_id) != null) return error.NonCanonicalSeedTopology;
        for (parsed.value.catalog.ranges) |range| if (try terminals.get(range.group_id) != null) return error.NonCanonicalSeedTopology;
        if (parsed.value.private_provisioning) |projection| for (projection.ranges) |range| {
            if (try terminals.get(range.group_id) != null) return error.NonCanonicalSeedTopology;
        };
    }

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

        var transition = try generation_lifecycle.beginProcessExclusiveWithIo(db_path, io);
        defer transition.deinit();
        var staged = try transition.beginStaging();
        defer staged.deinit();
        try db_mod.DB.restoreCoherentHASeedReplicaToStagedGeneration(&staged, alloc, snapshot_root, staged.path(), .{
            .identity_namespace = .{
                .table_id = replica.identity_table_id,
                .shard_id = replica.identity_shard_id,
                .range_id = replica.identity_range_id,
            },
            .start_index_workers = false,
            .start_optional_runtimes = false,
        }, .{
            .table_id = replica.identity_table_id,
            .shard_id = replica.identity_shard_id,
            .range_id = replica.identity_range_id,
        });
        var native_ha_owner = false;
        for (native_owners) |owner| {
            if (owner.scope.target_namespace.shard_id != replica.group_id) continue;
            native_ha_owner = true;
            var verified = try db_mod.DB.open(alloc, staged.path(), .{ .open_mode = .query_readonly, .primary_only_readonly = true, .identity_namespace = owner.scope.target_namespace, .start_index_workers = false, .start_optional_runtimes = false });
            defer verified.close();
            var bootstrap = (try verified.readRestoreStagingBootstrap(alloc)) orelse return error.SeedReplicaIdentityMismatch;
            defer bootstrap.deinit();
            const actual = try bootstrap.value.encode(alloc);
            defer alloc.free(actual);
            const expected = try owner.encode(alloc);
            defer alloc.free(expected);
            if (!std.mem.eql(u8, actual, expected)) return error.SeedReplicaIdentityMismatch;
            var progress = (try verified.restoreStagingStatus(alloc)) orelse return error.SeedReplicaIdentityMismatch;
            defer progress.deinit();
            if (!std.mem.eql(u8, &progress.value.scope.digest(), &owner.scope.digest())) return error.SeedReplicaIdentityMismatch;
        }
        if (try staged.publish() != .durable) return error.LiveDBPublicationConflict;
        // Stream authority reconstructs an owner, not Raft membership. The
        // independent registry keeps it discoverable for replay and reseeding.
        if (native_ha_owner) continue;
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
    if (topology.value.auth_enabled) {
        const auth_root = try std.fs.path.join(alloc, &.{ live_root, "metadata/auth" });
        defer alloc.free(auth_root);
        var auth_dir = std.Io.Dir.cwd().openDir(io_impl.io(), auth_root, .{}) catch
            return error.LiveAuthStoreMissing;
        auth_dir.close(io_impl.io());
    }

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

fn verifyStandaloneMetadataTopology(alloc: Allocator, store: *@import("../../metadata/storage/raft_apply_store.zig").RaftApplyStore, topology: Topology) !void {
    const manager = @import("../../metadata/table_manager.zig");
    const group_id = @import("../../common/group_ids.zig").main_metadata_group_id;
    if (try store.standaloneRevision() != topology.catalog.epoch) return error.SeedMetadataTopologyMismatch;
    var projection = try store.captureProvisioningCatalog(alloc, group_id);
    defer projection.deinit(alloc);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const expected_tables = if (topology.private_provisioning) |private| try std.mem.concat(scratch, topology_records.TableRecord, &.{ topology.catalog.tables, private.tables }) else topology.catalog.tables;
    const expected_ranges = if (topology.private_provisioning) |private| try std.mem.concat(scratch, topology_records.RangeRecord, &.{ topology.catalog.ranges, private.ranges }) else topology.catalog.ranges;
    if (projection.tables.len != expected_tables.len or projection.ranges.len != expected_ranges.len) return error.SeedMetadataTopologyMismatch;
    var table_indices: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    var range_indices: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    for (expected_tables, 0..) |table, index| {
        const entry = try table_indices.getOrPut(scratch, table.table_id);
        if (entry.found_existing) return error.SeedMetadataTopologyMismatch;
        entry.value_ptr.* = index;
    }
    for (projection.tables) |table| {
        const index = table_indices.get(table.table_id) orelse return error.SeedMetadataTopologyMismatch;
        if (!manager.tableDefinitionsEqual(table, expected_tables[index])) return error.SeedMetadataTopologyMismatch;
    }
    for (expected_ranges, 0..) |range, index| {
        const entry = try range_indices.getOrPut(scratch, range.group_id);
        if (entry.found_existing) return error.SeedMetadataTopologyMismatch;
        entry.value_ptr.* = index;
    }
    for (projection.ranges) |range| {
        const index = range_indices.get(range.group_id) orelse return error.SeedMetadataTopologyMismatch;
        var actual = range;
        var expected = expected_ranges[index];
        // Seed descriptors make the released implicit identity explicit.
        actual.doc_identity_shard_id = manager.rangeDocIdentityShardId(actual);
        actual.doc_identity_range_id = manager.rangeDocIdentityRangeId(actual);
        expected.doc_identity_shard_id = manager.rangeDocIdentityShardId(expected);
        expected.doc_identity_range_id = manager.rangeDocIdentityRangeId(expected);
        if (!manager.rangeRecordsEqual(actual, expected)) return error.SeedMetadataTopologyMismatch;
    }
    const expected_jobs = if (topology.private_provisioning) |private| private.jobs_json else &.{};
    if (projection.jobs_json.len != expected_jobs.len) return error.SeedMetadataTopologyMismatch;
    var jobs: std.StringHashMapUnmanaged(void) = .empty;
    for (expected_jobs) |job| try jobs.put(scratch, job, {});
    for (projection.jobs_json) |job| if (!jobs.remove(job)) return error.SeedMetadataTopologyMismatch;
}

pub fn validateTopology(
    alloc: Allocator,
    io: std.Io,
    raw_root: []const u8,
    expected_generation: []const u8,
    topology: Topology,
) !void {
    var proof_arena = std.heap.ArenaAllocator.init(alloc);
    defer proof_arena.deinit();
    const proof_alloc = proof_arena.allocator();
    const private = topology.private_provisioning;
    if (private) |projection| _ = try @import("../../data/private_provisioning.zig").validate(proof_alloc, topology.catalog.tables, topology.catalog.ranges, projection);
    const placed_tables = if (private) |projection| try std.mem.concat(proof_alloc, topology_records.TableRecord, &.{ topology.catalog.tables, projection.tables }) else topology.catalog.tables;
    const placed_ranges = if (private) |projection| try std.mem.concat(proof_alloc, topology_records.RangeRecord, &.{ topology.catalog.ranges, projection.ranges }) else topology.catalog.ranges;
    const native_owners = try @import("restore_owner_registry.zig").expand(proof_alloc, topology.native_restore_tables, topology.native_restore_owners);
    const native_projection = try @import("restore_owner_registry.zig").project(proof_alloc, native_owners, topology.catalog.tables, placed_tables, placed_ranges);
    if (native_projection.owners.len != topology.native_restore_owners.len) return error.NonCanonicalSeedTopology;
    for (topology.native_restore_owners, 0..) |owner, index| if (index != 0 and topology.native_restore_owners[index - 1].scope.target_namespace.shard_id >= owner.scope.target_namespace.shard_id) return error.NonCanonicalSeedTopology;
    const all_tables = try std.mem.concat(proof_alloc, topology_records.TableRecord, &.{ placed_tables, native_projection.tables });
    const all_ranges = try std.mem.concat(proof_alloc, topology_records.RangeRecord, &.{ placed_ranges, native_projection.ranges });
    if (topology.restore_terminals) |artifact| {
        if (!std.mem.eql(u8, artifact.path, @import("restore_terminal_ledger.zig").artifact_name) or artifact.size_bytes < 8 or artifact.size_bytes > max_file_bytes or !isCanonicalSha256(artifact.sha256)) return error.InvalidRestoreTerminal;
        const path = try std.fs.path.join(proof_alloc, &.{ raw_root, artifact.path });
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size != artifact.size_bytes) return error.InvalidRestoreTerminal;
        try expectFileSha256(io, alloc, path, artifact.sha256);
    }
    if (topology.standalone_metadata) |artifact| {
        if (!std.mem.eql(u8, artifact.path, standalone_metadata_name) or artifact.size_bytes < 8 or artifact.size_bytes > max_file_bytes or !isCanonicalSha256(artifact.sha256)) return error.InvalidStandaloneMetadataCheckpoint;
        const path = try std.fs.path.join(proof_alloc, &.{ raw_root, artifact.path });
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size != artifact.size_bytes) return error.InvalidStandaloneMetadataCheckpoint;
        try expectFileSha256(io, alloc, path, artifact.sha256);
    }
    // Empty public placement is a complete state when the authenticated full
    // standalone checkpoint owns it (including a first, still-hidden restore).
    // Older catalog-only seed contracts still require populated placement.
    const empty_standalone_catalog = topology.standalone_metadata != null and topology.catalog.tables.len == 0 and topology.catalog.ranges.len == 0;
    if (topology.format_version != topology_format_version or
        !std.mem.eql(u8, topology.generation, expected_generation) or
        topology.catalog.epoch == 0 or
        (!empty_standalone_catalog and (topology.catalog.tables.len == 0 or topology.catalog.ranges.len == 0)) or
        topology.replicas.len != all_ranges.len) return error.InvalidSeedTopology;

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
        const range = findRange(all_ranges, replica.group_id) orelse return error.SeedReplicaRangeMissing;
        const table = findTable(all_tables, replica.table_id) orelse return error.SeedReplicaTableMissing;
        const expected_path = try std.fmt.allocPrint(alloc, "replicas/group-{d}", .{replica.group_id});
        defer alloc.free(expected_path);
        if (replica.group_id != range.group_id or replica.table_id != range.table_id or
            !std.mem.eql(u8, replica.table_name, table.name) or
            !std.mem.eql(u8, replica.snapshot_path, expected_path) or
            replica.identity_table_id != table.table_id or
            replica.identity_shard_id != @import("../../metadata/table_manager.zig").rangeDocIdentityShardId(range) or
            replica.identity_range_id != @import("../../metadata/table_manager.zig").rangeDocIdentityRangeId(range) or
            !isCanonicalSha256(replica.logical_sha256)) return error.SeedReplicaIdentityMismatch;
        const store_path = try std.fs.path.join(alloc, &.{ raw_root, replica.snapshot_path, "store.bin" });
        defer alloc.free(store_path);
        try expectFileSha256(io, alloc, store_path, replica.logical_sha256);
        if (replica.snapshot_manifest_sha256) |expected_sha256| {
            if (!isCanonicalSha256(expected_sha256)) return error.SeedReplicaIdentityMismatch;
            const manifest_path = try std.fs.path.join(alloc, &.{
                raw_root,
                replica.snapshot_path,
                db_core.logical_snapshot_manifest_file_name,
            });
            defer alloc.free(manifest_path);
            try expectFileSha256(io, alloc, manifest_path, expected_sha256);
        }
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

    if (topology.auth_enabled != (topology.auth_artifact != null)) return error.AuthSeedTopologyMismatch;
    if (topology.auth_artifact) |artifact| {
        if (!std.mem.eql(u8, artifact.path, "auth/auth-seed.json") or
            artifact.size_bytes == 0 or artifact.size_bytes > max_file_bytes or
            !isCanonicalSha256(artifact.sha256)) return error.InvalidAuthSeedArtifact;
        const path = try std.fs.path.join(alloc, &.{ raw_root, artifact.path });
        defer alloc.free(path);
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
        if (stat.kind != .file or stat.size != artifact.size_bytes) return error.AuthSeedArtifactMismatch;
        try expectFileSha256(io, alloc, path, artifact.sha256);
        const body = try readFileAlloc(io, alloc, path, @intCast(max_file_bytes));
        defer alloc.free(body);
        validatePortableAuthSeedBody(alloc, expected_generation, body) catch |err| switch (err) {
            error.AuthSeedGenerationMismatch => return error.AuthSeedGenerationMismatch,
            else => return error.InvalidAuthSeedArtifact,
        };
    }
}

fn materializePortableAuthSeedToPath(
    alloc: Allocator,
    target_root: []const u8,
    expected_generation: []const u8,
    artifact_json: []const u8,
) !void {
    try validatePortableAuthSeedBody(alloc, expected_generation, artifact_json);
    var parsed = std.json.parseFromSlice(PortableAuthSeed, alloc, artifact_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidPortableAuthSeed;
    defer parsed.deinit();
    var backend = try lsm_backend.BackendHandle.open(alloc, target_root, .{ .flush_threshold = 1 });
    defer backend.close();
    var store = try backend.backend.runtimeNamespaceStore(alloc);
    defer store.deinit();
    var batch = try store.beginBatch();
    errdefer batch.abort();
    for (parsed.value.entries) |entry| {
        const namespace = if (std.mem.eql(u8, entry.namespace, auth_users_namespace.name.?))
            auth_users_namespace
        else if (std.mem.eql(u8, entry.namespace, auth_casbin_namespace.name.?))
            auth_casbin_namespace
        else
            return error.InvalidPortableAuthSeed;
        const key = decodeBase64Alloc(alloc, entry.key_base64) catch return error.InvalidPortableAuthSeed;
        defer alloc.free(key);
        const value = decodeBase64Alloc(alloc, entry.value_base64) catch return error.InvalidPortableAuthSeed;
        defer alloc.free(value);
        try batch.put(namespace, key, value);
    }
    try batch.commit();
}

fn validatePortableAuthSeedBody(alloc: Allocator, expected_generation: []const u8, artifact_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(PortableAuthSeed, alloc, artifact_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidPortableAuthSeed;
    defer parsed.deinit();
    if (parsed.value.format_version != portable_auth_seed_format_version or parsed.value.entries.len == 0)
        return error.InvalidPortableAuthSeed;
    if (!std.mem.eql(u8, parsed.value.generation, expected_generation)) return error.AuthSeedGenerationMismatch;
    var previous_namespace: []const u8 = "";
    var previous_key: []const u8 = "";
    var user_count: usize = 0;
    for (parsed.value.entries) |entry| {
        const namespace = if (std.mem.eql(u8, entry.namespace, auth_users_namespace.name.?))
            auth_users_namespace
        else if (std.mem.eql(u8, entry.namespace, auth_casbin_namespace.name.?))
            auth_casbin_namespace
        else
            return error.InvalidPortableAuthSeed;
        const order = std.mem.order(u8, previous_namespace, entry.namespace);
        if (previous_namespace.len > 0 and (order == .gt or
            (order == .eq and std.mem.order(u8, previous_key, entry.key_base64) != .lt)))
            return error.NonCanonicalPortableAuthSeed;
        const key = decodeBase64Alloc(alloc, entry.key_base64) catch return error.InvalidPortableAuthSeed;
        defer alloc.free(key);
        const value = decodeBase64Alloc(alloc, entry.value_base64) catch return error.InvalidPortableAuthSeed;
        defer alloc.free(value);
        if (key.len == 0 or !validPortableAuthSeedKey(namespace, key)) return error.InvalidPortableAuthSeed;
        if (std.mem.startsWith(u8, key, "userpass:")) {
            if (key.len == "userpass:".len or value.len == 0) return error.InvalidPortableAuthSeed;
            user_count += 1;
        }
        if (std.mem.startsWith(u8, key, "usermeta:")) {
            var metadata = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch
                return error.InvalidPortableAuthSeed;
            metadata.deinit();
        }
        previous_namespace = entry.namespace;
        previous_key = entry.key_base64;
    }
    if (user_count == 0) return error.InvalidPortableAuthSeed;
}

fn validPortableAuthSeedKey(namespace: backend_types.Namespace, key: []const u8) bool {
    if (std.mem.eql(u8, namespace.name.?, auth_users_namespace.name.?)) {
        return std.mem.startsWith(u8, key, "userpass:") or
            std.mem.startsWith(u8, key, "usermeta:") or
            std.mem.startsWith(u8, key, "apikey:");
    }
    return std.mem.startsWith(u8, key, "p::") or
        std.mem.startsWith(u8, key, "p2::") or
        std.mem.startsWith(u8, key, "g::");
}

fn decodeBase64Alloc(alloc: Allocator, raw: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(raw);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, raw);
    return out;
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

fn findTable(tables: []const topology_records.TableRecord, table_id: u64) ?topology_records.TableRecord {
    for (tables) |table| if (table.table_id == table_id) return table;
    return null;
}

fn findRange(ranges: []const topology_records.RangeRecord, group_id: u64) ?topology_records.RangeRecord {
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
