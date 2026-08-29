// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Portable HA seed topology schema and validation.
//!
//! This module intentionally contains no physical database implementation.
//! Seed capture belongs to the distributed runtime, while materialization into
//! a live storage tree belongs to the compiled storage owner.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const topology_records = @import("../../common/topology_records.zig");
const extensions = @import("../../extensions/mod.zig");
const validation = @import("validation.zig");

pub const topology_format_version: u16 = 3;
pub const topology_name = "TOPOLOGY.json";
pub const logical_snapshot_manifest_name = "SNAPSHOT.json";
pub const max_topology_bytes: usize = 64 * 1024 * 1024;
pub const max_files: usize = 1_000_000;
pub const max_file_bytes: u64 = 64 * 1024 * 1024 * 1024;
pub const portable_auth_seed_format_version: u16 = 1;
pub const auth_users_namespace = "usermgr_users";
pub const auth_casbin_namespace = "usermgr_casbin";

pub const PortableAuthSeedEntry = struct {
    namespace: []const u8,
    key_base64: []const u8,
    value_base64: []const u8,
};

pub const PortableAuthSeed = struct {
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
};

pub fn validate(
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
        if (replica.snapshot_manifest_sha256) |expected_sha256| {
            if (!isCanonicalSha256(expected_sha256)) return error.SeedReplicaIdentityMismatch;
            const manifest_path = try std.fs.path.join(alloc, &.{
                raw_root,
                replica.snapshot_path,
                logical_snapshot_manifest_name,
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
        _ = findPackage(topology.catalog.extension_packages, artifact.package_name, artifact.package_version) orelse
            return error.ExtensionSeedCatalogMismatch;
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

pub fn validatePortableAuthSeedBody(alloc: Allocator, expected_generation: []const u8, artifact_json: []const u8) !void {
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
        if (!std.mem.eql(u8, entry.namespace, auth_users_namespace) and
            !std.mem.eql(u8, entry.namespace, auth_casbin_namespace)) return error.InvalidPortableAuthSeed;
        const order = std.mem.order(u8, previous_namespace, entry.namespace);
        if (previous_namespace.len > 0 and (order == .gt or
            (order == .eq and std.mem.order(u8, previous_key, entry.key_base64) != .lt)))
            return error.NonCanonicalPortableAuthSeed;
        const key = decodeBase64Alloc(alloc, entry.key_base64) catch return error.InvalidPortableAuthSeed;
        defer alloc.free(key);
        const value = decodeBase64Alloc(alloc, entry.value_base64) catch return error.InvalidPortableAuthSeed;
        defer alloc.free(value);
        if (key.len == 0 or !validPortableAuthSeedKey(entry.namespace, key)) return error.InvalidPortableAuthSeed;
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

fn validPortableAuthSeedKey(namespace: []const u8, key: []const u8) bool {
    if (std.mem.eql(u8, namespace, auth_users_namespace)) {
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
