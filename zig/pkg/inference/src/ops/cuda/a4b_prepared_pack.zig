// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Versioned, pre-sharded Gemma 4 A4B CUDA expert packs.
//!
//! A pack is an offline deployment artifact. It copies only the packed MoE
//! source tensors into balanced, sequential shard files. Admission still
//! validates the canonical GGUF catalog and matches every source by exact name
//! and byte length before using a pack. The manifest binds the pack to the
//! canonical artifact's stable file identity and records shard SHA-256 values
//! for offline verification. The hot admission path checks immutable file
//! identities and bounds instead of hashing 12+ GiB on every worker start.

const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../../io/compat.zig");
const c_file = @import("../../util/c_file.zig");

pub const schema = "antfly_gemma4_a4b_cuda_prepared_pack/v1";
pub const default_directory_name = "a4b-cuda-pack-v1";
pub const manifest_name = "manifest.json";
pub const max_shards: u8 = 8;
pub const max_manifest_bytes: usize = 4 * 1024 * 1024;

pub const GeometryIdentity = struct {
    moe_layer_count: u16,
    expert_count: u16,
    top_k: u8,
    hidden_size: u32,
    expert_intermediate_size: u32,
    encoded_expert_bytes: u64,
};

pub const SourceView = struct {
    name: []const u8,
    bytes: []const u8,
};

pub const ArtifactIdentity = struct {
    size: u64,
    inode: u64,
    mtime_seconds: i64,
    mtime_nanoseconds: u32,
    device_major: u32,
    device_minor: u32,
    quick_fingerprint_sha256: [32]u8,

    fn fromFile(value: c_file.FileIdentity) ArtifactIdentity {
        return .{
            .size = value.size,
            .inode = value.inode,
            .mtime_seconds = value.mtime_seconds,
            .mtime_nanoseconds = value.mtime_nanoseconds,
            .device_major = value.device_major,
            .device_minor = value.device_minor,
            .quick_fingerprint_sha256 = value.quick_fingerprint_sha256,
        };
    }

    fn eql(self: ArtifactIdentity, value: c_file.FileIdentity) bool {
        // Device/inode/mtime remain useful provenance, but the deployable pack
        // must survive a byte-preserving copy to another host or volume.
        return self.size == value.size and
            std.mem.eql(u8, &self.quick_fingerprint_sha256, &value.quick_fingerprint_sha256);
    }
};

const ManifestShard = struct {
    path: []const u8,
    identity: ArtifactIdentity,
    sha256: []const u8,
};

const ManifestSource = struct {
    name: []const u8,
    shard: u8,
    offset: u64,
    length: u64,
    load_order: u32,
};

const Manifest = struct {
    schema: []const u8,
    source_artifact: []const u8,
    source_identity: ArtifactIdentity,
    geometry: GeometryIdentity,
    total_source_bytes: u64,
    shards: []const ManifestShard,
    sources: []const ManifestSource,
};

pub const WriteReport = struct {
    shard_count: u8,
    source_count: usize,
    total_source_bytes: u64,
};

pub const VerifyReport = struct {
    shard_count: usize,
    source_count: usize,
    total_source_bytes: u64,
};

pub const PrefetchReport = struct {
    shard_count: usize,
    bytes: u64,
    workers: u8,
};

pub const LoadedSource = struct {
    name: []const u8,
    bytes: []const u8,
    shard: u8,
    load_order: u32,
};

pub const Loaded = struct {
    allocator: std.mem.Allocator,
    parsed: std.json.Parsed(Manifest),
    regions: []c_file.MmapRegion,
    sources: []LoadedSource,

    pub fn deinit(self: *Loaded) void {
        for (self.regions) |*region| region.deinit();
        self.allocator.free(self.regions);
        self.allocator.free(self.sources);
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn preserveFileCacheOnDeinit(self: *Loaded) void {
        for (self.regions) |*region| region.preserveFileCacheOnDeinit();
    }
};

var temp_sequence: std.atomic.Value(u64) = .init(0);

fn validRelativeFileName(path: []const u8) bool {
    if (path.len == 0 or std.mem.eql(u8, path, ".") or std.mem.eql(u8, path, "..")) return false;
    return std.mem.indexOfAny(u8, path, "/\\") == null;
}

fn geometryEqual(lhs: GeometryIdentity, rhs: GeometryIdentity) bool {
    return std.meta.eql(lhs, rhs);
}

fn shardName(buffer: *[32]u8, index: usize) ![]const u8 {
    return std.fmt.bufPrint(buffer, "experts-{d:0>2}.bin", .{index});
}

fn digestHex(bytes: []const u8) [64]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

fn syncDirectoryPath(io: std.Io, path: []const u8) !void {
    switch (builtin.os.tag) {
        .linux, .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => {
            var dir = try compat.cwd().openDir(io, path, .{ .iterate = true });
            defer dir.close(io);
            const file: std.Io.File = .{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
            try file.sync(io);
        },
        else => {},
    }
}

pub fn write(
    allocator: std.mem.Allocator,
    io: std.Io,
    source_artifact_path: []const u8,
    output_path: []const u8,
    geometry: GeometryIdentity,
    sources: []const SourceView,
    requested_shards: u8,
) !WriteReport {
    if (sources.len == 0) return error.A4bPreparedPackHasNoSources;
    if (requested_shards == 0 or requested_shards > max_shards)
        return error.A4bPreparedPackInvalidShardCount;
    if (c_file.fileExists(allocator, output_path)) return error.A4bPreparedPackAlreadyExists;

    const source_identity = try c_file.fileIdentity(allocator, source_artifact_path);
    const source_basename = std.fs.path.basename(source_artifact_path);
    if (!validRelativeFileName(source_basename)) return error.A4bPreparedPackInvalidSourceArtifact;

    const shard_count: usize = @min(@as(usize, requested_shards), sources.len);
    const assignments = try allocator.alloc(u8, sources.len);
    defer allocator.free(assignments);
    const shard_sizes = try allocator.alloc(u64, shard_count);
    defer allocator.free(shard_sizes);
    @memset(shard_sizes, 0);
    var total_source_bytes: u64 = 0;
    for (sources, 0..) |source, source_index| {
        if (source.name.len == 0 or source.bytes.len == 0) return error.A4bPreparedPackInvalidSource;
        for (sources[0..source_index]) |previous| {
            if (std.mem.eql(u8, source.name, previous.name)) return error.A4bPreparedPackDuplicateSource;
        }
        var smallest: usize = 0;
        for (shard_sizes[1..], 1..) |size, shard_index| {
            if (size < shard_sizes[smallest]) smallest = shard_index;
        }
        assignments[source_index] = @intCast(smallest);
        shard_sizes[smallest] = try std.math.add(u64, shard_sizes[smallest], source.bytes.len);
        total_source_bytes = try std.math.add(u64, total_source_bytes, source.bytes.len);
    }

    const tmp_path = try std.fmt.allocPrint(
        allocator,
        "{s}.tmp-{d}-{d}",
        .{ output_path, std.posix.system.getpid(), temp_sequence.fetchAdd(1, .monotonic) },
    );
    defer allocator.free(tmp_path);
    try compat.cwd().createDir(io, tmp_path, .default_dir);
    var committed = false;
    defer if (!committed) compat.cwd().deleteTree(io, tmp_path) catch {};

    const shard_manifest = try allocator.alloc(ManifestShard, shard_count);
    defer allocator.free(shard_manifest);
    const shard_names = try allocator.alloc([32]u8, shard_count);
    defer allocator.free(shard_names);
    const shard_name_lengths = try allocator.alloc(usize, shard_count);
    defer allocator.free(shard_name_lengths);
    const shard_digests = try allocator.alloc([64]u8, shard_count);
    defer allocator.free(shard_digests);
    const source_manifest = try allocator.alloc(ManifestSource, sources.len);
    defer allocator.free(source_manifest);

    var offsets = try allocator.alloc(u64, shard_count);
    defer allocator.free(offsets);
    @memset(offsets, 0);
    for (sources, 0..) |source, index| {
        const shard = assignments[index];
        source_manifest[index] = .{
            .name = source.name,
            .shard = shard,
            .offset = offsets[shard],
            .length = source.bytes.len,
            .load_order = @intCast(index),
        };
        offsets[shard] += source.bytes.len;
    }

    for (0..shard_count) |shard_index| {
        const name = try shardName(&shard_names[shard_index], shard_index);
        shard_name_lengths[shard_index] = name.len;
        const path = try std.fs.path.join(allocator, &.{ tmp_path, name });
        defer allocator.free(path);
        var file = try compat.cwd().createFile(io, path, .{ .exclusive = true, .truncate = false });
        var open = true;
        defer if (open) file.close(io);
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        for (sources, assignments) |source, assigned| {
            if (assigned != shard_index) continue;
            try file.writeStreamingAll(io, source.bytes);
            hasher.update(source.bytes);
        }
        try file.sync(io);
        file.close(io);
        open = false;
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        shard_digests[shard_index] = std.fmt.bytesToHex(digest, .lower);
        shard_manifest[shard_index] = .{
            .path = shard_names[shard_index][0..shard_name_lengths[shard_index]],
            .identity = ArtifactIdentity.fromFile(try c_file.fileIdentity(allocator, path)),
            .sha256 = &shard_digests[shard_index],
        };
    }

    const manifest = Manifest{
        .schema = schema,
        .source_artifact = source_basename,
        .source_identity = ArtifactIdentity.fromFile(source_identity),
        .geometry = geometry,
        .total_source_bytes = total_source_bytes,
        .shards = shard_manifest,
        .sources = source_manifest,
    };
    const rendered = try std.json.Stringify.valueAlloc(allocator, manifest, .{ .whitespace = .indent_2 });
    defer allocator.free(rendered);
    const manifest_path = try std.fs.path.join(allocator, &.{ tmp_path, manifest_name });
    defer allocator.free(manifest_path);
    var manifest_file = try compat.cwd().createFile(io, manifest_path, .{ .exclusive = true, .truncate = false });
    var manifest_open = true;
    defer if (manifest_open) manifest_file.close(io);
    try manifest_file.writeStreamingAll(io, rendered);
    try manifest_file.sync(io);
    manifest_file.close(io);
    manifest_open = false;

    try syncDirectoryPath(io, tmp_path);
    try c_file.renameNoReplace(allocator, tmp_path, output_path);
    try syncDirectoryPath(io, std.fs.path.dirname(output_path) orelse ".");
    committed = true;
    return .{
        .shard_count = @intCast(shard_count),
        .source_count = sources.len,
        .total_source_bytes = total_source_bytes,
    };
}

fn validateManifest(manifest: Manifest, expected_geometry: GeometryIdentity) !void {
    if (!std.mem.eql(u8, manifest.schema, schema)) return error.A4bPreparedPackSchemaMismatch;
    if (!validRelativeFileName(manifest.source_artifact)) return error.A4bPreparedPackInvalidSourceArtifact;
    if (!geometryEqual(manifest.geometry, expected_geometry)) return error.A4bPreparedPackGeometryMismatch;
    if (manifest.shards.len == 0 or manifest.shards.len > max_shards) return error.A4bPreparedPackInvalidShardCount;
    if (manifest.sources.len == 0) return error.A4bPreparedPackHasNoSources;
    var total: u64 = 0;
    for (manifest.shards, 0..) |shard, index| {
        if (!validRelativeFileName(shard.path) or shard.sha256.len != 64)
            return error.A4bPreparedPackInvalidShard;
        for (manifest.shards[0..index]) |previous| {
            if (std.mem.eql(u8, shard.path, previous.path)) return error.A4bPreparedPackDuplicateShard;
        }
    }
    for (manifest.sources, 0..) |source, index| {
        if (source.name.len == 0 or source.length == 0 or source.shard >= manifest.shards.len)
            return error.A4bPreparedPackInvalidSource;
        const end = try std.math.add(u64, source.offset, source.length);
        if (end > manifest.shards[source.shard].identity.size) return error.A4bPreparedPackSourceOutOfBounds;
        for (manifest.sources[0..index]) |previous| {
            if (std.mem.eql(u8, source.name, previous.name)) return error.A4bPreparedPackDuplicateSource;
            if (source.load_order == previous.load_order) return error.A4bPreparedPackDuplicateLoadOrder;
            if (source.shard != previous.shard) continue;
            const previous_end = try std.math.add(u64, previous.offset, previous.length);
            if (source.offset < previous_end and previous.offset < end)
                return error.A4bPreparedPackOverlappingSources;
        }
        total = try std.math.add(u64, total, source.length);
    }
    if (total != manifest.total_source_bytes) return error.A4bPreparedPackTotalSizeMismatch;
}

/// Validate an installed pack before constructing the native model session or
/// allocating CUDA memory. This intentionally stops short of mmaping shard
/// payloads: the hot loader repeats these inexpensive identity checks and then
/// validates the manifest's exact source inventory against the canonical GGUF
/// catalog before consuming any bytes.
pub fn preflightInstalled(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    source_artifact_path: []const u8,
    expected_geometry: GeometryIdentity,
) !bool {
    const pack_path = try std.fs.path.join(allocator, &.{ model_path, default_directory_name });
    defer allocator.free(pack_path);
    const manifest_path = try std.fs.path.join(allocator, &.{ pack_path, manifest_name });
    defer allocator.free(manifest_path);
    if (!c_file.fileExists(allocator, manifest_path)) return false;

    const raw = try c_file.readFileMax(allocator, manifest_path, max_manifest_bytes);
    defer allocator.free(raw);
    var parsed = std.json.parseFromSlice(Manifest, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.A4bPreparedPackInvalidManifest;
    defer parsed.deinit();
    try validateManifest(parsed.value, expected_geometry);

    const source_identity = try c_file.fileIdentity(allocator, source_artifact_path);
    if (!parsed.value.source_identity.eql(source_identity) or
        !std.mem.eql(u8, parsed.value.source_artifact, std.fs.path.basename(source_artifact_path)))
    {
        return error.A4bPreparedPackSourceIdentityMismatch;
    }
    for (parsed.value.shards) |shard| {
        const path = try std.fs.path.join(allocator, &.{ pack_path, shard.path });
        defer allocator.free(path);
        const identity = try c_file.fileIdentity(allocator, path);
        if (!shard.identity.eql(identity)) return error.A4bPreparedPackShardIdentityMismatch;
    }
    return true;
}

pub fn load(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    source_artifact_path: []const u8,
    expected_geometry: GeometryIdentity,
) !?Loaded {
    const pack_path = try std.fs.path.join(allocator, &.{ model_path, default_directory_name });
    defer allocator.free(pack_path);
    const manifest_path = try std.fs.path.join(allocator, &.{ pack_path, manifest_name });
    defer allocator.free(manifest_path);
    if (!c_file.fileExists(allocator, manifest_path)) return null;
    const raw = try c_file.readFileMax(allocator, manifest_path, max_manifest_bytes);
    defer allocator.free(raw);
    var parsed = std.json.parseFromSlice(Manifest, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.A4bPreparedPackInvalidManifest;
    errdefer parsed.deinit();
    try validateManifest(parsed.value, expected_geometry);
    const source_identity = try c_file.fileIdentity(allocator, source_artifact_path);
    if (!parsed.value.source_identity.eql(source_identity) or
        !std.mem.eql(u8, parsed.value.source_artifact, std.fs.path.basename(source_artifact_path)))
    {
        return error.A4bPreparedPackSourceIdentityMismatch;
    }

    const regions = try allocator.alloc(c_file.MmapRegion, parsed.value.shards.len);
    errdefer allocator.free(regions);
    var mapped: usize = 0;
    errdefer for (regions[0..mapped]) |*region| region.deinit();
    for (parsed.value.shards, 0..) |shard, index| {
        const path = try std.fs.path.join(allocator, &.{ pack_path, shard.path });
        defer allocator.free(path);
        const identity = try c_file.fileIdentity(allocator, path);
        if (!shard.identity.eql(identity)) return error.A4bPreparedPackShardIdentityMismatch;
        regions[index] = try c_file.MmapRegion.init(allocator, path);
        regions[index].adviseRandom();
        mapped += 1;
    }

    const sources = try allocator.alloc(LoadedSource, parsed.value.sources.len);
    errdefer allocator.free(sources);
    for (parsed.value.sources, 0..) |source, index| {
        const offset: usize = @intCast(source.offset);
        const len: usize = @intCast(source.length);
        sources[index] = .{
            .name = source.name,
            .bytes = regions[source.shard].data[offset..][0..len],
            .shard = source.shard,
            .load_order = source.load_order,
        };
    }
    return .{
        .allocator = allocator,
        .parsed = parsed,
        .regions = regions,
        .sources = sources,
    };
}

/// Full offline integrity verification. Admission intentionally uses stable
/// identities and structural validation so it does not double-read the model;
/// deployment pipelines should run this once after copying a pack.
pub fn verify(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    source_artifact_path: []const u8,
    expected_geometry: GeometryIdentity,
) !VerifyReport {
    var loaded = (try load(allocator, model_path, source_artifact_path, expected_geometry)) orelse
        return error.A4bPreparedPackRequired;
    defer loaded.deinit();
    for (loaded.regions, loaded.parsed.value.shards) |region, shard| {
        const actual = digestHex(region.data);
        if (!std.ascii.eqlIgnoreCase(&actual, shard.sha256)) return error.A4bPreparedPackChecksumMismatch;
    }
    return .{
        .shard_count = loaded.regions.len,
        .source_count = loaded.sources.len,
        .total_source_bytes = loaded.parsed.value.total_source_bytes,
    };
}

/// Populate the shared kernel page cache from installed prepared shards. At
/// most `requested_workers` independent read streams run concurrently across
/// all shards; this operation intentionally retains no userspace copies.
pub fn prefetchInstalled(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    source_artifact_path: []const u8,
    requested_workers: u8,
) !?PrefetchReport {
    const pack_path = try std.fs.path.join(allocator, &.{ model_path, default_directory_name });
    defer allocator.free(pack_path);
    const manifest_path = try std.fs.path.join(allocator, &.{ pack_path, manifest_name });
    defer allocator.free(manifest_path);
    if (!c_file.fileExists(allocator, manifest_path)) return null;
    const raw = try c_file.readFileMax(allocator, manifest_path, max_manifest_bytes);
    defer allocator.free(raw);
    var parsed = std.json.parseFromSlice(Manifest, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.A4bPreparedPackInvalidManifest;
    defer parsed.deinit();
    try validateManifest(parsed.value, parsed.value.geometry);
    const source_identity = try c_file.fileIdentity(allocator, source_artifact_path);
    if (!parsed.value.source_identity.eql(source_identity) or
        !std.mem.eql(u8, parsed.value.source_artifact, std.fs.path.basename(source_artifact_path)))
    {
        return error.A4bPreparedPackSourceIdentityMismatch;
    }

    const requested = std.math.clamp(requested_workers, 1, max_shards);
    const owned_paths = try allocator.alloc([]u8, parsed.value.shards.len);
    defer allocator.free(owned_paths);
    var path_count: usize = 0;
    defer for (owned_paths[0..path_count]) |path| allocator.free(path);
    for (parsed.value.shards, 0..) |shard, index| {
        owned_paths[index] = try std.fs.path.join(allocator, &.{ pack_path, shard.path });
        path_count += 1;
        const identity = try c_file.fileIdentity(allocator, owned_paths[index]);
        if (!shard.identity.eql(identity)) return error.A4bPreparedPackShardIdentityMismatch;
    }

    const workers: usize = @min(@as(usize, requested), owned_paths.len);
    var next_path: std.atomic.Value(usize) = .init(0);
    const State = struct {
        allocator: std.mem.Allocator,
        paths: []const []u8,
        next_path: *std.atomic.Value(usize),
        bytes: u64 = 0,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            while (true) {
                const index = self.next_path.fetchAdd(1, .monotonic);
                if (index >= self.paths.len) return;
                const result = c_file.prefetchFile(self.allocator, self.paths[index], 1) catch |err| {
                    self.failure = err;
                    return;
                };
                self.bytes = std.math.add(u64, self.bytes, result.bytes) catch {
                    self.failure = error.Overflow;
                    return;
                };
            }
        }
    };
    const states = try allocator.alloc(State, workers);
    defer allocator.free(states);
    const threads = try allocator.alloc(std.Thread, workers);
    defer allocator.free(threads);
    var spawned: usize = 0;
    defer for (threads[0..spawned]) |thread| thread.join();
    for (states, 0..) |*state, index| {
        state.* = .{ .allocator = allocator, .paths = owned_paths, .next_path = &next_path };
        threads[index] = try std.Thread.spawn(.{}, State.run, .{state});
        spawned += 1;
    }
    for (threads[0..spawned]) |thread| thread.join();
    spawned = 0;
    var bytes: u64 = 0;
    for (states) |state| {
        if (state.failure) |err| return err;
        bytes = try std.math.add(u64, bytes, state.bytes);
    }
    return .{ .shard_count = owned_paths.len, .bytes = bytes, .workers = @intCast(workers) };
}

test "prepared pack validates source identity geometry bounds and shards" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(root);
    const source_path = try std.fs.path.join(allocator, &.{ root, "model.gguf" });
    defer allocator.free(source_path);
    try compat.cwd().writeFile(std.testing.io, .{ .sub_path = source_path, .data = "canonical-source" });
    const output_path = try std.fs.path.join(allocator, &.{ root, default_directory_name });
    defer allocator.free(output_path);
    const geometry = GeometryIdentity{
        .moe_layer_count = 2,
        .expert_count = 4,
        .top_k = 2,
        .hidden_size = 8,
        .expert_intermediate_size = 4,
        .encoded_expert_bytes = 16,
    };
    try std.testing.expect(!(try preflightInstalled(allocator, root, source_path, geometry)));
    const a = "abcdefgh";
    const b = "ijklmnopqr";
    const report = try write(allocator, std.testing.io, source_path, output_path, geometry, &.{
        .{ .name = "layer.0.w13", .bytes = a },
        .{ .name = "layer.0.w2", .bytes = b },
    }, 2);
    try std.testing.expectEqual(@as(u8, 2), report.shard_count);
    try std.testing.expectEqual(@as(u64, a.len + b.len), report.total_source_bytes);
    try std.testing.expectError(error.A4bPreparedPackAlreadyExists, write(
        allocator,
        std.testing.io,
        source_path,
        output_path,
        geometry,
        &.{.{ .name = "duplicate", .bytes = a }},
        1,
    ));
    try std.testing.expect(try preflightInstalled(allocator, root, source_path, geometry));

    var loaded = (try load(allocator, root, source_path, geometry)).?;
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), loaded.sources.len);
    try std.testing.expectEqualStrings(a, loaded.sources[0].bytes);
    try std.testing.expectEqualStrings(b, loaded.sources[1].bytes);
    var wrong = geometry;
    wrong.expert_count += 1;
    try std.testing.expectError(error.A4bPreparedPackGeometryMismatch, load(allocator, root, source_path, wrong));
    const verified = try verify(allocator, root, source_path, geometry);
    try std.testing.expectEqual(@as(usize, 2), verified.shard_count);
    const prefetched = (try prefetchInstalled(allocator, root, source_path, 4)).?;
    try std.testing.expectEqual(@as(usize, 2), prefetched.shard_count);
    try std.testing.expectEqual(@as(u64, a.len + b.len), prefetched.bytes);
}

test "prepared pack digest helper is stable" {
    try std.testing.expectEqualStrings(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        &digestHex("abc"),
    );
}
