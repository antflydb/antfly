// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const fs_paths = @import("../../common/fs_paths.zig");
const validation = @import("validation.zig");

pub const marker_name = ".antfly-ha-local-gc-eligible.json";
pub const generations_dir_name = "generations";
const tombstone_prefix = ".gc-";
const schema_version: u16 = 1;

/// Local deletion authority is deliberately split by lifecycle boundary. A
/// remote artifact prune is never evidence that either local scope is safe.
pub const Scope = enum {
    source_capture,
    target_activation,
};

pub const MarkRequest = struct {
    root: []const u8,
    scope: Scope,
    generation: []const u8,
    slot_name: []const u8,
    checkpoint_lsn: u64,
    /// The already-durable lifecycle receipt. Only its digest is retained in
    /// the eligibility marker; callers must validate the typed receipt first.
    checkpoint_bytes: []const u8,
    max_checkpoint_bytes: usize = 1024 * 1024,
};

pub const PruneRequest = struct {
    root: []const u8,
    scope: Scope,
    slot_name: []const u8,
    current_generation: []const u8,
    protected_generations: []const []const u8 = &.{},
    retain_generations: usize = 2,
    max_entries: usize = 10_000,
    max_marker_bytes: usize = 1024 * 1024,
    /// Optional sibling generation tree that must be deleted before its raw
    /// generation. Target activation uses this for mutable live-generations.
    /// Deleting the mutable side first guarantees a crash can leave only a raw
    /// orphan that remains eligible and retryable, never an unowned live tree.
    paired_generations_dir_name: ?[]const u8 = null,
};

pub const PruneResult = struct {
    result_json: []u8,
    deleted_generations: usize,
    retained_generations: usize,
    resumed_tombstones: usize,
    skipped_ineligible: usize,

    pub fn deinit(self: *PruneResult, alloc: Allocator) void {
        alloc.free(self.result_json);
        self.* = undefined;
    }
};

const PruneOptions = struct {
    /// Test-only crash boundary after the durable rename and before deletion.
    fail_after_tombstones: ?usize = null,
    /// Test-only crash boundary after durable paired-tree deletion and before
    /// the raw generation is renamed to its tombstone.
    fail_after_paired_deletions: ?usize = null,
};

const EligibilityMarker = struct {
    schema_version: u16 = schema_version,
    scope: Scope,
    generation: []const u8,
    slot_name: []const u8,
    checkpoint_lsn: u64,
    checkpoint_sha256: []const u8,
};

const Candidate = struct {
    generation: []u8,
    checkpoint_lsn: u64,
    checkpoint_sha256: []u8,

    fn deinit(self: *Candidate, alloc: Allocator) void {
        alloc.free(self.generation);
        alloc.free(self.checkpoint_sha256);
        self.* = undefined;
    }
};

const Tombstone = struct {
    name: []u8,

    fn deinit(self: *Tombstone, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

const OwnedMarker = struct {
    scope: Scope,
    slot_name: []u8,
    checkpoint_lsn: u64,
    checkpoint_sha256: []u8,

    fn deinit(self: *OwnedMarker, alloc: Allocator) void {
        alloc.free(self.slot_name);
        alloc.free(self.checkpoint_sha256);
        self.* = undefined;
    }
};

const PruneReceipt = struct {
    schema_version: u16 = schema_version,
    action_kind: []const u8 = "gc_local_seed_generations",
    scope: Scope,
    slot_name: []const u8,
    current_generation: []const u8,
    checkpoint_sha256: []const u8,
    retained_generations: usize,
    protected_generations: usize,
    deleted_generations: usize,
    resumed_tombstones: usize,
    skipped_ineligible: usize,
};

/// Records a typed lifecycle checkpoint as immutable local deletion authority.
///
/// This primitive intentionally does not infer safety from directory age or
/// remote prune state. Source callers must first verify a durable v2 artifact
/// COMPLETE receipt; target callers must first verify the durable slot
/// activation receipt against ACTIVE.json.
pub fn markEligible(alloc: Allocator, request: MarkRequest) !void {
    try validateRootAndIdentity(request.root, request.slot_name, request.generation);
    if (request.checkpoint_lsn == 0) return error.InvalidLocalGCCheckpoint;
    if (request.checkpoint_bytes.len == 0 or request.checkpoint_bytes.len > request.max_checkpoint_bytes)
        return error.InvalidLocalGCCheckpoint;

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const generation_root = try generationPath(alloc, request.root, request.generation);
    defer alloc.free(generation_root);
    const generation_stat = std.Io.Dir.cwd().statFile(io, generation_root, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return error.LocalGCGenerationNotFound,
        else => return err,
    };
    if (generation_stat.kind != .directory) return error.UnsafeLocalGCGeneration;

    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(request.checkpoint_bytes, &digest, .{});
    var digest_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&digest_hex, &digest);
    const marker_json = try std.json.Stringify.valueAlloc(alloc, EligibilityMarker{
        .scope = request.scope,
        .generation = request.generation,
        .slot_name = request.slot_name,
        .checkpoint_lsn = request.checkpoint_lsn,
        .checkpoint_sha256 = &digest_hex,
    }, .{});
    defer alloc.free(marker_json);

    const marker_path = try std.fs.path.join(alloc, &.{ generation_root, marker_name });
    defer alloc.free(marker_path);
    _ = try writeImmutableFile(io, alloc, marker_path, marker_json);
}

pub fn prune(alloc: Allocator, request: PruneRequest) !PruneResult {
    return pruneWithOptions(alloc, request, .{});
}

fn pruneWithOptions(alloc: Allocator, request: PruneRequest, options: PruneOptions) !PruneResult {
    try validateRootAndIdentity(request.root, request.slot_name, request.current_generation);
    if (request.retain_generations == 0) return error.InvalidLocalGCRetention;
    if (request.max_entries == 0 or request.max_marker_bytes == 0) return error.InvalidLocalGCLimit;
    if (request.paired_generations_dir_name) |dir_name| {
        if (request.scope != .target_activation or !validation.isIdentifier(dir_name) or
            std.mem.eql(u8, dir_name, generations_dir_name)) return error.InvalidPairedLocalGCRoot;
    }
    if (request.protected_generations.len > request.max_entries) return error.TooManyLocalGCEntries;
    for (request.protected_generations, 0..) |generation, index| {
        if (!validation.isIdentifier(generation)) return error.InvalidSeedGeneration;
        for (request.protected_generations[0..index]) |prior| {
            if (std.mem.eql(u8, prior, generation)) return error.DuplicateProtectedGeneration;
        }
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const generations_root = try std.fs.path.join(alloc, &.{ request.root, generations_dir_name });
    defer alloc.free(generations_root);
    const paired_generations_root = if (request.paired_generations_dir_name) |dir_name|
        try std.fs.path.join(alloc, &.{ request.root, dir_name })
    else
        null;
    defer if (paired_generations_root) |root| alloc.free(root);

    if (paired_generations_root) |root| {
        const paired_stat = std.Io.Dir.cwd().statFile(io, root, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return error.PairedLocalGCRootMissing,
            else => return err,
        };
        if (paired_stat.kind != .directory) return error.InvalidPairedLocalGCRoot;
    }

    var dir = std.Io.Dir.cwd().openDir(io, generations_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return error.CurrentGenerationNotEligible,
        error.NotDir => return error.UnsafeLocalGCRoot,
        else => return err,
    };
    defer dir.close(io);

    var candidates = std.ArrayListUnmanaged(Candidate).empty;
    defer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }
    var tombstones = std.ArrayListUnmanaged(Tombstone).empty;
    defer {
        for (tombstones.items) |*tombstone| tombstone.deinit(alloc);
        tombstones.deinit(alloc);
    }

    var entry_count: usize = 0;
    var skipped_ineligible: usize = 0;
    var found_current = false;
    var iterator = dir.iterateAssumeFirstIteration();
    while (try iterator.next(io)) |entry| {
        entry_count += 1;
        if (entry_count > request.max_entries) return error.TooManyLocalGCEntries;
        if (entry.kind != .directory) {
            skipped_ineligible += 1;
            continue;
        }

        if (std.mem.startsWith(u8, entry.name, tombstone_prefix)) {
            const generation = entry.name[tombstone_prefix.len..];
            if (!validation.isIdentifier(generation)) return error.InvalidLocalGCTombstone;
            var marker = try readAndValidateMarker(alloc, io, generations_root, entry.name, generation, request.max_marker_bytes);
            defer marker.deinit(alloc);
            if (marker.scope != request.scope or !std.mem.eql(u8, marker.slot_name, request.slot_name)) continue;
            const name = try alloc.dupe(u8, entry.name);
            errdefer alloc.free(name);
            try tombstones.append(alloc, .{
                .name = name,
            });
            continue;
        }
        if (!validation.isIdentifier(entry.name)) {
            skipped_ineligible += 1;
            continue;
        }

        var marker = readAndValidateMarker(alloc, io, generations_root, entry.name, entry.name, request.max_marker_bytes) catch |err| switch (err) {
            error.FileNotFound => {
                skipped_ineligible += 1;
                continue;
            },
            else => return err,
        };
        defer marker.deinit(alloc);
        if (marker.scope != request.scope or !std.mem.eql(u8, marker.slot_name, request.slot_name)) {
            skipped_ineligible += 1;
            continue;
        }
        const generation = try alloc.dupe(u8, entry.name);
        errdefer alloc.free(generation);
        const checkpoint_sha256 = try alloc.dupe(u8, marker.checkpoint_sha256);
        errdefer alloc.free(checkpoint_sha256);
        try candidates.append(alloc, .{
            .generation = generation,
            .checkpoint_lsn = marker.checkpoint_lsn,
            .checkpoint_sha256 = checkpoint_sha256,
        });
        if (std.mem.eql(u8, entry.name, request.current_generation)) found_current = true;
    }
    if (!found_current) return error.CurrentGenerationNotEligible;

    std.mem.sort(Candidate, candidates.items, {}, newerCandidate);

    var resumed_tombstones: usize = 0;
    for (tombstones.items) |tombstone| {
        if (paired_generations_root) |paired_root| {
            const generation = tombstone.name[tombstone_prefix.len..];
            _ = try deletePairedGenerationIfPresent(alloc, io, paired_root, generation);
        }
        const path = try std.fs.path.join(alloc, &.{ generations_root, tombstone.name });
        defer alloc.free(path);
        try std.Io.Dir.cwd().deleteTree(io, path);
        try fs_paths.syncDirPortable(io, generations_root);
        resumed_tombstones += 1;
    }

    var deleted_generations: usize = 0;
    var tombstones_created: usize = 0;
    var paired_deletions: usize = 0;
    for (candidates.items, 0..) |candidate, index| {
        if (index < request.retain_generations or
            std.mem.eql(u8, candidate.generation, request.current_generation) or
            isProtected(request.protected_generations, candidate.generation))
        {
            continue;
        }

        const source_path = try std.fs.path.join(alloc, &.{ generations_root, candidate.generation });
        defer alloc.free(source_path);
        const tombstone_name = try std.fmt.allocPrint(alloc, "{s}{s}", .{ tombstone_prefix, candidate.generation });
        defer alloc.free(tombstone_name);
        const tombstone_path = try std.fs.path.join(alloc, &.{ generations_root, tombstone_name });
        defer alloc.free(tombstone_path);

        if (paired_generations_root) |paired_root| {
            if (try deletePairedGenerationIfPresent(alloc, io, paired_root, candidate.generation)) {
                paired_deletions += 1;
                if (options.fail_after_paired_deletions) |limit| {
                    if (paired_deletions >= limit) return error.InjectedPairedLocalGCFailure;
                }
            }
        }

        std.Io.Dir.rename(std.Io.Dir.cwd(), source_path, std.Io.Dir.cwd(), tombstone_path, io) catch |err| switch (err) {
            error.FileNotFound => return error.LocalGCConcurrentMutation,
            error.DirNotEmpty => return error.LocalGCTombstoneConflict,
            else => return err,
        };
        try fs_paths.syncDirPortable(io, generations_root);
        tombstones_created += 1;
        if (options.fail_after_tombstones) |limit| {
            if (tombstones_created >= limit) return error.InjectedLocalGCFailure;
        }

        try std.Io.Dir.cwd().deleteTree(io, tombstone_path);
        try fs_paths.syncDirPortable(io, generations_root);
        deleted_generations += 1;
    }

    const retained_generations = candidates.items.len - deleted_generations;
    const current_checkpoint_sha256 = for (candidates.items) |candidate| {
        if (std.mem.eql(u8, candidate.generation, request.current_generation)) break candidate.checkpoint_sha256;
    } else unreachable;
    const result_json = try std.json.Stringify.valueAlloc(alloc, PruneReceipt{
        .scope = request.scope,
        .slot_name = request.slot_name,
        .current_generation = request.current_generation,
        .checkpoint_sha256 = current_checkpoint_sha256,
        .retained_generations = retained_generations,
        .protected_generations = request.protected_generations.len,
        .deleted_generations = deleted_generations,
        .resumed_tombstones = resumed_tombstones,
        .skipped_ineligible = skipped_ineligible,
    }, .{});
    return .{
        .result_json = result_json,
        .deleted_generations = deleted_generations,
        .retained_generations = retained_generations,
        .resumed_tombstones = resumed_tombstones,
        .skipped_ineligible = skipped_ineligible,
    };
}

fn deletePairedGenerationIfPresent(
    alloc: Allocator,
    io: std.Io,
    paired_root: []const u8,
    generation: []const u8,
) !bool {
    const path = try std.fs.path.join(alloc, &.{ paired_root, generation });
    defer alloc.free(path);
    const stat = std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    if (stat.kind != .directory) return error.UnsafePairedLocalGCGeneration;
    try std.Io.Dir.cwd().deleteTree(io, path);
    try fs_paths.syncDirPortable(io, paired_root);
    return true;
}

fn validateRootAndIdentity(root: []const u8, slot_name: []const u8, generation: []const u8) !void {
    if (!validation.isAbsoluteNormalizedPath(root)) return error.InvalidLocalGCRoot;
    if (!validation.isIdentifier(slot_name)) return error.InvalidSlotName;
    if (!validation.isIdentifier(generation)) return error.InvalidSeedGeneration;
}

fn readAndValidateMarker(
    alloc: Allocator,
    io: std.Io,
    generations_root: []const u8,
    directory_name: []const u8,
    expected_generation: []const u8,
    max_bytes: usize,
) !OwnedMarker {
    const marker_path = try std.fs.path.join(alloc, &.{ generations_root, directory_name, marker_name });
    defer alloc.free(marker_path);
    const stat = std.Io.Dir.cwd().statFile(io, marker_path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return error.FileNotFound,
        else => return err,
    };
    if (stat.kind != .file or stat.size > max_bytes) return error.InvalidLocalGCMarker;
    const raw = std.Io.Dir.cwd().readFileAlloc(io, marker_path, alloc, .limited(max_bytes)) catch return error.InvalidLocalGCMarker;
    defer alloc.free(raw);
    var parsed = std.json.parseFromSlice(EligibilityMarker, alloc, raw, .{ .ignore_unknown_fields = false }) catch
        return error.InvalidLocalGCMarker;
    defer parsed.deinit();
    const marker = parsed.value;
    if (marker.schema_version != schema_version or
        !validation.isIdentifier(marker.generation) or
        !validation.isIdentifier(marker.slot_name) or
        !std.mem.eql(u8, marker.generation, expected_generation) or
        marker.checkpoint_lsn == 0 or
        marker.checkpoint_sha256.len != Sha256.digest_length * 2 or
        !isLowerHex(marker.checkpoint_sha256))
    {
        return error.InvalidLocalGCMarker;
    }
    const slot_name = try alloc.dupe(u8, marker.slot_name);
    errdefer alloc.free(slot_name);
    const checkpoint_sha256 = try alloc.dupe(u8, marker.checkpoint_sha256);
    return .{
        .scope = marker.scope,
        .slot_name = slot_name,
        .checkpoint_lsn = marker.checkpoint_lsn,
        .checkpoint_sha256 = checkpoint_sha256,
    };
}

fn writeImmutableFile(io: std.Io, alloc: Allocator, path: []const u8, body: []const u8) !bool {
    var atomic_file = try std.Io.Dir.cwd().createFileAtomic(io, path, .{ .make_path = false, .replace = false });
    defer atomic_file.deinit(io);
    try atomic_file.file.writeStreamingAll(io, body);
    try atomic_file.file.sync(io);
    atomic_file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => {
            const existing = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(@max(body.len +| 1, 1024 * 1024))) catch
                return error.LocalGCEligibilityConflict;
            defer alloc.free(existing);
            if (!std.mem.eql(u8, existing, body)) return error.LocalGCEligibilityConflict;
            return false;
        },
        else => return err,
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse return error.InvalidLocalGCRoot);
    return true;
}

fn isProtected(protected: []const []const u8, generation: []const u8) bool {
    for (protected) |item| if (std.mem.eql(u8, item, generation)) return true;
    return false;
}

fn newerCandidate(_: void, a: Candidate, b: Candidate) bool {
    if (a.checkpoint_lsn != b.checkpoint_lsn) return a.checkpoint_lsn > b.checkpoint_lsn;
    return std.mem.order(u8, a.generation, b.generation) == .gt;
}

fn isLowerHex(value: []const u8) bool {
    for (value) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
    return true;
}

fn encodeHex(out: []u8, bytes: []const u8) void {
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, i| {
        out[i * 2] = alphabet[byte >> 4];
        out[i * 2 + 1] = alphabet[byte & 0x0f];
    }
}

fn writeTestFile(path: []const u8, body: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(std.testing.io, parent);
    var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, body);
    try file.sync(std.testing.io);
}

fn expectPathExists(path: []const u8) !void {
    try std.Io.Dir.cwd().access(std.testing.io, path, .{});
}

fn expectPathMissing(path: []const u8) !void {
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
}

fn generationPath(alloc: std.mem.Allocator, root: []const u8, generation: []const u8) ![]u8 {
    return std.fs.path.join(alloc, &.{ root, "generations", generation });
}

fn pairedGenerationPath(alloc: std.mem.Allocator, root: []const u8, generation: []const u8) ![]u8 {
    return std.fs.path.join(alloc, &.{ root, "live-generations", generation });
}

fn prepareGeneration(alloc: std.mem.Allocator, root: []const u8, generation: []const u8) ![]u8 {
    const path = try generationPath(alloc, root, generation);
    errdefer alloc.free(path);
    const payload = try std.fs.path.join(alloc, &.{ path, "content/payload" });
    defer alloc.free(payload);
    try writeTestFile(payload, generation);
    return path;
}

test "storage.ha local generation gc retains newest and protected eligible generations only" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);

    const ineligible = try prepareGeneration(alloc, root, "gen-incomplete");
    defer alloc.free(ineligible);
    const target_scope = try prepareGeneration(alloc, root, "gen-target");
    defer alloc.free(target_scope);
    try markEligible(alloc, .{
        .root = root,
        .scope = .target_activation,
        .generation = "gen-target",
        .slot_name = "standby-a",
        .checkpoint_lsn = 90,
        .checkpoint_bytes = "durable-target-activation-receipt",
    });

    for (1..6) |index| {
        const generation = try std.fmt.allocPrint(alloc, "gen-{d}", .{index});
        defer alloc.free(generation);
        const path = try prepareGeneration(alloc, root, generation);
        defer alloc.free(path);
        const checkpoint = try std.fmt.allocPrint(alloc, "durable-publish-receipt-{d}", .{index});
        defer alloc.free(checkpoint);
        try markEligible(alloc, .{
            .root = root,
            .scope = .source_capture,
            .generation = generation,
            .slot_name = "standby-a",
            .checkpoint_lsn = index,
            .checkpoint_bytes = checkpoint,
        });
    }

    var result = try prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-5",
        .protected_generations = &.{"gen-1"},
        .retain_generations = 2,
    });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.deleted_generations);
    try std.testing.expectEqual(@as(usize, 3), result.retained_generations);

    for ([_][]const u8{ "gen-1", "gen-4", "gen-5", "gen-incomplete", "gen-target" }) |generation| {
        const path = try generationPath(alloc, root, generation);
        defer alloc.free(path);
        try expectPathExists(path);
    }
    for ([_][]const u8{ "gen-2", "gen-3" }) |generation| {
        const path = try generationPath(alloc, root, generation);
        defer alloc.free(path);
        try expectPathMissing(path);
    }

    var repeated = try prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-5",
        .protected_generations = &.{"gen-1"},
        .retain_generations = 2,
    });
    defer repeated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), repeated.deleted_generations);
}

test "storage.ha local generation gc resumes an interrupted tombstone deletion" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);

    for (1..3) |index| {
        const generation = try std.fmt.allocPrint(alloc, "gen-{d}", .{index});
        defer alloc.free(generation);
        const path = try prepareGeneration(alloc, root, generation);
        defer alloc.free(path);
        try markEligible(alloc, .{
            .root = root,
            .scope = .source_capture,
            .generation = generation,
            .slot_name = "standby-a",
            .checkpoint_lsn = index,
            .checkpoint_bytes = generation,
        });
    }

    try std.testing.expectError(error.InjectedLocalGCFailure, pruneWithOptions(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-2",
        .retain_generations = 1,
    }, .{ .fail_after_tombstones = 1 }));
    const old_path = try generationPath(alloc, root, "gen-1");
    defer alloc.free(old_path);
    try expectPathMissing(old_path);
    const tombstone = try generationPath(alloc, root, ".gc-gen-1");
    defer alloc.free(tombstone);
    try expectPathExists(tombstone);

    var recovered = try prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-2",
        .retain_generations = 1,
    });
    defer recovered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), recovered.resumed_tombstones);
    try expectPathMissing(tombstone);
}

test "storage.ha local generation gc crash between paired and raw deletion resumes without live orphan" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);

    for (1..3) |index| {
        const generation = try std.fmt.allocPrint(alloc, "gen-{d}", .{index});
        defer alloc.free(generation);
        const raw_path = try prepareGeneration(alloc, root, generation);
        defer alloc.free(raw_path);
        const live_path = try pairedGenerationPath(alloc, root, generation);
        defer alloc.free(live_path);
        const live_payload = try std.fs.path.join(alloc, &.{ live_path, "runtime/payload" });
        defer alloc.free(live_payload);
        try writeTestFile(live_payload, generation);
        try markEligible(alloc, .{
            .root = root,
            .scope = .target_activation,
            .generation = generation,
            .slot_name = "standby-a",
            .checkpoint_lsn = index,
            .checkpoint_bytes = generation,
        });
    }

    try std.testing.expectError(error.InjectedPairedLocalGCFailure, pruneWithOptions(alloc, .{
        .root = root,
        .scope = .target_activation,
        .slot_name = "standby-a",
        .current_generation = "gen-2",
        .retain_generations = 1,
        .paired_generations_dir_name = "live-generations",
    }, .{ .fail_after_paired_deletions = 1 }));
    const old_raw = try generationPath(alloc, root, "gen-1");
    defer alloc.free(old_raw);
    const old_live = try pairedGenerationPath(alloc, root, "gen-1");
    defer alloc.free(old_live);
    try expectPathExists(old_raw);
    try expectPathMissing(old_live);

    var recovered = try prune(alloc, .{
        .root = root,
        .scope = .target_activation,
        .slot_name = "standby-a",
        .current_generation = "gen-2",
        .retain_generations = 1,
        .paired_generations_dir_name = "live-generations",
    });
    defer recovered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), recovered.deleted_generations);
    try expectPathMissing(old_raw);
    try expectPathMissing(old_live);

    const current_raw = try generationPath(alloc, root, "gen-2");
    defer alloc.free(current_raw);
    const current_live = try pairedGenerationPath(alloc, root, "gen-2");
    defer alloc.free(current_live);
    try expectPathExists(current_raw);
    try expectPathExists(current_live);
}

test "storage.ha local generation gc fails closed without current durable eligibility" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try prepareGeneration(alloc, root, "gen-unpublished");
    defer alloc.free(path);

    try std.testing.expectError(error.CurrentGenerationNotEligible, prune(alloc, .{
        .root = root,
        .scope = .source_capture,
        .slot_name = "standby-a",
        .current_generation = "gen-unpublished",
        .retain_generations = 1,
    }));
    try expectPathExists(path);
}
