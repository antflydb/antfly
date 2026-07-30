// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const lsm_backend = @import("../lsm_backend/storage_io.zig");

const rebuild_state_name = "rebuild.state";
const rebuild_state_generation_separator = ".g-";
const rebuild_state_magic = "AFRBST01";
const rebuild_state_version_legacy_envelope: u32 = 1;
const rebuild_state_version: u32 = 2;
const rebuild_state_owner_bytes = @sizeOf(u64);
const rebuild_state_flags_bytes = @sizeOf(u8);
const rebuild_state_flag_complete: u8 = 1;
const rebuild_state_key_length_bytes = @sizeOf(u32);
const rebuild_state_checksum_bytes = @sizeOf(u32);
const rebuild_state_v1_header_bytes = rebuild_state_magic.len + @sizeOf(@TypeOf(rebuild_state_version)) + rebuild_state_key_length_bytes;
const rebuild_state_header_bytes = rebuild_state_magic.len + @sizeOf(@TypeOf(rebuild_state_version)) + rebuild_state_owner_bytes + rebuild_state_flags_bytes + rebuild_state_key_length_bytes;
const rebuild_state_max_key_bytes = 64 * 1024;
const rebuild_state_max_encoded_bytes = rebuild_state_header_bytes + rebuild_state_max_key_bytes + rebuild_state_checksum_bytes;
const rebuild_state_max_read_bytes = rebuild_state_max_encoded_bytes + 1;
const rebuild_state_temp_attempts = 16;

const PublishFaultPoint = enum {
    after_temp_sync,
    after_rename,
};

var test_publish_fault: ?PublishFaultPoint = null;

pub const LoadResult = union(enum) {
    absent,
    legacy,
    corrupt,
    valid: []u8,

    pub fn deinit(self: *LoadResult, alloc: Allocator) void {
        switch (self.*) {
            .valid => |key| alloc.free(key),
            else => {},
        }
        self.* = undefined;
    }
};

const DecodedCursor = struct {
    owner_generation: ?u64,
    complete: bool,
    key: []u8,
};

const DecodedLoadResult = union(enum) {
    absent,
    legacy,
    corrupt,
    valid: DecodedCursor,

    fn deinit(self: *DecodedLoadResult, alloc: Allocator) void {
        switch (self.*) {
            .valid => |cursor| alloc.free(cursor.key),
            else => {},
        }
        self.* = undefined;
    }
};

pub const RebuildState = struct {
    root_path: []const u8,
    storage: ?lsm_backend.Storage = null,
    /// Immutable catalog-generation identity. A same-name index replacement
    /// receives a new coverage generation, so an old worker can never publish
    /// a cursor that the replacement generation will accept.
    owner_generation: ?u64 = null,

    pub fn init(root_path: []const u8) RebuildState {
        return .{ .root_path = root_path };
    }

    pub fn initWithStorage(root_path: []const u8, storage: ?lsm_backend.Storage) RebuildState {
        return .{ .root_path = root_path, .storage = storage };
    }

    pub fn initOwned(root_path: []const u8, storage: ?lsm_backend.Storage, owner_generation: u64) RebuildState {
        std.debug.assert(owner_generation != 0);
        return .{
            .root_path = root_path,
            .storage = storage,
            .owner_generation = owner_generation,
        };
    }

    pub fn check(self: RebuildState, alloc: Allocator) !?[]u8 {
        if (builtin.os.tag == .freestanding) {
            if (self.storage == null) return null;
            return try self.checkWithIo(alloc, undefined);
        }
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return try self.checkWithIo(alloc, io_impl.io());
    }

    pub fn checkWithIo(self: RebuildState, alloc: Allocator, io: std.Io) !?[]u8 {
        var loaded = try self.loadDecodedWithIo(alloc, io);
        defer loaded.deinit(alloc);
        return switch (loaded) {
            .absent => null,
            .valid => |cursor| blk: {
                if (self.owner_generation) |expected_owner| {
                    if (cursor.owner_generation != expected_owner) {
                        // A legacy cursor or a cursor from a same-name prior
                        // generation is never a valid resume point. Publish an
                        // owned restart marker before exposing the empty key.
                        try self.publishOwnedRestartWithIo(io);
                        try self.removeLegacyAfterMigrationWithIo(io);
                        break :blk try alloc.dupe(u8, "");
                    }
                }
                if (cursor.complete) break :blk null;
                break :blk try alloc.dupe(u8, cursor.key);
            },
            .legacy => blk: {
                // Pre-v1 cursors have no integrity envelope. Their position
                // cannot be trusted after an upgrade, so preserve the active
                // marker while forcing a safe rebuild from the beginning.
                try self.updateWithIo(io, "");
                if (self.owner_generation != null) {
                    try self.removeLegacyAfterMigrationWithIo(io);
                }
                break :blk try alloc.dupe(u8, "");
            },
            .corrupt => error.InvalidRebuildState,
        };
    }

    pub fn loadWithIo(self: RebuildState, alloc: Allocator, io: std.Io) !LoadResult {
        var decoded = try self.loadDecodedWithIo(alloc, io);
        defer decoded.deinit(alloc);
        return switch (decoded) {
            .absent => .absent,
            .legacy => .legacy,
            .corrupt => .corrupt,
            .valid => |cursor| if (cursor.complete)
                .absent
            else
                .{ .valid = try alloc.dupe(u8, cursor.key) },
        };
    }

    fn loadDecodedWithIo(self: RebuildState, alloc: Allocator, io: std.Io) !DecodedLoadResult {
        if (builtin.os.tag == .freestanding and self.storage == null) return .absent;
        const path = try self.pathAlloc(alloc);
        defer alloc.free(path);
        const loaded = try self.loadDecodedAtPathWithIo(alloc, io, path);
        if (loaded != .absent or self.owner_generation == null) return loaded;

        // A pre-generation rebuild may be in flight during an upgrade. Its
        // cursor cannot prove ownership, but its presence does prove that a
        // rebuild was active. Return it so checkWithIo migrates to a safe
        // generation-owned restart rather than silently treating it as done.
        const legacy_path = try self.legacyPathAlloc(alloc);
        defer alloc.free(legacy_path);
        return try self.loadDecodedAtPathWithIo(alloc, io, legacy_path);
    }

    fn loadDecodedAtPathWithIo(
        self: RebuildState,
        alloc: Allocator,
        io: std.Io,
        path: []const u8,
    ) !DecodedLoadResult {
        if (self.storage) |storage| {
            const encoded = storage.readFileAlloc(alloc, path, rebuild_state_max_read_bytes) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => return .absent,
                error.FileTooBig, error.StreamTooLong => return .corrupt,
                else => return err,
            };
            defer alloc.free(encoded);
            return try decodeStateLoadResult(alloc, encoded);
        }
        return try loadDecodedPathWithIo(alloc, io, path);
    }

    pub fn update(self: RebuildState, key: []const u8) !void {
        if (builtin.os.tag == .freestanding) {
            if (self.storage == null) return;
            return try self.updateWithIo(undefined, key);
        }
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try self.updateWithIo(io_impl.io(), key);
    }

    pub fn updateWithIo(self: RebuildState, io: std.Io, key: []const u8) !void {
        if (builtin.os.tag == .freestanding and self.storage == null) return;
        const alloc = std.heap.page_allocator;
        const encoded = try encodeState(alloc, self.owner_generation, false, key);
        defer alloc.free(encoded);
        try self.publishEncodedWithIo(alloc, io, encoded);
    }

    pub fn clear(self: RebuildState) !void {
        if (builtin.os.tag == .freestanding) {
            if (self.storage == null) return;
            return try self.clearWithIo(undefined);
        }
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try self.clearWithIo(io_impl.io());
    }

    pub fn clearWithIo(self: RebuildState, io: std.Io) !void {
        if (builtin.os.tag == .freestanding and self.storage == null) return;
        const path = try self.pathAlloc(std.heap.page_allocator);
        defer std.heap.page_allocator.free(path);
        if (self.storage) |storage| {
            storage.deleteFileAbsolute(path) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => return,
                else => return err,
            };
            storage.syncParentAbsolute(path) catch |err| switch (err) {
                error.DurableDirectorySyncUnsupported => return,
                else => return error.RebuildStateDurabilityUncertain,
            };
            return;
        }
        deleteFileWithIo(io, path) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return,
            else => return err,
        };
        try syncPublishedParent(io, path);
    }

    pub fn estimateProgress(self: RebuildState, range_start: []const u8, range_end: []const u8, alloc: Allocator) !?f64 {
        const resume_key = (try self.check(alloc)) orelse return null;
        defer alloc.free(resume_key);
        return estimateProgressForKey(range_start, range_end, resume_key);
    }

    pub fn pathAlloc(self: RebuildState, alloc: Allocator) ![]u8 {
        if (self.owner_generation) |owner| {
            return try std.fmt.allocPrint(
                alloc,
                "{s}/{s}{s}{x:0>16}",
                .{ self.root_path, rebuild_state_name, rebuild_state_generation_separator, owner },
            );
        }
        return try self.legacyPathAlloc(alloc);
    }

    fn legacyPathAlloc(self: RebuildState, alloc: Allocator) ![]u8 {
        return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.root_path, rebuild_state_name });
    }

    fn publishOwnedRestartWithIo(self: RebuildState, io: std.Io) !void {
        const owner = self.owner_generation orelse return error.RebuildStateOwnerRequired;
        const alloc = std.heap.page_allocator;
        const encoded = try encodeState(alloc, owner, false, "");
        defer alloc.free(encoded);
        try self.publishEncodedWithIo(alloc, io, encoded);
    }

    fn removeLegacyAfterMigrationWithIo(self: RebuildState, io: std.Io) !void {
        if (self.owner_generation == null) return;
        const alloc = std.heap.page_allocator;
        const path = try self.legacyPathAlloc(alloc);
        defer alloc.free(path);
        if (self.storage) |storage| {
            storage.deleteFileAbsolute(path) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => return,
                else => return err,
            };
            storage.syncParentAbsolute(path) catch |err| switch (err) {
                error.DurableDirectorySyncUnsupported => return,
                else => return error.RebuildStateDurabilityUncertain,
            };
            return;
        }
        deleteFileWithIo(io, path) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return,
            else => return err,
        };
        try syncPublishedParent(io, path);
    }

    fn publishEncodedWithIo(self: RebuildState, alloc: Allocator, io: std.Io, encoded: []const u8) !void {
        const path = try self.pathAlloc(alloc);
        defer alloc.free(path);
        if (self.storage) |storage| {
            var sink = try storage.beginAtomicWrite(alloc, path);
            var sink_active = true;
            defer if (sink_active) sink.abort();
            try sink.appendSlice(encoded);
            sink_active = false;
            try sink.finish();
            return;
        }

        const tmp_path = try writeExclusiveTempStateFile(alloc, io, path, encoded);
        defer alloc.free(tmp_path);
        var tmp_exists = true;
        defer if (tmp_exists) deleteFileWithIo(io, tmp_path) catch {};
        try injectPublishFault(.after_temp_sync);
        renameWithIo(io, tmp_path, path) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return error.RebuildStateOwnerStale,
            else => return err,
        };
        tmp_exists = false;
        try injectPublishFault(.after_rename);
        try syncPublishedParent(io, path);
    }
};

pub fn ownerGenerationFromPath(path: []const u8) ?u64 {
    const prefix = rebuild_state_name ++ rebuild_state_generation_separator;
    const basename = std.fs.path.basename(path);
    if (!std.mem.startsWith(u8, basename, prefix)) return null;
    const encoded = basename[prefix.len..];
    if (encoded.len != 16) return null;
    const generation = std.fmt.parseInt(u64, encoded, 16) catch return null;
    return if (generation == 0) null else generation;
}

fn injectPublishFault(point: PublishFaultPoint) !void {
    if (!builtin.is_test) return;
    if (test_publish_fault == point) return error.TestRebuildStateCrash;
}

fn loadDecodedPathWithIo(alloc: Allocator, io: std.Io, path: []const u8) !DecodedLoadResult {
    const encoded = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(rebuild_state_max_read_bytes)) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return .absent,
        error.StreamTooLong => return .corrupt,
        else => return err,
    };
    defer alloc.free(encoded);
    return try decodeStateLoadResult(alloc, encoded);
}

fn decodeStateLoadResult(alloc: Allocator, encoded: []const u8) !DecodedLoadResult {
    if (!std.mem.startsWith(u8, encoded, rebuild_state_magic)) {
        // The old on-disk format was exactly the raw resume key.
        return if (encoded.len <= rebuild_state_max_key_bytes) .legacy else .corrupt;
    }
    const cursor = decodeState(alloc, encoded) catch |err| switch (err) {
        error.InvalidRebuildState => return .corrupt,
        else => return err,
    };
    return .{ .valid = cursor };
}

fn writeExclusiveTempStateFile(alloc: Allocator, io: std.Io, path: []const u8, contents: []const u8) ![]u8 {
    for (0..rebuild_state_temp_attempts) |_| {
        var entropy: [@sizeOf(u128)]u8 = undefined;
        try io.randomSecure(&entropy);
        const nonce = std.mem.readInt(u128, &entropy, .little);
        const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{x}", .{ path, nonce });
        writeStateFile(io, tmp_path, contents, true) catch |err| switch (err) {
            error.PathAlreadyExists => {
                alloc.free(tmp_path);
                continue;
            },
            error.FileNotFound, error.NotDir => {
                alloc.free(tmp_path);
                return error.RebuildStateOwnerStale;
            },
            else => {
                deleteFileWithIo(io, tmp_path) catch {};
                alloc.free(tmp_path);
                return err;
            },
        };
        return tmp_path;
    }
    return error.RebuildStateTempCollision;
}

fn renameWithIo(io: std.Io, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(new_path)) {
        try std.Io.Dir.renameAbsolute(old_path, new_path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
    }
}

fn deleteFileWithIo(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

fn syncPublishedParent(io: std.Io, path: []const u8) !void {
    fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".") catch |err| switch (err) {
        // Platforms without durable directory sync have an explicit
        // best-effort publication policy. Do not report an ordinary write
        // failure after the atomic replacement is already visible.
        error.DurableDirectorySyncUnsupported => return,
        else => return error.RebuildStateDurabilityUncertain,
    };
}

fn encodeState(alloc: Allocator, owner_generation: ?u64, complete: bool, key: []const u8) ![]u8 {
    if (key.len > rebuild_state_max_key_bytes or key.len > std.math.maxInt(u32)) {
        return error.RebuildStateTooLarge;
    }

    const encoded_len = if (owner_generation == null)
        rebuild_state_v1_header_bytes + key.len + rebuild_state_checksum_bytes
    else
        rebuild_state_header_bytes + key.len + rebuild_state_checksum_bytes;
    const encoded = try alloc.alloc(u8, encoded_len);
    @memcpy(encoded[0..rebuild_state_magic.len], rebuild_state_magic);

    var pos = rebuild_state_magic.len;
    const version = if (owner_generation == null) rebuild_state_version_legacy_envelope else rebuild_state_version;
    std.mem.writeInt(u32, encoded[pos..][0..@sizeOf(@TypeOf(rebuild_state_version))], version, .little);
    pos += @sizeOf(@TypeOf(rebuild_state_version));
    if (owner_generation) |owner| {
        std.mem.writeInt(u64, encoded[pos..][0..rebuild_state_owner_bytes], owner, .little);
        pos += rebuild_state_owner_bytes;
        encoded[pos] = if (complete) rebuild_state_flag_complete else 0;
        pos += rebuild_state_flags_bytes;
    } else if (complete) {
        return error.RebuildStateOwnerRequired;
    }
    std.mem.writeInt(u32, encoded[pos..][0..rebuild_state_key_length_bytes], @intCast(key.len), .little);
    pos += rebuild_state_key_length_bytes;
    @memcpy(encoded[pos .. pos + key.len], key);
    pos += key.len;
    std.mem.writeInt(u32, encoded[pos..][0..rebuild_state_checksum_bytes], std.hash.Crc32.hash(encoded[0..pos]), .little);
    return encoded;
}

fn decodeState(alloc: Allocator, encoded: []const u8) !DecodedCursor {
    if (encoded.len < rebuild_state_v1_header_bytes + rebuild_state_checksum_bytes) {
        return error.InvalidRebuildState;
    }
    if (!std.mem.eql(u8, encoded[0..rebuild_state_magic.len], rebuild_state_magic)) {
        return error.InvalidRebuildState;
    }

    var pos = rebuild_state_magic.len;
    const version = std.mem.readInt(u32, encoded[pos..][0..@sizeOf(@TypeOf(rebuild_state_version))], .little);
    if (version != rebuild_state_version_legacy_envelope and version != rebuild_state_version) {
        return error.InvalidRebuildState;
    }
    pos += @sizeOf(@TypeOf(rebuild_state_version));

    const owner_generation: ?u64 = if (version == rebuild_state_version) blk: {
        if (encoded.len < rebuild_state_header_bytes + rebuild_state_checksum_bytes) {
            return error.InvalidRebuildState;
        }
        const owner = std.mem.readInt(u64, encoded[pos..][0..rebuild_state_owner_bytes], .little);
        if (owner == 0) return error.InvalidRebuildState;
        pos += rebuild_state_owner_bytes;
        break :blk owner;
    } else null;
    const flags: u8 = if (version == rebuild_state_version) blk: {
        const value = encoded[pos];
        if (value & ~rebuild_state_flag_complete != 0) return error.InvalidRebuildState;
        pos += rebuild_state_flags_bytes;
        break :blk value;
    } else 0;
    const key_len: usize = std.mem.readInt(u32, encoded[pos..][0..rebuild_state_key_length_bytes], .little);
    pos += rebuild_state_key_length_bytes;
    if (key_len > rebuild_state_max_key_bytes or encoded.len - pos - rebuild_state_checksum_bytes != key_len) {
        return error.InvalidRebuildState;
    }

    const checksum_offset = encoded.len - rebuild_state_checksum_bytes;
    const stored_checksum = std.mem.readInt(u32, encoded[checksum_offset..][0..rebuild_state_checksum_bytes], .little);
    if (stored_checksum != std.hash.Crc32.hash(encoded[0..checksum_offset])) {
        return error.InvalidRebuildState;
    }
    const complete = flags & rebuild_state_flag_complete != 0;
    if (complete and key_len != 0) return error.InvalidRebuildState;
    return .{
        .owner_generation = owner_generation,
        .complete = complete,
        .key = try alloc.dupe(u8, encoded[pos..checksum_offset]),
    };
}

fn writeStateFile(io: std.Io, path: []const u8, contents: []const u8, exclusive: bool) !void {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true, .exclusive = exclusive });
    defer file.close(io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(contents);
    try writer.end();
    try file.sync(io);
}

pub fn estimateProgressForKey(range_start: []const u8, range_end: []const u8, current_key: []const u8) f64 {
    const start_val = keyToU64(range_start);
    const end_val = keyToU64(range_end);
    const cur_val = keyToU64(current_key);

    if (end_val <= start_val) return if (current_key.len == 0) 0.0 else 1.0;
    if (cur_val <= start_val) return 0.0;
    if (cur_val >= end_val) return 1.0;

    const range: f64 = @floatFromInt(end_val - start_val);
    const pos: f64 = @floatFromInt(cur_val - start_val);
    return pos / range;
}

fn keyToU64(key: []const u8) u64 {
    if (key.len == 0) return 0;
    var buf: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const copy_len = @min(key.len, 8);
    @memcpy(buf[0..copy_len], key[0..copy_len]);
    return std.mem.readInt(u64, &buf, .big);
}

fn createTestStateRoot(path: []const u8) !void {
    try fs_paths.createDirPathPortable(std.testing.io, path);
}

test "rebuild state round trips and clears" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    const state = RebuildState.init(path);
    try std.testing.expect((try state.check(std.testing.allocator)) == null);
    try createTestStateRoot(path);
    try state.update("doc:m");
    const loaded = (try state.check(std.testing.allocator)) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings("doc:m", loaded);
    try state.update("");
    const empty = (try state.check(std.testing.allocator)) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(empty);
    try std.testing.expectEqualStrings("", empty);
    try state.clear();
    try std.testing.expect((try state.check(std.testing.allocator)) == null);
}

test "rebuild state rejects key corruption" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-corrupt", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    const state = RebuildState.init(path);
    try createTestStateRoot(path);
    try state.update("doc:m");
    const state_path = try state.pathAlloc(std.testing.allocator);
    defer std.testing.allocator.free(state_path);
    const encoded = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        state_path,
        std.testing.allocator,
        .limited(rebuild_state_max_encoded_bytes),
    );
    defer std.testing.allocator.free(encoded);
    encoded[rebuild_state_v1_header_bytes + 4] = 'z';
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = state_path, .data = encoded });

    try std.testing.expectError(error.InvalidRebuildState, state.check(std.testing.allocator));
    // Validation quarantines the cursor; it must not silently clear it.
    try std.testing.expectError(error.InvalidRebuildState, state.check(std.testing.allocator));
}

test "rebuild state rejects truncation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-truncated", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    const state = RebuildState.init(path);
    try createTestStateRoot(path);
    try state.update("doc:m");
    const state_path = try state.pathAlloc(std.testing.allocator);
    defer std.testing.allocator.free(state_path);
    const encoded = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        state_path,
        std.testing.allocator,
        .limited(rebuild_state_max_encoded_bytes),
    );
    defer std.testing.allocator.free(encoded);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = state_path, .data = encoded[0 .. encoded.len - 1] });

    try std.testing.expectError(error.InvalidRebuildState, state.check(std.testing.allocator));
}

test "rebuild state upgrades legacy cursor by restarting from scratch" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-legacy", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try createTestStateRoot(path);

    const state = RebuildState.init(path);
    const state_path = try state.pathAlloc(std.testing.allocator);
    defer std.testing.allocator.free(state_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = state_path, .data = "doc:m" });

    var legacy = try state.loadWithIo(std.testing.allocator, std.testing.io);
    defer legacy.deinit(std.testing.allocator);
    try std.testing.expect(legacy == .legacy);

    const restart_key = (try state.checkWithIo(std.testing.allocator, std.testing.io)) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(restart_key);
    try std.testing.expectEqualStrings("", restart_key);

    var migrated = try state.loadWithIo(std.testing.allocator, std.testing.io);
    defer migrated.deinit(std.testing.allocator);
    switch (migrated) {
        .valid => |key| try std.testing.expectEqualStrings("", key),
        else => return error.TestExpectedValidRebuildState,
    }
}

test "rebuild state update does not recreate vanished owner directory" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-stale-owner", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try createTestStateRoot(path);
    try std.Io.Dir.cwd().deleteTree(std.testing.io, path);

    const state = RebuildState.init(path);
    try std.testing.expectError(error.RebuildStateOwnerStale, state.updateWithIo(std.testing.io, "doc:m"));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, path, .{}));
}

test "rebuild state uses injected durable storage" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const storage = memory.storage();
    const state = RebuildState.initWithStorage("__test/indexes/search", storage);

    try std.testing.expect((try state.checkWithIo(alloc, std.testing.io)) == null);
    try state.updateWithIo(std.testing.io, "doc:m");
    const loaded = (try state.checkWithIo(alloc, std.testing.io)) orelse return error.TestExpectedEqual;
    defer alloc.free(loaded);
    try std.testing.expectEqualStrings("doc:m", loaded);

    const state_path = try state.pathAlloc(alloc);
    defer alloc.free(state_path);
    const encoded = try storage.readFileAlloc(alloc, state_path, rebuild_state_max_encoded_bytes);
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings(rebuild_state_magic, encoded[0..rebuild_state_magic.len]);

    try state.clearWithIo(std.testing.io);
    try std.testing.expect((try state.checkWithIo(alloc, std.testing.io)) == null);
}

test "rebuild state generation owner fences same-name replacement ABA" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-aba", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try createTestStateRoot(path);

    const old_generation = RebuildState.initOwned(path, null, 41);
    const replacement = RebuildState.initOwned(path, null, 42);
    try old_generation.updateWithIo(std.testing.io, "doc:m");

    // The replacement cannot even observe the old generation's plausible
    // cursor. Its normal empty-index path starts a new rebuild.
    try std.testing.expect((try replacement.checkWithIo(std.testing.allocator, std.testing.io)) == null);
    try replacement.updateWithIo(std.testing.io, "");
    try replacement.updateWithIo(std.testing.io, "doc:f");

    // A stale worker may keep running, but it can only mutate its own
    // generation namespace. No timing interleaving can clobber generation 42.
    try old_generation.updateWithIo(std.testing.io, "doc:z");
    try old_generation.clearWithIo(std.testing.io);

    const current = (try replacement.checkWithIo(std.testing.allocator, std.testing.io)) orelse
        return error.TestExpectedEqual;
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("doc:f", current);
}

test "rebuild state owned completion cannot erase replacement cursor" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-owned-clear", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try createTestStateRoot(path);

    const old_generation = RebuildState.initOwned(path, null, 71);
    const replacement = RebuildState.initOwned(path, null, 72);
    try old_generation.updateWithIo(std.testing.io, "doc:b");
    try old_generation.clearWithIo(std.testing.io);
    try std.testing.expect((try old_generation.checkWithIo(std.testing.allocator, std.testing.io)) == null);

    try replacement.updateWithIo(std.testing.io, "");
    try replacement.updateWithIo(std.testing.io, "doc:q");
    try old_generation.clearWithIo(std.testing.io);
    const current = (try replacement.checkWithIo(std.testing.allocator, std.testing.io)) orelse
        return error.TestExpectedEqual;
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("doc:q", current);
}

test "rebuild state owned migration safely restarts an in-flight legacy cursor" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-owner-migration", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try createTestStateRoot(path);

    const legacy = RebuildState.init(path);
    try legacy.updateWithIo(std.testing.io, "doc:m");

    const owned = RebuildState.initOwned(path, null, 91);
    const restart = (try owned.checkWithIo(std.testing.allocator, std.testing.io)) orelse
        return error.TestExpectedEqual;
    defer std.testing.allocator.free(restart);
    try std.testing.expectEqualStrings("", restart);

    const owned_path = try owned.pathAlloc(std.testing.allocator);
    defer std.testing.allocator.free(owned_path);
    try std.testing.expect(std.mem.endsWith(u8, owned_path, "rebuild.state.g-000000000000005b"));

    // Migration retires the unowned marker only after the owned restart is
    // durable. Completion therefore cannot rediscover the legacy cursor and
    // spuriously restart the same generation forever.
    try owned.clearWithIo(std.testing.io);
    try std.testing.expect((try owned.checkWithIo(std.testing.allocator, std.testing.io)) == null);
}

test "rebuild state publication crash boundaries preserve a valid state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/rebuild-state-publication-crash", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try createTestStateRoot(path);

    const state = RebuildState.initOwned(path, null, 101);
    try state.updateWithIo(std.testing.io, "doc:a");

    test_publish_fault = .after_temp_sync;
    defer test_publish_fault = null;
    try std.testing.expectError(
        error.TestRebuildStateCrash,
        state.updateWithIo(std.testing.io, "doc:b"),
    );
    var loaded = (try state.checkWithIo(std.testing.allocator, std.testing.io)) orelse
        return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("doc:a", loaded);
    std.testing.allocator.free(loaded);

    // A crash after rename has an ambiguous acknowledgement but never a
    // partial cursor: recovery sees the fully checksummed replacement.
    test_publish_fault = .after_rename;
    try std.testing.expectError(
        error.TestRebuildStateCrash,
        state.updateWithIo(std.testing.io, "doc:c"),
    );
    loaded = (try state.checkWithIo(std.testing.allocator, std.testing.io)) orelse
        return error.TestExpectedEqual;
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings("doc:c", loaded);
}

test "rebuild state estimates progress from resume key" {
    const progress = estimateProgressForKey("doc:a", "doc:z", "doc:m");
    try std.testing.expect(progress > 0.0);
    try std.testing.expect(progress < 1.0);
}
