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

//! Resumable verification of canonical source-copy AFB2 files. The existing
//! bounded manifest parser runs once; footer/object work thereafter uses a
//! fixed-record disk index and never scans an already consumed corpus prefix.
//! Hash checkpoints encode algorithm words/tail/length, not Zig struct bytes.
const std = @import("std");
const codec = @import("backup_codec.zig");
const bundle = @import("backup_bundle.zig");
const snapshot = @import("source_snapshot.zig");
const native = @import("db/native_backup.zig");
const fs = @import("../common/fs_paths.zig");
const Allocator = std.mem.Allocator;
const Cancellation = @import("../common/cancellation.zig").CancellationToken;
const Sha = std.crypto.hash.sha2.Sha256;
const Crc = @import("antfly_hash").Crc32;
const cohort_key = "\x00\x00__metadata__:portable_cohort";
const no_ordinal = std.math.maxInt(u32);
const blob_record_size = 88;
const object_record_size = 48;

pub const Budget = struct { bytes: usize = 1024 * 1024, units: usize = 4096, duration_ns: u64 = 5 * std.time.ns_per_ms };
pub const max_initialization_bytes = codec.header_size + bundle.trailer_size + codec.block_envelope_overhead + bundle.max_manifest_bytes + 16;
pub const Result = struct { complete: bool, bytes_read: u64, units: usize, initialized: bool = false, verified_bytes: u64 = 0 };
const Work = struct {
    budget: Budget,
    result: Result = .{ .complete = false, .bytes_read = 0, .units = 0 },
    start: std.Io.Timestamp,
    fn stop(self: Work, io: std.Io) bool {
        return self.result.bytes_read >= self.budget.bytes or self.result.units >= self.budget.units or
            (self.result.units != 0 and self.start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds() >= self.budget.duration_ns);
    }
};

pub const HashState = struct {
    words: [8]u32,
    tail: [64]u8 = @splat(0),
    tail_len: u8 = 0,
    total: u64 = 0,
    pub fn init() HashState {
        return .{ .words = Sha.init(.{}).s };
    }
    fn restore(self: HashState) !Sha {
        if (self.tail_len >= 64 or self.total % 64 != self.tail_len or !std.mem.allEqual(u8, self.tail[self.tail_len..], 0)) return error.SourceSnapshotCorrupt;
        return .{ .s = self.words, .buf = self.tail, .buf_len = self.tail_len, .total_len = self.total };
    }
    pub fn update(self: *HashState, bytes: []const u8) !void {
        var hash = try self.restore();
        if (bytes.len > std.math.maxInt(u64) - hash.total_len) return error.SourceSnapshotTooLarge;
        hash.update(bytes);
        self.words = hash.s;
        self.tail_len = hash.buf_len;
        self.total = hash.total_len;
        @memset(&self.tail, 0);
        @memcpy(self.tail[0..self.tail_len], hash.buf[0..self.tail_len]);
    }
    pub fn finish(self: HashState) ![32]u8 {
        var hash = try self.restore();
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    fn length(self: *HashState, value: u64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        try self.update(&bytes);
    }
};

const Metadata = struct {
    phase: enum { count, key_length, key, value_length, value, done } = .count,
    word: [4]u8 = @splat(0),
    word_used: u8 = 0,
    remaining_entries: u32 = 0,
    key_length: u32 = 0,
    value_length: u32 = 0,
    consumed: u64 = 0,
    skip: bool = false,
    integrity_binding: bool = false,
    binding_bytes: [64]u8 = @splat(0),
    content: HashState = HashState.init(),
    schema: HashState = HashState.init(),
};
const State = struct {
    version: u8 = 2,
    scope: [32]u8,
    expected: snapshot.Certificate,
    inode: u64,
    size: u64,
    mtime: i128,
    index_inode: u64 = 0,
    blobs: u32 = 0,
    objects: u32 = 0,
    footer_offset: u64 = 0,
    footer_size: u64 = 0,
    footer_next: u32 = 0,
    footer_crc: u32 = 0,
    phase: enum { footer, objects, done } = .footer,
    ordinal: u32 = 0,
    kind: u8 = 0,
    blob_index: u32 = 0,
    blob_size: u64 = 0,
    blob_digest: [32]u8 = @splat(0),
    blob_offset: u64 = 0,
    physical_offset: u64 = 0,
    chunk_remaining: u32 = 0,
    chunk_crc: u32 = 0,
    active: bool = false,
    content: snapshot.Certificate,
    blob_hash: HashState = HashState.init(),
    object_hash: HashState = HashState.init(),
    metadata: Metadata = .{},
    logical_bytes: u64 = 0,
};

fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    Sha.hash(bytes, &result, .{});
    return result;
}
fn readExact(io: std.Io, file: std.Io.File, bytes: []u8, offset: u64) !void {
    if (try file.readPositionalAll(io, bytes, offset) != bytes.len) return error.SourceSnapshotCorrupt;
}
fn atomicWrite(alloc: Allocator, io: std.Io, path: []const u8, bytes: []const u8) !void {
    const staging = try std.fmt.allocPrint(alloc, "{s}.staging", .{path});
    defer alloc.free(staging);
    _ = try native.writeFileDurable(io, staging, bytes);
    try std.Io.Dir.rename(.cwd(), staging, .cwd(), path, io);
    try fs.syncDirPortable(io, std.fs.path.dirname(path).?);
}
fn save(alloc: Allocator, io: std.Io, path: []const u8, state: State) !void {
    const json = try std.json.Stringify.valueAlloc(alloc, state, .{});
    defer alloc.free(json);
    if (json.len > 16 * 1024) return error.SourceSnapshotTooLarge;
    const encoded = try std.mem.concat(alloc, u8, &.{ "ASV1", &digest(json), json });
    defer alloc.free(encoded);
    try atomicWrite(alloc, io, path, encoded);
}
fn load(alloc: Allocator, io: std.Io, path: []const u8) !?State {
    const bytes = native.readFileAlloc(alloc, io, path, 16 * 1024 + 37) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(bytes);
    if (bytes.len < 36 or !std.mem.eql(u8, bytes[0..4], "ASV1") or !std.mem.eql(u8, bytes[4..36], &digest(bytes[36..]))) return error.SourceSnapshotCorrupt;
    var parsed = std.json.parseFromSlice(State, alloc, bytes[36..], .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.SourceSnapshotCorrupt,
    };
    defer parsed.deinit();
    return parsed.value;
}

const Blob = struct { digest: [32]u8, size: u64, offset: u64 = 0, footer_ordinal: u32 = no_ordinal };
fn encodeBlob(blob: Blob) [blob_record_size]u8 {
    var bytes: [blob_record_size]u8 = @splat(0);
    bytes[0..32].* = blob.digest;
    std.mem.writeInt(u64, bytes[32..40], blob.size, .little);
    std.mem.writeInt(u64, bytes[40..48], blob.offset, .little);
    std.mem.writeInt(u32, bytes[48..52], blob.footer_ordinal, .little);
    bytes[56..88].* = digest(bytes[0..56]);
    return bytes;
}
fn writeBlob(io: std.Io, index: std.Io.File, ordinal: u32, blob: Blob) !void {
    try index.writePositionalAll(io, &encodeBlob(blob), @as(u64, ordinal) * blob_record_size);
}
fn readBlob(io: std.Io, index: std.Io.File, ordinal: u32) !Blob {
    var bytes: [blob_record_size]u8 = undefined;
    try readExact(io, index, &bytes, @as(u64, ordinal) * blob_record_size);
    if (!std.mem.eql(u8, bytes[56..88], &digest(bytes[0..56]))) return error.SourceSnapshotCorrupt;
    return .{ .digest = bytes[0..32].*, .size = std.mem.readInt(u64, bytes[32..40], .little), .offset = std.mem.readInt(u64, bytes[40..48], .little), .footer_ordinal = std.mem.readInt(u32, bytes[48..52], .little) };
}
fn findBlob(io: std.Io, index: std.Io.File, count: u32, key: [32]u8) !u32 {
    var low: u32 = 0;
    var high = count;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const blob = try readBlob(io, index, middle);
        switch (std.mem.order(u8, &blob.digest, &key)) {
            .lt => low = middle + 1,
            .gt => high = middle,
            .eq => return middle,
        }
    }
    return error.InvalidBundleFooter;
}

fn initialize(alloc: Allocator, io: std.Io, file: std.Io.File, index: std.Io.File, state: *State, work: *Work, cancellation: Cancellation) !void {
    var raw = codec.FileReader.init(io, file, state.size);
    const header = try raw.readHeader();
    if (header.format_version != codec.format_version or header.table_count != 1 or header.shard_count != 1) return error.InvalidSourceSnapshot;
    var trailer_bytes: [bundle.trailer_size]u8 = undefined;
    if (state.size < codec.header_size + bundle.trailer_size) return error.InvalidBundleFooter;
    try readExact(io, file, &trailer_bytes, state.size - bundle.trailer_size);
    const trailer = try bundle.decodeTrailer(&trailer_bytes, state.size);
    var envelope: [6]u8 = undefined;
    try readExact(io, file, &envelope, codec.header_size);
    if (envelope[0] != @intFromEnum(codec.BlockType.bundle_manifest) or envelope[1] != 0) return error.InvalidBackupManifest;
    const manifest_length = std.mem.readInt(u32, envelope[2..6], .little);
    if (manifest_length > bundle.max_manifest_bytes) return error.BackupManifestTooLarge;
    const block = try raw.readBlock(alloc);
    defer alloc.free(block.payload);
    var manifest = try bundle.parseManifest(alloc, block.payload);
    defer manifest.deinit();
    try bundle.validateReadablePayloadFeatures(manifest.value);
    if (manifest.value.representation != .portable or manifest.value.mode != .full or manifest.value.objects.len == 0 or manifest.value.blobs.len == 0) return error.InvalidBackupManifest;
    state.blobs = @intCast(manifest.value.blobs.len);
    state.objects = @intCast(manifest.value.objects.len);
    state.footer_offset = trailer.footer_offset;
    state.footer_size = trailer.footer_payload_size;
    if (state.footer_size != 4 + @as(u64, state.blobs) * 48 or state.footer_offset < raw.pos or
        state.footer_offset + state.footer_size + codec.block_envelope_overhead != state.size - bundle.trailer_size) return error.InvalidBundleFooter;
    const used = try alloc.alloc(bool, state.blobs);
    defer alloc.free(used);
    @memset(used, false);
    var index_buffer: [64 * 1024]u8 = undefined;
    var index_writer = index.writer(io, &index_buffer);
    for (manifest.value.blobs, 0..) |blob, ordinal| {
        try cancellation.check();
        if (!blob.included or blob.stored_size_bytes > codec.max_block_payload_bytes) return error.BackupBlockTooLarge;
        var hash: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&hash, blob.sha256);
        _ = ordinal;
        try index_writer.interface.writeAll(&encodeBlob(.{ .digest = hash, .size = blob.stored_size_bytes }));
    }
    for (manifest.value.objects, 0..) |object, ordinal| {
        try cancellation.check();
        const blob_index = bundle.blobIndex(manifest.value.blobs, object.sha256) orelse return error.InvalidBackupManifest;
        used[blob_index] = true;
        const kind = std.meta.stringToEnum(codec.BlockType, object.role) orelse return error.InvalidBackupManifest;
        switch (kind) {
            .cluster_manifest, .table_manifest, .shard_header, .document_batch, .embedding_batch, .sparse_batch, .summary_batch, .chunk_batch, .edge_batch, .transaction_batch, .doc_identity_batch, .metadata_batch, .artifact_batch, .resolution_batch, .integrity_batch, .shard_footer, .file_footer => {},
            else => return error.InvalidBackupManifest,
        }
        var bytes: [object_record_size]u8 = @splat(0);
        bytes[0] = @intFromEnum(kind);
        std.mem.writeInt(u32, bytes[4..8], @intCast(blob_index), .little);
        std.mem.writeInt(u64, bytes[8..16], object.size_bytes, .little);
        bytes[16..48].* = digest(bytes[0..16]);
        _ = ordinal;
        try index_writer.interface.writeAll(&bytes);
    }
    // A source exporter emits no unreachable blobs. Requiring exact reachability
    // means every physical payload will be checked by the object stream.
    for (used) |referenced| if (!referenced) return error.IncompleteBackupInventory;
    try index_writer.end();
    try index.setLength(io, @as(u64, state.blobs) * blob_record_size + @as(u64, state.objects) * object_record_size);
    try index.sync(io);
    state.index_inode = (try index.stat(io)).inode;
    try readExact(io, file, &envelope, state.footer_offset);
    if (envelope[0] != @intFromEnum(codec.BlockType.footer_index) or envelope[1] != 0 or std.mem.readInt(u32, envelope[2..6], .little) != state.footer_size) return error.InvalidBundleFooter;
    var count: [4]u8 = undefined;
    try readExact(io, file, &count, state.footer_offset + 6);
    if (std.mem.readInt(u32, &count, .little) != state.blobs) return error.InvalidBundleFooter;
    var crc = Crc.init();
    crc.update(&envelope);
    crc.update(&count);
    state.footer_crc = crc.crc;
    work.result.initialized = true;
    work.result.bytes_read = codec.header_size + bundle.trailer_size + codec.block_envelope_overhead + manifest_length + 16;
    work.result.units = @as(usize, state.objects) + state.blobs;
}

fn footerStep(io: std.Io, file: std.Io.File, index: std.Io.File, state: *State, work: *Work) !void {
    while (state.footer_next < state.blobs and !work.stop(io)) {
        if (work.budget.bytes - work.result.bytes_read < 52) break;
        var bytes: [48]u8 = undefined;
        try readExact(io, file, &bytes, state.footer_offset + 10 + @as(u64, state.footer_next) * 48);
        const entry = bundle.decodeFooterIndexEntry(&bytes);
        const ordinal = try findBlob(io, index, state.blobs, entry.sha256);
        var blob = try readBlob(io, index, ordinal);
        if (blob.size != entry.stored_size_bytes or entry.header_offset < codec.header_size or entry.header_offset >= state.footer_offset or
            (blob.footer_ordinal != no_ordinal and blob.footer_ordinal != state.footer_next)) return error.InvalidBundleFooter;
        blob.offset = entry.header_offset;
        blob.footer_ordinal = state.footer_next;
        try writeBlob(io, index, ordinal, blob);
        var crc: Crc = .{ .crc = state.footer_crc };
        crc.update(&bytes);
        state.footer_crc = crc.crc;
        state.footer_next += 1;
        work.result.bytes_read += bytes.len;
        work.result.units += 1;
    }
    if (state.footer_next == state.blobs) {
        var crc_bytes: [4]u8 = undefined;
        try readExact(io, file, &crc_bytes, state.footer_offset + 6 + state.footer_size);
        work.result.bytes_read += crc_bytes.len;
        if (std.mem.readInt(u32, &crc_bytes, .little) != (Crc{ .crc = state.footer_crc }).final()) return error.BlockCrcMismatch;
        state.phase = .objects;
    }
}

fn beginObject(alloc: Allocator, io: std.Io, file: std.Io.File, index: std.Io.File, state: *State, work: *Work) !void {
    var bytes: [object_record_size]u8 = undefined;
    try readExact(io, index, &bytes, @as(u64, state.blobs) * blob_record_size + @as(u64, state.ordinal) * object_record_size);
    if (!std.mem.eql(u8, bytes[16..48], &digest(bytes[0..16]))) return error.SourceSnapshotCorrupt;
    state.kind = bytes[0];
    state.blob_index = std.mem.readInt(u32, bytes[4..8], .little);
    if (state.blob_index >= state.blobs or (state.ordinal == 0 and state.kind != 1) or (state.kind == 0xff and state.ordinal + 1 != state.objects)) return error.InvalidSourceSnapshot;
    const blob = try readBlob(io, index, state.blob_index);
    if (blob.footer_ordinal == no_ordinal or blob.size != std.mem.readInt(u64, bytes[8..16], .little)) return error.SourceSnapshotCorrupt;
    var raw = codec.FileReader.init(io, file, state.footer_offset);
    raw.pos = @intCast(blob.offset);
    var envelope: [6]u8 = undefined;
    try readExact(io, file, &envelope, raw.pos);
    if (envelope[0] != @intFromEnum(codec.BlockType.blob_header) or envelope[1] != 0 or std.mem.readInt(u32, envelope[2..6], .little) > 8192) return error.InvalidBackupManifest;
    const block = try raw.readBlock(alloc);
    defer alloc.free(block.payload);
    var header = try bundle.decodeBlobHeader(alloc, block.payload);
    defer header.deinit(alloc);
    const hex = std.fmt.bytesToHex(blob.digest, .lower);
    var expected_path: [70]u8 = undefined;
    @memcpy(expected_path[0..6], "blobs/");
    @memcpy(expected_path[6..], &hex);
    if (header.ordinal != state.blob_index or header.compression != .none or !std.mem.eql(u8, header.logical_path, &expected_path) or !std.mem.eql(u8, header.role, "portable_blob") or
        header.logical_size_bytes != blob.size or header.stored_size_bytes != blob.size or !std.mem.eql(u8, &header.sha256, &blob.digest)) return error.BackupArtifactIntegrityMismatch;
    state.blob_size = blob.size;
    state.blob_digest = blob.digest;
    state.blob_offset = 0;
    state.physical_offset = raw.pos;
    state.chunk_remaining = 0;
    state.blob_hash = HashState.init();
    state.object_hash = HashState.init();
    state.metadata = .{};
    if (state.kind == 0x1b and state.content.integrity == null) return error.InvalidSourceSnapshot;
    if (state.kind == 0xff and blob.size != 24) return error.InvalidSourceSnapshot;
    if (state.kind != 0x18) {
        try state.object_hash.update(&state.content.ordered_content_digest);
        try state.object_hash.update(&.{state.kind});
        try state.object_hash.length(0);
        try state.object_hash.length(if (state.kind == 0xff) 16 else blob.size);
    } else if (blob.size < 4) return error.InvalidSourceSnapshot;
    state.active = true;
    work.result.bytes_read += block.payload.len + 16;
    work.result.units += 1;
}

fn metadataFeed(state: *State, bytes: []const u8, work: *Work, io: std.Io) !usize {
    const meta = &state.metadata;
    var offset: usize = 0;
    while (offset < bytes.len and !work.stop(io) and work.result.units + 1 < work.budget.units) {
        switch (meta.phase) {
            .done => return error.InvalidSourceSnapshot,
            .count, .key_length, .value_length => {
                const size = @min(4 - meta.word_used, bytes.len - offset);
                @memcpy(meta.word[meta.word_used..][0..size], bytes[offset..][0..size]);
                offset += size;
                meta.word_used += @intCast(size);
                if (meta.word_used != 4) continue;
                const value = std.mem.readInt(u32, &meta.word, .little);
                meta.word_used = 0;
                switch (meta.phase) {
                    .count => {
                        if (value > (state.blob_size - 4) / 8) return error.InvalidSourceSnapshot;
                        meta.remaining_entries = value;
                        meta.phase = if (value == 0) .done else .key_length;
                    },
                    .key_length => {
                        meta.key_length = value;
                        meta.consumed = 0;
                        meta.skip = value == cohort_key.len;
                        meta.integrity_binding = value == snapshot.integrity_key.len;
                        meta.content = HashState.init();
                        meta.schema = HashState.init();
                        try meta.content.update(&state.content.ordered_content_digest);
                        try meta.schema.update(&state.content.schema_manifest_digest);
                        try meta.content.update(&.{0x18});
                        try meta.content.length(value);
                        try meta.schema.length(value);
                        meta.phase = if (value == 0) .value_length else .key;
                    },
                    .value_length => {
                        meta.value_length = value;
                        if (meta.integrity_binding and value != 64) return error.InvalidSourceSnapshot;
                        meta.consumed = 0;
                        try meta.content.length(value);
                        try meta.schema.length(value);
                        meta.phase = .value;
                        if (value == 0) try finishMetadataEntry(state, work);
                    },
                    else => unreachable,
                }
            },
            .key, .value => {
                const total = if (meta.phase == .key) meta.key_length else meta.value_length;
                const size: usize = @intCast(@min(total - meta.consumed, bytes.len - offset));
                const part = bytes[offset..][0..size];
                try meta.content.update(part);
                try meta.schema.update(part);
                if (meta.phase == .key and meta.skip and !std.mem.eql(u8, part, cohort_key[@intCast(meta.consumed)..][0..size])) meta.skip = false;
                if (meta.phase == .key and meta.integrity_binding and !std.mem.eql(u8, part, snapshot.integrity_key[@intCast(meta.consumed)..][0..size])) meta.integrity_binding = false;
                if (meta.phase == .value and meta.integrity_binding) @memcpy(meta.binding_bytes[@intCast(meta.consumed)..][0..size], part);
                meta.consumed += size;
                offset += size;
                if (meta.consumed == total) {
                    if (meta.phase == .key) meta.phase = .value_length else try finishMetadataEntry(state, work);
                }
            },
        }
    }
    return offset;
}
fn finishMetadataEntry(state: *State, work: *Work) !void {
    const meta = &state.metadata;
    if (meta.integrity_binding) {
        if (state.content.integrity != null or meta.value_length != 64) return error.InvalidSourceSnapshot;
        state.content.integrity = .{ .catalog_digest = meta.binding_bytes[0..32].*, .generation_set = meta.binding_bytes[32..64].* };
    }
    if (!meta.skip) {
        state.content.ordered_content_digest = try meta.content.finish();
        state.content.schema_manifest_digest = try meta.schema.finish();
        state.content.objects = std.math.add(u64, state.content.objects, 1) catch return error.SourceSnapshotTooLarge;
        state.content.content_bytes = std.math.add(u64, state.content.content_bytes, @as(u64, meta.key_length) + meta.value_length) catch return error.SourceSnapshotTooLarge;
    }
    if (meta.remaining_entries == 0) return error.InvalidSourceSnapshot;
    meta.remaining_entries -= 1;
    meta.phase = if (meta.remaining_entries == 0) .done else .key_length;
    work.result.units += 1;
}

fn objectsStep(alloc: Allocator, io: std.Io, file: std.Io.File, index: std.Io.File, state: *State, work: *Work, cancellation: Cancellation) !void {
    var buffer: [64 * 1024]u8 = undefined;
    while (state.ordinal < state.objects and !work.stop(io)) {
        try cancellation.check();
        if (!state.active) {
            if (work.budget.bytes - work.result.bytes_read < 8192 + 32) break;
            try beginObject(alloc, io, file, index, state, work);
            if (work.stop(io)) break;
        }
        if (state.blob_offset == state.blob_size) {
            if (!std.mem.eql(u8, &try state.blob_hash.finish(), &state.blob_digest)) return error.BackupArtifactIntegrityMismatch;
            if (state.kind == 0x18) {
                if (state.metadata.phase != .done) return error.InvalidSourceSnapshot;
            } else {
                state.content.ordered_content_digest = try state.object_hash.finish();
                state.content.objects = std.math.add(u64, state.content.objects, 1) catch return error.SourceSnapshotTooLarge;
                state.content.content_bytes = std.math.add(u64, state.content.content_bytes, if (state.kind == 0xff) 16 else state.blob_size) catch return error.SourceSnapshotTooLarge;
            }
            state.ordinal += 1;
            state.active = false;
            work.result.units += 1;
            continue;
        }
        if (state.chunk_remaining == 0) {
            if (work.budget.bytes - work.result.bytes_read < 22) break;
            var prefix: [18]u8 = undefined;
            if (state.physical_offset + prefix.len + 4 > state.footer_offset) return error.InvalidBundleFooter;
            try readExact(io, file, &prefix, state.physical_offset);
            const size = std.mem.readInt(u32, prefix[2..6], .little);
            if (prefix[0] != @intFromEnum(codec.BlockType.blob_chunk) or prefix[1] != 0 or size <= 12 or size > bundle.native_chunk_target_bytes + 12 or
                std.mem.readInt(u32, prefix[6..10], .little) != state.blob_index or std.mem.readInt(u64, prefix[10..18], .little) != state.blob_offset or
                size - 12 != @min(bundle.native_chunk_target_bytes, state.blob_size - state.blob_offset) or state.physical_offset + 10 + size > state.footer_offset) return error.InvalidNativeFileChunk;
            var crc = Crc.init();
            crc.update(&prefix);
            state.chunk_crc = crc.crc;
            state.chunk_remaining = size - 12;
            state.physical_offset += prefix.len;
            work.result.bytes_read += prefix.len;
        }
        if (work.stop(io)) break;
        const size: usize = @intCast(@min(buffer.len, state.chunk_remaining, (work.budget.bytes - work.result.bytes_read) -| 4));
        if (size == 0) break;
        try readExact(io, file, buffer[0..size], state.physical_offset);
        const used = if (state.kind == 0x18) try metadataFeed(state, buffer[0..size], work, io) else size;
        if (state.kind != 0x18) {
            const logical_size = if (state.kind == 0xff) @min(used, 16 -| state.blob_offset) else used;
            try state.object_hash.update(buffer[0..@intCast(logical_size)]);
        }
        try state.blob_hash.update(buffer[0..used]);
        var crc: Crc = .{ .crc = state.chunk_crc };
        crc.update(buffer[0..used]);
        state.chunk_crc = crc.crc;
        state.chunk_remaining -= @intCast(used);
        state.physical_offset += used;
        state.blob_offset += used;
        state.logical_bytes += used;
        work.result.bytes_read += size;
        if (used < size) break;
        if (state.chunk_remaining == 0) {
            var checksum: [4]u8 = undefined;
            try readExact(io, file, &checksum, state.physical_offset);
            if (std.mem.readInt(u32, &checksum, .little) != crc.final()) return error.BlockCrcMismatch;
            state.physical_offset += 4;
            work.result.bytes_read += 4;
            work.result.units += 1;
        }
    }
    if (state.ordinal == state.objects) {
        if (state.kind != 0xff or !state.content.eql(state.expected)) return error.SourceSnapshotCorrupt;
        state.phase = .done;
    }
}

/// Call while holding the existing source-pin slot lock. Receipt/index files
/// live under that pin and are removed by its shared bounded GC machinery.
pub fn step(alloc: Allocator, io: std.Io, file: std.Io.File, root: []const u8, scope: [32]u8, expected: snapshot.Certificate, cancellation: Cancellation, budget: Budget) !Result {
    try cancellation.check();
    if (budget.bytes < 64 * 1024 or budget.units < 16 or budget.duration_ns == 0) return error.InvalidSourceSnapshot;
    const cursor_path = try std.fmt.allocPrint(alloc, "{s}/source.verify", .{root});
    defer alloc.free(cursor_path);
    const index_path = try std.fmt.allocPrint(alloc, "{s}/source.verify.index", .{root});
    defer alloc.free(index_path);
    const stat = try file.stat(io);
    var state = (try load(alloc, io, cursor_path)) orelse State{ .scope = scope, .expected = expected, .inode = stat.inode, .size = stat.size, .mtime = stat.mtime.toNanoseconds(), .content = (try snapshot.Builder.init(expected.cut)).certificate };
    if (state.version != 2 or !std.mem.eql(u8, &state.scope, &scope) or !state.expected.eql(expected) or state.inode != stat.inode or state.size != stat.size or state.mtime != stat.mtime.toNanoseconds()) return error.SourceFileChanged;
    if (state.ordinal > state.objects or state.footer_next > state.blobs or state.blob_offset > state.blob_size or state.chunk_remaining > bundle.native_chunk_target_bytes) return error.SourceSnapshotCorrupt;
    if (state.phase == .done) {
        if (state.ordinal != state.objects or state.active or state.kind != 0xff or !state.content.eql(expected)) return error.SourceSnapshotCorrupt;
        const certified_index = try std.Io.Dir.cwd().openFile(io, index_path, .{});
        defer certified_index.close(io);
        const index_stat = try certified_index.stat(io);
        if (index_stat.inode != state.index_inode or index_stat.size != @as(u64, state.blobs) * blob_record_size + @as(u64, state.objects) * object_record_size) return error.SourceFileChanged;
        return .{ .complete = true, .bytes_read = 0, .units = 0, .verified_bytes = state.logical_bytes };
    }
    const index = try std.Io.Dir.cwd().createFile(io, index_path, .{ .read = true, .truncate = state.index_inode == 0 });
    defer index.close(io);
    var work: Work = .{ .budget = budget, .start = std.Io.Clock.awake.now(io) };
    if (state.index_inode == 0) {
        try initialize(alloc, io, file, index, &state, &work, cancellation);
    } else {
        const index_stat = try index.stat(io);
        if (index_stat.inode != state.index_inode or index_stat.size != @as(u64, state.blobs) * blob_record_size + @as(u64, state.objects) * object_record_size) return error.SourceFileChanged;
        if (state.phase == .footer) try footerStep(io, file, index, &state, &work);
        if (state.phase == .objects and !work.stop(io)) try objectsStep(alloc, io, file, index, &state, &work, cancellation);
    }
    try cancellation.check();
    const after = try file.stat(io);
    if (after.inode != state.inode or after.size != state.size or after.mtime.toNanoseconds() != state.mtime) return error.SourceFileChanged;
    try index.sync(io);
    try save(alloc, io, cursor_path, state);
    work.result.complete = state.phase == .done;
    work.result.verified_bytes = state.logical_bytes;
    return work.result;
}

/// Certified random logical access for source-row cursors after failover. It
/// borrows the immutable artifact handle and owns only the small index handle.
/// Callers hold the existing source slot lease for the reader lifetime.
pub const ObjectReader = struct {
    io: std.Io,
    file: std.Io.File,
    index: std.Io.File,
    state: State,
    pub const Object = struct { kind: codec.BlockType, size: u64, blob_index: u32, header_offset: u64, digest: [32]u8 };
    pub fn open(alloc: Allocator, io: std.Io, file: std.Io.File, root: []const u8, scope: [32]u8, expected: snapshot.Certificate) !ObjectReader {
        const cursor_path = try std.fmt.allocPrint(alloc, "{s}/source.verify", .{root});
        defer alloc.free(cursor_path);
        const state = (try load(alloc, io, cursor_path)) orelse return error.SourceSnapshotIncomplete;
        const stat = try file.stat(io);
        if (state.version != 2 or state.phase != .done or !std.mem.eql(u8, &state.scope, &scope) or !state.expected.eql(expected) or !state.content.eql(expected) or
            state.inode != stat.inode or state.size != stat.size or state.mtime != stat.mtime.toNanoseconds()) return error.SourceFileChanged;
        const index_path = try std.fmt.allocPrint(alloc, "{s}/source.verify.index", .{root});
        defer alloc.free(index_path);
        const index = try std.Io.Dir.cwd().openFile(io, index_path, .{});
        errdefer index.close(io);
        const index_stat = try index.stat(io);
        if (index_stat.inode != state.index_inode or index_stat.size != @as(u64, state.blobs) * blob_record_size + @as(u64, state.objects) * object_record_size) return error.SourceFileChanged;
        return .{ .io = io, .file = file, .index = index, .state = state };
    }
    pub fn deinit(self: *ObjectReader) void {
        self.index.close(self.io);
        self.* = undefined;
    }
    pub fn objectCount(self: ObjectReader) u32 {
        return self.state.objects;
    }
    pub fn object(self: ObjectReader, ordinal: u32) !Object {
        if (ordinal >= self.state.objects) return error.InvalidSourceSnapshot;
        var bytes: [object_record_size]u8 = undefined;
        try readExact(self.io, self.index, &bytes, @as(u64, self.state.blobs) * blob_record_size + @as(u64, ordinal) * object_record_size);
        if (!std.mem.eql(u8, bytes[16..48], &digest(bytes[0..16]))) return error.SourceSnapshotCorrupt;
        const blob_index = std.mem.readInt(u32, bytes[4..8], .little);
        if (blob_index >= self.state.blobs) return error.SourceSnapshotCorrupt;
        const blob = try readBlob(self.io, self.index, blob_index);
        if (blob.size != std.mem.readInt(u64, bytes[8..16], .little) or blob.footer_ordinal == no_ordinal) return error.SourceSnapshotCorrupt;
        const kind = std.enums.fromInt(codec.BlockType, bytes[0]) orelse return error.SourceSnapshotCorrupt;
        return .{ .kind = kind, .size = blob.size, .blob_index = blob_index, .header_offset = blob.offset, .digest = blob.digest };
    }
    pub fn readAt(self: ObjectReader, ordinal: u32, offset: u64, buffer: []u8) !usize {
        const entry = try self.object(ordinal);
        if (offset > entry.size) return error.InvalidSourceSnapshot;
        if (offset == entry.size or buffer.len == 0) return 0;
        var envelope: [6]u8 = undefined;
        try readExact(self.io, self.file, &envelope, entry.header_offset);
        if (envelope[0] != @intFromEnum(codec.BlockType.blob_header) or envelope[1] != 0) return error.SourceSnapshotCorrupt;
        const header_size = std.mem.readInt(u32, envelope[2..6], .little);
        if (header_size > 8192) return error.SourceSnapshotCorrupt;
        const chunk = offset / bundle.native_chunk_target_bytes;
        const inside = offset % bundle.native_chunk_target_bytes;
        const physical = entry.header_offset + 10 + header_size + chunk * (bundle.native_chunk_target_bytes + 22) + 18 + inside;
        const size: usize = @intCast(@min(buffer.len, entry.size - offset, bundle.native_chunk_target_bytes - inside));
        if (physical + size > self.state.footer_offset) return error.SourceSnapshotCorrupt;
        try readExact(self.io, self.file, buffer[0..size], physical);
        return size;
    }
};
