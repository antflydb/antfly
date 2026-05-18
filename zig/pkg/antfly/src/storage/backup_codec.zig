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
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const ArrayList = std.ArrayList;

/// AFB file magic bytes: "ANTFLYB\n"
pub const magic = [8]u8{ 'A', 'N', 'T', 'F', 'L', 'Y', 'B', '\n' };

/// Current format version.
pub const format_version: u32 = 1;

/// Fixed size of the file header in bytes.
pub const header_size: usize = 64;

/// Block envelope overhead: type(1) + flags(1) + payload_len(4) + crc32(4).
pub const block_envelope_overhead: usize = 10;

// --- Block types ---

pub const BlockType = enum(u8) {
    cluster_manifest = 0x01,
    table_manifest = 0x02,
    shard_header = 0x03,
    document_batch = 0x10,
    embedding_batch = 0x11,
    sparse_batch = 0x12,
    summary_batch = 0x13,
    chunk_batch = 0x14,
    edge_batch = 0x15,
    transaction_batch = 0x16,
    shard_footer = 0xF0,
    file_footer = 0xFF,
    _,
};

pub const block_flag_compressed: u8 = 1 << 0;

/// Document value flag: value is zstd-compressed JSON.
pub const doc_value_flag_compressed: u8 = 1;

// --- File Header ---

pub const FileHeader = struct {
    format_version: u32,
    flags: u32,
    created_at_ns: i64,
    backup_id: [16]u8,
    table_count: u32,
    shard_count: u32,
};

// --- Entry types ---

pub const DocumentEntry = struct {
    key: []const u8,
    value_flags: u8,
    value: []const u8,
    timestamp_ns: u64,
};

pub const EmbeddingEntry = struct {
    doc_key: []const u8,
    hash_id: u64,
    vector: []const f32,
};

pub const SparseEntry = struct {
    doc_key: []const u8,
    hash_id: u64,
    indices: []const u32,
    values: []const f32,
};

pub const EdgeEntry = struct {
    source_key: []const u8,
    target_key: []const u8,
    edge_type: []const u8,
    value: []const u8,
};

pub const ShardHeaderEntry = struct {
    table_name: []const u8,
    shard_id: u32,
    start_key: []const u8,
    end_key: []const u8,
};

pub const ShardFooterEntry = struct {
    shard_id: u32,
    document_count: u64,
    embedding_count: u64,
    edge_count: u64,
    transaction_count: u64,
};

pub const FileFooterEntry = struct {
    table_count: u32,
    shard_count: u32,
    total_documents: u64,
    total_bytes: u64,
};

pub const Block = struct {
    block_type: BlockType,
    payload: []const u8,
};

// --- Writer ---
// Appends AFB data to an ArrayList(u8). Zig writer always writes uncompressed
// (no zstd encoder in std).

pub fn writeHeader(buf: *ArrayList(u8), alloc: Allocator, h: FileHeader) !void {
    const dest = try buf.addManyAsArray(alloc, header_size);
    @memset(dest, 0);
    @memcpy(dest[0..8], &magic);
    std.mem.writeInt(u32, dest[8..12], h.format_version, .little);
    std.mem.writeInt(u32, dest[12..16], h.flags, .little);
    std.mem.writeInt(i64, dest[16..24], h.created_at_ns, .little);
    @memcpy(dest[24..40], &h.backup_id);
    std.mem.writeInt(u32, dest[40..44], h.table_count, .little);
    std.mem.writeInt(u32, dest[44..48], h.shard_count, .little);
    const crc = Crc32.hash(dest[0..48]);
    std.mem.writeInt(u32, dest[48..52], crc, .little);
}

pub fn writeBlock(buf: *ArrayList(u8), alloc: Allocator, block_type: BlockType, payload: []const u8) !void {
    var env_header: [6]u8 = undefined;
    env_header[0] = @intFromEnum(block_type);
    env_header[1] = 0; // no compression
    std.mem.writeInt(u32, env_header[2..6], @intCast(payload.len), .little);

    var crc = Crc32.init();
    crc.update(&env_header);
    crc.update(payload);
    const crc_val = crc.final();

    try buf.appendSlice(alloc, &env_header);
    try buf.appendSlice(alloc, payload);

    var crc_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc_buf, crc_val, .little);
    try buf.appendSlice(alloc, &crc_buf);
}

// --- Reader ---
// Reads AFB data from a byte slice using a position cursor.

pub const SliceReader = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) SliceReader {
        return .{ .data = data, .pos = 0 };
    }

    fn readExact(self: *SliceReader, n: usize) ![]const u8 {
        if (self.pos + n > self.data.len) return error.EndOfStream;
        const slice = self.data[self.pos..][0..n];
        self.pos += n;
        return slice;
    }

    pub fn readHeader(self: *SliceReader) !FileHeader {
        const buf = try self.readExact(header_size);

        if (!std.mem.eql(u8, buf[0..8], &magic)) {
            return error.InvalidMagic;
        }

        const stored_crc = std.mem.readInt(u32, buf[48..52], .little);
        const computed_crc = Crc32.hash(buf[0..48]);
        if (stored_crc != computed_crc) {
            return error.HeaderCrcMismatch;
        }

        const ver = std.mem.readInt(u32, buf[8..12], .little);
        if (ver > format_version) {
            return error.UnsupportedVersion;
        }

        return .{
            .format_version = ver,
            .flags = std.mem.readInt(u32, buf[12..16], .little),
            .created_at_ns = std.mem.readInt(i64, buf[16..24], .little),
            .backup_id = buf[24..40].*,
            .table_count = std.mem.readInt(u32, buf[40..44], .little),
            .shard_count = std.mem.readInt(u32, buf[44..48], .little),
        };
    }

    pub fn readBlock(self: *SliceReader, alloc: Allocator) !Block {
        const env = try self.readExact(6);
        const block_type: BlockType = @enumFromInt(env[0]);
        const flags = env[1];
        const payload_len = std.mem.readInt(u32, env[2..6], .little);

        const payload = try self.readExact(payload_len);
        const crc_bytes = try self.readExact(4);
        const stored_crc = std.mem.readInt(u32, crc_bytes[0..4], .little);

        // Verify CRC over [env + payload]
        var crc = Crc32.init();
        crc.update(env);
        crc.update(payload);
        if (crc.final() != stored_crc) {
            return error.BlockCrcMismatch;
        }

        // Decompress if needed
        if (flags & block_flag_compressed != 0) {
            const decompressed = try decompressZstd(alloc, payload);
            return .{ .block_type = block_type, .payload = decompressed };
        }

        // Return a copy so caller can free uniformly
        const owned = try alloc.dupe(u8, payload);
        return .{ .block_type = block_type, .payload = owned };
    }
};

fn decompressZstd(alloc: Allocator, compressed: []const u8) ![]u8 {
    const Reader = std.Io.Reader;
    var input = Reader.fixed(compressed);
    var window_buf: [std.compress.zstd.default_window_len + std.compress.zstd.block_size_max]u8 = undefined;
    var decomp = std.compress.zstd.Decompress.init(&input, &window_buf, .{});
    return decomp.reader.allocRemaining(alloc, .unlimited);
}

// --- Batch Encoding helpers ---

fn appendU16LE(buf: *ArrayList(u8), alloc: Allocator, val: u16) !void {
    const dest = try buf.addManyAsArray(alloc, 2);
    std.mem.writeInt(u16, dest, val, .little);
}

fn appendU32LE(buf: *ArrayList(u8), alloc: Allocator, val: u32) !void {
    const dest = try buf.addManyAsArray(alloc, 4);
    std.mem.writeInt(u32, dest, val, .little);
}

fn appendU64LE(buf: *ArrayList(u8), alloc: Allocator, val: u64) !void {
    const dest = try buf.addManyAsArray(alloc, 8);
    std.mem.writeInt(u64, dest, val, .little);
}

// --- Batch Encoding ---

pub fn encodeDocumentBatch(alloc: Allocator, entries: []const DocumentEntry) ![]u8 {
    var buf: ArrayList(u8) = .empty;
    errdefer buf.deinit(alloc);

    try appendU32LE(&buf, alloc, @intCast(entries.len));
    for (entries) |e| {
        try appendU32LE(&buf, alloc, @intCast(e.key.len));
        try buf.appendSlice(alloc, e.key);
        try buf.append(alloc, e.value_flags);
        try appendU32LE(&buf, alloc, @intCast(e.value.len));
        try buf.appendSlice(alloc, e.value);
        try appendU64LE(&buf, alloc, e.timestamp_ns);
    }

    return buf.toOwnedSlice(alloc);
}

pub fn decodeDocumentBatch(alloc: Allocator, data: []const u8) ![]DocumentEntry {
    if (data.len < 4) return error.BatchTooShort;

    const count = std.mem.readInt(u32, data[0..4], .little);
    var off: usize = 4;

    var entries = try ArrayList(DocumentEntry).initCapacity(alloc, count);
    errdefer entries.deinit(alloc);

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (off + 4 > data.len) return error.Truncated;
        const key_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;

        if (off + key_len > data.len) return error.Truncated;
        const key = try alloc.dupe(u8, data[off..][0..key_len]);
        off += key_len;

        if (off + 1 > data.len) return error.Truncated;
        const value_flags = data[off];
        off += 1;

        if (off + 4 > data.len) return error.Truncated;
        const value_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;

        if (off + value_len > data.len) return error.Truncated;
        const value = try alloc.dupe(u8, data[off..][0..value_len]);
        off += value_len;

        if (off + 8 > data.len) return error.Truncated;
        const timestamp_ns = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;

        try entries.append(alloc, .{
            .key = key,
            .value_flags = value_flags,
            .value = value,
            .timestamp_ns = timestamp_ns,
        });
    }

    return entries.toOwnedSlice(alloc);
}

pub fn encodeEmbeddingBatch(alloc: Allocator, index_name: []const u8, dimension: u16, entries: []const EmbeddingEntry) ![]u8 {
    var buf: ArrayList(u8) = .empty;
    errdefer buf.deinit(alloc);

    try appendU32LE(&buf, alloc, @intCast(index_name.len));
    try buf.appendSlice(alloc, index_name);
    try appendU16LE(&buf, alloc, dimension);
    try appendU32LE(&buf, alloc, @intCast(entries.len));

    for (entries) |e| {
        try appendU32LE(&buf, alloc, @intCast(e.doc_key.len));
        try buf.appendSlice(alloc, e.doc_key);
        try appendU64LE(&buf, alloc, e.hash_id);
        for (e.vector) |f| {
            try appendU32LE(&buf, alloc, @bitCast(f));
        }
    }

    return buf.toOwnedSlice(alloc);
}

pub fn decodeEmbeddingBatch(alloc: Allocator, data: []const u8) !struct {
    index_name: []const u8,
    dimension: u16,
    entries: []EmbeddingEntry,
} {
    if (data.len < 4) return error.BatchTooShort;
    var off: usize = 0;

    const name_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + name_len > data.len) return error.Truncated;
    const index_name = try alloc.dupe(u8, data[off..][0..name_len]);
    off += name_len;

    if (off + 2 > data.len) return error.Truncated;
    const dimension = std.mem.readInt(u16, data[off..][0..2], .little);
    off += 2;

    if (off + 4 > data.len) return error.Truncated;
    const count = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;

    var entries = try ArrayList(EmbeddingEntry).initCapacity(alloc, count);
    errdefer entries.deinit(alloc);

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (off + 4 > data.len) return error.Truncated;
        const key_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + key_len > data.len) return error.Truncated;
        const doc_key = try alloc.dupe(u8, data[off..][0..key_len]);
        off += key_len;

        if (off + 8 > data.len) return error.Truncated;
        const hash_id = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;

        const vec_bytes = @as(usize, dimension) * 4;
        if (off + vec_bytes > data.len) return error.Truncated;
        const vec = try alloc.alloc(f32, dimension);
        for (0..dimension) |j| {
            const bits = std.mem.readInt(u32, data[off..][0..4], .little);
            vec[j] = @bitCast(bits);
            off += 4;
        }

        try entries.append(alloc, .{
            .doc_key = doc_key,
            .hash_id = hash_id,
            .vector = vec,
        });
    }

    return .{
        .index_name = index_name,
        .dimension = dimension,
        .entries = try entries.toOwnedSlice(alloc),
    };
}

pub fn encodeSparseBatch(alloc: Allocator, index_name: []const u8, entries: []const SparseEntry) ![]u8 {
    var buf: ArrayList(u8) = .empty;
    errdefer buf.deinit(alloc);

    try appendU32LE(&buf, alloc, @intCast(index_name.len));
    try buf.appendSlice(alloc, index_name);
    try appendU32LE(&buf, alloc, @intCast(entries.len));

    for (entries) |e| {
        try appendU32LE(&buf, alloc, @intCast(e.doc_key.len));
        try buf.appendSlice(alloc, e.doc_key);
        try appendU64LE(&buf, alloc, e.hash_id);
        try appendU32LE(&buf, alloc, @intCast(e.indices.len));
        for (e.indices) |idx| {
            try appendU32LE(&buf, alloc, idx);
        }
        for (e.values) |v| {
            try appendU32LE(&buf, alloc, @bitCast(v));
        }
    }

    return buf.toOwnedSlice(alloc);
}

pub fn encodeEdgeBatch(alloc: Allocator, index_name: []const u8, entries: []const EdgeEntry) ![]u8 {
    var buf: ArrayList(u8) = .empty;
    errdefer buf.deinit(alloc);

    try appendU32LE(&buf, alloc, @intCast(index_name.len));
    try buf.appendSlice(alloc, index_name);
    try appendU32LE(&buf, alloc, @intCast(entries.len));

    for (entries) |e| {
        try appendU32LE(&buf, alloc, @intCast(e.source_key.len));
        try buf.appendSlice(alloc, e.source_key);
        try appendU32LE(&buf, alloc, @intCast(e.target_key.len));
        try buf.appendSlice(alloc, e.target_key);
        try appendU32LE(&buf, alloc, @intCast(e.edge_type.len));
        try buf.appendSlice(alloc, e.edge_type);
        try appendU32LE(&buf, alloc, @intCast(e.value.len));
        try buf.appendSlice(alloc, e.value);
    }

    return buf.toOwnedSlice(alloc);
}

pub fn decodeSparseBatch(alloc: Allocator, data: []const u8) !struct {
    index_name: []const u8,
    entries: []SparseEntry,
} {
    if (data.len < 4) return error.BatchTooShort;
    var off: usize = 0;

    const name_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + name_len > data.len) return error.Truncated;
    const index_name = try alloc.dupe(u8, data[off..][0..name_len]);
    off += name_len;

    if (off + 4 > data.len) return error.Truncated;
    const count = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;

    var entries = try ArrayList(SparseEntry).initCapacity(alloc, count);
    errdefer entries.deinit(alloc);

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (off + 4 > data.len) return error.Truncated;
        const key_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + key_len > data.len) return error.Truncated;
        const doc_key = try alloc.dupe(u8, data[off..][0..key_len]);
        off += key_len;

        if (off + 8 > data.len) return error.Truncated;
        const hash_id = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;

        if (off + 4 > data.len) return error.Truncated;
        const nnz = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;

        if (off + nnz * 4 > data.len) return error.Truncated;
        const indices = try alloc.alloc(u32, nnz);
        for (0..nnz) |j| {
            indices[j] = std.mem.readInt(u32, data[off..][0..4], .little);
            off += 4;
        }

        if (off + nnz * 4 > data.len) return error.Truncated;
        const values = try alloc.alloc(f32, nnz);
        for (0..nnz) |j| {
            values[j] = @bitCast(std.mem.readInt(u32, data[off..][0..4], .little));
            off += 4;
        }

        try entries.append(alloc, .{
            .doc_key = doc_key,
            .hash_id = hash_id,
            .indices = indices,
            .values = values,
        });
    }

    return .{
        .index_name = index_name,
        .entries = try entries.toOwnedSlice(alloc),
    };
}

pub fn decodeShardHeader(alloc: Allocator, data: []const u8) !ShardHeaderEntry {
    if (data.len < 4) return error.BatchTooShort;
    var off: usize = 0;

    const name_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + name_len > data.len) return error.Truncated;
    const table_name = try alloc.dupe(u8, data[off..][0..name_len]);
    off += name_len;

    if (off + 4 > data.len) return error.Truncated;
    const shard_id = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;

    if (off + 4 > data.len) return error.Truncated;
    const sk_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + sk_len > data.len) return error.Truncated;
    const start_key = try alloc.dupe(u8, data[off..][0..sk_len]);
    off += sk_len;

    if (off + 4 > data.len) return error.Truncated;
    const ek_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + ek_len > data.len) return error.Truncated;
    const end_key = try alloc.dupe(u8, data[off..][0..ek_len]);

    return .{
        .table_name = table_name,
        .shard_id = shard_id,
        .start_key = start_key,
        .end_key = end_key,
    };
}

pub fn encodeShardHeader(alloc: Allocator, h: ShardHeaderEntry) ![]u8 {
    var buf: ArrayList(u8) = .empty;
    errdefer buf.deinit(alloc);

    try appendU32LE(&buf, alloc, @intCast(h.table_name.len));
    try buf.appendSlice(alloc, h.table_name);
    try appendU32LE(&buf, alloc, h.shard_id);
    try appendU32LE(&buf, alloc, @intCast(h.start_key.len));
    try buf.appendSlice(alloc, h.start_key);
    try appendU32LE(&buf, alloc, @intCast(h.end_key.len));
    try buf.appendSlice(alloc, h.end_key);

    return buf.toOwnedSlice(alloc);
}

pub fn encodeShardFooter(f: ShardFooterEntry) [36]u8 {
    var buf: [36]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], f.shard_id, .little);
    std.mem.writeInt(u64, buf[4..12], f.document_count, .little);
    std.mem.writeInt(u64, buf[12..20], f.embedding_count, .little);
    std.mem.writeInt(u64, buf[20..28], f.edge_count, .little);
    std.mem.writeInt(u64, buf[28..36], f.transaction_count, .little);
    return buf;
}

pub fn decodeShardFooter(data: []const u8) !ShardFooterEntry {
    if (data.len < 36) return error.BatchTooShort;
    return .{
        .shard_id = std.mem.readInt(u32, data[0..4], .little),
        .document_count = std.mem.readInt(u64, data[4..12], .little),
        .embedding_count = std.mem.readInt(u64, data[12..20], .little),
        .edge_count = std.mem.readInt(u64, data[20..28], .little),
        .transaction_count = std.mem.readInt(u64, data[28..36], .little),
    };
}

pub fn encodeFileFooter(f: FileFooterEntry) [24]u8 {
    var buf: [24]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], f.table_count, .little);
    std.mem.writeInt(u32, buf[4..8], f.shard_count, .little);
    std.mem.writeInt(u64, buf[8..16], f.total_documents, .little);
    std.mem.writeInt(u64, buf[16..24], f.total_bytes, .little);
    return buf;
}

pub fn decodeFileFooter(data: []const u8) !FileFooterEntry {
    if (data.len < 24) return error.BatchTooShort;
    return .{
        .table_count = std.mem.readInt(u32, data[0..4], .little),
        .shard_count = std.mem.readInt(u32, data[4..8], .little),
        .total_documents = std.mem.readInt(u64, data[8..16], .little),
        .total_bytes = std.mem.readInt(u64, data[16..24], .little),
    };
}

/// Returns true if the first 8 bytes match the AFB magic.
pub fn isAfbFormat(header: []const u8) bool {
    if (header.len < 8) return false;
    return std.mem.eql(u8, header[0..8], &magic);
}

// ---- Tests ----

test "header round-trip" {
    const alloc = std.testing.allocator;
    var buf: ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    const backup_id = [16]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    try writeHeader(&buf, alloc, .{
        .format_version = format_version,
        .flags = 0,
        .created_at_ns = 1234567890,
        .backup_id = backup_id,
        .table_count = 2,
        .shard_count = 5,
    });

    var reader = SliceReader.init(buf.items);
    const h = try reader.readHeader();

    try std.testing.expectEqual(format_version, h.format_version);
    try std.testing.expectEqual(backup_id, h.backup_id);
    try std.testing.expectEqual(@as(u32, 2), h.table_count);
    try std.testing.expectEqual(@as(u32, 5), h.shard_count);
}

test "header CRC validation" {
    const alloc = std.testing.allocator;
    var buf: ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    try writeHeader(&buf, alloc, .{
        .format_version = format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = .{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });

    // Corrupt a byte in the header
    buf.items[20] ^= 0xFF;

    var reader = SliceReader.init(buf.items);
    try std.testing.expectError(error.HeaderCrcMismatch, reader.readHeader());
}

test "block round-trip uncompressed" {
    const alloc = std.testing.allocator;
    var buf: ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    try writeHeader(&buf, alloc, .{
        .format_version = format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = .{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });

    const payload = "{\"tables\":[\"t1\"]}";
    try writeBlock(&buf, alloc, .cluster_manifest, payload);

    var reader = SliceReader.init(buf.items);
    _ = try reader.readHeader();

    const block = try reader.readBlock(alloc);
    defer alloc.free(block.payload);

    try std.testing.expectEqual(BlockType.cluster_manifest, block.block_type);
    try std.testing.expectEqualStrings(payload, block.payload);
}

test "block CRC validation" {
    const alloc = std.testing.allocator;
    var buf: ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    try writeHeader(&buf, alloc, .{
        .format_version = format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = .{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    try writeBlock(&buf, alloc, .cluster_manifest, "{}");

    // Corrupt the payload (first byte after 6-byte envelope header, after 64-byte file header)
    buf.items[header_size + 6] ^= 0xFF;

    var reader = SliceReader.init(buf.items);
    _ = try reader.readHeader();
    try std.testing.expectError(error.BlockCrcMismatch, reader.readBlock(alloc));
}

test "document batch round-trip" {
    const alloc = std.testing.allocator;

    const entries = [_]DocumentEntry{
        .{ .key = "doc1", .value_flags = 0, .value = "{\"title\":\"Hello\"}", .timestamp_ns = 0 },
        .{ .key = "doc2", .value_flags = doc_value_flag_compressed, .value = &.{ 0x28, 0xb5, 0x2f, 0xfd }, .timestamp_ns = 1234567890 },
    };

    const encoded = try encodeDocumentBatch(alloc, &entries);
    defer alloc.free(encoded);

    const decoded = try decodeDocumentBatch(alloc, encoded);
    defer {
        for (decoded) |e| {
            alloc.free(e.key);
            alloc.free(e.value);
        }
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqualStrings("doc1", decoded[0].key);
    try std.testing.expectEqual(@as(u8, 0), decoded[0].value_flags);
    try std.testing.expectEqualStrings("{\"title\":\"Hello\"}", decoded[0].value);
    try std.testing.expectEqual(@as(u64, 0), decoded[0].timestamp_ns);

    try std.testing.expectEqualStrings("doc2", decoded[1].key);
    try std.testing.expectEqual(doc_value_flag_compressed, decoded[1].value_flags);
    try std.testing.expectEqual(@as(u64, 1234567890), decoded[1].timestamp_ns);
}

test "embedding batch round-trip" {
    const alloc = std.testing.allocator;

    const entries = [_]EmbeddingEntry{
        .{ .doc_key = "doc1", .hash_id = 42, .vector = &.{ 1.0, 2.0, 3.0, 4.0 } },
        .{ .doc_key = "doc2", .hash_id = 99, .vector = &.{ 0.5, -0.5, 0.0, 1.0 } },
    };

    const encoded = try encodeEmbeddingBatch(alloc, "my_index", 4, &entries);
    defer alloc.free(encoded);

    const result = try decodeEmbeddingBatch(alloc, encoded);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |e| {
            alloc.free(e.doc_key);
            alloc.free(e.vector);
        }
        alloc.free(result.entries);
    }

    try std.testing.expectEqualStrings("my_index", result.index_name);
    try std.testing.expectEqual(@as(u16, 4), result.dimension);
    try std.testing.expectEqual(@as(usize, 2), result.entries.len);
    try std.testing.expectEqualStrings("doc1", result.entries[0].doc_key);
    try std.testing.expectEqual(@as(u64, 42), result.entries[0].hash_id);
    try std.testing.expectEqual(@as(f32, 1.0), result.entries[0].vector[0]);
    try std.testing.expectEqual(@as(f32, 2.0), result.entries[0].vector[1]);
}

test "shard footer round-trip" {
    const f = ShardFooterEntry{
        .shard_id = 3,
        .document_count = 1000,
        .embedding_count = 500,
        .edge_count = 200,
        .transaction_count = 0,
    };
    const encoded = encodeShardFooter(f);
    const decoded = try decodeShardFooter(&encoded);
    try std.testing.expectEqual(f, decoded);
}

test "file footer round-trip" {
    const f = FileFooterEntry{
        .table_count = 2,
        .shard_count = 4,
        .total_documents = 10000,
        .total_bytes = 5000000,
    };
    const encoded = encodeFileFooter(f);
    const decoded = try decodeFileFooter(&encoded);
    try std.testing.expectEqual(f, decoded);
}

test "isAfbFormat" {
    try std.testing.expect(isAfbFormat(&magic));
    try std.testing.expect(!isAfbFormat(&.{ 0x28, 0xb5, 0x2f, 0xfd, 0, 0, 0, 0 }));
    try std.testing.expect(!isAfbFormat("short"));
}

test "float32 LE cross-backend contract" {
    const cases = [_]struct { val: f32, bits: u32 }{
        .{ .val = 1.0, .bits = 0x3f800000 },
        .{ .val = -1.0, .bits = 0xbf800000 },
        .{ .val = 0.0, .bits = 0x00000000 },
        .{ .val = std.math.floatMax(f32), .bits = 0x7f7fffff },
    };

    for (cases) |tc| {
        const encoded: u32 = @bitCast(tc.val);
        try std.testing.expectEqual(tc.bits, encoded);

        const decoded: f32 = @bitCast(tc.bits);
        try std.testing.expectEqual(tc.val, decoded);
    }
}

test "sparse batch round-trip" {
    const alloc = std.testing.allocator;

    const entries = [_]SparseEntry{
        .{ .doc_key = "doc1", .hash_id = 100, .indices = &.{ 5, 20, 100 }, .values = &.{ 1.5, 2.0, 0.3 } },
        .{ .doc_key = "doc2", .hash_id = 200, .indices = &.{10}, .values = &.{3.14} },
    };

    const encoded = try encodeSparseBatch(alloc, "my_sparse", &entries);
    defer alloc.free(encoded);

    const result = try decodeSparseBatch(alloc, encoded);
    defer {
        alloc.free(result.index_name);
        for (result.entries) |e| {
            alloc.free(e.doc_key);
            alloc.free(e.indices);
            alloc.free(e.values);
        }
        alloc.free(result.entries);
    }

    try std.testing.expectEqualStrings("my_sparse", result.index_name);
    try std.testing.expectEqual(@as(usize, 2), result.entries.len);
    try std.testing.expectEqualStrings("doc1", result.entries[0].doc_key);
    try std.testing.expectEqual(@as(u64, 100), result.entries[0].hash_id);
    try std.testing.expectEqual(@as(usize, 3), result.entries[0].indices.len);
    try std.testing.expectEqual(@as(u32, 5), result.entries[0].indices[0]);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), result.entries[0].values[0], 1e-6);
}

test "shard header round-trip" {
    const alloc = std.testing.allocator;

    const encoded = try encodeShardHeader(alloc, .{
        .table_name = "wiki",
        .shard_id = 42,
        .start_key = "abc",
        .end_key = "xyz",
    });
    defer alloc.free(encoded);

    const decoded = try decodeShardHeader(alloc, encoded);
    defer {
        alloc.free(decoded.table_name);
        alloc.free(decoded.start_key);
        alloc.free(decoded.end_key);
    }

    try std.testing.expectEqualStrings("wiki", decoded.table_name);
    try std.testing.expectEqual(@as(u32, 42), decoded.shard_id);
    try std.testing.expectEqualStrings("abc", decoded.start_key);
    try std.testing.expectEqualStrings("xyz", decoded.end_key);
}

test "cross-backend AFB fixture from Go" {
    const alloc = std.testing.allocator;

    // Embed the fixture written by Go's TestAFBCrossBackendFixture
    const fixture = @embedFile("testdata/cross_backend_v1.afb");
    var reader = SliceReader.init(fixture);

    // -- File header --
    const hdr = try reader.readHeader();
    try std.testing.expectEqual(@as(u32, 1), hdr.format_version);
    try std.testing.expectEqual(@as(u32, 0), hdr.flags); // no compression
    try std.testing.expectEqual(@as(i64, 1700000000_000000000), hdr.created_at_ns);
    try std.testing.expectEqual(@as(u32, 1), hdr.table_count);
    try std.testing.expectEqual(@as(u32, 2), hdr.shard_count);

    // Expected backup ID: cafebabe-dead-beef-0123-456789abcdef
    const expected_uuid = [16]u8{ 0xca, 0xfe, 0xba, 0xbe, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef };
    try std.testing.expectEqual(expected_uuid, hdr.backup_id);

    // -- Track counts across shards --
    var total_docs: usize = 0;
    var total_embeddings: usize = 0;
    var total_sparse: usize = 0;
    var total_edges: usize = 0;
    var shard_headers_seen: usize = 0;
    var shard_footers_seen: usize = 0;
    var got_cluster_manifest = false;
    var got_table_manifest = false;
    var got_file_footer = false;

    // Expected documents by key
    const ExpectedDoc = struct { title: []const u8, born: []const u8 };
    const expected_docs = [_]struct { key: []const u8, doc: ExpectedDoc }{
        .{ .key = "albert-einstein", .doc = .{ .title = "Albert Einstein", .born = "1879" } },
        .{ .key = "alan-turing", .doc = .{ .title = "Alan Turing", .born = "1912" } },
        .{ .key = "ada-lovelace", .doc = .{ .title = "Ada Lovelace", .born = "1815" } },
        .{ .key = "marie-curie", .doc = .{ .title = "Marie Curie", .born = "1867" } },
        .{ .key = "nikola-tesla", .doc = .{ .title = "Nikola Tesla", .born = "1856" } },
    };

    // Read all blocks
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);

        switch (block.block_type) {
            .cluster_manifest => {
                got_cluster_manifest = true;
                // Verify it contains expected fields
                try std.testing.expect(std.mem.indexOf(u8, block.payload, "cafebabe") != null);
                try std.testing.expect(std.mem.indexOf(u8, block.payload, "\"source_backend\":\"go\"") != null);
            },
            .table_manifest => {
                got_table_manifest = true;
                try std.testing.expect(std.mem.indexOf(u8, block.payload, "\"name\":\"wiki\"") != null);
            },
            .shard_header => {
                const sh = try decodeShardHeader(alloc, block.payload);
                defer {
                    alloc.free(sh.table_name);
                    alloc.free(sh.start_key);
                    alloc.free(sh.end_key);
                }
                try std.testing.expectEqualStrings("wiki", sh.table_name);

                if (shard_headers_seen == 0) {
                    try std.testing.expectEqual(@as(u32, 1), sh.shard_id);
                    try std.testing.expectEqualStrings("a", sh.start_key);
                    try std.testing.expectEqualStrings("m", sh.end_key);
                } else {
                    try std.testing.expectEqual(@as(u32, 2), sh.shard_id);
                    try std.testing.expectEqualStrings("m", sh.start_key);
                    try std.testing.expectEqualStrings("{", sh.end_key);
                }
                shard_headers_seen += 1;
            },
            .document_batch => {
                const docs = try decodeDocumentBatch(alloc, block.payload);
                defer {
                    for (docs) |e| {
                        alloc.free(e.key);
                        alloc.free(e.value);
                    }
                    alloc.free(docs);
                }

                for (docs) |doc| {
                    // Verify this key is expected and the value contains the expected title
                    var found = false;
                    for (expected_docs) |exp| {
                        if (std.mem.eql(u8, doc.key, exp.key)) {
                            found = true;
                            try std.testing.expect(std.mem.indexOf(u8, doc.value, exp.doc.title) != null);
                            try std.testing.expect(std.mem.indexOf(u8, doc.value, exp.doc.born) != null);
                            break;
                        }
                    }
                    try std.testing.expect(found);
                }
                total_docs += docs.len;
            },
            .embedding_batch => {
                const result = try decodeEmbeddingBatch(alloc, block.payload);
                defer {
                    alloc.free(result.index_name);
                    for (result.entries) |e| {
                        alloc.free(e.doc_key);
                        alloc.free(e.vector);
                    }
                    alloc.free(result.entries);
                }

                try std.testing.expectEqualStrings("emb_v0", result.index_name);
                try std.testing.expectEqual(@as(u16, 4), result.dimension);

                for (result.entries) |e| {
                    try std.testing.expectEqual(@as(usize, 4), e.vector.len);
                    // Verify hash_id is in expected range (1001-1005)
                    try std.testing.expect(e.hash_id >= 1001 and e.hash_id <= 1005);
                }
                total_embeddings += result.entries.len;
            },
            .sparse_batch => {
                const result = try decodeSparseBatch(alloc, block.payload);
                defer {
                    alloc.free(result.index_name);
                    for (result.entries) |e| {
                        alloc.free(e.doc_key);
                        alloc.free(e.indices);
                        alloc.free(e.values);
                    }
                    alloc.free(result.entries);
                }

                try std.testing.expectEqualStrings("sparse_v0", result.index_name);

                for (result.entries) |e| {
                    // Verify nnz matches between indices and values
                    try std.testing.expectEqual(e.indices.len, e.values.len);
                    try std.testing.expect(e.indices.len > 0);
                    // hash_id in expected range (2001-2005)
                    try std.testing.expect(e.hash_id >= 2001 and e.hash_id <= 2005);
                    // All entries include term index 10
                    try std.testing.expectEqual(@as(u32, 10), e.indices[0]);
                }
                total_sparse += result.entries.len;
            },
            .edge_batch => {
                const result = try decodeEdgeBatch(alloc, block.payload);
                defer {
                    alloc.free(result.index_name);
                    for (result.entries) |e| {
                        alloc.free(e.source_key);
                        alloc.free(e.target_key);
                        alloc.free(e.edge_type);
                        alloc.free(e.value);
                    }
                    alloc.free(result.entries);
                }

                try std.testing.expectEqualStrings("links", result.index_name);

                for (result.entries) |e| {
                    // source and target should be non-empty
                    try std.testing.expect(e.source_key.len > 0);
                    try std.testing.expect(e.target_key.len > 0);
                    try std.testing.expect(e.edge_type.len > 0);
                }
                total_edges += result.entries.len;
            },
            .shard_footer => {
                shard_footers_seen += 1;
            },
            .file_footer => {
                got_file_footer = true;
            },
            else => {},
        }
    }

    // Verify totals match expected fixture content
    try std.testing.expectEqual(@as(usize, 5), total_docs);
    try std.testing.expectEqual(@as(usize, 5), total_embeddings);
    try std.testing.expectEqual(@as(usize, 5), total_sparse);
    try std.testing.expectEqual(@as(usize, 5), total_edges);
    try std.testing.expectEqual(@as(usize, 2), shard_headers_seen);
    try std.testing.expectEqual(@as(usize, 2), shard_footers_seen);
    try std.testing.expect(got_cluster_manifest);
    try std.testing.expect(got_table_manifest);
    try std.testing.expect(got_file_footer);
}

// Reuse the edge batch decoder from portable_backup.zig for the cross-backend test.
fn decodeEdgeBatch(alloc: Allocator, data: []const u8) !struct {
    index_name: []const u8,
    entries: []EdgeEntry,
} {
    if (data.len < 4) return error.BatchTooShort;
    var off: usize = 0;

    const name_len = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;
    if (off + name_len > data.len) return error.Truncated;
    const index_name = try alloc.dupe(u8, data[off..][0..name_len]);
    off += name_len;

    if (off + 4 > data.len) return error.Truncated;
    const count = std.mem.readInt(u32, data[off..][0..4], .little);
    off += 4;

    var entries = try ArrayList(EdgeEntry).initCapacity(alloc, count);
    errdefer entries.deinit(alloc);

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (off + 4 > data.len) return error.Truncated;
        const src_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + src_len > data.len) return error.Truncated;
        const source_key = try alloc.dupe(u8, data[off..][0..src_len]);
        off += src_len;

        if (off + 4 > data.len) return error.Truncated;
        const tgt_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + tgt_len > data.len) return error.Truncated;
        const target_key = try alloc.dupe(u8, data[off..][0..tgt_len]);
        off += tgt_len;

        if (off + 4 > data.len) return error.Truncated;
        const et_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + et_len > data.len) return error.Truncated;
        const edge_type = try alloc.dupe(u8, data[off..][0..et_len]);
        off += et_len;

        if (off + 4 > data.len) return error.Truncated;
        const val_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + val_len > data.len) return error.Truncated;
        const value = try alloc.dupe(u8, data[off..][0..val_len]);
        off += val_len;

        try entries.append(alloc, .{
            .source_key = source_key,
            .target_key = target_key,
            .edge_type = edge_type,
            .value = value,
        });
    }

    return .{
        .index_name = index_name,
        .entries = try entries.toOwnedSlice(alloc),
    };
}
