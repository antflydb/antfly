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

const std = @import("std");

pub const meta_key = "hbc:meta";
pub const hbc_index_version: u32 = 2;

pub const ProjectionCheckpointMetadata = struct {
    applied_sequence: u64 = 0,
    status: u8 = 1,
    generation: u64 = 0,
    config_hash: u64 = 0,
};

pub const IndexMetadata = struct {
    version: u32 = hbc_index_version,
    dims: u32,
    branching_factor: u32,
    leaf_size: u32,
    root_node: u64 = 1,
    active_count: u64 = 0,
    node_count: u64 = 1,
    use_quantization: bool = true,
    quantizer_seed: u64 = 42,
    metric: u8 = 0,
    projection_applied_sequence: u64 = 0,
    projection_status: u8 = 1,
    projection_generation: u64 = 0,
    projection_config_hash: u64 = 0,
    topology_rebuild_algorithm: u8 = std.math.maxInt(u8),
    topology_vector_base_generation: u64 = 0,
    topology_vector_wal_mutation_sequence: u64 = 0,

    pub fn encode(self: *const IndexMetadata, buf: []u8) []u8 {
        var pos: usize = 0;
        writeU32LE(buf, &pos, self.version);
        writeU32LE(buf, &pos, self.dims);
        writeU32LE(buf, &pos, self.branching_factor);
        writeU32LE(buf, &pos, self.leaf_size);
        writeU64LE(buf, &pos, self.root_node);
        writeU64LE(buf, &pos, self.active_count);
        writeU64LE(buf, &pos, self.node_count);
        buf[pos] = if (self.use_quantization) 1 else 0;
        pos += 1;
        writeU64LE(buf, &pos, self.quantizer_seed);
        buf[pos] = self.metric;
        pos += 1;
        writeU64LE(buf, &pos, self.projection_applied_sequence);
        buf[pos] = self.projection_status;
        pos += 1;
        writeU64LE(buf, &pos, self.projection_generation);
        writeU64LE(buf, &pos, self.projection_config_hash);
        buf[pos] = self.topology_rebuild_algorithm;
        pos += 1;
        writeU64LE(buf, &pos, self.topology_vector_base_generation);
        writeU64LE(buf, &pos, self.topology_vector_wal_mutation_sequence);
        return buf[0..pos];
    }

    pub fn decode(data: []const u8) IndexMetadata {
        var pos: usize = 0;
        const version = readU32LE(data, &pos);
        const dims = readU32LE(data, &pos);
        const branching_factor = readU32LE(data, &pos);
        const leaf_size = readU32LE(data, &pos);
        const root_node = readU64LE(data, &pos);
        const active_count = readU64LE(data, &pos);
        const node_count = readU64LE(data, &pos);
        const use_quant = data[pos] != 0;
        pos += 1;
        const seed = readU64LE(data, &pos);
        const metric = data[pos];
        pos += 1;
        const projection_applied_sequence = readU64LE(data, &pos);
        const projection_status = data[pos];
        pos += 1;
        const projection_generation = readU64LE(data, &pos);
        const projection_config_hash = readU64LE(data, &pos);
        const topology_rebuild_algorithm = if (data.len >= encoded_size) data[pos] else std.math.maxInt(u8);
        if (data.len >= encoded_size) pos += 1;
        const topology_vector_base_generation = if (data.len >= encoded_size) readU64LE(data, &pos) else 0;
        const topology_vector_wal_mutation_sequence = if (data.len >= encoded_size) readU64LE(data, &pos) else 0;
        return .{
            .version = version,
            .dims = dims,
            .branching_factor = branching_factor,
            .leaf_size = leaf_size,
            .root_node = root_node,
            .active_count = active_count,
            .node_count = node_count,
            .use_quantization = use_quant,
            .quantizer_seed = seed,
            .metric = metric,
            .projection_applied_sequence = projection_applied_sequence,
            .projection_status = projection_status,
            .projection_generation = projection_generation,
            .projection_config_hash = projection_config_hash,
            .topology_rebuild_algorithm = topology_rebuild_algorithm,
            .topology_vector_base_generation = topology_vector_base_generation,
            .topology_vector_wal_mutation_sequence = topology_vector_wal_mutation_sequence,
        };
    }

    pub fn projectionCheckpoint(self: *const IndexMetadata) ProjectionCheckpointMetadata {
        return .{
            .applied_sequence = self.projection_applied_sequence,
            .status = self.projection_status,
            .generation = self.projection_generation,
            .config_hash = self.projection_config_hash,
        };
    }

    pub fn setProjectionCheckpoint(self: *IndexMetadata, checkpoint: ProjectionCheckpointMetadata) void {
        self.projection_applied_sequence = checkpoint.applied_sequence;
        self.projection_status = checkpoint.status;
        self.projection_generation = checkpoint.generation;
        self.projection_config_hash = checkpoint.config_hash;
    }

    pub const legacy_encoded_size = 4 + 4 + 4 + 4 + 8 + 8 + 8 + 1 + 8 + 1 + 8 + 1 + 8 + 8;
    pub const encoded_size = legacy_encoded_size + 1 + 8 + 8;
};

pub const Suffix = enum(u8) {
    header = 'h',
    centroid = 'c',
    children = 'k',
    members = 'm',
    packed_node = 'p',
    range = 'r',
    posting = 's',
};

pub fn encodeNodeKey(buf: *[12]u8, node_id: u64, suffix: Suffix) []u8 {
    buf[0] = 'n';
    buf[1] = ':';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, node_id));
    buf[10] = ':';
    buf[11] = @intFromEnum(suffix);
    return buf;
}

pub fn encodeVecKey(buf: *[10]u8, vector_id: u64) []u8 {
    buf[0] = 'v';
    buf[1] = ':';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, vector_id));
    return buf;
}

pub fn encodeVecLeafKey(buf: *[10]u8, vector_id: u64) []u8 {
    buf[0] = 'l';
    buf[1] = ':';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, vector_id));
    return buf;
}

pub fn encodeVecMetaKey(buf: *[10]u8, vector_id: u64) []u8 {
    buf[0] = 'm';
    buf[1] = ':';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, vector_id));
    return buf;
}

pub fn encodeQuantKey(buf: *[10]u8, node_id: u64) []u8 {
    buf[0] = 'q';
    buf[1] = ':';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, node_id));
    return buf;
}

pub const NodeHeader = struct {
    is_leaf: bool,
    level: u16,
    parent: u64,

    pub fn encode(self: *const NodeHeader, buf: *[11]u8) []u8 {
        buf[0] = if (self.is_leaf) 1 else 0;
        buf[1..3].* = @bitCast(std.mem.nativeToLittle(u16, self.level));
        buf[3..11].* = @bitCast(std.mem.nativeToLittle(u64, self.parent));
        return buf;
    }

    pub fn decode(data: []const u8) NodeHeader {
        return .{
            .is_leaf = data[0] != 0,
            .level = std.mem.readInt(u16, data[1..3], .little),
            .parent = std.mem.readInt(u64, data[3..11], .little),
        };
    }

    pub const encoded_size = 11;
};

pub const packed_node_magic_v1 = "HBN1";
pub const packed_node_magic = "HBN2";
pub const packed_node_header_size_v1 = 4 + NodeHeader.encoded_size + 4 + 4;
pub const packed_node_header_size = 4 + NodeHeader.encoded_size + @sizeOf(f32) + 4 + 4;

pub const PackedNodeValue = struct {
    header: NodeHeader,
    covering_radius: f32,
    centroid_bytes: []const u8,
    ids_bytes: []const u8,
};

pub fn packedNodeValueSize(centroid_len: usize, ids_len: usize) usize {
    return packed_node_header_size + centroid_len + ids_len;
}

pub fn encodePackedNodeValue(
    buf: []u8,
    header: NodeHeader,
    covering_radius: f32,
    centroid_bytes: []const u8,
    ids_bytes: []const u8,
) ![]u8 {
    const needed = packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    if (buf.len < needed) return error.BufferTooSmall;
    @memcpy(buf[0..4], packed_node_magic);
    var hdr_buf: [NodeHeader.encoded_size]u8 = undefined;
    @memcpy(buf[4..][0..NodeHeader.encoded_size], header.encode(&hdr_buf));
    var pos: usize = 4 + NodeHeader.encoded_size;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(covering_radius), .little);
    pos += 4;
    writeU32LE(buf, &pos, @intCast(centroid_bytes.len));
    writeU32LE(buf, &pos, @intCast(ids_bytes.len));
    std.mem.copyForwards(u8, buf[pos..][0..centroid_bytes.len], centroid_bytes);
    pos += centroid_bytes.len;
    std.mem.copyForwards(u8, buf[pos..][0..ids_bytes.len], ids_bytes);
    pos += ids_bytes.len;
    return buf[0..pos];
}

pub fn decodePackedNodeValue(data: []const u8) !PackedNodeValue {
    if (data.len < packed_node_header_size_v1) return error.Corrupted;
    const is_v2 = std.mem.eql(u8, data[0..4], packed_node_magic);
    const is_v1 = std.mem.eql(u8, data[0..4], packed_node_magic_v1);
    if (!is_v1 and !is_v2) return error.Corrupted;
    const header = NodeHeader.decode(data[4..][0..NodeHeader.encoded_size]);
    var pos: usize = 4 + NodeHeader.encoded_size;
    const covering_radius: f32 = if (is_v2) blk: {
        if (data.len < packed_node_header_size) return error.Corrupted;
        const bits = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk @bitCast(bits);
    } else std.math.nan(f32);
    const centroid_len: usize = @intCast(readU32LE(data, &pos));
    const ids_len: usize = @intCast(readU32LE(data, &pos));
    const header_size: usize = if (is_v2) packed_node_header_size else packed_node_header_size_v1;
    if (data.len != header_size + centroid_len + ids_len) return error.Corrupted;
    return .{
        .header = header,
        .covering_radius = covering_radius,
        .centroid_bytes = data[pos..][0..centroid_len],
        .ids_bytes = data[pos + centroid_len ..][0..ids_len],
    };
}

fn writeU32LE(buf: []u8, pos: *usize, val: u32) void {
    buf[pos.*..][0..4].* = @bitCast(std.mem.nativeToLittle(u32, val));
    pos.* += 4;
}

fn writeU64LE(buf: []u8, pos: *usize, val: u64) void {
    buf[pos.*..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, val));
    pos.* += 8;
}

fn readU32LE(data: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

fn readU64LE(data: []const u8, pos: *usize) u64 {
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

test "packed node v2 round trips covering radius" {
    const header = NodeHeader{ .is_leaf = true, .level = 3, .parent = 9 };
    const centroid = std.mem.sliceAsBytes(&[_]f32{ 1, 2 });
    const ids = std.mem.sliceAsBytes(&[_]u64{ 7, 8 });
    var storage: [128]u8 = undefined;
    const encoded = try encodePackedNodeValue(&storage, header, 4.5, centroid, ids);
    const decoded = try decodePackedNodeValue(encoded);
    try std.testing.expectEqual(@as(f32, 4.5), decoded.covering_radius);
    try std.testing.expectEqualSlices(u8, centroid, decoded.centroid_bytes);
    try std.testing.expectEqualSlices(u8, ids, decoded.ids_bytes);
}

test "packed node v1 decodes with unknown covering radius" {
    const header = NodeHeader{ .is_leaf = false, .level = 1, .parent = 0 };
    const centroid = std.mem.sliceAsBytes(&[_]f32{1});
    const ids = std.mem.sliceAsBytes(&[_]u64{2});
    var storage: [128]u8 = undefined;
    @memcpy(storage[0..4], packed_node_magic_v1);
    var header_storage: [NodeHeader.encoded_size]u8 = undefined;
    @memcpy(storage[4..][0..NodeHeader.encoded_size], header.encode(&header_storage));
    var pos: usize = 4 + NodeHeader.encoded_size;
    writeU32LE(&storage, &pos, @intCast(centroid.len));
    writeU32LE(&storage, &pos, @intCast(ids.len));
    @memcpy(storage[pos..][0..centroid.len], centroid);
    pos += centroid.len;
    @memcpy(storage[pos..][0..ids.len], ids);
    pos += ids.len;

    const decoded = try decodePackedNodeValue(storage[0..pos]);
    try std.testing.expect(std.math.isNan(decoded.covering_radius));
    try std.testing.expectEqualSlices(u8, centroid, decoded.centroid_bytes);
    try std.testing.expectEqualSlices(u8, ids, decoded.ids_bytes);
}
