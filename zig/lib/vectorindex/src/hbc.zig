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
pub const hbc_index_version: u32 = 1;

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
        };
    }

    pub const encoded_size = 4 + 4 + 4 + 4 + 8 + 8 + 8 + 1 + 8 + 1;
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

pub fn encodeNodeKeyPrefix(buf: *[2]u8) []u8 {
    buf[0] = 'n';
    buf[1] = ':';
    return buf;
}

pub const DecodedNodeKey = struct {
    id: u64,
    suffix: Suffix,
};

pub fn decodeNodeKey(key: []const u8) ?DecodedNodeKey {
    if (key.len != 12) return null;
    if (key[0] != 'n' or key[1] != ':' or key[10] != ':') return null;
    const suffix: Suffix = switch (key[11]) {
        @intFromEnum(Suffix.header) => .header,
        @intFromEnum(Suffix.centroid) => .centroid,
        @intFromEnum(Suffix.children) => .children,
        @intFromEnum(Suffix.members) => .members,
        @intFromEnum(Suffix.packed_node) => .packed_node,
        @intFromEnum(Suffix.range) => .range,
        @intFromEnum(Suffix.posting) => .posting,
        else => return null,
    };
    return .{
        .id = std.mem.readInt(u64, key[2..10], .big),
        .suffix = suffix,
    };
}

pub fn nodeKeyPrefixMatches(key: []const u8) bool {
    return key.len >= 2 and key[0] == 'n' and key[1] == ':';
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

pub fn encodeAssignmentKey(buf: *[10]u8, vector_id: u64) []u8 {
    buf[0] = 'A';
    buf[1] = 'M';
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

pub fn encodePostingBaseKey(buf: *[10]u8, posting_id: u64) []u8 {
    buf[0] = 'P';
    buf[1] = 'B';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, posting_id));
    return buf;
}

pub fn encodePostingDeltaKey(buf: *[18]u8, posting_id: u64, sequence: u64) []u8 {
    buf[0] = 'P';
    buf[1] = 'D';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, posting_id));
    buf[10..18].* = @bitCast(std.mem.nativeToBig(u64, sequence));
    return buf;
}

pub fn encodePostingDeltaPrefix(buf: *[10]u8, posting_id: u64) []u8 {
    buf[0] = 'P';
    buf[1] = 'D';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, posting_id));
    return buf;
}

pub fn encodeCentroidDirectoryKey(buf: *[10]u8, posting_id: u64) []u8 {
    buf[0] = 'C';
    buf[1] = 'D';
    buf[2..10].* = @bitCast(std.mem.nativeToBig(u64, posting_id));
    return buf;
}

pub fn encodeCentroidDirectoryPrefix(buf: *[2]u8) []u8 {
    buf[0] = 'C';
    buf[1] = 'D';
    return buf;
}

pub fn centroidDirectoryKeyMatches(key: []const u8) bool {
    return key.len == 10 and key[0] == 'C' and key[1] == 'D';
}

pub fn postingDeltaKeyMatchesPosting(key: []const u8, posting_id: u64) bool {
    if (key.len != 18) return false;
    var prefix_buf: [10]u8 = undefined;
    return std.mem.eql(u8, key[0..10], encodePostingDeltaPrefix(&prefix_buf, posting_id));
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

pub const packed_node_magic = "HBN1";
pub const packed_node_header_size = 4 + NodeHeader.encoded_size + 4 + 4;

pub const PackedNodeValue = struct {
    header: NodeHeader,
    centroid_bytes: []const u8,
    ids_bytes: []const u8,
};

pub fn packedNodeValueSize(centroid_len: usize, ids_len: usize) usize {
    return packed_node_header_size + centroid_len + ids_len;
}

pub fn encodePackedNodeValue(
    buf: []u8,
    header: NodeHeader,
    centroid_bytes: []const u8,
    ids_bytes: []const u8,
) ![]u8 {
    const needed = packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    if (buf.len < needed) return error.BufferTooSmall;
    @memcpy(buf[0..4], packed_node_magic);
    var hdr_buf: [NodeHeader.encoded_size]u8 = undefined;
    @memcpy(buf[4..][0..NodeHeader.encoded_size], header.encode(&hdr_buf));
    var pos: usize = 4 + NodeHeader.encoded_size;
    writeU32LE(buf, &pos, @intCast(centroid_bytes.len));
    writeU32LE(buf, &pos, @intCast(ids_bytes.len));
    std.mem.copyForwards(u8, buf[pos..][0..centroid_bytes.len], centroid_bytes);
    pos += centroid_bytes.len;
    std.mem.copyForwards(u8, buf[pos..][0..ids_bytes.len], ids_bytes);
    pos += ids_bytes.len;
    return buf[0..pos];
}

pub fn decodePackedNodeValue(data: []const u8) !PackedNodeValue {
    if (data.len < packed_node_header_size) return error.Corrupted;
    if (!std.mem.eql(u8, data[0..4], packed_node_magic)) return error.Corrupted;
    const header = NodeHeader.decode(data[4..][0..NodeHeader.encoded_size]);
    var pos: usize = 4 + NodeHeader.encoded_size;
    const centroid_len: usize = @intCast(readU32LE(data, &pos));
    const ids_len: usize = @intCast(readU32LE(data, &pos));
    if (data.len != packed_node_header_size + centroid_len + ids_len) return error.Corrupted;
    return .{
        .header = header,
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

test "posting delta keys sort by posting id then sequence" {
    var a: [18]u8 = undefined;
    var b: [18]u8 = undefined;
    var c: [18]u8 = undefined;
    const key_a = encodePostingDeltaKey(&a, 7, 1);
    const key_b = encodePostingDeltaKey(&b, 7, 2);
    const key_c = encodePostingDeltaKey(&c, 8, 0);
    try std.testing.expect(std.mem.order(u8, key_a, key_b) == .lt);
    try std.testing.expect(std.mem.order(u8, key_b, key_c) == .lt);
}

test "posting delta key matcher accepts only current exact key shape" {
    var key_buf: [18]u8 = undefined;
    const key = encodePostingDeltaKey(&key_buf, 7, 1);
    try std.testing.expect(postingDeltaKeyMatchesPosting(key, 7));
    try std.testing.expect(!postingDeltaKeyMatchesPosting(key, 8));
    try std.testing.expect(!postingDeltaKeyMatchesPosting(key[0..17], 7));

    var extended: [19]u8 = undefined;
    @memcpy(extended[0..18], key);
    extended[18] = 0;
    try std.testing.expect(!postingDeltaKeyMatchesPosting(extended[0..], 7));
}

test "centroid directory keys are distinct from posting base keys" {
    var centroid_key_buf: [10]u8 = undefined;
    var base_key_buf: [10]u8 = undefined;
    const centroid_key = encodeCentroidDirectoryKey(&centroid_key_buf, 7);
    const base_key = encodePostingBaseKey(&base_key_buf, 7);
    try std.testing.expectEqualSlices(u8, "CD", centroid_key[0..2]);
    try std.testing.expect(!std.mem.eql(u8, centroid_key, base_key));
}

test "assignment keys are distinct from legacy vector leaf keys" {
    var assignment_key_buf: [10]u8 = undefined;
    var legacy_key_buf: [10]u8 = undefined;
    const assignment_key = encodeAssignmentKey(&assignment_key_buf, 7);
    const legacy_key = encodeVecLeafKey(&legacy_key_buf, 7);
    try std.testing.expectEqualSlices(u8, "AM", assignment_key[0..2]);
    try std.testing.expect(!std.mem.eql(u8, assignment_key, legacy_key));
}
