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
const packet = @import("packet.zig");
const tile = @import("tile.zig");
const tagtree = @import("tagtree.zig");

pub const native_port_available = true;

/// Tier-1 encoded codeblock data ready for packet assembly.
pub const EncodedCodeblockInfo = struct {
    component_index: u16,
    resolution_index: u8,
    subband: tile.SubbandType,
    precinct_index: u32,
    codeblock_x: u32,
    codeblock_y: u32,
    data: []const u8,
    num_coding_passes: u16,
    zero_bit_planes: u8,
    pass_lengths: []const u32 = &.{},
};

/// A single assembled packet: header bytes followed by body bytes.
pub const EncodedPacket = struct {
    header: []u8,
    body: []u8,

    pub fn deinit(self: *EncodedPacket, allocator: std.mem.Allocator) void {
        allocator.free(self.header);
        allocator.free(self.body);
        self.* = undefined;
    }
};

/// Bit-level writer that produces JPEG 2000 packet header bytes.
///
/// After an 0xff byte, the most significant bit of the following byte is a
/// stuffed zero (the "bit-stuffing" rule from Annex D).  The writer tracks
/// this so that a corresponding `PacketHeaderBitReader` will recover the
/// original bit stream.
const PacketHeaderBitWriter = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    allocator: std.mem.Allocator,
    bit_index: u4 = 0,
    current_limit: u4 = 8,

    fn init(allocator: std.mem.Allocator) PacketHeaderBitWriter {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *PacketHeaderBitWriter) void {
        self.bytes.deinit(self.allocator);
    }

    fn writeBit(self: *PacketHeaderBitWriter, bit: u1) !void {
        if (self.bit_index == 0) {
            try self.bytes.append(self.allocator, 0);
        }
        const byte_ptr = &self.bytes.items[self.bytes.items.len - 1];
        // In the current byte, the stuffed-zero bit position (MSB) is
        // never written explicitly -- we skip it by starting bit_index at
        // 0 which maps to the MSB of the *usable* portion.  When
        // current_limit is 7 the MSB is reserved (stuffed zero).
        const shift: u3 = @intCast(self.current_limit - 1 - self.bit_index);
        byte_ptr.* |= @as(u8, bit) << shift;
        self.bit_index += 1;
        if (self.bit_index >= self.current_limit) {
            // Byte complete -- determine the limit for the next byte.
            self.current_limit = if (byte_ptr.* == 0xff) 7 else 8;
            self.bit_index = 0;
        }
    }

    fn writeBits(self: *PacketHeaderBitWriter, value: u32, nbits: u8) !void {
        if (nbits == 0) return;
        var remaining: u8 = nbits;
        while (remaining > 0) {
            remaining -= 1;
            const bit: u1 = @intCast((value >> @intCast(remaining)) & 1);
            try self.writeBit(bit);
        }
    }

    /// Byte-align and return the finished header bytes as an owned slice.
    fn finish(self: *PacketHeaderBitWriter) ![]u8 {
        // Pad remaining bits with zeros to byte-align.
        if (self.bit_index != 0) {
            // The remaining bits in the current byte are already zero
            // (we start each byte at 0).  Just advance past them.
            self.bit_index = 0;
            self.current_limit = if (self.bytes.items.len > 0 and
                self.bytes.items[self.bytes.items.len - 1] == 0xff) 7 else 8;
        }
        if (self.current_limit == 7) {
            try self.bytes.append(self.allocator, 0);
            self.current_limit = 8;
        }
        return self.bytes.toOwnedSlice(self.allocator);
    }
};

/// Encode the number of coding passes using the JPEG 2000 variable-length code.
///
/// The encoding matches the inverse of `codeblock.decodeNumCodingPasses`:
///   1       -> 0
///   2       -> 10
///   3       -> 1100
///   4       -> 1101
///   5       -> 1110
///   6..36   -> 1111 + 5-bit (count - 6)
///   37..164 -> 1111 11111 + 7-bit (count - 37)
fn encodeCodingPassCount(writer: *PacketHeaderBitWriter, count: u16) !void {
    if (count == 0) return error.InvalidCodingPassCount;
    if (count == 1) {
        try writer.writeBit(0);
        return;
    }
    // First bit: 1
    try writer.writeBit(1);
    if (count == 2) {
        try writer.writeBit(0);
        return;
    }
    // Second bit: 1
    try writer.writeBit(1);
    if (count <= 5) {
        // 2-bit suffix: 00=3, 01=4, 10=5
        try writer.writeBits(@as(u32, count - 3), 2);
        return;
    }
    // Two more 1-bits: 11
    try writer.writeBits(0b11, 2);
    if (count <= 36) {
        // 5-bit suffix encoding (count - 6), value 0..30
        try writer.writeBits(@as(u32, count - 6), 5);
        return;
    }
    if (count > 164) return error.InvalidCodingPassCount;
    // 5 bits all-ones (31) to signal extension, then 7-bit suffix
    try writer.writeBits(31, 5);
    try writer.writeBits(@as(u32, count - 37), 7);
}

/// Encode a comma code (unary): `value` one-bits followed by a zero-bit.
/// This is the inverse of `codeblock.decodeCommaCode`.
fn encodeCommaCode(writer: *PacketHeaderBitWriter, value: u8) !void {
    var remaining: u8 = value;
    while (remaining > 0) : (remaining -= 1) {
        try writer.writeBit(1);
    }
    try writer.writeBit(0);
}

/// Encode a tag tree value for a leaf, emitting bits from root to leaf.
///
/// This is the encoding counterpart to `TagTree.decodeBelowThreshold`.
/// For each node along the path from root to leaf, while the node's
/// encode_state is below the threshold, emit 0 if the node's value is
/// above the current state (not yet reached) or 1 if the value equals
/// the current state (reached).
fn encodeTagTreeValue(
    tree: *tagtree.TagTree,
    writer: *PacketHeaderBitWriter,
    x: usize,
    y: usize,
    threshold: u32,
) !void {
    const path = try tree.pathToRoot(x, y);
    // Walk from root (last index) down to leaf (first index).
    var low: u32 = 0;
    var i: usize = path.len;
    while (i > 0) {
        i -= 1;
        const index = path.indices[i];
        var state = tree.nodes[index].encode_state;
        if (state < low) state = low;
        while (state < threshold) {
            if (tree.nodes[index].value <= state) {
                // Value is at or below current state -- emit 1 (found).
                try writer.writeBit(1);
                state += 1;
                break;
            }
            // Value is above current state -- emit 0 (not yet).
            try writer.writeBit(0);
            state += 1;
        }
        tree.nodes[index].encode_state = state;
        low = state;
    }
}

/// Encode a tag tree value fully (used for zero-bit-planes).
/// Encodes the exact leaf value by running thresholds 1..value+1.
fn encodeTagTreeFullValue(
    tree: *tagtree.TagTree,
    writer: *PacketHeaderBitWriter,
    x: usize,
    y: usize,
    value: u32,
) !void {
    var threshold: u32 = 1;
    while (threshold <= value + 1) : (threshold += 1) {
        try encodeTagTreeValue(tree, writer, x, y, threshold);
    }
}

/// Encode a single packet's header and body.
///
/// A packet contains all codeblock contributions for a specific
/// (tile, layer, resolution, component, precinct) tuple.
///
/// The `codeblock_states` slice must have one entry per codeblock in this
/// packet, in the same order as `codeblocks`.  On return, each state is
/// updated to reflect the encoded information (included flag, lblock, etc.).
///
/// `layer_index` is the current layer being encoded (used for the inclusion
/// tag tree threshold).
pub fn encodePacket(
    allocator: std.mem.Allocator,
    codeblocks: []const EncodedCodeblockInfo,
    codeblock_states: []packet.CodeBlockState,
    inclusion_trees: []tagtree.TagTree,
    zero_bit_plane_trees: []tagtree.TagTree,
    tree_bindings: []const packet.PacketTreeBinding,
    layer_index: u16,
) !EncodedPacket {
    var writer = PacketHeaderBitWriter.init(allocator);
    errdefer writer.deinit();
    var body: std.ArrayListUnmanaged(u8) = .empty;
    errdefer body.deinit(allocator);

    // Packet present bit (1 = non-empty packet).
    try writer.writeBit(1);

    for (codeblocks, 0..) |cb, idx| {
        const state = &codeblock_states[idx];
        const binding = tree_bindings[idx];
        const contributing = cb.num_coding_passes > 0;

        if (!state.included) {
            if (contributing) {
                // First inclusion: encode via inclusion tag tree.
                // Set the leaf value to this layer, propagate, then encode.
                try inclusion_trees[binding.tree_index].setLeafValue(
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, layer_index),
                );
                try encodeTagTreeValue(
                    &inclusion_trees[binding.tree_index],
                    &writer,
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, layer_index) + 1,
                );

                // Zero bit planes: encode via zero-bit-plane tag tree.
                try zero_bit_plane_trees[binding.tree_index].setLeafValue(
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, cb.zero_bit_planes),
                );
                try encodeTagTreeFullValue(
                    &zero_bit_plane_trees[binding.tree_index],
                    &writer,
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, cb.zero_bit_planes),
                );

                state.included = true;
                state.first_layer_index = layer_index;
                state.zero_bit_planes = cb.zero_bit_planes;

                // Record encode thresholds so future packets skip already-coded nodes.
                try inclusion_trees[binding.tree_index].recordEncodedThreshold(
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, layer_index) + 1,
                );
                try zero_bit_plane_trees[binding.tree_index].recordEncodedThreshold(
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, cb.zero_bit_planes),
                );
            } else {
                // Not yet included and no contribution this layer: emit a
                // tag-tree run that proves the leaf's inclusion threshold
                // exceeds `layer_index + 1`. The leaf value is left at its
                // sentinel (maxInt or a later-layer value) so the walk emits
                // zeros.
                try encodeTagTreeValue(
                    &inclusion_trees[binding.tree_index],
                    &writer,
                    binding.leaf_x,
                    binding.leaf_y,
                    @as(u32, layer_index) + 1,
                );
                // No length, no body, no state change.
                continue;
            }
        } else {
            // Already included in a previous layer.
            if (contributing) {
                try writer.writeBit(1);
            } else {
                // No new passes this layer: single bit (0 = no contribution).
                try writer.writeBit(0);
                continue;
            }
        }

        // Number of coding passes (variable-length code).
        try encodeCodingPassCount(&writer, cb.num_coding_passes);

        // lblock increment (comma code).
        // Determine how much lblock must grow to encode the segment length.
        const base_lblock = state.lblock;
        const needed_lblock = computeRequiredLblock(cb.data.len, base_lblock, cb.num_coding_passes);
        const lblock_increment: u8 = needed_lblock - base_lblock;
        try encodeCommaCode(&writer, lblock_increment);

        // Segment length: log2(num_coding_passes) + lblock bits per the
        // packet-header contract used by the round-trip tests.
        const length_bits: u8 = std.math.log2_int(u16, cb.num_coding_passes) + needed_lblock;
        try writer.writeBits(@intCast(cb.data.len), length_bits);

        // Update state.
        state.lblock = needed_lblock;
        state.num_coding_passes +%= cb.num_coding_passes;

        // Body: append codeblock data.
        try body.appendSlice(allocator, cb.data);
    }

    const header_bytes = try writer.finish();
    errdefer allocator.free(header_bytes);
    const body_bytes = try body.toOwnedSlice(allocator);

    return .{
        .header = header_bytes,
        .body = body_bytes,
    };
}

/// Compute the minimum lblock so that the data length can be encoded.
///
/// The number of bits available for the length field is:
///   log2_int(num_coding_passes) + lblock
/// per ITU-T T.800 B.10.7. The length must fit in that many bits.
fn computeRequiredLblock(data_len: usize, base_lblock: u8, num_coding_passes: u16) u8 {
    const pass_bits: u8 = if (num_coding_passes <= 1) 0 else std.math.log2_int(u16, num_coding_passes);
    var lblock = base_lblock;
    while (true) {
        const length_bits: u8 = pass_bits + lblock;
        if (length_bits >= 32) return lblock;
        const max_encodable: u64 = (@as(u64, 1) << @intCast(length_bits)) - 1;
        if (data_len <= max_encodable) return lblock;
        lblock += 1;
    }
}

/// Encode an empty packet (packet not present).
pub fn encodeEmptyPacket(allocator: std.mem.Allocator) !EncodedPacket {
    var writer = PacketHeaderBitWriter.init(allocator);
    errdefer writer.deinit();

    // Packet present bit = 0 (empty).
    try writer.writeBit(0);

    const header_bytes = try writer.finish();
    const body_bytes = try allocator.alloc(u8, 0);

    return .{
        .header = header_bytes,
        .body = body_bytes,
    };
}

/// Options controlling error-resilience marker emission during tile-part
/// assembly. `packet_lengths`, when non-null, receives one u32 per emitted
/// packet (marker + header + body bytes) for PLT construction.
pub const AssembleOptions = struct {
    emit_sop: bool = false,
    emit_eph: bool = false,
    sop_counter: *u16 = undefined,
    packet_lengths: ?*std.ArrayListUnmanaged(u32) = null,
};

pub const AssembleResult = struct {
    /// Concatenated packet bitstream including any SOP/EPH markers.
    data: []u8,
};

/// Assemble all encoded codeblocks for a tile into a complete tile-part
/// bitstream using the requested progression order.
///
/// The returned slice contains all packet headers and bodies concatenated
/// in progression order, ready to be wrapped in a tile-part SOT/SOD.
pub fn assembleTileData(
    allocator: std.mem.Allocator,
    codeblocks: []const EncodedCodeblockInfo,
    num_layers: u16,
    num_resolutions: u8,
    num_components: u16,
    progression_order: u8,
) ![]u8 {
    var dummy_counter: u16 = 0;
    return assembleTileDataWithOptions(
        allocator,
        codeblocks,
        num_layers,
        num_resolutions,
        num_components,
        progression_order,
        .{ .sop_counter = &dummy_counter },
    );
}

pub fn assembleTileDataWithOptions(
    allocator: std.mem.Allocator,
    codeblocks: []const EncodedCodeblockInfo,
    num_layers: u16,
    num_resolutions: u8,
    num_components: u16,
    progression_order: u8,
    options: AssembleOptions,
) ![]u8 {
    var output: std.ArrayListUnmanaged(u8) = .empty;
    defer output.deinit(allocator);

    // Build per-codeblock state tracking.
    const states = try allocator.alloc(packet.CodeBlockState, codeblocks.len);
    defer allocator.free(states);
    for (codeblocks, 0..) |cb, idx| {
        states[idx] = .{
            .coordinate = .{
                .tile_index = 0,
                .layer_index = 0,
                .resolution_index = cb.resolution_index,
                .component_index = cb.component_index,
                .precinct_index = cb.precinct_index,
            },
            .subband = cb.subband,
            .subband_index = subbandToIndex(cb.subband, cb.resolution_index),
            .codeblock_index = 0,
            .codeblock_x = cb.codeblock_x,
            .codeblock_y = cb.codeblock_y,
        };
    }

    const TreeKey = struct {
        component_index: u16,
        resolution_index: u8,
        precinct_index: u32,
        subband_index: u8,
    };
    var tree_keys: std.ArrayListUnmanaged(TreeKey) = .empty;
    defer tree_keys.deinit(allocator);
    var tree_widths: std.ArrayListUnmanaged(usize) = .empty;
    defer tree_widths.deinit(allocator);
    var tree_heights: std.ArrayListUnmanaged(usize) = .empty;
    defer tree_heights.deinit(allocator);
    const state_bindings = try allocator.alloc(packet.PacketTreeBinding, codeblocks.len);
    defer allocator.free(state_bindings);

    for (codeblocks, 0..) |cb, idx| {
        const subband_index = subbandToIndex(cb.subband, cb.resolution_index);
        const key: TreeKey = .{
            .component_index = cb.component_index,
            .resolution_index = cb.resolution_index,
            .precinct_index = cb.precinct_index,
            .subband_index = subband_index,
        };
        var tree_index: ?usize = null;
        for (tree_keys.items, 0..) |existing, existing_index| {
            if (existing.component_index == key.component_index and
                existing.resolution_index == key.resolution_index and
                existing.precinct_index == key.precinct_index and
                existing.subband_index == key.subband_index)
            {
                tree_index = existing_index;
                tree_widths.items[existing_index] = @max(tree_widths.items[existing_index], @as(usize, cb.codeblock_x) + 1);
                tree_heights.items[existing_index] = @max(tree_heights.items[existing_index], @as(usize, cb.codeblock_y) + 1);
                break;
            }
        }
        if (tree_index == null) {
            try tree_keys.append(allocator, key);
            try tree_widths.append(allocator, @as(usize, cb.codeblock_x) + 1);
            try tree_heights.append(allocator, @as(usize, cb.codeblock_y) + 1);
            tree_index = tree_keys.items.len - 1;
        }
        state_bindings[idx] = .{
            .tree_index = tree_index.?,
            .leaf_x = cb.codeblock_x,
            .leaf_y = cb.codeblock_y,
        };
    }

    var inclusion_trees = try allocator.alloc(tagtree.TagTree, tree_keys.items.len);
    defer allocator.free(inclusion_trees);
    var zero_bit_plane_trees = try allocator.alloc(tagtree.TagTree, tree_keys.items.len);
    defer allocator.free(zero_bit_plane_trees);
    var trees_inited: usize = 0;
    defer {
        var ti: usize = 0;
        while (ti < trees_inited) : (ti += 1) {
            inclusion_trees[ti].deinit();
            zero_bit_plane_trees[ti].deinit();
        }
    }
    for (0..tree_keys.items.len) |ti| {
        inclusion_trees[ti] = try tagtree.TagTree.init(allocator, tree_widths.items[ti], tree_heights.items[ti]);
        zero_bit_plane_trees[ti] = try tagtree.TagTree.init(allocator, tree_widths.items[ti], tree_heights.items[ti]);
        trees_inited += 1;
        inclusion_trees[ti].setAllValues(std.math.maxInt(u32));
        zero_bit_plane_trees[ti].setAllValues(std.math.maxInt(u32));
    }

    switch (progression_order) {
        0 => {
            var layer_index: u16 = 0;
            while (layer_index < num_layers) : (layer_index += 1) {
                var res_index: u8 = 0;
                while (res_index < num_resolutions) : (res_index += 1) {
                    var comp_index: u16 = 0;
                    while (comp_index < num_components) : (comp_index += 1) {
                        const max_precinct = maxPrecinctIndexForResolutionAndComponent(codeblocks, res_index, comp_index);
                        var prec_index: u32 = 0;
                        while (prec_index < max_precinct) : (prec_index += 1) {
                            try emitPacketForCoordinate(
                                allocator,
                                &output,
                                codeblocks,
                                states,
                                layer_index,
                                num_layers,
                                res_index,
                                comp_index,
                                prec_index,
                                inclusion_trees,
                                zero_bit_plane_trees,
                                state_bindings,
                                options,
                            );
                        }
                    }
                }
            }
        },
        1 => {
            var res_index: u8 = 0;
            while (res_index < num_resolutions) : (res_index += 1) {
                var layer_index: u16 = 0;
                while (layer_index < num_layers) : (layer_index += 1) {
                    var comp_index: u16 = 0;
                    while (comp_index < num_components) : (comp_index += 1) {
                        const max_precinct = maxPrecinctIndexForResolutionAndComponent(codeblocks, res_index, comp_index);
                        var prec_index: u32 = 0;
                        while (prec_index < max_precinct) : (prec_index += 1) {
                            try emitPacketForCoordinate(
                                allocator,
                                &output,
                                codeblocks,
                                states,
                                layer_index,
                                num_layers,
                                res_index,
                                comp_index,
                                prec_index,
                                inclusion_trees,
                                zero_bit_plane_trees,
                                state_bindings,
                                options,
                            );
                        }
                    }
                }
            }
        },
        2 => {
            var res_index: u8 = 0;
            while (res_index < num_resolutions) : (res_index += 1) {
                const max_precinct = maxPrecinctIndexForResolution(codeblocks, res_index);
                var prec_index: u32 = 0;
                while (prec_index < max_precinct) : (prec_index += 1) {
                    var comp_index: u16 = 0;
                    while (comp_index < num_components) : (comp_index += 1) {
                        var layer_index: u16 = 0;
                        while (layer_index < num_layers) : (layer_index += 1) {
                            try emitPacketForCoordinate(
                                allocator,
                                &output,
                                codeblocks,
                                states,
                                layer_index,
                                num_layers,
                                res_index,
                                comp_index,
                                prec_index,
                                inclusion_trees,
                                zero_bit_plane_trees,
                                state_bindings,
                                options,
                            );
                        }
                    }
                }
            }
        },
        3 => {
            const max_precinct = maxPrecinctIndex(codeblocks);
            var prec_index: u32 = 0;
            while (prec_index < max_precinct) : (prec_index += 1) {
                var comp_index: u16 = 0;
                while (comp_index < num_components) : (comp_index += 1) {
                    var res_index: u8 = 0;
                    while (res_index < num_resolutions) : (res_index += 1) {
                        var layer_index: u16 = 0;
                        while (layer_index < num_layers) : (layer_index += 1) {
                            try emitPacketForCoordinate(
                                allocator,
                                &output,
                                codeblocks,
                                states,
                                layer_index,
                                num_layers,
                                res_index,
                                comp_index,
                                prec_index,
                                inclusion_trees,
                                zero_bit_plane_trees,
                                state_bindings,
                                options,
                            );
                        }
                    }
                }
            }
        },
        4 => {
            var comp_index: u16 = 0;
            while (comp_index < num_components) : (comp_index += 1) {
                const max_precinct = maxPrecinctIndexForComponent(codeblocks, comp_index);
                var prec_index: u32 = 0;
                while (prec_index < max_precinct) : (prec_index += 1) {
                    var res_index: u8 = 0;
                    while (res_index < num_resolutions) : (res_index += 1) {
                        var layer_index: u16 = 0;
                        while (layer_index < num_layers) : (layer_index += 1) {
                            try emitPacketForCoordinate(
                                allocator,
                                &output,
                                codeblocks,
                                states,
                                layer_index,
                                num_layers,
                                res_index,
                                comp_index,
                                prec_index,
                                inclusion_trees,
                                zero_bit_plane_trees,
                                state_bindings,
                                options,
                            );
                        }
                    }
                }
            }
        },
        else => return error.UnsupportedProgressionOrder,
    }

    return output.toOwnedSlice(allocator);
}

fn maxPrecinctIndex(codeblocks: []const EncodedCodeblockInfo) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |cb| {
        max_precinct = @max(max_precinct, cb.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexForResolution(codeblocks: []const EncodedCodeblockInfo, resolution_index: u8) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |cb| {
        if (cb.resolution_index != resolution_index) continue;
        max_precinct = @max(max_precinct, cb.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexForResolutionAndComponent(codeblocks: []const EncodedCodeblockInfo, resolution_index: u8, component_index: u16) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |cb| {
        if (cb.resolution_index != resolution_index or cb.component_index != component_index) continue;
        max_precinct = @max(max_precinct, cb.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexForComponent(codeblocks: []const EncodedCodeblockInfo, component_index: u16) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |cb| {
        if (cb.component_index != component_index) continue;
        max_precinct = @max(max_precinct, cb.precinct_index + 1);
    }
    return max_precinct;
}

fn emitPacketForCoordinate(
    allocator: std.mem.Allocator,
    output: *std.ArrayListUnmanaged(u8),
    codeblocks: []const EncodedCodeblockInfo,
    states: []packet.CodeBlockState,
    layer_index: u16,
    num_layers: u16,
    res_index: u8,
    comp_index: u16,
    prec_index: u32,
    inclusion_trees: []tagtree.TagTree,
    zero_bit_plane_trees: []tagtree.TagTree,
    state_bindings: []const packet.PacketTreeBinding,
    options: AssembleOptions,
) !void {
    const packet_start_offset = output.items.len;
    if (options.emit_sop) {
        // SOP: 0xff91 Lsop(=4) Nsop(counter mod 2^16). Written before the
        // packet header so a resync decoder can find packet boundaries.
        try output.append(allocator, 0xff);
        try output.append(allocator, 0x91);
        try output.append(allocator, 0x00);
        try output.append(allocator, 0x04);
        const counter = options.sop_counter.*;
        try output.append(allocator, @intCast(counter >> 8));
        try output.append(allocator, @intCast(counter & 0xff));
        options.sop_counter.* = counter +% 1;
    }
    var packet_cbs: std.ArrayListUnmanaged(EncodedCodeblockInfo) = .empty;
    defer packet_cbs.deinit(allocator);
    var packet_state_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer packet_state_indices.deinit(allocator);
    var matching_codeblocks: usize = 0;

    var any_contribution: bool = false;
    for (codeblocks, 0..) |cb, idx| {
        if (cb.resolution_index == res_index and
            cb.component_index == comp_index and
            cb.precinct_index == prec_index)
        {
            matching_codeblocks += 1;
            const emitted_passes = states[idx].num_coding_passes;
            const target_passes = targetPassCountForLayer(cb.num_coding_passes, layer_index, num_layers);
            const pass_delta: u16 = if (target_passes > emitted_passes) target_passes - emitted_passes else 0;

            // All precinct codeblocks must appear in the packet header, even
            // when they contribute no new passes this layer — the decoder
            // walks the full precinct layout and expects a contribution bit
            // (or tag-tree step) for each one.
            const start_offset: usize = if (cb.pass_lengths.len == 0 or emitted_passes == 0) 0 else cb.pass_lengths[emitted_passes - 1];
            const end_offset: usize = if (pass_delta == 0)
                start_offset
            else if (cb.pass_lengths.len == 0)
                cb.data.len
            else
                cb.pass_lengths[target_passes - 1];
            try packet_cbs.append(allocator, .{
                .component_index = cb.component_index,
                .resolution_index = cb.resolution_index,
                .subband = cb.subband,
                .precinct_index = cb.precinct_index,
                .codeblock_x = cb.codeblock_x,
                .codeblock_y = cb.codeblock_y,
                .data = cb.data[start_offset..end_offset],
                .num_coding_passes = pass_delta,
                .zero_bit_planes = cb.zero_bit_planes,
                .pass_lengths = cb.pass_lengths,
            });
            try packet_state_indices.append(allocator, idx);
            if (pass_delta > 0) any_contribution = true;
        }
    }

    if (!any_contribution) {
        var empty = try encodeEmptyPacket(allocator);
        defer empty.deinit(allocator);
        try output.appendSlice(allocator, empty.header);
        if (options.emit_eph) {
            try output.append(allocator, 0xff);
            try output.append(allocator, 0x92);
        }
        try output.appendSlice(allocator, empty.body);
        if (options.packet_lengths) |lengths| {
            try lengths.append(allocator, @intCast(output.items.len - packet_start_offset));
        }
        return;
    }

    const pkt_states = try allocator.alloc(packet.CodeBlockState, packet_cbs.items.len);
    defer allocator.free(pkt_states);
    const pkt_bindings = try allocator.alloc(packet.PacketTreeBinding, packet_cbs.items.len);
    defer allocator.free(pkt_bindings);

    for (0..packet_cbs.items.len) |i| {
        pkt_states[i] = states[packet_state_indices.items[i]];
        pkt_bindings[i] = state_bindings[packet_state_indices.items[i]];
    }

    var pkt = try encodePacket(
        allocator,
        packet_cbs.items,
        pkt_states,
        inclusion_trees,
        zero_bit_plane_trees,
        pkt_bindings,
        layer_index,
    );
    defer pkt.deinit(allocator);

    for (0..packet_cbs.items.len) |i| {
        states[packet_state_indices.items[i]] = pkt_states[i];
    }

    try output.appendSlice(allocator, pkt.header);
    if (options.emit_eph) {
        try output.append(allocator, 0xff);
        try output.append(allocator, 0x92);
    }
    try output.appendSlice(allocator, pkt.body);
    if (options.packet_lengths) |lengths| {
        try lengths.append(allocator, @intCast(output.items.len - packet_start_offset));
    }
}

fn targetPassCountForLayer(total_passes: u16, layer_index: u16, num_layers: u16) u16 {
    if (num_layers <= 1 or total_passes == 0) return total_passes;
    const numerator = @as(u32, total_passes) * @as(u32, layer_index + 1);
    return @intCast(@min(
        @as(u32, total_passes),
        (numerator + @as(u32, num_layers) - 1) / @as(u32, num_layers),
    ));
}

/// Map a subband type and resolution index to the subband_index used in
/// the packet layout (matching tile.enumerateComponentCodeblocks).
fn subbandToIndex(subband: tile.SubbandType, resolution_index: u8) u8 {
    if (resolution_index == 0) return 0; // LL band
    return switch (subband) {
        .hl => 0,
        .lh => 1,
        .hh => 2,
        .ll => 0,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encodeCodingPassCount produces correct bit patterns" {
    const allocator = std.testing.allocator;

    // Helper to collect individual bits from the writer into a u8 slice.
    const Helper = struct {
        fn collectBits(alloc: std.mem.Allocator, count: u16) ![]u8 {
            var w = PacketHeaderBitWriter.init(alloc);
            defer w.deinit();
            try encodeCodingPassCount(&w, count);
            // Extract individual bits from the byte buffer.
            var result: std.ArrayListUnmanaged(u8) = .empty;
            defer result.deinit(alloc);
            for (w.bytes.items, 0..) |byte, byte_idx| {
                const limit: u4 = if (byte_idx == 0) 8 else if (byte_idx > 0 and w.bytes.items[byte_idx - 1] == 0xff) 7 else 8;
                for (0..limit) |b| {
                    const shift: u3 = @intCast(limit - 1 - b);
                    try result.append(alloc, @intCast((byte >> shift) & 1));
                }
            }
            // Trim trailing zero-padding bits from the last (incomplete) byte.
            // The writer's bit_index tells us how many valid bits are in the
            // current (last) byte.  If bit_index == 0 the last byte was
            // fully completed, so all bits extracted from it are valid.
            if (w.bytes.items.len > 0 and w.bit_index != 0) {
                const padding = @as(usize, 8) - @as(usize, w.bit_index);
                const trim = @min(padding, result.items.len);
                result.items.len -= trim;
            }
            return try result.toOwnedSlice(alloc);
        }
    };

    // 1 pass -> 0
    {
        const bits = try Helper.collectBits(allocator, 1);
        defer allocator.free(bits);
        try std.testing.expectEqualSlices(u8, &.{0}, bits);
    }

    // 2 passes -> 10
    {
        const bits = try Helper.collectBits(allocator, 2);
        defer allocator.free(bits);
        try std.testing.expectEqualSlices(u8, &.{ 1, 0 }, bits);
    }

    // 3 passes -> 1100
    {
        const bits = try Helper.collectBits(allocator, 3);
        defer allocator.free(bits);
        try std.testing.expectEqualSlices(u8, &.{ 1, 1, 0, 0 }, bits);
    }

    // 5 passes -> 1110
    {
        const bits = try Helper.collectBits(allocator, 5);
        defer allocator.free(bits);
        try std.testing.expectEqualSlices(u8, &.{ 1, 1, 1, 0 }, bits);
    }

    // 10 passes -> 1111 + 5-bit (10-6=4 = 00100)
    {
        const bits = try Helper.collectBits(allocator, 10);
        defer allocator.free(bits);
        try std.testing.expectEqualSlices(u8, &.{ 1, 1, 1, 1, 0, 0, 1, 0, 0 }, bits);
    }
}

test "bitsToBytes correctly pads and converts via PacketHeaderBitWriter" {
    const allocator = std.testing.allocator;

    // Write 5 bits: 10110, should produce one byte 10110_000 = 0xB0.
    {
        var w = PacketHeaderBitWriter.init(allocator);
        defer w.deinit();
        try w.writeBit(1);
        try w.writeBit(0);
        try w.writeBit(1);
        try w.writeBit(1);
        try w.writeBit(0);
        const bytes = try w.finish();
        defer allocator.free(bytes);
        try std.testing.expectEqual(@as(usize, 1), bytes.len);
        try std.testing.expectEqual(@as(u8, 0xB0), bytes[0]);
    }

    // Write 8 bits: 11111111 (0xFF), then 3 bits: 101.
    // After 0xFF byte, next byte has 7-bit limit (MSB is stuffed zero).
    // With limit=7, shift = 6-bit_index:
    //   bit_index=0: shift=6 -> bit 1 at position 6
    //   bit_index=1: shift=5 -> bit 0 at position 5
    //   bit_index=2: shift=4 -> bit 1 at position 4
    // Byte = 0_101_0000 = 0x50.
    {
        var w = PacketHeaderBitWriter.init(allocator);
        defer w.deinit();
        try w.writeBits(0xFF, 8);
        try w.writeBit(1);
        try w.writeBit(0);
        try w.writeBit(1);
        const bytes = try w.finish();
        defer allocator.free(bytes);
        try std.testing.expectEqual(@as(usize, 2), bytes.len);
        try std.testing.expectEqual(@as(u8, 0xFF), bytes[0]);
        try std.testing.expectEqual(@as(u8, 0x50), bytes[1]);
    }

    // Write zero bits -> empty result.
    {
        var w = PacketHeaderBitWriter.init(allocator);
        defer w.deinit();
        const bytes = try w.finish();
        defer allocator.free(bytes);
        try std.testing.expectEqual(@as(usize, 0), bytes.len);
    }
}

test "encodePacket produces non-empty header and body for a single codeblock" {
    const allocator = std.testing.allocator;

    const cb_data = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    const cbs = [_]EncodedCodeblockInfo{.{
        .component_index = 0,
        .resolution_index = 0,
        .subband = .ll,
        .precinct_index = 0,
        .codeblock_x = 0,
        .codeblock_y = 0,
        .data = &cb_data,
        .num_coding_passes = 1,
        .zero_bit_planes = 2,
    }};

    var states = [_]packet.CodeBlockState{.{
        .coordinate = .{
            .tile_index = 0,
            .layer_index = 0,
            .resolution_index = 0,
            .component_index = 0,
            .precinct_index = 0,
        },
        .subband = .ll,
        .subband_index = 0,
        .codeblock_index = 0,
        .codeblock_x = 0,
        .codeblock_y = 0,
    }};

    var inc_tree = try tagtree.TagTree.init(allocator, 1, 1);
    defer inc_tree.deinit();
    var zbp_tree = try tagtree.TagTree.init(allocator, 1, 1);
    defer zbp_tree.deinit();
    inc_tree.setAllValues(std.math.maxInt(u32));
    zbp_tree.setAllValues(std.math.maxInt(u32));

    var inc_trees = [_]tagtree.TagTree{inc_tree};
    var zbp_trees = [_]tagtree.TagTree{zbp_tree};
    const bindings = [_]packet.PacketTreeBinding{.{
        .tree_index = 0,
        .leaf_x = 0,
        .leaf_y = 0,
    }};

    var pkt = try encodePacket(
        allocator,
        &cbs,
        &states,
        &inc_trees,
        &zbp_trees,
        &bindings,
        0,
    );
    defer pkt.deinit(allocator);

    // Header must be non-empty (at minimum: packet-present bit + inclusion + zbp + passes + length).
    try std.testing.expect(pkt.header.len > 0);
    // Body must contain the codeblock data.
    try std.testing.expectEqual(@as(usize, 4), pkt.body.len);
    try std.testing.expectEqualSlices(u8, &cb_data, pkt.body);

    // State should be updated.
    try std.testing.expect(states[0].included);
    try std.testing.expectEqual(@as(?u8, 2), states[0].zero_bit_planes);
}

test "encodePacket round-trips with decoder for single codeblock" {
    const allocator = std.testing.allocator;
    const arithmetic = @import("arithmetic.zig");
    const codeblock_mod = @import("codeblock.zig");

    const cb_data = [_]u8{ 0x01, 0x02, 0x03 };
    const cbs = [_]EncodedCodeblockInfo{.{
        .component_index = 0,
        .resolution_index = 0,
        .subband = .ll,
        .precinct_index = 0,
        .codeblock_x = 0,
        .codeblock_y = 0,
        .data = &cb_data,
        .num_coding_passes = 3,
        .zero_bit_planes = 1,
    }};

    var enc_states = [_]packet.CodeBlockState{.{
        .coordinate = .{
            .tile_index = 0,
            .layer_index = 0,
            .resolution_index = 0,
            .component_index = 0,
            .precinct_index = 0,
        },
        .subband = .ll,
        .subband_index = 0,
        .codeblock_index = 0,
        .codeblock_x = 0,
        .codeblock_y = 0,
    }};

    var inc_tree = try tagtree.TagTree.init(allocator, 1, 1);
    defer inc_tree.deinit();
    var zbp_tree = try tagtree.TagTree.init(allocator, 1, 1);
    defer zbp_tree.deinit();
    inc_tree.setAllValues(std.math.maxInt(u32));
    zbp_tree.setAllValues(std.math.maxInt(u32));

    var inc_trees = [_]tagtree.TagTree{inc_tree};
    var zbp_trees = [_]tagtree.TagTree{zbp_tree};
    const bindings = [_]packet.PacketTreeBinding{.{
        .tree_index = 0,
        .leaf_x = 0,
        .leaf_y = 0,
    }};

    var pkt = try encodePacket(
        allocator,
        &cbs,
        &enc_states,
        &inc_trees,
        &zbp_trees,
        &bindings,
        0,
    );
    defer pkt.deinit(allocator);

    // Now decode the header with the same logic the decoder uses.
    // Concatenate header + body into a single payload.
    var payload: std.ArrayListUnmanaged(u8) = .empty;
    defer payload.deinit(allocator);
    try payload.appendSlice(allocator, pkt.header);
    try payload.appendSlice(allocator, pkt.body);

    // Decode: packet present bit, then for the single first-inclusion codeblock:
    //   inclusion tag tree, zbp tag tree, num_coding_passes, lblock_increment, length
    var reader = arithmetic.PacketHeaderBitReader.init(payload.items);

    // Packet present bit.
    const present = try reader.readBit();
    try std.testing.expectEqual(@as(u1, 1), present);

    // First inclusion via tag tree on a 1x1 tree: the only node is the root=leaf.
    // The encoder sets the leaf value to layer_index=0, threshold=1.
    // decodeBelowThreshold with threshold=1 should return true (value 0 < 1).
    var dec_inc_tree = try tagtree.TagTree.init(allocator, 1, 1);
    defer dec_inc_tree.deinit();
    dec_inc_tree.setAllValues(std.math.maxInt(u32));
    const included = try dec_inc_tree.decodeBelowThreshold(&reader, 0, 0, 1);
    try std.testing.expect(included);

    // Zero bit planes via tag tree decodeValue.
    var dec_zbp_tree = try tagtree.TagTree.init(allocator, 1, 1);
    defer dec_zbp_tree.deinit();
    dec_zbp_tree.setAllValues(std.math.maxInt(u32));
    const zbp = try dec_zbp_tree.decodeValue(&reader, 0, 0, 64);
    try std.testing.expectEqual(@as(u32, 1), zbp);

    // Number of coding passes.
    const num_passes = try codeblock_mod.decodeNumCodingPasses(&reader);
    try std.testing.expectEqual(@as(u16, 3), num_passes);

    // lblock increment (comma code).
    const lblock_increment = try codeblock_mod.decodeCommaCode(&reader);
    const lblock: u8 = 3 + lblock_increment;

    // Segment length.
    const length_bits = std.math.log2_int(u16, num_passes) + lblock;
    const body_length = try reader.readBits(length_bits);
    try std.testing.expectEqual(@as(u32, 3), body_length);

    // Verify body data.
    reader.alignToByte();
    const header_len = reader.consumedBytes();
    try std.testing.expectEqual(pkt.header.len, header_len);
    try std.testing.expectEqualSlices(u8, &cb_data, payload.items[header_len .. header_len + body_length]);
}

test "computeRequiredLblock raises lblock when data is too large" {
    // With 1 pass, pass_bits=0 and base_lblock=3 gives length_bits=3 -> max 7 bytes.
    try std.testing.expectEqual(@as(u8, 3), computeRequiredLblock(7, 3, 1));
    // 8 bytes needs lblock=4 (length_bits=4 -> max 15).
    try std.testing.expectEqual(@as(u8, 4), computeRequiredLblock(8, 3, 1));
    // 0 bytes fits in minimum.
    try std.testing.expectEqual(@as(u8, 3), computeRequiredLblock(0, 3, 1));
}

test "encodeEmptyPacket produces a single byte" {
    const allocator = std.testing.allocator;
    var pkt = try encodeEmptyPacket(allocator);
    defer pkt.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), pkt.header.len);
    // Bit 0 (packet present) = 0, rest padding -> 0x00.
    try std.testing.expectEqual(@as(u8, 0x00), pkt.header[0]);
    try std.testing.expectEqual(@as(usize, 0), pkt.body.len);
}

test "assembleTileData produces output for a simple single-codeblock tile" {
    const allocator = std.testing.allocator;

    const cb_data = [_]u8{ 0xAA, 0xBB };
    const cbs = [_]EncodedCodeblockInfo{.{
        .component_index = 0,
        .resolution_index = 0,
        .subband = .ll,
        .precinct_index = 0,
        .codeblock_x = 0,
        .codeblock_y = 0,
        .data = &cb_data,
        .num_coding_passes = 1,
        .zero_bit_planes = 0,
    }};

    const result = try assembleTileData(allocator, &cbs, 1, 1, 1, 0);
    defer allocator.free(result);

    // Should contain at least the header + 2-byte body.
    try std.testing.expect(result.len >= 3);
    // The body data should appear somewhere in the output.
    var found = false;
    for (0..result.len - 1) |i| {
        if (result[i] == 0xAA and result[i + 1] == 0xBB) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "assembleTileData emits empty packet for missing single-precinct slot" {
    const allocator = std.testing.allocator;

    const result = try assembleTileData(allocator, &.{}, 1, 1, 1, 0);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(u8, 0x00), result[0]);
}
