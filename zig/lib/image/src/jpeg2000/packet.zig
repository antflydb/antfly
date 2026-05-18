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
const codestream = @import("codestream.zig");
const tile = @import("tile.zig");
const tagtree = @import("tagtree.zig");
const codeblock = @import("codeblock.zig");
const quantization = @import("quantization.zig");

pub const native_port_available = true;

pub fn nativeDecodeSupportsCodeBlockStyle(style_byte: u8) bool {
    const supported: u8 = 0x01 | 0x02 | 0x04 | 0x08 | 0x10 | 0x20;
    return (style_byte & ~supported) == 0;
}

pub const PacketCoordinate = struct {
    tile_index: u16,
    layer_index: u16,
    resolution_index: u8,
    component_index: u16,
    precinct_index: u32,

    pub fn eql(a: PacketCoordinate, b: PacketCoordinate) bool {
        return a.tile_index == b.tile_index and
            a.layer_index == b.layer_index and
            a.resolution_index == b.resolution_index and
            a.component_index == b.component_index and
            a.precinct_index == b.precinct_index;
    }
};

pub const PacketCodeblockEntry = struct {
    coordinate: PacketCoordinate,
    state_index: usize,
    subband: tile.SubbandType,
    subband_index: u8,
    codeblock_index: u32,
    codeblock_x: u32,
    codeblock_y: u32,
    rect: tile.CodeBlockRect,
    zero_bit_planes: u8,
    num_coding_passes: u16,
    lblock: u8,
    body_offset: usize,
    body_length: usize,
    segment_lengths: []u32 = &.{},
};

pub const PacketLayoutEntry = struct {
    coordinate: PacketCoordinate,
    state_index: usize,
    subband: tile.SubbandType,
    subband_index: u8,
    codeblock_index: u32,
    codeblock_x: u32,
    codeblock_y: u32,
    rect: tile.CodeBlockRect,
};

pub const PacketGroup = struct {
    coordinate: PacketCoordinate,
    start_index: usize,
    entry_count: usize,
};

pub const CodeBlockState = struct {
    coordinate: PacketCoordinate,
    subband: tile.SubbandType,
    subband_index: u8,
    codeblock_index: u32,
    codeblock_x: u32,
    codeblock_y: u32,
    included: bool = false,
    first_layer_index: ?u16 = null,
    zero_bit_planes: ?u8 = null,
    num_coding_passes: u16 = 0,
    lblock: u8 = 3,
};

pub const PacketTreeKey = struct {
    component_index: u16,
    resolution_index: u8,
    precinct_index: u32,
    subband_index: u8,
};

pub const PacketTreeBinding = struct {
    tree_index: usize,
    leaf_x: usize,
    leaf_y: usize,
};

pub const PacketStateMachine = struct {
    allocator: std.mem.Allocator,
    layout: []const PacketLayoutEntry,
    codeblock_states: []CodeBlockState,
    entry_tree_bindings: []PacketTreeBinding,
    inclusion_trees: []tagtree.TagTree,
    zero_bit_plane_trees: []tagtree.TagTree,

    pub fn init(
        allocator: std.mem.Allocator,
        layout: []const PacketLayoutEntry,
        unique_codeblock_count: usize,
    ) !PacketStateMachine {
        const codeblock_states = try allocator.alloc(CodeBlockState, unique_codeblock_count);
        errdefer allocator.free(codeblock_states);
        const entry_tree_bindings = try allocator.alloc(PacketTreeBinding, layout.len);
        errdefer allocator.free(entry_tree_bindings);
        const seen = try allocator.alloc(bool, unique_codeblock_count);
        defer allocator.free(seen);
        @memset(seen, false);

        var tree_keys = std.ArrayListUnmanaged(PacketTreeKey).empty;
        defer tree_keys.deinit(allocator);
        var tree_widths = std.ArrayListUnmanaged(usize).empty;
        defer tree_widths.deinit(allocator);
        var tree_heights = std.ArrayListUnmanaged(usize).empty;
        defer tree_heights.deinit(allocator);

        for (layout, 0..) |entry, entry_index| {
            const key: PacketTreeKey = .{
                .component_index = entry.coordinate.component_index,
                .resolution_index = entry.coordinate.resolution_index,
                .precinct_index = entry.coordinate.precinct_index,
                .subband_index = entry.subband_index,
            };
            const maybe_existing = findPacketTreeKey(tree_keys.items, key);
            const tree_index = if (maybe_existing) |idx| blk: {
                tree_widths.items[idx] = @max(tree_widths.items[idx], @as(usize, entry.codeblock_x) + 1);
                tree_heights.items[idx] = @max(tree_heights.items[idx], @as(usize, entry.codeblock_y) + 1);
                break :blk idx;
            } else blk: {
                try tree_keys.append(allocator, key);
                try tree_widths.append(allocator, @as(usize, entry.codeblock_x) + 1);
                try tree_heights.append(allocator, @as(usize, entry.codeblock_y) + 1);
                break :blk tree_keys.items.len - 1;
            };
            entry_tree_bindings[entry_index] = .{
                .tree_index = tree_index,
                .leaf_x = entry.codeblock_x,
                .leaf_y = entry.codeblock_y,
            };
        }

        const inclusion_trees = try allocator.alloc(tagtree.TagTree, tree_keys.items.len);
        errdefer allocator.free(inclusion_trees);
        const zero_bit_plane_trees = try allocator.alloc(tagtree.TagTree, tree_keys.items.len);
        errdefer allocator.free(zero_bit_plane_trees);

        for (0..tree_keys.items.len) |i| {
            inclusion_trees[i] = try tagtree.TagTree.init(allocator, tree_widths.items[i], tree_heights.items[i]);
            errdefer {
                var j: usize = 0;
                while (j <= i) : (j += 1) inclusion_trees[j].deinit();
            }
            zero_bit_plane_trees[i] = try tagtree.TagTree.init(allocator, tree_widths.items[i], tree_heights.items[i]);
            errdefer {
                var j: usize = 0;
                while (j <= i) : (j += 1) zero_bit_plane_trees[j].deinit();
            }
            inclusion_trees[i].clear();
            zero_bit_plane_trees[i].clear();
            inclusion_trees[i].setAllValues(std.math.maxInt(u32));
            zero_bit_plane_trees[i].setAllValues(std.math.maxInt(u32));
        }

        for (layout) |entry| {
            if (entry.state_index >= unique_codeblock_count) return error.InvalidPacketStateIndex;
            if (seen[entry.state_index]) continue;
            seen[entry.state_index] = true;
            codeblock_states[entry.state_index] = .{
                .coordinate = .{
                    .tile_index = entry.coordinate.tile_index,
                    .layer_index = 0,
                    .resolution_index = entry.coordinate.resolution_index,
                    .component_index = entry.coordinate.component_index,
                    .precinct_index = entry.coordinate.precinct_index,
                },
                .subband = entry.subband,
                .subband_index = entry.subband_index,
                .codeblock_index = entry.codeblock_index,
                .codeblock_x = entry.codeblock_x,
                .codeblock_y = entry.codeblock_y,
            };
        }

        return .{
            .allocator = allocator,
            .layout = layout,
            .codeblock_states = codeblock_states,
            .entry_tree_bindings = entry_tree_bindings,
            .inclusion_trees = inclusion_trees,
            .zero_bit_plane_trees = zero_bit_plane_trees,
        };
    }

    pub fn deinit(self: *PacketStateMachine) void {
        for (self.inclusion_trees) |*tree| tree.deinit();
        for (self.zero_bit_plane_trees) |*tree| tree.deinit();
        self.allocator.free(self.entry_tree_bindings);
        self.allocator.free(self.inclusion_trees);
        self.allocator.free(self.zero_bit_plane_trees);
        self.allocator.free(self.codeblock_states);
        self.* = undefined;
    }

    pub fn applyEntryUpdate(self: *PacketStateMachine, entry_index: usize, zero_bit_planes: u8, num_coding_passes: u16, lblock: u8) !void {
        if (entry_index >= self.layout.len) return error.PacketEntryOutOfBounds;
        const layout_entry = self.layout[entry_index];
        if (layout_entry.state_index >= self.codeblock_states.len) return error.InvalidPacketStateIndex;
        if (entry_index >= self.entry_tree_bindings.len) return error.PacketEntryOutOfBounds;
        const binding = self.entry_tree_bindings[entry_index];
        if (binding.tree_index >= self.inclusion_trees.len or binding.tree_index >= self.zero_bit_plane_trees.len) return error.InvalidPacketStateIndex;

        const state = &self.codeblock_states[layout_entry.state_index];
        if (!state.included) {
            state.included = true;
            state.first_layer_index = layout_entry.coordinate.layer_index;
            state.zero_bit_planes = zero_bit_planes;
        } else if (state.zero_bit_planes != null and state.zero_bit_planes.? != zero_bit_planes) {
            return error.InconsistentZeroBitPlanes;
        }

        state.num_coding_passes +%= num_coding_passes;
        state.lblock = @max(state.lblock, lblock);
    }
};

pub const PacketModel = struct {
    entries: []PacketCodeblockEntry,
    layout: []PacketLayoutEntry,
    packet_groups: []PacketGroup,
    codeblock_states: []CodeBlockState,
    entry_tree_bindings: []PacketTreeBinding,
    inclusion_trees: []tagtree.TagTree,
    zero_bit_plane_trees: []tagtree.TagTree,

    pub fn deinit(self: *PacketModel, allocator: std.mem.Allocator) void {
        for (self.inclusion_trees) |*tree| tree.deinit();
        for (self.zero_bit_plane_trees) |*tree| tree.deinit();
        for (self.entries) |entry| allocator.free(entry.segment_lengths);
        allocator.free(self.inclusion_trees);
        allocator.free(self.zero_bit_plane_trees);
        allocator.free(self.codeblock_states);
        allocator.free(self.packet_groups);
        allocator.free(self.layout);
        allocator.free(self.entry_tree_bindings);
        allocator.free(self.entries);
        self.* = undefined;
    }
};

pub const ParsedPacketHeaderEntry = struct {
    coordinate: PacketCoordinate,
    state_index: usize,
    first_inclusion: bool,
    included: bool,
    zero_bit_planes: u8,
    num_coding_passes: u16,
    lblock: u8,
    body_length: usize,
    segment_lengths: []u32 = &.{},
};

pub const PacketHeaderMode = enum {
    bounded_fixture,
    bounded_fixture_prefer_extra_bit_true,
    tagtree_first_inclusion,
    tagtree_one_based_inclusion,
    tagtree_first_inclusion_zero_bitplanes_zero,
    tagtree_one_based_zero_bitplanes_zero,
    packet_present_tagtree_first_inclusion,
    packet_present_tagtree_one_based_inclusion,
    packet_present_tagtree_one_based_zero_bitplanes_zero,
};

pub const ParsedPacketGroup = struct {
    coordinate: PacketCoordinate,
    header_length: usize,
    body_length: usize,
    entries: []ParsedPacketHeaderEntry,

    pub fn deinit(self: *ParsedPacketGroup, allocator: std.mem.Allocator) void {
        for (self.entries) |entry| allocator.free(entry.segment_lengths);
        allocator.free(self.entries);
        self.* = undefined;
    }
};

pub const ParsedPacketGroupsResult = struct {
    groups: []ParsedPacketGroup,
    consumed_bytes: usize,
    fully_consumed: bool,

    pub fn deinit(self: *ParsedPacketGroupsResult, allocator: std.mem.Allocator) void {
        for (self.groups) |*group| group.deinit(allocator);
        allocator.free(self.groups);
        self.* = undefined;
    }
};

const PacketGroupParseOptions = struct {
    body_length_extra_bit: bool = true,
    headers_only: bool = false,
};

pub const Tier1Segment = struct {
    coordinate: PacketCoordinate,
    state_index: usize,
    subband: tile.SubbandType,
    rect: tile.CodeBlockRect,
    zero_bit_planes: u8,
    start_pass_index: u16,
    num_coding_passes: u16,
    body_offset: usize,
    body_length: usize,
    segment_lengths: []u32 = &.{},
};

pub const Tier1CodeblockState = struct {
    coordinate: PacketCoordinate,
    subband: tile.SubbandType,
    rect: tile.CodeBlockRect,
    zero_bit_planes: u8,
    executed_passes: u16,
    magnitude_scale: u8 = 1,
    grid: codeblock.CoefficientGrid,

    pub fn deinit(self: *Tier1CodeblockState) void {
        self.grid.deinit();
        self.* = undefined;
    }
};

pub const Tier1Execution = struct {
    segments: []Tier1Segment,
    codeblocks: []Tier1CodeblockState,

    pub fn deinit(self: *Tier1Execution, allocator: std.mem.Allocator) void {
        for (self.codeblocks) |*entry| entry.deinit();
        freeTier1Segments(allocator, self.segments);
        allocator.free(self.codeblocks);
        self.* = undefined;
    }
};

fn freeTier1Segments(allocator: std.mem.Allocator, segments: []Tier1Segment) void {
    for (segments) |segment| allocator.free(segment.segment_lengths);
    allocator.free(segments);
}

pub fn buildLrcpPacketLayout(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketLayoutEntry {
    if (state.header.uses_multiple_tiles) return error.UnsupportedPacketLayout;
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;

    var geometry = try tile.buildSingleTileGeometry(allocator, state);
    defer geometry.deinit(allocator);

    var total_unique: usize = 0;
    for (geometry.components) |*component| total_unique += component.totalCodeblocks();
    const total_entries = total_unique * coding_style.num_layers;

    const layout = try allocator.alloc(PacketLayoutEntry, total_entries);
    errdefer allocator.free(layout);

    // Pre-enumerate codeblocks per component and compute state_base offsets.
    const comp_codeblocks = try allocator.alloc([]tile.CodeBlockCoordinate, geometry.components.len);
    defer {
        for (comp_codeblocks) |cb| allocator.free(cb);
        allocator.free(comp_codeblocks);
    }
    const state_bases = try allocator.alloc(usize, geometry.components.len);
    defer allocator.free(state_bases);
    {
        var base: usize = 0;
        for (geometry.components, 0..) |*component, ci| {
            comp_codeblocks[ci] = try tile.enumerateComponentCodeblocks(allocator, component);
            state_bases[ci] = base;
            base += comp_codeblocks[ci].len;
        }
    }

    const max_resolutions = try maxResolutionCountForState(state);
    var out_index: usize = 0;

    // LRCP: Layer → Resolution → Component → Position (codeblocks)
    var layer_index: u16 = 0;
    while (layer_index < coding_style.num_layers) : (layer_index += 1) {
        var res_index: u8 = 0;
        while (res_index < max_resolutions) : (res_index += 1) {
            for (comp_codeblocks, 0..) |codeblocks, ci| {
                const max_precinct = maxPrecinctIndexForResolution(codeblocks, res_index);
                var precinct_index: u32 = 0;
                while (precinct_index < max_precinct) : (precinct_index += 1) {
                    for (codeblocks, 0..) |codeblock_entry, local_index| {
                        if (codeblock_entry.resolution_index != res_index or codeblock_entry.precinct_index != precinct_index) continue;
                        layout[out_index] = .{
                            .coordinate = .{
                                .tile_index = 0,
                                .layer_index = layer_index,
                                .resolution_index = codeblock_entry.resolution_index,
                                .component_index = codeblock_entry.component_index,
                                .precinct_index = codeblock_entry.precinct_index,
                            },
                            .state_index = state_bases[ci] + local_index,
                            .subband = codeblock_entry.subband,
                            .subband_index = codeblock_entry.subband_index,
                            .codeblock_index = codeblock_entry.codeblock_index,
                            .codeblock_x = codeblock_entry.codeblock_x,
                            .codeblock_y = codeblock_entry.codeblock_y,
                            .rect = codeblock_entry.rect,
                        };
                        out_index += 1;
                    }
                }
            }
        }
    }

    return layout;
}

pub fn buildRlcpPacketLayout(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketLayoutEntry {
    if (state.header.uses_multiple_tiles) return error.UnsupportedPacketLayout;
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;

    var geometry = try tile.buildSingleTileGeometry(allocator, state);
    defer geometry.deinit(allocator);

    var total_unique: usize = 0;
    for (geometry.components) |*component| total_unique += component.totalCodeblocks();
    const total_entries = total_unique * coding_style.num_layers;

    const layout = try allocator.alloc(PacketLayoutEntry, total_entries);
    errdefer allocator.free(layout);

    // Pre-enumerate codeblocks for each component.
    const component_codeblocks = try allocator.alloc([]tile.CodeBlockCoordinate, geometry.components.len);
    defer {
        for (component_codeblocks) |cb| allocator.free(cb);
        allocator.free(component_codeblocks);
    }
    for (geometry.components, 0..) |*component, i| {
        component_codeblocks[i] = try tile.enumerateComponentCodeblocks(allocator, component);
    }

    // Compute state_base offsets per component.
    const state_bases = try allocator.alloc(usize, geometry.components.len);
    defer allocator.free(state_bases);
    {
        var base: usize = 0;
        for (component_codeblocks, 0..) |cbs, i| {
            state_bases[i] = base;
            base += cbs.len;
        }
    }

    var out_index: usize = 0;
    const num_resolutions = try maxResolutionCountForState(state);
    var resolution_index: u8 = 0;
    while (resolution_index < num_resolutions) : (resolution_index += 1) {
        var layer_index: u16 = 0;
        while (layer_index < coding_style.num_layers) : (layer_index += 1) {
            for (component_codeblocks, 0..) |codeblocks, comp_idx| {
                const max_precinct = maxPrecinctIndexForResolution(codeblocks, resolution_index);
                var precinct_index: u32 = 0;
                while (precinct_index < max_precinct) : (precinct_index += 1) {
                    for (codeblocks, 0..) |codeblock_entry, local_index| {
                        if (codeblock_entry.resolution_index != resolution_index or codeblock_entry.precinct_index != precinct_index) continue;
                        layout[out_index] = .{
                            .coordinate = .{
                                .tile_index = 0,
                                .layer_index = layer_index,
                                .resolution_index = codeblock_entry.resolution_index,
                                .component_index = codeblock_entry.component_index,
                                .precinct_index = codeblock_entry.precinct_index,
                            },
                            .state_index = state_bases[comp_idx] + local_index,
                            .subband = codeblock_entry.subband,
                            .subband_index = codeblock_entry.subband_index,
                            .codeblock_index = codeblock_entry.codeblock_index,
                            .codeblock_x = codeblock_entry.codeblock_x,
                            .codeblock_y = codeblock_entry.codeblock_y,
                            .rect = codeblock_entry.rect,
                        };
                        out_index += 1;
                    }
                }
            }
        }
    }

    return layout;
}

pub fn buildRpclPacketLayout(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketLayoutEntry {
    if (state.header.uses_multiple_tiles) return error.UnsupportedPacketLayout;
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;

    var geometry = try tile.buildSingleTileGeometry(allocator, state);
    defer geometry.deinit(allocator);

    var total_unique: usize = 0;
    for (geometry.components) |*component| total_unique += component.totalCodeblocks();
    const total_entries = total_unique * coding_style.num_layers;

    const layout = try allocator.alloc(PacketLayoutEntry, total_entries);
    errdefer allocator.free(layout);

    const component_codeblocks = try allocator.alloc([]tile.CodeBlockCoordinate, geometry.components.len);
    defer {
        for (component_codeblocks) |cb| allocator.free(cb);
        allocator.free(component_codeblocks);
    }
    for (geometry.components, 0..) |*component, i| {
        component_codeblocks[i] = try tile.enumerateComponentCodeblocks(allocator, component);
    }

    const state_bases = try allocator.alloc(usize, geometry.components.len);
    defer allocator.free(state_bases);
    {
        var base: usize = 0;
        for (component_codeblocks, 0..) |cbs, i| {
            state_bases[i] = base;
            base += cbs.len;
        }
    }

    const packet_coordinates = try buildPacketCoordinatesForOrder(
        allocator,
        state,
        &geometry,
        component_codeblocks,
        2,
    );
    defer allocator.free(packet_coordinates);

    var out_index: usize = 0;
    for (packet_coordinates) |coordinate| {
        try appendLayoutEntriesForPacketCoordinate(
            layout,
            &out_index,
            component_codeblocks,
            state_bases,
            coordinate,
        );
    }
    return try allocator.realloc(layout, out_index);
}

pub fn buildPcrlPacketLayout(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketLayoutEntry {
    if (state.header.uses_multiple_tiles) return error.UnsupportedPacketLayout;
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;

    var geometry = try tile.buildSingleTileGeometry(allocator, state);
    defer geometry.deinit(allocator);

    var total_unique: usize = 0;
    for (geometry.components) |*component| total_unique += component.totalCodeblocks();
    const total_entries = total_unique * coding_style.num_layers;

    const layout = try allocator.alloc(PacketLayoutEntry, total_entries);
    errdefer allocator.free(layout);

    const component_codeblocks = try allocator.alloc([]tile.CodeBlockCoordinate, geometry.components.len);
    defer {
        for (component_codeblocks) |cb| allocator.free(cb);
        allocator.free(component_codeblocks);
    }
    for (geometry.components, 0..) |*component, i| {
        component_codeblocks[i] = try tile.enumerateComponentCodeblocks(allocator, component);
    }

    const state_bases = try allocator.alloc(usize, geometry.components.len);
    defer allocator.free(state_bases);
    {
        var base: usize = 0;
        for (component_codeblocks, 0..) |cbs, i| {
            state_bases[i] = base;
            base += cbs.len;
        }
    }

    const packet_coordinates = try buildPacketCoordinatesForOrder(
        allocator,
        state,
        &geometry,
        component_codeblocks,
        3,
    );
    defer allocator.free(packet_coordinates);

    var out_index: usize = 0;
    for (packet_coordinates) |coordinate| {
        try appendLayoutEntriesForPacketCoordinate(
            layout,
            &out_index,
            component_codeblocks,
            state_bases,
            coordinate,
        );
    }
    return try allocator.realloc(layout, out_index);
}

pub fn buildCprlPacketLayout(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketLayoutEntry {
    if (state.header.uses_multiple_tiles) return error.UnsupportedPacketLayout;
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;

    var geometry = try tile.buildSingleTileGeometry(allocator, state);
    defer geometry.deinit(allocator);

    var total_unique: usize = 0;
    for (geometry.components) |*component| total_unique += component.totalCodeblocks();
    const total_entries = total_unique * coding_style.num_layers;

    const layout = try allocator.alloc(PacketLayoutEntry, total_entries);
    errdefer allocator.free(layout);

    const component_codeblocks = try allocator.alloc([]tile.CodeBlockCoordinate, geometry.components.len);
    defer {
        for (component_codeblocks) |cb| allocator.free(cb);
        allocator.free(component_codeblocks);
    }
    for (geometry.components, 0..) |*component, i| {
        component_codeblocks[i] = try tile.enumerateComponentCodeblocks(allocator, component);
    }

    const state_bases = try allocator.alloc(usize, geometry.components.len);
    defer allocator.free(state_bases);
    {
        var base: usize = 0;
        for (component_codeblocks, 0..) |cbs, i| {
            state_bases[i] = base;
            base += cbs.len;
        }
    }

    // Component -> Position (precinct) -> Resolution -> Layer
    var out_index: usize = 0;
    const num_resolutions = try maxResolutionCountForState(state);
    for (component_codeblocks, 0..) |codeblocks, comp_idx| {
        const component_max_precinct = maxPrecinctIndexForComponent(codeblocks);
        var precinct_index: u32 = 0;
        while (precinct_index < component_max_precinct) : (precinct_index += 1) {
            var resolution_index: u8 = 0;
            while (resolution_index < num_resolutions) : (resolution_index += 1) {
                var layer_index: u16 = 0;
                while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                    for (codeblocks, 0..) |codeblock_entry, local_index| {
                        if (codeblock_entry.resolution_index != resolution_index or codeblock_entry.precinct_index != precinct_index) continue;
                        layout[out_index] = .{
                            .coordinate = .{
                                .tile_index = 0,
                                .layer_index = layer_index,
                                .resolution_index = codeblock_entry.resolution_index,
                                .component_index = codeblock_entry.component_index,
                                .precinct_index = codeblock_entry.precinct_index,
                            },
                            .state_index = state_bases[comp_idx] + local_index,
                            .subband = codeblock_entry.subband,
                            .subband_index = codeblock_entry.subband_index,
                            .codeblock_index = codeblock_entry.codeblock_index,
                            .codeblock_x = codeblock_entry.codeblock_x,
                            .codeblock_y = codeblock_entry.codeblock_y,
                            .rect = codeblock_entry.rect,
                        };
                        out_index += 1;
                    }
                }
            }
        }
    }

    return layout;
}

fn appendLayoutEntriesForPacketCoordinate(
    layout: []PacketLayoutEntry,
    out_index: *usize,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    state_bases: []const usize,
    coordinate: PacketCoordinate,
) !void {
    const comp_idx: usize = coordinate.component_index;
    if (comp_idx >= component_codeblocks.len or comp_idx >= state_bases.len) return;
    for (component_codeblocks[comp_idx], 0..) |codeblock_entry, local_index| {
        if (codeblock_entry.resolution_index != coordinate.resolution_index or
            codeblock_entry.precinct_index != coordinate.precinct_index)
        {
            continue;
        }
        if (out_index.* >= layout.len) return error.InvalidPacketSpanLayout;
        layout[out_index.*] = .{
            .coordinate = coordinate,
            .state_index = state_bases[comp_idx] + local_index,
            .subband = codeblock_entry.subband,
            .subband_index = codeblock_entry.subband_index,
            .codeblock_index = codeblock_entry.codeblock_index,
            .codeblock_x = codeblock_entry.codeblock_x,
            .codeblock_y = codeblock_entry.codeblock_y,
            .rect = codeblock_entry.rect,
        };
        out_index.* += 1;
    }
}

fn buildPacketCoordinatesForOrder(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    order: u8,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    return switch (order) {
        0 => buildLrcpPacketCoordinates(allocator, state, geometry, component_codeblocks),
        1 => buildRlcpPacketCoordinates(allocator, state, geometry, component_codeblocks),
        2 => if (coding_style.precincts_present)
            buildRpclPacketCoordinates(allocator, state, geometry)
        else
            buildFlatRpclPacketCoordinates(allocator, state, geometry, component_codeblocks),
        3 => if (coding_style.precincts_present)
            buildPcrlPacketCoordinates(allocator, state, geometry)
        else
            buildFlatPcrlPacketCoordinates(allocator, state, geometry, component_codeblocks),
        4 => buildCprlPacketCoordinates(allocator, state, geometry, component_codeblocks),
        else => error.UnsupportedProgressionOrder,
    };
}

fn buildPacketCoordinatesForOrderIncludingEmptyPackets(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    order: u8,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    return switch (order) {
        2 => if (coding_style.precincts_present)
            buildRpclPacketCoordinatesIncludingEmpty(allocator, state, geometry)
        else
            buildDefaultPrecinctPacketCoordinates(allocator, state, geometry, component_codeblocks, order),
        3 => if (coding_style.precincts_present)
            buildPcrlPacketCoordinatesIncludingEmpty(allocator, state, geometry)
        else
            buildDefaultPrecinctPacketCoordinates(allocator, state, geometry, component_codeblocks, order),
        else => buildDefaultPrecinctPacketCoordinates(allocator, state, geometry, component_codeblocks, order),
    };
}

fn buildPacketCoordinatesWithWindows(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    windows: []const PacketProgressionWindow,
    include_empty_packets: bool,
) ![]PacketCoordinate {
    var result: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer result.deinit(allocator);

    var processed_packets: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    defer processed_packets.deinit(allocator);

    for (windows) |window| {
        if (window.re <= window.rs) return error.InvalidProgressionWindow;
        if (window.ce <= window.cs) return error.InvalidProgressionWindow;
        if (window.lye == 0) return error.InvalidProgressionWindow;
        if (window.order > 4) return error.UnsupportedProgressionOrder;

        const full = if (include_empty_packets)
            try buildPacketCoordinatesForOrderIncludingEmptyPackets(allocator, state, geometry, component_codeblocks, window.order)
        else
            try buildPacketCoordinatesForOrder(allocator, state, geometry, component_codeblocks, window.order);
        defer allocator.free(full);

        for (full) |coordinate| {
            if (packetCoordinateInProgressionWindow(coordinate, window) and
                !packetCoordinateSeen(processed_packets.items, coordinate))
            {
                try result.append(allocator, coordinate);
                try processed_packets.append(allocator, coordinate);
            }
        }
    }

    return result.toOwnedSlice(allocator);
}

fn buildDefaultPrecinctPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    order: u8,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    const num_resolutions = try maxResolutionCountForState(state);
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    switch (order) {
        0 => {
            var layer_index: u16 = 0;
            while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                var resolution_index: u8 = 0;
                while (resolution_index < num_resolutions) : (resolution_index += 1) {
                    for (geometry.components, 0..) |component, comp_idx| {
                        const max_precinct = maxPrecinctIndexForResolutionIncludingEmpty(component_codeblocks[comp_idx], component, resolution_index);
                        var precinct_index: u32 = 0;
                        while (precinct_index < max_precinct) : (precinct_index += 1) {
                            try coordinates.append(allocator, defaultPrecinctCoordinate(layer_index, resolution_index, comp_idx, precinct_index));
                        }
                    }
                }
            }
        },
        1 => {
            var resolution_index: u8 = 0;
            while (resolution_index < num_resolutions) : (resolution_index += 1) {
                var layer_index: u16 = 0;
                while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                    for (geometry.components, 0..) |component, comp_idx| {
                        const max_precinct = maxPrecinctIndexForResolutionIncludingEmpty(component_codeblocks[comp_idx], component, resolution_index);
                        var precinct_index: u32 = 0;
                        while (precinct_index < max_precinct) : (precinct_index += 1) {
                            try coordinates.append(allocator, defaultPrecinctCoordinate(layer_index, resolution_index, comp_idx, precinct_index));
                        }
                    }
                }
            }
        },
        2 => {
            var resolution_index: u8 = 0;
            while (resolution_index < num_resolutions) : (resolution_index += 1) {
                const max_precinct = maxPrecinctIndexAcrossComponentsForResolutionIncludingEmpty(geometry, component_codeblocks, resolution_index);
                var precinct_index: u32 = 0;
                while (precinct_index < max_precinct) : (precinct_index += 1) {
                    for (geometry.components, 0..) |component, comp_idx| {
                        if (precinct_index >= maxPrecinctIndexForResolutionIncludingEmpty(component_codeblocks[comp_idx], component, resolution_index)) continue;
                        var layer_index: u16 = 0;
                        while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                            try coordinates.append(allocator, defaultPrecinctCoordinate(layer_index, resolution_index, comp_idx, precinct_index));
                        }
                    }
                }
            }
        },
        3 => {
            const max_precinct = maxPrecinctIndexAcrossComponentsIncludingEmpty(geometry, component_codeblocks);
            var precinct_index: u32 = 0;
            while (precinct_index < max_precinct) : (precinct_index += 1) {
                for (geometry.components, 0..) |component, comp_idx| {
                    var resolution_index: u8 = 0;
                    while (resolution_index < num_resolutions) : (resolution_index += 1) {
                        if (precinct_index >= maxPrecinctIndexForResolutionIncludingEmpty(component_codeblocks[comp_idx], component, resolution_index)) continue;
                        var layer_index: u16 = 0;
                        while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                            try coordinates.append(allocator, defaultPrecinctCoordinate(layer_index, resolution_index, comp_idx, precinct_index));
                        }
                    }
                }
            }
        },
        4 => {
            for (geometry.components, 0..) |component, comp_idx| {
                const component_max_precinct = maxPrecinctIndexForComponentIncludingEmpty(component_codeblocks[comp_idx], component);
                var precinct_index: u32 = 0;
                while (precinct_index < component_max_precinct) : (precinct_index += 1) {
                    var resolution_index: u8 = 0;
                    while (resolution_index < num_resolutions) : (resolution_index += 1) {
                        if (precinct_index >= maxPrecinctIndexForResolutionIncludingEmpty(component_codeblocks[comp_idx], component, resolution_index)) continue;
                        var layer_index: u16 = 0;
                        while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                            try coordinates.append(allocator, defaultPrecinctCoordinate(layer_index, resolution_index, comp_idx, precinct_index));
                        }
                    }
                }
            }
        },
        else => return error.UnsupportedProgressionOrder,
    }

    return coordinates.toOwnedSlice(allocator);
}

fn maxPrecinctIndexForResolutionIncludingEmpty(codeblocks: []const tile.CodeBlockCoordinate, component: tile.ComponentGeometry, resolution_index: u8) u32 {
    const max_precinct = maxPrecinctIndexForResolutionWithCodeblocks(codeblocks, resolution_index);
    if (max_precinct > 0) return max_precinct;
    return if (componentHasResolutionPacket(component, resolution_index)) 1 else 0;
}

fn maxPrecinctIndexAcrossComponentsForResolutionIncludingEmpty(
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    resolution_index: u8,
) u32 {
    var max_precinct: u32 = 0;
    for (geometry.components, 0..) |component, comp_idx| {
        max_precinct = @max(max_precinct, maxPrecinctIndexForResolutionIncludingEmpty(component_codeblocks[comp_idx], component, resolution_index));
    }
    return max_precinct;
}

fn maxPrecinctIndexAcrossComponentsIncludingEmpty(
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) u32 {
    var max_precinct: u32 = 0;
    for (geometry.components, 0..) |component, comp_idx| {
        max_precinct = @max(max_precinct, maxPrecinctIndexForComponentIncludingEmpty(component_codeblocks[comp_idx], component));
    }
    return max_precinct;
}

fn maxPrecinctIndexForComponentIncludingEmpty(codeblocks: []const tile.CodeBlockCoordinate, component: tile.ComponentGeometry) u32 {
    var max_precinct: u32 = 0;
    for (component.resolutions, 0..) |_, resolution_index| {
        max_precinct = @max(max_precinct, maxPrecinctIndexForResolutionIncludingEmpty(codeblocks, component, @intCast(resolution_index)));
    }
    return max_precinct;
}

fn componentHasResolutionPacket(component: tile.ComponentGeometry, resolution_index: u8) bool {
    if (resolution_index >= component.resolutions.len) return false;
    const resolution = component.resolutions[resolution_index];
    return resolution.bounds.width > 0 and resolution.bounds.height > 0;
}

fn defaultPrecinctCoordinate(layer_index: u16, resolution_index: u8, component_index: usize, precinct_index: u32) PacketCoordinate {
    return .{
        .tile_index = 0,
        .layer_index = layer_index,
        .resolution_index = resolution_index,
        .component_index = @intCast(component_index),
        .precinct_index = precinct_index,
    };
}

fn buildLrcpPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    var layer_index: u16 = 0;
    while (layer_index < coding_style.num_layers) : (layer_index += 1) {
        var resolution_index: u8 = 0;
        while (resolution_index < num_resolutions) : (resolution_index += 1) {
            for (component_codeblocks, 0..) |codeblocks, comp_idx| {
                if (resolution_index >= geometry.components[comp_idx].resolutions.len) continue;
                const max_precinct = maxPrecinctIndexForExistingResolution(codeblocks, resolution_index);
                var precinct_index: u32 = 0;
                while (precinct_index < max_precinct) : (precinct_index += 1) {
                    try coordinates.append(allocator, .{
                        .tile_index = 0,
                        .layer_index = layer_index,
                        .resolution_index = resolution_index,
                        .component_index = @intCast(comp_idx),
                        .precinct_index = precinct_index,
                    });
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildRlcpPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    var resolution_index: u8 = 0;
    while (resolution_index < num_resolutions) : (resolution_index += 1) {
        var layer_index: u16 = 0;
        while (layer_index < coding_style.num_layers) : (layer_index += 1) {
            for (component_codeblocks, 0..) |codeblocks, comp_idx| {
                if (resolution_index >= geometry.components[comp_idx].resolutions.len) continue;
                const max_precinct = maxPrecinctIndexForExistingResolution(codeblocks, resolution_index);
                var precinct_index: u32 = 0;
                while (precinct_index < max_precinct) : (precinct_index += 1) {
                    try coordinates.append(allocator, .{
                        .tile_index = 0,
                        .layer_index = layer_index,
                        .resolution_index = resolution_index,
                        .component_index = @intCast(comp_idx),
                        .precinct_index = precinct_index,
                    });
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildRpclPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    const x0 = state.header.x_offset;
    const y0 = state.header.y_offset;
    const x1 = state.header.x_offset + state.header.width;
    const y1 = state.header.y_offset + state.header.height;

    var resolution_index: u8 = 0;
    while (resolution_index < num_resolutions) : (resolution_index += 1) {
        const min_step = minReferencePrecinctStepForResolution(state, geometry, resolution_index) orelse continue;
        var y = y0;
        while (y < y1) : (y += positionStep(y, min_step.y)) {
            var x = x0;
            while (x < x1) : (x += positionStep(x, min_step.x)) {
                for (geometry.components, 0..) |component, comp_idx| {
                    const precinct_index = precinctIndexForReferencePosition(
                        state,
                        component,
                        @intCast(comp_idx),
                        resolution_index,
                        x,
                        y,
                    ) orelse continue;
                    var layer_index: u16 = 0;
                    while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                        try appendUniquePacketCoordinate(allocator, &coordinates, .{
                            .tile_index = 0,
                            .layer_index = layer_index,
                            .resolution_index = resolution_index,
                            .component_index = @intCast(comp_idx),
                            .precinct_index = precinct_index,
                        });
                    }
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildRpclPacketCoordinatesIncludingEmpty(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    const x0 = state.header.x_offset;
    const y0 = state.header.y_offset;
    const x1 = state.header.x_offset + state.header.width;
    const y1 = state.header.y_offset + state.header.height;

    var resolution_index: u8 = 0;
    while (resolution_index < num_resolutions) : (resolution_index += 1) {
        const min_step = minReferencePrecinctStepForResolutionIncludingEmpty(state, geometry, resolution_index) orelse continue;
        var y = y0;
        while (y < y1) : (y += positionStep(y, min_step.y)) {
            var x = x0;
            while (x < x1) : (x += positionStep(x, min_step.x)) {
                for (geometry.components, 0..) |component, comp_idx| {
                    const precinct_index = precinctIndexForReferencePositionIncludingEmpty(
                        state,
                        component,
                        @intCast(comp_idx),
                        resolution_index,
                        x,
                        y,
                    ) orelse continue;
                    var layer_index: u16 = 0;
                    while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                        try appendUniquePacketCoordinate(allocator, &coordinates, .{
                            .tile_index = 0,
                            .layer_index = layer_index,
                            .resolution_index = resolution_index,
                            .component_index = @intCast(comp_idx),
                            .precinct_index = precinct_index,
                        });
                    }
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildFlatRpclPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    var resolution_index: u8 = 0;
    while (resolution_index < num_resolutions) : (resolution_index += 1) {
        const max_precinct = maxPrecinctIndexAcrossComponentsForExistingResolution(geometry, component_codeblocks, resolution_index);
        var precinct_index: u32 = 0;
        while (precinct_index < max_precinct) : (precinct_index += 1) {
            for (component_codeblocks, 0..) |_, comp_idx| {
                if (resolution_index >= geometry.components[comp_idx].resolutions.len) continue;
                var layer_index: u16 = 0;
                while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                    try coordinates.append(allocator, .{
                        .tile_index = 0,
                        .layer_index = layer_index,
                        .resolution_index = resolution_index,
                        .component_index = @intCast(comp_idx),
                        .precinct_index = precinct_index,
                    });
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildPcrlPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const min_step = minReferencePrecinctStep(state, geometry) orelse return coordinates.toOwnedSlice(allocator);
    const num_resolutions = try maxResolutionCountForState(state);
    const x0 = state.header.x_offset;
    const y0 = state.header.y_offset;
    const x1 = state.header.x_offset + state.header.width;
    const y1 = state.header.y_offset + state.header.height;

    var y = y0;
    while (y < y1) : (y += positionStep(y, min_step.y)) {
        var x = x0;
        while (x < x1) : (x += positionStep(x, min_step.x)) {
            for (geometry.components, 0..) |component, comp_idx| {
                var resolution_index: u8 = 0;
                while (resolution_index < num_resolutions) : (resolution_index += 1) {
                    const precinct_index = precinctIndexForReferencePosition(
                        state,
                        component,
                        @intCast(comp_idx),
                        resolution_index,
                        x,
                        y,
                    ) orelse continue;
                    var layer_index: u16 = 0;
                    while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                        try appendUniquePacketCoordinate(allocator, &coordinates, .{
                            .tile_index = 0,
                            .layer_index = layer_index,
                            .resolution_index = resolution_index,
                            .component_index = @intCast(comp_idx),
                            .precinct_index = precinct_index,
                        });
                    }
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildPcrlPacketCoordinatesIncludingEmpty(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const min_step = minReferencePrecinctStepIncludingEmpty(state, geometry) orelse return coordinates.toOwnedSlice(allocator);
    const num_resolutions = try maxResolutionCountForState(state);
    const x0 = state.header.x_offset;
    const y0 = state.header.y_offset;
    const x1 = state.header.x_offset + state.header.width;
    const y1 = state.header.y_offset + state.header.height;

    var y = y0;
    while (y < y1) : (y += positionStep(y, min_step.y)) {
        var x = x0;
        while (x < x1) : (x += positionStep(x, min_step.x)) {
            for (geometry.components, 0..) |component, comp_idx| {
                var resolution_index: u8 = 0;
                while (resolution_index < num_resolutions) : (resolution_index += 1) {
                    const precinct_index = precinctIndexForReferencePositionIncludingEmpty(
                        state,
                        component,
                        @intCast(comp_idx),
                        resolution_index,
                        x,
                        y,
                    ) orelse continue;
                    var layer_index: u16 = 0;
                    while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                        try appendUniquePacketCoordinate(allocator, &coordinates, .{
                            .tile_index = 0,
                            .layer_index = layer_index,
                            .resolution_index = resolution_index,
                            .component_index = @intCast(comp_idx),
                            .precinct_index = precinct_index,
                        });
                    }
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildFlatPcrlPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    const max_precinct = maxPrecinctIndexAcrossExistingComponents(geometry, component_codeblocks);
    var precinct_index: u32 = 0;
    while (precinct_index < max_precinct) : (precinct_index += 1) {
        for (component_codeblocks, 0..) |_, comp_idx| {
            var resolution_index: u8 = 0;
            while (resolution_index < num_resolutions) : (resolution_index += 1) {
                if (resolution_index >= geometry.components[comp_idx].resolutions.len) continue;
                var layer_index: u16 = 0;
                while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                    try coordinates.append(allocator, .{
                        .tile_index = 0,
                        .layer_index = layer_index,
                        .resolution_index = resolution_index,
                        .component_index = @intCast(comp_idx),
                        .precinct_index = precinct_index,
                    });
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

fn buildCprlPacketCoordinates(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) ![]PacketCoordinate {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    var coordinates: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    errdefer coordinates.deinit(allocator);

    const num_resolutions = try maxResolutionCountForState(state);
    for (component_codeblocks, 0..) |codeblocks, comp_idx| {
        var precinct_index: u32 = 0;
        const component_max_precinct = maxPrecinctIndexForComponent(codeblocks);
        while (precinct_index < component_max_precinct) : (precinct_index += 1) {
            var resolution_index: u8 = 0;
            while (resolution_index < num_resolutions) : (resolution_index += 1) {
                if (resolution_index >= geometry.components[comp_idx].resolutions.len) continue;
                var layer_index: u16 = 0;
                while (layer_index < coding_style.num_layers) : (layer_index += 1) {
                    try coordinates.append(allocator, .{
                        .tile_index = 0,
                        .layer_index = layer_index,
                        .resolution_index = resolution_index,
                        .component_index = @intCast(comp_idx),
                        .precinct_index = precinct_index,
                    });
                }
            }
        }
    }
    return coordinates.toOwnedSlice(allocator);
}

const ReferencePrecinctStep = struct {
    x: u32,
    y: u32,
};

const PacketPrecinctSize = struct {
    x: u32,
    y: u32,
};

fn packetIterationPrecinctSize(
    state: *const codestream.State,
    component_index: usize,
    resolution_index: u8,
) ?PacketPrecinctSize {
    const coding_style = if (component_index < state.component_coding_styles.len)
        state.component_coding_styles[component_index] orelse state.coding_style orelse return null
    else
        state.coding_style orelse return null;
    if (!coding_style.precincts_present or coding_style.precinct_sizes == null) {
        return .{ .x = 1 << 15, .y = 1 << 15 };
    }
    const precinct_sizes = coding_style.precinct_sizes.?;
    if (resolution_index >= precinct_sizes.len) return null;
    const encoded = precinct_sizes[resolution_index];
    return .{
        .x = @max(@as(u32, 1), @as(u32, 1) << @intCast(encoded & 0x0f)),
        .y = @max(@as(u32, 1), @as(u32, 1) << @intCast((encoded >> 4) & 0x0f)),
    };
}

fn minReferencePrecinctStep(
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
) ?ReferencePrecinctStep {
    var out: ?ReferencePrecinctStep = null;
    for (geometry.components, 0..) |component, comp_idx| {
        var resolution_index: u8 = 0;
        while (resolution_index < component.resolutions.len) : (resolution_index += 1) {
            const step = referencePrecinctStep(state, component, @intCast(comp_idx), resolution_index) orelse continue;
            out = if (out) |current| .{ .x = @min(current.x, step.x), .y = @min(current.y, step.y) } else step;
        }
    }
    return out;
}

fn minReferencePrecinctStepIncludingEmpty(
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
) ?ReferencePrecinctStep {
    var out: ?ReferencePrecinctStep = null;
    for (geometry.components, 0..) |component, comp_idx| {
        var resolution_index: u8 = 0;
        while (resolution_index < component.resolutions.len) : (resolution_index += 1) {
            const step = referencePrecinctStepIncludingEmpty(state, component, @intCast(comp_idx), resolution_index) orelse continue;
            out = if (out) |current| .{ .x = @min(current.x, step.x), .y = @min(current.y, step.y) } else step;
        }
    }
    return out;
}

fn minReferencePrecinctStepForResolution(
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    resolution_index: u8,
) ?ReferencePrecinctStep {
    var out: ?ReferencePrecinctStep = null;
    for (geometry.components, 0..) |component, comp_idx| {
        const step = referencePrecinctStep(state, component, @intCast(comp_idx), resolution_index) orelse continue;
        out = if (out) |current| .{ .x = @min(current.x, step.x), .y = @min(current.y, step.y) } else step;
    }
    return out;
}

fn minReferencePrecinctStepForResolutionIncludingEmpty(
    state: *const codestream.State,
    geometry: *const tile.TileGeometry,
    resolution_index: u8,
) ?ReferencePrecinctStep {
    var out: ?ReferencePrecinctStep = null;
    for (geometry.components, 0..) |component, comp_idx| {
        const step = referencePrecinctStepIncludingEmpty(state, component, @intCast(comp_idx), resolution_index) orelse continue;
        out = if (out) |current| .{ .x = @min(current.x, step.x), .y = @min(current.y, step.y) } else step;
    }
    return out;
}

fn referencePrecinctStep(
    state: *const codestream.State,
    component: tile.ComponentGeometry,
    component_index: usize,
    resolution_index: u8,
) ?ReferencePrecinctStep {
    if (resolution_index >= component.resolutions.len) return null;
    const resolution = component.resolutions[resolution_index];
    if (resolution.bounds.width == 0 or resolution.bounds.height == 0) return null;
    const precinct_size = packetIterationPrecinctSize(state, component_index, resolution_index) orelse return null;
    const siz_component = state.header.components[component_index];
    const xrsiz: u32 = @max(@as(u32, siz_component.xrsiz), 1);
    const yrsiz: u32 = @max(@as(u32, siz_component.yrsiz), 1);
    const scale_power: u5 = @intCast(component.decomposition_levels - resolution_index);
    return .{
        .x = @max(@as(u32, 1), xrsiz * precinct_size.x * (@as(u32, 1) << scale_power)),
        .y = @max(@as(u32, 1), yrsiz * precinct_size.y * (@as(u32, 1) << scale_power)),
    };
}

fn referencePrecinctStepIncludingEmpty(
    state: *const codestream.State,
    component: tile.ComponentGeometry,
    component_index: usize,
    resolution_index: u8,
) ?ReferencePrecinctStep {
    if (resolution_index >= component.resolutions.len) return null;
    const resolution = component.resolutions[resolution_index];
    if (resolution.subbands.len == 0) return null;
    if (resolution.bounds.width == 0 or resolution.bounds.height == 0) return null;
    const precinct_size = packetIterationPrecinctSize(state, component_index, resolution_index) orelse return null;
    const siz_component = state.header.components[component_index];
    const xrsiz: u32 = @max(@as(u32, siz_component.xrsiz), 1);
    const yrsiz: u32 = @max(@as(u32, siz_component.yrsiz), 1);
    const scale_power: u5 = @intCast(component.decomposition_levels - resolution_index);
    return .{
        .x = @max(@as(u32, 1), xrsiz * precinct_size.x * (@as(u32, 1) << scale_power)),
        .y = @max(@as(u32, 1), yrsiz * precinct_size.y * (@as(u32, 1) << scale_power)),
    };
}

fn precinctIndexForReferencePosition(
    state: *const codestream.State,
    component: tile.ComponentGeometry,
    component_index: usize,
    resolution_index: u8,
    x: u32,
    y: u32,
) ?u32 {
    if (resolution_index >= component.resolutions.len) return null;
    const resolution = component.resolutions[resolution_index];
    if (resolution.bounds.width == 0 or resolution.bounds.height == 0) return null;
    const step = referencePrecinctStep(state, component, component_index, resolution_index) orelse return null;
    const precinct_size = packetIterationPrecinctSize(state, component_index, resolution_index) orelse return null;
    const prc_step_x = @max(precinct_size.x, @as(u32, 1));
    const prc_step_y = @max(precinct_size.y, @as(u32, 1));
    const tc_x0 = state.header.x_offset;
    const tc_y0 = state.header.y_offset;
    const x_aligned = (x % step.x == 0) or (x == tc_x0 and resolution.origin_x % prc_step_x != 0);
    const y_aligned = (y % step.y == 0) or (y == tc_y0 and resolution.origin_y % prc_step_y != 0);
    if (!x_aligned or !y_aligned) return null;

    const siz_component = state.header.components[component_index];
    const xrsiz: u32 = @max(@as(u32, siz_component.xrsiz), 1);
    const yrsiz: u32 = @max(@as(u32, siz_component.yrsiz), 1);
    const scale: u32 = @as(u32, 1) << @intCast(component.decomposition_levels - resolution_index);
    const comp_x = x / xrsiz;
    const comp_y = y / yrsiz;
    const res_x = ceilDivU32(comp_x, scale);
    const res_y = ceilDivU32(comp_y, scale);

    const origin_precinct_x = resolution.origin_x / prc_step_x;
    const origin_precinct_y = resolution.origin_y / prc_step_y;
    const precinct_x = res_x / prc_step_x;
    const precinct_y = res_y / prc_step_y;
    if (precinct_x < origin_precinct_x or precinct_y < origin_precinct_y) return null;
    const local_x = precinct_x - origin_precinct_x;
    const local_y = precinct_y - origin_precinct_y;
    const precincts_x = precinctCountForPosition(resolution.origin_x, resolution.bounds.width, prc_step_x);
    const precincts_y = precinctCountForPosition(resolution.origin_y, resolution.bounds.height, prc_step_y);
    if (local_x >= precincts_x or local_y >= precincts_y) return null;
    return local_y * precincts_x + local_x;
}

fn precinctIndexForReferencePositionIncludingEmpty(
    state: *const codestream.State,
    component: tile.ComponentGeometry,
    component_index: usize,
    resolution_index: u8,
    x: u32,
    y: u32,
) ?u32 {
    if (resolution_index >= component.resolutions.len) return null;
    const resolution = component.resolutions[resolution_index];
    if (resolution.subbands.len == 0) return null;
    if (resolution.bounds.width == 0 or resolution.bounds.height == 0) return null;
    const step = referencePrecinctStepIncludingEmpty(state, component, component_index, resolution_index) orelse return null;
    const precinct_size = packetIterationPrecinctSize(state, component_index, resolution_index) orelse return null;
    const prc_step_x = @max(precinct_size.x, @as(u32, 1));
    const prc_step_y = @max(precinct_size.y, @as(u32, 1));
    const tc_x0 = state.header.x_offset;
    const tc_y0 = state.header.y_offset;
    const x_aligned = (x % step.x == 0) or (x == tc_x0 and resolution.origin_x % prc_step_x != 0);
    const y_aligned = (y % step.y == 0) or (y == tc_y0 and resolution.origin_y % prc_step_y != 0);
    if (!x_aligned or !y_aligned) return null;

    const siz_component = state.header.components[component_index];
    const xrsiz: u32 = @max(@as(u32, siz_component.xrsiz), 1);
    const yrsiz: u32 = @max(@as(u32, siz_component.yrsiz), 1);
    const scale: u32 = @as(u32, 1) << @intCast(component.decomposition_levels - resolution_index);
    const comp_x = x / xrsiz;
    const comp_y = y / yrsiz;
    const res_x = ceilDivU32(comp_x, scale);
    const res_y = ceilDivU32(comp_y, scale);

    const origin_precinct_x = resolution.origin_x / prc_step_x;
    const origin_precinct_y = resolution.origin_y / prc_step_y;
    const precinct_x = res_x / prc_step_x;
    const precinct_y = res_y / prc_step_y;
    if (precinct_x < origin_precinct_x or precinct_y < origin_precinct_y) return null;
    const local_x = precinct_x - origin_precinct_x;
    const local_y = precinct_y - origin_precinct_y;
    const precincts_x = precinctCountForPositionIncludingEmpty(resolution.origin_x, resolution.bounds.width, prc_step_x);
    const precincts_y = precinctCountForPositionIncludingEmpty(resolution.origin_y, resolution.bounds.height, prc_step_y);
    if (local_x >= precincts_x or local_y >= precincts_y) return null;
    return local_y * precincts_x + local_x;
}

fn precinctCountForPosition(origin: u32, width: u32, precinct_width: u32) u32 {
    if (width == 0) return 0;
    const safe_width = @max(precinct_width, @as(u32, 1));
    return @max(ceilDivU32(origin + width, safe_width) - origin / safe_width, @as(u32, 1));
}

fn precinctCountForPositionIncludingEmpty(origin: u32, width: u32, precinct_width: u32) u32 {
    const safe_width = @max(precinct_width, @as(u32, 1));
    return @max(ceilDivU32(origin + width, safe_width) - origin / safe_width, @as(u32, 1));
}

fn positionStep(position: u32, step: u32) u32 {
    const safe_step = @max(step, @as(u32, 1));
    const rem = position % safe_step;
    return if (rem == 0) safe_step else safe_step - rem;
}

fn ceilDivU32(value: u32, denom: u32) u32 {
    const safe_denom = @max(denom, @as(u32, 1));
    return if (value == 0) 0 else (value + safe_denom - 1) / safe_denom;
}

fn appendUniquePacketCoordinate(
    allocator: std.mem.Allocator,
    coordinates: *std.ArrayListUnmanaged(PacketCoordinate),
    coordinate: PacketCoordinate,
) !void {
    if (packetCoordinateSeen(coordinates.items, coordinate)) return;
    try coordinates.append(allocator, coordinate);
}

fn buildPacketLayoutForOrder(allocator: std.mem.Allocator, state: *const codestream.State, order: u8) ![]PacketLayoutEntry {
    return switch (order) {
        0 => buildLrcpPacketLayout(allocator, state),
        1 => buildRlcpPacketLayout(allocator, state),
        2 => buildRpclPacketLayout(allocator, state),
        3 => buildPcrlPacketLayout(allocator, state),
        4 => buildCprlPacketLayout(allocator, state),
        else => error.UnsupportedProgressionOrder,
    };
}

pub fn buildPacketLayout(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketLayoutEntry {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    if (state.poc_entries.len == 0) {
        return buildPacketLayoutForOrder(allocator, state, coding_style.progression_order);
    }

    const windows = try buildProgressionWindowsFromPoc(allocator, state);
    defer allocator.free(windows);
    return buildPacketLayoutWithWindows(allocator, state, windows);
}

/// A slice of the packet enumeration space, scoped to resolution range
/// `[rs, re)`, component range `[cs, ce)`, layer range `[0, lye)` and visited
/// in a specific progression order. Mirrors `PocEntry` but avoids importing it
/// so packet.zig stays independent.
pub const PacketProgressionWindow = struct {
    rs: u8,
    re: u8,
    cs: u16,
    ce: u16,
    lye: u16,
    order: u8,
};

fn buildProgressionWindowsFromPoc(allocator: std.mem.Allocator, state: *const codestream.State) ![]PacketProgressionWindow {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    if (state.header.components.len > std.math.maxInt(u16)) return error.InvalidProgressionWindow;
    const component_count: u16 = @intCast(state.header.components.len);
    const num_resolutions = try maxResolutionCountForState(state);

    const windows = try allocator.alloc(PacketProgressionWindow, state.poc_entries.len);
    errdefer allocator.free(windows);

    for (state.poc_entries, windows) |poc, *window| {
        const re = @min(poc.re_poc, num_resolutions);
        const ce = @min(poc.ce_poc, component_count);
        const lye = @min(poc.lye_poc, coding_style.num_layers);
        if (re <= poc.rs_poc) return error.InvalidProgressionWindow;
        if (ce <= poc.cs_poc) return error.InvalidProgressionWindow;
        if (lye == 0) return error.InvalidProgressionWindow;

        window.* = .{
            .rs = poc.rs_poc,
            .re = re,
            .cs = poc.cs_poc,
            .ce = ce,
            .lye = lye,
            .order = poc.progression_order,
        };
    }

    return windows;
}

fn packetCoordinateInProgressionWindow(coordinate: PacketCoordinate, window: PacketProgressionWindow) bool {
    return coordinate.resolution_index >= window.rs and
        coordinate.resolution_index < window.re and
        coordinate.component_index >= window.cs and
        coordinate.component_index < window.ce and
        coordinate.layer_index < window.lye;
}

fn packetCoordinateSeen(coordinates: []const PacketCoordinate, needle: PacketCoordinate) bool {
    for (coordinates) |coordinate| {
        if (PacketCoordinate.eql(coordinate, needle)) return true;
    }
    return false;
}

/// Multi-window packet enumeration. Each window is visited in sequence; within
/// a window, entries are emitted in the window's progression order and filtered
/// to the window's resolution/component/layer range.
///
/// When `windows.len == 0` falls back to the single-order path driven by
/// `coding_style.progression_order`.
pub fn buildPacketLayoutWithWindows(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    windows: []const PacketProgressionWindow,
) ![]PacketLayoutEntry {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    if (windows.len == 0) return buildPacketLayoutForOrder(allocator, state, coding_style.progression_order);

    var result: std.ArrayListUnmanaged(PacketLayoutEntry) = .empty;
    errdefer result.deinit(allocator);

    var processed_packets: std.ArrayListUnmanaged(PacketCoordinate) = .empty;
    defer processed_packets.deinit(allocator);

    for (windows) |window| {
        if (window.re <= window.rs) return error.InvalidProgressionWindow;
        if (window.ce <= window.cs) return error.InvalidProgressionWindow;
        if (window.lye == 0) return error.InvalidProgressionWindow;
        if (window.order > 4) return error.UnsupportedProgressionOrder;

        // Build the full layout for this window's order, then filter.
        const full = try buildPacketLayoutForOrder(allocator, state, window.order);
        defer allocator.free(full);

        var group_start: usize = 0;
        while (group_start < full.len) {
            const coordinate = full[group_start].coordinate;
            var group_end = group_start + 1;
            while (group_end < full.len and PacketCoordinate.eql(full[group_end].coordinate, coordinate)) : (group_end += 1) {}

            if (packetCoordinateInProgressionWindow(coordinate, window) and
                !packetCoordinateSeen(processed_packets.items, coordinate))
            {
                try result.appendSlice(allocator, full[group_start..group_end]);
                try processed_packets.append(allocator, coordinate);
            }
            group_start = group_end;
        }
    }

    return try result.toOwnedSlice(allocator);
}

pub fn buildPacketGroups(allocator: std.mem.Allocator, state: *const codestream.State, layout: []const PacketLayoutEntry) ![]PacketGroup {
    return buildPacketGroupsWithEmptyPolicy(allocator, state, layout, false);
}

fn buildPacketGroupsIncludingEmptyPackets(allocator: std.mem.Allocator, state: *const codestream.State, layout: []const PacketLayoutEntry) ![]PacketGroup {
    return buildPacketGroupsWithEmptyPolicy(allocator, state, layout, true);
}

fn buildPacketGroupsWithEmptyPolicy(allocator: std.mem.Allocator, state: *const codestream.State, layout: []const PacketLayoutEntry, include_empty_packets: bool) ![]PacketGroup {
    const coding_style = state.coding_style orelse return error.MissingCodingStyle;
    if (!coding_style.precincts_present and !include_empty_packets) return groupPacketLayoutByPacket(allocator, layout);
    if (state.header.uses_multiple_tiles) return error.UnsupportedPacketLayout;

    var geometry = try tile.buildSingleTileGeometry(allocator, state);
    defer geometry.deinit(allocator);

    const component_codeblocks = try allocator.alloc([]tile.CodeBlockCoordinate, geometry.components.len);
    defer {
        for (component_codeblocks) |cb| allocator.free(cb);
        allocator.free(component_codeblocks);
    }
    for (geometry.components, 0..) |*component, i| {
        component_codeblocks[i] = try tile.enumerateComponentCodeblocks(allocator, component);
    }

    var groups: std.ArrayListUnmanaged(PacketGroup) = .empty;
    errdefer groups.deinit(allocator);

    var layout_index: usize = 0;
    const packet_coordinates = if (state.poc_entries.len > 0) blk: {
        const windows = try buildProgressionWindowsFromPoc(allocator, state);
        defer allocator.free(windows);
        break :blk try buildPacketCoordinatesWithWindows(allocator, state, &geometry, component_codeblocks, windows, include_empty_packets);
    } else if (include_empty_packets)
        try buildPacketCoordinatesForOrderIncludingEmptyPackets(
            allocator,
            state,
            &geometry,
            component_codeblocks,
            coding_style.progression_order,
        )
    else
        try buildPacketCoordinatesForOrder(
            allocator,
            state,
            &geometry,
            component_codeblocks,
            coding_style.progression_order,
        );
    defer allocator.free(packet_coordinates);

    for (packet_coordinates) |coordinate| {
        try appendPacketGroupForCoordinate(allocator, &groups, layout, &layout_index, coordinate, include_empty_packets);
    }

    if (layout_index != layout.len) return error.InvalidPacketSpanLayout;
    return try groups.toOwnedSlice(allocator);
}

fn appendPacketGroupForCoordinate(
    allocator: std.mem.Allocator,
    groups: *std.ArrayListUnmanaged(PacketGroup),
    layout: []const PacketLayoutEntry,
    layout_index: *usize,
    coordinate: PacketCoordinate,
    include_empty: bool,
) !void {
    const start_index = layout_index.*;
    while (layout_index.* < layout.len and PacketCoordinate.eql(layout[layout_index.*].coordinate, coordinate)) : (layout_index.* += 1) {}
    if (layout_index.* == start_index and !include_empty) return;
    try groups.append(allocator, .{
        .coordinate = coordinate,
        .start_index = start_index,
        .entry_count = layout_index.* - start_index,
    });
}

pub fn groupPacketLayoutByPacket(allocator: std.mem.Allocator, layout: []const PacketLayoutEntry) ![]PacketGroup {
    if (layout.len == 0) return allocator.alloc(PacketGroup, 0);

    var groups: std.ArrayListUnmanaged(PacketGroup) = .empty;
    errdefer groups.deinit(allocator);

    var start_index: usize = 0;
    var current = layout[0].coordinate;
    for (layout[1..], 1..) |entry, idx| {
        if (PacketCoordinate.eql(current, entry.coordinate)) continue;
        try groups.append(allocator, .{
            .coordinate = current,
            .start_index = start_index,
            .entry_count = idx - start_index,
        });
        current = entry.coordinate;
        start_index = idx;
    }
    try groups.append(allocator, .{
        .coordinate = current,
        .start_index = start_index,
        .entry_count = layout.len - start_index,
    });
    return try groups.toOwnedSlice(allocator);
}

fn shouldIncludeEmptyPacketGroups(state: *const codestream.State) bool {
    const coding_style = state.coding_style orelse return false;
    const packet_boundaries_are_framed = (coding_style.scod & 0x06) != 0;
    return packet_boundaries_are_framed or
        (!coding_style.precincts_present and
            state.poc_entries.len > 0 and
            state.header.components.len > std.math.maxInt(u8));
}

fn findPacketTreeKey(keys: []const PacketTreeKey, needle: PacketTreeKey) ?usize {
    for (keys, 0..) |key, idx| {
        if (key.component_index == needle.component_index and
            key.resolution_index == needle.resolution_index and
            key.precinct_index == needle.precinct_index and
            key.subband_index == needle.subband_index)
        {
            return idx;
        }
    }
    return null;
}

fn maxPrecinctIndexForResolution(codeblocks: []const tile.CodeBlockCoordinate, resolution_index: u8) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |codeblock_entry| {
        if (codeblock_entry.resolution_index != resolution_index) continue;
        max_precinct = @max(max_precinct, codeblock_entry.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexForExistingResolution(codeblocks: []const tile.CodeBlockCoordinate, resolution_index: u8) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |codeblock_entry| {
        if (codeblock_entry.resolution_index != resolution_index) continue;
        max_precinct = @max(max_precinct, codeblock_entry.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexForResolutionWithCodeblocks(codeblocks: []const tile.CodeBlockCoordinate, resolution_index: u8) u32 {
    var max_precinct: u32 = 0;
    for (codeblocks) |codeblock_entry| {
        if (codeblock_entry.resolution_index != resolution_index) continue;
        max_precinct = @max(max_precinct, codeblock_entry.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexAcrossComponentsForResolution(component_codeblocks: []const []tile.CodeBlockCoordinate, resolution_index: u8) u32 {
    var max_precinct: u32 = 1;
    for (component_codeblocks) |codeblocks| {
        max_precinct = @max(max_precinct, maxPrecinctIndexForResolution(codeblocks, resolution_index));
    }
    return max_precinct;
}

fn maxPrecinctIndexAcrossComponentsForExistingResolution(
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
    resolution_index: u8,
) u32 {
    var max_precinct: u32 = 0;
    for (component_codeblocks, 0..) |codeblocks, comp_idx| {
        if (resolution_index >= geometry.components[comp_idx].resolutions.len) continue;
        max_precinct = @max(max_precinct, maxPrecinctIndexForExistingResolution(codeblocks, resolution_index));
    }
    return max_precinct;
}

fn maxPrecinctIndexForComponent(codeblocks: []const tile.CodeBlockCoordinate) u32 {
    var max_precinct: u32 = 1;
    for (codeblocks) |codeblock_entry| {
        max_precinct = @max(max_precinct, codeblock_entry.precinct_index + 1);
    }
    return max_precinct;
}

fn maxPrecinctIndexAcrossComponents(component_codeblocks: []const []tile.CodeBlockCoordinate) u32 {
    var max_precinct: u32 = 1;
    for (component_codeblocks) |codeblocks| {
        max_precinct = @max(max_precinct, maxPrecinctIndexForComponent(codeblocks));
    }
    return max_precinct;
}

fn maxPrecinctIndexAcrossExistingComponents(
    geometry: *const tile.TileGeometry,
    component_codeblocks: []const []tile.CodeBlockCoordinate,
) u32 {
    var max_precinct: u32 = 0;
    for (component_codeblocks, 0..) |codeblocks, comp_idx| {
        if (geometry.components[comp_idx].resolutions.len == 0) continue;
        max_precinct = @max(max_precinct, maxPrecinctIndexForComponent(codeblocks));
    }
    return max_precinct;
}

fn maxResolutionCountAcrossComponents(component_codeblocks: []const []tile.CodeBlockCoordinate) u8 {
    var max_resolutions: u8 = 1;
    for (component_codeblocks) |codeblocks| {
        for (codeblocks) |codeblock_entry| {
            max_resolutions = @max(max_resolutions, codeblock_entry.resolution_index + 1);
        }
    }
    return max_resolutions;
}

fn maxResolutionCountForState(state: *const codestream.State) !u8 {
    var max_resolutions: u8 = 1;
    for (state.header.components, 0..) |_, component_index| {
        const coding_style = try tile.effectiveCodingStyle(state, component_index);
        max_resolutions = @max(max_resolutions, coding_style.decomposition_levels + 1);
    }
    return max_resolutions;
}

pub fn parsePacketGroupsFromPayload(
    allocator: std.mem.Allocator,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
) ![]ParsedPacketGroup {
    return parsePacketGroupsFromPayloadForState(allocator, null, layout, packet_groups, state_machine, mode, payload);
}

fn parsePacketGroupsFromPayloadForState(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
) ![]ParsedPacketGroup {
    if (mode == .tagtree_first_inclusion or
        mode == .tagtree_one_based_inclusion or
        mode == .tagtree_first_inclusion_zero_bitplanes_zero or
        mode == .tagtree_one_based_zero_bitplanes_zero)
    {
        return parsePlainTagTreePacketGroupsFromPayload(
            allocator,
            state,
            layout,
            packet_groups,
            state_machine,
            mode,
            payload,
        );
    }
    const best = try chooseBestPacketGroupLengthBits(
        allocator,
        packet_groups,
        mode,
        payload,
    );
    defer allocator.free(best.prefer_extra_bits);

    var result = try buildParsedPacketGroupsFromChoices(
        allocator,
        state,
        layout,
        packet_groups,
        state_machine,
        mode,
        payload,
        best.prefer_extra_bits,
    );
    if (!result.fully_consumed and !payloadHasOnlyZeroPadding(payload[result.consumed_bytes..])) {
        result.deinit(allocator);
        return error.InvalidPacketSpanLayout;
    }
    return result.groups;
}

fn parsePlainTagTreePacketGroupsFromPayload(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
) ![]ParsedPacketGroup {
    const prefer_extra_bits = try allocator.alloc(bool, packet_groups.len);
    defer allocator.free(prefer_extra_bits);
    var best_prefer_extra_bit: ?bool = null;
    var best_included_entries: usize = 0;
    var best_fully_consumed = false;
    var best_consumed_bytes: usize = 0;

    for ([_]bool{ false, true }) |prefer_extra_bit| {
        var branch_state_machine = try clonePacketStateMachine(allocator, layout, state_machine);
        defer branch_state_machine.deinit();

        for (prefer_extra_bits) |*bit| bit.* = prefer_extra_bit;
        var result = buildParsedPacketGroupsFromChoices(
            allocator,
            state,
            layout,
            packet_groups,
            &branch_state_machine,
            mode,
            payload,
            prefer_extra_bits,
        ) catch |err| switch (err) {
            error.InvalidPacketSpanLayout,
            error.TruncatedPacketBody,
            error.TruncatedPacketHeader,
            error.EndOfBitstream,
            => continue,
            else => return err,
        };
        if (!result.fully_consumed and !payloadHasOnlyZeroPadding(payload[result.consumed_bytes..])) {
            result.deinit(allocator);
            continue;
        }

        var included_entries: usize = 0;
        for (result.groups) |group| {
            for (group.entries) |entry| {
                if (entry.included) included_entries += 1;
            }
        }

        if (best_prefer_extra_bit == null or
            included_entries > best_included_entries or
            (included_entries == best_included_entries and result.fully_consumed and !best_fully_consumed) or
            (included_entries == best_included_entries and result.fully_consumed == best_fully_consumed and result.consumed_bytes > best_consumed_bytes))
        {
            best_prefer_extra_bit = prefer_extra_bit;
            best_included_entries = included_entries;
            best_fully_consumed = result.fully_consumed;
            best_consumed_bytes = result.consumed_bytes;
        }
        result.deinit(allocator);
    }
    if (best_prefer_extra_bit == null) return error.InvalidPacketSpanLayout;

    for (prefer_extra_bits) |*bit| bit.* = best_prefer_extra_bit.?;
    var branch_state_machine = try clonePacketStateMachine(allocator, layout, state_machine);
    var branch_state_machine_owned = true;
    defer if (branch_state_machine_owned) branch_state_machine.deinit();

    var result = try buildParsedPacketGroupsFromChoices(
        allocator,
        state,
        layout,
        packet_groups,
        &branch_state_machine,
        mode,
        payload,
        prefer_extra_bits,
    );
    if (!result.fully_consumed and !payloadHasOnlyZeroPadding(payload[result.consumed_bytes..])) {
        result.deinit(allocator);
        return error.InvalidPacketSpanLayout;
    }
    state_machine.deinit();
    state_machine.* = branch_state_machine;
    branch_state_machine_owned = false;
    return result.groups;
}

const PacketChoiceSearchResult = struct {
    prefer_extra_bits: []bool,
    consumed_bytes: usize,
    fully_consumed: bool,
    included_entries: usize,
    zero_length_included_entries: usize,
    weighted_body_score: usize,

    fn deinit(self: *PacketChoiceSearchResult, allocator: std.mem.Allocator) void {
        allocator.free(self.prefer_extra_bits);
        self.* = undefined;
    }
};

fn chooseBestPacketGroupLengthBits(
    allocator: std.mem.Allocator,
    packet_groups: []const PacketGroup,
    mode: PacketHeaderMode,
    payload: []const u8,
) !PacketChoiceSearchResult {
    if (mode == .bounded_fixture_prefer_extra_bit_true) {
        const prefer_extra_bits = try allocator.alloc(bool, packet_groups.len);
        @memset(prefer_extra_bits, true);
        return .{
            .prefer_extra_bits = prefer_extra_bits,
            .consumed_bytes = payload.len,
            .fully_consumed = true,
            .included_entries = 0,
            .zero_length_included_entries = 0,
            .weighted_body_score = 0,
        };
    }
    if (mode == .bounded_fixture) {
        const prefer_extra_bits = try allocator.alloc(bool, packet_groups.len);
        @memset(prefer_extra_bits, false);
        return .{
            .prefer_extra_bits = prefer_extra_bits,
            .consumed_bytes = payload.len,
            .fully_consumed = true,
            .included_entries = 0,
            .zero_length_included_entries = 0,
            .weighted_body_score = 0,
        };
    }
    if (mode == .packet_present_tagtree_first_inclusion or
        mode == .packet_present_tagtree_one_based_inclusion or
        mode == .packet_present_tagtree_one_based_zero_bitplanes_zero)
    {
        const prefer_extra_bits = try allocator.alloc(bool, packet_groups.len);
        @memset(prefer_extra_bits, false);
        return .{
            .prefer_extra_bits = prefer_extra_bits,
            .consumed_bytes = payload.len,
            .fully_consumed = true,
            .included_entries = 0,
            .zero_length_included_entries = 0,
            .weighted_body_score = 0,
        };
    }
    return error.InvalidPacketSpanLayout;
}

fn clonePacketStateMachine(
    allocator: std.mem.Allocator,
    layout: []const PacketLayoutEntry,
    source: *const PacketStateMachine,
) !PacketStateMachine {
    var cloned = try PacketStateMachine.init(allocator, layout, source.codeblock_states.len);
    errdefer cloned.deinit();
    @memcpy(cloned.codeblock_states, source.codeblock_states);
    @memcpy(cloned.entry_tree_bindings, source.entry_tree_bindings);
    for (source.inclusion_trees, 0..) |tree, idx| try cloned.inclusion_trees[idx].copyFrom(&tree);
    for (source.zero_bit_plane_trees, 0..) |tree, idx| try cloned.zero_bit_plane_trees[idx].copyFrom(&tree);
    return cloned;
}

fn buildParsedPacketGroupsFromChoices(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
    prefer_extra_bits: []const bool,
) !ParsedPacketGroupsResult {
    if (prefer_extra_bits.len != packet_groups.len) return error.InvalidPacketSpanLayout;

    var parsed_groups = std.ArrayListUnmanaged(ParsedPacketGroup).empty;
    errdefer {
        for (parsed_groups.items) |*group| group.deinit(allocator);
        parsed_groups.deinit(allocator);
    }

    var payload_offset: usize = 0;
    for (packet_groups, 0..) |group, group_index| {
        var parsed_group = try parsePacketGroup(
            allocator,
            layout,
            group,
            state_machine,
            mode,
            payload[payload_offset..],
            state,
            .{ .body_length_extra_bit = prefer_extra_bits[group_index] },
        );
        errdefer parsed_group.deinit(allocator);

        const group_total_length = parsed_group.header_length + parsed_group.body_length;
        if (payload_offset + group_total_length > payload.len) return error.TruncatedPacketBody;

        for (parsed_group.entries, 0..) |entry, idx| {
            if (!entry.included) continue;
            try state_machine.applyEntryUpdate(group.start_index + idx, entry.zero_bit_planes, entry.num_coding_passes, entry.lblock);
        }

        try parsed_groups.append(allocator, parsed_group);
        payload_offset += group_total_length;
    }

    return .{
        .groups = try parsed_groups.toOwnedSlice(allocator),
        .consumed_bytes = payload_offset,
        .fully_consumed = payload_offset == payload.len,
    };
}

fn forceSingleIncludedBodyToPacketLength(parsed_group: *ParsedPacketGroup, packet_length: usize) !void {
    if (parsed_group.header_length > packet_length) return error.TruncatedPacketHeader;
    var included_count: usize = 0;
    var included_index: usize = 0;
    for (parsed_group.entries, 0..) |entry, idx| {
        if (!entry.included) continue;
        included_count += 1;
        included_index = idx;
    }
    if (included_count != 1) return error.InvalidPacketSpanLayout;

    const recovered_body_length = packet_length - parsed_group.header_length;
    const entry = &parsed_group.entries[included_index];
    if (entry.segment_lengths.len > 1) return error.InvalidPacketSpanLayout;
    if (entry.segment_lengths.len == 1) entry.segment_lengths[0] = @intCast(recovered_body_length);
    entry.body_length = recovered_body_length;
    parsed_group.body_length = recovered_body_length;
}

fn absorbSmallPacketLengthRemainder(parsed_group: *ParsedPacketGroup, packet_length: usize) !bool {
    const group_total_length = parsed_group.header_length + parsed_group.body_length;
    if (group_total_length > packet_length) return false;
    const remainder = packet_length - group_total_length;
    if (remainder == 0) return true;
    if (remainder > 1) return false;

    parsed_group.body_length += remainder;
    return true;
}

fn buildParsedPacketGroupsFromPacketLengthsChoices(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
    prefer_extra_bits: []const bool,
    packet_lengths: []const u32,
) !ParsedPacketGroupsResult {
    if (prefer_extra_bits.len != packet_groups.len) return error.InvalidPacketSpanLayout;
    if (packet_lengths.len < packet_groups.len) return error.InvalidPacketSpanLayout;

    var parsed_groups = std.ArrayListUnmanaged(ParsedPacketGroup).empty;
    errdefer {
        for (parsed_groups.items) |*group| group.deinit(allocator);
        parsed_groups.deinit(allocator);
    }

    var payload_offset: usize = 0;
    for (packet_groups, 0..) |group, group_index| {
        const packet_length: usize = packet_lengths[group_index];
        if (payload_offset + packet_length > payload.len) return error.TruncatedPacketBody;
        var parsed_group = parsePacketGroup(
            allocator,
            layout,
            group,
            state_machine,
            mode,
            payload[payload_offset .. payload_offset + packet_length],
            state,
            .{ .body_length_extra_bit = prefer_extra_bits[group_index] },
        ) catch |err| switch (err) {
            error.TruncatedPacketBody,
            error.EndOfBitstream,
            => try parsePacketGroup(
                allocator,
                layout,
                group,
                state_machine,
                mode,
                payload[payload_offset .. payload_offset + packet_length],
                state,
                .{ .body_length_extra_bit = !prefer_extra_bits[group_index] },
            ),
            else => return err,
        };
        errdefer parsed_group.deinit(allocator);

        const group_total_length = parsed_group.header_length + parsed_group.body_length;
        if (group_total_length != packet_length) {
            var retry_group = parsePacketGroup(
                allocator,
                layout,
                group,
                state_machine,
                mode,
                payload[payload_offset .. payload_offset + packet_length],
                state,
                .{ .body_length_extra_bit = !prefer_extra_bits[group_index] },
            ) catch null;
            if (retry_group) |*candidate| {
                const candidate_total_length = candidate.header_length + candidate.body_length;
                if (candidate_total_length == packet_length) {
                    parsed_group.deinit(allocator);
                    parsed_group = candidate.*;
                    candidate.* = undefined;
                } else {
                    candidate.deinit(allocator);
                    if (!try absorbSmallPacketLengthRemainder(&parsed_group, packet_length)) {
                        try forceSingleIncludedBodyToPacketLength(&parsed_group, packet_length);
                    }
                }
            } else {
                if (!try absorbSmallPacketLengthRemainder(&parsed_group, packet_length)) {
                    try forceSingleIncludedBodyToPacketLength(&parsed_group, packet_length);
                }
            }
        }

        for (parsed_group.entries, 0..) |entry, idx| {
            if (!entry.included) continue;
            try state_machine.applyEntryUpdate(group.start_index + idx, entry.zero_bit_planes, entry.num_coding_passes, entry.lblock);
        }

        try parsed_groups.append(allocator, parsed_group);
        payload_offset += packet_length;
    }

    return .{
        .groups = try parsed_groups.toOwnedSlice(allocator),
        .consumed_bytes = payload_offset,
        .fully_consumed = payload_offset == payload.len or payloadHasOnlyZeroPadding(payload[payload_offset..]),
    };
}

fn parsePacketGroupsFromHeaderPayload(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    header_payload: []const u8,
) ![]ParsedPacketGroup {
    var result = try parsePacketGroupsFromHeaderPayloadPrefix(
        allocator,
        state,
        layout,
        packet_groups,
        state_machine,
        mode,
        header_payload,
    );
    errdefer result.deinit(allocator);
    if (!headerPayloadHasOnlyZeroOrEmptyPacketPadding(header_payload[result.consumed_bytes..])) return error.InvalidPacketSpanLayout;
    return result.groups;
}

fn parsePacketGroupsFromHeaderPayloadPrefix(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    header_payload: []const u8,
) !ParsedPacketGroupsResult {
    const prefer_extra_bits = try allocator.alloc(bool, packet_groups.len);
    defer allocator.free(prefer_extra_bits);
    @memset(prefer_extra_bits, false);

    var parsed_groups = std.ArrayListUnmanaged(ParsedPacketGroup).empty;
    errdefer {
        for (parsed_groups.items) |*group| group.deinit(allocator);
        parsed_groups.deinit(allocator);
    }

    var header_offset: usize = 0;
    for (packet_groups, 0..) |group, group_index| {
        var parsed_group = try parsePacketGroup(
            allocator,
            layout,
            group,
            state_machine,
            mode,
            header_payload[header_offset..],
            state,
            .{
                .body_length_extra_bit = prefer_extra_bits[group_index],
                .headers_only = true,
            },
        );
        errdefer parsed_group.deinit(allocator);

        if (header_offset + parsed_group.header_length > header_payload.len) return error.TruncatedPacketHeader;

        for (parsed_group.entries, 0..) |entry, idx| {
            if (!entry.included) continue;
            try state_machine.applyEntryUpdate(group.start_index + idx, entry.zero_bit_planes, entry.num_coding_passes, entry.lblock);
        }

        header_offset += parsed_group.header_length;
        if (header_offset + 2 <= header_payload.len and
            header_payload[header_offset] == 0xff and
            header_payload[header_offset + 1] == 0x92)
        {
            header_offset += 2;
        }
        try parsed_groups.append(allocator, parsed_group);
    }

    return .{
        .groups = try parsed_groups.toOwnedSlice(allocator),
        .consumed_bytes = header_offset,
        .fully_consumed = header_offset == header_payload.len or headerPayloadHasOnlyZeroOrEmptyPacketPadding(header_payload[header_offset..]),
    };
}

fn headerPayloadHasOnlyZeroOrEmptyPacketPadding(bytes: []const u8) bool {
    var index: usize = 0;
    while (index < bytes.len) {
        if (bytes[index] != 0 and bytes[index] != 0x80) return false;
        index += 1;
        if (index + 2 <= bytes.len and bytes[index] == 0xff and bytes[index + 1] == 0x92) {
            index += 2;
        }
    }
    return true;
}

pub fn parsePacketGroupsFromPayloadDetailed(
    allocator: std.mem.Allocator,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
) !ParsedPacketGroupsResult {
    return parsePacketGroupsFromPayloadDetailedWithPreferredLengthBit(
        allocator,
        layout,
        packet_groups,
        state_machine,
        mode,
        payload,
        true,
    );
}

pub fn parsePacketGroupsFromPayloadDetailedWithPreferredLengthBit(
    allocator: std.mem.Allocator,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
    prefer_extra_bit: bool,
) !ParsedPacketGroupsResult {
    var parsed_groups = std.ArrayListUnmanaged(ParsedPacketGroup).empty;
    errdefer {
        for (parsed_groups.items) |*group| group.deinit(allocator);
        parsed_groups.deinit(allocator);
    }

    var payload_offset: usize = 0;
    for (packet_groups) |group| {
        var parsed_group = parsePacketGroup(
            allocator,
            layout,
            group,
            state_machine,
            mode,
            payload[payload_offset..],
            null,
            .{ .body_length_extra_bit = prefer_extra_bit },
        ) catch |err| switch (err) {
            error.TruncatedPacketBody,
            error.EndOfBitstream,
            => try parsePacketGroup(
                allocator,
                layout,
                group,
                state_machine,
                mode,
                payload[payload_offset..],
                null,
                .{ .body_length_extra_bit = !prefer_extra_bit },
            ),
            else => return err,
        };
        errdefer parsed_group.deinit(allocator);

        if (payload_offset + parsed_group.header_length + parsed_group.body_length > payload.len) return error.TruncatedPacketBody;

        for (parsed_group.entries, 0..) |entry, idx| {
            if (!entry.included) continue;
            try state_machine.applyEntryUpdate(group.start_index + idx, entry.zero_bit_planes, entry.num_coding_passes, entry.lblock);
        }

        try parsed_groups.append(allocator, parsed_group);
        payload_offset += parsed_group.header_length + parsed_group.body_length;
    }

    return .{
        .groups = try parsed_groups.toOwnedSlice(allocator),
        .consumed_bytes = payload_offset,
        .fully_consumed = payload_offset == payload.len,
    };
}

pub fn payloadHasOnlyZeroPadding(bytes: []const u8) bool {
    for (bytes) |byte| {
        if (byte != 0) return false;
    }
    return true;
}

fn parsePacketGroup(
    allocator: std.mem.Allocator,
    layout: []const PacketLayoutEntry,
    group: PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
    state: ?*const codestream.State,
    options: PacketGroupParseOptions,
) !ParsedPacketGroup {
    if (payload.len == 0) return error.TruncatedPacketHeader;
    var reader = @import("arithmetic.zig").PacketHeaderBitReader.init(payload);
    var group_entries = std.ArrayListUnmanaged(ParsedPacketHeaderEntry).empty;
    errdefer {
        for (group_entries.items) |entry| allocator.free(entry.segment_lengths);
        group_entries.deinit(allocator);
    }
    var group_body_length: usize = 0;
    const packet_present = if (mode == .packet_present_tagtree_first_inclusion or
        mode == .packet_present_tagtree_one_based_inclusion or
        mode == .packet_present_tagtree_one_based_zero_bitplanes_zero)
        (try reader.readBit()) == 1
    else
        true;

    for (layout[group.start_index .. group.start_index + group.entry_count]) |layout_entry| {
        const cb_state = &state_machine.codeblock_states[layout_entry.state_index];
        var included = false;
        var first_inclusion = false;
        var zero_bit_planes: u8 = cb_state.zero_bit_planes orelse 0;
        var num_coding_passes: u16 = 0;
        var lblock: u8 = cb_state.lblock;
        var body_length: usize = 0;
        var segment_lengths: []u32 = &.{};

        switch (mode) {
            .bounded_fixture, .bounded_fixture_prefer_extra_bit_true => {
                included = (try reader.readBit()) == 1;
                first_inclusion = included and !cb_state.included;
                if (first_inclusion) {
                    zero_bit_planes = try codeblock.decodeCommaCode(&reader);
                }
            },
            .tagtree_first_inclusion,
            .tagtree_one_based_inclusion,
            .tagtree_first_inclusion_zero_bitplanes_zero,
            .tagtree_one_based_zero_bitplanes_zero,
            .packet_present_tagtree_first_inclusion,
            .packet_present_tagtree_one_based_inclusion,
            .packet_present_tagtree_one_based_zero_bitplanes_zero,
            => {
                if (!packet_present) {
                    included = false;
                } else if (findLayoutIndex(layout, layout_entry.coordinate, layout_entry.state_index)) |layout_index| {
                    const binding = state_machine.entry_tree_bindings[layout_index];
                    if (!cb_state.included) {
                        const inclusion_threshold: u32 = if (mode == .tagtree_one_based_inclusion or
                            mode == .packet_present_tagtree_one_based_inclusion or
                            mode == .packet_present_tagtree_one_based_zero_bitplanes_zero)
                            layout_entry.coordinate.layer_index + 2
                        else
                            layout_entry.coordinate.layer_index + 1;
                        included = try state_machine.inclusion_trees[binding.tree_index].decodeBelowThreshold(&reader, binding.leaf_x, binding.leaf_y, inclusion_threshold);
                        first_inclusion = included;
                        if (included) {
                            if (mode == .packet_present_tagtree_one_based_zero_bitplanes_zero or
                                mode == .tagtree_first_inclusion_zero_bitplanes_zero or
                                mode == .tagtree_one_based_zero_bitplanes_zero)
                            {
                                zero_bit_planes = 0;
                            } else {
                                zero_bit_planes = @intCast(try state_machine.zero_bit_plane_trees[binding.tree_index].decodeValue(&reader, binding.leaf_x, binding.leaf_y, 64));
                            }
                        }
                    } else {
                        included = (try reader.readBit()) == 1;
                    }
                } else return error.InvalidPacketStateIndex;
            },
        }

        if (included) {
            num_coding_passes = try codeblock.decodeNumCodingPasses(&reader);
            const lblock_increment = try codeblock.decodeCommaCode(&reader);
            lblock = cb_state.lblock + lblock_increment;
            const body_lengths = try readPacketBodyLengths(
                allocator,
                &reader,
                cb_state.num_coding_passes,
                num_coding_passes,
                lblock,
                options.body_length_extra_bit,
                codeBlockStyleForSegment(state, layout_entry.coordinate.component_index),
            );
            body_length = body_lengths.body_length;
            segment_lengths = body_lengths.segment_lengths;
        }

        group_entries.append(allocator, .{
            .coordinate = layout_entry.coordinate,
            .state_index = layout_entry.state_index,
            .first_inclusion = first_inclusion,
            .included = included,
            .zero_bit_planes = zero_bit_planes,
            .num_coding_passes = num_coding_passes,
            .lblock = lblock,
            .body_length = body_length,
            .segment_lengths = segment_lengths,
        }) catch |err| {
            allocator.free(segment_lengths);
            return err;
        };
        segment_lengths = &.{};
        group_body_length += body_length;
    }

    reader.alignToByte();
    const header_length = reader.consumedBytes();
    if (!options.headers_only and header_length + group_body_length > payload.len) {
        var included_count: usize = 0;
        var last_included_index: usize = 0;
        for (group_entries.items, 0..) |entry, idx| {
            if (!entry.included) continue;
            included_count += 1;
            last_included_index = idx;
        }
        if (included_count == 1 and header_length <= payload.len) {
            const recovered_body_length = payload.len - header_length;
            if (group_entries.items[last_included_index].segment_lengths.len > 1) return error.TruncatedPacketBody;
            if (group_entries.items[last_included_index].segment_lengths.len == 1) {
                group_entries.items[last_included_index].segment_lengths[0] = @intCast(recovered_body_length);
            }
            group_entries.items[last_included_index].body_length = recovered_body_length;
            group_body_length = recovered_body_length;
        } else {
            return error.TruncatedPacketBody;
        }
    }

    return .{
        .coordinate = group.coordinate,
        .header_length = header_length,
        .body_length = group_body_length,
        .entries = try group_entries.toOwnedSlice(allocator),
    };
}

const PacketBodyLengths = struct {
    body_length: usize,
    segment_lengths: []u32 = &.{},
};

fn readPacketBodyLengths(
    allocator: std.mem.Allocator,
    reader: anytype,
    prior_coding_passes: u16,
    num_coding_passes: u16,
    lblock: u8,
    body_length_extra_bit: bool,
    code_block_style: codeblock.CodeBlockStyle,
) !PacketBodyLengths {
    if (code_block_style.termination) {
        const segment_lengths = try allocator.alloc(u32, num_coding_passes);
        errdefer allocator.free(segment_lengths);

        var body_length: usize = 0;
        const segment_length_bits = lblock;
        for (segment_lengths) |*segment_length| {
            segment_length.* = reader.readBits(segment_length_bits) catch |err| switch (err) {
                error.EndOfBitstream => return error.TruncatedPacketBody,
                else => return err,
            };
            body_length += segment_length.*;
        }
        return .{
            .body_length = body_length,
            .segment_lengths = segment_lengths,
        };
    }

    if (code_block_style.bypass) {
        const segment_count = bypassSegmentContributionCount(prior_coding_passes, num_coding_passes);
        const segment_lengths = try allocator.alloc(u32, segment_count);
        errdefer allocator.free(segment_lengths);

        var body_length: usize = 0;
        var out_index: usize = 0;
        var pass_cursor: u16 = 0;
        var previous_segment_max: u16 = 0;
        var segment_index: u16 = 0;
        const contribution_start = prior_coding_passes;
        const contribution_end = prior_coding_passes + num_coding_passes;
        while (pass_cursor < contribution_end) : (segment_index += 1) {
            const max_passes = bypassSegmentMaxPasses(segment_index, previous_segment_max);
            const segment_start = pass_cursor;
            const segment_end = pass_cursor + max_passes;
            if (segment_end > contribution_start) {
                const start = @max(segment_start, contribution_start);
                const end = @min(segment_end, contribution_end);
                if (start < end) {
                    const passes_in_segment = end - start;
                    const length_bits = lblock + std.math.log2_int(u16, passes_in_segment);
                    const length = reader.readBits(length_bits) catch |err| switch (err) {
                        error.EndOfBitstream => return error.TruncatedPacketBody,
                        else => return err,
                    };
                    segment_lengths[out_index] = length;
                    out_index += 1;
                    body_length += length;
                }
            }
            pass_cursor = segment_end;
            previous_segment_max = max_passes;
        }

        if (out_index != segment_lengths.len) return error.InvalidPacketSpanLayout;
        return .{
            .body_length = body_length,
            .segment_lengths = segment_lengths,
        };
    }

    const length_bits = std.math.log2_int(u16, num_coding_passes) + lblock + @as(u8, if (body_length_extra_bit) 1 else 0);
    const body_length: usize = @intCast(reader.readBits(length_bits) catch |err| switch (err) {
        error.EndOfBitstream => return error.TruncatedPacketBody,
        else => return err,
    });
    return .{ .body_length = body_length };
}

fn bypassSegmentContributionCount(prior_coding_passes: u16, num_coding_passes: u16) usize {
    var count: usize = 0;
    var pass_cursor: u16 = 0;
    var previous_segment_max: u16 = 0;
    var segment_index: u16 = 0;
    const contribution_start = prior_coding_passes;
    const contribution_end = prior_coding_passes + num_coding_passes;
    while (pass_cursor < contribution_end) : (segment_index += 1) {
        const max_passes = bypassSegmentMaxPasses(segment_index, previous_segment_max);
        const segment_start = pass_cursor;
        const segment_end = pass_cursor + max_passes;
        if (segment_end > contribution_start and @max(segment_start, contribution_start) < @min(segment_end, contribution_end)) {
            count += 1;
        }
        pass_cursor = segment_end;
        previous_segment_max = max_passes;
    }
    return count;
}

fn bypassSegmentMaxPasses(segment_index: u16, previous_segment_max: u16) u16 {
    if (segment_index == 0) return 10;
    if (previous_segment_max == 1 or previous_segment_max == 10) return 2;
    return 1;
}

pub fn buildPacketModelFromPayload(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    payload: []const u8,
    payload_base_offset: usize,
    mode: PacketHeaderMode,
) !PacketModel {
    return buildPacketModelFromPayloadWithOptionalPacketLengths(
        allocator,
        state,
        payload,
        payload_base_offset,
        mode,
        null,
    );
}

pub fn buildPacketModelFromPayloadWithPacketLengths(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    payload: []const u8,
    payload_base_offset: usize,
    mode: PacketHeaderMode,
    packet_lengths: []const u32,
) !PacketModel {
    return buildPacketModelFromPayloadWithOptionalPacketLengths(
        allocator,
        state,
        payload,
        payload_base_offset,
        mode,
        packet_lengths,
    );
}

fn buildPacketModelFromPayloadWithOptionalPacketLengths(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    payload: []const u8,
    payload_base_offset: usize,
    mode: PacketHeaderMode,
    packet_lengths: ?[]const u32,
) !PacketModel {
    const layout = try buildPacketLayout(allocator, state);
    errdefer allocator.free(layout);
    const packet_groups = if (packet_lengths == null and producedByAntflyEncoder(state.comments))
        try groupPacketLayoutByPacket(allocator, layout)
    else if (shouldIncludeEmptyPacketGroups(state))
        try buildPacketGroupsIncludingEmptyPackets(allocator, state, layout)
    else
        try buildPacketGroups(allocator, state, layout);
    errdefer allocator.free(packet_groups);

    var unique_codeblock_count: usize = 0;
    for (layout) |entry| unique_codeblock_count = @max(unique_codeblock_count, entry.state_index + 1);
    var state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
    errdefer state_machine.deinit();

    const parsed_groups = parsePacketGroupsFromPayloadForStateOrPacketLengths(allocator, state, layout, packet_groups, &state_machine, mode, payload, packet_lengths) catch |err| switch (err) {
        error.InvalidPacketSpanLayout,
        error.TruncatedPacketBody,
        error.TruncatedPacketHeader,
        error.EndOfBitstream,
        => blk: {
            if (mode != .packet_present_tagtree_first_inclusion) return err;
            state_machine.deinit();
            state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
            break :blk parsePacketGroupsFromPayloadForStateOrPacketLengths(
                allocator,
                state,
                layout,
                packet_groups,
                &state_machine,
                .packet_present_tagtree_one_based_inclusion,
                payload,
                packet_lengths,
            ) catch |fallback_err| switch (fallback_err) {
                error.InvalidPacketSpanLayout,
                error.TruncatedPacketBody,
                error.TruncatedPacketHeader,
                error.EndOfBitstream,
                => blk2: {
                    state_machine.deinit();
                    state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
                    break :blk2 try parsePacketGroupsFromPayloadForStateOrPacketLengths(
                        allocator,
                        state,
                        layout,
                        packet_groups,
                        &state_machine,
                        .packet_present_tagtree_one_based_zero_bitplanes_zero,
                        payload,
                        packet_lengths,
                    );
                },
                else => return fallback_err,
            };
        },
        else => return err,
    };
    defer {
        for (parsed_groups) |*group| group.deinit(allocator);
        allocator.free(parsed_groups);
    }

    return buildPacketModelFromParsedGroups(
        allocator,
        layout,
        packet_groups,
        &state_machine,
        parsed_groups,
        payload_base_offset,
        false,
    );
}

fn parsePacketGroupsFromPayloadForStateOrPacketLengths(
    allocator: std.mem.Allocator,
    state: ?*const codestream.State,
    layout: []const PacketLayoutEntry,
    packet_groups: []const PacketGroup,
    state_machine: *PacketStateMachine,
    mode: PacketHeaderMode,
    payload: []const u8,
    packet_lengths: ?[]const u32,
) ![]ParsedPacketGroup {
    if (packet_lengths) |lengths| {
        const prefer_extra_bits = try allocator.alloc(bool, packet_groups.len);
        defer allocator.free(prefer_extra_bits);
        @memset(prefer_extra_bits, false);
        var result = try buildParsedPacketGroupsFromPacketLengthsChoices(
            allocator,
            state,
            layout,
            packet_groups,
            state_machine,
            mode,
            payload,
            prefer_extra_bits,
            lengths,
        );
        if (!result.fully_consumed) {
            result.deinit(allocator);
            return error.InvalidPacketSpanLayout;
        }
        return result.groups;
    }
    return parsePacketGroupsFromPayloadForState(allocator, state, layout, packet_groups, state_machine, mode, payload);
}

fn producedByAntflyEncoder(comments: []const codestream.Comment) bool {
    for (comments) |comment| {
        if (std.mem.startsWith(u8, comment.text, "antfly-zig j2k v1")) return true;
    }
    return false;
}

pub fn buildPacketModelFromSplitPayload(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    header_payload: []const u8,
    body_payload_base_offset: usize,
    mode: PacketHeaderMode,
) !PacketModel {
    const layout = try buildPacketLayout(allocator, state);
    errdefer allocator.free(layout);

    const prefer_empty_packets = shouldIncludeEmptyPacketGroups(state);
    const primary_packet_groups = if (prefer_empty_packets)
        try buildPacketGroupsIncludingEmptyPackets(allocator, state, layout)
    else
        try buildPacketGroups(allocator, state, layout);
    if (buildPacketModelFromSplitPayloadWithGroups(
        allocator,
        state,
        layout,
        primary_packet_groups,
        header_payload,
        body_payload_base_offset,
        mode,
    )) |model| {
        return model;
    } else |err| switch (err) {
        error.InvalidPacketSpanLayout,
        error.TruncatedPacketBody,
        error.TruncatedPacketHeader,
        error.EndOfBitstream,
        => {
            allocator.free(primary_packet_groups);
        },
        else => {
            allocator.free(primary_packet_groups);
            return err;
        },
    }

    const packet_groups = if (prefer_empty_packets)
        try buildPacketGroups(allocator, state, layout)
    else
        try buildPacketGroupsIncludingEmptyPackets(allocator, state, layout);
    errdefer allocator.free(packet_groups);

    return try buildPacketModelFromSplitPayloadWithGroups(
        allocator,
        state,
        layout,
        packet_groups,
        header_payload,
        body_payload_base_offset,
        mode,
    );
}

pub const SplitPacketModelPrefix = struct {
    model: PacketModel,
    header_length: usize,

    pub fn deinit(self: *SplitPacketModelPrefix, allocator: std.mem.Allocator) void {
        self.model.deinit(allocator);
        self.* = undefined;
    }
};

pub fn buildPacketModelFromSplitPayloadPrefix(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    header_payload: []const u8,
    body_payload_base_offset: usize,
    mode: PacketHeaderMode,
    include_empty_packets: bool,
) !SplitPacketModelPrefix {
    const layout = try buildPacketLayout(allocator, state);
    errdefer allocator.free(layout);

    const packet_groups = if (include_empty_packets)
        try buildPacketGroupsIncludingEmptyPackets(allocator, state, layout)
    else
        try buildPacketGroups(allocator, state, layout);
    errdefer allocator.free(packet_groups);

    return try buildPacketModelFromSplitPayloadPrefixWithGroups(
        allocator,
        state,
        layout,
        packet_groups,
        header_payload,
        body_payload_base_offset,
        mode,
    );
}

fn buildPacketModelFromSplitPayloadWithGroups(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    layout: []PacketLayoutEntry,
    packet_groups: []PacketGroup,
    header_payload: []const u8,
    body_payload_base_offset: usize,
    mode: PacketHeaderMode,
) !PacketModel {
    var unique_codeblock_count: usize = 0;
    for (layout) |entry| unique_codeblock_count = @max(unique_codeblock_count, entry.state_index + 1);
    var state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
    errdefer state_machine.deinit();

    const parsed_groups = parsePacketGroupsFromHeaderPayload(allocator, state, layout, packet_groups, &state_machine, mode, header_payload) catch |err| switch (err) {
        error.InvalidPacketSpanLayout,
        error.TruncatedPacketBody,
        error.TruncatedPacketHeader,
        error.EndOfBitstream,
        => blk: {
            if (mode != .packet_present_tagtree_first_inclusion) return err;
            state_machine.deinit();
            state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
            break :blk parsePacketGroupsFromHeaderPayload(
                allocator,
                state,
                layout,
                packet_groups,
                &state_machine,
                .packet_present_tagtree_one_based_inclusion,
                header_payload,
            ) catch |fallback_err| switch (fallback_err) {
                error.InvalidPacketSpanLayout,
                error.TruncatedPacketBody,
                error.TruncatedPacketHeader,
                error.EndOfBitstream,
                => blk2: {
                    state_machine.deinit();
                    state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
                    break :blk2 try parsePacketGroupsFromHeaderPayload(
                        allocator,
                        state,
                        layout,
                        packet_groups,
                        &state_machine,
                        .packet_present_tagtree_one_based_zero_bitplanes_zero,
                        header_payload,
                    );
                },
                else => return fallback_err,
            };
        },
        else => return err,
    };
    defer {
        for (parsed_groups) |*group| group.deinit(allocator);
        allocator.free(parsed_groups);
    }

    return buildPacketModelFromParsedGroups(
        allocator,
        layout,
        packet_groups,
        &state_machine,
        parsed_groups,
        body_payload_base_offset,
        true,
    );
}

fn buildPacketModelFromSplitPayloadPrefixWithGroups(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    layout: []PacketLayoutEntry,
    packet_groups: []PacketGroup,
    header_payload: []const u8,
    body_payload_base_offset: usize,
    mode: PacketHeaderMode,
) !SplitPacketModelPrefix {
    var unique_codeblock_count: usize = 0;
    for (layout) |entry| unique_codeblock_count = @max(unique_codeblock_count, entry.state_index + 1);
    var state_machine = try PacketStateMachine.init(allocator, layout, unique_codeblock_count);
    errdefer state_machine.deinit();

    var parsed_result = try parsePacketGroupsFromHeaderPayloadPrefix(allocator, state, layout, packet_groups, &state_machine, mode, header_payload);
    defer parsed_result.deinit(allocator);

    const model = try buildPacketModelFromParsedGroups(
        allocator,
        layout,
        packet_groups,
        &state_machine,
        parsed_result.groups,
        body_payload_base_offset,
        true,
    );

    return .{
        .model = model,
        .header_length = parsed_result.consumed_bytes,
    };
}

fn buildPacketModelFromParsedGroups(
    allocator: std.mem.Allocator,
    layout: []PacketLayoutEntry,
    packet_groups: []PacketGroup,
    state_machine: *PacketStateMachine,
    parsed_groups: []const ParsedPacketGroup,
    payload_base_offset: usize,
    split_header_body_payloads: bool,
) !PacketModel {
    var entry_count: usize = 0;
    for (parsed_groups) |group| {
        for (group.entries) |entry| {
            if (entry.included) entry_count += 1;
        }
    }
    const entries = try allocator.alloc(PacketCodeblockEntry, entry_count);
    var out_index: usize = 0;
    errdefer {
        for (entries[0..out_index]) |entry| allocator.free(entry.segment_lengths);
        allocator.free(entries);
    }

    var body_base_offset: usize = payload_base_offset;
    for (parsed_groups) |group| {
        var body_cursor = body_base_offset + if (split_header_body_payloads) 0 else group.header_length;
        for (group.entries) |entry| {
            if (!entry.included) continue;
            const layout_entry = layout[findLayoutIndex(layout, entry.coordinate, entry.state_index) orelse return error.InvalidPacketStateIndex];
            const segment_lengths = try allocator.dupe(u32, entry.segment_lengths);
            entries[out_index] = .{
                .coordinate = entry.coordinate,
                .state_index = entry.state_index,
                .subband = layout_entry.subband,
                .subband_index = layout_entry.subband_index,
                .codeblock_index = layout_entry.codeblock_index,
                .codeblock_x = layout_entry.codeblock_x,
                .codeblock_y = layout_entry.codeblock_y,
                .rect = layout_entry.rect,
                .zero_bit_planes = entry.zero_bit_planes,
                .num_coding_passes = entry.num_coding_passes,
                .lblock = entry.lblock,
                .body_offset = body_cursor,
                .body_length = entry.body_length,
                .segment_lengths = segment_lengths,
            };
            body_cursor += entry.body_length;
            out_index += 1;
        }
        body_base_offset += if (split_header_body_payloads) group.body_length else group.header_length + group.body_length;
    }

    return .{
        .entries = entries,
        .layout = layout,
        .packet_groups = packet_groups,
        .codeblock_states = state_machine.codeblock_states,
        .entry_tree_bindings = state_machine.entry_tree_bindings,
        .inclusion_trees = state_machine.inclusion_trees,
        .zero_bit_plane_trees = state_machine.zero_bit_plane_trees,
    };
}

pub fn buildTier1Segments(allocator: std.mem.Allocator, model: *const PacketModel) ![]Tier1Segment {
    const segments = try allocator.alloc(Tier1Segment, model.entries.len);
    var initialized_segments: usize = 0;
    errdefer {
        for (segments[0..initialized_segments]) |segment| allocator.free(segment.segment_lengths);
        allocator.free(segments);
    }

    const executed_passes = try allocator.alloc(u16, model.codeblock_states.len);
    defer allocator.free(executed_passes);
    @memset(executed_passes, 0);

    for (model.entries, 0..) |entry, idx| {
        if (entry.state_index >= executed_passes.len) return error.InvalidPacketStateIndex;
        const segment_lengths = try allocator.dupe(u32, entry.segment_lengths);
        segments[idx] = .{
            .coordinate = entry.coordinate,
            .state_index = entry.state_index,
            .subband = entry.subband,
            .rect = entry.rect,
            .zero_bit_planes = entry.zero_bit_planes,
            .start_pass_index = executed_passes[entry.state_index],
            .num_coding_passes = entry.num_coding_passes,
            .body_offset = entry.body_offset,
            .body_length = entry.body_length,
            .segment_lengths = segment_lengths,
        };
        initialized_segments += 1;
        executed_passes[entry.state_index] +%= entry.num_coding_passes;
    }

    return segments;
}

pub fn executeTier1Segments(
    allocator: std.mem.Allocator,
    model: *const PacketModel,
    codestream_bytes: []const u8,
    bits_per_component: u8,
    sign_policy: codeblock.SignPolicy,
    refinement_policy: codeblock.RefinementPolicy,
    magnitude_policy: codeblock.MagnitudePolicy,
    zero_bit_plane_adjustment: i8,
    context_init_policy: codeblock.ContextInitPolicy,
) !Tier1Execution {
    return executeTier1SegmentsWithBitplaneResolver(
        allocator,
        model,
        codestream_bytes,
        bits_per_component,
        sign_policy,
        refinement_policy,
        magnitude_policy,
        zero_bit_plane_adjustment,
        context_init_policy,
        null,
    );
}

pub fn executeTier1SegmentsForState(
    allocator: std.mem.Allocator,
    model: *const PacketModel,
    codestream_bytes: []const u8,
    state: *const codestream.State,
    sign_policy: codeblock.SignPolicy,
    refinement_policy: codeblock.RefinementPolicy,
    magnitude_policy: codeblock.MagnitudePolicy,
    zero_bit_plane_adjustment: i8,
    context_init_policy: codeblock.ContextInitPolicy,
) !Tier1Execution {
    return executeTier1SegmentsWithBitplaneResolver(
        allocator,
        model,
        codestream_bytes,
        0,
        sign_policy,
        refinement_policy,
        magnitude_policy,
        zero_bit_plane_adjustment,
        context_init_policy,
        state,
    );
}

fn executeTier1SegmentsWithBitplaneResolver(
    allocator: std.mem.Allocator,
    model: *const PacketModel,
    codestream_bytes: []const u8,
    default_bits_per_component: u8,
    sign_policy: codeblock.SignPolicy,
    refinement_policy: codeblock.RefinementPolicy,
    magnitude_policy: codeblock.MagnitudePolicy,
    zero_bit_plane_adjustment: i8,
    context_init_policy: codeblock.ContextInitPolicy,
    state: ?*const codestream.State,
) !Tier1Execution {
    const segments = try buildTier1Segments(allocator, model);
    errdefer freeTier1Segments(allocator, segments);

    const codeblocks = try allocator.alloc(Tier1CodeblockState, model.codeblock_states.len);
    errdefer allocator.free(codeblocks);
    var initialized_codeblocks: usize = 0;
    errdefer {
        for (codeblocks[0..initialized_codeblocks]) |*codeblock_state| codeblock_state.deinit();
    }

    const rects = try allocator.alloc(tile.CodeBlockRect, model.codeblock_states.len);
    defer allocator.free(rects);
    const subbands = try allocator.alloc(tile.SubbandType, model.codeblock_states.len);
    defer allocator.free(subbands);
    const seen = try allocator.alloc(bool, model.codeblock_states.len);
    defer allocator.free(seen);
    @memset(seen, false);

    for (model.layout) |entry| {
        if (entry.state_index >= model.codeblock_states.len) return error.InvalidPacketStateIndex;
        if (seen[entry.state_index]) continue;
        seen[entry.state_index] = true;
        rects[entry.state_index] = entry.rect;
        subbands[entry.state_index] = entry.subband;
    }

    for (model.codeblock_states, 0..) |codeblock_state, idx| {
        if (!seen[idx]) return error.InvalidPacketStateIndex;
        const adjusted_zero_bit_planes = adjustedZeroBitPlanesForExecution(
            state,
            subbands[idx],
            codeblock_state.zero_bit_planes orelse 0,
            zero_bit_plane_adjustment,
        );
        codeblocks[idx] = .{
            .coordinate = codeblock_state.coordinate,
            .subband = subbands[idx],
            .rect = rects[idx],
            .zero_bit_planes = adjusted_zero_bit_planes,
            .executed_passes = 0,
            .grid = try codeblock.CoefficientGrid.init(allocator, rects[idx].width(), rects[idx].height()),
        };
        initialized_codeblocks += 1;
        codeblocks[idx].grid.clear();
    }

    const segment_next = try allocator.alloc(?usize, segments.len);
    defer allocator.free(segment_next);
    @memset(segment_next, null);
    const group_first = try allocator.alloc(?usize, codeblocks.len);
    defer allocator.free(group_first);
    @memset(group_first, null);
    const group_last = try allocator.alloc(?usize, codeblocks.len);
    defer allocator.free(group_last);
    @memset(group_last, null);

    for (segments, 0..) |segment, idx| {
        if (segment.state_index >= codeblocks.len) return error.InvalidPacketStateIndex;
        const body_end = segment.body_offset + segment.body_length;
        if (body_end > codestream_bytes.len) return error.TruncatedPacketBody;
        if (group_first[segment.state_index] == null) {
            group_first[segment.state_index] = idx;
        } else {
            segment_next[group_last[segment.state_index].?] = idx;
        }
        group_last[segment.state_index] = idx;
    }

    for (codeblocks, 0..) |_, state_index| {
        const first_segment_index = group_first[state_index] orelse continue;
        const first_segment = segments[first_segment_index];
        const segment_context_init_policy = contextInitPolicyForSegment(state, first_segment.subband, context_init_policy);
        const segment_magnitude_policy = magnitudePolicyForSegment(state, first_segment.coordinate.component_index, first_segment.subband, magnitude_policy);
        const segment_refinement_policy = refinementPolicyForSegment(state, first_segment.coordinate.component_index, first_segment.subband, refinement_policy);
        const segment_code_block_style = codeBlockStyleForSegment(state, first_segment.coordinate.component_index);
        var total_body_length: usize = 0;
        var total_passes: u16 = 0;

        var cursor = first_segment_index;
        while (true) {
            const segment = segments[cursor];
            total_body_length += segment.body_length;
            total_passes +%= segment.num_coding_passes;
            if (segment_next[cursor]) |next_index| {
                cursor = next_index;
            } else {
                break;
            }
        }

        const body = try allocator.alloc(u8, total_body_length);
        defer allocator.free(body);

        var passes = std.ArrayListUnmanaged(codeblock.CodingPass).empty;
        errdefer passes.deinit(allocator);
        var segment_lengths = std.ArrayListUnmanaged(u32).empty;
        defer segment_lengths.deinit(allocator);

        var body_offset: usize = 0;
        cursor = first_segment_index;
        while (true) {
            const segment = segments[cursor];
            const segment_end = segment.body_offset + segment.body_length;
            if (segment.body_length != 0) {
                std.mem.copyForwards(u8, body[body_offset .. body_offset + segment.body_length], codestream_bytes[segment.body_offset..segment_end]);
            }

            const bits_per_component = if (state) |resolved_state|
                try codeblockBitplanesForSegment(resolved_state, segment)
            else
                default_bits_per_component;
            const adjusted_segment_zero_bit_planes = adjustedZeroBitPlanesForExecution(
                state,
                segment.subband,
                segment.zero_bit_planes,
                zero_bit_plane_adjustment,
            );
            var plan = try codeblock.planContributionPassRange(
                allocator,
                segment.coordinate.component_index,
                segment.subband,
                adjusted_segment_zero_bit_planes,
                segment.start_pass_index,
                segment.num_coding_passes,
                bits_per_component,
            );
            defer plan.deinit(allocator);
            try passes.appendSlice(allocator, plan.passes);
            if (segment.segment_lengths.len != 0) {
                if (segment_code_block_style.termination and segment.segment_lengths.len != segment.num_coding_passes) return error.PassLengthMismatch;
                var segment_body_length: usize = 0;
                for (segment.segment_lengths) |segment_length| segment_body_length += segment_length;
                if (segment_body_length != segment.body_length) return error.PassLengthMismatch;
                try segment_lengths.appendSlice(allocator, segment.segment_lengths);
            } else if ((segment_code_block_style.termination or segment_code_block_style.bypass) and segment.num_coding_passes != 0) {
                return error.PassLengthMismatch;
            }

            body_offset += segment.body_length;
            if (segment_next[cursor]) |next_index| {
                cursor = next_index;
            } else {
                break;
            }
        }

        codeblocks[state_index].magnitude_scale = switch (segment_magnitude_policy) {
            .openjpeg_midpoint => 2,
            else => 1,
        };
        var combined_plan = codeblock.ContributionPassPlan{
            .component_index = first_segment.coordinate.component_index,
            .subband = first_segment.subband,
            .zero_bit_planes = codeblocks[state_index].zero_bit_planes,
            .start_pass_index = 0,
            .num_passes = total_passes,
            .passes = try passes.toOwnedSlice(allocator),
        };
        defer combined_plan.deinit(allocator);

        if (segment_code_block_style.termination or segment_code_block_style.bypass) {
            try codeblock.executeContributionPassPlanMqWithSegments(
                allocator,
                &codeblocks[state_index].grid,
                &combined_plan,
                body,
                segment_lengths.items,
                sign_policy,
                segment_refinement_policy,
                segment_magnitude_policy,
                segment_context_init_policy,
                segment_code_block_style,
            );
        } else {
            try codeblock.executeContributionPassPlanMq(
                allocator,
                &codeblocks[state_index].grid,
                &combined_plan,
                body,
                sign_policy,
                segment_refinement_policy,
                segment_magnitude_policy,
                segment_context_init_policy,
                segment_code_block_style,
            );
        }
        codeblocks[state_index].executed_passes +%= total_passes;
    }

    return .{
        .segments = segments,
        .codeblocks = codeblocks,
    };
}

fn magnitudePolicyForSegment(
    state: ?*const codestream.State,
    component_index: u16,
    subband: tile.SubbandType,
    base_policy: codeblock.MagnitudePolicy,
) codeblock.MagnitudePolicy {
    _ = subband;
    if (state) |resolved_state| {
        const coding_style = tile.effectiveCodingStyle(resolved_state, component_index) catch return base_policy;
        if (coding_style.wavelet_transform != 0) return .exact_bitplane;
    }
    return base_policy;
}

fn refinementPolicyForSegment(
    state: ?*const codestream.State,
    component_index: u16,
    subband: tile.SubbandType,
    base_policy: codeblock.RefinementPolicy,
) codeblock.RefinementPolicy {
    _ = subband;
    if (state) |resolved_state| {
        const coding_style = tile.effectiveCodingStyle(resolved_state, component_index) catch return base_policy;
        if (coding_style.wavelet_transform != 0) return .exact_bitplane;
    }
    return base_policy;
}

fn contextInitPolicyForSegment(
    state: ?*const codestream.State,
    subband: tile.SubbandType,
    base_policy: codeblock.ContextInitPolicy,
) codeblock.ContextInitPolicy {
    _ = state;
    _ = subband;
    return base_policy;
}

fn codeBlockStyleForSegment(
    state: ?*const codestream.State,
    component_index: u16,
) codeblock.CodeBlockStyle {
    return codeBlockStyleForComponent(state, component_index);
}

fn codeBlockStyleForComponent(
    state: ?*const codestream.State,
    component_index: u16,
) codeblock.CodeBlockStyle {
    const resolved_state = state orelse return .{};
    if (component_index < resolved_state.component_coding_styles.len) {
        if (resolved_state.component_coding_styles[component_index]) |component_style| {
            return codeblock.CodeBlockStyle.fromByte(component_style.code_block_style);
        }
    }
    const coding_style = resolved_state.coding_style orelse return .{};
    return codeblock.CodeBlockStyle.fromByte(coding_style.code_block_style);
}

pub fn codeblockBitplanesForSegment(state: *const codestream.State, segment: Tier1Segment) !u8 {
    const qcd = quantizationStyleForSegment(state, segment.coordinate.component_index) orelse return error.MissingQuantizationStyle;
    if (qcd.step_values.len == 0) return error.UnsupportedQuantizationMode;
    if (qcd.style > 2) return error.UnsupportedQuantizationMode;
    const coding_style = try tile.effectiveCodingStyle(state, segment.coordinate.component_index);
    _ = coding_style.decomposition_levels;
    const expn = quantization.exponentForSubband(
        qcd.style,
        qcd.step_values,
        segment.coordinate.resolution_index,
        segment.subband,
    ) orelse return error.InvalidBitplaneCount;
    if (expn == 0) return error.InvalidBitplaneCount;
    var bitplanes = qcd.guard_bits + expn - 1;
    const roi_shift = roiShiftForSegment(state, segment.coordinate.component_index);
    if (roi_shift > 0) {
        const shifted: u16 = @as(u16, bitplanes) + roi_shift;
        bitplanes = @intCast(@min(shifted, std.math.maxInt(u8)));
    }

    if (segment.num_coding_passes > 0) {
        const total_passes: u16 = segment.start_pass_index + segment.num_coding_passes;
        const min_start_bitplane: u16 = (total_passes - 1 + 2) / 3;
        const min_bitplanes: u16 = min_start_bitplane + 1 + segment.zero_bit_planes;
        if (min_bitplanes > bitplanes) bitplanes = @intCast(min_bitplanes);
    }

    return bitplanes;
}

fn roiShiftForSegment(state: *const codestream.State, component_index: u16) u8 {
    var shift: u8 = 0;
    for (state.rgn_entries) |entry| {
        if (entry.style != 0) continue;
        if (entry.component == component_index) shift = entry.shift;
    }
    return shift;
}

fn quantizationStyleForSegment(state: *const codestream.State, component_index: u16) ?codestream.QuantizationStyle {
    if (component_index < state.component_quantization_styles.len) {
        if (state.component_quantization_styles[component_index]) |component_quantization| return component_quantization;
    }
    return state.quantization_style;
}

/// Compute the QCD step_values index for a subband at a given resolution level.
/// For N decomposition levels:
///   - Resolution 0 (LL_N): index 0
///   - Resolution r > 0: index 1 + 3*(r-1) + subband_offset (HL=0, LH=1, HH=2)
pub fn subbandStepIndex(_: u8, resolution_index: u8, subband: tile.SubbandType) usize {
    if (resolution_index == 0) return 0;
    const subband_offset: usize = switch (subband) {
        .hl => 0,
        .lh => 1,
        .hh => 2,
        .ll => 0,
    };
    return 1 + @as(usize, resolution_index - 1) * 3 + subband_offset;
}

fn adjustZeroBitPlanes(zero_bit_planes: u8, adjustment: i8) u8 {
    if (adjustment >= 0) {
        return zero_bit_planes +% @as(u8, @intCast(adjustment));
    }
    const magnitude: u8 = @intCast(-adjustment);
    return if (zero_bit_planes > magnitude) zero_bit_planes - magnitude else 0;
}

fn adjustedZeroBitPlanesForExecution(
    state: ?*const codestream.State,
    subband: tile.SubbandType,
    zero_bit_planes: u8,
    base_adjustment: i8,
) u8 {
    _ = state;
    _ = subband;
    return adjustZeroBitPlanes(zero_bit_planes, base_adjustment);
}

fn findLayoutIndex(layout: []const PacketLayoutEntry, coordinate: PacketCoordinate, state_index: usize) ?usize {
    for (layout, 0..) |entry, idx| {
        if (entry.state_index == state_index and PacketCoordinate.eql(entry.coordinate, coordinate)) return idx;
    }
    return null;
}

test "lrcp packet layout enumerates one entry per component for bounded zero-decomposition state" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 3);
    defer allocator.free(components);
    @memset(components, .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 });

    var state = codestream.State{
        .header = .{
            .width = 2,
            .height = 1,
            .components = components,
            .tile_width = 2,
            .tile_height = 1,
            .uses_multiple_tiles = false,
        },
        .coding_style = .{
            .progression_order = 0,
            .num_layers = 1,
            .multiple_component_transform = false,
            .decomposition_levels = 0,
            .code_block_width_exponent = 2,
            .code_block_height_exponent = 2,
            .code_block_style = 0,
            .wavelet_transform = 1,
            .precincts_present = false,
        },
        .quantization_style = null,
        .comments = &.{},
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };

    const layout = try buildLrcpPacketLayout(allocator, &state);
    defer allocator.free(layout);
    try std.testing.expectEqual(@as(usize, 3), layout.len);
    try std.testing.expectEqual(@as(usize, 0), layout[0].state_index);
    try std.testing.expectEqual(@as(usize, 1), layout[1].state_index);
    try std.testing.expectEqual(@as(usize, 2), layout[2].state_index);
    try std.testing.expectEqual(.ll, layout[0].subband);
}

test "lrcp packet layout persists state indices across layers" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 1);
    defer allocator.free(components);
    @memset(components, .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 });

    var state = codestream.State{
        .header = .{
            .width = 2,
            .height = 1,
            .components = components,
            .tile_width = 2,
            .tile_height = 1,
            .uses_multiple_tiles = false,
        },
        .coding_style = .{
            .progression_order = 0,
            .num_layers = 2,
            .multiple_component_transform = false,
            .decomposition_levels = 0,
            .code_block_width_exponent = 2,
            .code_block_height_exponent = 2,
            .code_block_style = 0,
            .wavelet_transform = 1,
            .precincts_present = false,
        },
        .quantization_style = null,
        .comments = &.{},
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };

    const layout = try buildLrcpPacketLayout(allocator, &state);
    defer allocator.free(layout);
    try std.testing.expectEqual(@as(usize, 2), layout.len);
    try std.testing.expectEqual(@as(usize, 0), layout[0].state_index);
    try std.testing.expectEqual(@as(usize, 0), layout[1].state_index);
    try std.testing.expectEqual(@as(u16, 0), layout[0].coordinate.layer_index);
    try std.testing.expectEqual(@as(u16, 1), layout[1].coordinate.layer_index);
}

test "packet groups collapse contiguous entries with the same packet coordinate" {
    const allocator = std.testing.allocator;
    const layout = [_]PacketLayoutEntry{
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 1, .y1 = 1 } },
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 1, .subband = .ll, .subband_index = 0, .codeblock_index = 1, .codeblock_x = 1, .codeblock_y = 0, .rect = .{ .x0 = 1, .y0 = 0, .x1 = 2, .y1 = 1 } },
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 1, .precinct_index = 0 }, .state_index = 2, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 1, .y1 = 1 } },
    };
    const groups = try groupPacketLayoutByPacket(allocator, layout[0..]);
    defer allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(usize, 2), groups[0].entry_count);
    try std.testing.expectEqual(@as(u16, 1), groups[1].coordinate.component_index);
}

test "grouped packet parser accumulates a single codeblock across two layers" {
    const allocator = std.testing.allocator;
    const layout = [_]PacketLayoutEntry{
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 2, .y1 = 1 } },
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 1, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 2, .y1 = 1 } },
    };
    const groups = try groupPacketLayoutByPacket(allocator, layout[0..]);
    defer allocator.free(groups);
    var machine = try PacketStateMachine.init(allocator, layout[0..], 1);
    defer machine.deinit();

    const payload = [_]u8{ 0x84, 0xaa, 0xbb, 0x88, 0xcc, 0xdd };
    const parsed = try parsePacketGroupsFromPayload(allocator, layout[0..], groups, &machine, .bounded_fixture, payload[0..]);
    defer {
        for (parsed) |*group| group.deinit(allocator);
        allocator.free(parsed);
    }

    try std.testing.expectEqual(@as(usize, 2), parsed.len);
    try std.testing.expect(parsed[0].entries[0].first_inclusion);
    try std.testing.expect(parsed[0].entries[0].included);
    try std.testing.expectEqual(@as(u8, 0), parsed[0].entries[0].zero_bit_planes);
    try std.testing.expect(parsed[1].entries[0].included);
    try std.testing.expect(!parsed[1].entries[0].first_inclusion);
    try std.testing.expectEqual(@as(u16, 2), machine.codeblock_states[0].num_coding_passes);
    try std.testing.expectEqual(@as(?u16, 0), machine.codeblock_states[0].first_layer_index);
    try std.testing.expectEqual(@as(?u8, 0), machine.codeblock_states[0].zero_bit_planes);
    try std.testing.expect(machine.codeblock_states[0].included);
}

test "grouped packet parser supports tagtree-driven first inclusion across layers" {
    const allocator = std.testing.allocator;
    const layout = [_]PacketLayoutEntry{
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 2, .y1 = 1 } },
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 1, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 2, .y1 = 1 } },
    };
    const groups = try groupPacketLayoutByPacket(allocator, layout[0..]);
    defer allocator.free(groups);
    var machine = try PacketStateMachine.init(allocator, layout[0..], 1);
    defer machine.deinit();

    const payload = [_]u8{ 0xc4, 0xaa, 0xbb, 0x88, 0xcc, 0xdd };
    const parsed = try parsePacketGroupsFromPayload(allocator, layout[0..], groups, &machine, .tagtree_first_inclusion, payload[0..]);
    defer {
        for (parsed) |*group| group.deinit(allocator);
        allocator.free(parsed);
    }

    try std.testing.expectEqual(@as(usize, 2), parsed.len);
    try std.testing.expect(parsed[0].entries[0].first_inclusion);
    try std.testing.expect(parsed[0].entries[0].included);
    try std.testing.expectEqual(@as(u8, 0), parsed[0].entries[0].zero_bit_planes);
    try std.testing.expect(parsed[1].entries[0].included);
    try std.testing.expect(!parsed[1].entries[0].first_inclusion);
    try std.testing.expectEqual(@as(u16, 2), machine.codeblock_states[0].num_coding_passes);
    try std.testing.expectEqual(@as(?u16, 0), machine.codeblock_states[0].first_layer_index);
    try std.testing.expectEqual(@as(u32, 0), try machine.inclusion_trees[0].leafValue(0, 0));
}

test "tagtree packet parser supports multiple codeblocks in one packet tree" {
    const allocator = std.testing.allocator;
    const layout = [_]PacketLayoutEntry{
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 1, .y1 = 1 } },
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 1, .subband = .ll, .subband_index = 0, .codeblock_index = 1, .codeblock_x = 1, .codeblock_y = 0, .rect = .{ .x0 = 1, .y0 = 0, .x1 = 2, .y1 = 1 } },
    };
    const groups = try groupPacketLayoutByPacket(allocator, layout[0..]);
    defer allocator.free(groups);
    var machine = try PacketStateMachine.init(allocator, layout[0..], 2);
    defer machine.deinit();

    try std.testing.expectEqual(@as(usize, 1), machine.inclusion_trees.len);
    try std.testing.expectEqual(@as(usize, 2), machine.inclusion_trees[0].width);
    try std.testing.expectEqual(@as(usize, 1), machine.inclusion_trees[0].height);
    try std.testing.expectEqual(@as(usize, 0), machine.entry_tree_bindings[0].tree_index);
    try std.testing.expectEqual(@as(usize, 0), machine.entry_tree_bindings[0].leaf_x);
    try std.testing.expectEqual(@as(usize, 1), machine.entry_tree_bindings[1].leaf_x);

    const payload = [_]u8{ 0xf0, 0xe1, 0xaa, 0xbb };
    const parsed = try parsePacketGroupsFromPayload(allocator, layout[0..], groups, &machine, .tagtree_first_inclusion, payload[0..]);
    defer {
        for (parsed) |*group| group.deinit(allocator);
        allocator.free(parsed);
    }

    try std.testing.expectEqual(@as(usize, 1), parsed.len);
    try std.testing.expectEqual(@as(usize, 2), parsed[0].entries.len);
    try std.testing.expect(parsed[0].entries[0].included);
    try std.testing.expect(parsed[0].entries[1].included);
    try std.testing.expectEqual(@as(u8, 0), parsed[0].entries[0].zero_bit_planes);
    try std.testing.expectEqual(@as(u8, 0), parsed[0].entries[1].zero_bit_planes);
    try std.testing.expectEqual(@as(u16, 1), machine.codeblock_states[0].num_coding_passes);
    try std.testing.expectEqual(@as(u16, 1), machine.codeblock_states[1].num_coding_passes);
    try std.testing.expectEqual(@as(u32, 0), try machine.inclusion_trees[0].leafValue(0, 0));
    try std.testing.expectEqual(@as(u32, 0), try machine.inclusion_trees[0].leafValue(1, 0));
}

test "generic packet model builder parses decomposition packet groups in tagtree mode" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 1);
    defer allocator.free(components);
    @memset(components, .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 });

    var state = codestream.State{
        .header = .{
            .width = 8,
            .height = 8,
            .components = components,
            .tile_width = 8,
            .tile_height = 8,
            .uses_multiple_tiles = false,
        },
        .coding_style = .{
            .progression_order = 0,
            .num_layers = 1,
            .multiple_component_transform = false,
            .decomposition_levels = 1,
            .code_block_width_exponent = 0,
            .code_block_height_exponent = 0,
            .code_block_style = 0,
            .wavelet_transform = 1,
            .precincts_present = false,
        },
        .quantization_style = null,
        .comments = &.{},
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };

    const payload = [_]u8{
        0xc2,
        0xaa,
        0xc3,
        0x87,
        0x08,
        0xbb,
        0xcc,
        0xdd,
    };

    var model = try buildPacketModelFromPayload(allocator, &state, payload[0..], 0, .tagtree_first_inclusion);
    defer model.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 4), model.layout.len);
    try std.testing.expectEqual(@as(usize, 2), model.packet_groups.len);
    try std.testing.expectEqual(@as(usize, 4), model.entries.len);
    try std.testing.expectEqual(@as(usize, 4), model.codeblock_states.len);
    try std.testing.expectEqual(@as(usize, 4), model.inclusion_trees.len);
    try std.testing.expectEqual(.ll, model.entries[0].subband);
    try std.testing.expectEqual(.hl, model.entries[1].subband);
    try std.testing.expectEqual(.lh, model.entries[2].subband);
    try std.testing.expectEqual(.hh, model.entries[3].subband);
    try std.testing.expectEqual(@as(usize, 1), model.entries[0].body_offset);
    try std.testing.expectEqual(@as(usize, 5), model.entries[1].body_offset);
    try std.testing.expectEqual(@as(usize, 6), model.entries[2].body_offset);
    try std.testing.expectEqual(@as(usize, 7), model.entries[3].body_offset);
}

test "deterministic decomposed packet model builds for 3x2 grayscale payload" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 1);
    defer allocator.free(components);
    components[0] = .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 };
    const step_values = try allocator.alloc(u16, 4);
    defer allocator.free(step_values);
    step_values[0] = 0x40;
    step_values[1] = 0x40;
    step_values[2] = 0x48;
    step_values[3] = 0x48;

    var state = codestream.State{
        .header = .{
            .width = 3,
            .height = 2,
            .components = components,
            .tile_width = 3,
            .tile_height = 2,
            .uses_multiple_tiles = false,
        },
        .coding_style = .{
            .progression_order = 0,
            .num_layers = 1,
            .multiple_component_transform = false,
            .decomposition_levels = 1,
            .code_block_width_exponent = 0,
            .code_block_height_exponent = 0,
            .code_block_style = 0,
            .wavelet_transform = 1,
            .precincts_present = false,
        },
        .quantization_style = .{
            .style = 0,
            .guard_bits = 2,
            .step_values = step_values,
        },
        .comments = &.{},
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };

    const payload = [_]u8{
        0xC7, 0xD4, 0x06, 0x00, 0x9A, 0x3F, 0xC7, 0xDA, 0x05, 0x1F, 0x68,
        0x1C, 0x7E, 0x00, 0x40, 0x04, 0x4F, 0x05, 0x61, 0x67, 0x01, 0xA7,
    };

    const payload_base_offset: usize = 0xCE;
    const full_codestream = try allocator.alloc(u8, payload_base_offset + payload.len);
    defer allocator.free(full_codestream);
    @memset(full_codestream, 0);
    @memcpy(full_codestream[payload_base_offset..][0..payload.len], payload[0..]);

    var model = try buildPacketModelFromPayload(allocator, &state, payload[0..], payload_base_offset, .packet_present_tagtree_first_inclusion);
    defer model.deinit(allocator);

    try std.testing.expect(model.entries.len > 0);

    var execution = try executeTier1SegmentsForState(
        allocator,
        &model,
        full_codestream,
        &state,
        .decomposed_single_component_split,
        .standard_additive,
        .exact_bitplane,
        0,
        .decomposed_single_component_relaxed_zc0,
    );
    defer execution.deinit(allocator);

    const reconstruct = @import("reconstruct.zig");
    const pixels = try reconstruct.reconstructTier1ExecutionU8(allocator, &state, &execution);
    defer allocator.free(pixels);
    try std.testing.expectEqual(@as(usize, 6), pixels.len);
}

test "packet state machine accumulates passes across layers for one codeblock" {
    const allocator = std.testing.allocator;
    const layout = [_]PacketLayoutEntry{
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 0, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 2, .y1 = 1 } },
        .{ .coordinate = .{ .tile_index = 0, .layer_index = 1, .resolution_index = 0, .component_index = 0, .precinct_index = 0 }, .state_index = 0, .subband = .ll, .subband_index = 0, .codeblock_index = 0, .codeblock_x = 0, .codeblock_y = 0, .rect = .{ .x0 = 0, .y0 = 0, .x1 = 2, .y1 = 1 } },
    };
    var machine = try PacketStateMachine.init(allocator, layout[0..], 1);
    defer machine.deinit();

    try machine.applyEntryUpdate(0, 5, 3, 4);
    try machine.applyEntryUpdate(1, 5, 2, 5);
    try std.testing.expect(machine.codeblock_states[0].included);
    try std.testing.expectEqual(@as(?u16, 0), machine.codeblock_states[0].first_layer_index);
    try std.testing.expectEqual(@as(?u8, 5), machine.codeblock_states[0].zero_bit_planes);
    try std.testing.expectEqual(@as(u16, 5), machine.codeblock_states[0].num_coding_passes);
    try std.testing.expectEqual(@as(u8, 5), machine.codeblock_states[0].lblock);
}

test "lrcp packet layout expands decomposition subbands" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 1);
    defer allocator.free(components);
    @memset(components, .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 });

    var state = codestream.State{
        .header = .{
            .width = 8,
            .height = 8,
            .components = components,
            .tile_width = 8,
            .tile_height = 8,
            .uses_multiple_tiles = false,
        },
        .coding_style = .{
            .progression_order = 0,
            .num_layers = 1,
            .multiple_component_transform = false,
            .decomposition_levels = 1,
            .code_block_width_exponent = 0,
            .code_block_height_exponent = 0,
            .code_block_style = 0,
            .wavelet_transform = 1,
            .precincts_present = false,
        },
        .quantization_style = null,
        .comments = &.{},
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };

    const layout = try buildLrcpPacketLayout(allocator, &state);
    defer allocator.free(layout);
    try std.testing.expectEqual(@as(usize, 4), layout.len);
    try std.testing.expectEqual(.ll, layout[0].subband);
    try std.testing.expectEqual(.hl, layout[1].subband);
    try std.testing.expectEqual(.lh, layout[2].subband);
    try std.testing.expectEqual(.hh, layout[3].subband);
}

fn buildTwoComponentTestState(allocator: std.mem.Allocator, progression_order: u8) !struct { state: codestream.State, components: []codestream.Component } {
    const components = try allocator.alloc(codestream.Component, 2);
    @memset(components, .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 });
    return .{
        .state = .{
            .header = .{
                .width = 8,
                .height = 8,
                .components = components,
                .tile_width = 8,
                .tile_height = 8,
                .uses_multiple_tiles = false,
            },
            .coding_style = .{
                .progression_order = progression_order,
                .num_layers = 1,
                .multiple_component_transform = false,
                .decomposition_levels = 1,
                .code_block_width_exponent = 0,
                .code_block_height_exponent = 0,
                .code_block_style = 0,
                .wavelet_transform = 1,
                .precincts_present = false,
            },
            .quantization_style = null,
            .comments = &.{},
            .tile_parts = &.{},
            .has_start_of_data = true,
            .has_end_of_codestream = true,
        },
        .components = components,
    };
}

test "rlcp packet layout produces correct entry count for two components" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 1);
    defer allocator.free(ctx.components);

    const layout = try buildRlcpPacketLayout(allocator, &ctx.state);
    defer allocator.free(layout);

    // 2 components * (1 LL + 3 detail subbands) * 1 layer = 8 entries
    try std.testing.expectEqual(@as(usize, 8), layout.len);

    // RLCP: resolution is outermost. Resolution 0 entries (LL) should come first for both components.
    try std.testing.expectEqual(@as(u8, 0), layout[0].coordinate.resolution_index);
    try std.testing.expectEqual(@as(u16, 0), layout[0].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 0), layout[1].coordinate.resolution_index);
    try std.testing.expectEqual(@as(u16, 1), layout[1].coordinate.component_index);
    // Then resolution 1 entries.
    try std.testing.expectEqual(@as(u8, 1), layout[2].coordinate.resolution_index);
}

test "rpcl packet layout produces correct entry count for two components" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 2);
    defer allocator.free(ctx.components);

    const layout = try buildRpclPacketLayout(allocator, &ctx.state);
    defer allocator.free(layout);

    try std.testing.expectEqual(@as(usize, 8), layout.len);

    // RPCL: resolution outermost, then position (precinct=0), then component, then layer.
    // Resolution 0: component 0 layer 0, component 1 layer 0.
    try std.testing.expectEqual(@as(u8, 0), layout[0].coordinate.resolution_index);
    try std.testing.expectEqual(@as(u16, 0), layout[0].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 0), layout[1].coordinate.resolution_index);
    try std.testing.expectEqual(@as(u16, 1), layout[1].coordinate.component_index);
    // Resolution 1 entries follow.
    try std.testing.expectEqual(@as(u8, 1), layout[2].coordinate.resolution_index);
}

test "pcrl packet layout produces correct entry count for two components" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 3);
    defer allocator.free(ctx.components);

    const layout = try buildPcrlPacketLayout(allocator, &ctx.state);
    defer allocator.free(layout);

    try std.testing.expectEqual(@as(usize, 8), layout.len);

    // PCRL: position (precinct=0) outermost, then component, then resolution, then layer.
    // Component 0: res 0 (LL), res 1 (HL, LH, HH) -- 4 entries.
    try std.testing.expectEqual(@as(u16, 0), layout[0].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 0), layout[0].coordinate.resolution_index);
    try std.testing.expectEqual(@as(u16, 0), layout[1].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 1), layout[1].coordinate.resolution_index);
    // Component 1 starts at index 4.
    try std.testing.expectEqual(@as(u16, 1), layout[4].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 0), layout[4].coordinate.resolution_index);
}

test "cprl packet layout produces correct entry count for two components" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 4);
    defer allocator.free(ctx.components);

    const layout = try buildCprlPacketLayout(allocator, &ctx.state);
    defer allocator.free(layout);

    try std.testing.expectEqual(@as(usize, 8), layout.len);

    // CPRL: component outermost, then position (precinct=0), then resolution, then layer.
    // Component 0: res 0 (LL), res 1 (HL, LH, HH) -- 4 entries.
    try std.testing.expectEqual(@as(u16, 0), layout[0].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 0), layout[0].coordinate.resolution_index);
    try std.testing.expectEqual(@as(u16, 0), layout[3].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 1), layout[3].coordinate.resolution_index);
    // Component 1 starts at index 4.
    try std.testing.expectEqual(@as(u16, 1), layout[4].coordinate.component_index);
    try std.testing.expectEqual(@as(u8, 0), layout[4].coordinate.resolution_index);
}

test "buildPacketLayout dispatches to correct progression order" {
    const allocator = std.testing.allocator;

    // Test LRCP (0).
    {
        var ctx = try buildTwoComponentTestState(allocator, 0);
        defer allocator.free(ctx.components);
        const layout = try buildPacketLayout(allocator, &ctx.state);
        defer allocator.free(layout);
        try std.testing.expectEqual(@as(usize, 8), layout.len);
    }

    // Test RLCP (1).
    {
        var ctx = try buildTwoComponentTestState(allocator, 1);
        defer allocator.free(ctx.components);
        const layout = try buildPacketLayout(allocator, &ctx.state);
        defer allocator.free(layout);
        try std.testing.expectEqual(@as(usize, 8), layout.len);
    }

    // Test RPCL (2).
    {
        var ctx = try buildTwoComponentTestState(allocator, 2);
        defer allocator.free(ctx.components);
        const layout = try buildPacketLayout(allocator, &ctx.state);
        defer allocator.free(layout);
        try std.testing.expectEqual(@as(usize, 8), layout.len);
    }

    // Test PCRL (3).
    {
        var ctx = try buildTwoComponentTestState(allocator, 3);
        defer allocator.free(ctx.components);
        const layout = try buildPacketLayout(allocator, &ctx.state);
        defer allocator.free(layout);
        try std.testing.expectEqual(@as(usize, 8), layout.len);
    }

    // Test CPRL (4).
    {
        var ctx = try buildTwoComponentTestState(allocator, 4);
        defer allocator.free(ctx.components);
        const layout = try buildPacketLayout(allocator, &ctx.state);
        defer allocator.free(layout);
        try std.testing.expectEqual(@as(usize, 8), layout.len);
    }

    // Test unsupported (5).
    {
        var ctx = try buildTwoComponentTestState(allocator, 5);
        defer allocator.free(ctx.components);
        try std.testing.expectError(error.UnsupportedProgressionOrder, buildPacketLayout(allocator, &ctx.state));
    }
}

test "buildPacketLayoutWithWindows falls back to single-order when empty" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 0);
    defer allocator.free(ctx.components);

    const default_layout = try buildPacketLayout(allocator, &ctx.state);
    defer allocator.free(default_layout);
    const windowed_layout = try buildPacketLayoutWithWindows(allocator, &ctx.state, &.{});
    defer allocator.free(windowed_layout);

    try std.testing.expectEqual(default_layout.len, windowed_layout.len);
    for (default_layout, windowed_layout) |a, b| {
        try std.testing.expectEqual(a.coordinate.resolution_index, b.coordinate.resolution_index);
        try std.testing.expectEqual(a.coordinate.component_index, b.coordinate.component_index);
        try std.testing.expectEqual(a.coordinate.layer_index, b.coordinate.layer_index);
    }
}

test "buildPacketLayoutWithWindows enumerates 3 windows with different orders" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 0);
    defer allocator.free(ctx.components);

    const full = try buildPacketLayout(allocator, &ctx.state);
    defer allocator.free(full);
    // Full layout covers 2 components x 2 resolutions x 1 layer x (varying codeblocks/subbands).
    try std.testing.expect(full.len > 0);

    const windows = [_]PacketProgressionWindow{
        // Window 0: resolution 0 only, both components, LRCP.
        .{ .rs = 0, .re = 1, .cs = 0, .ce = 2, .lye = 1, .order = 0 },
        // Window 1: resolution 1, component 0 only, RLCP.
        .{ .rs = 1, .re = 2, .cs = 0, .ce = 1, .lye = 1, .order = 1 },
        // Window 2: resolution 1, component 1 only, CPRL.
        .{ .rs = 1, .re = 2, .cs = 1, .ce = 2, .lye = 1, .order = 4 },
    };
    const layout = try buildPacketLayoutWithWindows(allocator, &ctx.state, &windows);
    defer allocator.free(layout);
    // Union of the three windows exactly covers the full enumeration space.
    try std.testing.expectEqual(full.len, layout.len);

    // Partition the layout into per-window sections by scanning attributes.
    // Window 0 runs first and must see resolution 0 only.
    var idx: usize = 0;
    while (idx < layout.len and layout[idx].coordinate.resolution_index == 0) : (idx += 1) {}
    try std.testing.expect(idx > 0);
    const w0_end = idx;
    for (layout[0..w0_end]) |entry| {
        try std.testing.expectEqual(@as(u8, 0), entry.coordinate.resolution_index);
    }
    // Next block is window 1: resolution 1, component 0.
    while (idx < layout.len and layout[idx].coordinate.component_index == 0) : (idx += 1) {
        try std.testing.expectEqual(@as(u8, 1), layout[idx].coordinate.resolution_index);
    }
    try std.testing.expect(idx > w0_end);
    // Tail is window 2: resolution 1, component 1.
    for (layout[idx..]) |entry| {
        try std.testing.expectEqual(@as(u8, 1), entry.coordinate.resolution_index);
        try std.testing.expectEqual(@as(u16, 1), entry.coordinate.component_index);
    }
}

test "buildPacketLayoutWithWindows rejects invalid window bounds" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 0);
    defer allocator.free(ctx.components);
    const bad = [_]PacketProgressionWindow{
        .{ .rs = 1, .re = 1, .cs = 0, .ce = 2, .lye = 1, .order = 0 },
    };
    try std.testing.expectError(
        error.InvalidProgressionWindow,
        buildPacketLayoutWithWindows(allocator, &ctx.state, &bad),
    );
}

test "buildPacketLayout applies POC entries with clamped marker bounds" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 0);
    defer allocator.free(ctx.components);
    var poc_entries = [_]codestream.PocEntry{
        .{
            .rs_poc = 0,
            .cs_poc = 0,
            .lye_poc = 9,
            .re_poc = 255,
            .ce_poc = 255,
            .progression_order = 4,
        },
    };
    ctx.state.poc_entries = &poc_entries;

    const expected = try buildPacketLayoutForOrder(allocator, &ctx.state, 4);
    defer allocator.free(expected);
    const layout = try buildPacketLayout(allocator, &ctx.state);
    defer allocator.free(layout);

    try std.testing.expectEqual(expected.len, layout.len);
    for (expected, layout) |a, b| {
        try std.testing.expect(PacketCoordinate.eql(a.coordinate, b.coordinate));
        try std.testing.expectEqual(a.state_index, b.state_index);
    }
}

test "buildPacketLayoutWithWindows skips duplicate packet coordinates" {
    const allocator = std.testing.allocator;
    var ctx = try buildTwoComponentTestState(allocator, 0);
    defer allocator.free(ctx.components);

    const full = try buildPacketLayoutForOrder(allocator, &ctx.state, 0);
    defer allocator.free(full);
    const windows = [_]PacketProgressionWindow{
        .{ .rs = 0, .re = 2, .cs = 0, .ce = 2, .lye = 1, .order = 0 },
        .{ .rs = 0, .re = 2, .cs = 0, .ce = 2, .lye = 1, .order = 1 },
    };
    const layout = try buildPacketLayoutWithWindows(allocator, &ctx.state, &windows);
    defer allocator.free(layout);

    try std.testing.expectEqual(full.len, layout.len);
}
