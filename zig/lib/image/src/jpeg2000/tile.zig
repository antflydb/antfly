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

pub const native_port_available = true;

pub const ResBounds = struct {
    width: u32,
    height: u32,
    x0: u32,
    y0: u32,
};

pub const SubbandType = enum {
    ll,
    lh,
    hl,
    hh,
};

pub const CodeBlockRect = struct {
    x0: u32,
    y0: u32,
    x1: u32,
    y1: u32,

    pub fn width(self: CodeBlockRect) u32 {
        return self.x1 - self.x0;
    }

    pub fn height(self: CodeBlockRect) u32 {
        return self.y1 - self.y0;
    }
};

pub const CodeBlockCoordinate = struct {
    component_index: u16,
    resolution_index: u8,
    subband_index: u8,
    subband: SubbandType,
    precinct_index: u32,
    codeblock_index: u32,
    codeblock_x: u32,
    codeblock_y: u32,
    rect: CodeBlockRect,
};

pub const SubbandGeometry = struct {
    band: SubbandType,
    bounds: ResBounds,
    origin_x: u32 = 0,
    origin_y: u32 = 0,
    grid_x0: u32 = 0,
    grid_y0: u32 = 0,
    code_block_width: u32 = 1,
    code_block_height: u32 = 1,
    codeblocks_x: u32,
    codeblocks_y: u32,

    pub fn codeblockCount(self: SubbandGeometry) u32 {
        return self.codeblocks_x * self.codeblocks_y;
    }
};

pub const ResolutionGeometry = struct {
    level: u8,
    bounds: ResBounds,
    origin_x: u32 = 0,
    origin_y: u32 = 0,
    precinct_width: u32,
    precinct_height: u32,
    subbands: []SubbandGeometry,

    pub fn deinit(self: *ResolutionGeometry, allocator: std.mem.Allocator) void {
        allocator.free(self.subbands);
        self.* = undefined;
    }
};

pub const ComponentGeometry = struct {
    component_index: u16,
    bounds: ResBounds,
    origin_x: u32 = 0,
    origin_y: u32 = 0,
    decomposition_levels: u8,
    code_block_width: u32,
    code_block_height: u32,
    resolutions: []ResolutionGeometry,

    pub fn deinit(self: *ComponentGeometry, allocator: std.mem.Allocator) void {
        for (self.resolutions) |*res| res.deinit(allocator);
        allocator.free(self.resolutions);
        self.* = undefined;
    }

    pub fn totalCodeblocks(self: *const ComponentGeometry) u32 {
        var total: u32 = 0;
        for (self.resolutions) |resolution| {
            for (resolution.subbands) |subband| total += subband.codeblockCount();
        }
        return total;
    }
};

pub const TileGeometry = struct {
    bounds: ResBounds,
    components: []ComponentGeometry,

    pub fn deinit(self: *TileGeometry, allocator: std.mem.Allocator) void {
        for (self.components) |*component| component.deinit(allocator);
        allocator.free(self.components);
        self.* = undefined;
    }

    pub fn totalCodeblocks(self: *const TileGeometry) u32 {
        var total: u32 = 0;
        for (self.components) |component| total += component.totalCodeblocks();
        return total;
    }
};

pub fn effectiveCodingStyle(state: *const codestream.State, component_index: usize) !codestream.CodingStyle {
    if (component_index < state.component_coding_styles.len) {
        if (state.component_coding_styles[component_index]) |component_style| return component_style;
    }
    return state.coding_style orelse error.MissingCodingStyle;
}

pub const TileGrid = struct {
    tile_width: u32,
    tile_height: u32,
    x_offset: u32,
    y_offset: u32,
    image_x_offset: u32,
    image_y_offset: u32,
    image_width: u32,
    image_height: u32,
    cols: u32,
    rows: u32,

    pub fn tileCount(self: TileGrid) u32 {
        return self.cols * self.rows;
    }

    pub fn tileBounds(self: TileGrid, tile_index: u32) ResBounds {
        const col = tile_index % self.cols;
        const row = tile_index / self.cols;
        const raw_x0 = self.x_offset + col * self.tile_width;
        const raw_y0 = self.y_offset + row * self.tile_height;
        const image_x1 = self.image_width;
        const image_y1 = self.image_height;
        const x0 = @max(raw_x0, self.image_x_offset);
        const y0 = @max(raw_y0, self.image_y_offset);
        const x1 = @min(raw_x0 + self.tile_width, image_x1);
        const y1 = @min(raw_y0 + self.tile_height, image_y1);
        return .{
            .x0 = x0,
            .y0 = y0,
            .width = x1 - x0,
            .height = y1 - y0,
        };
    }
};

pub fn buildTileGrid(state: *const codestream.State) TileGrid {
    const image_x1 = state.header.x_offset + state.header.width;
    const image_y1 = state.header.y_offset + state.header.height;
    const cols = if (state.header.tile_width > 0)
        (image_x1 - state.header.tile_x_offset + state.header.tile_width - 1) / state.header.tile_width
    else
        1;
    const rows = if (state.header.tile_height > 0)
        (image_y1 - state.header.tile_y_offset + state.header.tile_height - 1) / state.header.tile_height
    else
        1;
    return .{
        .tile_width = state.header.tile_width,
        .tile_height = state.header.tile_height,
        .x_offset = state.header.tile_x_offset,
        .y_offset = state.header.tile_y_offset,
        .image_x_offset = state.header.x_offset,
        .image_y_offset = state.header.y_offset,
        .image_width = image_x1,
        .image_height = image_y1,
        .cols = cols,
        .rows = rows,
    };
}

pub fn buildTileGeometry(allocator: std.mem.Allocator, state: *const codestream.State, tile_bounds: ResBounds) !TileGeometry {
    const components = try allocator.alloc(ComponentGeometry, state.header.components.len);
    errdefer allocator.free(components);

    for (components, state.header.components, 0..) |*component, siz_comp, component_index| {
        const coding_style = try effectiveCodingStyle(state, component_index);
        const cb_width = @as(u32, 1) << @intCast(coding_style.code_block_width_exponent + 2);
        const cb_height = @as(u32, 1) << @intCast(coding_style.code_block_height_exponent + 2);
        const decomposition_levels = coding_style.decomposition_levels;
        const cap_codeblocks_to_precincts = !producedByAntflyEncoder(state.comments);
        const comp_span = componentSpan(tile_bounds.x0, tile_bounds.width, siz_comp.xrsiz);
        const comp_span_y = componentSpan(tile_bounds.y0, tile_bounds.height, siz_comp.yrsiz);
        const comp_bounds: ResBounds = .{
            .width = comp_span.width,
            .height = comp_span_y.width,
            .x0 = 0,
            .y0 = 0,
        };
        const resolutions = try allocator.alloc(ResolutionGeometry, decomposition_levels + 1);
        errdefer allocator.free(resolutions);

        var resolution_index: usize = 0;
        while (resolution_index < resolutions.len) : (resolution_index += 1) {
            const reduce = decomposition_levels - @as(u8, @intCast(resolution_index));
            const res_span = resolutionSpan(comp_span.origin, comp_bounds.width, reduce);
            const res_span_y = resolutionSpan(comp_span_y.origin, comp_bounds.height, reduce);
            const res_bounds: ResBounds = .{
                .width = res_span.width,
                .height = res_span_y.width,
                .x0 = comp_bounds.x0,
                .y0 = comp_bounds.y0,
            };
            const precinct = precinctSizeForResolution(coding_style, @intCast(resolution_index), res_bounds);

            const subbands = try buildResolutionSubbands(
                allocator,
                res_bounds,
                res_span.origin,
                res_span_y.origin,
                cb_width,
                cb_height,
                precinct.width,
                precinct.height,
                coding_style.precincts_present and cap_codeblocks_to_precincts,
                @intCast(resolution_index),
            );
            resolutions[resolution_index] = .{
                .level = @intCast(resolution_index),
                .bounds = res_bounds,
                .origin_x = res_span.origin,
                .origin_y = res_span_y.origin,
                .precinct_width = precinct.width,
                .precinct_height = precinct.height,
                .subbands = subbands,
            };
        }

        component.* = .{
            .component_index = @intCast(component_index),
            .bounds = comp_bounds,
            .origin_x = comp_span.origin,
            .origin_y = comp_span_y.origin,
            .decomposition_levels = decomposition_levels,
            .code_block_width = cb_width,
            .code_block_height = cb_height,
            .resolutions = resolutions,
        };
    }

    return .{
        .bounds = tile_bounds,
        .components = components,
    };
}

pub fn buildSingleTileGeometry(allocator: std.mem.Allocator, state: *const codestream.State) !TileGeometry {
    if (state.header.uses_multiple_tiles) return error.UnsupportedTileLayout;
    const components = try allocator.alloc(ComponentGeometry, state.header.components.len);
    errdefer allocator.free(components);

    for (components, state.header.components, 0..) |*component, siz_comp, component_index| {
        const coding_style = try effectiveCodingStyle(state, component_index);
        const cb_width = @as(u32, 1) << @intCast(coding_style.code_block_width_exponent + 2);
        const cb_height = @as(u32, 1) << @intCast(coding_style.code_block_height_exponent + 2);
        const decomposition_levels = coding_style.decomposition_levels;
        const cap_codeblocks_to_precincts = !producedByAntflyEncoder(state.comments);
        const comp_span = componentSpan(state.header.x_offset, state.header.width, siz_comp.xrsiz);
        const comp_span_y = componentSpan(state.header.y_offset, state.header.height, siz_comp.yrsiz);
        const comp_bounds: ResBounds = .{
            .width = comp_span.width,
            .height = comp_span_y.width,
            .x0 = 0,
            .y0 = 0,
        };
        const resolutions = try allocator.alloc(ResolutionGeometry, decomposition_levels + 1);
        errdefer allocator.free(resolutions);

        var resolution_index: usize = 0;
        while (resolution_index < resolutions.len) : (resolution_index += 1) {
            const reduce = decomposition_levels - @as(u8, @intCast(resolution_index));
            const res_span = resolutionSpan(comp_span.origin, comp_bounds.width, reduce);
            const res_span_y = resolutionSpan(comp_span_y.origin, comp_bounds.height, reduce);
            const res_bounds: ResBounds = .{
                .width = res_span.width,
                .height = res_span_y.width,
                .x0 = 0,
                .y0 = 0,
            };
            const precinct = precinctSizeForResolution(coding_style, @intCast(resolution_index), res_bounds);

            const subbands = try buildResolutionSubbands(
                allocator,
                res_bounds,
                res_span.origin,
                res_span_y.origin,
                cb_width,
                cb_height,
                precinct.width,
                precinct.height,
                coding_style.precincts_present and cap_codeblocks_to_precincts,
                @intCast(resolution_index),
            );
            resolutions[resolution_index] = .{
                .level = @intCast(resolution_index),
                .bounds = res_bounds,
                .origin_x = res_span.origin,
                .origin_y = res_span_y.origin,
                .precinct_width = precinct.width,
                .precinct_height = precinct.height,
                .subbands = subbands,
            };
        }

        component.* = .{
            .component_index = @intCast(component_index),
            .bounds = comp_bounds,
            .origin_x = comp_span.origin,
            .origin_y = comp_span_y.origin,
            .decomposition_levels = decomposition_levels,
            .code_block_width = cb_width,
            .code_block_height = cb_height,
            .resolutions = resolutions,
        };
    }

    return .{
        .bounds = .{
            .width = state.header.width,
            .height = state.header.height,
            .x0 = 0,
            .y0 = 0,
        },
        .components = components,
    };
}

pub fn enumerateComponentCodeblocks(allocator: std.mem.Allocator, component: *const ComponentGeometry) ![]CodeBlockCoordinate {
    const total = component.totalCodeblocks();
    const coords = try allocator.alloc(CodeBlockCoordinate, total);
    errdefer allocator.free(coords);

    var out_index: usize = 0;
    for (component.resolutions, 0..) |resolution, resolution_index| {
        for (resolution.subbands, 0..) |subband, subband_index| {
            var codeblock_index: u32 = 0;
            var y: u32 = 0;
            while (y < subband.codeblocks_y) : (y += 1) {
                var x: u32 = 0;
                while (x < subband.codeblocks_x) : (x += 1) {
                    const rect = codeblockRect(subband, x, y);
                    const precinct_info = precinctForCodeblock(
                        resolution,
                        subband,
                        x,
                        y,
                    );
                    coords[out_index] = .{
                        .component_index = component.component_index,
                        .resolution_index = @intCast(resolution_index),
                        .subband_index = @intCast(subband_index),
                        .subband = subband.band,
                        .precinct_index = precinct_info.index,
                        .codeblock_index = codeblock_index,
                        .codeblock_x = precinct_info.local_codeblock_x,
                        .codeblock_y = precinct_info.local_codeblock_y,
                        .rect = rect,
                    };
                    out_index += 1;
                    codeblock_index += 1;
                }
            }
        }
    }
    return coords;
}

pub fn codeblockRect(subband: SubbandGeometry, codeblock_x: u32, codeblock_y: u32) CodeBlockRect {
    const abs_x0 = (subband.grid_x0 + codeblock_x) * subband.code_block_width;
    const abs_y0 = (subband.grid_y0 + codeblock_y) * subband.code_block_height;
    const abs_x1 = @min(subband.origin_x + subband.bounds.width, abs_x0 + subband.code_block_width);
    const abs_y1 = @min(subband.origin_y + subband.bounds.height, abs_y0 + subband.code_block_height);
    const clipped_x0 = @max(abs_x0, subband.origin_x);
    const clipped_y0 = @max(abs_y0, subband.origin_y);
    return .{
        .x0 = clipped_x0 - subband.origin_x,
        .y0 = clipped_y0 - subband.origin_y,
        .x1 = abs_x1 - subband.origin_x,
        .y1 = abs_y1 - subband.origin_y,
    };
}

fn buildResolutionSubbands(
    allocator: std.mem.Allocator,
    bounds: ResBounds,
    origin_x: u32,
    origin_y: u32,
    code_block_width: u32,
    code_block_height: u32,
    precinct_width: u32,
    precinct_height: u32,
    precincts_present: bool,
    resolution_index: u8,
) ![]SubbandGeometry {
    if (resolution_index == 0) {
        const out = try allocator.alloc(SubbandGeometry, 1);
        out[0] = buildSubband(.ll, bounds, origin_x, origin_y, code_block_width, code_block_height, precinct_width, precinct_height, precincts_present, resolution_index);
        return out;
    }

    const low_width = lowPassSize(bounds.width, origin_x);
    const low_height = lowPassSize(bounds.height, origin_y);
    const high_width = bounds.width - low_width;
    const high_height = bounds.height - low_height;
    const out = try allocator.alloc(SubbandGeometry, 3);
    out[0] = buildSubband(.hl, .{ .width = high_width, .height = low_height, .x0 = 0, .y0 = 0 }, origin_x / 2, ceilDiv(origin_y, 2), code_block_width, code_block_height, precinct_width, precinct_height, precincts_present, resolution_index);
    out[1] = buildSubband(.lh, .{ .width = low_width, .height = high_height, .x0 = 0, .y0 = 0 }, ceilDiv(origin_x, 2), origin_y / 2, code_block_width, code_block_height, precinct_width, precinct_height, precincts_present, resolution_index);
    out[2] = buildSubband(.hh, .{ .width = high_width, .height = high_height, .x0 = 0, .y0 = 0 }, origin_x / 2, origin_y / 2, code_block_width, code_block_height, precinct_width, precinct_height, precincts_present, resolution_index);
    return out;
}

const PrecinctSize = struct {
    width: u32,
    height: u32,
};

fn precinctSizeForResolution(coding_style: codestream.CodingStyle, resolution_index: u8, bounds: ResBounds) PrecinctSize {
    _ = bounds;
    if (!coding_style.precincts_present or coding_style.precinct_sizes == null) {
        return .{
            .width = 1 << 15,
            .height = 1 << 15,
        };
    }
    const encoded = coding_style.precinct_sizes.?[resolution_index];
    const precinct_width = @as(u32, 1) << @intCast(encoded & 0x0f);
    const precinct_height = @as(u32, 1) << @intCast((encoded >> 4) & 0x0f);
    return .{
        .width = @max(precinct_width, @as(u32, 1)),
        .height = @max(precinct_height, @as(u32, 1)),
    };
}

const PrecinctAssignment = struct {
    index: u32,
    local_codeblock_x: u32,
    local_codeblock_y: u32,
};

fn precinctForCodeblock(
    resolution: ResolutionGeometry,
    subband: SubbandGeometry,
    codeblock_x: u32,
    codeblock_y: u32,
) PrecinctAssignment {
    const subband_precinct_width = if (resolution.level > 0 and subband.band != .ll) ceilDiv(resolution.precinct_width, 2) else resolution.precinct_width;
    const subband_precinct_height = if (resolution.level > 0 and subband.band != .ll) ceilDiv(resolution.precinct_height, 2) else resolution.precinct_height;
    const precincts_x = precinctCount(resolution.origin_x, resolution.bounds.width, @max(resolution.precinct_width, @as(u32, 1)));
    const precincts_y = precinctCount(resolution.origin_y, resolution.bounds.height, @max(resolution.precinct_height, @as(u32, 1)));
    const precinct_grid_x0 = resolution.origin_x / @max(resolution.precinct_width, @as(u32, 1));
    const precinct_grid_y0 = resolution.origin_y / @max(resolution.precinct_height, @as(u32, 1));
    const sb_x0 = subband.origin_x;
    const sb_y0 = subband.origin_y;
    const sb_x1 = subband.origin_x + subband.bounds.width;
    const sb_y1 = subband.origin_y + subband.bounds.height;

    var py: u32 = 0;
    while (py < precincts_y) : (py += 1) {
        var px: u32 = 0;
        while (px < precincts_x) : (px += 1) {
            const range = precinctCodeblockRange(
                px,
                py,
                precinct_grid_x0,
                precinct_grid_y0,
                @max(subband_precinct_width, @as(u32, 1)),
                @max(subband_precinct_height, @as(u32, 1)),
                sb_x0,
                sb_y0,
                sb_x1,
                sb_y1,
                subband.code_block_width,
                subband.code_block_height,
                subband.codeblocks_x,
                subband.codeblocks_y,
            );
            if (codeblock_x >= range.x0 and codeblock_x < range.x1 and
                codeblock_y >= range.y0 and codeblock_y < range.y1)
            {
                return .{
                    .index = py * precincts_x + px,
                    .local_codeblock_x = codeblock_x - range.x0,
                    .local_codeblock_y = codeblock_y - range.y0,
                };
            }
        }
    }
    return .{ .index = 0, .local_codeblock_x = codeblock_x, .local_codeblock_y = codeblock_y };
}

const CodeblockRange = struct {
    x0: u32,
    x1: u32,
    y0: u32,
    y1: u32,
};

fn precinctCodeblockRange(
    px: u32,
    py: u32,
    precinct_grid_x0: u32,
    precinct_grid_y0: u32,
    precinct_width: u32,
    precinct_height: u32,
    sb_x0: u32,
    sb_y0: u32,
    sb_x1: u32,
    sb_y1: u32,
    code_block_width: u32,
    code_block_height: u32,
    codeblocks_x: u32,
    codeblocks_y: u32,
) CodeblockRange {
    var prc_abs_x0 = (precinct_grid_x0 + px) * precinct_width;
    var prc_abs_y0 = (precinct_grid_y0 + py) * precinct_height;
    var prc_abs_x1 = prc_abs_x0 + precinct_width;
    var prc_abs_y1 = prc_abs_y0 + precinct_height;
    prc_abs_x0 = @max(prc_abs_x0, sb_x0);
    prc_abs_y0 = @max(prc_abs_y0, sb_y0);
    prc_abs_x1 = @min(prc_abs_x1, sb_x1);
    prc_abs_y1 = @min(prc_abs_y1, sb_y1);
    if (prc_abs_x0 >= prc_abs_x1 or prc_abs_y0 >= prc_abs_y1) return .{ .x0 = 0, .x1 = 0, .y0 = 0, .y1 = 0 };

    const grid_x0 = if (code_block_width == 0) 0 else sb_x0 / code_block_width;
    const grid_y0 = if (code_block_height == 0) 0 else sb_y0 / code_block_height;
    const cb_x0 = if (code_block_width == 0) 0 else prc_abs_x0 / code_block_width - grid_x0;
    const cb_y0 = if (code_block_height == 0) 0 else prc_abs_y0 / code_block_height - grid_y0;
    const cb_x1 = if (code_block_width == 0) 0 else ceilDiv(prc_abs_x1, code_block_width) - grid_x0;
    const cb_y1 = if (code_block_height == 0) 0 else ceilDiv(prc_abs_y1, code_block_height) - grid_y0;
    return .{
        .x0 = @min(cb_x0, codeblocks_x),
        .x1 = @min(cb_x1, codeblocks_x),
        .y0 = @min(cb_y0, codeblocks_y),
        .y1 = @min(cb_y1, codeblocks_y),
    };
}

fn buildSubband(
    kind: SubbandType,
    bounds: ResBounds,
    origin_x: u32,
    origin_y: u32,
    nominal_code_block_width: u32,
    nominal_code_block_height: u32,
    precinct_width: u32,
    precinct_height: u32,
    precincts_present: bool,
    resolution_index: u8,
) SubbandGeometry {
    var code_block_width = nominal_code_block_width;
    var code_block_height = nominal_code_block_height;
    if (precincts_present) {
        if (resolution_index == 0 or kind == .ll) {
            code_block_width = @min(code_block_width, @max(precinct_width, @as(u32, 1)));
            code_block_height = @min(code_block_height, @max(precinct_height, @as(u32, 1)));
        } else {
            code_block_width = @min(code_block_width, @max(ceilDiv(precinct_width, 2), @as(u32, 1)));
            code_block_height = @min(code_block_height, @max(ceilDiv(precinct_height, 2), @as(u32, 1)));
        }
    }
    const x1 = origin_x + bounds.width;
    const y1 = origin_y + bounds.height;
    const grid_x0 = if (code_block_width == 0) 0 else origin_x / code_block_width;
    const grid_y0 = if (code_block_height == 0) 0 else origin_y / code_block_height;
    return .{
        .band = kind,
        .bounds = bounds,
        .origin_x = origin_x,
        .origin_y = origin_y,
        .grid_x0 = grid_x0,
        .grid_y0 = grid_y0,
        .code_block_width = code_block_width,
        .code_block_height = code_block_height,
        .codeblocks_x = if (bounds.width == 0) 0 else ceilDiv(x1, code_block_width) - grid_x0,
        .codeblocks_y = if (bounds.height == 0) 0 else ceilDiv(y1, code_block_height) - grid_y0,
    };
}

fn subbandOriginForResolution(resolution: ResolutionGeometry, band: SubbandType) struct { x: u32, y: u32 } {
    return switch (band) {
        .ll => .{ .x = resolution.origin_x, .y = resolution.origin_y },
        .hl => .{ .x = resolution.origin_x / 2, .y = ceilDiv(resolution.origin_y, 2) },
        .lh => .{ .x = ceilDiv(resolution.origin_x, 2), .y = resolution.origin_y / 2 },
        .hh => .{ .x = resolution.origin_x / 2, .y = resolution.origin_y / 2 },
    };
}

fn precinctCount(origin: u32, width: u32, precinct_width: u32) u32 {
    if (width == 0) return 0;
    const safe_width = @max(precinct_width, @as(u32, 1));
    const count = ceilDiv(origin + width, safe_width) - origin / safe_width;
    return @max(count, @as(u32, 1));
}

fn ceilDiv(value: u32, denom: u32) u32 {
    return if (value == 0) 0 else (value + denom - 1) / denom;
}

fn producedByAntflyEncoder(comments: []const codestream.Comment) bool {
    for (comments) |comment| {
        if (std.mem.startsWith(u8, comment.text, "antfly-zig j2k v1")) return true;
    }
    return false;
}

const GridSpan = struct {
    origin: u32,
    width: u32,
};

fn componentSpan(origin: u32, width: u32, sampling: u8) GridSpan {
    const step: u32 = if (sampling == 0) 1 else @intCast(sampling);
    const x0 = ceilDiv(origin, step);
    const x1 = ceilDiv(origin + width, step);
    return .{ .origin = x0, .width = x1 - x0 };
}

fn resolutionSpan(origin: u32, width: u32, reduce: u8) GridSpan {
    if (reduce == 0) return .{ .origin = origin, .width = width };
    const step: u32 = @as(u32, 1) << @intCast(reduce);
    const x0 = ceilDiv(origin, step);
    const x1 = ceilDiv(origin + width, step);
    return .{ .origin = x0, .width = x1 - x0 };
}

fn lowPassSize(width: u32, origin: u32) u32 {
    return ceilDiv(origin + width, 2) - ceilDiv(origin, 2);
}

/// Per-component tile dimensions, accounting for SIZ XRsiz/YRsiz subsampling.
/// For a tile of size (tile_w, tile_h) on the reference grid, a component with
/// sampling factors (xrsiz, yrsiz) has dimensions
/// (ceil(tile_w / xrsiz), ceil(tile_h / yrsiz)).
pub fn componentDimensions(tile_w: u32, tile_h: u32, xrsiz: u8, yrsiz: u8) struct { width: u32, height: u32 } {
    const dims = componentDimensionsAt(0, 0, tile_w, tile_h, xrsiz, yrsiz);
    return .{ .width = dims.width, .height = dims.height };
}

pub fn componentDimensionsAt(x0: u32, y0: u32, tile_w: u32, tile_h: u32, xrsiz: u8, yrsiz: u8) struct { width: u32, height: u32, origin_x: u32, origin_y: u32 } {
    const x_span = componentSpan(x0, tile_w, xrsiz);
    const y_span = componentSpan(y0, tile_h, yrsiz);
    return .{
        .width = x_span.width,
        .height = y_span.width,
        .origin_x = x_span.origin,
        .origin_y = y_span.origin,
    };
}

fn ceilDivPow2(value: u32, shift: u8) u32 {
    if (shift == 0) return value;
    return (value + (@as(u32, 1) << @intCast(shift)) - 1) >> @intCast(shift);
}

test "single tile geometry builds one LL resolution for zero decomposition" {
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

    var geometry = try buildSingleTileGeometry(allocator, &state);
    defer geometry.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), geometry.components.len);
    try std.testing.expectEqual(@as(usize, 1), geometry.components[0].resolutions.len);
    try std.testing.expectEqual(@as(usize, 1), geometry.components[0].resolutions[0].subbands.len);
    try std.testing.expectEqual(.ll, geometry.components[0].resolutions[0].subbands[0].band);
    try std.testing.expectEqual(@as(u32, 3), geometry.totalCodeblocks());
}

test "component codeblock enumeration reflects decomposition structure" {
    const allocator = std.testing.allocator;
    const component = ComponentGeometry{
        .component_index = 0,
        .bounds = .{ .width = 8, .height = 8, .x0 = 0, .y0 = 0 },
        .decomposition_levels = 1,
        .code_block_width = 4,
        .code_block_height = 4,
        .resolutions = try allocator.dupe(ResolutionGeometry, &.{
            .{ .level = 0, .bounds = .{ .width = 4, .height = 4, .x0 = 0, .y0 = 0 }, .precinct_width = 4, .precinct_height = 4, .subbands = try allocator.dupe(SubbandGeometry, &.{.{ .band = .ll, .bounds = .{ .width = 4, .height = 4, .x0 = 0, .y0 = 0 }, .codeblocks_x = 1, .codeblocks_y = 1 }}) },
            .{ .level = 1, .bounds = .{ .width = 8, .height = 8, .x0 = 0, .y0 = 0 }, .precinct_width = 8, .precinct_height = 8, .subbands = try allocator.dupe(SubbandGeometry, &.{
                .{ .band = .hl, .bounds = .{ .width = 4, .height = 4, .x0 = 0, .y0 = 0 }, .codeblocks_x = 1, .codeblocks_y = 1 },
                .{ .band = .lh, .bounds = .{ .width = 4, .height = 4, .x0 = 0, .y0 = 0 }, .codeblocks_x = 1, .codeblocks_y = 1 },
                .{ .band = .hh, .bounds = .{ .width = 4, .height = 4, .x0 = 0, .y0 = 0 }, .codeblocks_x = 1, .codeblocks_y = 1 },
            }) },
        }),
    };
    var mutable_component = component;
    defer mutable_component.deinit(allocator);

    const coords = try enumerateComponentCodeblocks(allocator, &mutable_component);
    defer allocator.free(coords);
    try std.testing.expectEqual(@as(usize, 4), coords.len);
    try std.testing.expectEqual(.ll, coords[0].subband);
    try std.testing.expectEqual(.hl, coords[1].subband);
    try std.testing.expectEqual(.lh, coords[2].subband);
    try std.testing.expectEqual(.hh, coords[3].subband);
}

test "one-level odd-width geometry uses narrow high bands" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 1);
    defer allocator.free(components);
    components[0] = .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 };

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
        .quantization_style = null,
        .comments = &.{},
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };

    var geometry = try buildSingleTileGeometry(allocator, &state);
    defer geometry.deinit(allocator);

    const detail = geometry.components[0].resolutions[1].subbands;
    try std.testing.expectEqual(@as(usize, 3), detail.len);
    try std.testing.expectEqual(.hl, detail[0].band);
    try std.testing.expectEqual(@as(u32, 1), detail[0].bounds.width);
    try std.testing.expectEqual(@as(u32, 1), detail[0].bounds.height);
    try std.testing.expectEqual(.lh, detail[1].band);
    try std.testing.expectEqual(@as(u32, 2), detail[1].bounds.width);
    try std.testing.expectEqual(@as(u32, 1), detail[1].bounds.height);
    try std.testing.expectEqual(.hh, detail[2].band);
    try std.testing.expectEqual(@as(u32, 1), detail[2].bounds.width);
    try std.testing.expectEqual(@as(u32, 1), detail[2].bounds.height);
}

test "tile grid 2x2 on 8x8 image with 4x4 tiles" {
    const allocator = std.testing.allocator;
    const components = try allocator.alloc(codestream.Component, 1);
    defer allocator.free(components);
    components[0] = .{ .bits_per_component = 8, .is_signed = false, .xrsiz = 1, .yrsiz = 1 };

    const state = codestream.State{
        .header = .{
            .width = 8,
            .height = 8,
            .components = components,
            .tile_width = 4,
            .tile_height = 4,
            .uses_multiple_tiles = true,
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

    const grid = buildTileGrid(&state);
    try std.testing.expectEqual(@as(u32, 2), grid.cols);
    try std.testing.expectEqual(@as(u32, 2), grid.rows);
    try std.testing.expectEqual(@as(u32, 4), grid.tileCount());

    const b0 = grid.tileBounds(0);
    try std.testing.expectEqual(@as(u32, 0), b0.x0);
    try std.testing.expectEqual(@as(u32, 0), b0.y0);
    try std.testing.expectEqual(@as(u32, 4), b0.width);
    try std.testing.expectEqual(@as(u32, 4), b0.height);

    const b1 = grid.tileBounds(1);
    try std.testing.expectEqual(@as(u32, 4), b1.x0);
    try std.testing.expectEqual(@as(u32, 0), b1.y0);
    try std.testing.expectEqual(@as(u32, 4), b1.width);
    try std.testing.expectEqual(@as(u32, 4), b1.height);

    const b2 = grid.tileBounds(2);
    try std.testing.expectEqual(@as(u32, 0), b2.x0);
    try std.testing.expectEqual(@as(u32, 4), b2.y0);
    try std.testing.expectEqual(@as(u32, 4), b2.width);
    try std.testing.expectEqual(@as(u32, 4), b2.height);

    const b3 = grid.tileBounds(3);
    try std.testing.expectEqual(@as(u32, 4), b3.x0);
    try std.testing.expectEqual(@as(u32, 4), b3.y0);
    try std.testing.expectEqual(@as(u32, 4), b3.width);
    try std.testing.expectEqual(@as(u32, 4), b3.height);
}
