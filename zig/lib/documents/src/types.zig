// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Transport-independent document contracts. Definitions contain no executable
//! expressions or credentials. Every allocation belongs to the caller's arena.
const std = @import("std");

pub const Status = enum { complete, unresolved, truncated };
pub const Action = enum { accept, accurate, table, fallback, unresolved };
pub const Origin = enum { native, transcription, interpretation, derived, estimated };
pub const Transform = struct {
    matrix: [9]f64 = .{ 1, 0, 0, 0, 1, 0, 0, 0, 1 },
    pub fn point(self: Transform, p: [2]f64) ![2]f64 {
        const m = self.matrix;
        const w = m[6] * p[0] + m[7] * p[1] + m[8];
        if (!std.math.isFinite(w) or @abs(w) < 1e-12) return error.InvalidDocumentTransform;
        const result = [2]f64{ (m[0] * p[0] + m[1] * p[1] + m[2]) / w, (m[3] * p[0] + m[4] * p[1] + m[5]) / w };
        for (result) |v| if (!std.math.isFinite(v)) return error.InvalidDocumentTransform;
        return result;
    }
    pub fn inverse(self: Transform) !Transform {
        const m = self.matrix;
        const cofactors = [9]f64{
            m[4] * m[8] - m[5] * m[7], m[2] * m[7] - m[1] * m[8], m[1] * m[5] - m[2] * m[4],
            m[5] * m[6] - m[3] * m[8], m[0] * m[8] - m[2] * m[6], m[2] * m[3] - m[0] * m[5],
            m[3] * m[7] - m[4] * m[6], m[1] * m[6] - m[0] * m[7], m[0] * m[4] - m[1] * m[3],
        };
        const determinant = m[0] * cofactors[0] + m[1] * cofactors[3] + m[2] * cofactors[6];
        if (!std.math.isFinite(determinant) or @abs(determinant) < 1e-12) return error.InvalidDocumentTransform;
        var result: Transform = .{};
        for (&result.matrix, cofactors) |*v, cofactor| v.* = cofactor / determinant;
        return result;
    }
    pub fn box(self: Transform, bbox: [4]f64) ![4]f64 {
        for (bbox) |v| if (!std.math.isFinite(v)) return error.InvalidDocumentRegion;
        if (bbox[0] >= bbox[2] or bbox[1] >= bbox[3]) return error.InvalidDocumentRegion;
        var result: [4]f64 = .{ std.math.inf(f64), std.math.inf(f64), -std.math.inf(f64), -std.math.inf(f64) };
        for ([_][2]f64{ .{ bbox[0], bbox[1] }, .{ bbox[2], bbox[1] }, .{ bbox[0], bbox[3] }, .{ bbox[2], bbox[3] } }) |corner| {
            const p = try self.point(corner);
            result[0] = @min(result[0], p[0]);
            result[1] = @min(result[1], p[1]);
            result[2] = @max(result[2], p[0]);
            result[3] = @max(result[3], p[1]);
        }
        return result;
    }
};
pub const Region = struct {
    id: usize,
    text: []const u8,
    bbox: ?[4]f64 = null,
    label: ?[]const u8 = null,
    confidence: ?f64 = null,
    origin: Origin = .transcription,
    model: ?[]const u8 = null,
    validated: bool = false,
    /// UTF-8 byte range in Page.text, shared by nested regions.
    text_start: ?usize = null,
    text_end: ?usize = null,
    children: []const Region = &.{},
    tables: []const Table = &.{},
    fields: ?std.json.Value = null,
};
pub const Cell = struct {
    row: usize,
    column: usize,
    row_span: usize = 1,
    column_span: usize = 1,
    text: []const u8,
    bbox: ?[4]f64 = null,
};
pub const Table = struct {
    id: usize,
    region_id: ?usize = null,
    /// A derived flat view of the same table in the referenced region's table
    /// array. The ID and content must identify a matching authoritative view.
    projection: bool = false,
    rows: usize,
    columns: usize,
    cells: []const Cell,
    origin: Origin = .native,

    /// Shared semantic boundary for native, model and retained table views.
    /// Subtraction after bounds checks keeps spans safe at the usize ceiling.
    pub fn validate(self: Table) error{InvalidDocumentModelResult}!void {
        if (self.rows == 0 or self.columns == 0) return error.InvalidDocumentModelResult;
        for (self.cells) |cell| {
            if (cell.row >= self.rows or cell.column >= self.columns or cell.row_span == 0 or cell.column_span == 0 or
                cell.row_span > self.rows - cell.row or cell.column_span > self.columns - cell.column) return error.InvalidDocumentModelResult;
        }
    }
};
pub const InheritedPart = struct {
    path: []const u8,
    role: enum { layout, master },
    text: []const u8,
    tables: []const Table = &.{},
    shapes_visible: bool,
    background_visible: bool,
    shape_visual: bool,
    background_visual: bool,
    conditional_content: bool = false,
};
pub const Page = struct {
    object: []const u8 = "read",
    index: usize = 0,
    source_id: []const u8,
    number: usize,
    kind: enum { page, slide, image },
    native_text: []const u8 = "",
    inherited_parts: []const InheritedPart = &.{},
    notes: []const u8 = "",
    text: []const u8 = "",
    regions: []const Region = &.{},
    tables: []const Table = &.{},
    fields: ?std.json.Value = null,
    status: Status = .unresolved,
    origin: Origin = .native,
    coordinate_space: enum { pdf_points, slide_emu, image_pixels } = .image_pixels,
    transform: Transform = .{},
    raster_to_source: ?Transform = null,
    findings: []const []const u8 = &.{},
    model: ?[]const u8 = null,
};
pub const Event = struct {
    phase: enum { source, native_text, reading, decision, verification, extracting, accepted, unresolved, complete },
    source_id: []const u8,
    page_number: usize,
    model: ?[]const u8 = null,
    region_id: ?usize = null,
    action: ?Action = null,
    status: ?Status = null,
    finding: ?[]const u8 = null,
};
pub const Control = struct {
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    event_fn: ?*const fn (?*anyopaque, Event) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |f| try f(self.ptr);
    }
    pub fn emit(self: Control, event: Event) !void {
        try self.check();
        if (self.event_fn) |f| try f(self.ptr, event);
    }
};

/// Limits describe format work only; the owner supplies cumulative accounting.
pub const RenderLimits = struct {
    max_source_bytes: usize = 64 * 1024 * 1024,
    max_output_bytes: usize = 16 * 1024 * 1024,
    max_regions: usize = 4096,
    max_pixels: u64 = 40_000_000,
    max_dimension: u32 = 16384,
    max_total_pixels: u64 = 400_000_000,
};
pub const RenderContext = struct {
    limits: RenderLimits = .{},
    control: Control = .{},
    work_allocator: ?std.mem.Allocator = null,
    rendered_pixels: u64 = 0,
    accounting: ?struct { ptr: *anyopaque, pixels_fn: *const fn (*anyopaque, u64) anyerror!void } = null,
    pub fn pixels(self: *RenderContext, count: u64) !void {
        try self.control.check();
        if (count > self.limits.max_pixels or count > self.limits.max_total_pixels -| self.rendered_pixels) return error.DocumentPixelLimitExceeded;
        if (self.accounting) |owner| try owner.pixels_fn(owner.ptr, count);
        self.rendered_pixels += count;
    }
};
