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

//! Bounded native charts use the document's cached data. Formula strings and
//! workbook relationships are never evaluated or fetched.
const std = @import("std");
const xml = @import("ooxml.zig");
const themes = @import("drawingml_theme.zig");
const c = @import("types.zig");
const A = std.mem.Allocator;
pub const Series = struct { order: ?usize = null, index: ?usize = null, label_indices: std.AutoHashMapUnmanaged(usize, void) = .empty, name: []const u8 = "", values: std.ArrayList(?f64) = .empty, labels: std.ArrayList([]const u8) = .empty, color: ?themes.Color = null };
pub const Chart = struct {
    kind: enum { bar, line, pie },
    horizontal: bool = false,
    title: []const u8 = "",
    legend: bool = false,
    labels: bool = false,
    minimum: ?f64 = null,
    maximum: ?f64 = null,
    gridlines: bool = false,
    vary_colors: bool = true,
    series: []const Series,
};
pub const Painter = struct {
    ptr: *anyopaque,
    rect: *const fn (*anyopaque, [4]f64, themes.Color) anyerror!void,
    line: *const fn (*anyopaque, []const [2]f64, themes.Color) anyerror!void,
    polygon: *const fn (*anyopaque, []const [2]f64, themes.Color) anyerror!void,
    label: *const fn (*anyopaque, [4]f64, []const u8, f64, bool) anyerror!void,
};
fn eq(left: []const u8, right: []const u8) bool {
    return std.mem.eql(u8, left, right);
}
fn integer(a: A, tag: xml.Tag, name: []const u8) !usize {
    return std.fmt.parseInt(usize, try xml.decodedText(a, (try tag.uniqueAttr(name)) orelse return error.InvalidDocumentXml), 10) catch return error.InvalidDocumentXml;
}
pub fn parse(a: A, bytes: []const u8, theme: themes.Theme, run: *c.RenderContext) !Chart {
    var iter = xml.QualifiedXml.init(a, bytes);
    defer iter.deinit();
    var kind: ?@FieldType(Chart, "kind") = null;
    var plot_depth: ?usize = null;
    var series = std.ArrayList(Series).empty;
    var active: ?usize = null;
    var series_depth: usize = 0;
    var scope: ?enum { category, value, name } = null;
    var scope_depth: usize = 0;
    var point: ?usize = null;
    var point_depth: usize = 0;
    var format_depth: ?usize = null;
    var value_depth: ?usize = null;
    var title_depth: ?usize = null;
    var skip: ?usize = null;
    var text = std.ArrayList(u8).empty;
    var title = std.ArrayList(u8).empty;
    var result = Chart{ .kind = .bar, .series = &.{} };
    var points: usize = 0;
    while (try iter.next()) |token| {
        try run.control.check();
        if (token == .text) {
            if (skip != null) continue;
            if (value_depth != null or format_depth != null) try text.appendSlice(a, try xml.decodedText(a, token.text));
            continue;
        }
        const item = token.tag;
        const tag = item.value;
        const name = tag.local();
        if (skip) |depth| {
            if (tag.closing and item.depth == depth) skip = null;
            continue;
        }
        if (tag.closing) {
            if (format_depth == item.depth) {
                if (!eq(std.mem.trim(u8, text.items, " \r\n\t"), "General")) return error.DocumentRendererUnavailable;
                text.clearRetainingCapacity();
                format_depth = null;
            }
            if (point != null and point_depth == item.depth) point = null;
            if (value_depth == item.depth) {
                const value = try text.toOwnedSlice(a);
                if (title_depth != null and active == null) {
                    try title.appendSlice(a, value);
                } else if (active) |index| {
                    const target = &series.items[index];
                    if (scope == .name) target.name = value else if (point) |number| {
                        if (scope == .value) {
                            const scalar = std.fmt.parseFloat(f64, value) catch return error.InvalidDocumentXml;
                            if (!std.math.isFinite(scalar) or @abs(scalar) > 1e15) return error.DocumentRendererUnavailable;
                            if (target.values.items[number] != null) return error.InvalidDocumentXml;
                            target.values.items[number] = scalar;
                        } else if (scope == .category) target.labels.items[number] = value;
                    }
                }
                value_depth = null;
            }
            if (title_depth == item.depth) title_depth = null;
            if (scope != null and scope_depth == item.depth) scope = null;
            if (active != null and series_depth == item.depth) active = null;
            continue;
        }
        if (item.depth == 1 and (item.namespace != .chart or !eq(name, "chartSpace"))) return error.InvalidDocumentXml;
        if (item.namespace == .chart and eq(name, "extLst")) {
            if (!tag.empty) skip = item.depth;
            continue;
        }
        if (item.namespace != .chart and item.namespace != .drawing) return error.DocumentRendererUnavailable;
        if (item.namespace == .drawing) {
            // Explicit unsupported typography and shape effects must not
            // silently turn into plausible, differently styled chart data.
            var supported = false;
            for ([_][]const u8{ "rich", "bodyPr", "lstStyle", "p", "pPr", "endParaRPr", "defRPr", "r", "rPr", "t", "solidFill" }) |allowed| {
                if (eq(name, allowed)) supported = true;
            }
            if (!supported) return error.DocumentRendererUnavailable;
            if (!eq(name, "t") and !eq(name, "solidFill") and try tag.hasPresentationAttributes()) return error.DocumentRendererUnavailable;
            if (eq(name, "t") and title_depth != null) value_depth = item.depth;
            if (eq(name, "solidFill")) {
                if (active == null or item.depth != series_depth + 2) return error.DocumentRendererUnavailable;
                var next = (try iter.next()) orelse return error.InvalidDocumentXml;
                while (next == .text and std.mem.trim(u8, next.text, " \r\n\t").len == 0) next = (try iter.next()) orelse return error.InvalidDocumentXml;
                if (next != .tag or next.tag.namespace != .drawing) return error.InvalidDocumentXml;
                series.items[active.?].color = try theme.readColor(a, &iter, next.tag.value, run.control);
            }
            if (eq(name, "gradFill") or (eq(name, "effectLst") and !tag.empty) or eq(name, "scene3d")) return error.DocumentRendererUnavailable;
            continue;
        }
        var supported = false;
        for ([_][]const u8{ "chartSpace", "chart", "plotArea", "barChart", "lineChart", "pieChart", "autoTitleDeleted", "ser", "idx", "order", "tx", "cat", "val", "strRef", "numRef", "strCache", "numCache", "strLit", "numLit", "f", "ptCount", "pt", "v", "formatCode", "varyColors", "title", "rich", "legend", "legendPos", "barDir", "grouping", "showVal", "autoUpdate", "smooth", "overlay", "showPercent", "showCatName", "showSerName", "numFmt", "min", "max", "majorGridlines", "delete", "layout", "catAx", "valAx", "scaling", "axId", "crossAx", "spPr", "dLbls", "lang", "date1904", "roundedCorners", "externalData", "plotVisOnly", "showDLblsOverMax" }) |allowed| {
            if (eq(name, allowed)) supported = true;
        }
        if (!supported) return error.DocumentRendererUnavailable;
        if (eq(name, "formatCode")) {
            if (tag.empty) return error.DocumentRendererUnavailable;
            format_depth = item.depth;
        }
        if (eq(name, "roundedCorners") or eq(name, "showDLblsOverMax")) {
            if (try tag.booleanAttr("val", true)) return error.DocumentRendererUnavailable;
        }
        if (eq(name, "spPr") and (active == null or item.depth != series_depth + 1) and !tag.empty) return error.DocumentRendererUnavailable;
        if ((eq(name, "idx") or eq(name, "order")) and active != null) {
            const target = &series.items[active.?];
            const slot = if (eq(name, "idx")) &target.index else &target.order;
            if (slot.* != null) return error.InvalidDocumentXml;
            slot.* = try integer(a, tag, "val");
        }
        if (eq(name, "barChart") or eq(name, "lineChart") or eq(name, "pieChart")) {
            if (kind != null) return error.DocumentRendererUnavailable;
            kind = if (eq(name, "barChart")) .bar else if (eq(name, "lineChart")) .line else .pie;
            plot_depth = item.depth;
        } else if (std.mem.endsWith(u8, name, "Chart") and !eq(name, "chart")) return error.DocumentRendererUnavailable;
        if (eq(name, "ser")) {
            if (plot_depth == null or item.depth != plot_depth.? + 1 or active != null or series.items.len >= run.limits.max_regions) return error.DocumentRendererUnavailable;
            active = series.items.len;
            series_depth = item.depth;
            try series.append(a, .{});
        } else if (active != null and item.depth == series_depth + 1 and (eq(name, "cat") or eq(name, "val") or eq(name, "tx"))) {
            scope = if (eq(name, "cat")) .category else if (eq(name, "val")) .value else .name;
            scope_depth = item.depth;
        } else if (eq(name, "pt") and active != null and scope != null) {
            const index = try integer(a, tag, "idx");
            if (index >= run.limits.max_regions or points >= run.limits.max_regions) return error.DocumentRegionLimitExceeded;
            points += 1;
            if (point != null) return error.InvalidDocumentXml;
            point = index;
            point_depth = item.depth;
            const target = &series.items[active.?];
            if (scope == .value and index >= target.values.items.len) {
                const before = target.values.items.len;
                try target.values.resize(a, index + 1);
                @memset(target.values.items[before..], null);
            }
            if (scope == .category) {
                const seen = try target.label_indices.getOrPut(a, index);
                if (seen.found_existing) return error.InvalidDocumentXml;
            }
            if (scope == .category and index >= target.labels.items.len) {
                const before = target.labels.items.len;
                try target.labels.resize(a, index + 1);
                @memset(target.labels.items[before..], "");
            }
        } else if (eq(name, "v") and (scope != null or title_depth != null)) {
            if ((scope == .category or scope == .value) and point == null) return error.InvalidDocumentXml;
            value_depth = item.depth;
        } else if (eq(name, "varyColors")) result.vary_colors = try tag.booleanAttr("val", true) else if (eq(name, "title")) title_depth = item.depth else if (eq(name, "legend")) result.legend = true else if (eq(name, "legendPos")) {
            if (!eq((try tag.uniqueAttr("val")) orelse "r", "r")) return error.DocumentRendererUnavailable;
        } else if (eq(name, "barDir")) {
            const raw = (try tag.uniqueAttr("val")) orelse return error.InvalidDocumentXml;
            result.horizontal = eq(raw, "bar");
            if (!result.horizontal and !eq(raw, "col")) return error.DocumentRendererUnavailable;
        } else if (eq(name, "grouping")) {
            const raw = (try tag.uniqueAttr("val")) orelse return error.InvalidDocumentXml;
            if (!eq(raw, "clustered") and !eq(raw, "standard")) return error.DocumentRendererUnavailable;
        } else if (eq(name, "showVal")) result.labels = try tag.booleanAttr("val", true) else if (eq(name, "autoUpdate") or eq(name, "smooth") or eq(name, "overlay") or eq(name, "showPercent") or eq(name, "showCatName") or eq(name, "showSerName")) {
            if (try tag.booleanAttr("val", true)) return error.DocumentRendererUnavailable;
        } else if (eq(name, "numFmt")) {
            if (!eq((try tag.uniqueAttr("formatCode")) orelse "General", "General")) return error.DocumentRendererUnavailable;
        } else if (eq(name, "min") or eq(name, "max")) {
            const value = std.fmt.parseFloat(f64, try xml.decodedText(a, (try tag.uniqueAttr("val")) orelse return error.InvalidDocumentXml)) catch return error.InvalidDocumentXml;
            if (!std.math.isFinite(value) or @abs(value) > 1e15) return error.DocumentRendererUnavailable;
            if (eq(name, "min")) result.minimum = value else result.maximum = value;
        } else if (eq(name, "majorGridlines")) result.gridlines = true else if (eq(name, "delete")) {
            if (try tag.booleanAttr("val", true)) return error.DocumentRendererUnavailable;
        } else if (eq(name, "dPt") or (eq(name, "layout") and !tag.empty) or eq(name, "logBase") or eq(name, "marker")) return error.DocumentRendererUnavailable;
    }
    result.kind = kind orelse return error.DocumentRendererUnavailable;
    if (series.items.len == 0 or (result.kind == .pie and series.items.len != 1)) return error.DocumentRendererUnavailable;
    for (series.items, 0..) |item, i| {
        if (item.values.items.len == 0 or item.order == null or item.index == null) return error.DocumentRendererUnavailable;
        for (series.items[0..i]) |other| if (other.order.? == item.order.? or other.index.? == item.index.?) return error.InvalidDocumentXml;
    }
    std.mem.sort(Series, series.items, {}, struct {
        fn less(_: void, left: Series, right: Series) bool {
            return left.order.? < right.order.?;
        }
    }.less);
    result.series = try series.toOwnedSlice(a);
    result.title = try title.toOwnedSlice(a);
    return result;
}

pub fn paint(a: A, chart: Chart, width: f64, height: f64, theme: themes.Theme, painter: Painter, run: *c.RenderContext) !void {
    try painter.rect(painter.ptr, .{ 0, 0, width, height }, .{ .rgb = .{ 255, 255, 255 } });
    const left = @min(36, width * 0.15);
    const top: f64 = if (chart.title.len > 0) 24 else 10;
    const right: f64 = if (chart.legend) @min(90, width * 0.3) else 10;
    const bottom = @min(30, height * 0.2);
    const pw = width - left - right;
    const ph = height - top - bottom;
    if (pw <= 10 or ph <= 10) return error.DocumentRendererUnavailable;
    if (chart.title.len > 0) try painter.label(painter.ptr, .{ left, 0, pw, 22 }, chart.title, 14, true);
    const colors = try a.alloc(themes.Color, chart.series.len);
    const accents = [_]themes.Role{ .accent1, .accent2, .accent3, .accent4, .accent5, .accent6 };
    for (chart.series, 0..) |series, index| {
        colors[index] = series.color orelse try theme.resolve(@tagName(accents[index % 6]));
        if (chart.legend and chart.kind != .pie) {
            const y = top + @as(f64, @floatFromInt(index)) * 16;
            try painter.rect(painter.ptr, .{ width - right + 3, y + 3, 8, 8 }, colors[index]);
            try painter.label(painter.ptr, .{ width - right + 15, y, right - 15, 16 }, series.name, 10, false);
        }
    }
    if (chart.kind == .pie) {
        const series = chart.series[0];
        var sum: f64 = 0;
        for (series.values.items) |v| if (v) |number| {
            if (number < 0) return error.DocumentRendererUnavailable;
            sum += number;
        };
        if (sum <= 0) return error.DocumentRendererUnavailable;
        const radius = @min(pw, ph) / 2;
        const center = [2]f64{ left + pw / 2, top + ph / 2 };
        var angle: f64 = -std.math.pi / 2.0;
        for (series.values.items, 0..) |v, index| {
            try run.control.check();
            const value = v orelse continue;
            const span = value / sum * 2 * std.math.pi;
            const steps: usize = @intFromFloat(@ceil(std.math.clamp(span * radius / 2, 2, 2048)));
            const points = try a.alloc([2]f64, steps + 2);
            points[0] = center;
            for (points[1..], 0..) |*point, i| {
                const theta = angle + span * @as(f64, @floatFromInt(i)) / @as(f64, @floatFromInt(steps));
                point.* = .{ center[0] + radius * @cos(theta), center[1] + radius * @sin(theta) };
            }
            const color = if (chart.vary_colors) try theme.resolve(@tagName(accents[index % 6])) else colors[0];
            try painter.polygon(painter.ptr, points, color);
            if (chart.legend) {
                const y = top + @as(f64, @floatFromInt(index)) * 16;
                try painter.rect(painter.ptr, .{ width - right + 3, y + 3, 8, 8 }, color);
                if (index < series.labels.items.len) try painter.label(painter.ptr, .{ width - right + 15, y, right - 15, 16 }, series.labels.items[index], 10, false);
            }
            if (chart.labels) {
                const middle = angle + span / 2;
                try painter.label(painter.ptr, .{ center[0] + radius * 0.65 * @cos(middle) - 15, center[1] + radius * 0.65 * @sin(middle) - 6, 30, 12 }, try std.fmt.allocPrint(a, "{d}", .{value}), 10, false);
            }
            angle += span;
        }
        return;
    }
    var low: f64 = 0;
    var high: f64 = 0;
    var categories: usize = 0;
    for (chart.series) |series| {
        categories = @max(categories, series.values.items.len);
        for (series.values.items) |v| if (v) |number| {
            low = @min(low, number);
            high = @max(high, number);
        };
    }
    if (high == low) high = low + 1;
    low = chart.minimum orelse low;
    high = chart.maximum orelse high;
    if (high <= low) return error.DocumentRendererUnavailable;
    for (chart.series) |series| for (series.values.items) |v| if (v) |value| {
        if (value < low or value > high) return error.DocumentRendererUnavailable;
    };
    const range = high - low;
    const axis = themes.Color{ .rgb = .{ 96, 96, 96 } };
    for (0..5) |tick| {
        const fraction = @as(f64, @floatFromInt(tick)) / 4;
        const scalar = if (chart.horizontal) low + range * fraction else high - range * fraction;
        const label = try std.fmt.allocPrint(a, "{d}", .{scalar});
        if (chart.horizontal) {
            const x = left + pw * fraction;
            try painter.label(painter.ptr, .{ x - 10, top + ph + 3, 30, bottom }, label, 8, false);
            if (chart.gridlines) try painter.line(painter.ptr, &.{ .{ x, top }, .{ x, top + ph } }, .{ .rgb = .{ 220, 220, 220 } });
        } else {
            const y = top + ph * fraction;
            try painter.label(painter.ptr, .{ 0, y - 5, left - 3, 12 }, label, 8, false);
            if (chart.gridlines) try painter.line(painter.ptr, &.{ .{ left, y }, .{ left + pw, y } }, .{ .rgb = .{ 220, 220, 220 } });
        }
    }
    try painter.line(painter.ptr, &.{ .{ left, top }, .{ left, top + ph }, .{ left + pw, top + ph } }, axis);
    const category_width = (if (chart.horizontal) ph else pw) / @as(f64, @floatFromInt(categories));
    for (chart.series, 0..) |series, s| {
        var previous: ?[2]f64 = null;
        for (series.values.items, 0..) |v, index| {
            try run.control.check();
            const value = v orelse {
                previous = null;
                continue;
            };
            const x = left + (@as(f64, @floatFromInt(index)) + 0.5) * category_width;
            const y = top + (high - value) / range * ph;
            const point_color = if (chart.vary_colors and chart.kind == .bar and chart.series.len == 1) try theme.resolve(@tagName(accents[index % 6])) else colors[s];
            if (chart.kind == .line) {
                if (previous) |p| try painter.line(painter.ptr, &.{ p, .{ x, y } }, colors[s]);
                previous = .{ x, y };
            } else if (chart.horizontal) {
                const zero = left + (std.math.clamp(0, low, high) - low) / range * pw;
                const end = left + (value - low) / range * pw;
                const bar = category_width * 0.7 / @as(f64, @floatFromInt(chart.series.len));
                try painter.rect(painter.ptr, .{ @min(zero, end), top + @as(f64, @floatFromInt(index)) * category_width + category_width * 0.15 + @as(f64, @floatFromInt(s)) * bar, @abs(end - zero), bar }, point_color);
            } else {
                const bar = category_width * 0.7 / @as(f64, @floatFromInt(chart.series.len));
                const zero = top + (high - std.math.clamp(0, low, high)) / range * ph;
                try painter.rect(painter.ptr, .{ left + @as(f64, @floatFromInt(index)) * category_width + category_width * 0.15 + @as(f64, @floatFromInt(s)) * bar, @min(zero, y), bar, @abs(zero - y) }, point_color);
            }
            if (chart.labels) {
                const box: [4]f64 = if (chart.horizontal)
                    .{ left + (value - low) / range * pw - 25, top + (@as(f64, @floatFromInt(index)) + 0.5) * category_width - 7, 50, 14 }
                else
                    .{ x - 25, y - 14, 50, 14 };
                try painter.label(painter.ptr, box, try std.fmt.allocPrint(a, "{d}", .{value}), 10, false);
            }
            if (s == 0 and index < series.labels.items.len and !chart.horizontal) try painter.label(painter.ptr, .{ left + @as(f64, @floatFromInt(index)) * category_width, top + ph + 3, category_width, bottom - 3 }, series.labels.items[index], 10, false);
            if (s == 0 and index < series.labels.items.len and chart.horizontal) try painter.label(painter.ptr, .{ 0, top + @as(f64, @floatFromInt(index)) * category_width, left - 3, category_width }, series.labels.items[index], 10, false);
        }
    }
}

test "document native charts reject unsupported visible styling and malformed cached point structure" {
    const a = std.testing.allocator;
    const prefix = "<c:chartSpace xmlns:c='http://schemas.openxmlformats.org/drawingml/2006/chart' xmlns:a='http://schemas.openxmlformats.org/drawingml/2006/main'><c:chart><c:plotArea>";
    const series = "<c:barChart><c:ser><c:idx val='0'/><c:order val='0'/><c:cat><c:strLit><c:pt idx='0'><c:v>A</c:v></c:pt></c:strLit></c:cat><c:val><c:numLit><c:pt idx='0'><c:v>1</c:v></c:pt></c:numLit></c:val></c:ser></c:barChart>";
    const suffix = "</c:plotArea></c:chart></c:chartSpace>";
    const cases = [_]struct { body: []const u8, expected: anyerror }{
        .{ .body = "<c:barChart><c:gapWidth val='500'/></c:barChart>", .expected = error.DocumentRendererUnavailable },
        .{ .body = series ++ "<c:catAx><c:scaling><c:orientation val='maxMin'/></c:scaling></c:catAx>", .expected = error.DocumentRendererUnavailable },
        .{ .body = series ++ "<c:spPr><a:solidFill><a:srgbClr val='FF0000'/></a:solidFill></c:spPr>", .expected = error.DocumentRendererUnavailable },
        .{ .body = "<c:barChart><c:ser><c:idx val='0'/><c:order val='0'/><c:cat><c:strLit><c:pt idx='0'><c:v>A</c:v></c:pt><c:pt idx='0'><c:v>B</c:v></c:pt></c:strLit></c:cat></c:ser></c:barChart>", .expected = error.InvalidDocumentXml },
        .{ .body = "<c:barChart><c:ser><c:idx val='0'/><c:order val='0'/><c:val><c:numLit><c:formatCode>0.00%</c:formatCode><c:pt idx='0'><c:v>1</c:v></c:pt></c:numLit></c:val></c:ser></c:barChart>", .expected = error.DocumentRendererUnavailable },
        .{ .body = "<c:barChart><c:ser><c:idx val='0'/><c:order val='0'/><c:val><c:numLit><c:pt idx='0'><c:v>1</c:v></c:pt><c:v>2</c:v></c:numLit></c:val></c:ser></c:barChart>", .expected = error.InvalidDocumentXml },
    };
    for (cases) |case| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var run = c.RenderContext{ .limits = .{} };
        const bytes = try std.fmt.allocPrint(arena.allocator(), "{s}{s}{s}", .{ prefix, case.body, suffix });
        try std.testing.expectError(case.expected, parse(arena.allocator(), bytes, .{}, &run));
    }
}

test "document native chart bars clip their zero baseline to explicit axis bounds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Recorder = struct {
        bars: usize = 0,
        fn rect(raw: *anyopaque, box: [4]f64, color: themes.Color) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, &color.rgb, &.{ 1, 2, 3 })) return;
            self.bars += 1;
            try std.testing.expect(box[0] >= 0 and box[1] >= 0);
            try std.testing.expect(box[0] + box[2] <= 200 and box[1] + box[3] <= 100);
        }
        fn line(_: *anyopaque, _: []const [2]f64, _: themes.Color) !void {}
        fn label(_: *anyopaque, _: [4]f64, _: []const u8, _: f64, _: bool) !void {}
    };
    var series = Series{ .color = .{ .rgb = .{ 1, 2, 3 } } };
    try series.values.append(a, 15);
    for ([_]bool{ false, true }) |horizontal| {
        var recorder = Recorder{};
        var run = c.RenderContext{ .limits = .{} };
        try paint(a, .{ .kind = .bar, .horizontal = horizontal, .series = &.{series}, .minimum = 10, .maximum = 20, .vary_colors = false }, 200, 100, .{}, .{ .ptr = &recorder, .rect = Recorder.rect, .line = Recorder.line, .polygon = Recorder.line, .label = Recorder.label }, &run);
        try std.testing.expectEqual(@as(usize, 1), recorder.bars);
    }
}
