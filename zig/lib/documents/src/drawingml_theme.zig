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

//! Source-owned immutable theme colors and fonts, shared by drawing and chart
//! compilation. Prefix aliases and foreign lookalikes never select a color.
const std = @import("std");
const xml = @import("ooxml.zig");
const c = @import("types.zig");
const A = std.mem.Allocator;
pub const Role = enum { dk1, lt1, dk2, lt2, accent1, accent2, accent3, accent4, accent5, accent6, hlink, folHlink };
pub const Color = struct {
    rgb: [3]u8,
    alpha: f64 = 1,
    fn scale(self: *Color, factor: f64, offset: f64) void {
        for (&self.rgb) |*channel| channel.* = @intFromFloat(@round(std.math.clamp(@as(f64, @floatFromInt(channel.*)) * factor + 255 * offset, 0, 255)));
    }
    fn luminance(self: *Color, factor: f64, offset: f64) void {
        var channels: [3]f64 = undefined;
        for (self.rgb, &channels) |value, *channel| channel.* = @as(f64, @floatFromInt(value)) / 255;
        const high = @max(channels[0], channels[1], channels[2]);
        const low = @min(channels[0], channels[1], channels[2]);
        const old_l = (high + low) / 2;
        const new_l = std.math.clamp(old_l * factor + offset, 0, 1);
        const delta = high - low;
        const saturation = if (delta == 0) 0 else delta / (1 - @abs(2 * old_l - 1));
        const next_delta = (1 - @abs(2 * new_l - 1)) * saturation;
        for (&self.rgb, channels) |*channel, original| {
            const hue_component = if (delta == 0) 0 else (original - low) / delta;
            channel.* = @intFromFloat(@round(std.math.clamp((new_l - next_delta / 2 + hue_component * next_delta) * 255, 0, 255)));
        }
    }
};
pub const Theme = struct {
    colors: [12]?Color = @splat(null),
    mapping: [12]Role = .{ .lt1, .dk1, .lt2, .dk2, .accent1, .accent2, .accent3, .accent4, .accent5, .accent6, .hlink, .folHlink },
    major_latin: []const u8 = "Roboto",
    minor_latin: []const u8 = "Roboto",

    pub fn resolve(self: Theme, name: []const u8) !Color {
        const role = aliases: {
            inline for (.{ "bg1", "tx1", "bg2", "tx2", "accent1", "accent2", "accent3", "accent4", "accent5", "accent6", "hlink", "folHlink" }, 0..) |alias, index|
                if (std.mem.eql(u8, name, alias)) break :aliases self.mapping[index];
            break :aliases std.meta.stringToEnum(Role, name) orelse return error.DocumentRendererUnavailable;
        };
        return self.colors[@backingInt(role)] orelse error.DocumentRendererUnavailable;
    }
    pub fn parse(a: A, bytes: []const u8, control: c.Control) !Theme {
        var result = Theme{};
        var iter = xml.QualifiedXml.init(a, bytes);
        defer iter.deinit();
        var scheme: ?usize = null;
        var elements: ?usize = null;
        var font_scheme: ?usize = null;
        var font: ?enum { major, minor } = null;
        var font_depth: usize = 0;
        var root_seen = false;
        while (try iter.next()) |token| {
            try control.check();
            if (token != .tag) continue;
            const item = token.tag;
            const tag = item.value;
            if (tag.closing) {
                if (scheme == item.depth) scheme = null;
                if (elements == item.depth) elements = null;
                if (font_scheme == item.depth) font_scheme = null;
                if (font != null and font_depth == item.depth) font = null;
                continue;
            }
            if (item.depth == 1) {
                if (root_seen or item.namespace != .drawing or !std.mem.eql(u8, tag.local(), "theme")) return error.InvalidDocumentXml;
                root_seen = true;
            }
            if (item.namespace != .drawing) continue;
            if (item.depth == 2 and std.mem.eql(u8, tag.local(), "themeElements")) elements = item.depth;
            if (elements != null and item.depth == elements.? + 1 and std.mem.eql(u8, tag.local(), "clrScheme")) scheme = item.depth;
            if (elements != null and item.depth == elements.? + 1 and std.mem.eql(u8, tag.local(), "fontScheme")) font_scheme = item.depth;
            if (scheme) |depth| if (item.depth == depth + 1) {
                const role = std.meta.stringToEnum(Role, tag.local()) orelse return error.DocumentRendererUnavailable;
                if (result.colors[@backingInt(role)] != null or tag.empty) return error.InvalidDocumentXml;
                const value = (try iter.next()) orelse return error.InvalidDocumentXml;
                // Whitespace between schema elements is ordinary XML text.
                var color_token = value;
                while (color_token == .text and std.mem.trim(u8, color_token.text, " \t\r\n").len == 0) color_token = (try iter.next()) orelse return error.InvalidDocumentXml;
                if (color_token != .tag or color_token.tag.namespace != .drawing or color_token.tag.value.closing) return error.InvalidDocumentXml;
                result.colors[@backingInt(role)] = try result.readColor(a, &iter, color_token.tag.value, control);
            };
            if (font_scheme != null and item.depth == font_scheme.? + 1 and (std.mem.eql(u8, tag.local(), "majorFont") or std.mem.eql(u8, tag.local(), "minorFont"))) {
                font = if (std.mem.eql(u8, tag.local(), "majorFont")) .major else .minor;
                font_depth = item.depth;
            }
            if (font != null and item.depth == font_depth + 1 and std.mem.eql(u8, tag.local(), "latin")) {
                const family = try xml.decodedText(a, (try tag.uniqueAttr("typeface")) orelse return error.InvalidDocumentXml);
                if (family.len == 0 or family.len > 256) return error.InvalidDocumentXml;
                if (font.? == .major) result.major_latin = family else result.minor_latin = family;
            }
        }
        if (!root_seen) return error.InvalidDocumentXml;
        return result;
    }
    pub fn withMapping(self: Theme, a: A, view: xml.View, control: c.Control) !Theme {
        var result = self;
        var iter = xml.QualifiedXml.init(a, view.bytes);
        defer iter.deinit();
        var override: ?usize = null;
        while (try iter.next()) |token| {
            try control.check();
            if (token != .tag) continue;
            const item = token.tag;
            const tag = item.value;
            if (tag.closing) {
                if (override == item.depth) override = null;
                continue;
            }
            if (item.namespace == .presentation and item.depth == 2 and std.mem.eql(u8, tag.local(), "clrMapOvr")) override = item.depth;
            if ((item.namespace == .presentation and item.depth == 2 and std.mem.eql(u8, tag.local(), "clrMap")) or
                (item.namespace == .drawing and override != null and item.depth == override.? + 1 and std.mem.eql(u8, tag.local(), "overrideClrMapping")))
            {
                inline for (.{ "bg1", "tx1", "bg2", "tx2", "accent1", "accent2", "accent3", "accent4", "accent5", "accent6", "hlink", "folHlink" }, 0..) |alias, index| if (try tag.uniqueAttr(alias)) |raw| {
                    result.mapping[index] = std.meta.stringToEnum(Role, try xml.decodedText(a, raw)) orelse return error.InvalidDocumentXml;
                };
            }
        }
        return result;
    }
    /// Consume exactly one qualified color subtree, preserving transform order.
    pub fn readColor(self: Theme, a: A, iter: *xml.QualifiedXml, tag: xml.Tag, control: c.Control) !Color {
        const name = tag.local();
        const raw = (try tag.uniqueAttr(if (std.mem.eql(u8, name, "sysClr")) "lastClr" else "val")) orelse return error.InvalidDocumentXml;
        const value = try xml.decodedText(a, raw);
        var result: Color = undefined;
        if (std.mem.eql(u8, name, "schemeClr")) result = try self.resolve(value) else if (std.mem.eql(u8, name, "srgbClr") or std.mem.eql(u8, name, "sysClr")) {
            if (value.len != 6) return error.InvalidDocumentXml;
            var rgb: [3]u8 = undefined;
            _ = std.fmt.hexToBytes(&rgb, value) catch return error.InvalidDocumentXml;
            result = .{ .rgb = rgb };
        } else return error.DocumentRendererUnavailable;
        if (tag.empty) return result;
        const depth = iter.xml.depth;
        while (try iter.next()) |token| {
            try control.check();
            if (token == .text) {
                if (std.mem.trim(u8, token.text, " \t\r\n").len != 0) return error.InvalidDocumentXml;
                continue;
            }
            const item = token.tag;
            if (item.value.closing and item.depth == depth) return result;
            if (item.namespace != .drawing or item.value.closing or !item.value.empty or item.depth != depth + 1) return error.DocumentRendererUnavailable;
            const encoded = try xml.decodedText(a, (try item.value.uniqueAttr("val")) orelse return error.InvalidDocumentXml);
            const fraction = if (std.mem.endsWith(u8, encoded, "%")) percent: {
                const percent_value = std.fmt.parseFloat(f64, encoded[0 .. encoded.len - 1]) catch return error.InvalidDocumentXml;
                break :percent percent_value / 100;
            } else integer: {
                const integer_value = std.fmt.parseInt(i64, encoded, 10) catch return error.InvalidDocumentXml;
                break :integer @as(f64, @floatFromInt(integer_value)) / 100000;
            };
            if (!std.math.isFinite(fraction) or fraction < 0 or fraction > 1) return error.DocumentRendererUnavailable;
            const operation = item.value.local();
            if (std.mem.eql(u8, operation, "tint")) result.scale(fraction, 1 - fraction) else if (std.mem.eql(u8, operation, "shade")) result.scale(fraction, 0) else if (std.mem.eql(u8, operation, "lumMod")) result.luminance(fraction, 0) else if (std.mem.eql(u8, operation, "lumOff")) result.luminance(1, fraction) else if (std.mem.eql(u8, operation, "alpha")) result.alpha = fraction else if (std.mem.eql(u8, operation, "alphaMod")) result.alpha *= fraction else return error.DocumentRendererUnavailable;
        }
        return error.InvalidDocumentXml;
    }
};

test "document theme colors honor aliases ordered transforms and font families" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const theme = try Theme.parse(a,
        \\<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"><a:themeElements><a:clrScheme><a:dk1><a:sysClr val="windowText" lastClr="000000"/></a:dk1><a:lt1><a:srgbClr val="FFFFFF"/></a:lt1><a:accent1><a:srgbClr val="0080FF"/></a:accent1></a:clrScheme><a:fontScheme><a:majorFont><a:latin typeface="Example Display"/></a:majorFont><a:minorFont><a:latin typeface="Example Body"/></a:minorFont></a:fontScheme></a:themeElements></a:theme>
    , .{});
    try std.testing.expectEqualStrings("Example Display", theme.major_latin);
    try std.testing.expectEqualStrings("Example Body", theme.minor_latin);
    try std.testing.expectEqualSlices(u8, &.{ 255, 255, 255 }, &(try theme.resolve("bg1")).rgb);
    var iter = xml.QualifiedXml.init(a, "<a:schemeClr xmlns:a='http://schemas.openxmlformats.org/drawingml/2006/main' val='accent1'><a:tint val='50000'/><a:alpha val='25000'/></a:schemeClr>");
    defer iter.deinit();
    const color = try theme.readColor(a, &iter, (try iter.next()).?.tag.value, .{});
    try std.testing.expectEqualSlices(u8, &.{ 128, 192, 255 }, &color.rgb);
    try std.testing.expectApproxEqAbs(@as(f64, 0.25), color.alpha, 0.0001);
}
