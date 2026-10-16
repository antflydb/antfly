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

const std = @import("std");
const Options = @import("options.zig").Options;
pub fn validate(raw: []const u8) !void {
    var parts = std.mem.splitScalar(u8, raw, ':');
    const target = parts.next().?;
    if (!std.mem.eql(u8, target, "path") and !std.mem.eql(u8, target, "line") and !std.mem.eql(u8, target, "column") and !std.mem.eql(u8, target, "match")) return error.InvalidColorSpec;
    const attr = parts.next() orelse return error.InvalidColorSpec;
    if (std.mem.eql(u8, attr, "none")) {
        if (parts.next() != null) return error.InvalidColorSpec;
        return;
    }
    const value = parts.next() orelse return error.InvalidColorSpec;
    if (parts.next() != null) return error.InvalidColorSpec;
    if (std.mem.eql(u8, attr, "style")) {
        _ = try style(value);
    } else if (std.mem.eql(u8, attr, "fg") or std.mem.eql(u8, attr, "bg")) {
        if (named(value) == null) {
            var values = std.mem.splitScalar(u8, value, ',');
            var n: usize = 0;
            while (values.next()) |v| {
                _ = std.fmt.parseInt(u8, v, 10) catch return error.InvalidColorSpec;
                n += 1;
            }
            if (n != 1 and n != 3) return error.InvalidColorSpec;
        }
    } else return error.InvalidColorSpec;
}
fn named(value: []const u8) ?u8 {
    const names = [_][]const u8{ "black", "red", "green", "yellow", "blue", "magenta", "cyan", "white" };
    for (names, 0..) |name, i| if (std.mem.eql(u8, name, value)) return @intCast(i);
    return null;
}
fn style(value: []const u8) !u8 {
    const names = .{ .{ "bold", @as(u8, 1) }, .{ "nobold", @as(u8, 22) }, .{ "intense", @as(u8, 1) }, .{ "nointense", @as(u8, 22) }, .{ "underline", @as(u8, 4) }, .{ "nounderline", @as(u8, 24) } };
    inline for (names) |entry| if (std.mem.eql(u8, value, entry[0])) return entry[1];
    return error.InvalidColorSpec;
}
pub fn start(writer: *std.Io.Writer, o: *const Options, target: []const u8) !void {
    // Defaults, then ordered overrides. 'none' resets this target's attributes.
    var begin: usize = 0;
    var reset = false;
    for (o.colors.items, 0..) |raw, i| {
        var parts = std.mem.splitScalar(u8, raw, ':');
        if (!std.mem.eql(u8, parts.next().?, target)) continue;
        if (std.mem.eql(u8, parts.next().?, "none")) {
            begin = i + 1;
            reset = true;
        }
    }
    if (!reset) try writer.writeAll(if (std.mem.eql(u8, target, "path")) "\x1b[0m\x1b[35m" else if (std.mem.eql(u8, target, "match")) "\x1b[0m\x1b[1m\x1b[31m" else "\x1b[0m\x1b[32m");
    for (o.colors.items[begin..]) |raw| {
        var parts = std.mem.splitScalar(u8, raw, ':');
        if (!std.mem.eql(u8, parts.next().?, target)) continue;
        const attr = parts.next().?;
        if (std.mem.eql(u8, attr, "none")) continue;
        const value = parts.next().?;
        if (std.mem.eql(u8, attr, "style")) try writer.print("\x1b[{d}m", .{try style(value)}) else {
            const bg = std.mem.eql(u8, attr, "bg");
            if (named(value)) |n| try writer.print("\x1b[{d}m", .{@as(u16, if (bg) 40 else 30) + n}) else {
                try writer.print("\x1b[{d};{d}", .{ @as(u8, if (bg) 48 else 38), @as(u8, if (std.mem.indexOfScalar(u8, value, ',') != null) 2 else 5) });
                var values = std.mem.splitScalar(u8, value, ',');
                while (values.next()) |part| try writer.print(";{s}", .{part});
                try writer.writeByte('m');
            }
        }
    }
}
fn hyperlinkTemplate(format: []const u8) []const u8 {
    const presets = .{ .{ "default", "file://{host}{path}" }, .{ "file", "file://{host}{path}" }, .{ "kitty", "file://{host}{path}#{line}" }, .{ "grep+", "grep+://{path}:{line}" }, .{ "macvim", "mvim://open?url=file://{path}&line={line}&column={column}" }, .{ "textmate", "txmt://open?url=file://{path}&line={line}&column={column}" }, .{ "vscode", "vscode://file{path}:{line}:{column}" }, .{ "vscode-insiders", "vscode-insiders://file{path}:{line}:{column}" }, .{ "vscodium", "vscodium://file{path}:{line}:{column}" }, .{ "cursor", "cursor://file{path}:{line}:{column}" } };
    inline for (presets) |entry| if (std.mem.eql(u8, format, entry[0])) return entry[1];
    return format;
}
pub fn validateHyperlink(format: []const u8) !void {
    if (format.len == 0 or std.mem.eql(u8, format, "none")) return;
    const template = hyperlinkTemplate(format);
    if (std.mem.indexOf(u8, template, "{path}") == null) return error.InvalidHyperlinkFormat;
    var i: usize = 0;
    while (i < template.len) : (i += 1) {
        if (template[i] != '{') continue;
        const end = std.mem.indexOfScalarPos(u8, template, i, '}') orelse return error.InvalidHyperlinkFormat;
        const key = template[i + 1 .. end];
        if (!std.mem.eql(u8, key, "path") and !std.mem.eql(u8, key, "line") and !std.mem.eql(u8, key, "column") and !std.mem.eql(u8, key, "host")) return error.InvalidHyperlinkFormat;
        i = end;
    }
}
pub fn hyperlink(a: std.mem.Allocator, io: std.Io, w: *std.Io.Writer, o: *const Options, path: []const u8, line: usize, column: usize) !bool {
    const format = o.hyperlink_format orelse return false;
    if (std.mem.eql(u8, format, "none") or format.len == 0) return false;
    const full = try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
    defer a.free(full);
    const template = hyperlinkTemplate(format);

    try w.writeAll("\x1b]8;;");
    var i: usize = 0;
    while (i < template.len) {
        if (template[i] == '{') {
            const end = std.mem.indexOfScalarPos(u8, template, i, '}') orelse return error.InvalidHyperlinkFormat;
            const key = template[i + 1 .. end];
            if (std.mem.eql(u8, key, "path")) {
                for (full) |ch| {
                    if (std.ascii.isAlphanumeric(ch) or std.mem.indexOfScalar(u8, "/-._~:", ch) != null) try w.writeByte(ch) else try w.print("%{X:0>2}", .{ch});
                }
            } else if (std.mem.eql(u8, key, "host")) try w.writeAll(o.hostname) else if (std.mem.eql(u8, key, "line")) try w.print("{d}", .{line}) else if (std.mem.eql(u8, key, "column")) try w.print("{d}", .{column}) else return error.InvalidHyperlinkFormat;
            i = end + 1;
        } else {
            try w.writeByte(template[i]);
            i += 1;
        }
    }
    try w.writeAll("\x1b\\");
    return true;
}
