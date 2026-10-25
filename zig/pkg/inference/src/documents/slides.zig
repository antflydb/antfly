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

//! Source-local shared layout/master inspection. No rendering or external parts.
const std = @import("std");
const c = @import("contract.zig");
const xml = @import("ooxml.zig");
const A = std.mem.Allocator;
const Role = @FieldType(c.InheritedPart, "role");
const Part = struct {
    path: []const u8,
    role: Role,
    text: []const u8,
    tables: []const c.Table,
    show_master: bool,
    shape_visual: bool,
    has_background: bool,
    background_visual: bool,
    conditional_content: bool,
    parent: ?[]const u8,
};
pub const Cache = struct {
    parts: std.StringHashMapUnmanaged(*Part) = .{},
    max_regions: usize = 65536,
    pub fn inherited(self: *Cache, a: A, archive: xml.Archive, slide_path: []const u8, bytes: []const u8) ![]const c.InheritedPart {
        var out = std.ArrayList(c.InheritedPart).empty;
        const visible = try showsMaster(bytes);
        var background_visible = !try hasBackground(bytes);
        const layout_path = (try parentPath(a, archive, slide_path, .layout)) orelse return &.{};
        const layout = try self.load(a, archive, layout_path, .layout);
        try out.append(a, view(layout, visible, background_visible));
        background_visible = background_visible and !layout.has_background;
        if (layout.parent) |master_path| {
            if (std.mem.eql(u8, layout_path, master_path) or std.mem.eql(u8, slide_path, master_path)) return error.DocumentDependencyCycle;
            const master = try self.load(a, archive, master_path, .master);
            try out.append(a, view(master, visible and layout.show_master, background_visible));
        }
        return out.toOwnedSlice(a);
    }
    fn load(self: *Cache, a: A, archive: xml.Archive, path: []const u8, role: Role) !*Part {
        if (self.parts.get(path)) |part| {
            if (part.role != role) return error.InvalidDocumentXml;
            return part;
        }
        try archive.control.check();
        const visible = try xml.View.initWithControl(a, try archive.get(a, path), archive.control);
        const bytes = visible.bytes;
        var root = xml.Xml{ .bytes = bytes };
        var valid = false;
        while (try root.next()) |token| if (token == .tag and !token.tag.closing) {
            valid = std.mem.eql(u8, token.tag.local(), if (role == .layout) "sldLayout" else "sldMaster");
            break;
        };
        if (!valid) return error.InvalidDocumentXml;
        const content = try inheritedContent(a, bytes);
        const shapes = try xml.View.init(a, content.shapes);
        const part = try a.create(Part);
        part.* = .{
            .path = path,
            .role = role,
            .text = try shapes.text(a),
            .tables = try shapes.tables(a, self.max_regions),
            .show_master = try showsMaster(bytes),
            .shape_visual = try shapes.needsVisualInspection(),
            .has_background = content.background != null,
            .conditional_content = content.conditional_content,
            .background_visual = if (content.background) |bg| try (try xml.View.init(a, bg)).needsVisualInspection() else false,
            .parent = if (role == .layout) (try parentPath(a, archive, path, .master)) orelse return error.MissingDocumentPart else null,
        };
        try self.parts.put(a, path, part);
        return part;
    }
};
fn view(part: *const Part, shapes_visible: bool, background_visible: bool) c.InheritedPart {
    return .{ .path = part.path, .role = part.role, .text = part.text, .tables = part.tables, .shapes_visible = shapes_visible, .background_visible = background_visible and part.has_background, .shape_visual = part.shape_visual, .background_visual = part.background_visual, .conditional_content = part.conditional_content };
}
fn showsMaster(bytes: []const u8) !bool {
    var iter = xml.Xml{ .bytes = bytes };
    while (try iter.next()) |token| if (token == .tag and !token.tag.closing) {
        return token.tag.booleanAttr("showMasterSp", true);
    };
    return error.InvalidDocumentXml;
}
fn hasBackground(bytes: []const u8) !bool {
    var iter = xml.Xml{ .bytes = bytes };
    while (try iter.next()) |token| if (token == .tag and !token.tag.closing and std.mem.eql(u8, token.tag.local(), "bg")) return true;
    return false;
}

/// Placeholder prompts are template data, not displayed slide text. Keep
/// non-placeholder decorations and inspect backgrounds independently of shapes.
fn inheritedContent(a: A, bytes: []const u8) !struct { shapes: []const u8, background: ?[]const u8, conditional_content: bool } {
    var iter = xml.Xml{ .bytes = bytes };
    var output = std.Io.Writer.Allocating.init(a);
    var cursor: usize = 0;
    var start: ?usize = null;
    var depth: usize = 0;
    var placeholder = false;
    var background = false;
    var background_xml: ?[]const u8 = null;
    var conditional_content = false;
    while (try iter.next()) |token| {
        if (token != .tag) continue;
        const tag = token.tag;
        var started = false;
        if (start == null and !tag.closing) {
            const local = tag.local();
            const is_bg = std.mem.eql(u8, local, "bg");
            if (is_bg or std.mem.eql(u8, local, "sp") or std.mem.eql(u8, local, "pic") or std.mem.eql(u8, local, "graphicFrame") or std.mem.eql(u8, local, "cxnSp")) {
                const name_offset = @intFromPtr(tag.raw.ptr) - @intFromPtr(bytes.ptr);
                started = true;
                start = std.mem.lastIndexOfScalar(u8, bytes[0..name_offset], '<') orelse return error.InvalidDocumentXml;
                depth = iter.depth;
                placeholder = false;
                background = is_bg;
            }
        }
        if (start != null and !tag.closing and std.mem.eql(u8, tag.local(), "ph")) {
            const kind = tag.attr("type") orelse "obj";
            const conditional = std.mem.eql(u8, kind, "ftr") or std.mem.eql(u8, kind, "hdr") or std.mem.eql(u8, kind, "dt") or std.mem.eql(u8, kind, "sldNum");
            placeholder = !conditional;
            conditional_content = conditional_content or conditional;
        }
        if (start) |begin| if ((tag.closing and iter.depth < depth) or (tag.empty and started)) {
            if (placeholder or background) {
                try output.writer.writeAll(bytes[cursor..begin]);
                if (background) background_xml = bytes[begin..iter.index];
                cursor = iter.index;
            }
            start = null;
        };
    }
    try output.writer.writeAll(bytes[cursor..]);
    return .{ .shapes = try output.toOwnedSlice(), .background = background_xml, .conditional_content = conditional_content };
}
fn parentPath(a: A, archive: xml.Archive, path: []const u8, role: Role) !?[]const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse return error.InvalidDocumentSource;
    const rel_path = try std.fmt.allocPrint(a, "{s}/_rels/{s}.rels", .{ path[0..slash], path[slash + 1 ..] });
    const bytes = archive.get(a, rel_path) catch |err| switch (err) {
        error.MissingDocumentPart => return null,
        else => return err,
    };
    var iter = xml.Xml{ .bytes = bytes };
    var result: ?[]const u8 = null;
    while (try iter.next()) |token| {
        if (token != .tag or token.tag.closing or !std.mem.eql(u8, token.tag.local(), "Relationship")) continue;
        const kind = token.tag.attr("Type") orelse continue;
        if (!std.mem.endsWith(u8, kind, if (role == .layout) "/slideLayout" else "/slideMaster")) continue;
        if (result != null) return error.InvalidDocumentXml;
        if (token.tag.attr("TargetMode")) |mode| if (std.mem.eql(u8, mode, "External")) return error.ExternalDocumentPartDenied;
        result = try normalizedPart(a, path[0 .. slash + 1], token.tag.attr("Target") orelse return error.InvalidDocumentXml);
    }
    return result;
}
pub fn normalizedPart(a: A, base: []const u8, target: []const u8) ![]const u8 {
    if (target.len == 0 or target[0] == '/' or std.mem.indexOfAny(u8, target, "\\:%?#") != null) return error.ExternalDocumentPartDenied;
    var parts = std.ArrayList([]const u8).empty;
    const combined = try std.mem.concat(a, u8, &.{ base, target });
    var iter = std.mem.splitScalar(u8, combined, '/');
    while (iter.next()) |part| {
        if (part.len == 0 or std.mem.eql(u8, part, ".")) continue;
        if (std.mem.eql(u8, part, "..")) {
            if (parts.items.len == 0) return error.ExternalDocumentPartDenied;
            _ = parts.pop();
        } else try parts.append(a, part);
    }
    const result = try std.mem.join(a, "/", parts.items);
    if (!std.mem.startsWith(u8, result, "ppt/")) return error.ExternalDocumentPartDenied;
    return result;
}

test "document inherited slide text excludes placeholders and separates backgrounds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const content = try inheritedContent(a, "<p:sldMaster><p:bg><a:blipFill/></p:bg><p:sp><p:ph/><a:t>Template prompt</a:t></p:sp><p:sp><a:t>Confidential</a:t></p:sp></p:sldMaster>");
    try std.testing.expectEqualStrings("Confidential", try (try xml.View.init(a, content.shapes)).text(a));
    try std.testing.expect(content.background != null);
    try std.testing.expect(try (try xml.View.init(a, content.background.?)).needsVisualInspection());
    try std.testing.expect(!try showsMaster("<p:sld showMasterSp='false'/>"));
    try std.testing.expect(!try showsMaster("<p:sld showMasterSp='&#48;'/>"));
    try std.testing.expect(try showsMaster("<p:sld showMasterSp=' tr&#117;e '/>"));
    try std.testing.expectError(error.InvalidDocumentXml, showsMaster("<p:sld showMasterSp='0' showMasterSp='1'/>"));
    const footer = try inheritedContent(a, "<p:sldMaster><p:sp><p:ph type='ftr'/><a:t>Footer</a:t></p:sp></p:sldMaster>");
    try std.testing.expect(footer.conditional_content);
    try std.testing.expectEqualStrings("Footer", try (try xml.View.init(a, footer.shapes)).text(a));
}

test "document inheritance shares parsed masters and honors visibility with bounded references" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const archive = try xml.Archive.init(a, @embedFile("testdata/inherited-slides.pptx"), 1024 * 1024);
    var cache = Cache{};
    const visible = try cache.inherited(a, archive, "ppt/slides/slide2.xml", try archive.get(a, "ppt/slides/slide2.xml"));
    const hidden = try cache.inherited(a, archive, "ppt/slides/slide1.xml", try archive.get(a, "ppt/slides/slide1.xml"));
    try std.testing.expectEqual(@as(usize, 2), cache.parts.count());
    try std.testing.expectEqual(@as(usize, 2), visible.len);
    try std.testing.expectEqualStrings("Confidential\n", visible[1].text);
    try std.testing.expect(visible[1].text.ptr == hidden[1].text.ptr);
    try std.testing.expect(visible[1].shapes_visible and visible[1].shape_visual);
    try std.testing.expect(!hidden[0].shapes_visible and !hidden[1].shapes_visible);
    const layout = cache.parts.get("ppt/slideLayouts/slideLayout1.xml").?;
    layout.parent = layout.path;
    try std.testing.expectError(error.DocumentDependencyCycle, cache.inherited(a, archive, "ppt/slides/slide2.xml", try archive.get(a, "ppt/slides/slide2.xml")));
    try std.testing.expectError(error.ExternalDocumentPartDenied, normalizedPart(a, "ppt/", "https://example.com/master.xml"));
}
