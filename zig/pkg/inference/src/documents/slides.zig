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
const Theme = @import("drawingml_theme.zig").Theme;
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
    render_view: xml.View,
    parent: ?[]const u8,
};
pub const Cache = struct {
    parts: std.StringHashMapUnmanaged(*Part) = .{},
    max_regions: usize = 65536,
    relationships: std.StringHashMapUnmanaged(*xml.Relationships) = .{},
    themes: std.StringHashMapUnmanaged(*Theme) = .{},
    pub fn themeForPart(self: *Cache, a: A, archive: xml.Archive, path: []const u8) !?Theme {
        const target = (try self.relatedPath(a, archive, path, .theme)) orelse return null;
        if (self.themes.get(target)) |known| return known.*;
        const parsed = try a.create(Theme);
        parsed.* = try Theme.parse(a, try archive.get(a, target), archive.control);
        try self.themes.put(a, target, parsed);
        return parsed.*;
    }
    pub fn renderView(self: *const Cache, path: []const u8) !xml.View {
        return (self.parts.get(path) orelse return error.MissingDocumentPart).render_view;
    }
    pub fn relationshipsFor(self: *Cache, a: A, archive: xml.Archive, path: []const u8) !?*xml.Relationships {
        if (self.relationships.get(path)) |known| return known;
        const slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse return error.InvalidDocumentSource;
        const rel_path = try std.fmt.allocPrint(a, "{s}/_rels/{s}.rels", .{ path[0..slash], path[slash + 1 ..] });
        const bytes = archive.get(a, rel_path) catch |err| switch (err) {
            error.MissingDocumentPart => return null,
            else => return err,
        };
        const result = try a.create(xml.Relationships);
        result.* = try xml.Relationships.init(a, bytes, archive.control);
        try self.relationships.put(a, path, result);
        return result;
    }
    pub fn relatedPath(self: *Cache, a: A, archive: xml.Archive, path: []const u8, kind: xml.RelationshipKind) !?[]const u8 {
        const index = (try self.relationshipsFor(a, archive, path)) orelse return null;
        const related = (try index.unique(kind)) orelse return null;
        const slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse return error.InvalidDocumentSource;
        return try normalizedPart(a, path[0 .. slash + 1], related.target);
    }
    pub fn inherited(self: *Cache, a: A, archive: xml.Archive, slide_path: []const u8, bytes: []const u8) ![]const c.InheritedPart {
        var out = std.ArrayList(c.InheritedPart).empty;
        const visible = try showsMaster(bytes);
        var background_visible = !try hasBackground(bytes);
        const layout_path = (try self.relatedPath(a, archive, slide_path, .layout)) orelse return &.{};
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
        try visible.requirePart(if (role == .layout) .layout else .master);
        const content = try inheritedContent(a, visible);
        const shapes = content.shapes;
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
            .render_view = content.render_view,
            .background_visual = if (content.background) |bg| try bg.needsVisualInspection() else false,
            .parent = if (role == .layout) (try self.relatedPath(a, archive, path, .master)) orelse return error.MissingDocumentPart else null,
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
fn inheritedContent(a: A, visible: xml.View) !struct { shapes: xml.View, background: ?xml.View, conditional_content: bool, render_view: xml.View } {
    const bytes = visible.bytes;
    var iter = xml.Xml{ .bytes = bytes };
    var exclusions = std.ArrayList(xml.View.Range).empty;
    defer exclusions.deinit(a);
    var placeholders = std.ArrayList(xml.View.Range).empty;
    defer placeholders.deinit(a);
    var start: ?usize = null;
    var depth: usize = 0;
    var placeholder = false;
    var background = false;
    var background_xml: ?xml.View = null;
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
                try exclusions.append(a, .{ .start = begin, .end = iter.index });
                if (placeholder) try placeholders.append(a, .{ .start = begin, .end = iter.index });
                if (background) background_xml = visible.slice(bytes[begin..iter.index]);
            }
            start = null;
        };
    }
    return .{ .shapes = try visible.excluding(a, exclusions.items), .background = background_xml, .conditional_content = conditional_content, .render_view = try visible.excluding(a, placeholders.items) };
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
    const content = try inheritedContent(a, .{ .bytes = "<p:sldMaster><p:bg><a:blipFill/></p:bg><p:sp><p:ph/><a:t>Template prompt</a:t></p:sp><p:sp><a:t>Confidential</a:t></p:sp></p:sldMaster>" });
    try std.testing.expectEqualStrings("Confidential", try content.shapes.text(a));
    try std.testing.expect(content.background != null);
    try std.testing.expect(try content.background.?.needsVisualInspection());
    try std.testing.expect(!try showsMaster("<p:sld showMasterSp='false'/>"));
    try std.testing.expect(!try showsMaster("<p:sld showMasterSp='&#48;'/>"));
    try std.testing.expect(try showsMaster("<p:sld showMasterSp=' tr&#117;e '/>"));
    try std.testing.expectError(error.InvalidDocumentXml, showsMaster("<p:sld showMasterSp='0' showMasterSp='1'/>"));
    const footer = try inheritedContent(a, .{ .bytes = "<p:sldMaster><p:sp><p:ph type='ftr'/><a:t>Footer</a:t></p:sp></p:sldMaster>" });
    try std.testing.expect(footer.conditional_content);
    try std.testing.expectEqualStrings("Footer", try footer.shapes.text(a));
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
    try std.testing.expectEqual(@as(usize, 3), cache.relationships.count());
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

test "document inherited fragments discard placeholder findings and preserve visible unknown shapes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = "<p:sldMaster xmlns:p='http://schemas.openxmlformats.org/presentationml/2006/main' xmlns:x='urn:future'>";
    const placeholder = "<p:sp><p:nvSpPr><p:nvPr><p:ph/></p:nvPr></p:nvSpPr><p:txBody><x:text/></p:txBody></p:sp>";
    const checked = try xml.View.init(a, root ++ "<p:cSld><p:bg><x:paint/></p:bg><p:spTree>" ++ placeholder ++ "</p:spTree></p:cSld></p:sldMaster>");
    const content = try inheritedContent(a, checked);
    try std.testing.expect(!try content.shapes.needsVisualInspection());
    try std.testing.expect(try content.background.?.needsVisualInspection());
    const visible = try xml.View.init(a, root ++ "<p:cSld><p:spTree>" ++ placeholder ++ "<p:sp><p:txBody><x:visible/></p:txBody></p:sp></p:spTree></p:cSld></p:sldMaster>");
    try std.testing.expect(try (try inheritedContent(a, visible)).shapes.needsVisualInspection());
}
