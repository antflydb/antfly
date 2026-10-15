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

//! Bounded XML tree for DASH inheritance. Names and unescaped attributes borrow
//! input; decoded attributes, text and node storage are tree-owned. No DTDs,
//! external entities or processing instructions other than the XML declaration.
const std = @import("std");
pub const Limits = struct {
    max_bytes: usize = 1024 * 1024,
    max_nodes: usize = 65536,
    max_depth: usize = 32,
    max_attributes: usize = 32,
    max_metadata_bytes: usize = 64 * 1024 * 1024,
};
pub const Attribute = struct { name: []const u8, value: []const u8 };
pub const Node = struct {
    name: []const u8,
    attributes: []const Attribute,
    text: std.ArrayList(u8) = .empty,
    parent: ?usize,
    first_child: ?usize = null,
    last_child: ?usize = null,
    next_sibling: ?usize = null,
    pub fn get(self: Node, name: []const u8) ?[]const u8 {
        for (self.attributes) |attribute| if (std.mem.eql(u8, attribute.name, name)) return attribute.value;
        return null;
    }
    pub fn localName(self: Node) []const u8 {
        return if (std.mem.lastIndexOfScalar(u8, self.name, ':')) |colon| self.name[colon + 1 ..] else self.name;
    }
};
pub const Tree = struct {
    arena: std.heap.ArenaAllocator,
    nodes: []Node,
    pub fn deinit(self: *Tree) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn namespace(self: Tree, index: usize, name: []const u8, attribute: bool) ![]const u8 {
        if (index >= self.nodes.len) return error.MalformedDash;
        const colon = std.mem.indexOfScalar(u8, name, ':');
        if (attribute and (std.mem.eql(u8, name, "xmlns") or (colon != null and std.mem.eql(u8, name[0..colon.?], "xmlns")))) return xmlns_uri;
        if (colon) |at| {
            const prefix = name[0..at];
            if (std.mem.eql(u8, prefix, "xmlns")) return error.MalformedDash;
            if (std.mem.eql(u8, prefix, "xml")) return xml_uri;
        } else if (attribute) return "";
        var current: ?usize = index;
        while (current) |node| : (current = self.nodes[node].parent) {
            for (self.nodes[node].attributes) |item| {
                const matches = if (colon) |at| std.mem.startsWith(u8, item.name, "xmlns:") and std.mem.eql(u8, item.name[6..], name[0..at]) else std.mem.eql(u8, item.name, "xmlns");
                if (matches) return if (colon != null and item.value.len == 0) error.MalformedDash else item.value;
            }
        }
        return if (colon != null) error.MalformedDash else "";
    }
    fn validateNamespaces(self: Tree) !void {
        for (self.nodes, 0..) |node, index| {
            for (node.attributes) |item| {
                if (std.mem.eql(u8, item.name, "xmlns:xmlns")) return error.MalformedDash;
                if (std.mem.eql(u8, item.name, "xmlns") or std.mem.startsWith(u8, item.name, "xmlns:")) {
                    if (std.mem.startsWith(u8, item.name, "xmlns:") and item.value.len == 0) return error.MalformedDash;
                    if (std.mem.eql(u8, item.value, xmlns_uri)) return error.MalformedDash;
                    if (std.mem.eql(u8, item.name, "xmlns:xml") != std.mem.eql(u8, item.value, xml_uri)) return error.MalformedDash;
                }
            }
            _ = try self.namespace(index, node.name, false);
            for (node.attributes, 0..) |item, i| {
                const ns = try self.namespace(index, item.name, true);
                const local = localName(item.name);
                for (node.attributes[0..i]) |previous| if (std.mem.eql(u8, local, localName(previous.name)) and std.mem.eql(u8, ns, try self.namespace(index, previous.name, true))) return error.MalformedDash;
            }
        }
    }
    pub fn init(a: std.mem.Allocator, bytes: []const u8, limits: Limits) !Tree {
        if (bytes.len > limits.max_bytes) return error.ResourceLimitExceeded;
        if (!std.unicode.utf8ValidateSlice(bytes)) return error.MalformedDash;
        var characters = std.unicode.Utf8View.initUnchecked(bytes).iterator();
        while (characters.nextCodepoint()) |codepoint| if (!validCharacter(codepoint)) return error.MalformedDash;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        var parser = Parser{ .a = arena.allocator(), .bytes = bytes, .limits = limits };
        try parser.parse();
        const tree = Tree{ .arena = arena, .nodes = parser.nodes.items };
        try tree.validateNamespaces();
        return tree;
    }
};
const xml_uri = "http://www.w3.org/XML/1998/namespace";
const xmlns_uri = "http://www.w3.org/2000/xmlns/";
fn localName(name: []const u8) []const u8 {
    return if (std.mem.lastIndexOfScalar(u8, name, ':')) |colon| name[colon + 1 ..] else name;
}
/// Decode an attribute into the supplied temporary owner when escaping is used.
/// Unescaped values remain borrowed. Callers must keep either owner alive.
pub fn decodeAttribute(a: std.mem.Allocator, raw: []const u8) ![]const u8 {
    var parser = Parser{ .a = a, .bytes = raw, .limits = .{} };
    return parser.decoded(raw, true);
}
fn space(byte: u8) bool {
    return byte == ' ' or byte == '\t' or byte == '\r' or byte == '\n';
}
fn validCharacter(codepoint: u32) bool {
    return codepoint == 9 or codepoint == 10 or codepoint == 13 or (codepoint >= 0x20 and codepoint <= 0xd7ff) or (codepoint >= 0xe000 and codepoint <= 0xfffd) or (codepoint >= 0x10000 and codepoint <= 0x10ffff);
}
fn entityNumber(digits: []const u8, base: u8) !u32 {
    if (digits.len == 0) return error.MalformedDash;
    for (digits) |digit| if (!(std.ascii.isDigit(digit) or (base == 16 and ((digit >= 'a' and digit <= 'f') or (digit >= 'A' and digit <= 'F'))))) return error.MalformedDash;
    return std.fmt.parseInt(u32, digits, base) catch error.MalformedDash;
}
const Parser = struct {
    a: std.mem.Allocator,
    bytes: []const u8,
    limits: Limits,
    at: usize = 0,
    charged: usize = 0,
    nodes: std.ArrayList(Node) = .empty,
    stack: std.ArrayList(usize) = .empty,
    declaration: bool = false,
    // Account conservatively for arena-retained geometric buffer growth, not
    // just live element counts. Limits are checked before allocating buffers.
    fn charge(self: *Parser, bytes: usize) !void {
        const reserve = std.math.mul(usize, bytes, 8) catch return error.ResourceLimitExceeded;
        self.charged = std.math.add(usize, self.charged, reserve) catch return error.ResourceLimitExceeded;
        if (self.charged > self.limits.max_metadata_bytes) return error.ResourceLimitExceeded;
    }
    fn spaces(self: *Parser) void {
        while (self.at < self.bytes.len and space(self.bytes[self.at])) self.at += 1;
    }
    fn starts(self: *Parser, prefix: []const u8) bool {
        return std.mem.startsWith(u8, self.bytes[self.at..], prefix);
    }
    fn name(self: *Parser) ![]const u8 {
        const start = self.at;
        if (start == self.bytes.len or (!std.ascii.isAlphabetic(self.bytes[start]) and self.bytes[start] != '_')) return error.MalformedDash;
        self.at += 1;
        var colon: ?usize = null;
        while (self.at < self.bytes.len) : (self.at += 1) {
            const byte = self.bytes[self.at];
            if (byte == ':') {
                if (colon != null) return error.MalformedDash;
                colon = self.at;
            } else if (!std.ascii.isAlphanumeric(byte) and byte != '_' and byte != '-' and byte != '.') break;
        }
        if (colon) |position| if (position + 1 == self.at or (!std.ascii.isAlphabetic(self.bytes[position + 1]) and self.bytes[position + 1] != '_')) return error.MalformedDash;
        return self.bytes[start..self.at];
    }
    fn decoded(self: *Parser, raw: []const u8, attribute: bool) ![]const u8 {
        for (raw) |byte| if (byte < 0x20 and !space(byte)) return error.MalformedDash;
        if (std.mem.indexOfScalar(u8, raw, '&') == null and (!attribute or std.mem.indexOfAny(u8, raw, "\t\r\n") == null)) return raw;
        try self.charge(raw.len);
        var output: std.ArrayList(u8) = .empty;
        var at: usize = 0;
        while (at < raw.len) {
            const byte = raw[at];
            at += 1;
            if (byte != '&') {
                try output.append(self.a, if (attribute and space(byte)) ' ' else byte);
                continue;
            }
            const end = std.mem.indexOfScalarPos(u8, raw, at, ';') orelse return error.MalformedDash;
            const entity = raw[at..end];
            const codepoint: u32 = if (std.mem.eql(u8, entity, "amp")) '&' else if (std.mem.eql(u8, entity, "lt")) '<' else if (std.mem.eql(u8, entity, "gt")) '>' else if (std.mem.eql(u8, entity, "quot")) '"' else if (std.mem.eql(u8, entity, "apos")) '\'' else if (std.mem.startsWith(u8, entity, "#x")) try entityNumber(entity[2..], 16) else if (std.mem.startsWith(u8, entity, "#")) try entityNumber(entity[1..], 10) else return error.UnsupportedDash;
            if (!validCharacter(codepoint)) return error.MalformedDash;
            var encoded: [4]u8 = undefined;
            const size = std.unicode.utf8Encode(@intCast(codepoint), &encoded) catch return error.MalformedDash;
            try output.appendSlice(self.a, encoded[0..size]);
            at = end + 1;
        }
        return output.items;
    }
    fn attributes(self: *Parser) ![]const Attribute {
        var attributes_: std.ArrayList(Attribute) = .empty;
        while (true) {
            const before = self.at;
            self.spaces();
            if (self.at == self.bytes.len) return error.MalformedDash;
            if (self.bytes[self.at] == '>' or self.bytes[self.at] == '/' or self.bytes[self.at] == '?') return attributes_.items;
            if (self.at == before) return error.MalformedDash;
            if (attributes_.items.len == self.limits.max_attributes) return error.ResourceLimitExceeded;
            const key = try self.name();
            for (attributes_.items) |attribute| if (std.mem.eql(u8, attribute.name, key)) return error.MalformedDash;
            self.spaces();
            if (self.at == self.bytes.len or self.bytes[self.at] != '=') return error.MalformedDash;
            self.at += 1;
            self.spaces();
            if (self.at == self.bytes.len) return error.MalformedDash;
            const quote = self.bytes[self.at];
            if (quote != '\'' and quote != '"') return error.MalformedDash;
            self.at += 1;
            const start = self.at;
            while (self.at < self.bytes.len and self.bytes[self.at] != quote) : (self.at += 1) if (self.bytes[self.at] == '<') return error.MalformedDash;
            if (self.at == self.bytes.len) return error.MalformedDash;
            const value = try self.decoded(self.bytes[start..self.at], true);
            self.at += 1;
            try self.charge(@sizeOf(Attribute));
            try attributes_.append(self.a, .{ .name = key, .value = value });
        }
    }
    fn text(self: *Parser, raw: []const u8, cdata: bool) !void {
        if (self.stack.items.len == 0) {
            for (raw) |byte| if (!space(byte)) return error.MalformedDash;
            return;
        }
        for (raw) |byte| if (byte < 0x20 and !space(byte)) return error.MalformedDash;
        if (!cdata and std.mem.indexOf(u8, raw, "]]>") != null) return error.MalformedDash;
        const value = if (cdata) raw else try self.decoded(raw, false);
        try self.charge(value.len);
        try self.nodes.items[self.stack.items[self.stack.items.len - 1]].text.appendSlice(self.a, value);
    }
    fn parse(self: *Parser) !void {
        if (std.mem.startsWith(u8, self.bytes, "\xef\xbb\xbf")) self.at = 3;
        while (self.at < self.bytes.len) {
            if (self.bytes[self.at] != '<') {
                const end = std.mem.indexOfScalarPos(u8, self.bytes, self.at, '<') orelse self.bytes.len;
                try self.text(self.bytes[self.at..end], false);
                self.at = end;
                continue;
            }
            if (self.starts("<!--")) {
                const end = std.mem.indexOfPos(u8, self.bytes, self.at + 4, "-->") orelse return error.MalformedDash;
                const comment = self.bytes[self.at + 4 .. end];
                if (std.mem.indexOf(u8, comment, "--") != null or std.mem.endsWith(u8, comment, "-")) return error.MalformedDash;
                self.at = end + 3;
                continue;
            }
            if (self.starts("<![CDATA[")) {
                if (self.stack.items.len == 0) return error.MalformedDash;
                const end = std.mem.indexOfPos(u8, self.bytes, self.at + 9, "]]>") orelse return error.MalformedDash;
                try self.text(self.bytes[self.at + 9 .. end], true);
                self.at = end + 3;
                continue;
            }
            if (self.starts("<?")) {
                if (!self.starts("<?xml ") or self.declaration or self.nodes.items.len != 0) return error.UnsupportedDash;
                if (self.at != (if (std.mem.startsWith(u8, self.bytes, "\xef\xbb\xbf")) @as(usize, 3) else 0)) return error.MalformedDash;
                self.at += 5;
                const attrs = try self.attributes();
                if (!self.starts("?>")) return error.MalformedDash;
                const declaration_ = Node{ .name = "xml", .attributes = attrs, .parent = null };
                if (!std.mem.eql(u8, declaration_.get("version") orelse return error.MalformedDash, "1.0")) return error.UnsupportedDash;
                if (declaration_.get("encoding")) |encoding| if (!std.ascii.eqlIgnoreCase(encoding, "utf-8")) return error.UnsupportedDash;
                for (attrs) |attribute| if (!std.mem.eql(u8, attribute.name, "version") and !std.mem.eql(u8, attribute.name, "encoding") and !std.mem.eql(u8, attribute.name, "standalone")) return error.UnsupportedDash;
                if (declaration_.get("standalone")) |value| if (!std.mem.eql(u8, value, "yes") and !std.mem.eql(u8, value, "no")) return error.MalformedDash;
                self.declaration = true;
                self.at += 2;
                continue;
            }
            if (self.starts("<!")) return error.UnsupportedDash;
            self.at += 1;
            if (self.at < self.bytes.len and self.bytes[self.at] == '/') {
                self.at += 1;
                const closing = try self.name();
                self.spaces();
                if (self.at == self.bytes.len or self.bytes[self.at] != '>' or self.stack.items.len == 0) return error.MalformedDash;
                const last = self.stack.pop().?;
                if (!std.mem.eql(u8, closing, self.nodes.items[last].name)) return error.MalformedDash;
                self.at += 1;
                continue;
            }
            const tag = try self.name();
            const attrs = try self.attributes();
            const empty = self.bytes[self.at] == '/';
            if (empty) self.at += 1;
            if (self.at == self.bytes.len or self.bytes[self.at] != '>') return error.MalformedDash;
            self.at += 1;
            if (self.nodes.items.len == self.limits.max_nodes or self.stack.items.len >= self.limits.max_depth) return error.ResourceLimitExceeded;
            const parent = if (self.stack.items.len != 0) self.stack.items[self.stack.items.len - 1] else null;
            if (parent == null and self.nodes.items.len != 0) return error.MalformedDash;
            try self.charge(@sizeOf(Node));
            const index = self.nodes.items.len;
            try self.nodes.append(self.a, .{ .name = tag, .attributes = attrs, .parent = parent });
            if (parent) |p| {
                if (self.nodes.items[p].last_child) |last| self.nodes.items[last].next_sibling = index else self.nodes.items[p].first_child = index;
                self.nodes.items[p].last_child = index;
            }
            if (!empty) {
                try self.charge(@sizeOf(usize));
                try self.stack.append(self.a, index);
            }
        }
        if (self.stack.items.len != 0 or self.nodes.items.len == 0) return error.MalformedDash;
    }
};

test "DASH XML tree preserves hierarchy namespaces comments CDATA and escaped references" {
    const a = std.testing.allocator;
    var tree = try Tree.init(a, "<?xml version='1.0' encoding='UTF-8'?><d:MPD xmlns:d='urn:mpeg:dash:schema:mpd:2011'><!--note--><BaseURL>https://host/<!--split-->v/<![CDATA[?x=1&y=2]]></BaseURL><Period id='p&#49;' uri='a?x=1&amp;y=2'/></d:MPD>", .{});
    defer tree.deinit();
    try std.testing.expectEqualStrings("MPD", tree.nodes[0].localName());
    try std.testing.expectEqualStrings("urn:mpeg:dash:schema:mpd:2011", try tree.namespace(0, tree.nodes[0].name, false));
    try std.testing.expectEqualStrings("", try tree.namespace(2, "id", true));
    try std.testing.expectEqualStrings("https://host/v/?x=1&y=2", tree.nodes[1].text.items);
    try std.testing.expectEqualStrings("p1", tree.nodes[2].get("id").?);
    try std.testing.expectEqualStrings("a?x=1&y=2", tree.nodes[2].get("uri").?);
    try std.testing.expectEqual(@as(?usize, 1), tree.nodes[0].first_child);
    try std.testing.expectEqual(@as(?usize, 2), tree.nodes[1].next_sibling);
}
test "DASH XML rejects expansion malformed nesting duplicate attributes and resource exhaustion" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "<MPD><Period></MPD>", "<MPD id='a' id='b'/>", "<MPD>&#0;</MPD>", "<MPD/>tail", "<MPD/><MPD/>", "<MPD><!--a--b--></MPD>", "<MPD>]]></MPD>", "<MPD><!--a---></MPD>", "<MPD>&#+49;</MPD>", "<MPD>&#x3_1;</MPD>", "<!--note--><?xml version='1.0'?><MPD/>", "<MPD>\xef\xbf\xbe</MPD>", "<x:MPD/>", "<MPD xmlns:xml='wrong'/>", "<MPD xmlns:a='urn:x' xmlns:b='urn:x' a:id='1' b:id='2'/>" }) |bytes| try std.testing.expectError(error.MalformedDash, Tree.init(a, bytes, .{}));
    try std.testing.expectError(error.UnsupportedDash, Tree.init(a, "<!DOCTYPE MPD [<!ENTITY x SYSTEM 'file:///etc/passwd'>]><MPD/>", .{}));
    try std.testing.expectError(error.UnsupportedDash, Tree.init(a, "<MPD>&custom;</MPD>", .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, Tree.init(a, "<MPD><Period/></MPD>", .{ .max_nodes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, Tree.init(a, "<MPD><Period/></MPD>", .{ .max_depth = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, Tree.init(a, "<MPD/>", .{ .max_metadata_bytes = 0 }));
}
test "DASH XML tree releases every failing allocation" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            var tree = try Tree.init(a, "<MPD><BaseURL>x&amp;y</BaseURL><Period id='p&#49;'/></MPD>", .{});
            defer tree.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
