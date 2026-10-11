// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! DASH representation selection and inherited addressing. Segment timing stays
//! in the existing representation planner. Generated references are owned.
const std = @import("std");
const xml = @import("dash_xml.zig");
const hls = @import("hls.zig");
const ns = "urn:mpeg:dash:schema:mpd:2011";
pub const Selection = struct {
    period_id: ?[]const u8 = null,
    adaptation_id: ?[]const u8 = null,
    representation_id: ?[]const u8 = null,
    max_bandwidth: ?u64 = null,
    base_url_index: usize = 0,
    /// Required when resolving relative BaseURL hierarchies. Live readers supply
    /// their actual manifest URL; authenticated transport still pins the origin.
    manifest_url: ?[]const u8 = null,
};
fn named(tree: xml.Tree, index: usize, name: []const u8) !bool {
    const node = tree.nodes[index];
    if (!std.mem.eql(u8, node.localName(), name)) return false;
    if (!std.mem.eql(u8, try tree.namespace(index, node.name, false), ns)) return error.UnsupportedDash;
    return true;
}
fn matches(node: xml.Node, id: ?[]const u8) bool {
    return if (id) |wanted| if (node.get("id")) |actual| std.mem.eql(u8, wanted, actual) else false else true;
}
fn video(node: xml.Node, parent: xml.Node) bool {
    const mime = node.get("mimeType") orelse parent.get("mimeType");
    const content = node.get("contentType") orelse parent.get("contentType");
    return (mime == null or std.mem.eql(u8, mime.?, "video/mp4")) and (content == null or std.mem.eql(u8, content.?, "video"));
}
fn escape(a: std.mem.Allocator, output: *std.ArrayList(u8), text: []const u8) !void {
    for (text) |byte| try output.appendSlice(a, switch (byte) {
        '&' => "&amp;",
        '<' => "&lt;",
        '>' => "&gt;",
        '"' => "&quot;",
        else => &.{byte},
    });
}
fn attrs(a: std.mem.Allocator, output: *std.ArrayList(u8), attributes: []const xml.Attribute) !void {
    for (attributes) |attribute| {
        if (std.mem.eql(u8, attribute.name, "xmlns") or std.mem.startsWith(u8, attribute.name, "xmlns:")) continue;
        // Schema-location hints are metadata, never fetched by this parser.
        if (std.mem.eql(u8, attribute.name, "xsi:schemaLocation")) continue;
        if (std.mem.indexOfScalar(u8, attribute.name, ':') != null) return error.UnsupportedDash;
        try output.append(a, ' ');
        try output.appendSlice(a, attribute.name);
        try output.appendSlice(a, "=\"");
        try escape(a, output, attribute.value);
        try output.append(a, '"');
    }
}
fn start(a: std.mem.Allocator, output: *std.ArrayList(u8), name: []const u8, attributes: []const xml.Attribute) !void {
    try output.append(a, '<');
    try output.appendSlice(a, name);
    try attrs(a, output, attributes);
    try output.append(a, '>');
}
fn subtree(a: std.mem.Allocator, output: *std.ArrayList(u8), tree: xml.Tree, index: usize) !void {
    const node = tree.nodes[index];
    if (!std.mem.eql(u8, try tree.namespace(index, node.name, false), ns)) return error.UnsupportedDash;
    try output.append(a, '<');
    try output.appendSlice(a, node.localName());
    try attrs(a, output, node.attributes);
    if (node.first_child == null) {
        if (std.mem.trim(u8, node.text.items, " \t\r\n").len != 0) return error.MalformedDash;
        try output.appendSlice(a, "/>");
        return;
    }
    try output.append(a, '>');
    if (std.mem.trim(u8, node.text.items, " \t\r\n").len != 0) return error.MalformedDash;
    var child = node.first_child;
    while (child) |next| : (child = tree.nodes[next].next_sibling) try subtree(a, output, tree, next);
    try output.appendSlice(a, "</");
    try output.appendSlice(a, node.localName());
    try output.append(a, '>');
}
fn resolve(a: std.mem.Allocator, base: []const u8, reference: []const u8, limit: usize) ![]const u8 {
    if (base.len > limit or reference.len > limit) return error.ResourceLimitExceeded;
    for (reference) |byte| if (byte <= 0x20 or byte == 0x7f or byte == '\\') return error.InvalidMediaUrl;
    if (base.len == 0) {
        const absolute = std.Uri.parse(reference) catch return error.DashManifestUrlRequired;
        if (absolute.user != null or absolute.password != null or absolute.fragment != null or absolute.host == null or (!std.ascii.eqlIgnoreCase(absolute.scheme, "http") and !std.ascii.eqlIgnoreCase(absolute.scheme, "https"))) return error.InvalidMediaUrl;
        return reference;
    }
    const base_uri = try std.Uri.parse(base);
    const storage = try a.alloc(u8, try std.math.add(usize, try std.math.mul(usize, limit, 3), 32));
    @memcpy(storage[0..reference.len], reference);
    var remaining = storage;
    const resolved = try std.Uri.resolveInPlace(base_uri, reference.len, &remaining);
    if (resolved.user != null or resolved.password != null or resolved.fragment != null or resolved.host == null or (!std.ascii.eqlIgnoreCase(resolved.scheme, "http") and !std.ascii.eqlIgnoreCase(resolved.scheme, "https"))) return error.InvalidMediaUrl;
    const result = try std.fmt.allocPrint(a, "{f}", .{resolved});
    if (result.len > limit) return error.ResourceLimitExceeded;
    return result;
}
const Addressing = struct {
    kind: ?[]const u8 = null,
    attributes: std.ArrayList(xml.Attribute) = .empty,
    timeline: ?usize = null,
    initialization: ?usize = null,
    urls: ?usize = null,
    fn inherit(self: *Addressing, a: std.mem.Allocator, tree: xml.Tree, level: usize) !void {
        var child = tree.nodes[level].first_child;
        var seen = false;
        while (child) |index| : (child = tree.nodes[index].next_sibling) {
            if (try named(tree, index, "SegmentBase")) return error.UnsupportedDash;
            if (!(try named(tree, index, "SegmentTemplate")) and !(try named(tree, index, "SegmentList"))) continue;
            if (seen) return error.MalformedDash;
            seen = true;
            const kind = tree.nodes[index].localName();
            if (self.kind != null and !std.mem.eql(u8, self.kind.?, kind)) {
                self.attributes.clearRetainingCapacity();
                self.timeline = null;
                self.initialization = null;
                self.urls = null;
            }
            self.kind = kind;
            for (tree.nodes[index].attributes) |attribute| {
                for (self.attributes.items) |*old| {
                    if (std.mem.eql(u8, old.name, attribute.name)) {
                        old.* = attribute;
                        break;
                    }
                } else try self.attributes.append(a, attribute);
            }
            var member = tree.nodes[index].first_child;
            var timeline_seen = false;
            var init_seen = false;
            var urls_seen = false;
            while (member) |item| : (member = tree.nodes[item].next_sibling) {
                if (try named(tree, item, "SegmentTimeline")) {
                    if (timeline_seen) return error.MalformedDash;
                    timeline_seen = true;
                    self.timeline = item;
                } else if (try named(tree, item, "Initialization")) {
                    if (init_seen) return error.MalformedDash;
                    init_seen = true;
                    self.initialization = item;
                } else if (try named(tree, item, "SegmentURL")) {
                    if (!urls_seen) self.urls = index;
                    urls_seen = true;
                } else return error.UnsupportedDash;
            }
        }
    }
};
fn baseAt(a: std.mem.Allocator, tree: xml.Tree, level: usize, current: []const u8, selection: Selection, limit: usize) ![]const u8 {
    var child = tree.nodes[level].first_child;
    var count: usize = 0;
    var chosen: ?usize = null;
    while (child) |index| : (child = tree.nodes[index].next_sibling) if (try named(tree, index, "BaseURL")) {
        if (count == selection.base_url_index) chosen = index;
        count += 1;
    };
    if (count == 0) return current;
    if (count == 1) {
        child = tree.nodes[level].first_child;
        while (child) |index| : (child = tree.nodes[index].next_sibling) if (try named(tree, index, "BaseURL")) {
            chosen = index;
            break;
        };
    }
    const node = tree.nodes[chosen orelse return error.DashSelectionNotFound];
    if (node.first_child != null) return error.MalformedDash;
    for (node.attributes) |attribute| {
        if (std.mem.eql(u8, attribute.name, "serviceLocation")) continue;
        if (std.mem.eql(u8, attribute.name, "availabilityTimeOffset") and std.mem.eql(u8, attribute.value, "0")) continue;
        if (std.mem.eql(u8, attribute.name, "availabilityTimeComplete") and std.mem.eql(u8, attribute.value, "true")) continue;
        return error.UnsupportedDash;
    }
    const reference = std.mem.trim(u8, node.text.items, " \t\r\n");
    if (reference.len == 0) return error.MalformedDash;
    return resolve(a, current, reference, limit);
}
fn owned(a: std.mem.Allocator, arena: std.mem.Allocator, parsed: hls.Playlist, base: []const u8, limits: hls.Limits) !hls.Playlist {
    var result = parsed;
    const segments = try a.dupe(hls.Segment, parsed.segments);
    errdefer a.free(segments);
    var values: std.ArrayList([]u8) = .empty;
    errdefer {
        for (values.items) |value| a.free(value);
        values.deinit(a);
    }
    var cache: std.StringHashMap([]const u8) = .init(arena);
    var used: usize = 0;
    for (segments) |*segment| {
        const references = [_]*[]const u8{ &segment.uri, &segment.initialization.uri };
        for (references) |destination| {
            const uri = if (base.len != 0) try resolve(arena, base, destination.*, limits.max_uri_bytes) else destination.*;
            if (cache.get(uri)) |value| destination.* = value else {
                used = try std.math.add(usize, used, uri.len);
                if (used > limits.max_bytes) return error.ResourceLimitExceeded;
                const copy = try a.dupe(u8, uri);
                values.append(a, copy) catch |err| {
                    a.free(copy);
                    return err;
                };
                try cache.put(uri, copy);
                destination.* = copy;
            }
        }
    }
    result.segments = segments;
    result.owned_uris = try values.toOwnedSlice(a);
    return result;
}
pub fn parse(a: std.mem.Allocator, bytes: []const u8, limits: hls.Limits, now: ?i64, selection: Selection, comptime parse_one: anytype) !hls.Playlist {
    var tree = try xml.Tree.init(a, bytes, .{ .max_bytes = limits.max_bytes });
    defer tree.deinit();
    if (!(try named(tree, 0, "MPD"))) return error.MalformedDash;
    for (tree.nodes, 0..) |node, index| for (node.attributes) |attribute| {
        if (std.mem.eql(u8, attribute.name, "xsi:schemaLocation") and !std.mem.eql(u8, try tree.namespace(index, attribute.name, true), "http://www.w3.org/2001/XMLSchema-instance")) return error.UnsupportedDash;
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var period: ?usize = null;
    var periods: usize = 0;
    var next = tree.nodes[0].first_child;
    while (next) |index| : (next = tree.nodes[index].next_sibling) if (try named(tree, index, "Period")) {
        periods += 1;
        if (matches(tree.nodes[index], selection.period_id)) {
            if (period != null and selection.period_id != null) return error.DashSelectionAmbiguous;
            if (period == null) period = index;
        }
    };
    if (periods > 1 and selection.period_id == null) return error.UnsupportedDash;
    const p = period orelse return error.DashSelectionNotFound;
    var adaptation: ?usize = null;
    var representation: ?usize = null;
    var selected_bandwidth: u64 = 0;
    next = tree.nodes[p].first_child;
    while (next) |index| : (next = tree.nodes[index].next_sibling) {
        if (!(try named(tree, index, "AdaptationSet")) or !matches(tree.nodes[index], selection.adaptation_id) or !video(tree.nodes[index], tree.nodes[index])) continue;
        var child = tree.nodes[index].first_child;
        while (child) |r| : (child = tree.nodes[r].next_sibling) {
            if (!(try named(tree, r, "Representation")) or !matches(tree.nodes[r], selection.representation_id) or !video(tree.nodes[r], tree.nodes[index])) continue;
            var bandwidth: u64 = 0;
            if (selection.max_bandwidth) |maximum| {
                bandwidth = std.fmt.parseInt(u64, tree.nodes[r].get("bandwidth") orelse continue, 10) catch return error.MalformedDash;
                if (bandwidth > maximum) continue;
            }
            if (representation != null) {
                if (selection.representation_id != null) return error.DashSelectionAmbiguous;
                if (selection.max_bandwidth == null or bandwidth <= selected_bandwidth) continue;
            }
            adaptation = index;
            representation = r;
            selected_bandwidth = bandwidth;
        }
    }
    const levels = [_]usize{ 0, p, adaptation orelse return error.DashSelectionNotFound, representation orelse return error.DashSelectionNotFound };
    var base: []const u8 = selection.manifest_url orelse "";
    var addressing = Addressing{};
    for (levels) |level| {
        base = try baseAt(scratch, tree, level, base, selection, limits.max_uri_bytes);
        if (level != 0) try addressing.inherit(scratch, tree, level);
        var child = tree.nodes[level].first_child;
        while (child) |index| : (child = tree.nodes[index].next_sibling) {
            if ((try named(tree, index, "ContentProtection")) or (try named(tree, index, "EssentialProperty"))) return error.UnsupportedDash;
        }
    }
    var output: std.ArrayList(u8) = .empty;
    try output.appendSlice(scratch, "<MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\"");
    try attrs(scratch, &output, tree.nodes[0].attributes);
    try output.append(scratch, '>');
    try start(scratch, &output, "Period", tree.nodes[p].attributes);
    try start(scratch, &output, "AdaptationSet", tree.nodes[levels[2]].attributes);
    try start(scratch, &output, "Representation", tree.nodes[levels[3]].attributes);
    try start(scratch, &output, addressing.kind orelse return error.MalformedDash, addressing.attributes.items);
    if (addressing.timeline) |index| try subtree(scratch, &output, tree, index);
    if (addressing.initialization) |index| try subtree(scratch, &output, tree, index);
    if (addressing.urls) |index| {
        var child = tree.nodes[index].first_child;
        while (child) |item| : (child = tree.nodes[item].next_sibling) if (try named(tree, item, "SegmentURL")) try subtree(scratch, &output, tree, item);
    }
    try output.appendSlice(scratch, "</");
    try output.appendSlice(scratch, addressing.kind.?);
    try output.appendSlice(scratch, "></Representation></AdaptationSet></Period></MPD>");
    if (output.items.len > limits.max_bytes) return error.ResourceLimitExceeded;
    var parsed = try parse_one(a, scratch, output.items, limits, now);
    defer parsed.deinit();
    // A reordered representation list or a changed codec/configuration cannot
    // silently switch a live reader when its default selection is used.
    var identity = std.crypto.hash.sha2.Sha256.init(.{});
    identity.update(&parsed.timing_identity.?);
    for (levels[1..]) |level| {
        for ([_][]const u8{ "id", "mimeType", "contentType", "codecs", "bandwidth", "width", "height", "frameRate", "sar" }) |name| {
            const value = tree.nodes[level].get(name) orelse "";
            identity.update(name);
            identity.update(&.{0});
            identity.update(value);
            identity.update(&.{0});
        }
    }
    identity.update(addressing.kind.?);
    for ([_][]const u8{ "media", "initialization" }) |name| {
        identity.update(&.{0});
        for (addressing.attributes.items) |attribute| if (std.mem.eql(u8, name, attribute.name)) {
            identity.update(attribute.value);
            break;
        };
    }
    parsed.timing_identity = identity.finalResult();
    return owned(a, scratch, parsed, base, limits);
}
