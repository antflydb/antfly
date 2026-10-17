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

//! DASH representation selection and inherited addressing. Segment timing stays
//! in the existing representation planner. Generated references are owned.
const std = @import("std");
const xml = @import("dash_xml.zig");
const hls = @import("hls.zig");
const availability = @import("dash_availability.zig");
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
    /// Opt in only when using Reader.streamNext for incomplete available media.
    allow_partial_availability: bool = false,
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
fn baseAt(a: std.mem.Allocator, tree: xml.Tree, level: usize, current: []const u8, selection: Selection, limit: usize, offset: *availability.Offset, complete: *bool) ![]const u8 {
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
        if (std.mem.eql(u8, attribute.name, "availabilityTimeOffset")) {
            offset.* = try offset.add(try availability.parse(attribute.value));
            continue;
        }
        if (std.mem.eql(u8, attribute.name, "availabilityTimeComplete")) {
            complete.* = try availability.complete(attribute.value);
            continue;
        }
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
const PeriodPlan = struct { node: usize, start_ms: ?u64, duration_ms: ?u64, next_start_ms: ?u64 = null, next_identity: ?[32]u8 = null };
const Planned = struct { playlist: hls.Playlist, period: hls.DashPeriod, planning_bytes: usize };
fn periodIdentity(a: std.mem.Allocator, node: xml.Node, start_ms: u64) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    if (node.get("id")) |id| {
        hash.update("named/");
        hash.update(id);
    } else {
        hash.update("anonymous/");
        hash.update(try std.fmt.allocPrint(a, "{d}", .{start_ms}));
    }
    return hash.finalResult();
}
fn durationText(a: std.mem.Allocator, milliseconds: u64) ![]const u8 {
    return std.fmt.allocPrint(a, "PT{d}.{d:0>3}S", .{ milliseconds / 1000, milliseconds % 1000 });
}
fn planPeriod(a: std.mem.Allocator, scratch: std.mem.Allocator, tree: xml.Tree, plan: PeriodPlan, limits: hls.Limits, now: ?i64, selection: Selection, comptime parse_one: anytype) !Planned {
    const p = plan.node;
    var adaptation: ?usize = null;
    var representation: ?usize = null;
    var selected_bandwidth: u64 = 0;
    var next = tree.nodes[p].first_child;
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
    var availability_offset: availability.Offset = .{ .finite = 0 };
    var availability_complete = true;
    for (levels) |level| {
        base = try baseAt(scratch, tree, level, base, selection, limits.max_uri_bytes, &availability_offset, &availability_complete);
        if (level != 0) try addressing.inherit(scratch, tree, level);
        var child = tree.nodes[level].first_child;
        while (child) |index| : (child = tree.nodes[index].next_sibling) {
            if ((try named(tree, index, "ContentProtection")) or (try named(tree, index, "EssentialProperty"))) return error.UnsupportedDash;
        }
    }
    // Effective SegmentTemplate attributes override their parent declaration;
    // each selected BaseURL contributes independently along the chosen path.
    for (addressing.attributes.items) |*attribute| if (std.mem.eql(u8, attribute.name, "availabilityTimeOffset")) {
        availability_offset = try availability_offset.add(try availability.parse(attribute.value));
        attribute.value = try availability_offset.text(scratch);
        break;
    };
    var has_offset = false;
    for (addressing.attributes.items) |attribute| if (std.mem.eql(u8, attribute.name, "availabilityTimeOffset")) {
        has_offset = true;
    };
    if (!has_offset and (availability_offset == .infinite or availability_offset.finite != 0))
        try addressing.attributes.append(scratch, .{ .name = "availabilityTimeOffset", .value = try availability_offset.text(scratch) });
    var has_complete = false;
    for (addressing.attributes.items) |attribute| if (std.mem.eql(u8, attribute.name, "availabilityTimeComplete")) {
        availability_complete = try availability.complete(attribute.value);
        has_complete = true;
    };
    if (!availability_complete and !selection.allow_partial_availability) return error.UnsupportedDash;
    if (!has_complete and !availability_complete)
        try addressing.attributes.append(scratch, .{ .name = "availabilityTimeComplete", .value = "false" });
    var output: std.ArrayList(u8) = .empty;
    try output.appendSlice(scratch, "<MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\"");
    try attrs(scratch, &output, tree.nodes[0].attributes);
    try output.append(scratch, '>');
    var period_attributes: std.ArrayList(xml.Attribute) = .empty;
    for (tree.nodes[p].attributes) |attribute| {
        if (!std.mem.eql(u8, attribute.name, "start") and !std.mem.eql(u8, attribute.name, "duration")) try period_attributes.append(scratch, attribute);
    }
    try period_attributes.append(scratch, .{ .name = "start", .value = try durationText(scratch, plan.start_ms.?) });
    if (plan.duration_ms) |duration| try period_attributes.append(scratch, .{ .name = "duration", .value = try durationText(scratch, duration) });
    try start(scratch, &output, "Period", period_attributes.items);
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
    if (output.items.len > limits.max_bytes or output.items.len > limits.max_planning_bytes) return error.ResourceLimitExceeded;
    var parsed = try parse_one(a, scratch, output.items, limits, now, selection.allow_partial_availability);
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
    const period_identity = try periodIdentity(scratch, tree.nodes[p], plan.start_ms.?);
    const metadata = hls.DashPeriod{ .identity = period_identity, .configuration = identity.finalResult(), .start_ms = plan.start_ms.?, .duration_ms = plan.duration_ms, .next_start_ms = plan.next_start_ms, .next_identity = plan.next_identity };
    for (parsed.segments, 0..) |*segment, index| {
        segment.dash.?.period = metadata;
        if (parsed.ended and plan.duration_ms == null and index + 1 == parsed.segments.len) segment.dash.?.final = true;
    }
    return .{ .playlist = try owned(a, scratch, parsed, base, limits), .period = metadata, .planning_bytes = output.items.len };
}

pub fn parse(a: std.mem.Allocator, bytes: []const u8, limits: hls.Limits, now: ?i64, selection: Selection, comptime parse_one: anytype, comptime elapsed: anytype, comptime utc: anytype) !hls.Playlist {
    var tree = try xml.Tree.init(a, bytes, .{ .max_bytes = limits.max_bytes });
    defer tree.deinit();
    if (!(try named(tree, 0, "MPD"))) return error.MalformedDash;
    for (tree.nodes, 0..) |node, index| for (node.attributes) |attribute| {
        if (std.mem.eql(u8, attribute.name, "xsi:schemaLocation") and !std.mem.eql(u8, try tree.namespace(index, attribute.name, true), "http://www.w3.org/2001/XMLSchema-instance")) return error.UnsupportedDash;
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var plans: std.ArrayList(PeriodPlan) = .empty;
    var next = tree.nodes[0].first_child;
    var ids: std.StringHashMap(void) = .init(scratch);
    while (next) |index| : (next = tree.nodes[index].next_sibling) if (try named(tree, index, "Period")) {
        if (plans.items.len >= limits.max_periods) return error.ResourceLimitExceeded;
        const node = tree.nodes[index];
        if (node.get("id")) |id| {
            const entry = try ids.getOrPut(id);
            if (entry.found_existing) return error.DashSelectionAmbiguous;
        }
        try plans.append(scratch, .{ .node = index, .start_ms = if (node.get("start")) |value| try elapsed(value) else null, .duration_ms = if (node.get("duration")) |value| try elapsed(value) else null });
    };
    if (plans.items.len == 0) return error.DashSelectionNotFound;
    const presentation_duration = if (tree.nodes[0].get("mediaPresentationDuration")) |value| try elapsed(value) else null;
    if (plans.items[0].start_ms == null) plans.items[0].start_ms = 0;
    // Each start follows a known preceding duration, or a following explicit
    // start determines the preceding duration. Never infer from segment counts.
    for (plans.items, 0..) |*plan, index| {
        if (index != 0 and plan.start_ms == null) {
            const previous = plans.items[index - 1];
            plan.start_ms = std.math.add(u64, previous.start_ms.?, previous.duration_ms orelse return error.MalformedDash) catch return error.MalformedDash;
        }
        if (plan.duration_ms == null) {
            if (index + 1 < plans.items.len) {
                if (plans.items[index + 1].start_ms) |following| {
                    if (following <= plan.start_ms.?) return error.MalformedDash;
                    plan.duration_ms = following - plan.start_ms.?;
                } else return error.MalformedDash;
            } else if (presentation_duration) |end| {
                if (end <= plan.start_ms.?) return error.MalformedDash;
                plan.duration_ms = end - plan.start_ms.?;
            }
        }
        if (plan.duration_ms) |duration| {
            if (duration == 0) return error.MalformedDash;
            const end = std.math.add(u64, plan.start_ms.?, duration) catch return error.MalformedDash;
            if (presentation_duration != null and end > presentation_duration.?) return error.MalformedDash;
            if (index + 1 < plans.items.len and plans.items[index + 1].start_ms != null and end > plans.items[index + 1].start_ms.?) return error.MalformedDash;
        }
    }
    for (plans.items, 0..) |*plan, index| if (index + 1 < plans.items.len) {
        const following = plans.items[index + 1];
        plan.next_start_ms = following.start_ms.?;
        plan.next_identity = try periodIdentity(scratch, tree.nodes[following.node], following.start_ms.?);
    };
    var planned: std.ArrayList(Planned) = .empty;
    defer for (planned.items) |*item| item.playlist.deinit();
    var all_segments: std.ArrayList(hls.Segment) = .empty;
    var all_periods: std.ArrayList(hls.DashPeriod) = .empty;
    var target: u64 = 1;
    var reload: ?f64 = null;
    var uri_bytes: usize = 0;
    var planning_bytes: usize = 0;
    const dynamic = std.mem.eql(u8, tree.nodes[0].get("type") orelse "static", "dynamic");
    for (plans.items) |plan| {
        if (!matches(tree.nodes[plan.node], selection.period_id)) continue;
        var remaining = limits;
        remaining.max_segments -= all_segments.items.len;
        remaining.max_planning_bytes -= planning_bytes;
        // Canonical XML and inherited addressing can repeat large MPD-level
        // attributes. Reclaim each period's temporary storage immediately.
        var period_arena = std.heap.ArenaAllocator.init(a);
        defer period_arena.deinit();
        var item = try planPeriod(a, period_arena.allocator(), tree, plan, remaining, now, selection, parse_one);
        planned.append(scratch, item) catch |err| {
            item.playlist.deinit();
            return err;
        };
        for (item.playlist.owned_uris) |uri| {
            uri_bytes = std.math.add(usize, uri_bytes, uri.len) catch return error.ResourceLimitExceeded;
            if (uri_bytes > limits.max_bytes) return error.ResourceLimitExceeded;
        }
        planning_bytes += item.planning_bytes;
        try all_segments.appendSlice(scratch, item.playlist.segments);
        try all_periods.append(scratch, item.period);
        target = @max(target, item.playlist.target_duration_seconds);
        reload = item.playlist.reload_after_seconds;
    }
    if (planned.items.len == 0) return error.DashSelectionNotFound;
    var root_identity = std.crypto.hash.sha2.Sha256.init(.{});
    root_identity.update("dash-presentation-v2/");
    root_identity.update(try std.fmt.allocPrint(scratch, "{d}", .{if (tree.nodes[0].get("availabilityStartTime")) |value| try utc(value) else @as(i64, -1)}));
    root_identity.update(&.{0});
    root_identity.update(tree.nodes[0].get("id") orelse "");
    const joined = hls.Playlist{ .allocator = a, .segments = all_segments.items, .target_duration_seconds = target, .ended = !dynamic, .reload_after_seconds = reload, .timing_identity = root_identity.finalResult(), .dash_window_start_ms = planned.items[0].playlist.dash_window_start_ms };
    var result = try owned(a, scratch, joined, "", limits);
    errdefer result.deinit();
    result.dash_periods = try a.dupe(hls.DashPeriod, all_periods.items);
    return result;
}
