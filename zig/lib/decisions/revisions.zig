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

//! Bounded, flat execution dependencies. Paths are relative to the reported
//! Decisions invocation. No recursive upstream traces are retained.
const std = @import("std");
const V = std.json.Value;
pub const max_nodes = 32;
pub const max_depth = 8;
pub const Node = struct {
    kind: enum { model, cascade, unverified },
    path: []const []const u8,
    identity: []const u8,
    model: ?[]const u8 = null,
};
pub const Manifest = struct {
    nodes: [max_nodes]Node = undefined,
    paths: [max_nodes][max_depth][]const u8 = undefined,
    len: usize = 0,
    visited: usize = 0,
    historical: bool = false,

    pub fn items(self: *const Manifest) []const Node {
        return self.nodes[0..self.len];
    }
    fn add(self: *Manifest, node: Node) !void {
        if (self.len == max_nodes or node.path.len > max_depth) return error.InvalidDecisionOutput;
        try validateNode(node);
        for (self.items()) |old| {
            if (old.kind != node.kind or old.path.len != node.path.len) continue;
            var equal = true;
            for (old.path, node.path) |l, r| equal = equal and std.mem.eql(u8, l, r);
            if (equal) return error.InvalidDecisionOutput;
        }
        @memcpy(self.paths[self.len][0..node.path.len], node.path);
        self.nodes[self.len] = node;
        self.nodes[self.len].path = self.paths[self.len][0..node.path.len];
        self.len += 1;
    }
    pub fn markIncomplete(self: *Manifest) !void {
        try self.add(.{ .kind = .unverified, .path = &.{}, .identity = "0000000000000000000000000000000000000000000000000000000000000000" });
    }
    pub fn collect(self: *Manifest, response: V) !void {
        self.historical = false;
        try self.collectInner(response);
    }
    /// Binding recognition only: reproduce the pre-versioned projection digest.
    /// This result must never authorize replay or be sent to a verifier.
    pub fn collectHistorical(self: *Manifest, response: V) !void {
        self.historical = true;
        try self.collectInner(response);
    }
    fn collectInner(self: *Manifest, response: V) !void {
        self.len = 0;
        self.visited = 0;
        try self.walk(response, &.{});
    }
    fn walk(self: *Manifest, response: V, path: []const []const u8) !void {
        self.visited += 1;
        if (self.visited > 128) return error.InvalidDecisionOutput;
        if (response != .object) return error.InvalidDecisionOutput;
        const version = response.object.get("upstream_revisions_version");
        if (version) |v| if (v != .integer or v.integer != 1 or !response.object.contains("upstream_revisions")) return error.InvalidDecisionOutput;
        if (response.object.get("upstream_revisions")) |flat| {
            // A stage projection replaces hidden upstream provenance. It must
            // never override a supplied public trace, even with an empty list.
            if (response.object.contains("cascade")) return error.InvalidDecisionOutput;
            const first = self.len;
            if (flat != .array or flat.array.items.len > max_nodes) return error.InvalidDecisionOutput;
            for (flat.array.items) |entry| {
                if (entry != .object) return error.InvalidDecisionOutput;
                const kind = entry.object.get("kind") orelse return error.InvalidDecisionOutput;
                const identity = entry.object.get("identity") orelse return error.InvalidDecisionOutput;
                const relative = entry.object.get("path") orelse return error.InvalidDecisionOutput;
                if (kind != .string or identity != .string or relative != .array or path.len + relative.array.items.len > max_depth) return error.InvalidDecisionOutput;
                var joined: [max_depth][]const u8 = undefined;
                @memcpy(joined[0..path.len], path);
                for (relative.array.items, path.len..) |part, i| {
                    if (part != .string) return error.InvalidDecisionOutput;
                    joined[i] = part.string;
                }
                const model = entry.object.get("model");
                if (model != null and model.? != .string) return error.InvalidDecisionOutput;
                try self.add(.{ .kind = std.meta.stringToEnum(@FieldType(Node, "kind"), kind.string) orelse return error.InvalidDecisionOutput, .identity = identity.string, .path = joined[0 .. path.len + relative.array.items.len], .model = if (model) |m| m.string else null });
            }
            // A projection cannot erase the final generated revision that is
            // still visible in the stage's public answer.
            if (response.object.get("model_revision")) |revision| {
                if (revision != .string) return error.InvalidDecisionOutput;
                try validateNode(.{ .kind = .cascade, .path = &.{}, .identity = revision.string });
                const answers = response.object.get("answers") orelse return error.InvalidDecisionOutput;
                if (answers != .array) return error.InvalidDecisionOutput;
                var generated = false;
                for (answers.array.items) |answer| {
                    if (answer != .object) return error.InvalidDecisionOutput;
                    const method = answer.object.get("decision_method") orelse return error.InvalidDecisionOutput;
                    if (method != .string) return error.InvalidDecisionOutput;
                    generated = generated or std.mem.eql(u8, method.string, "generated");
                }
                if (generated) {
                    const model = response.object.get("model") orelse return error.InvalidDecisionOutput;
                    if (model != .string) return error.InvalidDecisionOutput;
                    var found = false;
                    for (self.nodes[first..self.len]) |node| if (node.kind == .model and std.mem.eql(u8, node.identity, revision.string) and std.mem.eql(u8, node.model.?, model.string)) {
                        found = true;
                    };
                    if (!found) return error.InvalidDecisionOutput;
                }
            }
            if (!self.historical) {
                var incomplete = false;
                var definition = false;
                var reported: ?[]const u8 = null;
                if (response.object.get("upstream_agreement")) |summary| {
                    if (summary != .object) return error.InvalidDecisionOutput;
                    if (summary.object.get("definition_sha256")) |identity| {
                        if (identity != .string) return error.InvalidDecisionOutput;
                        try validateNode(.{ .kind = .cascade, .path = &.{}, .identity = identity.string });
                        reported = identity.string;
                    }
                }
                for (self.nodes[first..self.len]) |node| {
                    if (node.path.len != path.len) continue;
                    if (node.kind == .unverified) incomplete = true;
                    if (node.kind == .cascade) {
                        if (reported) |identity| if (!std.mem.eql(u8, node.identity, identity)) return error.InvalidDecisionOutput;
                        definition = true;
                    }
                }
                if (version != null and response.object.contains("upstream_agreement") and !definition and !incomplete) return error.InvalidDecisionOutput;
                // Historical projections could drop definition-only subtrees,
                // even while retaining other models. Nonempty is not proof.
                if (version == null and !incomplete) try self.add(.{ .kind = .unverified, .path = path, .identity = reported orelse "0000000000000000000000000000000000000000000000000000000000000000" });
            }
            return;
        }
        if (response.object.get("cascade")) |cascade| {
            if (cascade != .object) return error.InvalidDecisionOutput;
            if (cascade.object.get("stages")) |stages| {
                if (stages != .array or stages.array.items.len == 0 or stages.array.items.len > 4 or path.len == max_depth) return error.InvalidDecisionOutput;
                const identity = cascade.object.get("definition_sha256") orelse return error.InvalidDecisionOutput;
                if (identity != .string) return error.InvalidDecisionOutput;
                const before = self.len;
                for (stages.array.items) |stage| {
                    if (stage != .object) return error.InvalidDecisionOutput;
                    if (!stage.object.contains("answers")) continue;
                    const name = stage.object.get("name") orelse return error.InvalidDecisionOutput;
                    if (name != .string) return error.InvalidDecisionOutput;
                    var next: [max_depth][]const u8 = undefined;
                    @memcpy(next[0..path.len], path);
                    next[path.len] = name.string;
                    try self.walk(stage, next[0 .. path.len + 1]);
                }
                if (!self.historical or self.len != before) try self.add(.{ .kind = .cascade, .path = path, .identity = identity.string });
                return;
            }
            try self.markIncomplete();
        }
        // Old projected traces discarded hidden dependencies. Never interpret
        // that missing provenance as proof of an unversioned upstream.
        if (response.object.get("upstream_agreement")) |summary| {
            if (summary != .object) return error.InvalidDecisionOutput;
            if (summary.object.get("definition_sha256")) |identity| {
                if (identity != .string) return error.InvalidDecisionOutput;
                try self.add(.{ .kind = .unverified, .path = path, .identity = identity.string });
            } else try self.add(.{ .kind = .unverified, .path = path, .identity = "0000000000000000000000000000000000000000000000000000000000000000" });
            return;
        }
        if (response.object.contains("name")) {
            const method = response.object.get("decision_method") orelse return error.InvalidDecisionOutput;
            if (method != .string) return error.InvalidDecisionOutput;
            if (std.mem.eql(u8, method.string, "typed")) {
                // Historical typed stages may have been Decisions proxies;
                // their hidden upstream trace was discarded without a marker.
                try self.add(.{ .kind = .unverified, .path = path, .identity = "0000000000000000000000000000000000000000000000000000000000000000" });
            }
        }
        const answers = response.object.get("answers") orelse return error.InvalidDecisionOutput;
        if (answers != .array) return error.InvalidDecisionOutput;
        var generated = false;
        for (answers.array.items) |answer| {
            if (answer != .object) return error.InvalidDecisionOutput;
            const method = answer.object.get("decision_method") orelse return error.InvalidDecisionOutput;
            if (method != .string) return error.InvalidDecisionOutput;
            generated = generated or std.mem.eql(u8, method.string, "generated");
        }
        if (generated) if (response.object.get("model_revision")) |identity| {
            const model = response.object.get("model") orelse return error.InvalidDecisionOutput;
            if (identity != .string or model != .string) return error.InvalidDecisionOutput;
            try self.add(.{ .kind = .model, .path = path, .identity = identity.string, .model = model.string });
        };
    }
    pub fn digest(self: *const Manifest, selectors_only: bool) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("decision-dependencies-v1");
        for (self.items()) |node| {
            hashText(&hash, @tagName(node.kind));
            var depth: [4]u8 = undefined;
            std.mem.writeInt(u32, &depth, @intCast(node.path.len), .little);
            hash.update(&depth);
            for (node.path) |part| hashText(&hash, part);
            hashText(&hash, node.model orelse "");
            hashText(&hash, if (!selectors_only or node.kind != .model) node.identity else "");
        }
        var out: [32]u8 = undefined;
        hash.final(&out);
        return out;
    }
    pub fn value(self: *const Manifest, a: std.mem.Allocator) !V {
        return std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, self.items(), .{ .emit_null_optional_fields = false }), .{});
    }
};
fn hashText(hash: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(value.len), .little);
    hash.update(&len);
    hash.update(value);
}
pub fn validateNode(node: Node) !void {
    if (node.path.len > max_depth or node.identity.len != 64) return error.InvalidDecisionOutput;
    for (node.identity) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDecisionOutput;
    for (node.path) |part| {
        if (part.len == 0 or part.len > 64) return error.InvalidDecisionOutput;
        for (part) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return error.InvalidDecisionOutput;
    }
    if (node.kind == .model) {
        const model = node.model orelse return error.InvalidDecisionOutput;
        if (model.len == 0 or model.len > 2048 or !std.unicode.utf8ValidateSlice(model)) return error.InvalidDecisionOutput;
    } else if (node.model != null) return error.InvalidDecisionOutput;
}

test "flat dependencies preserve generated stages before typed finals and upstream definitions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var input = try std.json.parseFromSliceLeaky(V, a,
        \\{"model":"typed-final","answers":[{"decision_method":"typed"}],"cascade":{"definition_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","stages":[{"name":"fast","decision_method":"generated","model":"gen","model_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","answers":[{"decision_method":"generated"}]},{"name":"strong","decision_method":"typed","upstream_revisions_version":1,"upstream_revisions":[],"model":"typed-final","answers":[{"decision_method":"typed"}]}]}}
    , .{});
    var manifest: Manifest = .{};
    try manifest.collect(input);
    try std.testing.expectEqual(@as(usize, 2), manifest.len);
    try std.testing.expectEqualStrings("fast", manifest.nodes[0].path[0]);
    const original = manifest.digest(false);
    // Neither an empty nor populated root projection can shadow a full trace.
    try input.object.put(a, "upstream_revisions", .{ .array = .init(a) });
    var conflicting: Manifest = .{};
    try std.testing.expectError(error.InvalidDecisionOutput, conflicting.collect(input));
    try input.object.put(a, "upstream_revisions", try manifest.value(a));
    try std.testing.expectError(error.InvalidDecisionOutput, conflicting.collect(input));
    _ = input.object.swapRemove("upstream_revisions");
    try conflicting.collect(input);
    try std.testing.expectEqualSlices(u8, &original, &conflicting.digest(false));
    var flat = try manifest.value(a);
    var projected = V{ .object = .empty };
    try projected.object.put(a, "upstream_revisions", flat);
    try projected.object.put(a, "upstream_revisions_version", .{ .integer = 1 });
    var replay: Manifest = .{};
    try replay.collect(projected);
    try std.testing.expectEqualSlices(u8, &original, &replay.digest(false));
    // Collection uses bounded stack storage, independent of the response arena.
    for (0..256) |_| try replay.collect(projected);
    // Duplicate selectors and malformed paths are invalid, even at equal revisions.
    try flat.array.append(flat.array.items[0]);
    try projected.object.put(a, "upstream_revisions", flat);
    try projected.object.put(a, "upstream_revisions_version", .{ .integer = 1 });
    try std.testing.expectError(error.InvalidDecisionOutput, replay.collect(projected));
}

test "flat dependencies distinguish complete empty projections from historical omissions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var manifest: Manifest = .{};
    try manifest.collect(try std.json.parseFromSliceLeaky(V, a, "{\"upstream_agreement\":{}}", .{}));
    try std.testing.expectEqual(.unverified, manifest.nodes[0].kind);
    try manifest.collect(try std.json.parseFromSliceLeaky(V, a, "{\"upstream_agreement\":{},\"upstream_revisions\":[]}", .{}));
    try std.testing.expectEqual(.unverified, manifest.nodes[0].kind);
    try manifest.collect(try std.json.parseFromSliceLeaky(V, a, "{\"upstream_revisions_version\":1,\"upstream_revisions\":[]}", .{}));
    try std.testing.expectEqual(@as(usize, 0), manifest.len);
    try manifest.collect(try std.json.parseFromSliceLeaky(V, a,
        \\{"name":"proxy","decision_method":"typed","answers":[{"decision_method":"typed"}]}
    , .{}));
    try std.testing.expectEqual(.unverified, manifest.nodes[0].kind);
    try std.testing.expectError(error.InvalidDecisionOutput, manifest.collect(try std.json.parseFromSliceLeaky(V, a,
        \\{"model":"gen","model_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","answers":[{"decision_method":"generated"}],"upstream_revisions":[]}
    , .{})));
    try std.testing.expectError(error.InvalidDecisionOutput, manifest.collect(.{ .array = std.array_list.Managed(V).init(a) }));
}

test "flat dependencies bound cardinality depth and selector ambiguity" {
    var manifest: Manifest = .{};
    const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    var names: [max_nodes + 1][8]u8 = undefined;
    for (0..max_nodes) |i| {
        const name = try std.fmt.bufPrint(&names[i], "s{d}", .{i});
        try manifest.add(.{ .kind = .model, .path = &.{name}, .identity = id, .model = "gen" });
    }
    try std.testing.expectError(error.InvalidDecisionOutput, manifest.add(.{ .kind = .model, .path = &.{"overflow"}, .identity = id, .model = "gen" }));
    try std.testing.expectError(error.InvalidDecisionOutput, validateNode(.{ .kind = .model, .path = &@as([max_depth + 1][]const u8, @splat("x")), .identity = id, .model = "gen" }));
    try std.testing.expectError(error.InvalidDecisionOutput, validateNode(.{ .kind = .model, .path = &.{"bad/path"}, .identity = id, .model = "gen" }));
}

test "definition-only cascades are pinned and legacy projections cannot prove completeness" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const full = try std.json.parseFromSliceLeaky(V, a,
        \\{"cascade":{"definition_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","stages":[{"name":"typed","answers":[{"decision_method":"typed"}],"upstream_revisions_version":1,"upstream_revisions":[]}]}}
    , .{});
    var current: Manifest = .{};
    try current.collect(full);
    try std.testing.expectEqual(@as(usize, 1), current.len);
    try std.testing.expectEqual(.cascade, current.nodes[0].kind);
    try std.testing.expectEqualStrings(id, current.nodes[0].identity);
    var old: Manifest = .{};
    try old.collectHistorical(full);
    try std.testing.expectEqual(@as(usize, 0), old.len);
    var projection = V{ .object = .empty };
    try projection.object.put(a, "upstream_revisions", try current.value(a));
    var replay: Manifest = .{};
    try replay.collect(projection);
    try std.testing.expectEqual(@as(usize, 2), replay.len);
    try std.testing.expectEqual(.unverified, replay.nodes[1].kind);
    try old.collectHistorical(projection);
    try std.testing.expectEqual(@as(usize, 1), old.len);
    try projection.object.put(a, "upstream_revisions_version", .{ .integer = 1 });
    try replay.collect(projection);
    try std.testing.expectEqualSlices(u8, &current.digest(false), &replay.digest(false));
    try projection.object.put(a, "upstream_agreement", try std.json.parseFromSliceLeaky(V, a,
        \\{"definition_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
    , .{}));
    try replay.collect(projection);
    try projection.object.put(a, "upstream_revisions", .{ .array = .init(a) });
    try std.testing.expectError(error.InvalidDecisionOutput, replay.collect(projection));
    _ = projection.object.swapRemove("upstream_revisions_version");
    try replay.collect(projection);
    try std.testing.expectEqual(.unverified, replay.nodes[0].kind);
    try std.testing.expectEqualStrings(id, replay.nodes[0].identity);
    for ([_][]const u8{ "0", "2", "null", "true", "\"1\"" }) |raw| {
        try projection.object.put(a, "upstream_revisions_version", try std.json.parseFromSliceLeaky(V, a, raw, .{}));
        try std.testing.expectError(error.InvalidDecisionOutput, replay.collect(projection));
    }
    try projection.object.put(a, "upstream_revisions_version", .{ .integer = 1 });
    _ = projection.object.swapRemove("upstream_revisions");
    try std.testing.expectError(error.InvalidDecisionOutput, replay.collect(projection));
}
