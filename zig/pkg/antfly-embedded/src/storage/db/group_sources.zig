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

//! Durable fan-in membership. Source owners journal membership changes in the
//! primary commit; a group-hash-routed coordinator applies them idempotently.
//! Producer work is prepared from bounded segment ranges, outside write locks.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Digest = [32]u8;
pub const Config = struct {
    group_by: []const u8,
    order_by: []const u8,
    segment_by: ?[]const u8 = null,
    field: []const u8,
    filter: V = .null,
    max_bytes: u32 = 32768,
    max_members: u32 = 1024,
    /// Fixed numeric ordinal cells bound repartitioning for unsegmented groups.
    /// Within a cell, deterministic byte windows preserve completed prefixes.
    window_ordinals: u32 = 128,
    pub fn clone(self: Config, a: A) !Config {
        var out = self;
        out.group_by = "";
        out.order_by = "";
        out.segment_by = null;
        out.field = "";
        out.filter = .null;
        errdefer out.deinit(a);
        out.group_by = try a.dupe(u8, self.group_by);
        out.order_by = try a.dupe(u8, self.order_by);
        out.field = try a.dupe(u8, self.field);
        if (self.segment_by) |value| out.segment_by = try a.dupe(u8, value);
        out.filter = try cloneValue(a, self.filter);
        return out;
    }
    pub fn deinit(self: *Config, a: A) void {
        a.free(self.group_by);
        a.free(self.order_by);
        a.free(self.field);
        if (self.segment_by) |value| a.free(value);
        freeValue(a, &self.filter);
        self.* = undefined;
    }
    pub fn semanticDigest(self: Config) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:group-source-config:v1");
        for ([_][]const u8{ self.group_by, self.order_by, self.segment_by orelse "", self.field }) |value| hashBytes(&hash, value);
        for ([_]u32{ self.max_bytes, self.max_members, self.window_ordinals }) |value| {
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, value, .big);
            hash.update(&bytes);
        }
        hashJson(&hash, self.filter);
        return hash.finalResult();
    }
    pub fn validate(self: Config) !void {
        var nodes: usize = 0;
        try validateValue(self.filter, 0, &nodes);
        for ([_][]const u8{ self.group_by, self.order_by, self.field }) |field| if (field.len == 0 or field.len > 1024) return error.InvalidGroupedSourceConfig;
        if (self.segment_by) |field| if (field.len == 0 or field.len > 1024) return error.InvalidGroupedSourceConfig;
        if (self.max_bytes == 0 or self.max_bytes > 4 * 1024 * 1024 or self.max_members == 0 or self.max_members > 4096 or self.window_ordinals == 0 or self.window_ordinals > 4096) return error.InvalidGroupedSourceConfig;
    }
};
pub const PriorContext = struct {
    artifact: []const u8,
    max_items: u16 = 40,
    max_bytes: u32 = 16384,
    render: []const u8,
    horizon_segments: u16 = 8,
    pub fn clone(self: PriorContext, a: A) !PriorContext {
        var out = self;
        out.artifact = try a.dupe(u8, self.artifact);
        errdefer a.free(out.artifact);
        out.render = try a.dupe(u8, self.render);
        return out;
    }
    pub fn deinit(self: *PriorContext, a: A) void {
        a.free(self.artifact);
        a.free(self.render);
        self.* = undefined;
    }
    pub fn validate(self: PriorContext) !void {
        if (self.artifact.len == 0 or self.artifact.len > 256 or self.render.len == 0 or self.render.len > 65536 or self.max_items == 0 or self.max_items > 256 or self.max_bytes == 0 or self.max_bytes > 1024 * 1024 or self.horizon_segments == 0 or self.horizon_segments > 128) return error.InvalidGroupedSourceConfig;
    }
};
fn hashBytes(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .big);
    hash.update(&length);
    hash.update(bytes);
}
fn validateValue(value: V, depth: usize, nodes: *usize) anyerror!void {
    nodes.* += 1;
    if (depth > 16 or nodes.* > 4096) return error.InvalidGroupedSourceConfig;
    switch (value) {
        .object => |object| {
            if (object.count() > 256) return error.InvalidGroupedSourceConfig;
            for (object.keys(), object.values()) |name, child| {
                if (name.len > 1024) return error.InvalidGroupedSourceConfig;
                try validateValue(child, depth + 1, nodes);
            }
        },
        .array => |array| for (array.items) |child| try validateValue(child, depth + 1, nodes),
        .string => |bytes| if (bytes.len > 65536) {
            return error.InvalidGroupedSourceConfig;
        },
        .float => |number| if (!std.math.isFinite(number)) {
            return error.InvalidGroupedSourceConfig;
        },
        .number_string => return error.InvalidGroupedSourceConfig,
        else => {},
    }
}
fn hashJson(hash: *std.crypto.hash.sha2.Sha256, value: V) void {
    hash.update(&.{@backingInt(std.meta.activeTag(value))});
    switch (value) {
        .object => |object| {
            std.debug.assert(object.count() <= 256);
            var sorted: [256][]const u8 = undefined;
            @memcpy(sorted[0..object.count()], object.keys());
            std.mem.sort([]const u8, sorted[0..object.count()], {}, struct {
                fn before(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.order(u8, left, right) == .lt;
                }
            }.before);
            for (sorted[0..object.count()]) |name| {
                hashBytes(hash, name);
                hashJson(hash, object.get(name).?);
            }
            hash.update("end-object");
        },
        .array => |array| {
            for (array.items) |child| hashJson(hash, child);
            hash.update("end-array");
        },
        .string, .number_string => |bytes| hashBytes(hash, bytes),
        .integer => |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(i64, &bytes, number, .big);
            hash.update(&bytes);
        },
        .float => |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, @bitCast(if (number == 0) @as(f64, 0) else number), .big);
            hash.update(&bytes);
        },
        .bool => |boolean| hash.update(&.{@intFromBool(boolean)}),
        .null => {},
    }
}
pub fn normalizedFilter(a: A, value: V) !V {
    var out = try cloneValue(a, value);
    errdefer freeValue(a, &out);
    try normalizeFilterAt(a, &out);
    return out;
}
fn normalizeFilterAt(a: A, value: *V) anyerror!void {
    switch (value.*) {
        .object => |*object| {
            if (object.count() == 1) if (object.get("term")) |term| {
                if (term == .object and term.object.count() == 1 and term.object.values()[0] == .array) {
                    const old_key = object.keys()[0];
                    const new_key = try a.dupe(u8, "terms");
                    _ = object.swapRemove("term");
                    a.free(old_key);
                    object.put(a, new_key, term) catch |err| {
                        a.free(new_key);
                        var child = term;
                        freeValue(a, &child);
                        return err;
                    };
                }
            };
            for (object.values()) |*child| try normalizeFilterAt(a, child);
        },
        .array => |*array| for (array.items) |*child| try normalizeFilterAt(a, child),
        else => {},
    }
}
fn cloneValue(a: A, value: V) anyerror!V {
    switch (value) {
        .string => |bytes| return .{ .string = try a.dupe(u8, bytes) },
        .number_string => |bytes| return .{ .number_string = try a.dupe(u8, bytes) },
        .object => |object| {
            var out: V = .{ .object = .empty };
            errdefer freeValue(a, &out);
            for (object.keys(), object.values()) |name, child| {
                const owned_name = try a.dupe(u8, name);
                errdefer a.free(owned_name);
                var owned_child = try cloneValue(a, child);
                errdefer freeValue(a, &owned_child);
                try out.object.put(a, owned_name, owned_child);
            }
            return out;
        },
        .array => |array| {
            var out: V = .{ .array = std.json.Array.init(a) };
            errdefer freeValue(a, &out);
            for (array.items) |child| {
                var owned = try cloneValue(a, child);
                errdefer freeValue(a, &owned);
                try out.array.append(owned);
            }
            return out;
        },
        else => return value,
    }
}
fn freeValue(a: A, value: *V) void {
    switch (value.*) {
        .string, .number_string => |bytes| a.free(bytes),
        .array => |*array| {
            for (array.items) |*child| freeValue(a, child);
            array.deinit();
        },
        .object => |*object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                a.free(entry.key_ptr.*);
                freeValue(a, entry.value_ptr);
            }
            object.deinit(a);
        },
        else => {},
    }
    value.* = .null;
}
pub fn digest(bytes: []const u8) Digest {
    var value: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
    return value;
}
pub const Member = struct {
    document: []const u8,
    /// Monotone committed document revision, retained across shard migration.
    version: u128,
    ordinal: i64,
    segment: []const u8,
    text: []const u8,
};
fn memberFingerprint(a: A, member: Member) !Digest {
    const raw = try std.json.Stringify.valueAlloc(a, .{ member.document, member.ordinal, member.segment, member.text }, .{});
    defer a.free(raw);
    return digest(raw);
}
pub const Unit = struct {
    id: Digest,
    group: []const u8,
    segment: []const u8,
    member_keys: []const []const u8,
    ordinal_start: i64,
    ordinal_end: i64,
    fingerprint: Digest,
    text: []const u8,
};
/// Members come from one bounded ordered segment range. Ties are broken by
/// document key; callers must not concatenate arrival order or shard order.
pub fn units(a: A, config: Config, group: []const u8, members: []const Member) ![]Unit {
    try config.validate();
    if (members.len > 65536) return error.GroupedSourceMemberLimitExceeded;
    const ordered = try a.dupe(Member, members);
    defer a.free(ordered);
    std.mem.sort(Member, ordered, {}, struct {
        fn before(_: void, left: Member, right: Member) bool {
            return left.ordinal < right.ordinal or (left.ordinal == right.ordinal and std.mem.order(u8, left.document, right.document) == .lt);
        }
    }.before);
    var output: std.ArrayList(Unit) = .empty;
    errdefer output.deinit(a);
    var begin: usize = 0;
    while (begin < ordered.len) {
        var end = begin;
        var bytes: usize = 0;
        while (end < ordered.len and end - begin < config.max_members) {
            const member = ordered[end];
            if (!std.mem.eql(u8, member.segment, ordered[begin].segment)) return error.InvalidGroupedSourceMutation;
            if (member.text.len > config.max_bytes) return error.GroupedSourceMemberTooLarge;
            const separator: usize = @intFromBool(end > begin);
            if (separator + member.text.len > config.max_bytes - bytes) break;
            bytes += separator + member.text.len;
            end += 1;
        }
        var text_bytes: std.ArrayList(u8) = .empty;
        defer text_bytes.deinit(a);
        const keys = try a.alloc([]const u8, end - begin);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        for (ordered[begin..end], keys, 0..) |member, *key, i| {
            key.* = member.document;
            if (i != 0) try text_bytes.append(a, '\n');
            try text_bytes.appendSlice(a, member.text);
            // Source revisions fence deliveries; only the source projection
            // contributes to the producer's semantic skip fingerprint.
            hash.update(&(try memberFingerprint(a, member)));
        }
        var fingerprint: Digest = undefined;
        hash.final(&fingerprint);
        const identity = try std.json.Stringify.valueAlloc(a, .{ group, ordered[begin].segment, ordered[begin].document }, .{});
        defer a.free(identity);
        try output.append(a, .{ .id = digest(identity), .group = group, .segment = ordered[begin].segment, .member_keys = keys, .ordinal_start = ordered[begin].ordinal, .ordinal_end = ordered[end - 1].ordinal, .fingerprint = fingerprint, .text = try text_bytes.toOwnedSlice(a) });
        begin = end;
    }
    return output.toOwnedSlice(a);
}

/// Fixed ordinal cells prevent an out-of-order insertion from shifting every
/// subsequent unsegmented window in a large thread.
pub fn implicitSegment(a: A, config: Config, ordinal: i64) ![]const u8 {
    try config.validate();
    return std.fmt.allocPrint(a, "window:{d}", .{@divFloor(ordinal, @as(i64, config.window_ordinals))});
}

pub const Selection = struct { group: []const u8, member: Member };
fn fieldAt(value: V, path: []const u8) ?V {
    var current = value;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}
fn integerValue(value: V) !i64 {
    return switch (value) {
        .integer => value.integer,
        .number_string => std.fmt.parseInt(i64, value.number_string, 10) catch return error.InvalidGroupedSourceMember,
        else => error.InvalidGroupedSourceMember,
    };
}
fn scalarKey(a: A, value: V) ![]const u8 {
    const result = switch (value) {
        .string => if (std.mem.startsWith(u8, value.string, "\x00")) try std.mem.concat(a, u8, &.{ "\x00s:", value.string }) else value.string,
        .integer, .number_string => try std.fmt.allocPrint(a, "\x00i:{d}", .{try integerValue(value)}),
        else => return error.InvalidGroupedSourceMember,
    };
    if (result.len == 0 or result.len > 4096 or !std.unicode.utf8ValidateSlice(result)) return error.InvalidGroupedSourceMember;
    return result;
}
/// The source owner supplies the result of its compiled canonical row filter.
/// Missing source fields mean no membership, including for internal unit rows.
pub fn selectMember(a: A, config: Config, document: []const u8, version: u128, value: V, filter_matches: bool) !?Selection {
    try config.validate();
    if (!filter_matches) return null;
    const group = fieldAt(value, config.group_by) orelse return null;
    const order = fieldAt(value, config.order_by) orelse return null;
    const content = fieldAt(value, config.field) orelse return null;
    if (group == .null or order == .null or content == .null) return null;
    const ordinal = try integerValue(order);
    if (content != .string) return error.InvalidGroupedSourceMember;
    if (content.string.len > config.max_bytes) return error.GroupedSourceMemberTooLarge;
    const segment = if (config.segment_by) |field| blk: {
        const selected = fieldAt(value, field) orelse return null;
        if (selected == .null) return null;
        break :blk try scalarKey(a, selected);
    } else try implicitSegment(a, config, ordinal);
    return .{ .group = try scalarKey(a, group), .member = .{ .document = document, .version = version, .ordinal = ordinal, .segment = segment, .text = content.string } };
}

test "group source unit windows are arrival independent and append preserves completed prefixes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const config = Config{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text", .max_bytes = 5 };
    const members = [_]Member{ .{ .document = "b", .version = 1, .ordinal = 2, .segment = "turn", .text = "bbb" }, .{ .document = "a", .version = 1, .ordinal = 1, .segment = "turn", .text = "aaa" } };
    const before = try units(a, config, "thread", &members);
    const appended = [_]Member{ members[1], members[0], .{ .document = "c", .version = 1, .ordinal = 3, .segment = "turn", .text = "ccc" } };
    const after = try units(a, config, "thread", &appended);
    try std.testing.expectEqual(@as(usize, 2), before.len);
    try std.testing.expectEqual(@as(usize, 3), after.len);
    try std.testing.expectEqualStrings("aaa", before[0].text);
    for (before, after[0..before.len]) |old, new| try std.testing.expectEqualSlices(u8, &old.fingerprint, &new.fingerprint);
}

// One unnamed test block: unnamed tests ignore filters, and the test
// selection audit allows exactly one per file (test_0).
test {
    _ = @import("group_prior_context.zig");
    _ = @import("group_prior_scan.zig");
    _ = @import("group_source_index.zig");
    _ = @import("group_source_state.zig");
    _ = @import("group_source_snapshot.zig");
    _ = @import("group_source_baseline.zig");
    _ = @import("group_source_worker.zig");
    _ = @import("group_source_retirement.zig");
    _ = @import("group_source_propagation_test.zig");
    _ = @import("group_source_lease_test.zig");
    _ = @import("group_source_owner_identity.zig");
    _ = @import("group_source_bytes.zig");
    _ = @import("group_source_reads.zig");
}

test "group source configuration cloning unwinds every allocation and canonicalizes filter identity" {
    const filter_a = try std.json.parseFromSlice(V, std.testing.allocator, "{\"term\":{\"speaker\":[\"user\",\"assistant\"]},\"field\":\"speaker\"}", .{});
    defer filter_a.deinit();
    const filter_b = try std.json.parseFromSlice(V, std.testing.allocator, "{\"field\":\"speaker\",\"term\":{\"speaker\":[\"user\",\"assistant\"]}}", .{});
    defer filter_b.deinit();
    const config = Config{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text", .filter = filter_a.value };
    var reordered = config;
    reordered.filter = filter_b.value;
    try config.validate();
    try std.testing.expectEqualSlices(u8, &config.semanticDigest(), &reordered.semanticDigest());
    const Check = struct {
        fn run(a: A, original: Config) !void {
            var cloned = try original.clone(a);
            defer cloned.deinit(a);
            try std.testing.expectEqualSlices(u8, &original.semanticDigest(), &cloned.semanticDigest());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{config});
}

pub fn definitionDigest(name: []const u8, config: Config) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:group-source-definition:v1:");
    hash.update(&digest(name));
    hash.update(&config.semanticDigest());
    return hash.finalResult();
}

/// Typed internal scalar identities retain natural JSON metadata for graph
/// templates while keeping integer 1 distinct from string "1".
pub fn scalarMetadata(selected: []const u8) !V {
    if (std.mem.startsWith(u8, selected, "\x00i:")) return .{ .integer = std.fmt.parseInt(i64, selected[3..], 10) catch return error.GroupedSourceCorrupt };
    if (std.mem.startsWith(u8, selected, "\x00s:")) return .{ .string = selected[3..] };
    return .{ .string = selected };
}
test "group scalar identity distinguishes integer string and reserved-prefix string values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const number = try scalarKey(a, .{ .integer = 1 });
    const string = try scalarKey(a, .{ .string = "1" });
    const escaped = try scalarKey(a, .{ .string = number });
    try std.testing.expect(!std.mem.eql(u8, number, string) and !std.mem.eql(u8, number, escaped));
    try std.testing.expectEqual(@as(i64, 1), (try scalarMetadata(number)).integer);
    try std.testing.expectEqualStrings("1", (try scalarMetadata(string)).string);
    try std.testing.expectEqualStrings(number, (try scalarMetadata(escaped)).string);
}

test "group source admission excludes null membership and rejects oversized selected members" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const config = Config{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text", .max_bytes = 5 };
    const large = try std.json.parseFromSliceLeaky(V, owned, "{\"thread\":\"a\",\"ordinal\":1,\"turn\":\"one\",\"text\":\"abcdef\"}", .{});
    try std.testing.expectError(error.GroupedSourceMemberTooLarge, selectMember(owned, config, "row", 1, large, true));
    try std.testing.expect(try selectMember(owned, config, "row", 1, large, false) == null);
    const missing = try std.json.parseFromSliceLeaky(V, owned, "{\"thread\":\"a\",\"ordinal\":1,\"turn\":null,\"text\":\"ok\"}", .{});
    try std.testing.expect(try selectMember(owned, config, "row", 1, missing, true) == null);
}

test "group sources normalize prepared relational integer lexemes without conflating string identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const row = try std.json.parseFromSliceLeaky(V, a, "{\"group\":1,\"order\":2,\"text\":\"source\"}", .{ .parse_numbers = false });
    const config = Config{ .group_by = "group", .order_by = "order", .field = "text" };
    const selected = (try selectMember(a, config, "row", 1, row, true)).?;
    try std.testing.expectEqual(@as(i64, 2), selected.member.ordinal);
    try std.testing.expectEqualStrings(try scalarKey(a, .{ .integer = 1 }), selected.group);
    try std.testing.expect(!std.mem.eql(u8, try scalarKey(a, .{ .string = "1" }), selected.group));
    try std.testing.expectError(error.InvalidGroupedSourceMember, integerValue(.{ .number_string = "1.5" }));
    try std.testing.expectError(error.InvalidGroupedSourceMember, integerValue(.{ .number_string = "9223372036854775808" }));
}
