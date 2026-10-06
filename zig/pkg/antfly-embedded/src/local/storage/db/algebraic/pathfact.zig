// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const cylinder = @import("cylinder.zig");
const token = @import("token.zig");

pub const Kind = enum {
    null,
    bool,
    number,
    string,
    object,
    array,

    pub fn tag(self: Kind) []const u8 {
        return @tagName(self);
    }
};

pub const Fact = struct {
    path: []u8,
    kind: Kind,
    value: []u8,

    pub fn deinit(self: *Fact, alloc: Allocator) void {
        alloc.free(self.path);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const PathProfile = struct {
    path: []u8,
    null_count: u32 = 0,
    bool_count: u32 = 0,
    number_count: u32 = 0,
    string_count: u32 = 0,
    object_count: u32 = 0,
    array_count: u32 = 0,
    string_numeric_parse_success_count: u32 = 0,
    string_numeric_parse_failure_count: u32 = 0,
    string_datetime_parse_success_count: u32 = 0,
    string_datetime_parse_failure_count: u32 = 0,
    string_token_count: u32 = 0,
    max_string_token_count: u32 = 0,

    pub fn record(self: *PathProfile, kind: Kind, scalar_value: ?[]const u8) void {
        switch (kind) {
            .null => self.null_count += 1,
            .bool => self.bool_count += 1,
            .number => self.number_count += 1,
            .string => {
                self.string_count += 1;
                if (scalar_value) |value| {
                    if (parseProfileFloat(value)) |_| {
                        self.string_numeric_parse_success_count += 1;
                    } else |_| {
                        self.string_numeric_parse_failure_count += 1;
                    }
                    if (isProfileRfc3339(value)) {
                        self.string_datetime_parse_success_count += 1;
                    } else {
                        self.string_datetime_parse_failure_count += 1;
                    }
                    const tokens = countProfileTokens(value);
                    self.string_token_count += tokens;
                    self.max_string_token_count = @max(self.max_string_token_count, tokens);
                }
            },
            .object => self.object_count += 1,
            .array => self.array_count += 1,
        }
    }

    pub fn scalarKindCount(self: PathProfile) u32 {
        return self.null_count + self.bool_count + self.number_count + self.string_count;
    }

    pub fn mixedScalarKinds(self: PathProfile) bool {
        return @as(u32, @intFromBool(self.null_count > 0)) +
            @as(u32, @intFromBool(self.bool_count > 0)) +
            @as(u32, @intFromBool(self.number_count > 0)) +
            @as(u32, @intFromBool(self.string_count > 0)) > 1;
    }

    pub fn empty(self: PathProfile) bool {
        return self.null_count == 0 and
            self.bool_count == 0 and
            self.number_count == 0 and
            self.string_count == 0 and
            self.object_count == 0 and
            self.array_count == 0 and
            self.string_numeric_parse_success_count == 0 and
            self.string_numeric_parse_failure_count == 0 and
            self.string_datetime_parse_success_count == 0 and
            self.string_datetime_parse_failure_count == 0 and
            self.string_token_count == 0 and
            self.max_string_token_count == 0;
    }

    pub fn deinit(self: *PathProfile, alloc: Allocator) void {
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub const Projection = struct {
    facts: []Fact,
    profiles: []PathProfile,

    pub fn deinit(self: *Projection, alloc: Allocator) void {
        for (self.facts) |*fact| fact.deinit(alloc);
        alloc.free(self.facts);
        for (self.profiles) |*profile| profile.deinit(alloc);
        alloc.free(self.profiles);
        self.* = undefined;
    }
};

pub const StoredProjection = struct {
    facts: []Fact = &.{},
    profiles: []PathProfile = &.{},

    pub fn deinit(self: *StoredProjection, alloc: Allocator) void {
        for (self.facts) |*fact| fact.deinit(alloc);
        if (self.facts.len > 0) alloc.free(self.facts);
        for (self.profiles) |*profile| profile.deinit(alloc);
        if (self.profiles.len > 0) alloc.free(self.profiles);
        self.* = .{};
    }
};

pub const FactList = struct {
    facts: []Fact = &.{},

    pub fn deinit(self: *FactList, alloc: Allocator) void {
        for (self.facts) |*fact| fact.deinit(alloc);
        if (self.facts.len > 0) alloc.free(self.facts);
        self.* = .{};
    }
};

pub fn projectJsonValueAlloc(alloc: Allocator, value: std.json.Value) !Projection {
    var facts = std.ArrayListUnmanaged(Fact).empty;
    errdefer {
        for (facts.items) |*fact| fact.deinit(alloc);
        facts.deinit(alloc);
    }
    var profiles = std.ArrayListUnmanaged(PathProfile).empty;
    errdefer {
        for (profiles.items) |*profile| profile.deinit(alloc);
        profiles.deinit(alloc);
    }

    const root_path = try alloc.dupe(u8, "");
    defer alloc.free(root_path);
    try projectValue(alloc, value, root_path, &facts, &profiles);

    return .{
        .facts = try facts.toOwnedSlice(alloc),
        .profiles = try profiles.toOwnedSlice(alloc),
    };
}

pub fn encodeProjectionAlloc(alloc: Allocator, projection: Projection) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var owned_counts = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_counts.items) |value| alloc.free(value);
        owned_counts.deinit(alloc);
    }

    try parts.append(alloc, "pathfacts:v3");
    for (projection.facts) |fact| {
        try parts.append(alloc, "f");
        try parts.append(alloc, fact.path);
        try parts.append(alloc, fact.kind.tag());
        try parts.append(alloc, fact.value);
    }
    for (projection.profiles) |profile| {
        try parts.append(alloc, "p");
        try parts.append(alloc, profile.path);
        try appendCount(alloc, &parts, &owned_counts, profile.null_count);
        try appendCount(alloc, &parts, &owned_counts, profile.bool_count);
        try appendCount(alloc, &parts, &owned_counts, profile.number_count);
        try appendCount(alloc, &parts, &owned_counts, profile.string_count);
        try appendCount(alloc, &parts, &owned_counts, profile.object_count);
        try appendCount(alloc, &parts, &owned_counts, profile.array_count);
        try appendCount(alloc, &parts, &owned_counts, profile.string_numeric_parse_success_count);
        try appendCount(alloc, &parts, &owned_counts, profile.string_numeric_parse_failure_count);
        try appendCount(alloc, &parts, &owned_counts, profile.string_datetime_parse_success_count);
        try appendCount(alloc, &parts, &owned_counts, profile.string_datetime_parse_failure_count);
        try appendCount(alloc, &parts, &owned_counts, profile.string_token_count);
        try appendCount(alloc, &parts, &owned_counts, profile.max_string_token_count);
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn encodeFactListAlloc(alloc: Allocator, facts: []const Fact) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    try parts.append(alloc, "pathfacts:v1");
    for (facts) |fact| {
        try parts.append(alloc, fact.path);
        try parts.append(alloc, fact.kind.tag());
        try parts.append(alloc, fact.value);
    }
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn decodeFactListAlloc(alloc: Allocator, encoded: []const u8) !FactList {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    if (parts.len == 0) return error.InvalidPathFactList;
    if (std.mem.eql(u8, parts[0], "pathfacts:v2") or std.mem.eql(u8, parts[0], "pathfacts:v3")) {
        const profile_part_count: usize = if (std.mem.eql(u8, parts[0], "pathfacts:v3")) 14 else 8;
        var out = std.ArrayListUnmanaged(Fact).empty;
        errdefer {
            for (out.items) |*fact| fact.deinit(alloc);
            out.deinit(alloc);
        }
        var pos: usize = 1;
        while (pos < parts.len) {
            if (std.mem.eql(u8, parts[pos], "f")) {
                if (pos + 3 >= parts.len) return error.InvalidPathFactList;
                const kind = std.meta.stringToEnum(Kind, parts[pos + 2]) orelse return error.InvalidPathFactList;
                try out.append(alloc, .{
                    .path = try alloc.dupe(u8, parts[pos + 1]),
                    .kind = kind,
                    .value = try alloc.dupe(u8, parts[pos + 3]),
                });
                pos += 4;
            } else if (std.mem.eql(u8, parts[pos], "p")) {
                if (pos + profile_part_count > parts.len) return error.InvalidPathFactList;
                pos += profile_part_count;
            } else {
                return error.InvalidPathFactList;
            }
        }
        return .{ .facts = try out.toOwnedSlice(alloc) };
    }
    if (!std.mem.eql(u8, parts[0], "pathfacts:v1")) return error.InvalidPathFactList;
    if ((parts.len - 1) % 3 != 0) return error.InvalidPathFactList;

    var out = std.ArrayListUnmanaged(Fact).empty;
    errdefer {
        for (out.items) |*fact| fact.deinit(alloc);
        out.deinit(alloc);
    }

    var pos: usize = 1;
    while (pos < parts.len) : (pos += 3) {
        const kind = std.meta.stringToEnum(Kind, parts[pos + 1]) orelse return error.InvalidPathFactList;
        try out.append(alloc, .{
            .path = try alloc.dupe(u8, parts[pos]),
            .kind = kind,
            .value = try alloc.dupe(u8, parts[pos + 2]),
        });
    }
    return .{ .facts = try out.toOwnedSlice(alloc) };
}

pub fn decodeProjectionAlloc(alloc: Allocator, encoded: []const u8) !StoredProjection {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    if (parts.len == 0) return error.InvalidPathFactList;
    if (std.mem.eql(u8, parts[0], "pathfacts:v1")) {
        if ((parts.len - 1) % 3 != 0) return error.InvalidPathFactList;
        var facts = std.ArrayListUnmanaged(Fact).empty;
        errdefer {
            for (facts.items) |*fact| fact.deinit(alloc);
            facts.deinit(alloc);
        }
        var pos: usize = 1;
        while (pos < parts.len) : (pos += 3) {
            const kind = std.meta.stringToEnum(Kind, parts[pos + 1]) orelse return error.InvalidPathFactList;
            try facts.append(alloc, .{
                .path = try alloc.dupe(u8, parts[pos]),
                .kind = kind,
                .value = try alloc.dupe(u8, parts[pos + 2]),
            });
        }
        return .{ .facts = try facts.toOwnedSlice(alloc) };
    }
    const is_v3 = std.mem.eql(u8, parts[0], "pathfacts:v3");
    if (!std.mem.eql(u8, parts[0], "pathfacts:v2") and !is_v3) return error.InvalidPathFactList;

    var facts = std.ArrayListUnmanaged(Fact).empty;
    errdefer {
        for (facts.items) |*fact| fact.deinit(alloc);
        facts.deinit(alloc);
    }
    var profiles = std.ArrayListUnmanaged(PathProfile).empty;
    errdefer {
        for (profiles.items) |*profile| profile.deinit(alloc);
        profiles.deinit(alloc);
    }

    var pos: usize = 1;
    while (pos < parts.len) {
        if (std.mem.eql(u8, parts[pos], "f")) {
            if (pos + 3 >= parts.len) return error.InvalidPathFactList;
            const kind = std.meta.stringToEnum(Kind, parts[pos + 2]) orelse return error.InvalidPathFactList;
            try facts.append(alloc, .{
                .path = try alloc.dupe(u8, parts[pos + 1]),
                .kind = kind,
                .value = try alloc.dupe(u8, parts[pos + 3]),
            });
            pos += 4;
        } else if (std.mem.eql(u8, parts[pos], "p")) {
            if (is_v3) {
                if (pos + 13 >= parts.len) return error.InvalidPathFactList;
            } else if (pos + 7 >= parts.len) return error.InvalidPathFactList;
            try profiles.append(alloc, .{
                .path = try alloc.dupe(u8, parts[pos + 1]),
                .null_count = try parseCount(parts[pos + 2]),
                .bool_count = try parseCount(parts[pos + 3]),
                .number_count = try parseCount(parts[pos + 4]),
                .string_count = try parseCount(parts[pos + 5]),
                .object_count = try parseCount(parts[pos + 6]),
                .array_count = try parseCount(parts[pos + 7]),
                .string_numeric_parse_success_count = if (is_v3) try parseCount(parts[pos + 8]) else 0,
                .string_numeric_parse_failure_count = if (is_v3) try parseCount(parts[pos + 9]) else 0,
                .string_datetime_parse_success_count = if (is_v3) try parseCount(parts[pos + 10]) else 0,
                .string_datetime_parse_failure_count = if (is_v3) try parseCount(parts[pos + 11]) else 0,
                .string_token_count = if (is_v3) try parseCount(parts[pos + 12]) else 0,
                .max_string_token_count = if (is_v3) try parseCount(parts[pos + 13]) else 0,
            });
            pos += if (is_v3) 14 else 8;
        } else {
            return error.InvalidPathFactList;
        }
    }
    return .{
        .facts = try facts.toOwnedSlice(alloc),
        .profiles = try profiles.toOwnedSlice(alloc),
    };
}

pub fn encodeProfileAlloc(alloc: Allocator, profile: PathProfile) ![]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var owned_counts = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_counts.items) |value| alloc.free(value);
        owned_counts.deinit(alloc);
    }
    try parts.append(alloc, "pathprofile:v2");
    try appendCount(alloc, &parts, &owned_counts, profile.null_count);
    try appendCount(alloc, &parts, &owned_counts, profile.bool_count);
    try appendCount(alloc, &parts, &owned_counts, profile.number_count);
    try appendCount(alloc, &parts, &owned_counts, profile.string_count);
    try appendCount(alloc, &parts, &owned_counts, profile.object_count);
    try appendCount(alloc, &parts, &owned_counts, profile.array_count);
    try appendCount(alloc, &parts, &owned_counts, profile.string_numeric_parse_success_count);
    try appendCount(alloc, &parts, &owned_counts, profile.string_numeric_parse_failure_count);
    try appendCount(alloc, &parts, &owned_counts, profile.string_datetime_parse_success_count);
    try appendCount(alloc, &parts, &owned_counts, profile.string_datetime_parse_failure_count);
    try appendCount(alloc, &parts, &owned_counts, profile.string_token_count);
    try appendCount(alloc, &parts, &owned_counts, profile.max_string_token_count);
    return try token.canonicalTupleAlloc(alloc, parts.items);
}

pub fn decodeProfileAlloc(alloc: Allocator, path: []const u8, encoded: []const u8) !PathProfile {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    const is_v2 = std.mem.eql(u8, parts[0], "pathprofile:v2");
    const expected_parts: usize = if (is_v2) 13 else 7;
    if ((!std.mem.eql(u8, parts[0], "pathprofile:v1") and !is_v2) or parts.len != expected_parts) return error.InvalidPathProfile;
    return .{
        .path = try alloc.dupe(u8, path),
        .null_count = try parseCount(parts[1]),
        .bool_count = try parseCount(parts[2]),
        .number_count = try parseCount(parts[3]),
        .string_count = try parseCount(parts[4]),
        .object_count = try parseCount(parts[5]),
        .array_count = try parseCount(parts[6]),
        .string_numeric_parse_success_count = if (is_v2) try parseCount(parts[7]) else 0,
        .string_numeric_parse_failure_count = if (is_v2) try parseCount(parts[8]) else 0,
        .string_datetime_parse_success_count = if (is_v2) try parseCount(parts[9]) else 0,
        .string_datetime_parse_failure_count = if (is_v2) try parseCount(parts[10]) else 0,
        .string_token_count = if (is_v2) try parseCount(parts[11]) else 0,
        .max_string_token_count = if (is_v2) try parseCount(parts[12]) else 0,
    };
}

fn appendCount(
    alloc: Allocator,
    parts: *std.ArrayListUnmanaged([]const u8),
    owned_counts: *std.ArrayListUnmanaged([]u8),
    value: u32,
) !void {
    const text = try std.fmt.allocPrint(alloc, "{d}", .{value});
    errdefer alloc.free(text);
    try owned_counts.append(alloc, text);
    try parts.append(alloc, text);
}

fn parseCount(text: []const u8) !u32 {
    return std.fmt.parseUnsigned(u32, text, 10) catch error.InvalidPathProfile;
}

fn projectValue(
    alloc: Allocator,
    value: std.json.Value,
    path: []const u8,
    facts: *std.ArrayListUnmanaged(Fact),
    profiles: *std.ArrayListUnmanaged(PathProfile),
) !void {
    const kind = kindFromJsonValue(value);

    switch (value) {
        .object => |object| {
            try recordProfile(alloc, profiles, path, kind, null);
            if (path.len > 0) {
                try facts.append(alloc, .{
                    .path = try alloc.dupe(u8, path),
                    .kind = kind,
                    .value = try alloc.dupe(u8, ""),
                });
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                const child_path = try pointerChildAlloc(alloc, path, entry.key_ptr.*);
                defer alloc.free(child_path);
                try projectValue(alloc, entry.value_ptr.*, child_path, facts, profiles);
            }
        },
        .array => |array| {
            try recordProfile(alloc, profiles, path, kind, null);
            if (path.len > 0) {
                try facts.append(alloc, .{
                    .path = try alloc.dupe(u8, path),
                    .kind = kind,
                    .value = try alloc.dupe(u8, ""),
                });
            }
            for (array.items, 0..) |item, i| {
                const item_kind = kindFromJsonValue(item);
                if (path.len > 0 and kindIsScalar(item_kind)) {
                    const scalar = try scalarValueAlloc(alloc, item);
                    errdefer alloc.free(scalar);
                    try recordProfile(alloc, profiles, path, item_kind, scalar);
                    try facts.append(alloc, .{
                        .path = try alloc.dupe(u8, path),
                        .kind = item_kind,
                        .value = scalar,
                    });
                }
                const index_text = try std.fmt.allocPrint(alloc, "{d}", .{i});
                defer alloc.free(index_text);
                const child_path = try pointerChildAlloc(alloc, path, index_text);
                defer alloc.free(child_path);
                try projectValue(alloc, item, child_path, facts, profiles);
            }
        },
        else => {
            const scalar = try scalarValueAlloc(alloc, value);
            errdefer alloc.free(scalar);
            try recordProfile(alloc, profiles, path, kind, scalar);
            try facts.append(alloc, .{
                .path = try alloc.dupe(u8, path),
                .kind = kind,
                .value = scalar,
            });
        },
    }
}

fn kindIsScalar(kind: Kind) bool {
    return switch (kind) {
        .null, .bool, .number, .string => true,
        .object, .array => false,
    };
}

pub fn kindFromJsonValue(value: std.json.Value) Kind {
    return switch (value) {
        .null => .null,
        .bool => .bool,
        .integer, .float, .number_string => .number,
        .string => .string,
        .object => .object,
        .array => .array,
    };
}

pub fn scalarValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, ""),
        .bool => |v| try alloc.dupe(u8, if (v) "true" else "false"),
        .integer => |v| try std.fmt.allocPrint(alloc, "{}", .{v}),
        .float => |v| try std.fmt.allocPrint(alloc, "{d}", .{v}),
        .number_string => |v| try alloc.dupe(u8, v),
        .string => |v| try alloc.dupe(u8, v),
        .object, .array => error.InvalidPathFactScalar,
    };
}

fn recordProfile(
    alloc: Allocator,
    profiles: *std.ArrayListUnmanaged(PathProfile),
    path: []const u8,
    kind: Kind,
    scalar_value: ?[]const u8,
) !void {
    for (profiles.items) |*profile| {
        if (std.mem.eql(u8, profile.path, path)) {
            profile.record(kind, scalar_value);
            return;
        }
    }
    var profile = PathProfile{ .path = try alloc.dupe(u8, path) };
    profile.record(kind, scalar_value);
    try profiles.append(alloc, profile);
}

fn parseProfileFloat(text: []const u8) !f64 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidProfileNumber;
    return std.fmt.parseFloat(f64, trimmed) catch error.InvalidProfileNumber;
}

fn isProfileRfc3339(text: []const u8) bool {
    _ = cylinder.unixSeconds(text) catch return false;
    return true;
}

pub fn countProfileTokens(text: []const u8) u32 {
    var count: u32 = 0;
    var in_token = false;
    for (text) |ch| {
        const token_char = std.ascii.isAlphanumeric(ch) or ch == '_';
        if (token_char and !in_token) count += 1;
        in_token = token_char;
    }
    return count;
}

fn expectProjectionFact(projection: Projection, path: []const u8, kind: Kind, value: []const u8) !void {
    for (projection.facts) |fact| {
        if (std.mem.eql(u8, fact.path, path) and fact.kind == kind and std.mem.eql(u8, fact.value, value)) return;
    }
    return error.TestExpectedEqual;
}

fn pointerChildAlloc(alloc: Allocator, parent: []const u8, segment: []const u8) ![]u8 {
    const escaped = try escapePointerSegmentAlloc(alloc, segment);
    defer alloc.free(escaped);
    if (parent.len == 0) return try std.fmt.allocPrint(alloc, "/{s}", .{escaped});
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ parent, escaped });
}

fn escapePointerSegmentAlloc(alloc: Allocator, segment: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (segment) |c| {
        switch (c) {
            '~' => try out.appendSlice(alloc, "~0"),
            '/' => try out.appendSlice(alloc, "~1"),
            else => try out.append(alloc, c),
        }
    }
    return try out.toOwnedSlice(alloc);
}

pub fn profileByPath(profiles: []const PathProfile, path: []const u8) ?PathProfile {
    for (profiles) |profile| {
        if (std.mem.eql(u8, profile.path, path)) return profile;
    }
    return null;
}

test "schemaless pathfact projection emits canonical scalar and structural facts" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"customer":"alice","amount":20,"published":true,"meta":{"a/b":"x","tilde~key":null},"items":[{"sku":"s1"}]}
    , .{});
    defer parsed.deinit();

    var projection = try projectJsonValueAlloc(alloc, parsed.value);
    defer projection.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 9), projection.facts.len);
    try std.testing.expectEqualStrings("/customer", projection.facts[0].path);
    try std.testing.expectEqual(Kind.string, projection.facts[0].kind);
    try std.testing.expectEqualStrings("alice", projection.facts[0].value);
    try std.testing.expectEqualStrings("/amount", projection.facts[1].path);
    try std.testing.expectEqual(Kind.number, projection.facts[1].kind);
    try std.testing.expectEqualStrings("20", projection.facts[1].value);
    try std.testing.expectEqualStrings("/meta", projection.facts[3].path);
    try std.testing.expectEqual(Kind.object, projection.facts[3].kind);
    try std.testing.expectEqualStrings("", projection.facts[3].value);
    try std.testing.expectEqualStrings("/meta/a~1b", projection.facts[4].path);
    try std.testing.expectEqualStrings("/meta/tilde~0key", projection.facts[5].path);
    try std.testing.expectEqual(Kind.null, projection.facts[5].kind);
    try std.testing.expectEqualStrings("/items", projection.facts[6].path);
    try std.testing.expectEqual(Kind.array, projection.facts[6].kind);
    try std.testing.expectEqualStrings("/items/0", projection.facts[7].path);
    try std.testing.expectEqual(Kind.object, projection.facts[7].kind);
    try std.testing.expectEqualStrings("/items/0/sku", projection.facts[8].path);
}

test "schemaless pathfact projection aliases scalar array elements to parent path" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"tags":["new","vip","new"],"flags":[true,null],"items":[{"sku":"s1"}]}
    , .{});
    defer parsed.deinit();

    var projection = try projectJsonValueAlloc(alloc, parsed.value);
    defer projection.deinit(alloc);

    try expectProjectionFact(projection, "/tags", .array, "");
    try expectProjectionFact(projection, "/tags", .string, "new");
    try expectProjectionFact(projection, "/tags", .string, "vip");
    try expectProjectionFact(projection, "/tags/0", .string, "new");
    try expectProjectionFact(projection, "/flags", .array, "");
    try expectProjectionFact(projection, "/flags", .bool, "true");
    try expectProjectionFact(projection, "/flags", .null, "");
    try expectProjectionFact(projection, "/items", .array, "");
    try expectProjectionFact(projection, "/items/0", .object, "");

    const tags_profile = profileByPath(projection.profiles, "/tags") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), tags_profile.array_count);
    try std.testing.expectEqual(@as(u32, 3), tags_profile.string_count);
}

test "schemaless pathfact encoding preserves typed path value triples" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"customer":"alice|bob","meta":{"line":"a\nb"}}
    , .{});
    defer parsed.deinit();

    var projection = try projectJsonValueAlloc(alloc, parsed.value);
    defer projection.deinit(alloc);
    const encoded = try encodeProjectionAlloc(alloc, projection);
    defer alloc.free(encoded);
    var decoded_facts = try decodeFactListAlloc(alloc, encoded);
    defer decoded_facts.deinit(alloc);
    var decoded = try decodeProjectionAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), decoded_facts.facts.len);
    try std.testing.expectEqual(@as(usize, 3), decoded.facts.len);
    try std.testing.expect(decoded.profiles.len > 0);
    try std.testing.expectEqualStrings("/customer", decoded.facts[0].path);
    try std.testing.expectEqual(Kind.string, decoded.facts[0].kind);
    try std.testing.expectEqualStrings("alice|bob", decoded.facts[0].value);
    try std.testing.expectEqualStrings("/meta", decoded.facts[1].path);
    try std.testing.expectEqual(Kind.object, decoded.facts[1].kind);
    try std.testing.expectEqualStrings("", decoded.facts[1].value);
    try std.testing.expectEqualStrings("/meta/line", decoded.facts[2].path);
    try std.testing.expectEqualStrings("a\nb", decoded.facts[2].value);

    const profile = profileByPath(decoded.profiles, "/customer") orelse return error.TestExpectedEqual;
    const profile_encoded = try encodeProfileAlloc(alloc, profile);
    defer alloc.free(profile_encoded);
    var profile_decoded = try decodeProfileAlloc(alloc, "/customer", profile_encoded);
    defer profile_decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), profile_decoded.string_count);
    try std.testing.expectEqual(@as(u32, 0), profile_decoded.string_numeric_parse_success_count);
    try std.testing.expectEqual(@as(u32, 1), profile_decoded.string_numeric_parse_failure_count);
    try std.testing.expectEqual(@as(u32, 0), profile_decoded.string_datetime_parse_success_count);
    try std.testing.expectEqual(@as(u32, 1), profile_decoded.string_datetime_parse_failure_count);
    try std.testing.expectEqual(@as(u32, 2), profile_decoded.string_token_count);
    try std.testing.expectEqual(@as(u32, 2), profile_decoded.max_string_token_count);
}

test "schemaless path profiles expose mixed-kind fallback signal" {
    const alloc = std.testing.allocator;
    var profiles = std.ArrayListUnmanaged(PathProfile).empty;
    defer {
        for (profiles.items) |*profile| profile.deinit(alloc);
        profiles.deinit(alloc);
    }

    try recordProfile(alloc, &profiles, "/amount", .number, null);
    try recordProfile(alloc, &profiles, "/amount", .string, "42.5");
    try recordProfile(alloc, &profiles, "/amount", .string, "2026-05-10T14:03:00Z");
    try recordProfile(alloc, &profiles, "/amount", .number, null);

    const profile = profileByPath(profiles.items, "/amount") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), profile.number_count);
    try std.testing.expectEqual(@as(u32, 2), profile.string_count);
    try std.testing.expectEqual(@as(u32, 1), profile.string_numeric_parse_success_count);
    try std.testing.expectEqual(@as(u32, 1), profile.string_numeric_parse_failure_count);
    try std.testing.expectEqual(@as(u32, 1), profile.string_datetime_parse_success_count);
    try std.testing.expectEqual(@as(u32, 1), profile.string_datetime_parse_failure_count);
    try std.testing.expectEqual(@as(u32, 7), profile.string_token_count);
    try std.testing.expectEqual(@as(u32, 5), profile.max_string_token_count);
    try std.testing.expect(profile.mixedScalarKinds());
}
