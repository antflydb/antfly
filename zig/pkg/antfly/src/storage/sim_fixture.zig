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

pub const header = "storage-sim-fixture-v1";
pub const legacy_header = "lmdb-sim-fixture-v1";

pub const Fixture = struct {
    pub const Field = struct {
        key: []u8,
        value: []u8,
    };

    mode: ?[]u8 = null,
    label: ?[]u8 = null,
    case_label: ?[]u8 = null,
    origin_seed: ?[]u8 = null,
    phase: ?[]u8 = null,
    expectation: ?[]u8 = null,
    max_dbs: ?[]u8 = null,
    write_map: ?[]u8 = null,
    map_async: ?[]u8 = null,
    fixed_map: ?[]u8 = null,
    no_sync: ?[]u8 = null,
    no_meta_sync: ?[]u8 = null,
    commit_backend: ?[]u8 = null,
    extra_fields: std.ArrayListUnmanaged(Field) = .empty,
    comments: std.ArrayListUnmanaged([]u8) = .empty,
    actions: std.ArrayListUnmanaged([]u8) = .empty,
    crash_action: ?[]u8 = null,

    pub fn deinit(self: *Fixture, allocator: std.mem.Allocator) void {
        freeOptional(allocator, &self.mode);
        freeOptional(allocator, &self.label);
        freeOptional(allocator, &self.case_label);
        freeOptional(allocator, &self.origin_seed);
        freeOptional(allocator, &self.phase);
        freeOptional(allocator, &self.expectation);
        freeOptional(allocator, &self.max_dbs);
        freeOptional(allocator, &self.write_map);
        freeOptional(allocator, &self.map_async);
        freeOptional(allocator, &self.fixed_map);
        freeOptional(allocator, &self.no_sync);
        freeOptional(allocator, &self.no_meta_sync);
        freeOptional(allocator, &self.commit_backend);
        freeOptional(allocator, &self.crash_action);
        for (self.extra_fields.items) |field| {
            allocator.free(field.key);
            allocator.free(field.value);
        }
        self.extra_fields.deinit(allocator);
        for (self.comments.items) |line| allocator.free(line);
        self.comments.deinit(allocator);
        for (self.actions.items) |line| allocator.free(line);
        self.actions.deinit(allocator);
        self.* = undefined;
    }
};

pub fn parse(allocator: std.mem.Allocator, raw: []const u8) !Fixture {
    var lines = std.mem.splitScalar(u8, raw, '\n');
    const fixture_header = std.mem.trim(u8, lines.next() orelse return error.InvalidFixture, " \t\r");
    if (!std.mem.eql(u8, fixture_header, header) and !std.mem.eql(u8, fixture_header, legacy_header)) {
        return error.InvalidFixture;
    }

    var fixture: Fixture = .{};
    errdefer fixture.deinit(allocator);

    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        if (line[0] == '#') {
            try fixture.comments.append(allocator, try allocator.dupe(u8, line));
            continue;
        }
        if (std.mem.startsWith(u8, line, "action ")) {
            try fixture.actions.append(allocator, try allocator.dupe(u8, line["action ".len..]));
            continue;
        }
        if (std.mem.startsWith(u8, line, "crash_action ")) {
            try setOwnedField(allocator, &fixture.crash_action, line["crash_action ".len..]);
            continue;
        }
        if (std.mem.startsWith(u8, line, "expectation ")) {
            try setOwnedField(allocator, &fixture.expectation, line["expectation ".len..]);
            continue;
        }

        const space_index = std.mem.indexOfScalar(u8, line, ' ') orelse return error.InvalidFixture;
        const key = line[0..space_index];
        const value = line[space_index + 1 ..];

        if (std.mem.eql(u8, key, "mode")) {
            try setOwnedField(allocator, &fixture.mode, value);
            continue;
        }
        if (std.mem.eql(u8, key, "label")) {
            try setOwnedField(allocator, &fixture.label, value);
            continue;
        }
        if (std.mem.eql(u8, key, "case_label")) {
            try setOwnedField(allocator, &fixture.case_label, value);
            continue;
        }
        if (std.mem.eql(u8, key, "origin_seed")) {
            try setOwnedField(allocator, &fixture.origin_seed, value);
            continue;
        }
        if (std.mem.eql(u8, key, "phase")) {
            try setOwnedField(allocator, &fixture.phase, value);
            continue;
        }
        if (std.mem.eql(u8, key, "max_dbs")) {
            try setOwnedField(allocator, &fixture.max_dbs, value);
            continue;
        }
        if (std.mem.eql(u8, key, "write_map")) {
            try setOwnedField(allocator, &fixture.write_map, value);
            continue;
        }
        if (std.mem.eql(u8, key, "map_async")) {
            try setOwnedField(allocator, &fixture.map_async, value);
            continue;
        }
        if (std.mem.eql(u8, key, "fixed_map")) {
            try setOwnedField(allocator, &fixture.fixed_map, value);
            continue;
        }
        if (std.mem.eql(u8, key, "no_sync")) {
            try setOwnedField(allocator, &fixture.no_sync, value);
            continue;
        }
        if (std.mem.eql(u8, key, "no_meta_sync")) {
            try setOwnedField(allocator, &fixture.no_meta_sync, value);
            continue;
        }
        if (std.mem.eql(u8, key, "commit_backend")) {
            try setOwnedField(allocator, &fixture.commit_backend, value);
            continue;
        }

        try fixture.extra_fields.append(allocator, .{
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, value),
        });
    }

    if (fixture.mode == null) return error.InvalidFixture;
    if (fixture.label == null and fixture.case_label != null) {
        try setOwnedField(allocator, &fixture.label, fixture.case_label.?);
    }
    if (fixture.case_label == null and fixture.label != null) {
        try setOwnedField(allocator, &fixture.case_label, fixture.label.?);
    }
    return fixture;
}

pub fn render(allocator: std.mem.Allocator, fixture: *const Fixture) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.appendSlice(allocator, header ++ "\n");
    for (fixture.comments.items) |comment| {
        try appendFormat(allocator, &out, "{s}\n", .{comment});
    }
    if (fixture.comments.items.len != 0) try out.append(allocator, '\n');

    try appendFormat(allocator, &out, "mode {s}\n", .{fixture.mode orelse return error.InvalidFixture});
    if (fixture.label) |label| try appendFormat(allocator, &out, "label {s}\n", .{label});
    if (fixture.case_label) |case_label| try appendFormat(allocator, &out, "case_label {s}\n", .{case_label});
    if (fixture.origin_seed) |seed| try appendFormat(allocator, &out, "origin_seed {s}\n", .{seed});
    if (fixture.phase) |phase| try appendFormat(allocator, &out, "phase {s}\n", .{phase});
    if (fixture.expectation) |expectation| try appendFormat(allocator, &out, "expectation {s}\n", .{expectation});
    if (fixture.max_dbs) |value| try appendFormat(allocator, &out, "max_dbs {s}\n", .{value});
    if (fixture.write_map) |value| try appendFormat(allocator, &out, "write_map {s}\n", .{value});
    if (fixture.map_async) |value| try appendFormat(allocator, &out, "map_async {s}\n", .{value});
    if (fixture.fixed_map) |value| try appendFormat(allocator, &out, "fixed_map {s}\n", .{value});
    if (fixture.no_sync) |value| try appendFormat(allocator, &out, "no_sync {s}\n", .{value});
    if (fixture.no_meta_sync) |value| try appendFormat(allocator, &out, "no_meta_sync {s}\n", .{value});
    if (fixture.commit_backend) |value| try appendFormat(allocator, &out, "commit_backend {s}\n", .{value});
    for (fixture.extra_fields.items) |field| {
        try appendFormat(allocator, &out, "{s} {s}\n", .{ field.key, field.value });
    }
    try out.append(allocator, '\n');

    for (fixture.actions.items) |action| {
        try appendFormat(allocator, &out, "action {s}\n", .{action});
    }
    if (fixture.crash_action) |crash_action| {
        try appendFormat(allocator, &out, "crash_action {s}\n", .{crash_action});
    }

    return out.toOwnedSlice(allocator);
}

pub fn normalizeStem(allocator: std.mem.Allocator, stem: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    var prev_dash = false;
    for (stem) |ch| {
        if (std.ascii.isAlphanumeric(ch)) {
            try out.append(allocator, std.ascii.toLower(ch));
            prev_dash = false;
            continue;
        }
        if (ch == '-' or ch == '_' or ch == ' ' or ch == '.') {
            if (!prev_dash and out.items.len != 0) {
                try out.append(allocator, '-');
                prev_dash = true;
            }
        }
    }

    while (out.items.len > 0 and out.items[out.items.len - 1] == '-') {
        _ = out.pop();
    }
    if (out.items.len == 0) return error.InvalidFixture;
    return out.toOwnedSlice(allocator);
}

pub fn extraFieldValue(fixture: *const Fixture, key: []const u8) ?[]const u8 {
    for (fixture.extra_fields.items) |field| {
        if (std.mem.eql(u8, field.key, key)) return field.value;
    }
    return null;
}

pub fn parseOptionalUnsignedExtraField(
    comptime T: type,
    fixture: *const Fixture,
    key: []const u8,
) !?T {
    const value = extraFieldValue(fixture, key) orelse return null;
    return try std.fmt.parseUnsigned(T, value, 10);
}

pub fn parseOptionalEnumTagExtraField(
    comptime T: type,
    fixture: *const Fixture,
    key: []const u8,
) !?T {
    const value = extraFieldValue(fixture, key) orelse return null;
    return std.meta.stringToEnum(T, value) orelse error.InvalidFixture;
}

pub fn appendOptionalUnsignedExtraField(
    allocator: std.mem.Allocator,
    fixture: *Fixture,
    key: []const u8,
    value: anytype,
) !void {
    if (value) |unwrapped| {
        try fixture.extra_fields.append(allocator, .{
            .key = try allocator.dupe(u8, key),
            .value = try std.fmt.allocPrint(allocator, "{d}", .{unwrapped}),
        });
    }
}

pub fn appendOptionalEnumTagExtraField(
    allocator: std.mem.Allocator,
    fixture: *Fixture,
    key: []const u8,
    value: anytype,
) !void {
    if (value) |unwrapped| {
        try fixture.extra_fields.append(allocator, .{
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, @tagName(unwrapped)),
        });
    }
}

pub fn expectFieldEqual(
    fixture_name: []const u8,
    comptime field_name: []const u8,
    expected: anytype,
    actual: @TypeOf(expected),
) !void {
    if (std.meta.eql(expected, actual)) return;

    switch (@typeInfo(@TypeOf(expected))) {
        .@"enum" => std.debug.print(
            "storage sim fixture {s}: expected {s}={s}, found {s}\n",
            .{ fixture_name, field_name, @tagName(expected), @tagName(actual) },
        ),
        .int, .comptime_int => std.debug.print(
            "storage sim fixture {s}: expected {s}={d}, found {d}\n",
            .{ fixture_name, field_name, expected, actual },
        ),
        .bool => std.debug.print(
            "storage sim fixture {s}: expected {s}={any}, found {any}\n",
            .{ fixture_name, field_name, expected, actual },
        ),
        else => @compileError("unsupported fixture expectation type"),
    }
    return error.TestExpectedEqual;
}

fn appendFormat(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const rendered = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(rendered);
    try out.appendSlice(allocator, rendered);
}

fn setOwnedField(allocator: std.mem.Allocator, field: *?[]u8, value: []const u8) !void {
    if (field.*) |prev| allocator.free(prev);
    field.* = try allocator.dupe(u8, value);
}

fn freeOptional(allocator: std.mem.Allocator, field: *?[]u8) void {
    if (field.*) |value| allocator.free(value);
    field.* = null;
}
