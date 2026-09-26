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
const Type = @import("backend.zig").Type;

pub fn oid(kind: Type) u32 {
    return switch (kind) {
        .boolean => 16,
        .integer => 20,
        .number => 701,
        .datetime => 1184,
        .json => 3802,
        .string, .unknown => 25,
    };
}

pub fn fromOid(value: u32) !Type {
    return switch (value) {
        0 => .unknown,
        16 => .boolean,
        20, 21, 23 => .integer,
        700, 701, 1700 => .number,
        25, 1043 => .string,
        1184 => .datetime,
        114, 3802 => .json,
        else => error.UnsupportedParameterType,
    };
}

pub fn typeSize(kind: Type) i16 {
    return switch (kind) {
        .boolean => 1,
        .integer, .number, .datetime => 8,
        else => -1,
    };
}

pub fn decode(alloc: std.mem.Allocator, param_oid: u32, format: u16, bytes: []const u8) !std.json.Value {
    const kind = try fromOid(param_oid);
    if (format > 1) return error.UnsupportedParameterFormat;
    if (format == 0) {
        if (!std.unicode.utf8ValidateSlice(bytes) or std.mem.indexOfScalar(u8, bytes, 0) != null) return error.InvalidParameter;
        return switch (kind) {
            .integer => .{ .integer = std.fmt.parseInt(i64, bytes, 10) catch return error.InvalidParameter },
            .boolean => .{ .bool = if (std.ascii.eqlIgnoreCase(bytes, "true") or std.mem.eql(u8, bytes, "t") or std.mem.eql(u8, bytes, "1")) true else if (std.ascii.eqlIgnoreCase(bytes, "false") or std.mem.eql(u8, bytes, "f") or std.mem.eql(u8, bytes, "0")) false else return error.InvalidParameter },
            .number => blk: {
                const value = try parseJson(alloc, bytes);
                if (value != .number_string) return error.InvalidParameter;
                break :blk value;
            },
            .json => try parseJson(alloc, bytes),
            else => .{ .string = try alloc.dupe(u8, bytes) },
        };
    }
    return switch (param_oid) {
        16 => if (bytes.len == 1 and bytes[0] <= 1) .{ .bool = bytes[0] == 1 } else error.InvalidParameter,
        21 => if (bytes.len == 2) .{ .integer = std.mem.readInt(i16, bytes[0..2], .big) } else error.InvalidParameter,
        23 => if (bytes.len == 4) .{ .integer = std.mem.readInt(i32, bytes[0..4], .big) } else error.InvalidParameter,
        20 => if (bytes.len == 8) .{ .integer = std.mem.readInt(i64, bytes[0..8], .big) } else error.InvalidParameter,
        700 => if (bytes.len == 4) try finite(@as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .big)))) else error.InvalidParameter,
        701 => if (bytes.len == 8) try finite(@as(f64, @bitCast(std.mem.readInt(u64, bytes[0..8], .big)))) else error.InvalidParameter,
        1184 => blk: {
            if (bytes.len != 8) return error.InvalidParameter;
            const micros = std.mem.readInt(i64, bytes[0..8], .big);
            const nanos = std.math.cast(u64, (@as(i128, micros) + 946684800000000) * 1000) orelse return error.InvalidParameter;
            break :blk try timestampValue(alloc, nanos);
        },
        114 => try parseJson(alloc, bytes),
        3802 => if (bytes.len > 0 and bytes[0] == 1) try parseJson(alloc, bytes[1..]) else error.InvalidParameter,
        25, 1043 => if (std.unicode.utf8ValidateSlice(bytes) and std.mem.indexOfScalar(u8, bytes, 0) == null) .{ .string = try alloc.dupe(u8, bytes) } else error.InvalidParameter,
        // PostgreSQL NUMERIC binary is not IEEE float. Reject it explicitly;
        // its exact text representation remains supported without rounding.
        else => error.UnsupportedParameterFormat,
    };
}

fn finite(value: f64) !std.json.Value {
    if (!std.math.isFinite(value)) return error.InvalidParameter;
    return .{ .float = value };
}

fn parseJson(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Value {
    // Bound nesting before constructing values: backend result serialization
    // must not recurse to attacker-chosen stack depth even below the byte cap.
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes) |byte| {
        if (quoted) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (byte == '\\') {
                escaped = true;
                continue;
            }
            if (byte == '"') quoted = false;
        } else switch (byte) {
            '"' => quoted = true,
            '[', '{' => {
                depth += 1;
                if (depth > 64) return error.InvalidParameter;
            },
            ']', '}' => {
                if (depth == 0) return error.InvalidParameter;
                depth -= 1;
            },
            else => {},
        }
    }
    return std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{
        .allocate = .alloc_always,
        .parse_numbers = false,
    }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidParameter,
    };
}

pub fn integer(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |v| v,
        .number_string, .string => |v| std.fmt.parseInt(i64, v, 10) catch error.InvalidResult,
        else => error.InvalidResult,
    };
}

pub fn encode(alloc: std.mem.Allocator, kind: Type, format: u16, value: std.json.Value) ![]const u8 {
    if (format > 1) return error.UnsupportedResultFormat;
    if (format == 0) return switch (kind) {
        .boolean => if (value == .bool) try alloc.dupe(u8, if (value.bool) "t" else "f") else error.InvalidResult,
        .integer => try std.fmt.allocPrint(alloc, "{d}", .{try integer(value)}),
        .datetime => try timestampText(alloc, value),
        .json => try std.json.Stringify.valueAlloc(alloc, value, .{}),
        else => if (value == .string) try alloc.dupe(u8, value.string) else try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
    return switch (kind) {
        .boolean => if (value == .bool) try alloc.dupe(u8, &.{@intFromBool(value.bool)}) else error.InvalidResult,
        .integer => try encodeInteger(alloc, try integer(value)),
        .number => blk: {
            const number = switch (value) {
                .float => |v| v,
                .integer => |v| @as(f64, @floatFromInt(v)),
                .number_string, .string => |v| std.fmt.parseFloat(f64, v) catch return error.InvalidResult,
                else => return error.InvalidResult,
            };
            if (!std.math.isFinite(number)) return error.InvalidResult;
            break :blk try encodeInteger(alloc, @bitCast(number));
        },
        .datetime => blk: {
            const nanos = try timestampNanos(value);
            if (@mod(nanos, 1000) != 0) return error.UnsupportedResultPrecision;
            break :blk try encodeInteger(alloc, @as(i64, @intCast(nanos / 1000)) - 946684800000000);
        },
        .json => blk: {
            const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(json);
            break :blk try std.mem.concat(alloc, u8, &.{ &.{1}, json });
        },
        .string => if (value == .string) try alloc.dupe(u8, value.string) else error.InvalidResult,
        .unknown => if (value == .string) try alloc.dupe(u8, value.string) else try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn encodeInteger(alloc: std.mem.Allocator, value: i64) ![]const u8 {
    const bytes = try alloc.alloc(u8, 8);
    std.mem.writeInt(i64, bytes[0..8], value, .big);
    return bytes;
}

fn timestampText(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    // ISO strings remain strings; integer cells are the native nanosecond
    // representation, not PostgreSQL's microseconds-since-2000 wire value.
    if (value == .string and std.mem.indexOfScalar(u8, value.string, 'T') != null)
        return alloc.dupe(u8, value.string);
    const ns = try timestampNanos(value);
    if (@mod(ns, 1000) != 0) return error.UnsupportedResultPrecision;
    const epoch = std.time.epoch.EpochSeconds{ .secs = @intCast(@divTrunc(ns, std.time.ns_per_s)) };
    const year_day = epoch.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const seconds = epoch.getDaySeconds();
    return std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        year_day.year,                                                    month_day.month.numeric(),    month_day.day_index + 1,
        seconds.getHoursIntoDay(),                                        seconds.getMinutesIntoHour(), seconds.getSecondsIntoMinute(),
        @as(u32, @intCast(@divTrunc(@mod(ns, std.time.ns_per_s), 1000))),
    });
}

/// The protocol carries exact native unsigned nanoseconds until the native
/// adapter normalizes typed parameters to the engine's canonical ISO string.
pub fn timestampNanos(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |ns| std.math.cast(u64, ns) orelse error.InvalidResult,
        .number_string => |text| std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidResult,
        else => error.InvalidResult,
    };
}

pub fn timestampValue(alloc: std.mem.Allocator, nanos: u64) !std.json.Value {
    if (std.math.cast(i64, nanos)) |signed| return .{ .integer = signed };
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{nanos}) };
}

test "pgwire typed parameters preserve exact integers and reject binary guesses" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try decode(alloc, 20, 0, "9007199254740993")).integer);
    try std.testing.expectEqualStrings("9007199254740993.125", (try decode(alloc, 1700, 0, "9007199254740993.125")).number_string);
    try std.testing.expectError(error.UnsupportedParameterFormat, decode(alloc, 1700, 1, "123"));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 16, 1, &.{2}));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 20, 0, "1; DROP TABLE x"));
    const encoded = try encode(alloc, .integer, 1, .{ .string = "9007199254740993" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), std.mem.readInt(i64, encoded[0..8], .big));
}

test "pgwire JSON parameter nesting is bounded independently of frame bytes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var bytes: [130]u8 = undefined;
    @memset(bytes[0..65], '[');
    @memset(bytes[65..], ']');
    try std.testing.expectError(error.InvalidParameter, decode(arena.allocator(), 3802, 0, &bytes));
    try std.testing.expectEqualStrings("[{]}", (try decode(arena.allocator(), 3802, 0, "\"[{]}\"")).string);
}

test "pgwire timestamp conversion uses postgres epoch and refuses precision loss" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const value = std.json.Value{ .integer = 946684800000000000 };
    try std.testing.expectEqualStrings("2000-01-01T00:00:00.000000Z", try encode(alloc, .datetime, 0, value));
    const binary = try encode(alloc, .datetime, 1, value);
    try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, binary[0..8], .big));
    try std.testing.expectEqual(value.integer, (try decode(alloc, 1184, 1, binary)).integer);
    try std.testing.expectError(error.UnsupportedResultPrecision, encode(alloc, .datetime, 0, .{ .integer = value.integer + 1 }));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 1184, 1, &.{ 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }));
    var before_epoch: [8]u8 = undefined;
    std.mem.writeInt(i64, &before_epoch, -946684800000001, .big);
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 1184, 1, &before_epoch));
    const high_nanos = std.math.maxInt(u64) / 1000 * 1000;
    const high = try timestampValue(alloc, high_nanos);
    try std.testing.expect(high == .number_string);
    const high_binary = try encode(alloc, .datetime, 1, high);
    try std.testing.expectEqual(high_nanos, try timestampNanos(try decode(alloc, 1184, 1, high_binary)));
    try std.testing.expectError(error.UnsupportedResultPrecision, encode(alloc, .datetime, 1, try timestampValue(alloc, std.math.maxInt(u64))));
}
