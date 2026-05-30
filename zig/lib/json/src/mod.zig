// Copyright 2026 Antfly, Inc.
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

const builtin = @import("builtin");
const std = @import("std");
const simd_stage1 = @import("simd_stage1.zig");
const _skip_tape = @import("skip_tape.zig");
const simd_typed = @import("simd_typed.zig");
const simd_value = @import("simd_value.zig");

const Allocator = std.mem.Allocator;

pub const ObjectMap = std.json.ObjectMap;
pub const Array = std.json.Array;
pub const Value = std.json.Value;
pub const ArrayHashMap = std.json.ArrayHashMap;

pub const Scanner = std.json.Scanner;
pub const validate = std.json.validate;
pub const Error = std.json.Error;
pub const default_buffer_size = std.json.default_buffer_size;
pub const Token = std.json.Token;
pub const TokenType = std.json.TokenType;
pub const Diagnostics = std.json.Diagnostics;
pub const AllocWhen = std.json.AllocWhen;
pub const default_max_value_len = std.json.default_max_value_len;
pub const Reader = std.json.Reader;
pub const isNumberFormattedLikeAnInteger = std.json.isNumberFormattedLikeAnInteger;

pub const ParseOptions = std.json.ParseOptions;
pub const Parsed = std.json.Parsed;
pub const parseFromTokenSource = std.json.parseFromTokenSource;
pub const parseFromTokenSourceLeaky = std.json.parseFromTokenSourceLeaky;
pub const innerParse = std.json.innerParse;
pub const parseFromValue = std.json.parseFromValue;
pub const parseFromValueLeaky = std.json.parseFromValueLeaky;
pub const innerParseFromValue = std.json.innerParseFromValue;
pub const ParseError = std.json.ParseError;
pub const ParseFromValueError = std.json.ParseFromValueError;

pub const Stringify = std.json.Stringify;
pub const fmt = std.json.fmt;
pub const Formatter = std.json.Formatter;
pub const value = Stringify.value;
pub const valueAlloc = Stringify.valueAlloc;
pub const SimdStructuralIndex = simd_stage1.StructuralIndex;
pub const buildStructuralIndexAlloc = simd_stage1.buildStructuralIndexAlloc;
pub const simdTypedSupportsType = simd_typed.supportsType;
pub const containsCustomJsonParseType = simd_typed.containsCustomJsonParseType;

pub const Backend = enum {
    stdlib,
    simd,
};

pub const PreferredBackend = enum {
    auto,
    stdlib,
    simd,
};

pub const BackendSelectionReason = enum {
    explicit_stdlib,
    input_too_small,
    unsupported_target,
    simd_partial_backend,
    type_contains_custom_json_parse,
    options_ignore_unknown_fields,
};

pub const BackendConfig = struct {
    preferred_backend: PreferredBackend = .auto,
    simd_min_input_len: usize = 256,
};

pub const BackendSelection = struct {
    requested: PreferredBackend,
    selected: Backend,
    reason: BackendSelectionReason,
    simd_target_supported: bool,
};

pub fn parseFromSlice(
    comptime T: type,
    allocator: Allocator,
    s: []const u8,
    options: ParseOptions,
) ParseError(Scanner)!Parsed(T) {
    return parseFromSliceWithConfig(T, allocator, s, options, .{});
}

pub fn parseFromSliceWithConfig(
    comptime T: type,
    allocator: Allocator,
    s: []const u8,
    options: ParseOptions,
    config: BackendConfig,
) ParseError(Scanner)!Parsed(T) {
    const selection = backendSelectionForTypedSliceWithOptions(T, s, options, config);
    return switch (selection.selected) {
        .stdlib => std.json.parseFromSlice(T, allocator, s, options),
        .simd => parseFromSliceSimdCompat(T, allocator, s, options),
    };
}

pub fn parseFromSliceLeaky(
    comptime T: type,
    allocator: Allocator,
    s: []const u8,
    options: ParseOptions,
) ParseError(Scanner)!T {
    return parseFromSliceLeakyWithConfig(T, allocator, s, options, .{});
}

pub fn parseFromSliceLeakyWithConfig(
    comptime T: type,
    allocator: Allocator,
    s: []const u8,
    options: ParseOptions,
    config: BackendConfig,
) ParseError(Scanner)!T {
    const selection = backendSelectionForTypedSliceWithOptions(T, s, options, config);
    return switch (selection.selected) {
        .stdlib => std.json.parseFromSliceLeaky(T, allocator, s, options),
        .simd => parseFromSliceLeakySimdCompat(T, allocator, s, options),
    };
}

pub fn backendSelectionForSlice(input: []const u8, config: BackendConfig) BackendSelection {
    const simd_supported = simdTargetSupported();

    switch (config.preferred_backend) {
        .stdlib => return .{
            .requested = .stdlib,
            .selected = .stdlib,
            .reason = .explicit_stdlib,
            .simd_target_supported = simd_supported,
        },
        .auto, .simd => {},
    }

    if (input.len < config.simd_min_input_len) {
        return .{
            .requested = config.preferred_backend,
            .selected = .stdlib,
            .reason = .input_too_small,
            .simd_target_supported = simd_supported,
        };
    }

    if (!simd_supported) {
        return .{
            .requested = config.preferred_backend,
            .selected = .stdlib,
            .reason = .unsupported_target,
            .simd_target_supported = false,
        };
    }

    if (config.preferred_backend == .simd or config.preferred_backend == .auto) {
        return .{
            .requested = config.preferred_backend,
            .selected = .simd,
            .reason = .simd_partial_backend,
            .simd_target_supported = true,
        };
    }
    unreachable;
}

pub fn backendSelectionForTypedSlice(comptime T: type, input: []const u8, config: BackendConfig) BackendSelection {
    return backendSelectionForTypedSliceWithOptions(T, input, .{}, config);
}

pub fn backendSelectionForTypedSliceWithOptions(
    comptime T: type,
    input: []const u8,
    options: ParseOptions,
    config: BackendConfig,
) BackendSelection {
    const selection = backendSelectionForSlice(input, config);
    if (selection.selected == .simd and config.preferred_backend == .auto and options.ignore_unknown_fields and T != Value) {
        return .{
            .requested = config.preferred_backend,
            .selected = .stdlib,
            .reason = .options_ignore_unknown_fields,
            .simd_target_supported = selection.simd_target_supported,
        };
    }
    if (selection.selected == .simd and config.preferred_backend == .auto and T != Value and comptime simd_typed.containsCustomJsonParseType(T)) {
        return .{
            .requested = config.preferred_backend,
            .selected = .stdlib,
            .reason = .type_contains_custom_json_parse,
            .simd_target_supported = selection.simd_target_supported,
        };
    }
    return selection;
}

pub fn simdTargetSupported() bool {
    return switch (builtin.target.cpu.arch) {
        .x86_64, .aarch64 => true,
        else => false,
    };
}

fn parseFromSliceSimdCompat(
    comptime T: type,
    allocator: Allocator,
    s: []const u8,
    options: ParseOptions,
) ParseError(Scanner)!Parsed(T) {
    var structural_index = try simd_stage1.buildStructuralIndexAlloc(allocator, s);
    defer structural_index.deinit(allocator);

    if (T == Value) {
        return simd_value.parseValueFromSlice(allocator, s, options, structural_index);
    }
    if (comptime simd_typed.supportsType(T)) {
        return simd_typed.parseFromSlice(T, allocator, s, options, structural_index);
    }

    // Unsupported types still use the custom Value parser as a front-end.
    var parsed_value = try simd_value.parseValueFromSlice(allocator, s, options, structural_index);
    errdefer parsed_value.deinit();
    const typed_value = try std.json.parseFromValueLeaky(T, parsed_value.arena.allocator(), parsed_value.value, options);
    return .{ .arena = parsed_value.arena, .value = typed_value };
}

fn parseFromSliceLeakySimdCompat(
    comptime T: type,
    allocator: Allocator,
    s: []const u8,
    options: ParseOptions,
) ParseError(Scanner)!T {
    var structural_index = try simd_stage1.buildStructuralIndexAlloc(allocator, s);
    defer structural_index.deinit(allocator);

    if (T == Value) {
        return simd_value.parseValueFromSliceLeaky(allocator, s, options, structural_index);
    }
    if (comptime simd_typed.supportsType(T)) {
        return simd_typed.parseFromSliceLeaky(T, allocator, s, options, structural_index);
    }
    const parsed_value = try simd_value.parseValueFromSliceLeaky(allocator, s, options, structural_index);
    return std.json.parseFromValueLeaky(T, allocator, parsed_value, options);
}

test "parseFromSlice stays source-compatible with std.json callers" {
    const T = struct {
        name: []const u8,
        count: u32,
        enabled: bool = false,
    };

    var parsed = try parseFromSlice(T, std.testing.allocator, "{\"name\":\"alpha\",\"count\":2}", .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("alpha", parsed.value.name);
    try std.testing.expectEqual(@as(u32, 2), parsed.value.count);
    try std.testing.expectEqual(false, parsed.value.enabled);
}

test "parseFromSliceLeaky supports Value callers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const parsed = try parseFromSliceLeaky(Value, arena.allocator(), "{\"ok\":true,\"n\":1}", .{});
    try std.testing.expect(parsed == .object);
    try std.testing.expect(parsed.object.get("ok").?.bool);
    try std.testing.expectEqual(@as(i64, 1), parsed.object.get("n").?.integer);
}

// 320-byte fixture ("0123456789" repeated 32 times). Replaces the removed
// `digits_repeated_32` array-repeat operator (Zig 0.17) with a comptime concat.
const digits_repeated_32: []const u8 = blk: {
    var s: []const u8 = "";
    for (0..32) |_| s = s ++ "0123456789";
    break :blk s;
};

test "explicit simd requests select the stage1 backend path" {
    const selection = backendSelectionForSlice(
        digits_repeated_32,
        .{ .preferred_backend = .simd, .simd_min_input_len = 32 },
    );

    try std.testing.expectEqual(.simd, selection.requested);
    try std.testing.expectEqual(.simd, selection.selected);
    try std.testing.expectEqual(.simd_partial_backend, selection.reason);
    try std.testing.expectEqual(simdTargetSupported(), selection.simd_target_supported);
}

test "small inputs stay on stdlib path in auto mode" {
    const selection = backendSelectionForSlice("{}", .{});

    try std.testing.expectEqual(.auto, selection.requested);
    try std.testing.expectEqual(.stdlib, selection.selected);
    try std.testing.expectEqual(.input_too_small, selection.reason);
}

test "auto mode selects partial simd backend for large inputs" {
    const selection = backendSelectionForSlice(digits_repeated_32, .{ .simd_min_input_len = 32 });

    try std.testing.expectEqual(.auto, selection.requested);
    try std.testing.expectEqual(.simd, selection.selected);
    try std.testing.expectEqual(.simd_partial_backend, selection.reason);
}

test "parseFromValue and valueAlloc match common std.json helpers" {
    const alloc = std.testing.allocator;

    const source: Value = .{
        .object = blk: {
            var obj = ObjectMap{};
            try obj.put(alloc, "name", .{ .string = "ada" });
            try obj.put(alloc, "count", .{ .integer = 3 });
            break :blk obj;
        },
    };
    defer {
        var owned = source;
        owned.object.deinit(alloc);
    }

    const T = struct {
        name: []const u8,
        count: u32,
    };

    var parsed = try parseFromValue(T, alloc, source, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("ada", parsed.value.name);
    try std.testing.expectEqual(@as(u32, 3), parsed.value.count);

    const encoded = try valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"name\":\"ada\"") != null);
}

test "auto typed selection stays on stdlib for custom jsonParse subtrees" {
    const T = struct {
        custom: struct {
            pub fn jsonParse(allocator: Allocator, source: anytype, options: ParseOptions) !@This() {
                _ = try std.json.innerParse(u32, allocator, source, options);
                return .{};
            }
        },
    };

    const selection = backendSelectionForTypedSlice(T, digits_repeated_32, .{ .simd_min_input_len = 32 });

    try std.testing.expectEqual(.auto, selection.requested);
    try std.testing.expectEqual(.stdlib, selection.selected);
    try std.testing.expectEqual(.type_contains_custom_json_parse, selection.reason);
}

test "auto typed selection stays on stdlib when ignoring unknown fields" {
    const T = struct {
        count: u32,
    };

    const selection = backendSelectionForTypedSliceWithOptions(T, digits_repeated_32, .{
        .ignore_unknown_fields = true,
    }, .{ .simd_min_input_len = 32 });

    try std.testing.expectEqual(.auto, selection.requested);
    try std.testing.expectEqual(.stdlib, selection.selected);
    try std.testing.expectEqual(.options_ignore_unknown_fields, selection.reason);
}

test "explicit simd value parsing handles plain and escaped strings" {
    const alloc = std.testing.allocator;

    var fast = try parseFromSliceWithConfig(Value, alloc, "{\"name\":\"ada\",\"n\":1}", .{}, .{
        .preferred_backend = .simd,
        .simd_min_input_len = 0,
    });
    defer fast.deinit();
    try std.testing.expectEqualStrings("ada", fast.value.object.get("name").?.string);
    try std.testing.expectEqual(@as(i64, 1), fast.value.object.get("n").?.integer);

    var escaped = try parseFromSliceWithConfig(Value, alloc, "{\"msg\":\"line\\n\"}", .{}, .{
        .preferred_backend = .simd,
        .simd_min_input_len = 0,
    });
    defer escaped.deinit();
    try std.testing.expectEqualStrings("line\n", escaped.value.object.get("msg").?.string);

    var unicode = try parseFromSliceWithConfig(Value, alloc, "{\"emoji\":\"\\uD83D\\uDE00\"}", .{}, .{
        .preferred_backend = .simd,
        .simd_min_input_len = 0,
    });
    defer unicode.deinit();
    try std.testing.expectEqualStrings("😀", unicode.value.object.get("emoji").?.string);
}

test "explicit simd typed parsing uses the native typed parser when supported" {
    const alloc = std.testing.allocator;
    const T = struct {
        msg: []const u8,
        count: u32 = 0,
    };

    var parsed = try parseFromSliceWithConfig(T, alloc, "{\"msg\":\"line\\n\",\"count\":1,\"count\":2,\"extra\":true}", .{
        .ignore_unknown_fields = true,
        .duplicate_field_behavior = .use_last,
    }, .{
        .preferred_backend = .simd,
        .simd_min_input_len = 0,
    });
    defer parsed.deinit();

    try std.testing.expectEqualStrings("line\n", parsed.value.msg);
    try std.testing.expectEqual(@as(u32, 2), parsed.value.count);
}

test "auto mode uses simd backend transparently for large typed payloads" {
    const alloc = std.testing.allocator;
    const T = struct {
        msg: []const u8,
        count: u32,
    };

    const raw =
        \\{"msg":"this is a deliberately longer payload that crosses the backend threshold and includes an escaped newline -> \n done","count":7}
    ;

    var parsed = try parseFromSliceWithConfig(T, alloc, raw, .{}, .{
        .preferred_backend = .auto,
        .simd_min_input_len = 32,
    });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u32, 7), parsed.value.count);
    try std.testing.expect(std.mem.indexOf(u8, parsed.value.msg, "escaped newline") != null);
    try std.testing.expect(std.mem.indexOfScalar(u8, parsed.value.msg, '\n') != null);
}

test "native typed parser support predicate accepts common config shapes" {
    const T = struct {
        provider: enum { openai, termite },
        model: ?[]const u8 = null,
        nested: struct {
            enabled: bool,
            count: u32,
        },
        tags: []const []const u8,
    };

    try std.testing.expect(simdTypedSupportsType(T));
}
