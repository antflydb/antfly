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

const std = @import("std");

/// Validate generated OpenAPI objects without retaining their values. Arrays
/// retire each element immediately; object key storage lives only for that
/// object. Custom leaf/union parsers remain authoritative for their contract.
/// The default strict parse semantics (including duplicate and unknown fields)
/// are preserved. Dynamic JSON uses an explicit stack rather than one native
/// call frame per nested value.
/// Supply reclaimable scratch: each completed value retires its temporary
/// owners. This is schema validation, not just a syntax check.
pub fn validateTyped(comptime T: type, alloc: std.mem.Allocator, input: []const u8) !void {
    var scanner = std.json.Scanner.initCompleteInput(alloc, input);
    defer scanner.deinit();
    var validator: Validator = .{ .alloc = alloc, .scanner = &scanner, .options = .{ .allocate = .alloc_if_needed, .max_value_len = input.len } };
    try validator.value(T);
    if (try scanner.next() != .end_of_document) return error.UnexpectedToken;
}

const Validator = struct {
    alloc: std.mem.Allocator,
    scanner: *std.json.Scanner,
    options: std.json.ParseOptions,

    fn expect(self: *@This(), comptime token: std.meta.Tag(std.json.Token)) !void {
        if (try self.scanner.next() != token) return error.UnexpectedToken;
    }
    const Frame = struct {
        object: bool,
        keys: std.heap.ArenaAllocator,
        seen: std.StringHashMapUnmanaged(void) = .empty,
    };
    fn dynamic(self: *@This()) anyerror!void {
        var stack: std.ArrayListUnmanaged(Frame) = .empty;
        defer {
            for (stack.items) |*frame| frame.keys.deinit();
            stack.deinit(self.alloc);
        }
        try self.dynamicValue(&stack);
        while (stack.items.len != 0) {
            const top = &stack.items[stack.items.len - 1];
            if (top.object) {
                const token = try self.scanner.nextAllocMax(top.keys.allocator(), .alloc_if_needed, self.options.max_value_len.?);
                const name = switch (token) {
                    .string, .allocated_string => |name| name,
                    .object_end => {
                        var completed = stack.pop().?;
                        completed.keys.deinit();
                        continue;
                    },
                    else => return error.UnexpectedToken,
                };
                if ((try top.seen.getOrPut(top.keys.allocator(), name)).found_existing) return error.DuplicateField;
            } else if (try self.scanner.peekNextTokenType() == .array_end) {
                try self.expect(.array_end);
                var completed = stack.pop().?;
                completed.keys.deinit();
                continue;
            }
            // No pointer into stack survives a push/reallocation. Each object
            // owns its keys until it closes; sibling values reuse that space.
            try self.dynamicValue(&stack);
        }
    }
    fn dynamicValue(self: *@This(), stack: *std.ArrayListUnmanaged(Frame)) !void {
        switch (try self.scanner.peekNextTokenType()) {
            .object_begin, .array_begin => |kind| {
                _ = try self.scanner.next();
                try stack.append(self.alloc, .{ .object = kind == .object_begin, .keys = .init(self.alloc) });
            },
            // Value accepts every syntactically valid scalar. Numeric
            // conversion cannot reject one (overflow stays a number_string),
            // so validation need not allocate/copy/convert each component.
            .string, .number, .true, .false, .null => try self.scanner.skipValue(),
            else => return error.UnexpectedToken,
        }
    }
    fn array(self: *@This(), comptime Child: type) anyerror!void {
        try self.expect(.array_begin);
        while (try self.scanner.peekNextTokenType() != .array_end) try self.value(Child);
        try self.expect(.array_end);
    }
    fn map(self: *@This(), comptime Child: type) anyerror!void {
        try self.expect(.object_begin);
        var keys = std.heap.ArenaAllocator.init(self.alloc);
        defer keys.deinit();
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            const token = try self.scanner.nextAllocMax(keys.allocator(), .alloc_if_needed, self.options.max_value_len.?);
            const name = switch (token) {
                .string, .allocated_string => |name| name,
                .object_end => break,
                else => return error.UnexpectedToken,
            };
            if ((try seen.getOrPut(keys.allocator(), name)).found_existing) return error.DuplicateField;
            try self.value(Child);
        }
    }
    fn object(self: *@This(), comptime T: type) anyerror!void {
        @setEvalBranchQuota(100000);
        const info = @typeInfo(T).@"struct";
        var seen: [info.field_names.len]bool = @splat(false);
        try self.expect(.object_begin);
        var key_scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer key_scratch.deinit();
        while (true) {
            if (!key_scratch.reset(.{ .retain_with_limit = 4096 })) _ = key_scratch.reset(.free_all);
            const token = try self.scanner.nextAllocMax(key_scratch.allocator(), .alloc_if_needed, self.options.max_value_len.?);
            const name = switch (token) {
                .string, .allocated_string => |name| name,
                .object_end => break,
                else => return error.UnexpectedToken,
            };
            inline for (info.field_types, T.openApiFieldMetadata, 0..) |Field, descriptor, i| {
                if (std.mem.eql(u8, name, descriptor[0])) {
                    if (seen[i]) return error.DuplicateField;
                    if (descriptor[2] and try self.scanner.peekNextTokenType() == .null) return error.UnexpectedToken;
                    try self.value(Field);
                    seen[i] = true;
                    break;
                }
            } else return error.UnknownField;
        }
        inline for (info.field_types, info.field_attrs, 0..) |Field, attrs, i| {
            if (!seen[i] and attrs.defaultValue(Field) == null) return error.MissingField;
        }
    }
    fn value(self: *@This(), comptime T: type) anyerror!void {
        if (T == std.json.Value) return self.dynamic();
        switch (@typeInfo(T)) {
            .optional => |info| {
                if (try self.scanner.peekNextTokenType() == .null) return self.expect(.null);
                return self.value(info.child);
            },
            .pointer => |info| if (info.size == .slice) {
                if (info.child != u8 or try self.scanner.peekNextTokenType() == .array_begin) return self.array(info.child);
                if (try self.scanner.peekNextTokenType() != .string) return error.UnexpectedToken;
                return self.scanner.skipValue();
            },
            .@"struct" => {
                if (@hasDecl(T, "openApiFieldMetadata")) return self.object(T);
                if (@hasField(T, "map")) {
                    const Map = @FieldType(T, "map");
                    if (@typeInfo(Map) == .@"struct" and @hasDecl(Map, "values")) {
                        const Values = @typeInfo(@TypeOf(@as(Map, undefined).values())).pointer.child;
                        if (T == std.json.ArrayHashMap(Values)) return self.map(Values);
                    }
                }
            },
            else => {},
        }
        // Enums, generated oneOf unions, and other custom leaves keep their
        // original parser. Their temporary representation retires immediately.
        var scratch = std.heap.ArenaAllocator.init(self.alloc);
        defer scratch.deinit();
        _ = try std.json.innerParse(T, scratch.allocator(), self.scanner, self.options);
    }
};

test "typed validation scans deep dynamic values without native recursion" {
    const a = std.testing.allocator;
    const depth = 10000;
    const input = try a.alloc(u8, 2 * depth + 1);
    defer a.free(input);
    @memset(input[0..depth], '[');
    input[depth] = '0';
    @memset(input[depth + 1 ..], ']');
    try validateTyped(std.json.Value, a, input);
    try std.testing.expectError(error.UnexpectedEndOfInput, validateTyped(std.json.Value, a, input[0 .. input.len - 1]));
    try std.testing.expectError(error.DuplicateField, validateTyped(std.json.Value, a, "[{\"x\":0,\"\\u0078\":1}]"));
    const Fail = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try validateTyped(std.json.Value, alloc, "{\"a\":[{\"b\":1},{\"c\":[2,3]}]}");
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fail.run, .{});
}
