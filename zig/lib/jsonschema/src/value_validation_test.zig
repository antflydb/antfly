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
const schema = @import("mod.zig");
const semantics = @import("value_semantics.zig");
const a = std.testing.allocator;

fn check(raw_schema: []const u8, raw_value: []const u8, valid: bool) !void {
    if (valid) return schema.validateJsonSchemaJson(a, raw_schema, raw_value);
    try std.testing.expectError(error.InvalidBatchRequest, schema.validateJsonSchemaJson(a, raw_schema, raw_value));
}

test "JSON assertions are conjunctive and type specific across constrained vocabulary" {
    const Case = struct { []const u8, []const u8, bool };
    const cases = [_]Case{
        .{ "{}", "null", true },
        .{ "{\"type\":\"integer\",\"enum\":[null,1]}", "null", false },
        .{ "{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}],\"enum\":[\"a\",\"b\"]}", "null", false },
        .{ "{\"minimum\":0}", "-1", false },
        .{ "{\"minimum\":0}", "\"negative\"", true },
        .{ "{\"type\":\"number\",\"minimum\":0}", "-1", false },
        .{ "{\"enum\":[{\"a\":1,\"b\":2}]}", "{\"b\":2,\"a\":1}", true },
        .{ "{\"enum\":[1]}", "1.0", true },
        .{ "{\"properties\":{\"x\":{\"type\":\"integer\"}}}", "1", true },
        .{ "{\"items\":{\"type\":\"string\"}}", "{}", true },
        .{ "{\"minLength\":2}", "1", true },
        .{ "{\"type\":\"null\"}", "null", true },
        .{ "{\"type\":\"null\"}", "false", false },
        .{ "{\"type\":\"boolean\"}", "true", true },
        .{ "{\"type\":\"boolean\"}", "0", false },
        .{ "{\"type\":\"string\"}", "\"s\"", true },
        .{ "{\"type\":\"string\"}", "null", false },
        .{ "{\"type\":\"object\"}", "{}", true },
        .{ "{\"type\":\"object\"}", "[]", false },
        .{ "{\"type\":\"array\"}", "[]", true },
        .{ "{\"type\":\"array\"}", "{}", false },
        .{ "{\"type\":\"number\"}", "1.5", true },
        .{ "{\"type\":\"integer\"}", "1.0", true },
        .{ "{\"type\":\"integer\"}", "1.5", false },
        .{ "{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]}", "null", true },
        .{ "{\"anyOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]}", "1", false },
        .{ "{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"number\"}]}", "1", false },
        .{ "{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"number\"}]}", "1.5", true },
        .{ "{\"allOf\":[{\"minimum\":1},{\"maximum\":2}],\"const\":2}", "1", false },
        .{ "{\"allOf\":[{\"minimum\":1},{\"maximum\":2}],\"const\":2}", "2", true },
        .{ "{\"type\":\"integer\",\"anyOf\":[{\"type\":\"null\"},{\"const\":1}]}", "null", false },
        .{ "{\"const\":{\"a\":[1,{\"x\":true}],\"b\":null}}", "{\"b\":null,\"a\":[1.0,{\"x\":true}]}", true },
        .{ "{\"const\":[1,2]}", "[2,1]", false },
        .{ "{\"properties\":{\"x\":{\"type\":\"integer\"}},\"required\":[\"x\"],\"additionalProperties\":false}", "{\"x\":1}", true },
        .{ "{\"properties\":{\"x\":{\"type\":\"integer\"}},\"required\":[\"x\"],\"additionalProperties\":false}", "{}", false },
        .{ "{\"properties\":{\"x\":{\"type\":\"integer\"}},\"additionalProperties\":false}", "{\"x\":1,\"y\":2}", false },
        .{ "{\"properties\":{\"x\":{\"type\":\"integer\"}}}", "{\"x\":false}", false },
        .{ "{\"additionalProperties\":{\"type\":\"boolean\"}}", "{\"x\":true}", true },
        .{ "{\"additionalProperties\":{\"type\":\"boolean\"}}", "{\"x\":1}", false },
        .{ "{\"items\":{\"type\":\"string\"},\"minItems\":1,\"maxItems\":2}", "[\"a\",\"b\"]", true },
        .{ "{\"items\":{\"type\":\"string\"},\"minItems\":1}", "[]", false },
        .{ "{\"maxItems\":1}", "[1,2]", false },
        .{ "{\"items\":{\"type\":\"string\"}}", "[1]", false },
        .{ "{\"minItems\":1,\"maxItems\":2}", "null", true },
        .{ "{\"uniqueItems\":true}", "[{\"a\":1,\"b\":2},{\"b\":2,\"a\":1.0}]", false },
        .{ "{\"uniqueItems\":true}", "[0,-0.0]", false },
        .{ "{\"uniqueItems\":true}", "[1,true,\"1\",null]", true },
        .{ "{\"minLength\":2,\"maxLength\":2}", "\"é🍎\"", true },
        .{ "{\"minLength\":2}", "\"é\"", false },
        .{ "{\"maxLength\":1}", "\"é🍎\"", false },
        .{ "{\"maximum\":0}", "1", false },
        .{ "{\"exclusiveMinimum\":0}", "0", false },
        .{ "{\"exclusiveMaximum\":0}", "0", false },
        .{ "{\"exclusiveMinimum\":0,\"exclusiveMaximum\":2}", "1", true },
        .{ "{\"title\":\"T\",\"description\":\"D\"}", "null", true },
        .{ "{\"minimum\":9007199254740993}", "9007199254740992", false },
        .{ "{\"maximum\":9007199254740992}", "9007199254740993", false },
        .{ "{\"enum\":[9007199254740993]}", "9007199254740992", false },
    };
    for (cases, 0..) |case, index| {
        check(case[0], case[1], case[2]) catch |err| {
            std.debug.print("JSON vocabulary case {d}: schema={s}, instance={s}\n", .{ index, case[0], case[1] });
            return err;
        };
    }
}

test "JSON decimal equality ordering and fingerprint agree without exponent expansion" {
    const pairs = [_][2][]const u8{
        .{ "1", "1.0" },                                                  .{ "-0", "0.000e100000" },
        .{ "123e100000", "1.2300e100002" },                               .{ "123e-100000", "0.123e-99997" },
        .{ "1e999999999999999999999999", "10e999999999999999999999998" },
    };
    for (pairs) |pair| {
        const left: std.json.Value = .{ .number_string = pair[0] };
        const right: std.json.Value = .{ .number_string = pair[1] };
        try std.testing.expect(try semantics.equal(a, left, right));
        try std.testing.expectEqual(try semantics.fingerprint(a, left), try semantics.fingerprint(a, right));
    }
    try std.testing.expectEqual(std.math.Order.gt, try semantics.numberOrder(a, .{ .integer = 9007199254740993 }, .{ .float = 9007199254740992.0 }));
    try std.testing.expectEqual(std.math.Order.lt, try semantics.numberOrder(a, .{ .number_string = "-1e100000" }, .{ .integer = -1 }));
    try std.testing.expect(try semantics.integral(a, .{ .number_string = "1e100000" }));
    try std.testing.expect(!try semantics.integral(a, .{ .number_string = "1e-100000" }));
}

test "compiled JSON schema owns literals and reuses scalar validation without allocations" {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"type\":\"string\",\"enum\":[\"ok\",\"fine\"]}", .{});
    var compiled = try schema.CompiledSchema.init(a, parsed.value);
    parsed.deinit();
    defer compiled.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    for (0..100) |_| try compiled.validate(failing.allocator(), .{ .string = "ok" });
    try std.testing.expectError(error.InvalidBatchRequest, compiled.validate(failing.allocator(), .{ .string = "no" }));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "JSON uniqueItems supports large arrays and detects a late semantic duplicate" {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"uniqueItems\":true}", .{});
    defer parsed.deinit();
    var compiled = try schema.CompiledSchema.init(a, parsed.value);
    defer compiled.deinit();
    const values = try a.alloc(std.json.Value, 4096);
    defer a.free(values);
    for (values, 0..) |*value, index| value.* = .{ .integer = @intCast(index) };
    var array = std.array_list.Managed(std.json.Value).init(a);
    array.items = values;
    array.capacity = values.len;
    try compiled.validate(a, .{ .array = array });
    array.items[4095] = .{ .float = 1.0 };
    try std.testing.expectError(error.InvalidBatchRequest, compiled.validate(a, .{ .array = array }));
}

fn allocationSweep(allocator: std.mem.Allocator) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"anyOf":[{"type":"array","uniqueItems":true},{"const":null}],"oneOf":[{"type":"array"},{"type":"string"}]}
    , .{});
    defer parsed.deinit();
    var compiled = try schema.CompiledSchema.init(allocator, parsed.value);
    defer compiled.deinit();
    var value = try std.json.parseFromSlice(std.json.Value, allocator, "[{\"a\":1,\"b\":2},3.0]", .{});
    defer value.deinit();
    try compiled.validate(allocator, value.value);
}

test "compiled JSON schema propagates composition allocation failures without leaks" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, allocationSweep, .{});
}

test "compiled JSON schema retains exact native numeric literals" {
    var literal: std.json.Value = .{ .object = .empty };
    defer literal.object.deinit(a);
    try literal.object.put(a, "const", .{ .number_string = "1.0000000000000001" });
    var compiled = try schema.CompiledSchema.init(a, literal);
    defer compiled.deinit();
    try compiled.validate(a, .{ .number_string = "1.00000000000000010" });
    try std.testing.expectError(error.InvalidBatchRequest, compiled.validate(a, .{ .integer = 1 }));
}
