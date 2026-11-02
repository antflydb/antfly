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
const types = @import("wire_types");

test "required renamed fields round trip through slice value and composed objects" {
    const a = std.testing.allocator;
    inline for (.{ types.RequiredFields, types.ComposedFields }) |T| {
        const raw = if (T == types.RequiredFields)
            \\{"toolUseId":"c-1","content-block-index":7,"error":"none"}
        else
            \\{"toolUseId":"c-1","content-block-index":7,"error":"none","stopReason":"end_turn"}
        ;
        var from_slice = try std.json.parseFromSlice(T, a, raw, .{});
        defer from_slice.deinit();
        try std.testing.expectEqualStrings("c-1", from_slice.value.tool_use_id);
        try std.testing.expectEqual(@as(i64, 7), from_slice.value.content_block_index);
        try std.testing.expectEqualStrings("none", from_slice.value.@"error");
        var json = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer json.deinit();
        var from_value = try std.json.parseFromValue(T, a, json.value, .{});
        defer from_value.deinit();
        const serialized = try std.json.Stringify.valueAlloc(a, from_value.value, .{});
        defer a.free(serialized);
        try std.testing.expectEqualStrings(raw, serialized);
        try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(T, a, "{\"tool_use_id\":\"c-1\",\"content-block-index\":7,\"error\":\"none\"}", .{}));
    }
}

test "closed tagged unions retain one member while nested fields can evolve" {
    const a = std.testing.allocator;
    var valid = try std.json.parseFromSlice(types.ContentUnion, a,
        \\{"toolUse":{"toolUseId":"c","content-block-index":0,"error":"none","future":true}}
    , .{ .ignore_unknown_fields = true });
    defer valid.deinit();
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(types.ContentUnion, a,
        \\{"text":"hi","toolUse":{"toolUseId":"c","content-block-index":0,"error":"none"}}
    , .{ .ignore_unknown_fields = true }));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(types.ContentUnion, a, "{}", .{ .ignore_unknown_fields = true }));
}

test "untyped JSON preserves absent null and scalar documents through both parsers" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{\"requiredDocument\":null}",
        "{\"requiredDocument\":null,\"optionalDocument\":null,\"inlineDocument\":null}",
        "{\"requiredDocument\":true,\"optionalDocument\":[1,false],\"inlineDocument\":\"text\"}",
        "{\"requiredDocument\":{\"nested\":null},\"optionalDocument\":42}",
    }) |raw| {
        var json = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer json.deinit();
        inline for (.{ false, true }) |from_value| {
            var parsed = if (from_value)
                try std.json.parseFromValue(types.DocumentEnvelope, a, json.value, .{})
            else
                try std.json.parseFromSlice(types.DocumentEnvelope, a, raw, .{});
            defer parsed.deinit();
            const serialized = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
            defer a.free(serialized);
            try std.testing.expectEqualStrings(raw, serialized);
            if (!json.value.object.contains("optionalDocument")) {
                try std.testing.expect(parsed.value.optional_document == .absent);
            } else if (json.value.object.get("optionalDocument").? == .null) {
                try std.testing.expect(parsed.value.optional_document == .null_value);
            }
        }
    }
    try std.testing.expectError(error.MissingField, std.json.parseFromSlice(types.DocumentEnvelope, a, "{}", .{}));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(types.DocumentEnvelope, a, "{\"requiredDocument\":null,\"objectOnly\":null}", .{}));
}
