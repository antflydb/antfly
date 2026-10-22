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

//! Public named decision envelope. Evidence and context expansion are validated
//! for the entire batch before the runtime reserves budgets or dispatches work.
const std = @import("std");
const d = @import("decisions.zig");
const Json = std.json.Value;
const max_input_bytes = 1024 * 1024;
const max_batch_bytes = 16 * max_input_bytes;
pub const Item = struct { id: ?[]const u8 = null, input: d.Input };
pub const Batch = struct { items: []Item, batched: bool };

pub fn parse(a: std.mem.Allocator, request: Json) !Batch {
    if (request != .object) return error.InvalidDecisionSpecification;
    const root = request.object;
    for (root.keys()) |key| {
        for ([_][]const u8{ "model", "input", "inputs", "questions", "context_version", "contexts" }) |allowed| {
            if (std.mem.eql(u8, key, allowed)) break;
        } else return error.InvalidDecisionSpecification;
    }
    const batched = root.contains("inputs");
    if (batched == root.contains("input")) return error.InvalidDecisionSpecification;
    const contexts: []const Json = if (root.get("context_version")) |version| blk: {
        if (!batched or version != .integer or version.integer != 1) return error.InvalidDecisionSpecification;
        const values = root.get("contexts") orelse return error.InvalidDecisionSpecification;
        if (values != .array) return error.InvalidDecisionSpecification;
        if (values.array.items.len > 128) return error.DecisionLimitExceeded;
        for (values.array.items) |value| if ((try d.text(value)).len > max_input_bytes) return error.DecisionLimitExceeded;
        break :blk values.array.items;
    } else blk: {
        if (root.contains("contexts")) return error.InvalidDecisionSpecification;
        break :blk &.{};
    };
    if (!batched) {
        const input = try d.Input.fromJson(root.get("input").?);
        try input.checkLimit(a, max_input_bytes);
        return .{ .items = try a.dupe(Item, &.{.{ .input = input }}), .batched = false };
    }
    const entries = root.get("inputs").?;
    if (entries != .array) return error.InvalidDecisionSpecification;
    if (entries.array.items.len == 0 or entries.array.items.len > 128) return error.DecisionLimitExceeded;
    const items = try a.alloc(Item, entries.array.items.len);
    var total: usize = 0;
    for (entries.array.items, items) |entry, *item| {
        if (entry != .object) return error.InvalidDecisionSpecification;
        for (entry.object.keys()) |key| {
            if (!std.mem.eql(u8, key, "input") and !std.mem.eql(u8, key, "id") and !std.mem.eql(u8, key, "context_index")) return error.InvalidDecisionSpecification;
        }
        item.* = .{
            .id = if (entry.object.get("id")) |value| try d.text(value) else null,
            .input = try d.Input.fromJson(entry.object.get("input") orelse return error.InvalidDecisionSpecification),
        };
        if (entry.object.get("context_index")) |index| {
            if (!root.contains("context_version") or index != .integer or index.integer < 0 or index.integer >= contexts.len) return error.InvalidDecisionSpecification;
            const context = contexts[@intCast(index.integer)].string;
            if (item.input == .text) {
                if (context.len + 1 > max_input_bytes -| item.input.text.len) return error.DecisionLimitExceeded;
                item.input = .{ .text = try std.fmt.allocPrint(a, "{s}\n{s}", .{ context, item.input.text }) };
            } else {
                var message = d.jsonObject();
                try d.put(a, &message, "role", .{ .string = "user" });
                try d.put(a, &message, "content", .{ .string = context });
                const messages = try a.alloc(Json, item.input.messages.len + 1);
                messages[0] = message;
                @memcpy(messages[1..], item.input.messages);
                item.input = .{ .messages = messages };
            }
        }
        const bytes = try item.input.byteSize(a);
        if (bytes > max_input_bytes or bytes > max_batch_bytes - total) return error.DecisionLimitExceeded;
        total += bytes;
    }
    return .{ .items = items, .batched = true };
}

test "decision functions public batches preserve evidence and expand shared contexts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try std.json.parseFromSliceLeaky(Json, a,
        \\{"context_version":1,"contexts":["Shared context"],"inputs":[{"id":"text","input":"Inspect","context_index":0},{"id":"photo","context_index":0,"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.org/a.png"}]}]}]}
    , .{});
    const batch = try parse(a, request);
    try std.testing.expect(batch.batched);
    try std.testing.expectEqualStrings("Shared context\nInspect", batch.items[0].input.text);
    try std.testing.expectEqualStrings("photo", batch.items[1].id.?);
    try std.testing.expectEqualStrings("Shared context", batch.items[1].input.messages[0].object.get("content").?.string);
    try std.testing.expectEqual(@as(usize, 1), batch.items[1].input.imageCount());
    try std.testing.expectEqual(@as(usize, 2), batch.items[1].input.messages.len);
    for ([_][]const u8{
        "{}",                                                   "{\"inputs\":[]}",                        "{\"input\":\"x\",\"inputs\":[]}",
        "{\"inputs\":[{\"input\":\"x\",\"context_index\":0}]}", "{\"input\":\"x\",\"contexts\":[\"x\"]}", "{\"inputs\":[{\"input\":\"x\"}],\"contexts\":[\"x\"],\"context_version\":2}",
    }) |invalid| {
        const value = try std.json.parseFromSliceLeaky(Json, a, invalid, .{});
        if (parse(a, value)) |_| return error.TestExpectedError else |_| {}
    }
}

test "decision functions public batch byte limits include repeated expanded contexts without allocating serialized evidence" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const text = try alloc.alloc(u8, max_input_bytes);
    @memset(text, 'x');
    var entry = d.jsonObject();
    try d.put(alloc, &entry, "input", .{ .string = text });
    var inputs: Json = .{ .array = std.array_list.Managed(Json).init(alloc) };
    for (0..16) |_| try inputs.array.append(entry);
    var request = d.jsonObject();
    try d.put(alloc, &request, "inputs", inputs);
    try std.testing.expectEqual(@as(usize, 16), (try parse(alloc, request)).items.len);
    try inputs.array.append(entry);
    try d.put(alloc, &request, "inputs", inputs);
    try std.testing.expectError(error.DecisionLimitExceeded, parse(alloc, request));
    const evidence = try d.Input.fromJson(try std.json.parseFromSliceLeaky(Json, alloc,
        \\[{"role":"user","content":[{"type":"input_text","text":"hello\nworld"},{"type":"input_image","image_url":"https://example.org/a.png"}]}]
    , .{}));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const bytes = try std.json.Stringify.valueAlloc(alloc, evidence.jsonValue(alloc), .{});
    try std.testing.expectEqual(bytes.len, try evidence.byteSize(failing.allocator()));
}
