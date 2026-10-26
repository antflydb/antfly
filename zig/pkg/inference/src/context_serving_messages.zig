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

pub const schema_version: u32 = 1;
pub const tool_history_schema_version: u32 = 2;
pub const max_messages: usize = 64;
pub const max_parts_per_message: usize = 16;
pub const max_total_text_bytes: usize = 8 * 1024 * 1024;
pub const max_tool_calls_per_message: usize = 8;
pub const max_total_tool_calls: usize = 32;
pub const max_tool_id_bytes: usize = 128;
pub const max_tool_name_bytes: usize = 128;
pub const max_tool_arguments_bytes: usize = 4 * 1024;
pub const max_tool_argument_values: usize = 256;

pub const Role = enum(u8) { system = 1, user = 2, assistant = 3, tool = 4 };
pub const Part = struct { text: []const u8, selected: bool = false };
pub const ToolFunction = struct { name: []const u8, arguments_json: []const u8 };
pub const ToolCall = struct {
    id: []const u8,
    type: []const u8 = "function",
    function: ToolFunction,
};
pub const Message = struct {
    role: Role,
    parts: []const Part,
    content_is_null: bool = false,
    tool_calls: ?[]const ToolCall = null,
    tool_call_id: ?[]const u8 = null,
};
pub const Artifact = struct { schema_version: u32, messages: []const Message };

fn boundedUtf8(value: []const u8, max: usize) bool {
    return value.len > 0 and value.len <= max and std.unicode.utf8ValidateSlice(value);
}

fn validateRenderableArguments(value: std.json.Value, depth: usize, count: *usize) !void {
    count.* = std.math.add(usize, count.*, 1) catch return error.InvalidOrderedMessages;
    if (count.* > max_tool_argument_values) return error.InvalidOrderedMessages;
    switch (value) {
        .number_string => return error.InvalidOrderedMessages,
        .float => |number| if (!std.math.isFinite(number))
            return error.InvalidOrderedMessages,
        .array => |array| {
            if (depth > 64) return error.InvalidOrderedMessages;
            for (array.items) |item|
                try validateRenderableArguments(item, depth + 1, count);
        },
        .object => |object| {
            if (depth > 64) return error.InvalidOrderedMessages;
            var iterator = object.iterator();
            while (iterator.next()) |entry|
                try validateRenderableArguments(entry.value_ptr.*, depth + 1, count);
        },
        else => {},
    }
}

fn validateArguments(bytes: []const u8) !void {
    if (bytes.len == 0 or bytes.len > max_tool_arguments_bytes or !std.unicode.utf8ValidateSlice(bytes))
        return error.InvalidOrderedMessages;
    // Tool arguments are validation-only. A fixed arena bounds object-key
    // storage and keeps parsing out of request accounting. The parsed tree
    // also provides the cross-language 64-container nesting check.
    var storage: [64 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var parsed = std.json.parseFromSlice(std.json.Value, fixed.allocator(), bytes, .{
        .allocate = .alloc_if_needed,
        .duplicate_field_behavior = .@"error",
        .parse_numbers = true,
    }) catch return error.InvalidOrderedMessages;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidOrderedMessages;
    var value_count: usize = 0;
    try validateRenderableArguments(parsed.value, 1, &value_count);
}

fn hasOnly(object: std.json.ObjectMap, allowed: []const []const u8) bool {
    var iterator = object.iterator();
    while (iterator.next()) |entry| {
        var found = false;
        for (allowed) |name| if (std.mem.eql(u8, entry.key_ptr.*, name)) {
            found = true;
            break;
        };
        if (!found) return false;
    }
    return true;
}

fn validateRawShape(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{
        .allocate = .alloc_always,
        .duplicate_field_behavior = .@"error",
        .parse_numbers = true,
    }) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidOrderedMessages,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidOrderedMessages;
    const root = parsed.value.object;
    if (root.count() != 2 or root.get("schema_version") == null or root.get("messages") == null or
        root.get("schema_version").? != .integer or root.get("messages").? != .array)
        return error.InvalidOrderedMessages;
    const version = root.get("schema_version").?.integer;
    if (version != @as(i64, schema_version) and version != @as(i64, tool_history_schema_version))
        return error.InvalidOrderedMessages;
    for (root.get("messages").?.array.items) |message_value| {
        if (message_value != .object) return error.InvalidOrderedMessages;
        const message = message_value.object;
        if (message.get("role") == null or message.get("parts") == null or message.get("role").? != .string)
            return error.InvalidOrderedMessages;
        if (version == schema_version) {
            if (message.count() != 2 or !hasOnly(message, &.{ "role", "parts" }))
                return error.InvalidOrderedMessages;
            continue;
        }
        const role = message.get("role").?.string;
        if (std.mem.eql(u8, role, "assistant") and message.contains("tool_calls")) {
            if (message.count() != 4 or !hasOnly(message, &.{ "role", "parts", "content_is_null", "tool_calls" }))
                return error.InvalidOrderedMessages;
        } else if (std.mem.eql(u8, role, "tool")) {
            if (message.count() != 3 or !hasOnly(message, &.{ "role", "parts", "tool_call_id" }))
                return error.InvalidOrderedMessages;
        } else if (message.count() != 2 or !hasOnly(message, &.{ "role", "parts" })) {
            return error.InvalidOrderedMessages;
        }
        const calls_value = message.get("tool_calls") orelse continue;
        if (calls_value != .array) return error.InvalidOrderedMessages;
        if (calls_value.array.items.len == 0) return error.InvalidOrderedMessages;
        for (calls_value.array.items) |call_value| {
            if (call_value != .object) return error.InvalidOrderedMessages;
            const call = call_value.object;
            if (call.count() != 3 or !hasOnly(call, &.{ "id", "type", "function" }) or
                call.get("id") == null or call.get("type") == null or call.get("function") == null or
                call.get("function").? != .object)
                return error.InvalidOrderedMessages;
            const function = call.get("function").?.object;
            if (function.count() != 2 or !hasOnly(function, &.{ "name", "arguments_json" }) or
                function.get("name") == null or function.get("arguments_json") == null)
                return error.InvalidOrderedMessages;
        }
    }
}

pub fn validate(artifact: Artifact) !void {
    if ((artifact.schema_version != schema_version and artifact.schema_version != tool_history_schema_version) or artifact.messages.len == 0 or
        artifact.messages.len > max_messages) return error.InvalidOrderedMessages;
    var total: usize = 0;
    var selected: usize = 0;
    var total_tool_calls: usize = 0;
    for (artifact.messages, 0..) |message, message_index| {
        if (message.parts.len > max_parts_per_message)
            return error.InvalidOrderedMessages;
        if (artifact.schema_version == schema_version and
            (message.parts.len == 0 or message.content_is_null or message.tool_calls != null or message.tool_call_id != null))
            return error.InvalidOrderedMessages;
        if (artifact.schema_version == tool_history_schema_version) {
            if (message.role == .assistant and message.tool_calls != null) {
                const calls = message.tool_calls.?;
                if (!message.content_is_null or message.parts.len != 0 or calls.len == 0 or
                    calls.len > max_tool_calls_per_message or message.tool_call_id != null)
                    return error.InvalidOrderedMessages;
                total_tool_calls = std.math.add(usize, total_tool_calls, calls.len) catch
                    return error.InvalidOrderedMessages;
                if (total_tool_calls > max_total_tool_calls or message_index + calls.len >= artifact.messages.len)
                    return error.InvalidOrderedMessages;
                for (calls, 0..) |call, call_index| {
                    if (!boundedUtf8(call.id, max_tool_id_bytes) or !std.mem.eql(u8, call.type, "function") or
                        !boundedUtf8(call.function.name, max_tool_name_bytes))
                        return error.InvalidOrderedMessages;
                    try validateArguments(call.function.arguments_json);
                    for (calls[0..call_index]) |prior|
                        if (std.mem.eql(u8, prior.id, call.id)) return error.InvalidOrderedMessages;
                    for (artifact.messages[0..message_index]) |prior_message| if (prior_message.tool_calls) |prior_calls|
                        for (prior_calls) |prior|
                            if (std.mem.eql(u8, prior.id, call.id)) return error.InvalidOrderedMessages;
                    const response = artifact.messages[message_index + 1 + call_index];
                    if (response.role != .tool or response.tool_call_id == null or
                        !std.mem.eql(u8, response.tool_call_id.?, call.id))
                        return error.InvalidOrderedMessages;
                }
            } else if (message.role == .tool) {
                if (message.content_is_null or message.parts.len == 0 or message.tool_calls != null or
                    message.tool_call_id == null or !boundedUtf8(message.tool_call_id.?, max_tool_id_bytes))
                    return error.InvalidOrderedMessages;
                var matched = false;
                if (message_index > 0) {
                    var cursor = message_index;
                    while (cursor > 0) {
                        cursor -= 1;
                        const candidate = artifact.messages[cursor];
                        if (candidate.role == .tool) continue;
                        if (candidate.role == .assistant and candidate.tool_calls != null) {
                            const offset = message_index - cursor - 1;
                            if (offset < candidate.tool_calls.?.len and
                                std.mem.eql(u8, candidate.tool_calls.?[offset].id, message.tool_call_id.?)) matched = true;
                        }
                        break;
                    }
                }
                if (!matched) return error.InvalidOrderedMessages;
            } else if (message.content_is_null or message.parts.len == 0 or
                message.tool_calls != null or message.tool_call_id != null)
                return error.InvalidOrderedMessages;
        }
        for (message.parts) |part| {
            if (part.text.len == 0 or !std.unicode.utf8ValidateSlice(part.text))
                return error.InvalidOrderedMessages;
            total = std.math.add(usize, total, part.text.len) catch return error.InvalidOrderedMessages;
            if (part.selected) {
                if (message.role != .user and message.role != .tool)
                    return error.InvalidOrderedMessages;
                selected += 1;
            }
        }
    }
    if (total == 0 or total > max_total_text_bytes or selected == 0 or selected > 64)
        return error.InvalidOrderedMessages;
}

pub fn parseOwned(allocator: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Artifact) {
    try validateRawShape(allocator, bytes);
    var parsed = try std.json.parseFromSlice(Artifact, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    });
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

pub fn semanticDigest(artifact: Artifact) ![32]u8 {
    try validate(artifact);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(if (artifact.schema_version == schema_version)
        "antfly.context-serving.structured-messages.v1\x00"
    else
        "antfly.context-serving.structured-messages.v2\x00");
    var word: [8]u8 = undefined;
    std.mem.writeInt(u32, word[0..4], @intCast(artifact.messages.len), .little);
    hash.update(word[0..4]);
    for (artifact.messages) |message| {
        hash.update(&.{@backingInt(message.role)});
        std.mem.writeInt(u32, word[0..4], @intCast(message.parts.len), .little);
        hash.update(word[0..4]);
        for (message.parts) |part| {
            hash.update(&.{@intFromBool(part.selected)});
            std.mem.writeInt(u64, &word, @intCast(part.text.len), .little);
            hash.update(&word);
            hash.update(part.text);
        }
        if (artifact.schema_version == tool_history_schema_version) {
            hash.update(&.{@intFromBool(message.content_is_null)});
            const calls = message.tool_calls orelse &.{};
            std.mem.writeInt(u32, word[0..4], @intCast(calls.len), .little);
            hash.update(word[0..4]);
            for (calls) |call| {
                for ([_][]const u8{ call.id, call.type, call.function.name, call.function.arguments_json }) |field| {
                    std.mem.writeInt(u64, &word, @intCast(field.len), .little);
                    hash.update(&word);
                    hash.update(field);
                }
            }
            const tool_call_id = message.tool_call_id orelse "";
            std.mem.writeInt(u64, &word, @intCast(tool_call_id.len), .little);
            hash.update(&word);
            hash.update(tool_call_id);
        }
    }
    return hash.finalResult();
}

/// Identity of the selected source payload only. Unselected questions and
/// conversational wrapping deliberately do not affect this digest, so reuse
/// of one evaluation source cannot be disguised by changing the question.
pub fn selectedSourceDigest(artifact: Artifact) ![32]u8 {
    try validate(artifact);
    var count: u32 = 0;
    for (artifact.messages) |message| {
        for (message.parts) |part| {
            if (part.selected) count += 1;
        }
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.context-serving.selected-source.v1\x00");
    var word: [8]u8 = undefined;
    std.mem.writeInt(u32, word[0..4], count, .little);
    hash.update(word[0..4]);
    for (artifact.messages) |message| {
        for (message.parts) |part| {
            if (!part.selected) continue;
            std.mem.writeInt(u64, &word, @intCast(part.text.len), .little);
            hash.update(&word);
            hash.update(part.text);
        }
    }
    return hash.finalResult();
}

pub fn selectedPartDigests(artifact: Artifact, out: *[64][32]u8) ![]const [32]u8 {
    try validate(artifact);
    var count: usize = 0;
    for (artifact.messages) |message| {
        for (message.parts) |part| {
            if (!part.selected) continue;
            std.crypto.hash.sha2.Sha256.hash(part.text, &out[count], .{});
            count += 1;
        }
    }
    return out[0..count];
}

test "context ordered messages digest binds roles order text and selected targets" {
    const first = Artifact{ .schema_version = 1, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
        .{ .role = .assistant, .parts = &.{.{ .text = "stored" }} },
        .{ .role = .user, .parts = &.{.{ .text = "question" }} },
    } };
    const expected = try semanticDigest(first);
    var golden: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&golden, "7c3eaf2f4e5bb244a3ec01c5e2b068e29454582dcd963b2a2b4d040cb8606179");
    try std.testing.expectEqualSlices(u8, &golden, &expected);
    var changed = first;
    changed.messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
        .{ .role = .assistant, .parts = &.{.{ .text = "stored" }} },
        .{ .role = .user, .parts = &.{.{ .text = "question", .selected = true }} },
    };
    try std.testing.expect(!std.mem.eql(u8, &expected, &(try semanticDigest(changed))));
    changed = first;
    changed.messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
        .{ .role = .user, .parts = &.{.{ .text = "stored" }} },
        .{ .role = .user, .parts = &.{.{ .text = "question" }} },
    };
    try std.testing.expect(!std.mem.eql(u8, &expected, &(try semanticDigest(changed))));

    const source = try selectedSourceDigest(first);
    changed = first;
    changed.messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
        .{ .role = .assistant, .parts = &.{.{ .text = "stored differently" }} },
        .{ .role = .user, .parts = &.{.{ .text = "different question" }} },
    };
    try std.testing.expectEqualSlices(u8, &source, &(try selectedSourceDigest(changed)));
    changed.messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "different source", .selected = true }} },
        .{ .role = .assistant, .parts = &.{.{ .text = "stored" }} },
        .{ .role = .user, .parts = &.{.{ .text = "question" }} },
    };
    try std.testing.expect(!std.mem.eql(u8, &source, &(try selectedSourceDigest(changed))));

    var part_storage: [64][32]u8 = undefined;
    const parts = try selectedPartDigests(first, &part_storage);
    try std.testing.expectEqual(@as(usize, 1), parts.len);
    var raw_document: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("document", &raw_document, .{});
    try std.testing.expectEqualSlices(u8, &raw_document, &parts[0]);
}

test "context ordered messages reject empty oversized and targetless artifacts" {
    try std.testing.expectError(error.InvalidOrderedMessages, validate(.{ .schema_version = 1, .messages = &.{} }));
    try std.testing.expectError(error.InvalidOrderedMessages, validate(.{ .schema_version = 1, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "question" }} },
    } }));
    try std.testing.expectError(error.InvalidOrderedMessages, validate(.{ .schema_version = 3, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
    } }));
}

test "context ordered messages JSON rejects unknown and duplicate fields" {
    const valid =
        "{\"schema_version\":1,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"document\",\"selected\":true}]}]}";
    var parsed = try std.json.parseFromSlice(Artifact, std.testing.allocator, valid, .{
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();
    try validate(parsed.value);
    try std.testing.expectError(
        error.UnknownField,
        std.json.parseFromSlice(
            Artifact,
            std.testing.allocator,
            "{\"schema_version\":1,\"unknown\":0,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"document\",\"selected\":true}]}]}",
            .{ .ignore_unknown_fields = false },
        ),
    );
    try std.testing.expectError(
        error.DuplicateField,
        std.json.parseFromSlice(
            Artifact,
            std.testing.allocator,
            "{\"schema_version\":1,\"schema_version\":1,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"document\",\"selected\":true}]}]}",
            .{ .ignore_unknown_fields = false },
        ),
    );
}

test "context ordered messages owned parse survives source release" {
    var parsed = parsed: {
        const source = try std.testing.allocator.dupe(u8, "{\"schema_version\":1,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"document\",\"selected\":true}]}]}");
        defer std.testing.allocator.free(source);
        break :parsed try parseOwned(std.testing.allocator, source);
    };
    defer parsed.deinit();
    const expected = try semanticDigest(parsed.value);
    try std.testing.expect(!std.mem.allEqual(u8, &expected, 0));
}

test "context ordered messages selected content requires an authoritative role" {
    try std.testing.expectError(error.InvalidOrderedMessages, validate(.{ .schema_version = 1, .messages = &.{
        .{ .role = .assistant, .parts = &.{.{ .text = "generated", .selected = true }} },
    } }));
}

fn toolHistoryArtifact(
    message_storage: *[4]Message,
    call_storage: *[1]ToolCall,
    call_id: []const u8,
    name: []const u8,
    arguments: []const u8,
) Artifact {
    call_storage.* = .{.{
        .id = call_id,
        .function = .{ .name = name, .arguments_json = arguments },
    }};
    message_storage.* = .{
        .{ .role = .user, .parts = &.{.{ .text = "read the source" }} },
        .{
            .role = .assistant,
            .parts = &.{},
            .content_is_null = true,
            .tool_calls = call_storage,
        },
        .{ .role = .tool, .parts = &.{.{ .text = "authoritative tool output", .selected = true }}, .tool_call_id = call_id },
        .{ .role = .user, .parts = &.{.{ .text = "return the result" }} },
    };
    return .{ .schema_version = tool_history_schema_version, .messages = message_storage };
}

test "context ordered tool history digest binds call id name and exact arguments" {
    var base_messages: [4]Message = undefined;
    var base_calls: [1]ToolCall = undefined;
    const base = toolHistoryArtifact(&base_messages, &base_calls, "call-1", "read_source_ranges", "{\"end\":9,\"start\":1}");
    const expected = try semanticDigest(base);
    var golden: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&golden, "aca13549c4e9eb3fac3c8d6d64d67d8eef55aab7f79063425db350009111d436");
    try std.testing.expectEqualSlices(u8, &golden, &expected);
    var changed_messages: [4]Message = undefined;
    var changed_calls: [1]ToolCall = undefined;
    try std.testing.expect(!std.mem.eql(u8, &expected, &(try semanticDigest(
        toolHistoryArtifact(&changed_messages, &changed_calls, "call-2", "read_source_ranges", "{\"end\":9,\"start\":1}"),
    ))));
    try std.testing.expect(!std.mem.eql(u8, &expected, &(try semanticDigest(
        toolHistoryArtifact(&changed_messages, &changed_calls, "call-1", "read_other", "{\"end\":9,\"start\":1}"),
    ))));
    try std.testing.expect(!std.mem.eql(u8, &expected, &(try semanticDigest(
        toolHistoryArtifact(&changed_messages, &changed_calls, "call-1", "read_source_ranges", "{\"end\":10,\"start\":1}"),
    ))));
    const source = try selectedSourceDigest(base);
    try std.testing.expectEqualSlices(u8, &source, &(try selectedSourceDigest(
        toolHistoryArtifact(&changed_messages, &changed_calls, "call-2", "read_other", "{\"end\":10,\"start\":1}"),
    )));
}

test "context ordered tool history rejects orphan duplicate and selected metadata" {
    const orphan = Artifact{ .schema_version = 2, .messages = &.{
        .{ .role = .tool, .parts = &.{.{ .text = "source", .selected = true }}, .tool_call_id = "missing" },
    } };
    try std.testing.expectError(error.InvalidOrderedMessages, validate(orphan));
    const duplicate = Artifact{ .schema_version = 2, .messages = &.{
        .{ .role = .assistant, .parts = &.{}, .content_is_null = true, .tool_calls = &.{
            .{ .id = "same", .function = .{ .name = "read", .arguments_json = "{}" } },
            .{ .id = "same", .function = .{ .name = "read", .arguments_json = "{}" } },
        } },
        .{ .role = .tool, .parts = &.{.{ .text = "one", .selected = true }}, .tool_call_id = "same" },
        .{ .role = .tool, .parts = &.{.{ .text = "two" }}, .tool_call_id = "same" },
    } };
    try std.testing.expectError(error.InvalidOrderedMessages, validate(duplicate));
    const selected_metadata = Artifact{ .schema_version = 2, .messages = &.{
        .{ .role = .assistant, .parts = &.{.{ .text = "metadata", .selected = true }}, .content_is_null = true, .tool_calls = &.{
            .{ .id = "call", .function = .{ .name = "read", .arguments_json = "{}" } },
        } },
        .{ .role = .tool, .parts = &.{.{ .text = "source", .selected = true }}, .tool_call_id = "call" },
    } };
    try std.testing.expectError(error.InvalidOrderedMessages, validate(selected_metadata));
    var messages: [4]Message = undefined;
    var calls: [1]ToolCall = undefined;
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        validate(toolHistoryArtifact(&messages, &calls, "call", "read", "[]")),
    );
}

test "context ordered tool history owned parse retains metadata" {
    var parsed = parsed: {
        const source = try std.testing.allocator.dupe(u8, "{\"schema_version\":2,\"messages\":[{\"role\":\"assistant\",\"parts\":[],\"content_is_null\":true,\"tool_calls\":[{\"id\":\"call\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments_json\":\"{}\"}}]},{\"role\":\"tool\",\"parts\":[{\"text\":\"source\",\"selected\":true}],\"tool_call_id\":\"call\"}]}");
        defer std.testing.allocator.free(source);
        break :parsed try parseOwned(std.testing.allocator, source);
    };
    defer parsed.deinit();
    try std.testing.expectEqualStrings("read", parsed.value.messages[0].tool_calls.?[0].function.name);
    try std.testing.expectEqualStrings("call", parsed.value.messages[1].tool_call_id.?);
}

test "context ordered raw schema preserves v1 shape and requires v2 type" {
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        parseOwned(std.testing.allocator, "{\"schema_version\":1,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"source\",\"selected\":true}],\"content_is_null\":false}]}"),
    );
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        parseOwned(std.testing.allocator, "{\"schema_version\":2,\"messages\":[{\"role\":\"assistant\",\"parts\":[],\"content_is_null\":true,\"tool_calls\":[{\"id\":\"call\",\"function\":{\"name\":\"read\",\"arguments_json\":\"{}\"}}]},{\"role\":\"tool\",\"parts\":[{\"text\":\"source\",\"selected\":true}],\"tool_call_id\":\"call\"}]}"),
    );
    const aliases = [_][]const u8{
        "{\"schema_version\":2,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"source\",\"selected\":true}]},{\"role\":\"assistant\",\"parts\":[{\"text\":\"ordinary\"}],\"content_is_null\":false,\"tool_calls\":null}]}",
        "{\"schema_version\":2,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"source\",\"selected\":true}],\"content_is_null\":false}]}",
        "{\"schema_version\":2,\"messages\":[{\"role\":\"assistant\",\"parts\":[],\"content_is_null\":true,\"tool_calls\":[{\"id\":\"call\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments_json\":\"{}\"}}]},{\"role\":\"tool\",\"parts\":[{\"text\":\"source\",\"selected\":true}],\"tool_call_id\":null}]}",
    };
    for (aliases) |raw| try std.testing.expectError(
        error.InvalidOrderedMessages,
        parseOwned(std.testing.allocator, raw),
    );
    var empty_storage: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&empty_storage);
    try std.testing.expectError(
        error.OutOfMemory,
        parseOwned(fixed.allocator(), "{\"schema_version\":1,\"messages\":[{\"role\":\"user\",\"parts\":[{\"text\":\"source\",\"selected\":true}]}]}"),
    );
}

test "context ordered tool arguments require renderable numbers and bounded nesting" {
    var messages: [4]Message = undefined;
    var calls: [1]ToolCall = undefined;
    try validate(toolHistoryArtifact(&messages, &calls, "call", "read", "{\"min\":-9223372036854775808,\"max\":9223372036854775807,\"finite\":1.25e3}"));
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        validate(toolHistoryArtifact(&messages, &calls, "call", "read", "{\"value\":9223372036854775808}")),
    );
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        validate(toolHistoryArtifact(&messages, &calls, "call", "read", "{\"value\":-9223372036854775809}")),
    );
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        validate(toolHistoryArtifact(&messages, &calls, "call", "read", "{\"value\":1e999}")),
    );
    const depth64 = "{\"value\":" ++ @as([63]u8, @splat('[')) ++ "0" ++ @as([63]u8, @splat(']')) ++ "}";
    try validate(toolHistoryArtifact(&messages, &calls, "call", "read", depth64));
    const depth65 = "{\"value\":" ++ @as([64]u8, @splat('[')) ++ "0" ++ @as([64]u8, @splat(']')) ++ "}";
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        validate(toolHistoryArtifact(&messages, &calls, "call", "read", depth65)),
    );
    const prefix = "{\"values\":[\"";
    const suffix = "\"]}";
    var wide: [max_tool_arguments_bytes - 1]u8 = undefined;
    @memcpy(wide[0..prefix.len], prefix);
    @memset(wide[prefix.len .. wide.len - suffix.len], 'a');
    @memcpy(wide[wide.len - suffix.len ..], suffix);
    try validate(toolHistoryArtifact(&messages, &calls, "call", "read", &wide));
    const many_prefix = "{\"values\":[";
    const many_suffix = "0]}";
    var too_many_values: [many_prefix.len + 2 * max_tool_argument_values + many_suffix.len]u8 = undefined;
    @memcpy(too_many_values[0..many_prefix.len], many_prefix);
    var cursor = many_prefix.len;
    for (0..max_tool_argument_values) |_| {
        too_many_values[cursor] = '0';
        too_many_values[cursor + 1] = ',';
        cursor += 2;
    }
    @memcpy(too_many_values[cursor..], many_suffix);
    try std.testing.expectError(
        error.InvalidOrderedMessages,
        validate(toolHistoryArtifact(&messages, &calls, "call", "read", &too_many_values)),
    );
}
