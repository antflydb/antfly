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

//! Shared constrained-output contract. Callers own the request arena and
//! provider execution; this layer owns item identity, validation and repair.
const std = @import("std");
const schema_validator = @import("antfly_jsonschema");
pub const extraction = @import("generated_extractor.zig");
pub const token_evidence = @import("token_evidence.zig");
const Json = std.json.Value;
const Allocator = std.mem.Allocator;

pub const Output = enum { auto, tool_call, json_schema, grammar };
pub const Probabilities = enum { token_logprobs, verdict_only };
pub const Batch = struct {
    max_items_per_call: u16 = 8,
    max_input_bytes: u32 = 65536,
    pub fn validate(self: Batch) !void {
        if (self.max_items_per_call == 0 or self.max_items_per_call > 256 or
            self.max_input_bytes == 0 or self.max_input_bytes > 4 * 1024 * 1024)
            return error.InvalidConstrainedGenerationConfig;
    }
};
/// These are resolved executor/model facts, never caller-supplied claims.
pub const Capabilities = struct {
    tool_call: bool = false,
    json_schema: bool = false,
    grammar: bool = false,
    token_logprobs: bool = false,
    tool_call_token_logprobs: bool = false,
    model_revision: ?[]const u8 = null,

    pub fn select(self: Capabilities, output: Output, probabilities: ?Probabilities) !Output {
        if (probabilities == .token_logprobs and !self.token_logprobs)
            return error.UnsupportedConstrainedProbabilities;
        if (output == .auto) {
            if (self.tool_call and (probabilities != .token_logprobs or self.tool_call_token_logprobs)) return .tool_call;
            if (self.json_schema) return .json_schema;
            if (self.grammar) return .grammar;
        } else if (switch (output) {
            .tool_call => self.tool_call and (probabilities != .token_logprobs or self.tool_call_token_logprobs),
            .json_schema => self.json_schema,
            .grammar => self.grammar,
            .auto => unreachable,
        }) return output;
        return error.UnsupportedConstrainedOutput;
    }
};

pub const Item = struct { id: usize, input: []const u8 };
pub const Invocation = struct {
    mode: Output,
    schema: Json,
    instructions: []const u8,
    items: []const Item,
    validation_error: ?[]const u8 = null,
};
pub const TokenEvidence = struct {
    ptr: *const anyopaque,
    get_distribution: *const fn (*const anyopaque, Allocator, usize, []const []const u8, []const Json) anyerror![]f64,
    pub fn distribution(self: TokenEvidence, a: Allocator, item: usize, path: []const []const u8, values: []const Json) ![]f64 {
        return self.get_distribution(self.ptr, a, item, path, values);
    }
};
pub const Reply = struct {
    /// {"items":[{"item":<original position>,"value":<answer>}, ...]}
    value: Json,
    input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    /// Trusted token evidence, populated by an adapter, never model JSON.
    token_evidence: ?TokenEvidence = null,
};
pub const Executor = struct {
    ptr: *anyopaque,
    invoke: *const fn (*anyopaque, Allocator, Invocation) anyerror!Reply,
    checkpoint: ?*const fn (*anyopaque) anyerror!void = null,
    validate_item: ?*const fn (*anyopaque, Allocator, usize, Json, ?TokenEvidence) anyerror!void = null,
};
pub const Report = struct { native_batches: usize = 0, native_items: usize = 0, serial_items: usize = 0, rejected_items: usize = 0 };
pub const Result = struct {
    execution: Report = .{},
    values: []?Json,
    failures: []?anyerror = &.{},
    token_evidence: []?TokenEvidence,
    input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    calls: usize = 0,
};

pub fn object() Json {
    return .{ .object = .empty };
}
pub fn put(a: Allocator, value: *Json, name: []const u8, entry: Json) !void {
    try value.object.put(a, name, entry);
}
pub fn array(a: Allocator, entries: []const Json) !Json {
    var value: Json = .{ .array = std.json.Array.init(a) };
    try value.array.appendSlice(entries);
    return value;
}

/// Admit only the schema vocabulary enforced by the common validator. Unknown
/// keywords are rejected rather than silently weakening a producer contract.
pub fn validateSchema(schema: Json) !void {
    var nodes: usize = 0;
    try validateSchemaAt(schema, 0, &nodes);
}
fn validateSchemaAt(schema: Json, depth: usize, nodes: *usize) anyerror!void {
    nodes.* += 1;
    if (depth > 24 or nodes.* > 4096 or schema != .object) return error.InvalidConstrainedSchema;
    for (schema.object.keys(), schema.object.values()) |key, value| {
        if (std.mem.eql(u8, key, "type")) {
            if (value != .string) return error.UnsupportedConstrainedSchema;
            var valid = false;
            for ([_][]const u8{ "object", "array", "string", "number", "integer", "boolean", "null" }) |kind| if (std.mem.eql(u8, kind, value.string)) {
                valid = true;
            };
            if (!valid) return error.InvalidConstrainedSchema;
        } else if (std.mem.eql(u8, key, "properties")) {
            if (value != .object or value.object.count() > 256) return error.InvalidConstrainedSchema;
            for (value.object.values()) |child| try validateSchemaAt(child, depth + 1, nodes);
        } else if (std.mem.eql(u8, key, "items")) {
            try validateSchemaAt(value, depth + 1, nodes);
        } else if (std.mem.eql(u8, key, "additionalProperties")) {
            if (value == .object) try validateSchemaAt(value, depth + 1, nodes) else if (value != .bool) return error.InvalidConstrainedSchema;
        } else if (std.mem.eql(u8, key, "required")) {
            if (value != .array) return error.InvalidConstrainedSchema;
            for (value.array.items, 0..) |entry, i| {
                if (entry != .string) return error.InvalidConstrainedSchema;
                for (value.array.items[0..i]) |previous| if (std.mem.eql(u8, previous.string, entry.string)) return error.InvalidConstrainedSchema;
            }
        } else if (std.mem.eql(u8, key, "anyOf") or std.mem.eql(u8, key, "oneOf") or std.mem.eql(u8, key, "allOf")) {
            if (value != .array or value.array.items.len == 0 or value.array.items.len > 64) return error.InvalidConstrainedSchema;
            for (value.array.items) |child| try validateSchemaAt(child, depth + 1, nodes);
        } else if (std.mem.eql(u8, key, "enum")) {
            if (value != .array or value.array.items.len == 0 or value.array.items.len > 256) return error.InvalidConstrainedSchema;
        } else if (std.mem.eql(u8, key, "uniqueItems")) {
            if (value != .bool) return error.InvalidConstrainedSchema;
        } else if (std.mem.eql(u8, key, "minimum") or std.mem.eql(u8, key, "maximum") or std.mem.eql(u8, key, "exclusiveMinimum") or std.mem.eql(u8, key, "exclusiveMaximum")) {
            if (value != .integer and (value != .float or !std.math.isFinite(value.float))) return error.InvalidConstrainedSchema;
        } else if (std.mem.eql(u8, key, "minItems") or std.mem.eql(u8, key, "maxItems") or std.mem.eql(u8, key, "minLength") or std.mem.eql(u8, key, "maxLength")) {
            if (value != .integer or value.integer < 0 or value.integer > 65536) return error.InvalidConstrainedSchema;
        } else if (std.mem.eql(u8, key, "title") or std.mem.eql(u8, key, "description")) {
            if (value != .string) return error.InvalidConstrainedSchema;
        } else if (!std.mem.eql(u8, key, "const")) return error.UnsupportedConstrainedSchema;
    }
}

/// OpenAI-compatible strict tools require an object root. The envelope keeps
/// item identity explicit and also works for native grammar and provider JSON
/// schema output. Actual producer artifacts contain only each item's value.
pub fn batchSchema(a: Allocator, value_schema: Json, items: []const Item) !Json {
    var ids: Json = .{ .array = std.json.Array.init(a) };
    for (items) |item| try ids.array.append(.{ .integer = @intCast(item.id) });
    var id_schema = object();
    try put(a, &id_schema, "type", .{ .string = "integer" });
    try put(a, &id_schema, "enum", ids);
    var fields = object();
    try put(a, &fields, "item", id_schema);
    try put(a, &fields, "value", value_schema);
    var entry = object();
    try put(a, &entry, "type", .{ .string = "object" });
    try put(a, &entry, "properties", fields);
    try put(a, &entry, "required", try array(a, &.{ .{ .string = "item" }, .{ .string = "value" } }));
    try put(a, &entry, "additionalProperties", .{ .bool = false });
    var entries = object();
    try put(a, &entries, "type", .{ .string = "array" });
    try put(a, &entries, "items", entry);
    try put(a, &entries, "minItems", .{ .integer = @intCast(items.len) });
    try put(a, &entries, "maxItems", .{ .integer = @intCast(items.len) });
    var root = object();
    var properties = object();
    try put(a, &properties, "items", entries);
    try put(a, &root, "type", .{ .string = "object" });
    try put(a, &root, "properties", properties);
    try put(a, &root, "required", try array(a, &.{.{ .string = "items" }}));
    try put(a, &root, "additionalProperties", .{ .bool = false });
    return root;
}

/// A request arena bounds all parsing, schemas and retained answers. Successful
/// items survive malformed siblings and are never sent to the repair call.
/// Operational failures belong only to unanswered items; consumers apply their
/// ordinary failure policy. Cancellation and allocation failures abort work.
pub fn execute(a: Allocator, executor: Executor, mode: Output, value_schema: Json, instructions: []const u8, batch: Batch, inputs: []const []const u8) !Result {
    try batch.validate();
    try validateSchema(value_schema);
    if (mode == .auto or value_schema != .object) return error.InvalidConstrainedGenerationConfig;
    var result = Result{
        .values = try a.alloc(?Json, inputs.len),
        .failures = try a.alloc(?anyerror, inputs.len),
        .token_evidence = try a.alloc(?TokenEvidence, inputs.len),
    };
    @memset(result.values, null);
    @memset(result.failures, null);
    @memset(result.token_evidence, null);
    var begin: usize = 0;
    while (begin < inputs.len) {
        if (inputs[begin].len > batch.max_input_bytes) {
            result.failures[begin] = error.ConstrainedInputTooLarge;
            result.execution.rejected_items += 1;
            begin += 1;
            continue;
        }
        var pending: std.ArrayList(Item) = .empty;
        var bytes: usize = 0;
        while (begin + pending.items.len < inputs.len and pending.items.len < batch.max_items_per_call) {
            const id = begin + pending.items.len;
            if (inputs[id].len > batch.max_input_bytes) break;
            if (inputs[id].len > batch.max_input_bytes - bytes) break;
            bytes += inputs[id].len;
            try pending.append(a, .{ .id = id, .input = inputs[id] });
        }
        const end = begin + pending.items.len;
        var validation_error: ?[]const u8 = null;
        attempts: for (0..2) |attempt| {
            if (pending.items.len == 0) break;
            if (executor.checkpoint) |check| check(executor.ptr) catch |err| {
                if (err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled) return err;
                for (result.values, result.failures) |value, *failure| if (value == null) {
                    failure.* = err;
                };
                result.execution.rejected_items += inputs.len - end;
                if (attempt == 0) result.execution.rejected_items += pending.items.len;
                return result;
            };
            if (attempt == 0) {
                if (pending.items.len > 1) {
                    result.execution.native_batches += 1;
                    result.execution.native_items += pending.items.len;
                } else result.execution.serial_items += pending.items.len;
            }
            result.calls += 1;
            const reply = executor.invoke(executor.ptr, a, .{
                .mode = mode,
                .schema = try batchSchema(a, value_schema, pending.items),
                .instructions = instructions,
                .items = pending.items,
                .validation_error = if (attempt == 0) null else validation_error orelse "Return exactly one schema-valid result for each requested item, preserving the IDs.",
            }) catch |err| {
                if (err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled) return err;
                for (pending.items) |item| result.failures[item.id] = err;
                if (err == error.Timeout) {
                    for (end..inputs.len) |id| result.failures[id] = err;
                    result.execution.rejected_items += inputs.len - end;
                    return result;
                }
                break :attempts;
            };
            result.input_tokens +|= reply.input_tokens;
            result.output_tokens +|= reply.output_tokens;
            const rows = if (reply.value == .object) reply.value.object.get("items") else null;
            if (rows) |entries| if (entries == .array) {
                const duplicates = try a.alloc(bool, end - begin);
                @memset(duplicates, false);
                const seen = try a.alloc(bool, end - begin);
                @memset(seen, false);
                for (entries.array.items) |entry| {
                    const id = itemId(entry, begin, end) orelse continue;
                    if (seen[id - begin]) duplicates[id - begin] = true;
                    seen[id - begin] = true;
                }
                for (entries.array.items) |entry| {
                    const id = itemId(entry, begin, end) orelse continue;
                    if (duplicates[id - begin] or result.values[id] != null) continue;
                    const value = entry.object.get("value") orelse continue;
                    schema_validator.validateJsonSchemaValue(a, value_schema, value) catch |err| {
                        if (err == error.OutOfMemory) return err;
                        validation_error = try std.fmt.allocPrint(a, "Item {d}: {s}. Return exactly one valid result for each requested item.", .{ id, @errorName(err) });
                        continue;
                    };
                    if (executor.validate_item) |validate| validate(executor.ptr, a, id, value, reply.token_evidence) catch |err| {
                        if (err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled) return err;
                        validation_error = try std.fmt.allocPrint(a, "Item {d}: {s}. Supply complete structured token evidence and a valid value for each requested item.", .{ id, @errorName(err) });
                        continue;
                    };
                    result.values[id] = value;
                    result.token_evidence[id] = reply.token_evidence;
                }
            };
            var unanswered: std.ArrayList(Item) = .empty;
            for (pending.items) |item| if (result.values[item.id] == null) try unanswered.append(a, item);
            pending = unanswered;
            if (attempt == 1) for (pending.items) |item| {
                result.failures[item.id] = error.InvalidConstrainedOutput;
            };
        }
        begin = end;
    }
    return result;
}
fn itemId(entry: Json, begin: usize, end: usize) ?usize {
    if (entry != .object) return null;
    const id = entry.object.get("item") orelse return null;
    if (id != .integer or id.integer < 0) return null;
    const index = std.math.cast(usize, id.integer) orelse return null;
    return if (index >= begin and index < end) index else null;
}

/// Only trusted complete distributions can become probabilities. Missing
/// alternatives and non-finite logits are rejected rather than filled with 0.
pub fn normalizeLogprobs(a: Allocator, values: []const f64) ![]f64 {
    if (values.len == 0) return error.InvalidConstrainedProbabilities;
    var maximum: f64 = -std.math.inf(f64);
    for (values) |value| {
        if (!std.math.isFinite(value)) return error.InvalidConstrainedProbabilities;
        maximum = @max(maximum, value);
    }
    const probabilities = try a.alloc(f64, values.len);
    var total: f64 = 0;
    for (values, probabilities) |value, *probability| {
        probability.* = @exp(value - maximum);
        total += probability.*;
    }
    for (probabilities) |*probability| probability.* /= total;
    return probabilities;
}

test "asset producer runtime constrained output admission never silently substitutes verdict probabilities" {
    try std.testing.expectEqual(Output.grammar, try (Capabilities{ .grammar = true }).select(.auto, .verdict_only));
    try std.testing.expectError(error.UnsupportedConstrainedProbabilities, (Capabilities{ .grammar = true }).select(.auto, .token_logprobs));
    try std.testing.expectError(error.UnsupportedConstrainedOutput, (Capabilities{}).select(.auto, null));
    try std.testing.expectEqual(Output.tool_call, try (Capabilities{ .tool_call = true, .json_schema = true, .grammar = true }).select(.auto, null));
}

test "asset producer runtime constrained batches repair only unanswered items and preserve positional identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fake = struct {
        calls: usize = 0,
        fn invoke(ptr: *anyopaque, alloc: Allocator, request: Invocation) !Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 2), request.items.len);
                return .{ .value = try std.json.parseFromSliceLeaky(Json, alloc,
                    \\{"items":[{"item":0,"value":{"label":"yes"}},{"item":1,"value":{"label":"invalid"}}]}
                , .{}) };
            }
            try std.testing.expectEqual(@as(usize, 1), request.items.len);
            try std.testing.expectEqual(@as(usize, 1), request.items[0].id);
            try std.testing.expect(request.validation_error != null);
            return .{ .value = try std.json.parseFromSliceLeaky(Json, alloc,
                \\{"items":[{"item":1,"value":{"label":"no"}}]}
            , .{}) };
        }
    };
    var fake = Fake{};
    const schema = try std.json.parseFromSliceLeaky(Json, a,
        \\{"type":"object","properties":{"label":{"type":"string","enum":["yes","no"]}},"required":["label"],"additionalProperties":false}
    , .{});
    const result = try execute(a, .{ .ptr = &fake, .invoke = Fake.invoke }, .json_schema, schema, "", .{}, &.{ "one", "two" });
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("yes", result.values[0].?.object.get("label").?.string);
    try std.testing.expectEqualStrings("no", result.values[1].?.object.get("label").?.string);
}

test "asset producer runtime constrained semantic repair and upstream failure retain successful siblings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fake = struct {
        calls: usize = 0,
        fn invoke(ptr: *anyopaque, alloc: Allocator, request: Invocation) !Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) return .{ .value = try std.json.parseFromSliceLeaky(Json, alloc,
                \\{"items":[{"item":0,"value":true},{"item":1,"value":false}]}
            , .{}) };
            try std.testing.expectEqual(@as(usize, 1), request.items.len);
            try std.testing.expectEqual(@as(usize, 1), request.items[0].id);
            try std.testing.expect(request.validation_error != null);
            return error.Timeout;
        }
        fn validate(_: *anyopaque, _: Allocator, id: usize, _: Json, _: ?TokenEvidence) !void {
            if (id == 1) return error.InvalidConstrainedProbabilities;
        }
    };
    var fake = Fake{};
    const schema = try std.json.parseFromSliceLeaky(Json, a, "{\"type\":\"boolean\"}", .{});
    const result = try execute(a, .{ .ptr = &fake, .invoke = Fake.invoke, .validate_item = Fake.validate }, .grammar, schema, "", .{ .max_items_per_call = 2 }, &.{ "a", "b", "c" });
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expect(result.values[0].?.bool);
    try std.testing.expect(result.failures[0] == null);
    try std.testing.expectEqual(error.Timeout, result.failures[1].?);
    try std.testing.expectEqual(error.Timeout, result.failures[2].?);
}

test "asset producer runtime constrained oversized input does not starve later bounded items" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fake = struct {
        fn invoke(_: *anyopaque, alloc: Allocator, request: Invocation) !Reply {
            try std.testing.expectEqual(@as(usize, 1), request.items.len);
            try std.testing.expectEqual(@as(usize, 1), request.items[0].id);
            return .{ .value = try std.json.parseFromSliceLeaky(Json, alloc, "{\"items\":[{\"item\":1,\"value\":true}]}", .{}) };
        }
    };
    var dummy: u8 = 0;
    const schema = try std.json.parseFromSliceLeaky(Json, a, "{\"type\":\"boolean\"}", .{});
    const result = try execute(a, .{ .ptr = &dummy, .invoke = Fake.invoke }, .grammar, schema, "", .{ .max_input_bytes = 2 }, &.{ "oversized", "ok" });
    try std.testing.expectEqual(error.ConstrainedInputTooLarge, result.failures[0].?);
    try std.testing.expect(result.values[1].?.bool);
}

test {
    _ = token_evidence;
    _ = extraction;
}

/// Qualified adapter/model metadata. Unknown aliases and task variants (audio,
/// search, transcription, etc.) do not inherit a family's chat capabilities.
/// https://developers.openai.com/api/docs/guides/structured-outputs
/// https://ai.google.dev/gemini-api/docs/models
pub fn hostedCapabilities(cfg: anytype) Capabilities {
    const name = if (std.mem.lastIndexOfScalar(u8, cfg.model, '/')) |slash| cfg.model[slash + 1 ..] else cfg.model;
    switch (cfg.provider) {
        .openai => {
            if (std.mem.eql(u8, name, "gpt-4o-2024-05-13")) return .{ .tool_call = true, .token_logprobs = true };
            for ([_][]const u8{ "gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano", "gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "o3", "o3-mini", "o4-mini" }) |model| {
                if (std.mem.eql(u8, name, model) or datedSnapshot(name, model)) return .{ .tool_call = true, .json_schema = true, .token_logprobs = std.mem.startsWith(u8, model, "gpt-4") };
            }
        },
        .gemini, .vertex => {
            for ([_][]const u8{ "gemini-2.0-flash", "gemini-2.0-flash-lite", "gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.5-flash-lite", "gemini-3-pro-preview", "gemini-3-flash-preview" }) |model| if (std.mem.eql(u8, name, model)) return .{ .tool_call = true };
        },
        else => {},
    }
    return .{};
}
fn datedSnapshot(name: []const u8, model: []const u8) bool {
    if (name.len != model.len + 11 or !std.mem.startsWith(u8, name, model) or name[model.len] != '-') return false;
    const date = name[model.len + 1 ..];
    for (date, 0..) |byte, i| if (i == 4 or i == 7) {
        if (byte != '-') return false;
    } else if (!std.ascii.isDigit(byte)) return false;
    return true;
}
test "constrained hosted metadata rejects unqualified task variants and unknown aliases" {
    const Provider = enum { openai, gemini, vertex, antfly };
    const Config = struct { provider: Provider = .openai, model: []const u8 };
    try std.testing.expect((hostedCapabilities(Config{ .model = "gpt-4.1-2025-04-14" })).token_logprobs);
    for ([_][]const u8{ "gpt-4o-audio-preview", "gpt-4o-mini-transcribe", "gpt-4.1-custom-alias", "o3-deep-research" }) |name| try std.testing.expectError(error.UnsupportedConstrainedOutput, hostedCapabilities(Config{ .model = name }).select(.auto, null));
    try std.testing.expectError(error.UnsupportedConstrainedOutput, hostedCapabilities(Config{ .provider = .gemini, .model = "gemini-2.5-flash-image" }).select(.auto, null));
    try std.testing.expectError(error.UnsupportedConstrainedOutput, hostedCapabilities(Config{ .model = "gpt-4o-2024-05-13" }).select(.json_schema, null));
}

/// Provider dialect admission precedes I/O. OpenAI strict objects require all
/// declared keys and closed properties; optional values should use anyOf/null.
pub fn validateProviderSchema(cfg: anytype, mode: Output, schema: Json) !void {
    try validateSchema(schema);
    if (cfg.provider == .openai and (mode == .tool_call or mode == .json_schema)) {
        var enums: usize = 0;
        try validateOpenAiSchema(schema, 3, &enums);
    }
}
fn validateOpenAiSchema(schema: Json, depth: usize, enums: *usize) anyerror!void {
    if (depth > 10 or schema != .object) return error.UnsupportedConstrainedSchema;
    if (schema.object.contains("allOf") or schema.object.contains("oneOf") or schema.object.contains("exclusiveMinimum") or schema.object.contains("exclusiveMaximum")) return error.UnsupportedConstrainedSchema;
    if (schema.object.get("enum")) |values| {
        enums.* += values.array.items.len;
        if (enums.* > 1000) return error.UnsupportedConstrainedSchema;
    }
    if (schema.object.get("type")) |kind| {
        if (kind == .string and std.mem.eql(u8, kind.string, "object") and !schema.object.contains("properties")) return error.UnsupportedConstrainedSchema;
    } else if (!schema.object.contains("anyOf")) return error.UnsupportedConstrainedSchema;
    if (schema.object.get("properties")) |fields| {
        const additional = schema.object.get("additionalProperties") orelse return error.UnsupportedConstrainedSchema;
        if (additional != .bool or additional.bool) return error.UnsupportedConstrainedSchema;
        const required = schema.object.get("required") orelse return error.UnsupportedConstrainedSchema;
        if (required != .array or required.array.items.len != fields.object.count()) return error.UnsupportedConstrainedSchema;
        for (fields.object.keys(), fields.object.values()) |key, child| {
            var found = false;
            for (required.array.items) |entry| if (entry == .string and std.mem.eql(u8, key, entry.string)) {
                found = true;
            };
            if (!found) return error.UnsupportedConstrainedSchema;
            try validateOpenAiSchema(child, depth + 1, enums);
        }
    }
    if (schema.object.get("items")) |items| try validateOpenAiSchema(items, depth + 1, enums);
    if (schema.object.get("anyOf")) |choices| for (choices.array.items) |child| try validateOpenAiSchema(child, depth + 1, enums);
}
/// Array uniqueness stays a mandatory common validation rule. It is omitted
/// only from OpenAI's narrower wire dialect, which does not expose that keyword.
/// Typed arrays/enums remain provider constrained and duplicates get one repair.
pub fn providerSchema(a: Allocator, cfg: anytype, mode: Output, schema: Json) !Json {
    try validateSchema(schema);
    if (cfg.provider != .openai) return schema;
    if (mode != .tool_call and mode != .json_schema) return error.UnsupportedConstrainedOutput;
    var enums: usize = 0;
    try validateOpenAiSchema(schema, 0, &enums);
    return openAiSchema(a, schema);
}
fn openAiSchema(a: Allocator, schema: Json) anyerror!Json {
    var copy = object();
    for (schema.object.keys(), schema.object.values()) |key, value| {
        if (std.mem.eql(u8, key, "uniqueItems")) continue;
        var entry = value;
        if (std.mem.eql(u8, key, "properties")) {
            entry = object();
            for (value.object.keys(), value.object.values()) |name, child| try put(a, &entry, name, try openAiSchema(a, child));
        } else if (std.mem.eql(u8, key, "items")) entry = try openAiSchema(a, value) else if (std.mem.eql(u8, key, "anyOf")) {
            entry = try array(a, &.{});
            for (value.array.items) |child| try entry.array.append(try openAiSchema(a, child));
        }
        try put(a, &copy, key, entry);
    }
    return copy;
}

test "constrained OpenAI dialect admission rejects optional properties and retains uniqueness validation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Provider = enum { openai, antfly };
    const cfg = .{ .provider = Provider.openai };
    const optional = try std.json.parseFromSliceLeaky(Json, a, "{\"type\":\"object\",\"properties\":{\"label\":{\"type\":\"string\"}},\"additionalProperties\":false}", .{});
    try std.testing.expectError(error.UnsupportedConstrainedSchema, validateProviderSchema(cfg, .tool_call, optional));
    const unique = try std.json.parseFromSliceLeaky(Json, a, "{\"type\":\"array\",\"items\":{\"type\":\"string\",\"enum\":[\"a\",\"b\"]},\"uniqueItems\":true}", .{});
    const wire = try providerSchema(a, cfg, .tool_call, unique);
    try std.testing.expect(!wire.object.contains("uniqueItems"));
    try std.testing.expect(unique.object.get("uniqueItems").?.bool);
    const duplicate = try std.json.parseFromSliceLeaky(Json, a, "[\"a\",\"a\"]", .{});
    if (schema_validator.validateJsonSchemaValue(a, unique, duplicate)) |_| return error.TestExpectedError else |_| {}
}
