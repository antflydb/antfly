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

//! Typed decision contract over the admitted extraction v2 executors.
const std = @import("std");
const Value = std.json.Value;
const Allocator = std.mem.Allocator;
const extraction = @import("extraction_v2.zig");

pub const Kind = enum { choice, score, noul };
pub const Question = struct {
    name: []const u8,
    kind: Kind,
    instructions: []const u8,
    labels: []const []const u8,
    descriptions: []const []const u8,
};
pub const Request = struct {
    model: []const u8,
    state: []const u8,
    questions: []Question,
    long_document: extraction.LongDocument = .{},
};

fn object(v: Value) !std.json.ObjectMap {
    return if (v == .object) v.object else error.InvalidDecideRequest;
}
fn nonempty(v: Value) ![]const u8 {
    if (v != .string or !std.unicode.utf8ValidateSlice(v.string) or std.mem.trim(u8, v.string, " \r\n\t").len == 0) return error.InvalidDecideRequest;
    return v.string;
}
fn outputObject(v: Value) !std.json.ObjectMap {
    return if (v == .object) v.object else error.InvalidDecideOutput;
}
fn outputNonempty(v: Value) ![]const u8 {
    if (v != .string or !std.unicode.utf8ValidateSlice(v.string) or std.mem.trim(u8, v.string, " \r\n\t").len == 0) return error.InvalidDecideOutput;
    return v.string;
}
fn onlyKeys(map: std.json.ObjectMap, allowed: []const []const u8) !void {
    for (map.keys()) |key| {
        for (allowed) |candidate| {
            if (std.mem.eql(u8, key, candidate)) break;
        } else return error.InvalidDecideRequest;
    }
}

pub fn parse(a: Allocator, json: []const u8) !Request {
    if (json.len > 1024 * 1024) return error.DecideRequestLimitExceeded;
    const parsed = std.json.parseFromSlice(Value, a, json, .{ .duplicate_field_behavior = .@"error" }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDecideRequest,
    };
    // The request owns parsed strings through the caller's request arena.
    const root = try object(parsed.value);
    try onlyKeys(root, &.{ "model", "state", "questions", "long_document" });
    const long_document = if (root.get("long_document")) |raw| blk: {
        try onlyKeys(try object(raw), &.{ "mode", "window_words", "overlap_words", "max_windows" });
        break :blk extraction.parseLongDocument(raw) catch return error.InvalidDecideRequest;
    } else extraction.LongDocument{};
    const model = try nonempty(root.get("model") orelse return error.InvalidDecideRequest);
    const state = try nonempty(root.get("state") orelse return error.InvalidDecideRequest);
    if (state.len > 1024 * 1024) return error.DecideRequestLimitExceeded;
    const questions = try object(root.get("questions") orelse return error.InvalidDecideRequest);
    if (questions.count() == 0 or questions.count() > 64) return error.DecideRequestLimitExceeded;
    const out = try a.alloc(Question, questions.count());
    for (questions.keys(), questions.values(), out) |name, raw, *question| {
        if (name.len == 0 or !std.unicode.utf8ValidateSlice(name)) return error.InvalidDecideRequest;
        const fields = try object(raw);
        try onlyKeys(fields, &.{ "type", "instructions", "criteria" });
        const type_name = try nonempty(fields.get("type") orelse return error.InvalidDecideRequest);
        const kind = std.meta.stringToEnum(Kind, type_name) orelse return error.InvalidDecideRequest;
        const instructions = try nonempty(fields.get("instructions") orelse return error.InvalidDecideRequest);
        const criteria = fields.get("criteria");
        var labels: [][]const u8 = undefined;
        var descriptions: [][]const u8 = undefined;
        switch (kind) {
            .choice => {
                const choices = try object(criteria orelse return error.InvalidDecideRequest);
                if (choices.count() < 2 or choices.count() > 64) return error.InvalidDecideRequest;
                labels = try a.alloc([]const u8, choices.count());
                descriptions = try a.alloc([]const u8, choices.count());
                for (choices.keys(), choices.values(), labels, descriptions) |label, description, *dest_label, *dest_description| {
                    if (label.len == 0 or !std.unicode.utf8ValidateSlice(label)) return error.InvalidDecideRequest;
                    dest_label.* = label;
                    dest_description.* = try nonempty(description);
                }
            },
            .score => {
                const levels = criteria orelse return error.InvalidDecideRequest;
                if (levels != .array or levels.array.items.len < 2 or levels.array.items.len > 64) return error.InvalidDecideRequest;
                labels = try a.alloc([]const u8, levels.array.items.len);
                descriptions = try a.alloc([]const u8, levels.array.items.len);
                for (levels.array.items, labels, descriptions, 0..) |description, *label, *dest, index| {
                    label.* = try std.fmt.allocPrint(a, "{d}", .{index});
                    dest.* = try nonempty(description);
                }
            },
            .noul => {
                if (criteria != null) return error.InvalidDecideRequest;
                labels = try a.dupe([]const u8, &.{ "false", "true" });
                descriptions = try a.dupe([]const u8, &.{ "False", "True" });
            },
        }
        question.* = .{ .name = name, .kind = kind, .instructions = instructions, .labels = labels, .descriptions = descriptions };
    }
    return .{ .model = model, .state = state, .questions = out, .long_document = long_document };
}

fn put(a: Allocator, map: *std.json.ObjectMap, key: []const u8, value: Value) !void {
    try map.put(a, key, value);
}
fn string(s: []const u8) Value {
    return .{ .string = s };
}

pub const ExtractionInput = struct {
    json: []u8,
    schema_bytes: usize,
};

/// Produce an extraction v2 request and the exact serialized schema size used
/// by inference.limits.max_schema_bytes. GLiNER's top_k includes every option.
pub fn extractionInput(a: Allocator, request: Request, gliner: bool) !ExtractionInput {
    if (!gliner and request.long_document.mode == .window) return error.UnsupportedDecisionWindowing;
    var root: std.json.ObjectMap = .empty;
    try put(a, &root, "schema_version", .{ .integer = 2 });
    try put(a, &root, "model", string(request.model));
    var input: std.json.ObjectMap = .empty;
    try put(a, &input, "content", string(request.state));
    var inputs: std.array_list.Managed(Value) = .init(a);
    try inputs.append(.{ .object = input });
    try put(a, &root, "inputs", .{ .array = inputs });
    var schema: std.json.ObjectMap = .empty;
    var tasks: std.array_list.Managed(Value) = .init(a);
    for (request.questions) |question| {
        var task: std.json.ObjectMap = .empty;
        try put(a, &task, "name", string(question.name));
        try put(a, &task, if (gliner) "prompt" else "instruction", string(question.instructions));
        if (!gliner) try put(a, &task, "mode", string(switch (question.kind) {
            .choice => "single",
            .score => "ordinal",
            .noul => "boolean",
        }));
        if (gliner) try put(a, &task, "top_k", .{ .integer = @intCast(question.labels.len) });
        var labels: std.array_list.Managed(Value) = .init(a);
        var definitions: std.json.ObjectMap = .empty;
        for (question.labels, question.descriptions) |label, description| {
            try labels.append(string(label));
            var definition: std.json.ObjectMap = .empty;
            try put(a, &definition, "description", string(description));
            try put(a, &definitions, label, .{ .object = definition });
        }
        try put(a, &task, "labels", .{ .array = labels });
        try put(a, &task, "label_definitions", .{ .object = definitions });
        try tasks.append(.{ .object = task });
    }
    try put(a, &schema, "classifications", .{ .array = tasks });
    const schema_json = try std.json.Stringify.valueAlloc(a, Value{ .object = schema }, .{});
    try put(a, &root, "schema", .{ .object = schema });
    if (gliner) {
        var options: std.json.ObjectMap = .empty;
        try put(a, &options, "include_confidence", .{ .bool = true });
        if (request.long_document.mode == .window) {
            var windows: std.json.ObjectMap = .empty;
            try put(a, &windows, "mode", string("window"));
            try put(a, &windows, "window_words", .{ .integer = @intCast(request.long_document.window_words) });
            try put(a, &windows, "overlap_words", .{ .integer = @intCast(request.long_document.overlap_words) });
            try put(a, &windows, "max_windows", .{ .integer = @intCast(request.long_document.max_windows) });
            try put(a, &options, "long_document", .{ .object = windows });
        }
        try put(a, &root, "options", .{ .object = options });
    }
    return .{
        .json = try std.json.Stringify.valueAlloc(a, Value{ .object = root }, .{}),
        .schema_bytes = schema_json.len,
    };
}

fn number(v: Value) !f64 {
    return switch (v) {
        .integer => @floatFromInt(v.integer),
        .float => v.float,
        .number_string => std.fmt.parseFloat(f64, v.number_string) catch return error.InvalidDecideOutput,
        else => error.InvalidDecideOutput,
    };
}

test "typed decide windowing preserves extraction geometry and rejects invalid options" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const prefix = "{\"model\":\"multi-decide\",\"state\":\"hello\",\"questions\":{\"q\":{\"type\":\"noul\",\"instructions\":\"Relevant?\"}},\"long_document\":";
    const request = try parse(a, prefix ++ "{\"mode\":\"window\",\"window_words\":64,\"overlap_words\":8,\"max_windows\":4}}");
    const input = try extractionInput(a, request, true);
    const json = try std.json.parseFromSlice(Value, a, input.json, .{});
    const options = try extraction.parseOptions(json.value.object.get("options").?);
    try std.testing.expectEqualDeep(request.long_document, options.long_document);
    try std.testing.expectError(error.UnsupportedDecisionWindowing, extractionInput(a, request, false));
    for ([_][]const u8{
        "null",                                                     "{\"mode\":\"window\",\"window_words\":32,\"overlap_words\":32}",
        "{\"mode\":\"window\",\"max_windows\":129}",                "{\"mode\":\"reject\",\"window_words\":64}",
        "{\"mode\":\"window\",\"record_identity\":\"occurrence\"}",
    }) |invalid| {
        const bytes = try std.fmt.allocPrint(a, "{s}{s}}}", .{ prefix, invalid });
        try std.testing.expectError(error.InvalidDecideRequest, parse(a, bytes));
    }
}

fn tokenCount(v: Value) !Value {
    if (v != .integer or v.integer < 0) return error.InvalidDecideOutput;
    return v;
}

/// Translate both executors' distributions with one decision presenter.
pub fn responseJson(a: Allocator, request: Request, extraction_json: []const u8, gliner: bool) ![]u8 {
    const parsed = std.json.parseFromSlice(Value, a, extraction_json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDecideOutput,
    };
    const root = try outputObject(parsed.value);
    const data = root.get("data") orelse return error.InvalidDecideOutput;
    if (data != .array or data.array.items.len != 1) return error.InvalidDecideOutput;
    const item = try outputObject(data.array.items[0]);
    const rows = item.get(if (gliner) "classifications" else "decisions") orelse return error.InvalidDecideOutput;
    if (rows != .array) return error.InvalidDecideOutput;
    var expected_rows: usize = 0;
    for (request.questions) |question| expected_rows += if (gliner) question.labels.len else 1;
    if (rows.array.items.len != expected_rows) return error.InvalidDecideOutput;
    var answers: std.json.ObjectMap = .empty;
    for (request.questions, 0..) |question, question_index| {
        const probabilities = try a.alloc(f64, question.labels.len);
        @memset(probabilities, -1);
        const raw_probabilities = if (gliner) rows.array.items else blk: {
            const row = try outputObject(rows.array.items[question_index]);
            const row_name = try outputNonempty(row.get("name") orelse return error.InvalidDecideOutput);
            if (!std.mem.eql(u8, row_name, question.name)) return error.InvalidDecideOutput;
            const values = row.get("probabilities") orelse return error.InvalidDecideOutput;
            if (values != .array or values.array.items.len != question.labels.len) return error.InvalidDecideOutput;
            break :blk values.array.items;
        };
        for (raw_probabilities) |raw_probability| {
            const entry = try outputObject(raw_probability);
            if (gliner) {
                const row_name = try outputNonempty(entry.get("name") orelse return error.InvalidDecideOutput);
                if (!std.mem.eql(u8, row_name, question.name)) continue;
            }
            const label = try outputNonempty(entry.get("label") orelse return error.InvalidDecideOutput);
            const probability = try number(entry.get(if (gliner) "score" else "probability") orelse return error.InvalidDecideOutput);
            if (!std.math.isFinite(probability) or probability < 0 or probability > 1) return error.InvalidDecideOutput;
            for (question.labels, probabilities) |expected, *dest| {
                if (std.mem.eql(u8, label, expected)) {
                    if (dest.* >= 0) return error.InvalidDecideOutput;
                    dest.* = probability;
                    break;
                }
            } else return error.InvalidDecideOutput;
        }
        var distribution: std.json.ObjectMap = .empty;
        var total: f64 = 0;
        var best: usize = 0;
        var expected: f64 = 0;
        for (question.labels, probabilities, 0..) |label, probability, index| {
            if (probability < 0) return error.InvalidDecideOutput;
            total += probability;
            expected += @as(f64, @floatFromInt(index)) * probability;
            if (probability > probabilities[best]) best = index;
            try put(a, &distribution, label, .{ .float = probability });
        }
        if (@abs(total - 1) > 0.01) return error.InvalidDecideOutput;
        var answer: std.json.ObjectMap = .empty;
        try put(a, &answer, "type", string(@tagName(question.kind)));
        switch (question.kind) {
            .choice => try put(a, &answer, "choice", string(question.labels[best])),
            .score => {
                try put(a, &answer, "score", .{ .float = expected });
                var legend: std.json.ObjectMap = .empty;
                for (question.labels, question.descriptions) |label, description| try put(a, &legend, label, string(description));
                try put(a, &answer, "legend", .{ .object = legend });
            },
            .noul => try put(a, &answer, "noul", .{ .float = probabilities[1] }),
        }
        if (question.kind != .noul) try put(a, &answer, "probabilities", .{ .object = distribution });
        try put(a, &answers, question.name, .{ .object = answer });
    }
    var output: std.json.ObjectMap = .empty;
    try put(a, &output, "model", string(request.model));
    try put(a, &output, "answers", .{ .object = answers });
    const usage = try outputObject(root.get("usage") orelse return error.InvalidDecideOutput);
    var out_usage: std.json.ObjectMap = .empty;
    try put(a, &out_usage, "input_tokens", try tokenCount(usage.get("prompt_tokens") orelse return error.InvalidDecideOutput));
    try put(a, &out_usage, "output_tokens", try tokenCount(usage.get("completion_tokens") orelse return error.InvalidDecideOutput));
    try put(a, &output, "usage", .{ .object = out_usage });
    return std.json.Stringify.valueAlloc(a, Value{ .object = output }, .{});
}

test "decide validates questions before building an extraction request" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const valid =
        \\{"model":"m","state":"A refund is needed","questions":{"intent":{"type":"choice","instructions":"Intent?","criteria":{"refund":"Refund","other":"Other"}},"urgency":{"type":"score","instructions":"When?","criteria":["Later","Now"]},"act":{"type":"noul","instructions":"Act?"}}}
    ;
    const request = try parse(a, valid);
    try std.testing.expectEqual(@as(usize, 3), request.questions.len);
    const laya = try extractionInput(a, request, false);
    try std.testing.expect(std.mem.indexOf(u8, laya.json, "\"mode\":\"ordinal\"") != null);
    const gliner = try extractionInput(a, request, true);
    try std.testing.expect(std.mem.indexOf(u8, gliner.json, "\"top_k\":2") != null);
    const longer_state = try a.alloc(u8, 4096);
    @memset(longer_state, 'x');
    var longer_request = request;
    longer_request.state = longer_state;
    for ([_]bool{ false, true }) |span| {
        const short = try extractionInput(a, request, span);
        const long = try extractionInput(a, longer_request, span);
        var parsed = try std.json.parseFromSlice(Value, a, short.json, .{});
        defer parsed.deinit();
        const actual_schema = try std.json.Stringify.valueAlloc(a, parsed.value.object.get("schema").?, .{});
        try std.testing.expectEqual(actual_schema.len, short.schema_bytes);
        try std.testing.expectEqual(short.schema_bytes, long.schema_bytes);
        try std.testing.expect(long.json.len > short.json.len + 4000);
    }
    try std.testing.expectError(error.InvalidDecideRequest, parse(a,
        \\{"model":"m","state":"x","questions":{"q":{"type":"noul","instructions":"?","criteria":[]}}}
    ));
    try std.testing.expectError(error.InvalidDecideRequest, parse(a,
        \\{"model":"m","state":"x","questions":{"q":{"type":"choice","instructions":"?","criteria":{"a":"A","a":"B"}}}}
    ));
}

test "decide presents complete Laya and GLiNER distributions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try parse(a,
        \\{"model":"m","state":"x","questions":{"intent":{"type":"choice","instructions":"Intent?","criteria":{"refund":"Refund","other":"Other"}},"urgency":{"type":"score","instructions":"When?","criteria":["Later","Now"]},"act":{"type":"noul","instructions":"Act?"}}}
    );
    const laya =
        \\{"data":[{"decisions":[{"name":"intent","probabilities":[{"label":"refund","probability":0.8},{"label":"other","probability":0.2}]},{"name":"urgency","probabilities":[{"label":"0","probability":0.25},{"label":"1","probability":0.75}]},{"name":"act","probabilities":[{"label":"false","probability":0.1},{"label":"true","probability":0.9}]}]}],"usage":{"prompt_tokens":10,"completion_tokens":0}}
    ;
    const span =
        \\{"data":[{"classifications":[{"name":"intent","label":"other","score":0.2},{"name":"intent","label":"refund","score":0.8},{"name":"urgency","label":"0","score":0.25},{"name":"urgency","label":"1","score":0.75},{"name":"act","label":"false","score":0.1},{"name":"act","label":"true","score":0.9}]}],"usage":{"prompt_tokens":10,"completion_tokens":0}}
    ;
    for ([_]struct { []const u8, bool }{ .{ laya, false }, .{ span, true } }) |case| {
        const json = try responseJson(a, request, case[0], case[1]);
        var parsed = try std.json.parseFromSlice(Value, a, json, .{});
        defer parsed.deinit();
        const answers = parsed.value.object.get("answers").?.object;
        try std.testing.expectEqualStrings("refund", answers.get("intent").?.object.get("choice").?.string);
        try std.testing.expectApproxEqAbs(@as(f64, 0.75), answers.get("urgency").?.object.get("score").?.float, 0.00001);
        try std.testing.expectApproxEqAbs(@as(f64, 0.9), answers.get("act").?.object.get("noul").?.float, 0.00001);
    }
}

test "decide reports malformed executor responses as output failures" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try parse(a,
        \\{"model":"m","state":"x","questions":{"act":{"type":"noul","instructions":"Act?"}}}
    );
    for ([_][]const u8{
        "not json",
        "[]",
        "{\"data\":[null]}",
        "{\"data\":[{\"decisions\":[null]}]}",
        "{\"data\":[{\"decisions\":[{\"name\":42,\"probabilities\":[]}]}]}",
        "{\"data\":[{\"decisions\":[{\"name\":\"act\",\"probabilities\":[null,null]}]}]}",
    }) |malformed| try std.testing.expectError(error.InvalidDecideOutput, responseJson(a, request, malformed, false));
    const valid_decision = "{\"data\":[{\"decisions\":[{\"name\":\"act\",\"probabilities\":[{\"label\":\"false\",\"probability\":0.25},{\"label\":\"true\",\"probability\":0.75}]}]}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":0}}";
    _ = try responseJson(a, request, valid_decision, false);
    for ([_][]const u8{
        "\"prompt_tokens\":\"10\",\"completion_tokens\":0",
        "\"prompt_tokens\":-1,\"completion_tokens\":0",
        "\"prompt_tokens\":10,\"completion_tokens\":0.5",
    }) |invalid_usage| {
        const response = try std.fmt.allocPrint(
            a,
            "{{\"data\":[{{\"decisions\":[{{\"name\":\"act\",\"probabilities\":[{{\"label\":\"false\",\"probability\":0.25}},{{\"label\":\"true\",\"probability\":0.75}}]}}]}}],\"usage\":{{{s}}}}}",
            .{invalid_usage},
        );
        try std.testing.expectError(error.InvalidDecideOutput, responseJson(a, request, response, false));
    }
}
