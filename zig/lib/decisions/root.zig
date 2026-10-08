// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Public named-array decision contract. The model adapters retain their
//! private typed-classification representation behind this boundary.
const std = @import("std");
pub const legacy = @import("trained.zig");
pub const scoring = @import("embedding.zig");
const V = std.json.Value;
const A = std.mem.Allocator;
pub const Kind = enum { choice, multi_choice, score, predicate };
pub const Item = struct { input: []const u8, id: ?[]const u8 = null };
pub const Policy = struct {
    kind: Kind,
    options: scoring.Options,
    thresholds: ?[]const f64 = null,
    embedding_configured: bool = false,
    level_labels: []const []const u8 = &.{},
};
pub const Request = struct {
    inner: legacy.Request,
    items: []const Item,
    policies: []const Policy,
    batched: bool,
    pub fn forItem(self: Request, index: usize) legacy.Request {
        var out = self.inner;
        out.state = self.items[index].input;
        return out;
    }
};

pub fn validateExtractionBoundary(value: V) !void {
    if (value != .object) return;
    const root = value.object;
    if (root.get("options")) |options| if (options == .object and options.object.contains("embedding")) return error.UnsupportedExtractionFeature;
    if (root.get("schema")) |schema| try extractionSchema(schema);
    if (root.get("inputs")) |inputs| if (inputs == .array) {
        for (inputs.array.items) |input| if (input == .object) {
            if (input.object.get("schema")) |schema| try extractionSchema(schema);
        };
    };
}

fn extractionSchema(schema: V) !void {
    if (schema != .object) return;
    const tasks = schema.object.get("classifications") orelse return;
    if (tasks != .array) return;
    for (tasks.array.items) |task| if (task == .object) {
        if (task.object.contains("similarity_thresholds")) return error.UnsupportedExtractionFeature;
        if (task.object.get("mode")) |mode| if (mode == .string and (std.mem.eql(u8, mode.string, "boolean") or std.mem.eql(u8, mode.string, "ordinal"))) return error.UnsupportedExtractionFeature;
    };
}

fn object(v: V) !std.json.ObjectMap {
    return if (v == .object) v.object else error.InvalidDecideRequest;
}
fn text(v: V) ![]const u8 {
    if (v != .string or !std.unicode.utf8ValidateSlice(v.string) or std.mem.trim(u8, v.string, " \r\n\t").len == 0) return error.InvalidDecideRequest;
    return v.string;
}
fn keys(o: std.json.ObjectMap, allowed: []const []const u8) !void {
    for (o.keys()) |key| {
        for (allowed) |candidate| {
            if (std.mem.eql(u8, key, candidate)) break;
        } else return error.InvalidDecideRequest;
    }
}
fn str(s: []const u8) V {
    return .{ .string = s };
}
fn array(a: A, values: []const V) !V {
    var out: std.array_list.Managed(V) = .init(a);
    try out.appendSlice(values);
    return .{ .array = out };
}
fn scalar(v: V) !f64 {
    const n: f64 = switch (v) {
        .float => v.float,
        .integer => @floatFromInt(v.integer),
        else => return error.InvalidDecideRequest,
    };
    if (!std.math.isFinite(n) or n < -1 or n > 1) return error.InvalidDecideRequest;
    return n;
}

/// Shared provider serializer. Jev's private map/noul adapter stays separate.
pub fn requestJson(backing: A, model: []const u8, input: []const u8, questions: V, options: ?V, identity: ?[]const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const map = try object(questions);
    var defaults = std.json.ObjectMap{};
    var acceptance = std.json.ObjectMap{};
    if (options) |raw| {
        const opts = try object(raw);
        for (opts.keys(), opts.values()) |key, value| {
            if (std.mem.eql(u8, key, "task_type") or std.mem.eql(u8, key, "dimensions")) try defaults.put(a, key, value) else try acceptance.put(a, key, value);
        }
    }
    var out: std.array_list.Managed(V) = .init(a);
    for (map.keys(), map.values()) |name, raw| {
        const original = try object(raw);
        const kind = try text(original.get("type") orelse return error.InvalidDecideRequest);
        var q = std.json.ObjectMap{};
        try q.put(a, "name", str(name));
        try q.put(a, "type", str(if (std.mem.eql(u8, kind, "noul")) "predicate" else kind));
        try q.put(a, "instructions", original.get("instructions") orelse return error.InvalidDecideRequest);
        if (original.get("criteria")) |criteria| {
            var values: std.array_list.Managed(V) = .init(a);
            if (criteria == .object) {
                for (criteria.object.keys(), criteria.object.values()) |label, definition| {
                    var choice = std.json.ObjectMap{};
                    try choice.put(a, "value", str(label));
                    if (definition == .string) {
                        if (definition.string.len != 0) try choice.put(a, "description", definition);
                    } else {
                        const fields = try object(definition);
                        for (fields.keys(), fields.values()) |key, value| try choice.put(a, key, value);
                    }
                    try values.append(.{ .object = choice });
                }
                try q.put(a, "choices", .{ .array = values });
            } else if (criteria == .array) {
                for (criteria.array.items, 0..) |description, index| {
                    var level = std.json.ObjectMap{};
                    try level.put(a, "label", if (original.get("level_labels")) |labels| labels.array.items[index] else str(try std.fmt.allocPrint(a, "{d}", .{index})));
                    try level.put(a, "description", description);
                    try values.append(.{ .object = level });
                }
                try q.put(a, "levels", .{ .array = values });
            } else return error.InvalidDecideRequest;
        }
        if (original.get("embedding_options")) |configured| {
            try q.put(a, "embedding_options", configured);
        } else if (!original.contains("similarity_thresholds") and acceptance.count() != 0) try q.put(a, "embedding_options", .{ .object = acceptance });
        if (original.get("similarity_thresholds")) |thresholds| try q.put(a, "similarity_thresholds", thresholds);
        try out.append(.{ .object = q });
    }
    var root = std.json.ObjectMap{};
    try root.put(a, "model", str(model));
    try root.put(a, "input", str(input));
    try root.put(a, "questions", .{ .array = out });
    if (defaults.count() != 0) try root.put(a, "embedding_options", .{ .object = defaults });
    if (identity) |id| try root.put(a, "model_identity", str(id));
    const bytes = try std.json.Stringify.valueAlloc(a, V{ .object = root }, .{});
    _ = try parse(a, bytes);
    return backing.dupe(u8, bytes);
}

pub fn internalQuestions(a: A, questions: V) !V {
    var root = std.json.ObjectMap{};
    try root.put(a, "model", str("contract-validation"));
    try root.put(a, "input", str("validation"));
    try root.put(a, "questions", questions);
    const parsed = try parse(a, try std.json.Stringify.valueAlloc(a, V{ .object = root }, .{}));
    var out = std.json.ObjectMap{};
    for (parsed.inner.questions, parsed.policies, questions.array.items) |question, policy, raw| {
        var q = std.json.ObjectMap{};
        try q.put(a, "type", str(if (policy.kind == .multi_choice) "multi_choice" else @tagName(question.kind)));
        try q.put(a, "instructions", str(question.instructions));
        if (question.kind == .choice) {
            var criteria = std.json.ObjectMap{};
            for (question.labels, question.descriptions, question.examples) |label, description, examples| {
                if (examples.len == 0) {
                    try criteria.put(a, label, str(description));
                } else {
                    var definition = std.json.ObjectMap{};
                    try definition.put(a, "description", str(description));
                    var sample_values: std.array_list.Managed(V) = .init(a);
                    for (examples) |example| try sample_values.append(str(example));
                    try definition.put(a, "examples", .{ .array = sample_values });
                    try criteria.put(a, label, .{ .object = definition });
                }
            }
            try q.put(a, "criteria", .{ .object = criteria });
        } else if (question.kind == .score) {
            var criteria: std.array_list.Managed(V) = .init(a);
            for (question.descriptions) |description| try criteria.append(str(description));
            try q.put(a, "criteria", .{ .array = criteria });
            var labels: std.array_list.Managed(V) = .init(a);
            for (policy.level_labels) |label| try labels.append(str(label));
            try q.put(a, "level_labels", .{ .array = labels });
        }
        if (raw.object.get("embedding_options")) |options| try q.put(a, "embedding_options", options);
        if (raw.object.get("similarity_thresholds")) |thresholds| try q.put(a, "similarity_thresholds", thresholds);
        try out.put(a, question.name, .{ .object = q });
    }
    return .{ .object = out };
}

/// Lower only at a provider boundary; the public response always uses arrays.
pub fn internalResponse(a: A, response: V) !V {
    if (response != .object) return error.InvalidDecideOutput;
    const raw = response.object.get("answers") orelse return error.InvalidDecideOutput;
    if (raw != .array) return response;
    var answers = std.json.ObjectMap{};
    for (raw.array.items) |entry| {
        if (entry != .object) return error.InvalidDecideOutput;
        const name = entry.object.get("name") orelse return error.InvalidDecideOutput;
        if (name != .string or answers.contains(name.string)) return error.InvalidDecideOutput;
        var answer = std.json.ObjectMap{};
        for (entry.object.keys(), entry.object.values()) |key, v| {
            if (std.mem.eql(u8, key, "decision_method") and v == .string and std.mem.eql(u8, v.string, "typed")) continue;
            if (std.mem.eql(u8, key, "type") and v == .string and std.mem.eql(u8, v.string, "predicate")) {
                try answer.put(a, key, str("noul"));
            } else if (std.mem.eql(u8, key, "probability")) {
                try answer.put(a, "noul", v);
            } else if ((std.mem.eql(u8, key, "probabilities") or std.mem.eql(u8, key, "similarities")) and v == .array) {
                var values = std.json.ObjectMap{};
                var legend = std.json.ObjectMap{};
                for (v.array.items) |item| {
                    if (item != .object) return error.InvalidDecideOutput;
                    const id = item.object.get("value") orelse return error.InvalidDecideOutput;
                    const label = switch (id) {
                        .string => id.string,
                        .integer => try std.fmt.allocPrint(a, "{d}", .{id.integer}),
                        else => return error.InvalidDecideOutput,
                    };
                    if (values.contains(label)) return error.InvalidDecideOutput;
                    try values.put(a, label, item.object.get(if (std.mem.eql(u8, key, "similarities")) "similarity" else "probability") orelse return error.InvalidDecideOutput);
                    if (item.object.get("label")) |description| try legend.put(a, label, description);
                }
                try answer.put(a, key, .{ .object = values });
                if (legend.count() != 0) try answer.put(a, "legend", .{ .object = legend });
            } else try answer.put(a, key, v);
        }
        try answers.put(a, name.string, .{ .object = answer });
    }
    var root = std.json.ObjectMap{};
    for (response.object.keys(), response.object.values()) |key, v| try root.put(a, key, v);
    try root.put(a, "answers", .{ .object = answers });
    return .{ .object = root };
}

pub fn publicResponse(a: A, response: V) !V {
    if (response != .object) return error.InvalidDecideOutput;
    const raw = response.object.get("answers") orelse return error.InvalidDecideOutput;
    if (raw != .object) return response;
    var answers: std.array_list.Managed(V) = .init(a);
    for (raw.object.keys(), raw.object.values()) |name, entry| {
        if (entry != .object) return error.InvalidDecideOutput;
        var answer = std.json.ObjectMap{};
        for (entry.object.keys(), entry.object.values()) |key, v| {
            if (std.mem.eql(u8, key, "type") and v == .string and std.mem.eql(u8, v.string, "noul")) {
                try answer.put(a, key, str("predicate"));
            } else if (std.mem.eql(u8, key, "noul")) {
                try answer.put(a, "probability", v);
            } else if ((std.mem.eql(u8, key, "probabilities") or std.mem.eql(u8, key, "similarities")) and v == .object) {
                var values: std.array_list.Managed(V) = .init(a);
                for (v.object.keys(), v.object.values()) |label, score| {
                    var item = std.json.ObjectMap{};
                    const kind = entry.object.get("type").?;
                    if (kind == .string and std.mem.eql(u8, kind.string, "score")) {
                        try item.put(a, "value", .{ .integer = std.fmt.parseInt(i64, label, 10) catch return error.InvalidDecideOutput });
                        if (entry.object.get("legend")) |legend| try item.put(a, "label", legend.object.get(label) orelse return error.InvalidDecideOutput);
                    } else try item.put(a, "value", str(label));
                    try item.put(a, if (std.mem.eql(u8, key, "similarities")) "similarity" else "probability", score);
                    try values.append(.{ .object = item });
                }
                try answer.put(a, key, .{ .array = values });
            } else if (!std.mem.eql(u8, key, "legend")) try answer.put(a, key, v);
        }
        try answer.put(a, "name", str(name));
        if (!answer.contains("decision_method")) try answer.put(a, "decision_method", str("typed"));
        try answers.append(.{ .object = answer });
    }
    var root = std.json.ObjectMap{};
    for (response.object.keys(), response.object.values()) |key, v| try root.put(a, key, v);
    try root.put(a, "answers", .{ .array = answers });
    return .{ .object = root };
}

pub fn parse(a: A, bytes: []const u8) !Request {
    if (bytes.len > 16 * 1024 * 1024) return error.DecideRequestLimitExceeded;
    const value = std.json.parseFromSliceLeaky(V, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidDecideRequest;
    const root = try object(value);
    try keys(root, &.{ "model", "model_identity", "input", "inputs", "questions", "embedding_options" });
    const model = try text(root.get("model") orelse return error.InvalidDecideRequest);
    const batched = root.contains("inputs");
    if (batched == root.contains("input")) return error.InvalidDecideRequest;
    const items = if (batched) blk: {
        const raw = root.get("inputs").?;
        if (raw != .array or raw.array.items.len == 0 or raw.array.items.len > 128) return error.DecideRequestLimitExceeded;
        const out = try a.alloc(Item, raw.array.items.len);
        for (raw.array.items, out) |entry, *item| {
            const fields = try object(entry);
            try keys(fields, &.{ "id", "input" });
            item.* = .{ .input = try text(fields.get("input") orelse return error.InvalidDecideRequest), .id = if (fields.get("id")) |id| try text(id) else null };
        }
        break :blk out;
    } else try a.dupe(Item, &.{.{ .input = try text(root.get("input").?) }});
    for (items) |item| if (item.input.len > 1024 * 1024) return error.DecideRequestLimitExceeded;
    var defaults = std.json.ObjectMap{};
    if (root.get("embedding_options")) |options| {
        defaults = try object(options);
        try keys(defaults, &.{ "task_type", "dimensions" });
        _ = try scoring.Options.parse(options);
    }
    const questions = root.get("questions") orelse return error.InvalidDecideRequest;
    if (questions != .array or questions.array.items.len == 0 or questions.array.items.len > 64) return error.DecideRequestLimitExceeded;
    const policies = try a.alloc(Policy, questions.array.items.len);
    var lowered = std.json.ObjectMap{};
    for (questions.array.items, policies) |raw, *policy| {
        const fields = try object(raw);
        try keys(fields, &.{ "name", "type", "instructions", "choices", "levels", "embedding_options", "similarity_thresholds" });
        const name = try text(fields.get("name") orelse return error.InvalidDecideRequest);
        if (lowered.contains(name)) return error.InvalidDecideRequest;
        const kind = std.meta.stringToEnum(Kind, try text(fields.get("type") orelse return error.InvalidDecideRequest)) orelse return error.InvalidDecideRequest;
        var effective = std.json.ObjectMap{};
        for (defaults.keys(), defaults.values()) |key, v| try effective.put(a, key, v);
        if (fields.get("embedding_options")) |options| {
            const opts = try object(options);
            try keys(opts, &.{ "calibration_id", "min_similarity", "min_margin" });
            for (opts.keys(), opts.values()) |key, v| try effective.put(a, key, v);
        }
        policy.* = .{ .kind = kind, .options = try scoring.Options.parse(.{ .object = effective }), .embedding_configured = root.contains("embedding_options") or fields.contains("embedding_options") or fields.contains("similarity_thresholds") };
        var q = std.json.ObjectMap{};
        try q.put(a, "type", str(if (kind == .predicate) "noul" else if (kind == .multi_choice) "choice" else @tagName(kind)));
        try q.put(a, "instructions", str(try text(fields.get("instructions") orelse return error.InvalidDecideRequest)));
        switch (kind) {
            .choice, .multi_choice => {
                if (fields.contains("levels")) return error.InvalidDecideRequest;
                const choices = fields.get("choices") orelse return error.InvalidDecideRequest;
                if (choices != .array or choices.array.items.len < 2 or choices.array.items.len > 64) return error.DecideRequestLimitExceeded;
                var criteria = std.json.ObjectMap{};
                for (choices.array.items) |choice| {
                    const option = try object(choice);
                    try keys(option, &.{ "value", "description", "examples" });
                    const label = try text(option.get("value") orelse return error.InvalidDecideRequest);
                    if (criteria.contains(label)) return error.InvalidDecideRequest;
                    const description = if (option.get("description")) |d| try text(d) else label;
                    if (option.get("examples")) |examples| {
                        var definition = std.json.ObjectMap{};
                        try definition.put(a, "description", str(description));
                        try definition.put(a, "examples", examples);
                        try criteria.put(a, label, .{ .object = definition });
                    } else try criteria.put(a, label, str(description));
                }
                try q.put(a, "criteria", .{ .object = criteria });
                if (fields.get("similarity_thresholds")) |thresholds| {
                    if (kind != .multi_choice or policy.options.calibration_id != null) return error.InvalidDecideRequest;
                    const out = try a.alloc(f64, criteria.count());
                    if (thresholds == .object) {
                        if (thresholds.object.count() != criteria.count()) return error.InvalidDecideRequest;
                        for (criteria.keys(), out) |label, *dest| dest.* = try scalar(thresholds.object.get(label) orelse return error.InvalidDecideRequest);
                    } else @memset(out, try scalar(thresholds));
                    policy.thresholds = out;
                }
                if (kind == .multi_choice and policy.thresholds == null and policy.options.calibration_id == null) return error.EmbeddingMultiLabelThresholdRequired;
                if (kind == .multi_choice and policy.options.min_similarity != null) return error.InvalidDecideRequest;
            },
            .score => {
                if (fields.contains("choices") or fields.contains("similarity_thresholds") or policy.embedding_configured) return error.InvalidDecideRequest;
                const levels = fields.get("levels") orelse return error.InvalidDecideRequest;
                if (levels != .array or levels.array.items.len < 2 or levels.array.items.len > 64) return error.DecideRequestLimitExceeded;
                var descriptions: std.array_list.Managed(V) = .init(a);
                const labels = try a.alloc([]const u8, levels.array.items.len);
                for (levels.array.items, labels, 0..) |level, *label, i| {
                    const fields_level = try object(level);
                    try keys(fields_level, &.{ "label", "description" });
                    label.* = try text(fields_level.get("label") orelse return error.InvalidDecideRequest);
                    for (labels[0..i]) |other| if (std.mem.eql(u8, other, label.*)) return error.InvalidDecideRequest;
                    try descriptions.append(str(if (fields_level.get("description")) |d| try text(d) else label.*));
                }
                policy.level_labels = labels;
                try q.put(a, "criteria", .{ .array = descriptions });
            },
            .predicate => if (fields.contains("choices") or fields.contains("levels") or fields.contains("similarity_thresholds") or policy.embedding_configured) return error.InvalidDecideRequest,
        }
        try lowered.put(a, name, .{ .object = q });
    }
    var inner = std.json.ObjectMap{};
    try inner.put(a, "model", str(model));
    if (root.get("model_identity")) |id| try inner.put(a, "model_identity", id);
    try inner.put(a, "state", str(items[0].input));
    try inner.put(a, "questions", .{ .object = lowered });
    const serialized = try std.json.Stringify.valueAlloc(a, V{ .object = inner }, .{});
    return .{ .inner = try legacy.parse(a, serialized), .items = items, .policies = policies, .batched = batched };
}

pub fn extractionInput(a: A, request: Request, gliner: bool) !legacy.ExtractionInput {
    const lowered = try legacy.extractionInput(a, request.inner, gliner);
    const value = try std.json.parseFromSliceLeaky(V, a, lowered.json, .{});
    var inputs: std.array_list.Managed(V) = .init(a);
    for (request.items) |item| {
        var fields = std.json.ObjectMap{};
        try fields.put(a, "content", str(item.input));
        if (item.id) |id| try fields.put(a, "id", str(id));
        try inputs.append(.{ .object = fields });
    }
    var root = value.object;
    try root.put(a, "inputs", .{ .array = inputs });
    return .{ .json = try std.json.Stringify.valueAlloc(a, V{ .object = root }, .{}), .schema_bytes = lowered.schema_bytes };
}

/// Preserve trained diagnostics while presenting the public answer arrays.
pub fn trainedResponse(a: A, request: Request, bytes: []const u8, gliner: bool) ![]u8 {
    const raw = try std.json.parseFromSliceLeaky(V, a, bytes, .{});
    if (raw != .object) return error.InvalidDecideOutput;
    const data = raw.object.get("data") orelse return error.InvalidDecideOutput;
    if (data != .array or data.array.items.len != request.items.len) return error.InvalidDecideOutput;
    var rows: std.array_list.Managed(V) = .init(a);
    var single_answers: V = undefined;
    var usage: V = undefined;
    for (data.array.items, 0..) |row, index| {
        var one = std.json.ObjectMap{};
        try one.put(a, "data", try array(a, &.{row}));
        try one.put(a, "usage", raw.object.get("usage") orelse return error.InvalidDecideOutput);
        const old_json = try legacy.responseJson(a, request.forItem(index), try std.json.Stringify.valueAlloc(a, V{ .object = one }, .{}), gliner);
        const old = try std.json.parseFromSliceLeaky(V, a, old_json, .{});
        usage = old.object.get("usage").?;
        var answers: std.array_list.Managed(V) = .init(a);
        for (request.inner.questions, request.policies) |question, policy| {
            const original = old.object.get("answers").?.object.get(question.name).?.object;
            var answer = std.json.ObjectMap{};
            for (original.keys(), original.values()) |key, v| {
                if (std.mem.eql(u8, key, "type") or std.mem.eql(u8, key, "noul") or std.mem.eql(u8, key, "probabilities") or std.mem.eql(u8, key, "legend")) continue;
                try answer.put(a, key, v);
            }
            try answer.put(a, "name", str(question.name));
            try answer.put(a, "type", str(@tagName(policy.kind)));
            try answer.put(a, "decision_method", str("typed"));
            if (policy.kind == .predicate) {
                try answer.put(a, "probability", original.get("noul").?);
            } else {
                var probabilities: std.array_list.Managed(V) = .init(a);
                const distribution = original.get("probabilities").?.object;
                for (question.labels, 0..) |label, i| {
                    var probability = std.json.ObjectMap{};
                    try probability.put(a, "probability", distribution.get(label).?);
                    try probability.put(a, "value", if (policy.kind == .score) .{ .integer = @intCast(i) } else str(label));
                    if (policy.kind == .score) try probability.put(a, "label", str(policy.level_labels[i]));
                    try probabilities.append(.{ .object = probability });
                }
                try answer.put(a, "probabilities", .{ .array = probabilities });
            }
            try answers.append(.{ .object = answer });
        }
        single_answers = .{ .array = answers };
        var result = std.json.ObjectMap{};
        try result.put(a, "input_index", .{ .integer = @intCast(index) });
        if (request.items[index].id) |id| try result.put(a, "id", str(id));
        try result.put(a, "answers", single_answers);
        try rows.append(.{ .object = result });
    }
    var root = std.json.ObjectMap{};
    try root.put(a, "model", str(request.inner.model));
    try root.put(a, "usage", usage);
    try root.put(a, if (request.batched) "data" else "answers", if (request.batched) .{ .array = rows } else single_answers);
    return std.json.Stringify.valueAlloc(a, V{ .object = root }, .{});
}

test "decisions public contract isolates per-question acceptance and preserves batches" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try parse(a,
        \\{"model":"embeddinggemma2","inputs":[{"id":"a","input":"one"},{"input":"two"}],"embedding_options":{"dimensions":128},"questions":[{"name":"route","type":"choice","instructions":"Route","choices":[{"value":"a"},{"value":"b"}],"embedding_options":{"min_margin":0.2}},{"name":"tags","type":"multi_choice","instructions":"Tags","choices":[{"value":"x"},{"value":"y"}],"similarity_thresholds":{"x":0.4,"y":0.5}}]}
    );
    try std.testing.expectEqual(@as(usize, 2), request.items.len);
    try std.testing.expectEqual(@as(usize, 128), request.policies[0].options.dimensions);
    try std.testing.expectApproxEqAbs(@as(f64, 0.2), request.policies[0].options.min_margin.?, 1e-9);
    try std.testing.expect(request.policies[1].options.min_margin == null);
    try std.testing.expectEqualSlices(f64, &.{ 0.4, 0.5 }, request.policies[1].thresholds.?);
    try std.testing.expectError(error.InvalidDecideRequest, parse(a,
        \\{"model":"m","state":"old","questions":{}}
    ));
    try std.testing.expectError(error.InvalidDecideRequest, parse(a,
        \\{"model":"m","input":"x","embedding_options":{"min_margin":0.1},"questions":[{"name":"p","type":"predicate","instructions":"?"}]}
    ));
}

test "decisions public trained batches preserve diagnostics labels and aggregate usage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try parse(a,
        \\{"model":"laya","inputs":[{"id":"first","input":"one"},{"input":"two"}],"questions":[{"name":"risk","type":"score","instructions":"Risk?","levels":[{"label":"safe","description":"Low risk"},{"label":"unsafe","description":"High risk"}]},{"name":"act","type":"predicate","instructions":"Act?"}]}
    );
    const response = try trainedResponse(a, request,
        \\{"data":[{"decisions":[{"name":"risk","probabilities":[{"label":"0","probability":0.2},{"label":"1","probability":0.8}],"confidence":0.4,"confidence_method":"normalized_inverse_entropy","act_probability":0.9},{"name":"act","probabilities":[{"label":"false","probability":0.1},{"label":"true","probability":0.9}],"confidence":0.9,"confidence_method":"max_probability","act_probability":0.8}]},{"decisions":[{"name":"risk","probabilities":[{"label":"0","probability":0.7},{"label":"1","probability":0.3}],"confidence":0.1,"confidence_method":"normalized_inverse_entropy"},{"name":"act","probabilities":[{"label":"false","probability":0.8},{"label":"true","probability":0.2}],"confidence":0.8,"confidence_method":"max_probability"}]}],"usage":{"prompt_tokens":30,"completion_tokens":0}}
    , false);
    const value = try std.json.parseFromSliceLeaky(V, a, response, .{});
    try std.testing.expect(!value.object.contains("answers"));
    try std.testing.expectEqual(@as(i64, 30), value.object.get("usage").?.object.get("input_tokens").?.integer);
    const rows = value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("first", rows[0].object.get("id").?.string);
    const risk = rows[0].object.get("answers").?.array.items[0].object;
    try std.testing.expectEqualStrings("safe", risk.get("probabilities").?.array.items[0].object.get("label").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.4), risk.get("confidence").?.float, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), risk.get("act_probability").?.float, 1e-9);
    const predicate = rows[1].object.get("answers").?.array.items[1].object;
    try std.testing.expectEqualStrings("predicate", predicate.get("type").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 0.2), predicate.get("probability").?.float, 1e-9);
    try std.testing.expect(!predicate.contains("noul"));
}

test "decisions public rejects duplicate questions and extraction decision aliases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectError(error.InvalidDecideRequest, parse(a,
        \\{"model":"m","input":"x","questions":[{"name":"a","type":"predicate","instructions":"?"},{"name":"a","type":"predicate","instructions":"?"}]}
    ));
    try std.testing.expectError(error.InvalidDecideRequest, parse(a,
        \\{"model":"m","input":"x","inputs":[{"input":"y"}],"questions":[{"name":"a","type":"predicate","instructions":"?"}]}
    ));
    for ([_][]const u8{
        "{\"options\":{\"embedding\":{}}}",
        "{\"schema\":{\"classifications\":[{\"mode\":\"boolean\"}]}}",
        "{\"schema\":{\"classifications\":[{\"mode\":\"ordinal\"}]}}",
        "{\"inputs\":[{\"schema\":{\"classifications\":[{\"similarity_thresholds\":0.4}]}}]}",
    }) |json| try std.testing.expectError(error.UnsupportedExtractionFeature, validateExtractionBoundary(try std.json.parseFromSliceLeaky(V, a, json, .{})));
    try validateExtractionBoundary(try std.json.parseFromSliceLeaky(V, a, "{\"schema\":{\"entities\":[\"person\"],\"classifications\":[{\"mode\":\"multi\",\"labels\":[\"a\",\"b\"]}]}}", .{}));
}
