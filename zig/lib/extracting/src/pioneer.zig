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

//! Pioneer (Fastino Labs) hosted GLiNER extraction.
//!
//! Each input is one `POST /v1/chat/completions` call carrying the selected
//! `model` and Fastino's unified GLiNER `schema`. The encoder answer arrives as
//! a JSON string in `choices[0].message.content` using GLiNER2's formatted
//! result shape: entities grouped by label, relations under
//! `relation_extraction`, and one key per classification or structure task.
//! Character offsets are Python string indices (Unicode codepoints); they are
//! converted to UTF-8 byte offsets and kept only when they reproduce the
//! returned text exactly. Any unrecognized result shape fails closed.

const std = @import("std");
const text = @import("text.zig");

const A = std.mem.Allocator;
const V = std.json.Value;

pub const default_url = "https://api.fastino.ai";
pub const completions_path = "/v1/chat/completions";
pub const credential_env = "FASTINO_API_KEY";

pub const OutputOptions = struct {
    include_confidence: bool = true,
    include_spans: bool = true,
};

const FieldKind = enum { str, list };

/// The validated mapping from Antfly's extraction schema to Fastino's unified
/// GLiNER schema, retaining the task names needed to decode the answer.
pub const Plan = struct {
    schema: V,
    threshold: ?V = null,
    output: OutputOptions = .{},
    entity_labels: []const []const u8 = &.{},
    relation_types: []const []const u8 = &.{},
    classifications: []const Classification = &.{},
    structures: []const Structure = &.{},

    pub const Classification = struct { name: []const u8, multi_label: bool };
    pub const Structure = struct { name: []const u8, fields: []const Field };
    pub const Field = struct { name: []const u8, kind: FieldKind };

    fn isEntityLabel(self: Plan, name: []const u8) bool {
        return contains(self.entity_labels, name);
    }
    fn classification(self: Plan, name: []const u8) ?Classification {
        for (self.classifications) |item| if (std.mem.eql(u8, item.name, name)) return item;
        return null;
    }
    fn structure(self: Plan, name: []const u8) ?Structure {
        for (self.structures) |item| if (std.mem.eql(u8, item.name, name)) return item;
        return null;
    }
};

fn contains(names: []const []const u8, name: []const u8) bool {
    for (names) |item| if (std.mem.eql(u8, item, name)) return true;
    return false;
}

fn object() V {
    return .{ .object = .{} };
}

fn put(a: A, target: *V, key: []const u8, value: V) !void {
    try target.object.put(a, key, value);
}

fn onlyKeys(value: V, allowed: []const []const u8) !void {
    if (value != .object) return error.UnsupportedPioneerSchema;
    var it = value.object.iterator();
    while (it.next()) |entry| if (!contains(allowed, entry.key_ptr.*)) return error.UnsupportedPioneerSchema;
}

fn nonEmptyString(value: ?V) ![]const u8 {
    const raw = value orelse return error.UnsupportedPioneerSchema;
    if (raw != .string or raw.string.len == 0) return error.UnsupportedPioneerSchema;
    return raw.string;
}

/// Version 1 entities, relations, classifications and structures map to
/// Fastino's hosted schema. Endpoint constraints, attributes, constrained
/// classification, joint IE and typed numeric fields have no hosted
/// equivalent here and are rejected before any request is sent.
pub fn compile(a: A, schema: V, options: ?V) !Plan {
    try onlyKeys(schema, &.{ "entities", "relations", "classifications", "structures", "entity_definitions" });
    var plan = Plan{ .schema = object() };

    const definitions = schema.object.get("entity_definitions");
    if (schema.object.get("entities")) |raw| {
        if (raw != .array or raw.array.items.len == 0) return error.UnsupportedPioneerSchema;
        const labels = try a.alloc([]const u8, raw.array.items.len);
        var described = false;
        var entities = object();
        for (raw.array.items, labels) |item, *label| {
            label.* = try nonEmptyString(item);
            if (entities.object.contains(label.*)) return error.UnsupportedPioneerSchema;
            var description: V = .null;
            if (definitions) |defs| {
                if (defs != .object) return error.UnsupportedPioneerSchema;
                if (defs.object.get(label.*)) |def| {
                    try onlyKeys(def, &.{"description"});
                    if (def.object.get("description")) |d| {
                        description = .{ .string = try nonEmptyString(d) };
                        described = true;
                    }
                }
            }
            try put(a, &entities, label.*, description);
        }
        if (definitions) |defs| {
            var it = defs.object.iterator();
            while (it.next()) |entry| if (!contains(labels, entry.key_ptr.*)) return error.UnsupportedPioneerSchema;
        }
        if (described) {
            // Labels without a description keep an empty description.
            var it = entities.object.iterator();
            while (it.next()) |entry| if (entry.value_ptr.* == .null) {
                entry.value_ptr.* = .{ .string = "" };
            };
            try put(a, &plan.schema, "entities", entities);
        } else {
            var list = std.json.Array.init(a);
            for (labels) |label| try list.append(.{ .string = label });
            try put(a, &plan.schema, "entities", .{ .array = list });
        }
        plan.entity_labels = labels;
    } else if (definitions != null) return error.UnsupportedPioneerSchema;

    if (schema.object.get("relations")) |raw| {
        if (raw != .array or raw.array.items.len == 0) return error.UnsupportedPioneerSchema;
        const types = try a.alloc([]const u8, raw.array.items.len);
        var relations = object();
        var described = false;
        for (raw.array.items, 0..) |item, i| {
            // Fastino relation output carries endpoint text, not endpoint
            // labels, so source/target label constraints cannot be honored.
            try onlyKeys(item, &.{ "type", "description" });
            const name = try nonEmptyString(item.object.get("type"));
            if (contains(types[0..i], name)) return error.UnsupportedPioneerSchema;
            types[i] = name;
            const description: V = if (item.object.get("description")) |d| blk: {
                described = true;
                break :blk .{ .string = try nonEmptyString(d) };
            } else .{ .string = "" };
            try put(a, &relations, name, description);
        }
        if (described) {
            try put(a, &plan.schema, "relations", relations);
        } else {
            var list = std.json.Array.init(a);
            for (types) |name| try list.append(.{ .string = name });
            try put(a, &plan.schema, "relations", .{ .array = list });
        }
        plan.relation_types = types;
    }

    if (schema.object.get("classifications")) |raw| {
        if (raw != .array or raw.array.items.len == 0) return error.UnsupportedPioneerSchema;
        const tasks = try a.alloc(Plan.Classification, raw.array.items.len);
        var classifications = object();
        for (raw.array.items, tasks) |item, *task| {
            try onlyKeys(item, &.{ "name", "labels", "multi_label" });
            const name = try nonEmptyString(item.object.get("name"));
            const labels = item.object.get("labels") orelse return error.UnsupportedPioneerSchema;
            if (labels != .array or labels.array.items.len < 2) return error.UnsupportedPioneerSchema;
            var label_list = std.json.Array.init(a);
            for (labels.array.items) |label| try label_list.append(.{ .string = try nonEmptyString(label) });
            const multi = if (item.object.get("multi_label")) |m| switch (m) {
                .bool => |value| value,
                else => return error.UnsupportedPioneerSchema,
            } else false;
            if (classifications.object.contains(name)) return error.UnsupportedPioneerSchema;
            var task_schema = object();
            try put(a, &task_schema, "labels", .{ .array = label_list });
            try put(a, &task_schema, "multi_label", .{ .bool = multi });
            try put(a, &classifications, name, task_schema);
            task.* = .{ .name = name, .multi_label = multi };
        }
        try put(a, &plan.schema, "classifications", classifications);
        plan.classifications = tasks;
    }

    if (schema.object.get("structures")) |raw| {
        if (raw != .object or raw.object.count() == 0) return error.UnsupportedPioneerSchema;
        const structures = try a.alloc(Plan.Structure, raw.object.count());
        var wire = object();
        var it = raw.object.iterator();
        var i: usize = 0;
        while (it.next()) |entry| : (i += 1) {
            try onlyKeys(entry.value_ptr.*, &.{"fields"});
            const fields_raw = entry.value_ptr.object.get("fields") orelse return error.UnsupportedPioneerSchema;
            if (fields_raw != .object or fields_raw.object.count() == 0) return error.UnsupportedPioneerSchema;
            const fields = try a.alloc(Plan.Field, fields_raw.object.count());
            var specs = std.json.Array.init(a);
            var field_it = fields_raw.object.iterator();
            var j: usize = 0;
            while (field_it.next()) |field_entry| : (j += 1) {
                const spec = try fieldSpec(a, field_entry.key_ptr.*, field_entry.value_ptr.*);
                fields[j] = .{ .name = field_entry.key_ptr.*, .kind = spec.kind };
                try specs.append(spec.wire);
            }
            try put(a, &wire, entry.key_ptr.*, .{ .array = specs });
            structures[i] = .{ .name = entry.key_ptr.*, .fields = fields };
        }
        try put(a, &plan.schema, "structures", wire);
        plan.structures = structures;
    }

    if (plan.schema.object.count() == 0) return error.UnsupportedPioneerSchema;
    // Result keys are task names; a name shared between tasks would make the
    // answer ambiguous.
    for (plan.classifications) |task| {
        if (plan.structure(task.name) != null or plan.isEntityLabel(task.name) or isReservedKey(task.name)) return error.UnsupportedPioneerSchema;
    }
    for (plan.structures) |item| if (plan.isEntityLabel(item.name) or isReservedKey(item.name)) return error.UnsupportedPioneerSchema;

    if (options) |raw| if (raw != .null) {
        try onlyKeys(raw, &.{ "threshold", "include_confidence", "include_spans" });
        if (raw.object.get("threshold")) |threshold| switch (threshold) {
            .integer, .float, .number_string => plan.threshold = threshold,
            else => return error.UnsupportedPioneerSchema,
        };
        inline for (.{ "include_confidence", "include_spans" }) |name| if (raw.object.get(name)) |flag| switch (flag) {
            .bool => |value| @field(plan.output, name) = value,
            else => return error.UnsupportedPioneerSchema,
        };
    };
    return plan;
}

fn isReservedKey(name: []const u8) bool {
    return std.mem.eql(u8, name, "entities") or std.mem.eql(u8, name, "relation_extraction");
}

fn fieldSpec(a: A, name: []const u8, raw: V) !struct { wire: V, kind: FieldKind } {
    const type_name, const choices = switch (raw) {
        .string => |value| .{ value, null },
        .object => |fields| blk: {
            try onlyKeys(raw, &.{ "type", "dtype", "choices", "enum", "description" });
            const type_value = fields.get("type") orelse fields.get("dtype") orelse V{ .string = "str" };
            if (fields.get("type") != null and fields.get("dtype") != null) return error.UnsupportedPioneerSchema;
            const choice_value = fields.get("choices") orelse fields.get("enum");
            if (fields.get("choices") != null and fields.get("enum") != null) return error.UnsupportedPioneerSchema;
            break :blk .{ try nonEmptyString(type_value), choice_value };
        },
        else => return error.UnsupportedPioneerSchema,
    };
    const kind: FieldKind = if (std.mem.eql(u8, type_name, "str") or std.mem.eql(u8, type_name, "string") or std.mem.eql(u8, type_name, "text"))
        .str
    else if (std.mem.eql(u8, type_name, "list") or std.mem.eql(u8, type_name, "array"))
        .list
    else
        return error.UnsupportedPioneerSchema;
    const dtype: []const u8 = if (kind == .str) "str" else "list";
    const description = if (raw == .object) raw.object.get("description") else null;
    if (choices == null and description == null) {
        return .{ .wire = .{ .string = try std.fmt.allocPrint(a, "{s}::{s}", .{ name, dtype }) }, .kind = kind };
    }
    var wire = object();
    try put(a, &wire, "name", .{ .string = name });
    try put(a, &wire, "dtype", .{ .string = dtype });
    if (description) |d| try put(a, &wire, "description", .{ .string = try nonEmptyString(d) });
    if (choices) |c| {
        if (c != .array or c.array.items.len < 2) return error.UnsupportedPioneerSchema;
        var list = std.json.Array.init(a);
        for (c.array.items) |choice| try list.append(.{ .string = try nonEmptyString(choice) });
        try put(a, &wire, "choices", .{ .array = list });
    }
    return .{ .wire = wire, .kind = kind };
}

/// Extraction content is a chat message content value: a string, or text
/// parts. Media parts have no hosted GLiNER equivalent.
pub fn contentText(a: A, content: V) ![]const u8 {
    switch (content) {
        .string => |value| return value,
        .array => |parts| {
            var out = std.ArrayList(u8).empty;
            for (parts.items, 0..) |part, i| {
                if (part != .object) return error.UnsupportedPioneerInput;
                const kind = part.object.get("type") orelse return error.UnsupportedPioneerInput;
                if (kind != .string or !std.mem.eql(u8, kind.string, "text")) return error.UnsupportedPioneerInput;
                const value = part.object.get("text") orelse return error.UnsupportedPioneerInput;
                if (value != .string) return error.UnsupportedPioneerInput;
                if (i > 0) try out.append(a, '\n');
                try out.appendSlice(a, value.string);
            }
            return try out.toOwnedSlice(a);
        },
        else => return error.UnsupportedPioneerInput,
    }
}

pub fn requestBody(a: A, model: []const u8, plan: Plan, source: []const u8) ![]u8 {
    var body = object();
    try put(a, &body, "model", .{ .string = model });
    var message = object();
    try put(a, &message, "role", .{ .string = "user" });
    try put(a, &message, "content", .{ .string = source });
    var messages = std.json.Array.init(a);
    try messages.append(message);
    try put(a, &body, "messages", .{ .array = messages });
    try put(a, &body, "schema", plan.schema);
    if (plan.threshold) |threshold| try put(a, &body, "threshold", threshold);
    // Spans and confidence are always requested so offsets can be verified;
    // the caller's output options decide what is retained.
    try put(a, &body, "include_confidence", .{ .bool = true });
    try put(a, &body, "include_spans", .{ .bool = true });
    try put(a, &body, "stream", .{ .bool = false });
    // Enrichment inputs are database content; do not opt them into hosted
    // inference retention.
    try put(a, &body, "store", .{ .bool = false });
    return try std.json.Stringify.valueAlloc(a, body, .{});
}

pub const Usage = struct { input_tokens: i64 = 0, output_tokens: i64 = 0 };

pub const Decoded = struct { row: V, usage: Usage };

fn parseLeaky(a: A, raw: []const u8) !V {
    return std.json.parseFromSliceLeaky(V, a, raw, .{
        .allocate = .alloc_always,
        .duplicate_field_behavior = .@"error",
        .parse_numbers = false,
    }) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidExtractionResponse,
    };
}

fn integer(value: V) !i64 {
    return switch (value) {
        .integer => |n| n,
        .number_string => |lexeme| std.fmt.parseInt(i64, lexeme, 10) catch error.InvalidExtractionResponse,
        else => error.InvalidExtractionResponse,
    };
}

fn score(value: V) !V {
    const parsed: f64 = switch (value) {
        .integer => |n| @floatFromInt(n),
        .float => |f| f,
        .number_string => |lexeme| std.fmt.parseFloat(f64, lexeme) catch return error.InvalidExtractionResponse,
        else => return error.InvalidExtractionResponse,
    };
    if (!(parsed >= 0 and parsed <= 1)) return error.InvalidExtractionResponse;
    return value;
}

/// Decode one chat completion envelope into a canonical extraction row.
pub fn decode(a: A, body: []const u8, plan: Plan, source: []const u8, id: ?[]const u8) !Decoded {
    const envelope = try parseLeaky(a, body);
    if (envelope != .object) return error.InvalidExtractionResponse;
    const choices = envelope.object.get("choices") orelse return error.InvalidExtractionResponse;
    if (choices != .array or choices.array.items.len != 1) return error.InvalidExtractionResponse;
    const choice = choices.array.items[0];
    if (choice != .object) return error.InvalidExtractionResponse;
    const message = choice.object.get("message") orelse return error.InvalidExtractionResponse;
    if (message != .object) return error.InvalidExtractionResponse;
    const content = message.object.get("content") orelse return error.InvalidExtractionResponse;
    if (content != .string) return error.InvalidExtractionResponse;
    const result = try parseLeaky(a, content.string);

    var usage = Usage{};
    if (envelope.object.get("usage")) |raw| if (raw == .object) {
        if (raw.object.get("prompt_tokens")) |n| usage.input_tokens = try integer(n);
        if (raw.object.get("completion_tokens")) |n| usage.output_tokens = try integer(n);
    };
    return .{ .row = try decodeResult(a, result, plan, source, id), .usage = usage };
}

pub fn decodeResult(a: A, result: V, plan: Plan, source: []const u8, id: ?[]const u8) !V {
    if (result != .object) return error.InvalidExtractionResponse;
    var offsets = try text.OffsetMap.init(a, source, std.math.maxInt(usize));
    defer offsets.deinit();
    var decoder = Decoder{ .a = a, .plan = plan, .source = source, .offsets = offsets };

    var entities = std.json.Array.init(a);
    var relations = std.json.Array.init(a);
    var classifications = std.json.Array.init(a);
    var structures = object();

    var it = result.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "entities") and plan.entity_labels.len > 0) {
            try decoder.entityGroups(&entities, value);
        } else if (std.mem.eql(u8, key, "relation_extraction") and plan.relation_types.len > 0) {
            try decoder.relationGroups(&relations, value);
        } else if (plan.classification(key)) |task| {
            try decoder.classification(&classifications, task, value);
        } else if (plan.structure(key)) |item| {
            try put(a, &structures, key, try decoder.structureRecords(item, value));
        } else if (plan.isEntityLabel(key) and !result.object.contains("entities")) {
            // An entity-only answer may omit the `entities` wrapper.
            try decoder.entityGroup(&entities, key, value);
        } else return error.InvalidExtractionResponse;
    }

    var row = object();
    if (id) |value| try put(a, &row, "id", .{ .string = value });
    if (plan.output.include_spans) try put(a, &row, "offset_unit", .{ .string = "utf8_bytes" });
    if (plan.entity_labels.len > 0) try put(a, &row, "entities", .{ .array = entities });
    if (plan.relation_types.len > 0) try put(a, &row, "relations", .{ .array = relations });
    if (plan.classifications.len > 0) try put(a, &row, "classifications", .{ .array = classifications });
    if (plan.structures.len > 0) try put(a, &row, "structures", structures);
    return row;
}

const Decoder = struct {
    a: A,
    plan: Plan,
    source: []const u8,
    offsets: text.OffsetMap,

    /// A mention is `"text"` or `{text, confidence?, start?, end?}`.
    fn mention(self: Decoder, raw: V, label: ?[]const u8) !V {
        var out = object();
        if (label) |name| try put(self.a, &out, "label", .{ .string = name });
        switch (raw) {
            .string => |value| try put(self.a, &out, "text", .{ .string = value }),
            .object => |fields| {
                const surface = fields.get("text") orelse return error.InvalidExtractionResponse;
                if (surface != .string) return error.InvalidExtractionResponse;
                try put(self.a, &out, "text", surface);
                if (self.plan.output.include_spans) if (try self.span(fields, surface.string)) |range| {
                    try put(self.a, &out, "start", .{ .integer = @intCast(range.start) });
                    try put(self.a, &out, "end", .{ .integer = @intCast(range.end) });
                };
                if (fields.get("confidence")) |confidence| {
                    const checked = try score(confidence);
                    if (self.plan.output.include_confidence) try put(self.a, &out, "score", checked);
                }
            },
            else => return error.InvalidExtractionResponse,
        }
        return out;
    }

    /// Codepoint offsets become UTF-8 byte offsets only when they reproduce
    /// the returned surface text.
    fn span(self: Decoder, fields: std.json.ObjectMap, surface: []const u8) !?text.Offsets {
        const start_raw = fields.get("start") orelse return null;
        const end_raw = fields.get("end") orelse return error.InvalidExtractionResponse;
        const start = try integer(start_raw);
        const end = try integer(end_raw);
        if (start < 0 or end < start) return error.InvalidExtractionResponse;
        const bytes = self.offsets.toBytes(.{ .start = @intCast(start), .end = @intCast(end) }, .unicode_codepoints) catch return null;
        if (!std.mem.eql(u8, self.source[bytes.start..bytes.end], surface)) return null;
        return bytes;
    }

    fn entityGroups(self: Decoder, out: *std.json.Array, value: V) !void {
        if (value != .object) return error.InvalidExtractionResponse;
        var it = value.object.iterator();
        while (it.next()) |entry| {
            if (!self.plan.isEntityLabel(entry.key_ptr.*)) return error.InvalidExtractionResponse;
            try self.entityGroup(out, entry.key_ptr.*, entry.value_ptr.*);
        }
    }

    fn entityGroup(self: Decoder, out: *std.json.Array, label: []const u8, value: V) !void {
        switch (value) {
            .array => |items| for (items.items) |item| try out.append(try self.mention(item, label)),
            .null => {},
            // A scalar entity dtype returns one mention.
            .string, .object => try out.append(try self.mention(value, label)),
            else => return error.InvalidExtractionResponse,
        }
    }

    fn relationGroups(self: Decoder, out: *std.json.Array, value: V) !void {
        if (value != .object) return error.InvalidExtractionResponse;
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            if (!contains(self.plan.relation_types, name)) return error.InvalidExtractionResponse;
            const items = entry.value_ptr.*;
            if (items != .array) return error.InvalidExtractionResponse;
            for (items.array.items) |item| {
                // `{head, tail, confidence?}` or a `[head, tail]` pair.
                const head, const tail, const confidence = switch (item) {
                    .object => |fields| .{
                        fields.get("head") orelse return error.InvalidExtractionResponse,
                        fields.get("tail") orelse return error.InvalidExtractionResponse,
                        fields.get("confidence"),
                    },
                    .array => |pair| blk: {
                        if (pair.items.len != 2) return error.InvalidExtractionResponse;
                        break :blk .{ pair.items[0], pair.items[1], null };
                    },
                    else => return error.InvalidExtractionResponse,
                };
                var relation = object();
                try put(self.a, &relation, "type", .{ .string = name });
                try put(self.a, &relation, "source", try self.mention(head, null));
                try put(self.a, &relation, "target", try self.mention(tail, null));
                if (confidence) |c| {
                    const checked = try score(c);
                    if (self.plan.output.include_confidence) try put(self.a, &relation, "score", checked);
                }
                try out.append(relation);
            }
        }
    }

    /// A label is `"label"` or `{label|category, confidence?}`; multi-label
    /// tasks return an array of them.
    fn classification(self: Decoder, out: *std.json.Array, task: Plan.Classification, value: V) !void {
        switch (value) {
            .array => |items| {
                if (!task.multi_label and items.items.len > 1) return error.InvalidExtractionResponse;
                for (items.items) |item| try out.append(try self.classLabel(task.name, item));
            },
            .null => {},
            else => try out.append(try self.classLabel(task.name, value)),
        }
    }

    fn classLabel(self: Decoder, name: []const u8, raw: V) !V {
        var out = object();
        try put(self.a, &out, "name", .{ .string = name });
        switch (raw) {
            .string => try put(self.a, &out, "label", raw),
            .object => |fields| {
                const value = fields.get("label") orelse fields.get("category") orelse return error.InvalidExtractionResponse;
                if (value != .string) return error.InvalidExtractionResponse;
                try put(self.a, &out, "label", value);
                if (fields.get("confidence")) |confidence| {
                    const checked = try score(confidence);
                    if (self.plan.output.include_confidence) try put(self.a, &out, "score", checked);
                }
            },
            else => return error.InvalidExtractionResponse,
        }
        return out;
    }

    fn structureRecords(self: Decoder, item: Plan.Structure, value: V) !V {
        var records = std.json.Array.init(self.a);
        switch (value) {
            .array => |items| for (items.items) |record| try records.append(try self.structureRecord(item, record)),
            .object => try records.append(try self.structureRecord(item, value)),
            .null => {},
            else => return error.InvalidExtractionResponse,
        }
        return .{ .array = records };
    }

    fn structureRecord(self: Decoder, item: Plan.Structure, raw: V) !V {
        if (raw != .object) return error.InvalidExtractionResponse;
        var out = object();
        var it = raw.object.iterator();
        while (it.next()) |entry| {
            const field = for (item.fields) |f| {
                if (std.mem.eql(u8, f.name, entry.key_ptr.*)) break f;
            } else return error.InvalidExtractionResponse;
            try put(self.a, &out, field.name, try self.fieldValue(field.kind, entry.value_ptr.*));
        }
        return out;
    }

    fn fieldValue(self: Decoder, kind: FieldKind, raw: V) !V {
        var out = object();
        if (kind == .str) {
            const one = switch (raw) {
                .array => |items| switch (items.items.len) {
                    0 => V.null,
                    1 => items.items[0],
                    else => return error.InvalidExtractionResponse,
                },
                else => raw,
            };
            if (one == .null) {
                try put(self.a, &out, "value", .null);
                return out;
            }
            const value = try self.mention(one, null);
            try put(self.a, &out, "value", value.object.get("text").?);
            inline for (.{ "start", "end", "score" }) |name| if (value.object.get(name)) |v| try put(self.a, &out, name, v);
            return out;
        }
        const items: []const V = switch (raw) {
            .array => |list| list.items,
            .null => &.{},
            else => &.{raw},
        };
        var values = std.json.Array.init(self.a);
        var spans = std.json.Array.init(self.a);
        for (items) |entry| {
            const value = try self.mention(entry, null);
            try values.append(value.object.get("text").?);
            if (value.object.get("start")) |start| {
                var range = std.json.Array.init(self.a);
                try range.append(start);
                try range.append(value.object.get("end").?);
                try spans.append(.{ .array = range });
            } else try spans.append(.null);
        }
        try put(self.a, &out, "value", .{ .array = values });
        if (self.plan.output.include_spans) try put(self.a, &out, "spans", .{ .array = spans });
        return out;
    }
};

test "pioneer compiles antfly schema to fastino unified schema" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try parseLeaky(a,
        \\{"entities":["person","company"],"entity_definitions":{"company":{"description":"an organization"}},
        \\ "relations":[{"type":"works_for"}],
        \\ "classifications":[{"name":"sentiment","labels":["positive","negative"]},{"name":"aspects","labels":["camera","battery"],"multi_label":true}],
        \\ "structures":{"product":{"fields":{"name":"str","colors":{"type":"list"},"tier":{"type":"str","choices":["basic","pro"],"description":"plan tier"}}}}}
    );
    const plan = try compile(a, schema, try parseLeaky(a, "{\"threshold\":0.4}"));
    const wire = try std.json.Stringify.valueAlloc(a, plan.schema, .{});
    try std.testing.expectEqualStrings(
        \\{"entities":{"person":"","company":"an organization"},"relations":["works_for"],"classifications":{"sentiment":{"labels":["positive","negative"],"multi_label":false},"aspects":{"labels":["camera","battery"],"multi_label":true}},"structures":{"product":["name::str","colors::list",{"name":"tier","dtype":"str","description":"plan tier","choices":["basic","pro"]}]}}
    , wire);
    const body = try requestBody(a, "fastino/gliner2.5-multi-v1", plan, "Ada works for Antfly.");
    try std.testing.expectEqualStrings(
        \\{"model":"fastino/gliner2.5-multi-v1","messages":[{"role":"user","content":"Ada works for Antfly."}],"schema":{"entities":{"person":"","company":"an organization"},"relations":["works_for"],"classifications":{"sentiment":{"labels":["positive","negative"],"multi_label":false},"aspects":{"labels":["camera","battery"],"multi_label":true}},"structures":{"product":["name::str","colors::list",{"name":"tier","dtype":"str","description":"plan tier","choices":["basic","pro"]}]}},"threshold":0.4,"include_confidence":true,"include_spans":true,"stream":false,"store":false}
    , body);
}

test "pioneer rejects schema features without a hosted equivalent" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "{}",
        "{\"entities\":[]}",
        "{\"relations\":[{\"type\":\"works_for\",\"source\":\"person\"}]}",
        "{\"classifications\":[{\"name\":\"s\",\"labels\":[\"a\",\"b\"],\"top_k\":2}]}",
        "{\"structures\":{\"r\":{\"fields\":{\"total\":\"number\"}}}}",
        "{\"structures\":{\"r\":{\"fields\":{\"x\":\"str\"},\"mode\":\"natural\"}}}",
        "{\"entities\":[\"person\"],\"entity_attributes\":{}}",
        "{\"entities\":[\"person\"],\"entity_definitions\":{\"place\":{\"description\":\"x\"}}}",
        "{\"entities\":[\"person\"],\"entity_definitions\":{\"person\":{\"threshold\":0.2}}}",
        "{\"joint_ie\":{}}",
        "{\"entities\":[\"sentiment\"],\"classifications\":[{\"name\":\"sentiment\",\"labels\":[\"a\",\"b\"]}]}",
    }) |raw| try std.testing.expectError(error.UnsupportedPioneerSchema, compile(a, try parseLeaky(a, raw), null));
    try std.testing.expectError(error.UnsupportedPioneerSchema, compile(a, try parseLeaky(a, "{\"entities\":[\"person\"]}"), try parseLeaky(a, "{\"flat_ner\":true}")));
}

test "pioneer decodes formatted GLiNER2 results with verified spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const plan = try compile(a, try parseLeaky(a,
        \\{"entities":["person","company"],"relations":[{"type":"works_for"}],
        \\ "classifications":[{"name":"sentiment","labels":["positive","negative"]},{"name":"aspects","labels":["camera","battery"],"multi_label":true}],
        \\ "structures":{"product":{"fields":{"name":"str","colors":"list"}}}}
    ), null);
    // "Zoë" makes codepoint and byte offsets differ.
    const source = "Zoë works for Antfly. Buy the Ant Pro in red and blue.";
    const content =
        \\{"entities":{"person":[{"text":"Zoë","confidence":0.97,"start":0,"end":3}],"company":[{"text":"Antfly","confidence":0.91,"start":14,"end":20},{"text":"Antfly","start":0,"end":6}]},
        \\ "relation_extraction":{"works_for":[{"head":{"text":"Zoë","confidence":0.9,"start":0,"end":3},"tail":{"text":"Antfly","confidence":0.88,"start":14,"end":20}}]},
        \\ "sentiment":{"label":"positive","confidence":0.8},
        \\ "aspects":[{"label":"camera","confidence":0.7}],
        \\ "product":[{"name":{"text":"Ant Pro","confidence":0.9,"start":30,"end":37},"colors":[{"text":"red","start":41,"end":44},"blue"]}]}
    ;
    var envelope = object();
    var message = object();
    try put(a, &message, "content", .{ .string = content });
    var choice = object();
    try put(a, &choice, "message", message);
    var choices = std.json.Array.init(a);
    try choices.append(choice);
    try put(a, &envelope, "choices", .{ .array = choices });
    try put(a, &envelope, "usage", try parseLeaky(a, "{\"prompt_tokens\":21,\"completion_tokens\":0}"));
    const decoded = try decode(a, try std.json.Stringify.valueAlloc(a, envelope, .{}), plan, source, "doc-1");
    try std.testing.expectEqual(@as(i64, 21), decoded.usage.input_tokens);
    try std.testing.expectEqualStrings(
        \\{"id":"doc-1","offset_unit":"utf8_bytes","entities":[{"label":"person","text":"Zoë","start":0,"end":4,"score":0.97},{"label":"company","text":"Antfly","start":15,"end":21,"score":0.91},{"label":"company","text":"Antfly"}],"relations":[{"type":"works_for","source":{"text":"Zoë","start":0,"end":4,"score":0.9},"target":{"text":"Antfly","start":15,"end":21,"score":0.88}}],"classifications":[{"name":"sentiment","label":"positive","score":0.8},{"name":"aspects","label":"camera","score":0.7}],"structures":{"product":[{"name":{"value":"Ant Pro","start":31,"end":38,"score":0.9},"colors":{"value":["red","blue"],"spans":[[42,45],null]}}]}}
    , try std.json.Stringify.valueAlloc(a, decoded.row, .{}));
}

test "pioneer decodes plain and unwrapped entity answers and honors output options" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const plan = try compile(a, try parseLeaky(a, "{\"entities\":[\"person\"],\"relations\":[{\"type\":\"knows\"}]}"), try parseLeaky(a, "{\"include_confidence\":false,\"include_spans\":false}"));
    const row = try decodeResult(a, try parseLeaky(a, "{\"person\":[{\"text\":\"Ada\",\"confidence\":0.9,\"start\":0,\"end\":3},\"Bob\"],\"relation_extraction\":{\"knows\":[[\"Ada\",\"Bob\"]]}}"), plan, "Ada knows Bob", null);
    try std.testing.expectEqualStrings(
        \\{"entities":[{"label":"person","text":"Ada"},{"label":"person","text":"Bob"}],"relations":[{"type":"knows","source":{"text":"Ada"},"target":{"text":"Bob"}}]}
    , try std.json.Stringify.valueAlloc(a, row, .{}));
}

test "pioneer fails closed on unrecognized answers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const plan = try compile(a, try parseLeaky(a, "{\"entities\":[\"person\"],\"classifications\":[{\"name\":\"s\",\"labels\":[\"a\",\"b\"]}]}"), null);
    for ([_][]const u8{
        "[]",
        "{\"unknown\":[]}",
        "{\"entities\":{\"place\":[\"Paris\"]}}",
        "{\"entities\":{\"person\":[{\"name\":\"Ada\"}]}}",
        "{\"entities\":{\"person\":[{\"text\":\"Ada\",\"confidence\":1.5}]}}",
        "{\"entities\":{\"person\":[{\"text\":\"Ada\",\"start\":2}]}}",
        "{\"s\":[\"a\",\"b\"]}",
        "{\"s\":{\"value\":\"a\"}}",
    }) |raw| try std.testing.expectError(error.InvalidExtractionResponse, decodeResult(a, try parseLeaky(a, raw), plan, "Ada", null));
    for ([_][]const u8{
        "{}",
        "{\"choices\":[]}",
        "{\"choices\":[{\"message\":{\"content\":null}}]}",
        "{\"choices\":[{\"message\":{\"content\":\"not json\"}}]}",
    }) |raw| try std.testing.expectError(error.InvalidExtractionResponse, decode(a, raw, plan, "Ada", null));
}

test "pioneer content accepts text parts only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("a\nb", try contentText(a, try parseLeaky(a, "[{\"type\":\"text\",\"text\":\"a\"},{\"type\":\"text\",\"text\":\"b\"}]")));
    try std.testing.expectError(error.UnsupportedPioneerInput, contentText(a, try parseLeaky(a, "[{\"type\":\"image_url\",\"image_url\":{\"url\":\"x\"}}]")));
}
