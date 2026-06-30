// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");
const public_query_string_mod = @import("public_query_string.zig");
const search_filter_mod = @import("../search/query.zig");

pub const TextOperator = enum {
    all_terms,
    any_terms,
    phrase,
    prefix_any_term,
};

pub const PublicTextSpec = struct {
    text: []u8,
    operator: TextOperator,

    pub fn deinit(self: *PublicTextSpec, alloc: std.mem.Allocator) void {
        alloc.free(self.text);
        self.* = undefined;
    }
};

pub fn parseTextSpecAlloc(alloc: std.mem.Allocator, value: std.json.Value) anyerror!PublicTextSpec {
    if (value != .object) return error.UnsupportedQueryRequest;
    const obj = value.object;

    if (obj.get("query")) |query_value| {
        const query_text = jsonString(query_value) orelse return error.InvalidQueryRequest;
        return try parseQueryStringSpecAlloc(alloc, query_text);
    }
    if (obj.get("match")) |match_value| {
        return try parseMatchLikeSpecAlloc(alloc, obj, match_value, .all_terms);
    }
    if (obj.get("term")) |term_value| {
        return try parseMatchLikeSpecAlloc(alloc, obj, term_value, .all_terms);
    }
    if (obj.get("match_phrase")) |phrase_value| {
        return try parseMatchLikeSpecAlloc(alloc, obj, phrase_value, .phrase);
    }
    if (obj.get("prefix")) |prefix_value| {
        return try parseMatchLikeSpecAlloc(alloc, obj, prefix_value, .prefix_any_term);
    }
    return error.UnsupportedQueryRequest;
}

pub fn parseStatefulDirectTextQueryAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    boost: f32,
) anyerror!?db_types.TextQuery {
    if (try parseStatefulDirectTextOperatorQueryAlloc(alloc, value, boost)) |query| return query;
    return try parseStatefulDirectTextRangeQueryAlloc(alloc, value, boost);
}

pub fn parseStatefulDirectTextOperatorQueryAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    boost: f32,
) anyerror!?db_types.TextQuery {
    if (value != .object) return null;
    if (try parseStatefulDirectTextMultiMatchQueryAlloc(alloc, value, boost)) |query| return query;
    if (try directStringOperatorValue(value, "term", &.{ "term", "value" }, true)) |parsed| {
        return .{ .term = .{
            .field = try alloc.dupe(u8, parsed.field),
            .term = try alloc.dupe(u8, parsed.value),
            .boost = boost,
        } };
    }
    if (try directStringOperatorValue(value, "match", &.{ "text", "match", "value" }, true)) |parsed| {
        return .{ .match = .{
            .field = try alloc.dupe(u8, parsed.field),
            .text = try alloc.dupe(u8, parsed.value),
            .boost = boost,
        } };
    }
    if (try directStringOperatorValue(value, "match_phrase", &.{ "text", "match_phrase", "value" }, true)) |parsed| {
        return .{ .match_phrase = .{
            .field = try alloc.dupe(u8, parsed.field),
            .text = try alloc.dupe(u8, parsed.value),
            .boost = boost,
        } };
    }
    if (try directStringOperatorValue(value, "prefix", &.{ "prefix", "text", "value" }, true)) |parsed| {
        return .{ .prefix = .{
            .field = try alloc.dupe(u8, parsed.field),
            .prefix = try alloc.dupe(u8, parsed.value),
            .boost = boost,
        } };
    }
    if (try directStringOperatorValue(value, "wildcard", &.{ "pattern", "wildcard", "value" }, false)) |parsed| {
        return .{ .wildcard = .{
            .field = try alloc.dupe(u8, parsed.field),
            .pattern = try alloc.dupe(u8, parsed.value),
            .boost = boost,
        } };
    }
    if (try directStringOperatorValue(value, "regexp", &.{ "pattern", "regexp", "value" }, false)) |parsed| {
        return .{ .regexp = .{
            .field = try alloc.dupe(u8, parsed.field),
            .pattern = try alloc.dupe(u8, parsed.value),
            .boost = boost,
        } };
    }
    if (try directFuzzyOperatorValue(value)) |parsed| {
        return .{ .fuzzy = .{
            .field = try alloc.dupe(u8, parsed.field),
            .term = try alloc.dupe(u8, parsed.value),
            .max_edits = parsed.max_edits,
            .prefix_len = parsed.prefix_len,
            .auto_fuzzy = parsed.auto_fuzzy,
            .boost = boost,
        } };
    }
    return null;
}

pub fn parseStatefulDirectTextRangeQueryAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    boost: f32,
) anyerror!?db_types.TextQuery {
    return try directRangeQueryAlloc(alloc, value, boost);
}

fn parseStatefulDirectTextMultiMatchQueryAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    boost: f32,
) anyerror!?db_types.TextQuery {
    if (value != .object) return null;
    const multi_match = value.object.get("multi_match") orelse return null;
    if (multi_match != .object) return error.UnsupportedQueryRequest;

    const query_value = multi_match.object.get("query") orelse multi_match.object.get("text") orelse multi_match.object.get("match") orelse multi_match.object.get("value") orelse return error.UnsupportedQueryRequest;
    const query_text = try directNonBlankString(query_value);

    const type_value = multi_match.object.get("type") orelse return error.UnsupportedQueryRequest;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "bool_prefix")) return error.UnsupportedQueryRequest;

    const fields_value = multi_match.object.get("fields") orelse return error.UnsupportedQueryRequest;
    if (fields_value != .array or fields_value.array.items.len == 0) return error.UnsupportedQueryRequest;
    const query_boost: f32 = if (multi_match.object.get("boost")) |boost_value|
        @floatCast(try directNumber(boost_value))
    else
        boost;

    var field_specs = try alloc.alloc([]const u8, fields_value.array.items.len);
    defer alloc.free(field_specs);
    for (fields_value.array.items, 0..) |field_value, i| {
        if (field_value != .string) return error.UnsupportedQueryRequest;
        field_specs[i] = field_value.string;
    }

    return try parseMultiMatchBoolPrefixQueryAlloc(alloc, query_text, type_value.string, field_specs, query_boost);
}

pub fn parseMultiMatchBoolPrefixQueryAlloc(
    alloc: std.mem.Allocator,
    query_text: []const u8,
    query_type: []const u8,
    field_specs: []const []const u8,
    boost: f32,
) !db_types.TextQuery {
    if (std.mem.trim(u8, query_text, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    if (!std.mem.eql(u8, query_type, "bool_prefix")) return error.UnsupportedQueryRequest;
    if (field_specs.len == 0) return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(boost) or boost <= 0) return error.UnsupportedQueryRequest;

    var fields = try alloc.alloc(db_types.TextMultiMatchField, field_specs.len);
    errdefer alloc.free(fields);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field.field);
    }
    for (field_specs, 0..) |field_spec, i| {
        const parsed = try parseMultiMatchFieldSpec(field_spec);
        fields[i] = .{
            .field = try alloc.dupe(u8, parsed.field),
            .boost = parsed.boost,
        };
        initialized += 1;
    }

    return .{ .multi_match_bool_prefix = .{
        .query = try alloc.dupe(u8, query_text),
        .fields = fields,
        .boost = boost,
    } };
}

const ParsedMultiMatchField = struct {
    field: []const u8,
    boost: f32,
};

fn parseMultiMatchFieldSpec(field_spec: []const u8) !ParsedMultiMatchField {
    if (field_spec.len == 0) return error.UnsupportedQueryRequest;
    const boost_sep = std.mem.lastIndexOfScalar(u8, field_spec, '^') orelse return .{ .field = field_spec, .boost = 1.0 };
    if (boost_sep == 0 or boost_sep + 1 >= field_spec.len) return error.UnsupportedQueryRequest;
    const field = field_spec[0..boost_sep];
    const boost = std.fmt.parseFloat(f32, field_spec[boost_sep + 1 ..]) catch return error.UnsupportedQueryRequest;
    if (!std.math.isFinite(boost) or boost <= 0) return error.UnsupportedQueryRequest;
    return .{ .field = field, .boost = boost };
}

fn parseMatchLikeSpecAlloc(
    alloc: std.mem.Allocator,
    parent_obj: std.json.ObjectMap,
    value: std.json.Value,
    operator: TextOperator,
) anyerror!PublicTextSpec {
    var field_name: ?[]const u8 = jsonOptionalString(parent_obj.get("field"));
    var text_value: ?[]const u8 = null;

    switch (value) {
        .string => text_value = value.string,
        .object => |obj| {
            field_name = jsonOptionalString(obj.get("field")) orelse field_name;
            text_value = jsonOptionalString(obj.get("text")) orelse jsonOptionalString(obj.get("match")) orelse jsonOptionalString(obj.get("term")) orelse jsonOptionalString(obj.get("prefix"));
        },
        else => return error.InvalidQueryRequest,
    }

    try requireSupportedTextField(field_name);
    const text = text_value orelse return error.InvalidQueryRequest;
    if (std.mem.trim(u8, text, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    return .{
        .text = try alloc.dupe(u8, text),
        .operator = operator,
    };
}

fn parseQueryStringSpecAlloc(alloc: std.mem.Allocator, query_text: []const u8) anyerror!PublicTextSpec {
    var owned = try public_query_string_mod.parseFilterAlloc(alloc, query_text);
    defer owned.deinit(alloc);
    return try textSpecFromFilterAlloc(alloc, owned.filter);
}

fn textSpecFromFilterAlloc(alloc: std.mem.Allocator, filter: search_filter_mod.Filter) anyerror!PublicTextSpec {
    return switch (filter) {
        .term => |term| blk: {
            try requireSupportedTextField(term.field);
            break :blk .{
                .text = try alloc.dupe(u8, term.term),
                .operator = .all_terms,
            };
        },
        .phrase => |phrase| blk: {
            try requireSupportedTextField(phrase.field);
            break :blk .{
                .text = try public_query_string_mod.joinTermsWithSpacesAlloc(alloc, phrase.terms),
                .operator = .phrase,
            };
        },
        .prefix => |prefix| blk: {
            try requireSupportedTextField(prefix.field);
            break :blk .{
                .text = try alloc.dupe(u8, prefix.prefix),
                .operator = .prefix_any_term,
            };
        },
        .bool_filter => |bool_filter| try textSpecFromBoolFilterAlloc(alloc, bool_filter),
        else => error.UnsupportedQueryRequest,
    };
}

fn textSpecFromBoolFilterAlloc(alloc: std.mem.Allocator, bool_filter: search_filter_mod.BoolFilter) anyerror!PublicTextSpec {
    if (bool_filter.must_not.len > 0) return error.UnsupportedQueryRequest;
    if (bool_filter.must.len > 0 and bool_filter.should.len > 0) return error.UnsupportedQueryRequest;
    if (bool_filter.must.len > 0) return try combineTextFiltersAlloc(alloc, bool_filter.must, .all_terms);
    if (bool_filter.should.len > 0) return try combineTextFiltersAlloc(alloc, bool_filter.should, .any_terms);
    return error.UnsupportedQueryRequest;
}

fn combineTextFiltersAlloc(
    alloc: std.mem.Allocator,
    filters: []const search_filter_mod.Filter,
    operator: TextOperator,
) anyerror!PublicTextSpec {
    if (filters.len == 0) return error.UnsupportedQueryRequest;
    if (filters.len == 1) return try textSpecFromFilterAlloc(alloc, filters[0]);

    for (filters) |filter| {
        switch (filter) {
            .term => |term| try requireSupportedTextField(term.field),
            else => return error.UnsupportedQueryRequest,
        }
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (filters, 0..) |filter, idx| {
        if (idx > 0) try out.append(alloc, ' ');
        try out.appendSlice(alloc, filter.term.term);
    }
    return .{
        .text = try out.toOwnedSlice(alloc),
        .operator = operator,
    };
}

fn requireSupportedTextField(field_name: ?[]const u8) anyerror!void {
    if (field_name == null) return;
    const field = field_name.?;
    if (std.mem.eql(u8, field, "_all")) return;
    if (std.mem.eql(u8, field, "text")) return;
    if (std.mem.eql(u8, field, "body")) return;
    return error.UnsupportedQueryRequest;
}

const DirectStringOperatorValue = struct {
    field: []const u8,
    value: []const u8,
};

const DirectFuzzyOperatorValue = struct {
    field: []const u8,
    value: []const u8,
    max_edits: u8 = 1,
    prefix_len: u8 = 0,
    auto_fuzzy: bool = false,
};

fn directStringOperatorValue(
    query: std.json.Value,
    operator: []const u8,
    value_keys: []const []const u8,
    allow_single_field_object: bool,
) !?DirectStringOperatorValue {
    if (query != .object) return null;
    const operator_value = query.object.get(operator) orelse return null;
    if (operator_value == .string) {
        const field = directFieldValue(query.object) orelse return error.UnsupportedQueryRequest;
        if (field != .string) return error.UnsupportedQueryRequest;
        return .{ .field = field.string, .value = try directNonBlankString(operator_value) };
    }
    if (operator_value != .object) return error.UnsupportedQueryRequest;
    if (directFieldValue(operator_value.object)) |field| {
        if (field != .string) return error.UnsupportedQueryRequest;
        for (value_keys) |key| {
            if (operator_value.object.get(key)) |item| {
                return .{ .field = field.string, .value = try directNonBlankString(item) };
            }
        }
        return error.UnsupportedQueryRequest;
    }
    if (allow_single_field_object and operator_value.object.count() == 1) {
        var it = operator_value.object.iterator();
        const entry = it.next() orelse return error.UnsupportedQueryRequest;
        return .{ .field = entry.key_ptr.*, .value = try directNonBlankString(entry.value_ptr.*) };
    }
    return error.UnsupportedQueryRequest;
}

fn directFuzzyOperatorValue(query: std.json.Value) !?DirectFuzzyOperatorValue {
    if (query != .object) return null;
    const fuzzy = query.object.get("fuzzy") orelse return null;
    if (fuzzy == .string) {
        const field = directFieldValue(query.object) orelse return error.UnsupportedQueryRequest;
        if (field != .string) return error.UnsupportedQueryRequest;
        return .{ .field = field.string, .value = try directNonBlankString(fuzzy) };
    }
    if (fuzzy != .object) return error.UnsupportedQueryRequest;
    const field = directFieldValue(fuzzy.object) orelse return error.UnsupportedQueryRequest;
    const term = fuzzy.object.get("term") orelse fuzzy.object.get("query") orelse fuzzy.object.get("value") orelse return error.UnsupportedQueryRequest;
    if (field != .string) return error.UnsupportedQueryRequest;
    return .{
        .field = field.string,
        .value = try directNonBlankString(term),
        .max_edits = try directOptionalU8(fuzzy.object, "max_edits", 1),
        .prefix_len = try directOptionalU8(fuzzy.object, "prefix_length", 0),
        .auto_fuzzy = try directOptionalBool(fuzzy.object, "auto_fuzzy", false),
    };
}

fn directNonBlankString(value: std.json.Value) ![]const u8 {
    if (value != .string) return error.UnsupportedQueryRequest;
    if (std.mem.trim(u8, value.string, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    return value.string;
}

fn directRangeQueryAlloc(alloc: std.mem.Allocator, query: std.json.Value, boost: f32) !?db_types.TextQuery {
    if (query != .object) return null;
    const field = directFieldValue(query.object) orelse return null;
    if (field != .string) return error.UnsupportedQueryRequest;
    if (query.object.get("min") == null and query.object.get("max") == null) return null;

    const min = query.object.get("min");
    const max = query.object.get("max");
    const numeric_like = (min != null and (min.? == .integer or min.? == .float)) or
        (max != null and (max.? == .integer or max.? == .float));
    const string_like = (min != null and min.? == .string) or
        (max != null and max.? == .string);
    if (numeric_like and string_like) return error.UnsupportedQueryRequest;

    if (numeric_like) {
        return .{ .numeric_range = .{
            .field = try alloc.dupe(u8, field.string),
            .min = if (min) |item| try directNumber(item) else null,
            .max = if (max) |item| try directNumber(item) else null,
            .inclusive_min = try directOptionalBool(query.object, "inclusive_min", true),
            .inclusive_max = try directOptionalBool(query.object, "inclusive_max", false),
            .boost = boost,
        } };
    }
    if (string_like) {
        return .{ .term_range = .{
            .field = try alloc.dupe(u8, field.string),
            .min = if (min) |item| try directStringAlloc(alloc, item) else null,
            .max = if (max) |item| try directStringAlloc(alloc, item) else null,
            .inclusive_min = try directOptionalBool(query.object, "inclusive_min", true),
            .inclusive_max = try directOptionalBool(query.object, "inclusive_max", false),
            .boost = boost,
        } };
    }
    return error.UnsupportedQueryRequest;
}

fn directFieldValue(object: std.json.ObjectMap) ?std.json.Value {
    return object.get("field") orelse object.get("path");
}

fn directOptionalU8(object: std.json.ObjectMap, key: []const u8, default_value: u8) !u8 {
    const value = object.get(key) orelse return default_value;
    if (value != .integer or value.integer < 0 or value.integer > std.math.maxInt(u8)) return error.UnsupportedQueryRequest;
    return @intCast(value.integer);
}

fn directOptionalBool(object: std.json.ObjectMap, key: []const u8, default_value: bool) !bool {
    const value = object.get(key) orelse return default_value;
    if (value != .bool) return error.UnsupportedQueryRequest;
    return value.bool;
}

fn directNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        else => error.UnsupportedQueryRequest,
    };
}

fn directStringAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .string) return error.UnsupportedQueryRequest;
    return try alloc.dupe(u8, value.string);
}

fn jsonOptionalString(value: ?std.json.Value) ?[]const u8 {
    const inner = value orelse return null;
    return jsonString(inner);
}

fn jsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => value.string,
        else => null,
    };
}

test "public full text subset accepts supported query strings" {
    const alloc = std.testing.allocator;
    var spec = try parseTextSpecAlloc(alloc, .{
        .object = blk: {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, "query", .{ .string = "body:alpha OR body:bravo" });
            break :blk obj;
        },
    });
    defer spec.deinit(alloc);

    try std.testing.expectEqual(TextOperator.any_terms, spec.operator);
    try std.testing.expectEqualStrings("alpha bravo", spec.text);
}

test "public full text subset rejects unsupported fields" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedQueryRequest, parseTextSpecAlloc(alloc, .{
        .object = blk: {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, "query", .{ .string = "title:alpha" });
            break :blk obj;
        },
    }));
}

test "public direct text parser accepts query dsl fields" {
    const alloc = std.testing.allocator;
    const maybe_direct = try parseStatefulDirectTextQueryAlloc(alloc, .{
        .object = blk: {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, "field", .{ .string = "title" });
            try obj.put(alloc, "match", .{ .string = "hello" });
            break :blk obj;
        },
    }, 1.0);
    try std.testing.expect(maybe_direct != null);
    var query = maybe_direct.?;
    defer query.deinit(alloc);

    try std.testing.expect(query == .match);
    try std.testing.expectEqualStrings("title", query.match.field);
    try std.testing.expectEqualStrings("hello", query.match.text);
}

test "public direct text parser lowers multi_match bool_prefix" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"multi_match":{"query":"Quick Brown F","type":"bool_prefix","fields":["title"]}}
    , .{});
    defer parsed.deinit();

    const maybe_direct = try parseStatefulDirectTextQueryAlloc(alloc, parsed.value, 1.0);
    try std.testing.expect(maybe_direct != null);
    var query = maybe_direct.?;
    defer query.deinit(alloc);

    try std.testing.expect(query == .multi_match_bool_prefix);
    try std.testing.expectEqualStrings("Quick Brown F", query.multi_match_bool_prefix.query);
    try std.testing.expectEqual(@as(usize, 1), query.multi_match_bool_prefix.fields.len);
    try std.testing.expectEqualStrings("title", query.multi_match_bool_prefix.fields[0].field);
}

test "public direct text parser emits stateful match phrase" {
    const alloc = std.testing.allocator;
    const maybe_direct = try parseStatefulDirectTextQueryAlloc(alloc, .{
        .object = blk: {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, "match_phrase", .{
                .object = blk2: {
                    var inner = std.json.ObjectMap.empty;
                    try inner.put(alloc, "field", .{ .string = "body" });
                    try inner.put(alloc, "text", .{ .string = "hello world" });
                    break :blk2 inner;
                },
            });
            break :blk obj;
        },
    }, 1.0);
    try std.testing.expect(maybe_direct != null);
    var query = maybe_direct.?;
    defer query.deinit(alloc);

    try std.testing.expect(query == .match_phrase);
    try std.testing.expectEqualStrings("body", query.match_phrase.field);
    try std.testing.expectEqualStrings("hello world", query.match_phrase.text);
}

test "public direct text parser validates string operator operands" {
    const alloc = std.testing.allocator;
    const cases = [_]struct {
        operator: []const u8,
        value_key: []const u8,
    }{
        .{ .operator = "term", .value_key = "term" },
        .{ .operator = "match", .value_key = "text" },
        .{ .operator = "match_phrase", .value_key = "text" },
        .{ .operator = "prefix", .value_key = "text" },
        .{ .operator = "wildcard", .value_key = "pattern" },
        .{ .operator = "regexp", .value_key = "pattern" },
        .{ .operator = "fuzzy", .value_key = "term" },
    };

    for (cases) |case| {
        {
            var value = try directOperatorTestValue(alloc, case.operator, case.value_key, .{ .string = "hello" });
            defer directOperatorTestValueDeinit(alloc, &value);
            const maybe_direct = try parseStatefulDirectTextQueryAlloc(
                alloc,
                value,
                1.0,
            );
            try std.testing.expect(maybe_direct != null);
            var query = maybe_direct.?;
            defer query.deinit(alloc);
        }
        {
            var value = try directOperatorTestValue(alloc, case.operator, case.value_key, .{ .string = "" });
            defer directOperatorTestValueDeinit(alloc, &value);
            try std.testing.expectError(error.InvalidQueryRequest, parseStatefulDirectTextQueryAlloc(alloc, value, 1.0));
        }
        {
            var value = try directOperatorTestValue(alloc, case.operator, case.value_key, .{ .string = "   " });
            defer directOperatorTestValueDeinit(alloc, &value);
            try std.testing.expectError(error.InvalidQueryRequest, parseStatefulDirectTextQueryAlloc(alloc, value, 1.0));
        }
        {
            var value = try directOperatorTestValue(alloc, case.operator, case.value_key, .{ .integer = 42 });
            defer directOperatorTestValueDeinit(alloc, &value);
            try std.testing.expectError(error.UnsupportedQueryRequest, parseStatefulDirectTextQueryAlloc(alloc, value, 1.0));
        }
    }
}

fn directOperatorTestValue(
    alloc: std.mem.Allocator,
    operator: []const u8,
    value_key: []const u8,
    operand: std.json.Value,
) !std.json.Value {
    var inner = std.json.ObjectMap.empty;
    try inner.put(alloc, "field", .{ .string = "body" });
    try inner.put(alloc, value_key, operand);

    var outer = std.json.ObjectMap.empty;
    try outer.put(alloc, operator, .{ .object = inner });
    return .{ .object = outer };
}

fn directOperatorTestValueDeinit(alloc: std.mem.Allocator, value: *std.json.Value) void {
    if (value.* != .object) return;
    var outer_it = value.object.iterator();
    while (outer_it.next()) |entry| {
        if (entry.value_ptr.* == .object) entry.value_ptr.object.deinit(alloc);
    }
    value.object.deinit(alloc);
    value.* = undefined;
}
