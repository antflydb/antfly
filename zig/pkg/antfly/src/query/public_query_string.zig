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
const search_filter_mod = @import("../search/query.zig");
const query_string_mod = @import("../search/query_string.zig");

pub const OwnedFilter = struct {
    filter: search_filter_mod.Filter,

    pub fn deinit(self: *OwnedFilter, alloc: std.mem.Allocator) void {
        freeFilter(alloc, self.filter);
        self.* = undefined;
    }
};

const FilterCloneError = std.mem.Allocator.Error || error{UnsupportedQueryRequest};

pub fn parseFilterAlloc(alloc: std.mem.Allocator, input: []const u8) !OwnedFilter {
    return parseFilterAllocWithOptions(alloc, input, .{});
}

pub const ParseOptions = struct {
    default_operator: query_string_mod.QueryStringParser.DefaultOperator = .and_,
};

pub fn parseFilterAllocWithOptions(alloc: std.mem.Allocator, input: []const u8, options: ParseOptions) !OwnedFilter {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parser = query_string_mod.QueryStringParser{
        .default_operator = options.default_operator,
    };
    const filter = parser.parse(arena, input) catch |err| switch (err) {
        error.InvalidSyntax => return error.InvalidQueryRequest,
        error.OutOfMemory => return error.OutOfMemory,
    };

    return .{
        .filter = try cloneFilterAlloc(alloc, filter),
    };
}

pub fn filterToStatefulTextQueryAlloc(
    alloc: std.mem.Allocator,
    filter: search_filter_mod.Filter,
    root_boost: f32,
) anyerror!db_types.TextQuery {
    return switch (filter) {
        .match_all => .{ .match_all = {} },
        .match_none => .{ .match_none = {} },
        .term => |term| .{ .match = .{
            .field = try alloc.dupe(u8, term.field),
            .text = try alloc.dupe(u8, term.term),
            .boost = root_boost * term.boost,
        } },
        .prefix => |prefix| .{ .prefix = .{
            .field = try alloc.dupe(u8, prefix.field),
            .prefix = try alloc.dupe(u8, prefix.prefix),
            .boost = root_boost * prefix.boost,
        } },
        .regexp => |regexp| .{ .regexp = .{
            .field = try alloc.dupe(u8, regexp.field),
            .pattern = try alloc.dupe(u8, regexp.pattern),
            .boost = root_boost * regexp.boost,
        } },
        .wildcard => |wildcard| .{ .wildcard = .{
            .field = try alloc.dupe(u8, wildcard.field),
            .pattern = try alloc.dupe(u8, wildcard.pattern),
            .boost = root_boost * wildcard.boost,
        } },
        .fuzzy => |fuzzy| .{ .fuzzy = .{
            .field = try alloc.dupe(u8, fuzzy.field),
            .term = try alloc.dupe(u8, fuzzy.term),
            .max_edits = fuzzy.max_edits,
            .prefix_len = fuzzy.prefix_len,
            .auto_fuzzy = false,
            .boost = root_boost * fuzzy.boost,
        } },
        .phrase => |phrase| .{ .match_phrase = .{
            .field = try alloc.dupe(u8, phrase.field),
            .text = try joinTermsWithSpacesAlloc(alloc, phrase.terms),
            .analyzer = null,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = root_boost * phrase.boost,
        } },
        .range => |range| .{ .numeric_range = .{
            .field = try alloc.dupe(u8, range.field),
            .min = range.min_val,
            .max = range.max_val,
            .inclusive_min = range.inclusive_min,
            .inclusive_max = range.inclusive_max,
            .boost = root_boost * range.boost,
        } },
        .date_range => |range| .{ .date_range = .{
            .field = try alloc.dupe(u8, range.field),
            .start_ns = range.start_ns,
            .end_ns = range.end_ns,
            .inclusive_start = range.inclusive_start,
            .inclusive_end = range.inclusive_end,
            .boost = root_boost * range.boost,
        } },
        .term_range => |range| .{ .term_range = .{
            .field = try alloc.dupe(u8, range.field),
            .min = if (range.min) |min| try alloc.dupe(u8, min) else null,
            .max = if (range.max) |max| try alloc.dupe(u8, max) else null,
            .inclusive_min = range.inclusive_min,
            .inclusive_max = range.inclusive_max,
            .boost = root_boost * range.boost,
        } },
        .bool_filter => |bool_filter| .{ .bool_query = .{
            .must = try filterSliceToStatefulTextQueryAlloc(alloc, bool_filter.must, 1.0),
            .should = try filterSliceToStatefulTextQueryAlloc(alloc, bool_filter.should, 1.0),
            .must_not = try filterSliceToStatefulTextQueryAlloc(alloc, bool_filter.must_not, 1.0),
            .min_should = bool_filter.min_should_match,
            .boost = root_boost * bool_filter.boost,
        } },
        else => error.UnsupportedQueryRequest,
    };
}

pub fn joinTermsWithSpacesAlloc(alloc: std.mem.Allocator, items: []const []const u8) ![]u8 {
    if (items.len == 0) return try alloc.dupe(u8, "");
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ' ');
        try out.appendSlice(alloc, item);
    }
    return try out.toOwnedSlice(alloc);
}

fn filterSliceToStatefulTextQueryAlloc(
    alloc: std.mem.Allocator,
    items: []const search_filter_mod.Filter,
    boost: f32,
) anyerror![]const db_types.TextQuery {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(db_types.TextQuery, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| freeTextQuery(alloc, item);
        alloc.free(out);
    }
    for (items, 0..) |item, idx| {
        out[idx] = try filterToStatefulTextQueryAlloc(alloc, item, boost);
        initialized += 1;
    }
    return out;
}

fn cloneFilterAlloc(alloc: std.mem.Allocator, filter: search_filter_mod.Filter) FilterCloneError!search_filter_mod.Filter {
    return switch (filter) {
        .match_all => .{ .match_all = {} },
        .match_none => .{ .match_none = {} },
        .term => |term| .{ .term = .{
            .field = try alloc.dupe(u8, term.field),
            .term = try alloc.dupe(u8, term.term),
            .boost = term.boost,
        } },
        .prefix => |prefix| .{ .prefix = .{
            .field = try alloc.dupe(u8, prefix.field),
            .prefix = try alloc.dupe(u8, prefix.prefix),
            .boost = prefix.boost,
        } },
        .phrase => |phrase| .{ .phrase = .{
            .field = try alloc.dupe(u8, phrase.field),
            .terms = try cloneStringSliceAlloc(alloc, phrase.terms),
            .slop = phrase.slop,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost,
        } },
        .fuzzy => |fuzzy| .{ .fuzzy = .{
            .field = try alloc.dupe(u8, fuzzy.field),
            .term = try alloc.dupe(u8, fuzzy.term),
            .max_edits = fuzzy.max_edits,
            .prefix_len = fuzzy.prefix_len,
            .boost = fuzzy.boost,
        } },
        .regexp => |regexp| .{ .regexp = .{
            .field = try alloc.dupe(u8, regexp.field),
            .pattern = try alloc.dupe(u8, regexp.pattern),
            .boost = regexp.boost,
        } },
        .wildcard => |wildcard| .{ .wildcard = .{
            .field = try alloc.dupe(u8, wildcard.field),
            .pattern = try alloc.dupe(u8, wildcard.pattern),
            .boost = wildcard.boost,
        } },
        .range => |range| .{ .range = .{
            .field = try alloc.dupe(u8, range.field),
            .min_val = range.min_val,
            .max_val = range.max_val,
            .inclusive_min = range.inclusive_min,
            .inclusive_max = range.inclusive_max,
            .boost = range.boost,
        } },
        .date_range => |range| .{ .date_range = .{
            .field = try alloc.dupe(u8, range.field),
            .start_ns = range.start_ns,
            .end_ns = range.end_ns,
            .inclusive_start = range.inclusive_start,
            .inclusive_end = range.inclusive_end,
            .boost = range.boost,
        } },
        .term_range => |range| .{ .term_range = .{
            .field = try alloc.dupe(u8, range.field),
            .min = if (range.min) |min| try alloc.dupe(u8, min) else null,
            .max = if (range.max) |max| try alloc.dupe(u8, max) else null,
            .inclusive_min = range.inclusive_min,
            .inclusive_max = range.inclusive_max,
            .boost = range.boost,
        } },
        .bool_filter => |bool_filter| .{ .bool_filter = .{
            .must = try cloneFilterSliceAlloc(alloc, bool_filter.must),
            .should = try cloneFilterSliceAlloc(alloc, bool_filter.should),
            .must_not = try cloneFilterSliceAlloc(alloc, bool_filter.must_not),
            .min_should_match = bool_filter.min_should_match,
            .boost = bool_filter.boost,
        } },
        else => return error.UnsupportedQueryRequest,
    };
}

fn cloneFilterSliceAlloc(alloc: std.mem.Allocator, items: []const search_filter_mod.Filter) FilterCloneError![]search_filter_mod.Filter {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(search_filter_mod.Filter, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| freeFilter(alloc, item);
        alloc.free(out);
    }
    for (items, 0..) |item, idx| {
        out[idx] = try cloneFilterAlloc(alloc, item);
        initialized += 1;
    }
    return out;
}

fn cloneStringSliceAlloc(alloc: std.mem.Allocator, items: []const []const u8) std.mem.Allocator.Error![][]u8 {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc([]u8, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (items, 0..) |item, idx| {
        out[idx] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn freeFilter(alloc: std.mem.Allocator, filter: search_filter_mod.Filter) void {
    switch (filter) {
        .term => |term| {
            alloc.free(term.field);
            alloc.free(term.term);
        },
        .prefix => |prefix| {
            alloc.free(prefix.field);
            alloc.free(prefix.prefix);
        },
        .phrase => |phrase| {
            alloc.free(phrase.field);
            for (phrase.terms) |term| alloc.free(term);
            if (phrase.terms.len > 0) alloc.free(phrase.terms);
        },
        .fuzzy => |fuzzy| {
            alloc.free(fuzzy.field);
            alloc.free(fuzzy.term);
        },
        .regexp => |regexp| {
            alloc.free(regexp.field);
            alloc.free(regexp.pattern);
        },
        .wildcard => |wildcard| {
            alloc.free(wildcard.field);
            alloc.free(wildcard.pattern);
        },
        .bool_filter => |bool_filter| {
            freeFilterSlice(alloc, bool_filter.must);
            freeFilterSlice(alloc, bool_filter.should);
            freeFilterSlice(alloc, bool_filter.must_not);
        },
        else => {},
    }
}

fn freeFilterSlice(alloc: std.mem.Allocator, items: []const search_filter_mod.Filter) void {
    for (items) |item| freeFilter(alloc, item);
    if (items.len > 0) alloc.free(items);
}

fn freeTextQuery(alloc: std.mem.Allocator, query: db_types.TextQuery) void {
    switch (query) {
        .match => |match| {
            alloc.free(match.field);
            alloc.free(match.text);
            if (match.analyzer) |analyzer| alloc.free(analyzer);
        },
        .match_phrase => |phrase| {
            alloc.free(phrase.field);
            alloc.free(phrase.text);
            if (phrase.analyzer) |analyzer| alloc.free(analyzer);
        },
        .prefix => |prefix| {
            alloc.free(prefix.field);
            alloc.free(prefix.prefix);
        },
        .wildcard => |wildcard| {
            alloc.free(wildcard.field);
            alloc.free(wildcard.pattern);
        },
        .regexp => |regexp| {
            alloc.free(regexp.field);
            alloc.free(regexp.pattern);
        },
        .fuzzy => |fuzzy| {
            alloc.free(fuzzy.field);
            alloc.free(fuzzy.term);
        },
        .bool_query => |bool_query| {
            freeTextQuerySlice(alloc, bool_query.must);
            freeTextQuerySlice(alloc, bool_query.should);
            freeTextQuerySlice(alloc, bool_query.must_not);
        },
        else => {},
    }
}

fn freeTextQuerySlice(alloc: std.mem.Allocator, items: []const db_types.TextQuery) void {
    for (items) |item| freeTextQuery(alloc, item);
    if (items.len > 0) alloc.free(items);
}

test "public query string parser returns owned bool filter" {
    const alloc = std.testing.allocator;
    var owned = try parseFilterAlloc(alloc, "body:alpha OR body:bravo");
    defer owned.deinit(alloc);

    try std.testing.expect(owned.filter == .bool_filter);
    try std.testing.expectEqual(@as(usize, 2), owned.filter.bool_filter.should.len);
    try std.testing.expect(owned.filter.bool_filter.should[0] == .term);
    try std.testing.expectEqualStrings("body", owned.filter.bool_filter.should[0].term.field);
}

test "public query string parser supports default operator override" {
    const alloc = std.testing.allocator;
    var owned = try parseFilterAllocWithOptions(alloc, "alpha bravo", .{ .default_operator = .or_ });
    defer owned.deinit(alloc);

    try std.testing.expect(owned.filter == .bool_filter);
    try std.testing.expectEqual(@as(usize, 2), owned.filter.bool_filter.should.len);
}
