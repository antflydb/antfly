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
const regex_mod = @import("regex.zig");
const levenshtein_mod = @import("levenshtein.zig");

const Allocator = std.mem.Allocator;

pub fn storedDocMatchesPatternFilter(alloc: Allocator, key: []const u8, stored: []const u8, filter_query_json: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    var filter_query = try std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{});
    defer filter_query.deinit();
    return try jsonDocMatchesPatternFilter(alloc, key, parsed.value, filter_query.value);
}

pub fn jsonDocMatchesPatternFilter(alloc: Allocator, key: []const u8, doc: std.json.Value, filter_query: std.json.Value) !bool {
    if (filter_query != .object) return error.InvalidArgument;

    if (filter_query.object.get("match_all") != null) return true;
    if (filter_query.object.get("match_none") != null) return false;
    if (filter_query.object.get("doc_id")) |doc_id| return docIdMatchesPatternKey(key, doc_id);

    if (filter_query.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return error.InvalidArgument;
        for (conjuncts.array.items) |item| {
            if (!(try jsonDocMatchesPatternFilter(alloc, key, doc, item))) return false;
        }
        return true;
    }

    if (filter_query.object.get("disjuncts")) |disjuncts| {
        if (disjuncts != .array) return error.InvalidArgument;
        for (disjuncts.array.items) |item| {
            if (try jsonDocMatchesPatternFilter(alloc, key, doc, item)) return true;
        }
        return false;
    }

    if (filter_query.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidArgument;

        if (bool_query.object.get("must")) |must| {
            if (must != .array or must.array.items.len == 0) return error.InvalidArgument;
            for (must.array.items) |item| {
                if (!(try jsonDocMatchesPatternFilter(alloc, key, doc, item))) return false;
            }
        }
        if (bool_query.object.get("filter")) |filter| {
            if (filter != .array or filter.array.items.len == 0) return error.InvalidArgument;
            for (filter.array.items) |item| {
                if (!(try jsonDocMatchesPatternFilter(alloc, key, doc, item))) return false;
            }
        }

        var saw_should = false;
        if (bool_query.object.get("should")) |should| {
            if (should != .array or should.array.items.len == 0) return error.InvalidArgument;
            saw_should = true;
            var matched = false;
            for (should.array.items) |item| {
                if (try jsonDocMatchesPatternFilter(alloc, key, doc, item)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) return false;
        }

        if (bool_query.object.get("must_not")) |must_not| {
            if (must_not != .array or must_not.array.items.len == 0) return error.InvalidArgument;
            for (must_not.array.items) |item| {
                if (try jsonDocMatchesPatternFilter(alloc, key, doc, item)) return false;
            }
        }

        if (bool_query.object.count() == 0) return error.InvalidArgument;
        if (!saw_should and bool_query.object.get("must") == null and bool_query.object.get("filter") == null and bool_query.object.get("must_not") == null) {
            return error.InvalidArgument;
        }
        return true;
    }

    var path = std.ArrayListUnmanaged([]const u8).empty;
    defer path.deinit(alloc);
    const field = blk: {
        if (filter_query.object.get("term")) |term| {
            if (term != .object or term.object.count() != 1) return error.InvalidArgument;
            var it = term.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("terms")) |terms| {
            if (terms != .object or terms.object.count() != 1) return error.InvalidArgument;
            var it = terms.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("match")) |match| {
            if (match != .object or match.object.count() != 1) return error.InvalidArgument;
            var it = match.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("prefix")) |prefix| {
            if (prefix != .object or prefix.object.count() != 1) return error.InvalidArgument;
            var it = prefix.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("wildcard")) |wildcard| {
            if (wildcard != .object or wildcard.object.count() != 1) return error.InvalidArgument;
            var it = wildcard.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("regexp")) |regexp| {
            if (regexp != .object or regexp.object.count() != 1) return error.InvalidArgument;
            var it = regexp.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("fuzzy")) |fuzzy| {
            if (fuzzy != .object or fuzzy.object.count() != 1) return error.InvalidArgument;
            var it = fuzzy.object.iterator();
            const entry = it.next() orelse return error.InvalidArgument;
            break :blk entry.key_ptr.*;
        }
        if (filter_query.object.get("numeric_range")) |range_query| {
            if (range_query != .object) return error.InvalidArgument;
            const field_name = range_query.object.get("field") orelse return error.InvalidArgument;
            if (field_name != .string) return error.InvalidArgument;
            break :blk field_name.string;
        }
        if (filter_query.object.get("date_range")) |range_query| {
            if (range_query != .object) return error.InvalidArgument;
            const field_name = range_query.object.get("field") orelse return error.InvalidArgument;
            if (field_name != .string) return error.InvalidArgument;
            break :blk field_name.string;
        }
        return error.InvalidArgument;
    };

    var parts = std.mem.splitScalar(u8, field, '.');
    while (parts.next()) |part| try path.append(alloc, part);
    if (path.items.len == 0) return false;

    var values = std.ArrayListUnmanaged(std.json.Value).empty;
    defer values.deinit(alloc);
    try collectJsonValuesAtPath(alloc, doc, path.items, 0, &values);

    if (filter_query.object.get("term")) |term| {
        var it = term.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.value_ptr.* != .string) return error.InvalidArgument;
        return jsonValuesContainTerm(values.items, entry.value_ptr.string);
    }
    if (filter_query.object.get("terms")) |terms| {
        var it = terms.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.value_ptr.* != .array or entry.value_ptr.array.items.len == 0) return error.InvalidArgument;
        for (entry.value_ptr.array.items) |item| {
            if (item != .string) return error.InvalidArgument;
            if (jsonValuesContainTerm(values.items, item.string)) return true;
        }
        return false;
    }
    if (filter_query.object.get("match")) |match| {
        var it = match.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.value_ptr.* != .string) return error.InvalidArgument;
        return jsonValuesContainMatch(values.items, entry.value_ptr.string);
    }
    if (filter_query.object.get("prefix")) |prefix| {
        var it = prefix.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.value_ptr.* != .string) return error.InvalidArgument;
        return jsonValuesContainPrefix(values.items, entry.value_ptr.string);
    }
    if (filter_query.object.get("wildcard")) |wildcard| {
        var it = wildcard.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.value_ptr.* != .string) return error.InvalidArgument;
        return jsonValuesContainWildcard(values.items, entry.value_ptr.string);
    }
    if (filter_query.object.get("regexp")) |regexp| {
        var it = regexp.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        if (entry.value_ptr.* != .string) return error.InvalidArgument;
        return try jsonValuesContainRegexp(alloc, values.items, entry.value_ptr.string);
    }
    if (filter_query.object.get("fuzzy")) |fuzzy| {
        var it = fuzzy.object.iterator();
        const entry = it.next() orelse return error.InvalidArgument;
        return try jsonValuesContainFuzzy(alloc, values.items, entry.value_ptr.*);
    }
    if (filter_query.object.get("numeric_range")) |range_query| return try jsonValuesContainNumericRange(values.items, range_query);
    if (filter_query.object.get("date_range")) |range_query| return try jsonValuesContainDateRange(values.items, range_query);
    return error.InvalidArgument;
}

fn docIdMatchesPatternKey(key: []const u8, doc_id: std.json.Value) !bool {
    if (doc_id != .object) return error.InvalidArgument;
    const ids = doc_id.object.get("ids") orelse return error.InvalidArgument;
    if (ids != .array or ids.array.items.len == 0) return error.InvalidArgument;
    for (ids.array.items) |item| {
        if (item != .string) return error.InvalidArgument;
        if (std.mem.eql(u8, key, item.string)) return true;
    }
    return false;
}

fn collectJsonValuesAtPath(alloc: Allocator, value: std.json.Value, path: []const []const u8, depth: usize, out: *std.ArrayListUnmanaged(std.json.Value)) !void {
    if (depth >= path.len) {
        try out.append(alloc, value);
        return;
    }
    const segment = path[depth];
    switch (value) {
        .object => |object| {
            const next = object.get(segment) orelse return;
            try collectJsonValuesAtPath(alloc, next, path, depth + 1, out);
        },
        .array => |array| {
            if (std.fmt.parseInt(usize, segment, 10)) |index| {
                if (index >= array.items.len) return;
                try collectJsonValuesAtPath(alloc, array.items[index], path, depth + 1, out);
            } else |_| {
                for (array.items) |item| try collectJsonValuesAtPath(alloc, item, path, depth, out);
            }
        },
        else => {},
    }
}

fn jsonValuesContainTerm(values: []const std.json.Value, term: []const u8) bool {
    for (values) |value| switch (value) {
        .string => |text| if (std.mem.eql(u8, text, term)) return true,
        .integer => |number| {
            var buf: [32]u8 = undefined;
            const rendered = std.fmt.bufPrint(&buf, "{}", .{number}) catch continue;
            if (std.mem.eql(u8, rendered, term)) return true;
        },
        .float => |number| {
            var buf: [64]u8 = undefined;
            const rendered = std.fmt.bufPrint(&buf, "{d}", .{number}) catch continue;
            if (std.mem.eql(u8, rendered, term)) return true;
        },
        .bool => |boolean| {
            if ((boolean and std.mem.eql(u8, term, "true")) or (!boolean and std.mem.eql(u8, term, "false"))) return true;
        },
        else => {},
    };
    return false;
}

fn jsonValuesContainMatch(values: []const std.json.Value, text: []const u8) bool {
    for (values) |value| switch (value) {
        .string => |candidate| if (containsCaseInsensitive(candidate, text)) return true,
        else => {},
    };
    return false;
}

fn jsonValuesContainPrefix(values: []const std.json.Value, prefix: []const u8) bool {
    for (values) |value| switch (value) {
        .string => |candidate| if (std.mem.startsWith(u8, candidate, prefix)) return true,
        else => {},
    };
    return false;
}

fn jsonValuesContainWildcard(values: []const std.json.Value, pattern: []const u8) bool {
    for (values) |value| switch (value) {
        .string => |candidate| if (wildcardMatch(pattern, candidate)) return true,
        else => {},
    };
    return false;
}

fn jsonValuesContainRegexp(alloc: Allocator, values: []const std.json.Value, pattern: []const u8) !bool {
    for (values) |value| switch (value) {
        .string => |candidate| if (try regexMatches(alloc, pattern, candidate)) return true,
        else => {},
    };
    return false;
}

fn jsonValuesContainFuzzy(alloc: Allocator, values: []const std.json.Value, fuzzy_query: std.json.Value) !bool {
    var term: []const u8 = undefined;
    var max_edits: u8 = 1;
    var prefix_len: u8 = 0;
    var auto_fuzzy = false;
    switch (fuzzy_query) {
        .string => |text| term = text,
        .object => |object| {
            const query_value = object.get("query") orelse return error.InvalidArgument;
            if (query_value != .string) return error.InvalidArgument;
            term = query_value.string;
            if (object.get("max_edits")) |edits| {
                max_edits = switch (edits) {
                    .integer => |number| std.math.cast(u8, number) orelse return error.InvalidArgument,
                    .float => |number| blk: {
                        if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
                        const parsed: i64 = @intFromFloat(number);
                        break :blk std.math.cast(u8, parsed) orelse return error.InvalidArgument;
                    },
                    else => return error.InvalidArgument,
                };
            }
            if (object.get("prefix_length")) |prefix| {
                prefix_len = switch (prefix) {
                    .integer => |number| std.math.cast(u8, number) orelse return error.InvalidArgument,
                    .float => |number| blk: {
                        if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
                        const parsed: i64 = @intFromFloat(number);
                        break :blk std.math.cast(u8, parsed) orelse return error.InvalidArgument;
                    },
                    else => return error.InvalidArgument,
                };
            }
            if (object.get("auto_fuzzy")) |auto| {
                if (auto != .bool) return error.InvalidArgument;
                auto_fuzzy = auto.bool;
            }
        },
        else => return error.InvalidArgument,
    }

    if (auto_fuzzy) max_edits = if (term.len > 5) 2 else if (term.len > 2) 1 else 0;

    for (values) |value| switch (value) {
        .string => |candidate| {
            if (!fuzzyPrefixMatches(term, candidate, prefix_len)) continue;
            if (try fuzzyMatchString(alloc, candidate, term, max_edits)) return true;
        },
        else => {},
    };
    return false;
}

fn jsonValuesContainNumericRange(values: []const std.json.Value, range_query: std.json.Value) !bool {
    if (range_query != .object) return error.InvalidArgument;
    const min_value = if (range_query.object.get("min")) |value| try jsonNumberFromValue(value) else null;
    const max_value = if (range_query.object.get("max")) |value| try jsonNumberFromValue(value) else null;
    if (min_value == null and max_value == null) return error.InvalidArgument;
    const inclusive_min = if (range_query.object.get("inclusive_min")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else true;
    const inclusive_max = if (range_query.object.get("inclusive_max")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else false;

    for (values) |value| {
        const candidate = jsonNumberFromValue(value) catch null orelse continue;
        if (min_value) |min| if (candidate < min or (!inclusive_min and candidate == min)) continue;
        if (max_value) |max| if (candidate > max or (!inclusive_max and candidate == max)) continue;
        return true;
    }
    return false;
}

fn jsonValuesContainDateRange(values: []const std.json.Value, range_query: std.json.Value) !bool {
    if (range_query != .object) return error.InvalidArgument;
    const start_ns = if (range_query.object.get("start_ns")) |value| try jsonI64FromValue(value) else null;
    const end_ns = if (range_query.object.get("end_ns")) |value| try jsonI64FromValue(value) else null;
    if (start_ns == null and end_ns == null) return error.InvalidArgument;
    const inclusive_start = if (range_query.object.get("inclusive_start")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else true;
    const inclusive_end = if (range_query.object.get("inclusive_end")) |value| blk: {
        if (value != .bool) return error.InvalidArgument;
        break :blk value.bool;
    } else false;

    for (values) |value| {
        const candidate = jsonDateNsFromValue(value) catch null orelse continue;
        if (start_ns) |start| if (candidate < start or (!inclusive_start and candidate == start)) continue;
        if (end_ns) |end| if (candidate > end or (!inclusive_end and candidate == end)) continue;
        return true;
    }
    return false;
}

fn jsonNumberFromValue(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => error.InvalidArgument,
    };
}

fn jsonI64FromValue(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| std.math.cast(i64, number) orelse error.InvalidArgument,
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) return error.InvalidArgument;
            break :blk std.math.cast(i64, @as(i128, @intFromFloat(number))) orelse error.InvalidArgument;
        },
        else => error.InvalidArgument,
    };
}

fn jsonDateNsFromValue(value: std.json.Value) !i64 {
    return switch (value) {
        .string => |text| blk: {
            const ts = (try parsePatternRfc3339ToNs(text)) orelse return error.InvalidArgument;
            break :blk @as(i64, @intCast(ts));
        },
        .integer, .float => try jsonI64FromValue(value),
        else => error.InvalidArgument,
    };
}

fn parsePatternRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn wildcardMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == '?' or pattern[pi] == text[ti])) {
            pi += 1;
            ti += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while (pi < pattern.len and pattern[pi] == '*') pi += 1;
    return pi == pattern.len;
}

fn regexMatches(alloc: Allocator, pattern: []const u8, candidate: []const u8) !bool {
    var regex = try regex_mod.compile(alloc, pattern);
    defer regex.deinit();
    return regex_mod.matchesCompiled(pattern, &regex, candidate);
}

fn fuzzyMatchString(alloc: Allocator, candidate: []const u8, term: []const u8, max_edits: u8) !bool {
    if (candidate.len == 0 or term.len == 0) return std.mem.eql(u8, candidate, term);

    const folded_candidate = try asciiLowerDup(alloc, candidate);
    defer alloc.free(folded_candidate);
    const folded_term = try asciiLowerDup(alloc, term);
    defer alloc.free(folded_term);

    var automaton = levenshtein_mod.LevenshteinAutomaton{
        .term = folded_term,
        .max_distance = max_edits,
        .alloc = alloc,
    };
    defer automaton.deinit();

    var state = automaton.automaton().start();
    for (folded_candidate) |b| {
        state = automaton.automaton().accept(state, b);
        if (!automaton.automaton().canMatch(state)) return false;
    }
    return automaton.automaton().isMatch(state);
}

fn fuzzyPrefixMatches(term: []const u8, candidate: []const u8, prefix_len: u8) bool {
    if (prefix_len == 0) return true;
    const prefix: usize = @intCast(prefix_len);
    if (term.len < prefix or candidate.len < prefix) return false;
    return std.mem.eql(u8, term[0..prefix], candidate[0..prefix]);
}

fn asciiLowerDup(alloc: Allocator, input: []const u8) ![]u8 {
    const out = try alloc.dupe(u8, input);
    for (out) |*b| {
        if (b.* >= 'A' and b.* <= 'Z') b.* += 'a' - 'A';
    }
    return out;
}

fn containsCaseInsensitive(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (asciiEqualIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

fn asciiEqualIgnoreCase(left: []const u8, right: []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (std.ascii.toLower(l) != std.ascii.toLower(r)) return false;
    }
    return true;
}

test "stored doc matches simple term filter" {
    try std.testing.expect(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:gold",
        "{\"title\":\"gold doc\",\"tier\":\"gold\"}",
        "{\"term\":{\"tier\":\"gold\"}}",
    ));
    try std.testing.expect(!(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:silver",
        "{\"title\":\"silver doc\",\"tier\":\"silver\"}",
        "{\"term\":{\"tier\":\"gold\"}}",
    )));
}

test "stored doc matches terms filter against array values" {
    try std.testing.expect(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:gold",
        "{\"acl\":{\"roles\":[\"group:eng\",\"role:tenant_reader\"]}}",
        "{\"terms\":{\"acl.roles\":[\"role:tenant_reader\",\"role:admin\"]}}",
    ));
    try std.testing.expect(!(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:silver",
        "{\"acl\":{\"roles\":[\"group:sales\"]}}",
        "{\"terms\":{\"acl.roles\":[\"role:tenant_reader\",\"group:eng\"]}}",
    )));
}

test "stored doc matches conjunctive filters including doc id" {
    try std.testing.expect(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:gold",
        "{\"title\":\"gold doc\",\"tier\":\"gold\"}",
        "{\"conjuncts\":[{\"doc_id\":{\"ids\":[\"doc:gold\"]}},{\"term\":{\"tier\":\"gold\"}}]}",
    ));
    try std.testing.expect(!(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:silver",
        "{\"title\":\"silver doc\",\"tier\":\"gold\"}",
        "{\"conjuncts\":[{\"doc_id\":{\"ids\":[\"doc:gold\"]}},{\"term\":{\"tier\":\"gold\"}}]}",
    )));
}

test "stored doc bool filter clauses are required with must clauses" {
    try std.testing.expect(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:gold",
        "{\"tenant\":\"acme\",\"tier\":\"gold\"}",
        "{\"bool\":{\"must\":[{\"term\":{\"tenant\":\"acme\"}}],\"filter\":[{\"term\":{\"tier\":\"gold\"}}]}}",
    ));
    try std.testing.expect(!(try storedDocMatchesPatternFilter(
        std.testing.allocator,
        "doc:silver",
        "{\"tenant\":\"acme\",\"tier\":\"silver\"}",
        "{\"bool\":{\"must\":[{\"term\":{\"tenant\":\"acme\"}}],\"filter\":[{\"term\":{\"tier\":\"gold\"}}]}}",
    )));
}
