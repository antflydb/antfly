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

//! Lucene-style query string parser.
//!
//! Parses query strings into Filter ASTs using existing filter types.
//!
//! Supported syntax:
//!   hello                  → TermFilter("_all", "hello")
//!   hello world            → BoolFilter(must: [term("hello"), term("world")])
//!   field:value            → TermFilter("field", "value")
//!   "exact phrase"         → PhraseFilter("_all", ["exact", "phrase"])
//!   field:"exact phrase"   → PhraseFilter("field", ["exact", "phrase"])
//!   +required -excluded    → BoolFilter(must/must_not)
//!   foo AND bar            → BoolFilter(must: [term("foo"), term("bar")])
//!   foo OR bar             → BoolFilter(should: [term("foo"), term("bar")])
//!   NOT foo                → BoolFilter(must_not: [term("foo")], must: [match_all])
//!   field:pre*             → PrefixFilter("field", "pre")
//!   field:/regex/          → RegexpFilter("field", "regex")
//!   (foo AND bar)          → nested BoolFilter
//!   field:~term            → FuzzyFilter("field", "term")
//!   title:(foo bar)        → BoolFilter(must: [term("title","foo"), term("title","bar")])
//!   "hello world"~3        → PhraseFilter("_all", ["hello","world"], slop=3)
//!   title:[a TO z]         → TermRangeFilter("title", "a", "z")
//!   age:[10 TO 20}         → RangeFilter("age", 10, 20)
//!   hello^2                → boosted term filter

const std = @import("std");
const Allocator = std.mem.Allocator;
const query = @import("query.zig");
const Filter = query.Filter;
const analysis = @import("analysis.zig");

pub const ParseError = error{
    InvalidSyntax,
    OutOfMemory,
};

/// Physical kind of a declared relational typed column, used to auto-route
/// query-string predicates on that column to the matching typed-doc-values
/// filter (instead of an inverted-index term/term_range filter that would not
/// match a typed column). See zig/RELATIONAL.md.
pub const TypedColumnKind = enum {
    numeric,
    datetime,
    boolean,
};

pub const TypedColumn = struct {
    name: []const u8,
    kind: TypedColumnKind,
};

pub const QueryStringParser = struct {
    pub const DefaultOperator = enum {
        and_,
        or_,
    };

    default_field: []const u8 = "_all",
    default_operator: DefaultOperator = .and_,
    /// Declared relational typed columns. When a field predicate targets one of
    /// these, it is routed to the typed-doc-values filter. Empty for document
    /// (schemaless) tables, leaving behaviour unchanged.
    typed_columns: []const TypedColumn = &.{},

    /// Parse a query string into a Filter. Caller must manage the allocator
    /// for any allocated filter data (phrase terms, bool sub-filters).
    pub fn parse(self: *const QueryStringParser, alloc: Allocator, input: []const u8) ParseError!Filter {
        var parser = Parser{
            .alloc = alloc,
            .input = input,
            .pos = 0,
            .default_field = self.default_field,
            .default_operator = self.default_operator,
            .typed_columns = self.typed_columns,
        };
        const result = try parser.parseQuery();

        // If nothing was parsed, return match_all
        return result orelse .match_all;
    }
};

const Parser = struct {
    alloc: Allocator,
    input: []const u8,
    pos: usize,
    default_field: []const u8,
    default_operator: QueryStringParser.DefaultOperator,
    typed_columns: []const TypedColumn = &.{},

    /// Return the declared typed-column kind for `field`, if any.
    fn typedColumnKind(self: *const Parser, field: []const u8) ?TypedColumnKind {
        for (self.typed_columns) |column| {
            if (std.mem.eql(u8, column.name, field)) return column.kind;
        }
        return null;
    }

    /// Build a typed-doc-values equality filter for a declared typed column.
    /// Returns null if the value does not parse as the column's type, so the
    /// caller can fall back to a normal term filter.
    fn typedEqualityFilter(kind: TypedColumnKind, field: []const u8, value: []const u8) ?Filter {
        switch (kind) {
            .numeric => {
                const number = std.fmt.parseFloat(f64, value) catch return null;
                return .{ .range = .{
                    .field = field,
                    .min_val = number,
                    .max_val = number,
                    .inclusive_min = true,
                    .inclusive_max = true,
                } };
            },
            .datetime => {
                const ns = std.fmt.parseInt(u64, value, 10) catch return null;
                return .{ .date_range = .{
                    .field = field,
                    .start_ns = ns,
                    .end_ns = ns,
                    .inclusive_start = true,
                    .inclusive_end = true,
                } };
            },
            .boolean => {
                const flag = if (std.mem.eql(u8, value, "true"))
                    true
                else if (std.mem.eql(u8, value, "false"))
                    false
                else
                    return null;
                return .{ .bool_field = .{ .field = field, .value = flag } };
            },
        }
    }

    /// Build a typed-doc-values range filter for a declared numeric/datetime
    /// column from string bounds. Returns null on parse failure or for boolean
    /// columns (ranges are meaningless), so the caller can fall back.
    fn typedRangeFilter(
        kind: TypedColumnKind,
        field: []const u8,
        min_word: ?[]const u8,
        max_word: ?[]const u8,
        inclusive_min: bool,
        inclusive_max: bool,
    ) ?Filter {
        switch (kind) {
            .numeric => {
                const min_val: ?f64 = if (min_word) |w| (std.fmt.parseFloat(f64, w) catch return null) else null;
                const max_val: ?f64 = if (max_word) |w| (std.fmt.parseFloat(f64, w) catch return null) else null;
                return .{ .range = .{
                    .field = field,
                    .min_val = min_val,
                    .max_val = max_val,
                    .inclusive_min = inclusive_min,
                    .inclusive_max = inclusive_max,
                } };
            },
            .datetime => {
                const start_ns: ?u64 = if (min_word) |w| (std.fmt.parseInt(u64, w, 10) catch return null) else null;
                const end_ns: ?u64 = if (max_word) |w| (std.fmt.parseInt(u64, w, 10) catch return null) else null;
                return .{ .date_range = .{
                    .field = field,
                    .start_ns = start_ns,
                    .end_ns = end_ns,
                    .inclusive_start = inclusive_min,
                    .inclusive_end = inclusive_max,
                } };
            },
            .boolean => return null,
        }
    }

    fn parseQuery(self: *Parser) ParseError!?Filter {
        self.skipWhitespace();
        if (self.pos >= self.input.len) return null;

        var left = (try self.parseClause()) orelse return null;

        while (self.pos < self.input.len) {
            self.skipWhitespace();
            if (self.pos >= self.input.len) break;

            // Check for explicit AND/OR
            if (self.matchKeyword("AND")) {
                self.skipWhitespace();
                const right = (try self.parseClause()) orelse break;
                left = try self.makeBool(.must, left, right);
            } else if (self.matchKeyword("OR")) {
                self.skipWhitespace();
                const right = (try self.parseClause()) orelse break;
                left = try self.makeBool(.should, left, right);
            } else if (self.input[self.pos] == ')') {
                break; // end of group
            } else {
                // Implicit default operator
                const right = (try self.parseClause()) orelse break;
                left = try self.makeBool(switch (self.default_operator) {
                    .and_ => .must,
                    .or_ => .should,
                }, left, right);
            }
        }

        return left;
    }

    const BoolKind = enum { must, should };

    fn makeBool(self: *Parser, kind: BoolKind, a: Filter, b: Filter) ParseError!Filter {
        // If a is already a bool of the same kind, extend it
        switch (a) {
            .bool_filter => |bf| {
                if (kind == .must and bf.should.len == 0 and bf.must_not.len == 0 and bf.must.len > 0) {
                    // Extend existing must
                    const old = bf.must;
                    const new_filters = try self.alloc.alloc(Filter, old.len + 1);
                    @memcpy(new_filters[0..old.len], old);
                    new_filters[old.len] = b;
                    self.alloc.free(old);
                    return .{ .bool_filter = .{ .must = new_filters, .should = &.{}, .must_not = &.{}, .boost = bf.boost } };
                }
                if (kind == .should and bf.must.len == 0 and bf.must_not.len == 0 and bf.should.len > 0) {
                    const old = bf.should;
                    const new_filters = try self.alloc.alloc(Filter, old.len + 1);
                    @memcpy(new_filters[0..old.len], old);
                    new_filters[old.len] = b;
                    self.alloc.free(old);
                    return .{ .bool_filter = .{ .must = &.{}, .should = new_filters, .must_not = &.{}, .boost = bf.boost } };
                }
            },
            else => {},
        }

        const filters = try self.alloc.alloc(Filter, 2);
        filters[0] = a;
        filters[1] = b;
        return switch (kind) {
            .must => .{ .bool_filter = .{ .must = filters, .should = &.{}, .must_not = &.{} } },
            .should => .{ .bool_filter = .{ .must = &.{}, .should = filters, .must_not = &.{} } },
        };
    }

    fn parseClause(self: *Parser) ParseError!?Filter {
        self.skipWhitespace();
        if (self.pos >= self.input.len) return null;

        // +required
        if (self.input[self.pos] == '+') {
            self.pos += 1;
            return try self.parseAtom();
        }

        // -excluded → must_not
        if (self.input[self.pos] == '-') {
            self.pos += 1;
            const inner = (try self.parseAtom()) orelse return null;
            const must_not = try self.alloc.alloc(Filter, 1);
            must_not[0] = inner;
            const must = try self.alloc.alloc(Filter, 1);
            must[0] = .match_all;
            return .{ .bool_filter = .{ .must = must, .should = &.{}, .must_not = must_not } };
        }

        // NOT
        if (self.matchKeyword("NOT")) {
            self.skipWhitespace();
            const inner = (try self.parseAtom()) orelse return null;
            const must_not = try self.alloc.alloc(Filter, 1);
            must_not[0] = inner;
            const must = try self.alloc.alloc(Filter, 1);
            must[0] = .match_all;
            return .{ .bool_filter = .{ .must = must, .should = &.{}, .must_not = must_not } };
        }

        return try self.parseAtom();
    }

    fn parseAtom(self: *Parser) ParseError!?Filter {
        self.skipWhitespace();
        if (self.pos >= self.input.len) return null;

        // Parenthesized group
        if (self.input[self.pos] == '(') {
            self.pos += 1;
            const inner = try self.parseQuery();
            self.skipWhitespace();
            if (self.pos < self.input.len and self.input[self.pos] == ')') {
                self.pos += 1;
            }
            return try self.applyOptionalBoost(inner orelse return null);
        }

        // Quoted phrase at top level (no field prefix)
        if (self.input[self.pos] == '"') {
            return try self.applyOptionalBoost(try self.parsePhrase(self.default_field));
        }

        // Read a word (potential field name or term)
        const word = self.readWord();
        if (word.len == 0) return null;

        self.skipWhitespace();

        // Check for field:value syntax
        if (self.pos < self.input.len and self.input[self.pos] == ':') {
            self.pos += 1; // skip ':'
            self.skipWhitespace();

            if (self.pos >= self.input.len) {
                return try self.applyOptionalBoost(.{ .term = .{ .field = word, .term = "" } });
            }

            if (self.input[self.pos] == '(') {
                return try self.applyOptionalBoost(try self.parseFieldGroup(word));
            }

            // field:"phrase"
            if (self.input[self.pos] == '"') {
                return try self.applyOptionalBoost(try self.parsePhrase(word));
            }

            // field:/regex/
            if (self.input[self.pos] == '/') {
                return try self.applyOptionalBoost(try self.parseRegex(word));
            }

            // field:~fuzzy
            if (self.input[self.pos] == '~') {
                self.pos += 1;
                const fuzzy_term = self.readWord();
                return try self.applyOptionalBoost(.{ .fuzzy = .{ .field = word, .term = fuzzy_term, .max_edits = 1 } });
            }

            if (self.input[self.pos] == '[' or self.input[self.pos] == '{') {
                return try self.applyOptionalBoost(try self.parseRange(word));
            }

            // field:value or field:prefix*
            const val = self.readWord();
            if (val.len > 0 and self.pos < self.input.len and self.input[self.pos] == '*') {
                self.pos += 1;
                return try self.applyOptionalBoost(.{ .prefix = .{ .field = word, .prefix = val } });
            }

            // Route equality on a declared typed column to its typed filter.
            if (self.typedColumnKind(word)) |kind| {
                if (typedEqualityFilter(kind, word, val)) |typed_filter| {
                    return try self.applyOptionalBoost(typed_filter);
                }
            }

            return try self.applyOptionalBoost(.{ .term = .{ .field = word, .term = val } });
        }

        // Bare word: prefix* syntax
        if (self.pos < self.input.len and self.input[self.pos] == '*') {
            self.pos += 1;
            return try self.applyOptionalBoost(.{ .prefix = .{ .field = self.default_field, .prefix = word } });
        }

        // Plain term
        return try self.applyOptionalBoost(.{ .term = .{ .field = self.default_field, .term = word } });
    }

    fn parsePhrase(self: *Parser, field: []const u8) ParseError!Filter {
        self.pos += 1; // skip opening '"'
        var terms = std.ArrayListUnmanaged([]const u8).empty;

        while (self.pos < self.input.len and self.input[self.pos] != '"') {
            self.skipWhitespace();
            if (self.pos >= self.input.len or self.input[self.pos] == '"') break;
            const word = self.readWord();
            if (word.len > 0) {
                terms.append(self.alloc, word) catch return ParseError.OutOfMemory;
            }
        }

        if (self.pos < self.input.len and self.input[self.pos] == '"') {
            self.pos += 1; // skip closing '"'
        }

        if (terms.items.len == 0) {
            terms.deinit(self.alloc);
            return .match_all;
        }
        if (terms.items.len == 1) {
            const term = terms.items[0];
            terms.deinit(self.alloc);
            return .{ .term = .{ .field = field, .term = term } };
        }

        var slop: u32 = 0;
        self.skipWhitespace();
        if (self.pos < self.input.len and self.input[self.pos] == '~') {
            self.pos += 1;
            slop = try self.parseUnsigned();
        }

        const phrase_terms = terms.toOwnedSlice(self.alloc) catch return ParseError.OutOfMemory;
        return .{ .phrase = .{ .field = field, .terms = phrase_terms, .slop = slop } };
    }

    fn parseRegex(self: *Parser, field: []const u8) ParseError!Filter {
        self.pos += 1; // skip opening '/'
        const start = self.pos;
        while (self.pos < self.input.len and self.input[self.pos] != '/') {
            if (self.input[self.pos] == '\\' and self.pos + 1 < self.input.len) {
                self.pos += 2; // skip escaped char
            } else {
                self.pos += 1;
            }
        }
        const pattern = self.input[start..self.pos];
        if (self.pos < self.input.len and self.input[self.pos] == '/') {
            self.pos += 1; // skip closing '/'
        }
        return .{ .regexp = .{ .field = field, .pattern = pattern } };
    }

    fn parseFieldGroup(self: *Parser, field: []const u8) ParseError!Filter {
        std.debug.assert(self.input[self.pos] == '(');
        self.pos += 1;
        const saved_field = self.default_field;
        self.default_field = field;
        defer self.default_field = saved_field;

        const inner = try self.parseQuery();
        self.skipWhitespace();
        if (self.pos >= self.input.len or self.input[self.pos] != ')') return ParseError.InvalidSyntax;
        self.pos += 1;
        return inner orelse .match_all;
    }

    fn parseRange(self: *Parser, field: []const u8) ParseError!Filter {
        // Lucene range delimiters are independent: '[' / ']' are inclusive,
        // '{' / '}' are exclusive, and the closing bracket need not match the
        // opening one (e.g. "[10 TO 20}" is min-inclusive, max-exclusive).
        const start_delim = self.input[self.pos];
        const inclusive_min = start_delim == '[';
        self.pos += 1;
        self.skipWhitespace();
        const min_raw = self.readRangeBound();
        self.skipWhitespace();
        if (!self.matchKeyword("TO")) return ParseError.InvalidSyntax;
        self.skipWhitespace();
        const max_raw = self.readRangeBound();
        self.skipWhitespace();
        if (self.pos >= self.input.len) return ParseError.InvalidSyntax;
        const end_delim = self.input[self.pos];
        if (end_delim != ']' and end_delim != '}') return ParseError.InvalidSyntax;
        self.pos += 1;
        const inclusive_max = end_delim == ']';

        const min = if (min_raw.len == 0 or std.mem.eql(u8, min_raw, "*")) null else min_raw;
        const max = if (max_raw.len == 0 or std.mem.eql(u8, max_raw, "*")) null else max_raw;

        if (min == null and max == null) return ParseError.InvalidSyntax;

        // Route ranges on a declared numeric/datetime column to the typed filter.
        if (self.typedColumnKind(field)) |kind| {
            if (typedRangeFilter(kind, field, min, max, inclusive_min, inclusive_max)) |typed_filter| {
                return typed_filter;
            }
        }

        if ((min == null or parseF64(min.?) != null) and (max == null or parseF64(max.?) != null)) {
            return .{ .range = .{
                .field = field,
                .min_val = if (min) |v| parseF64(v) else null,
                .max_val = if (max) |v| parseF64(v) else null,
                .inclusive_min = inclusive_min,
                .inclusive_max = inclusive_max,
            } };
        }
        if ((min == null or tryParseRfc3339ToNs(min.?) != null) and (max == null or tryParseRfc3339ToNs(max.?) != null)) {
            return .{ .date_range = .{
                .field = field,
                .start_ns = if (min) |v| tryParseRfc3339ToNs(v) else null,
                .end_ns = if (max) |v| tryParseRfc3339ToNs(v) else null,
                .inclusive_start = inclusive_min,
                .inclusive_end = inclusive_max,
            } };
        }
        return .{ .term_range = .{
            .field = field,
            .min = min,
            .max = max,
            .inclusive_min = inclusive_min,
            .inclusive_max = inclusive_max,
        } };
    }

    fn readRangeBound(self: *Parser) []const u8 {
        const start = self.pos;
        while (self.pos < self.input.len) {
            // Stop at either closing delimiter (']' or '}') so mixed-delimiter
            // ranges parse; the upper bound's inclusivity is decided by which
            // bracket actually closes the range.
            const c = self.input[self.pos];
            if (c == ']' or c == '}') break;
            if (std.mem.startsWith(u8, self.input[self.pos..], " TO")) break;
            self.pos += 1;
        }
        return std.mem.trim(u8, self.input[start..self.pos], &std.ascii.whitespace);
    }

    fn applyOptionalBoost(self: *Parser, filter: Filter) ParseError!Filter {
        self.skipWhitespace();
        if (self.pos >= self.input.len or self.input[self.pos] != '^') return filter;
        self.pos += 1;
        const boost = try self.parseFloat();
        return applyBoost(filter, boost);
    }

    fn parseUnsigned(self: *Parser) ParseError!u32 {
        const start = self.pos;
        while (self.pos < self.input.len and std.ascii.isDigit(self.input[self.pos])) : (self.pos += 1) {}
        if (start == self.pos) return ParseError.InvalidSyntax;
        return std.fmt.parseInt(u32, self.input[start..self.pos], 10) catch ParseError.InvalidSyntax;
    }

    fn parseFloat(self: *Parser) ParseError!f32 {
        const start = self.pos;
        while (self.pos < self.input.len) {
            const c = self.input[self.pos];
            if (!(std.ascii.isDigit(c) or c == '.' or c == '+' or c == '-' or c == 'e' or c == 'E')) break;
            self.pos += 1;
        }
        if (start == self.pos) return ParseError.InvalidSyntax;
        return std.fmt.parseFloat(f32, self.input[start..self.pos]) catch ParseError.InvalidSyntax;
    }

    fn readWord(self: *Parser) []const u8 {
        const start = self.pos;
        while (self.pos < self.input.len) {
            const c = self.input[self.pos];
            if (c == ' ' or c == '\t' or c == '\n' or c == ':' or c == '(' or c == ')' or c == '"' or c == '*' or c == '^') break;
            self.pos += 1;
        }
        return self.input[start..self.pos];
    }

    fn skipWhitespace(self: *Parser) void {
        while (self.pos < self.input.len and (self.input[self.pos] == ' ' or self.input[self.pos] == '\t' or self.input[self.pos] == '\n')) {
            self.pos += 1;
        }
    }

    fn matchKeyword(self: *Parser, keyword: []const u8) bool {
        if (self.pos + keyword.len > self.input.len) return false;
        if (!std.mem.eql(u8, self.input[self.pos..][0..keyword.len], keyword)) return false;
        // Must be followed by whitespace or end of input (not part of a word)
        if (self.pos + keyword.len < self.input.len) {
            const next = self.input[self.pos + keyword.len];
            if (next != ' ' and next != '\t' and next != '\n' and next != '(' and next != ')') return false;
        }
        self.pos += keyword.len;
        return true;
    }
};

fn applyBoost(filter: Filter, boost: f32) ParseError!Filter {
    return switch (filter) {
        .term => |term| .{ .term = .{ .field = term.field, .term = term.term, .boost = term.boost * boost } },
        .prefix => |prefix| .{ .prefix = .{ .field = prefix.field, .prefix = prefix.prefix, .boost = prefix.boost * boost } },
        .phrase => |phrase| .{ .phrase = .{
            .field = phrase.field,
            .terms = phrase.terms,
            .slop = phrase.slop,
            .max_edits = phrase.max_edits,
            .auto_fuzzy = phrase.auto_fuzzy,
            .boost = phrase.boost * boost,
        } },
        .fuzzy => |fuzzy| .{ .fuzzy = .{
            .field = fuzzy.field,
            .term = fuzzy.term,
            .max_edits = fuzzy.max_edits,
            .prefix_len = fuzzy.prefix_len,
            .boost = fuzzy.boost * boost,
        } },
        .regexp => |regexp| .{ .regexp = .{ .field = regexp.field, .pattern = regexp.pattern, .boost = regexp.boost * boost } },
        .wildcard => |wildcard| .{ .wildcard = .{ .field = wildcard.field, .pattern = wildcard.pattern, .boost = wildcard.boost * boost } },
        .range => |range| .{ .range = .{
            .field = range.field,
            .min_val = range.min_val,
            .max_val = range.max_val,
            .inclusive_min = range.inclusive_min,
            .inclusive_max = range.inclusive_max,
            .boost = range.boost * boost,
        } },
        .date_range => |range| .{ .date_range = .{
            .field = range.field,
            .start_ns = range.start_ns,
            .end_ns = range.end_ns,
            .inclusive_start = range.inclusive_start,
            .inclusive_end = range.inclusive_end,
            .boost = range.boost * boost,
        } },
        .term_range => |range| .{ .term_range = .{
            .field = range.field,
            .min = range.min,
            .max = range.max,
            .inclusive_min = range.inclusive_min,
            .inclusive_max = range.inclusive_max,
            .boost = range.boost * boost,
        } },
        .bool_filter => |bool_filter| .{ .bool_filter = .{
            .must = bool_filter.must,
            .should = bool_filter.should,
            .must_not = bool_filter.must_not,
            .min_should_match = bool_filter.min_should_match,
            .boost = bool_filter.boost * boost,
        } },
        else => filter,
    };
}

fn parseF64(text: []const u8) ?f64 {
    return std.fmt.parseFloat(f64, text) catch null;
}

fn tryParseRfc3339ToNs(text: []const u8) ?u64 {
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

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "query string: simple term" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "hello");

    try testing.expectEqual(std.meta.activeTag(filter), .term);
    try testing.expectEqualStrings("_all", filter.term.field);
    try testing.expectEqualStrings("hello", filter.term.term);
}

test "query string: field-scoped term" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "title:hello");

    try testing.expectEqual(std.meta.activeTag(filter), .term);
    try testing.expectEqualStrings("title", filter.term.field);
    try testing.expectEqualStrings("hello", filter.term.term);
}

test "query string: quoted phrase" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "\"hello world\"");
    defer alloc.free(filter.phrase.terms);

    try testing.expectEqual(std.meta.activeTag(filter), .phrase);
    try testing.expectEqualStrings("_all", filter.phrase.field);
    try testing.expectEqual(@as(usize, 2), filter.phrase.terms.len);
    try testing.expectEqualStrings("hello", filter.phrase.terms[0]);
    try testing.expectEqualStrings("world", filter.phrase.terms[1]);
}

test "query string: boolean AND" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "foo AND bar");
    defer alloc.free(filter.bool_filter.must);

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    const must = filter.bool_filter.must;
    try testing.expectEqual(@as(usize, 2), must.len);
    try testing.expectEqualStrings("foo", must[0].term.term);
    try testing.expectEqualStrings("bar", must[1].term.term);
}

test "query string: boolean OR" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "foo OR bar");
    defer alloc.free(filter.bool_filter.should);

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    const should = filter.bool_filter.should;
    try testing.expectEqual(@as(usize, 2), should.len);
    try testing.expectEqualStrings("foo", should[0].term.term);
    try testing.expectEqualStrings("bar", should[1].term.term);
}

test "query string: prefix wildcard" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "hel*");

    try testing.expectEqual(std.meta.activeTag(filter), .prefix);
    try testing.expectEqualStrings("_all", filter.prefix.field);
    try testing.expectEqualStrings("hel", filter.prefix.prefix);
}

test "query string: combined field AND" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "title:hello AND body:world");
    defer alloc.free(filter.bool_filter.must);

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    const must = filter.bool_filter.must;
    try testing.expectEqual(@as(usize, 2), must.len);
    try testing.expectEqualStrings("title", must[0].term.field);
    try testing.expectEqualStrings("hello", must[0].term.term);
    try testing.expectEqualStrings("body", must[1].term.field);
    try testing.expectEqualStrings("world", must[1].term.term);
}

test "query string: NOT" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "NOT spam");
    defer {
        alloc.free(filter.bool_filter.must);
        alloc.free(filter.bool_filter.must_not);
    }

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    try testing.expect(filter.bool_filter.must_not.len > 0);
    try testing.expectEqualStrings("spam", filter.bool_filter.must_not[0].term.term);
}

test "query string: regex" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "field:/hel.*/");

    try testing.expectEqual(std.meta.activeTag(filter), .regexp);
    try testing.expectEqualStrings("field", filter.regexp.field);
    try testing.expectEqualStrings("hel.*", filter.regexp.pattern);
}

test "query string: fuzzy" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "title:~hello");

    try testing.expectEqual(std.meta.activeTag(filter), .fuzzy);
    try testing.expectEqualStrings("title", filter.fuzzy.field);
    try testing.expectEqualStrings("hello", filter.fuzzy.term);
}

test "query string: empty input" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "");
    try testing.expectEqual(std.meta.activeTag(filter), .match_all);
}

test "query string: parenthesized group" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "(foo OR bar) AND baz");

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    const must = filter.bool_filter.must;
    defer alloc.free(must);
    try testing.expectEqual(@as(usize, 2), must.len);

    // First element should be the OR group
    try testing.expectEqual(std.meta.activeTag(must[0]), .bool_filter);
    const should = must[0].bool_filter.should;
    defer alloc.free(should);
    try testing.expectEqualStrings("foo", should[0].term.term);
    try testing.expectEqualStrings("bar", should[1].term.term);

    // Second element is baz
    try testing.expectEqualStrings("baz", must[1].term.term);
}

test "query string: field-scoped phrase" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "title:\"hello world\"");
    defer alloc.free(filter.phrase.terms);

    try testing.expectEqual(std.meta.activeTag(filter), .phrase);
    try testing.expectEqualStrings("title", filter.phrase.field);
    try testing.expectEqual(@as(usize, 2), filter.phrase.terms.len);
}

test "query string: field group" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "title:(foo bar)");
    defer alloc.free(filter.bool_filter.must);

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    try testing.expectEqual(@as(usize, 2), filter.bool_filter.must.len);
    try testing.expectEqual(std.meta.activeTag(filter.bool_filter.must[0]), .term);
    try testing.expectEqual(std.meta.activeTag(filter.bool_filter.must[1]), .term);
    try testing.expectEqualStrings("title", filter.bool_filter.must[0].term.field);
    try testing.expectEqualStrings("foo", filter.bool_filter.must[0].term.term);
    try testing.expectEqualStrings("title", filter.bool_filter.must[1].term.field);
    try testing.expectEqualStrings("bar", filter.bool_filter.must[1].term.term);
}

test "query string: phrase slop" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "\"hello world\"~3");
    defer alloc.free(filter.phrase.terms);

    try testing.expectEqual(std.meta.activeTag(filter), .phrase);
    try testing.expectEqual(@as(u32, 3), filter.phrase.slop);
}

test "query string: term boost" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "hello^2.5");

    try testing.expectEqual(std.meta.activeTag(filter), .term);
    try testing.expectApproxEqAbs(@as(f32, 2.5), filter.term.boost, 0.0001);
}

test "query string: grouped boost" {
    const parser = QueryStringParser{};
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "(foo OR bar)^4");
    defer alloc.free(filter.bool_filter.should);

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    try testing.expectApproxEqAbs(@as(f32, 4.0), filter.bool_filter.boost, 0.0001);
}

test "query string: default operator OR" {
    const parser = QueryStringParser{ .default_operator = .or_ };
    const alloc = testing.allocator;
    const filter = try parser.parse(alloc, "foo bar");
    defer alloc.free(filter.bool_filter.should);

    try testing.expectEqual(std.meta.activeTag(filter), .bool_filter);
    try testing.expectEqual(@as(usize, 2), filter.bool_filter.should.len);
    try testing.expectEqual(std.meta.activeTag(filter.bool_filter.should[0]), .term);
    try testing.expectEqual(std.meta.activeTag(filter.bool_filter.should[1]), .term);
}

test "query string: numeric range" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "age:[10 TO 20}");

    try testing.expectEqual(std.meta.activeTag(filter), .range);
    try testing.expectEqualStrings("age", filter.range.field);
    try testing.expectEqual(@as(?f64, 10), filter.range.min_val);
    try testing.expectEqual(@as(?f64, 20), filter.range.max_val);
    try testing.expect(filter.range.inclusive_min);
    try testing.expect(!filter.range.inclusive_max);
}

test "query string: date range" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]");

    try testing.expectEqual(std.meta.activeTag(filter), .date_range);
    try testing.expectEqualStrings("created", filter.date_range.field);
    try testing.expect(filter.date_range.start_ns != null);
    try testing.expect(filter.date_range.end_ns != null);
}

test "query string: term range" {
    const parser = QueryStringParser{};
    const filter = try parser.parse(testing.allocator, "title:[alpha TO omega]");

    try testing.expectEqual(std.meta.activeTag(filter), .term_range);
    try testing.expectEqualStrings("title", filter.term_range.field);
    try testing.expectEqualStrings("alpha", filter.term_range.min.?);
    try testing.expectEqualStrings("omega", filter.term_range.max.?);
}

test "typed column routes equality to typed filters" {
    // range / bool_field / date_range filters allocate nothing, so no cleanup.
    const parser = QueryStringParser{ .typed_columns = &.{
        .{ .name = "amount", .kind = .numeric },
        .{ .name = "active", .kind = .boolean },
        .{ .name = "ts", .kind = .datetime },
    } };

    const numeric = try parser.parse(testing.allocator, "amount:25");
    try testing.expect(numeric == .range);
    try testing.expectEqualStrings("amount", numeric.range.field);
    try testing.expectEqual(@as(?f64, 25.0), numeric.range.min_val);
    try testing.expectEqual(@as(?f64, 25.0), numeric.range.max_val);

    const boolean = try parser.parse(testing.allocator, "active:true");
    try testing.expect(boolean == .bool_field);
    try testing.expectEqualStrings("active", boolean.bool_field.field);
    try testing.expect(boolean.bool_field.value);

    const datetime = try parser.parse(testing.allocator, "ts:1000");
    try testing.expect(datetime == .date_range);
    try testing.expectEqual(@as(?u64, 1000), datetime.date_range.start_ns);
    try testing.expectEqual(@as(?u64, 1000), datetime.date_range.end_ns);
}

test "typed numeric column routes range to typed range filter" {
    const parser = QueryStringParser{ .typed_columns = &.{
        .{ .name = "amount", .kind = .numeric },
    } };

    // Inclusive range (matched delimiters). Note: mixed delimiters like
    // "[10 TO 40}" are rejected by the underlying parseRange before routing is
    // reached -- that is a pre-existing parser limitation, not specific to
    // typed columns.
    const filter = try parser.parse(testing.allocator, "amount:[10 TO 40]");
    try testing.expect(filter == .range);
    try testing.expectEqualStrings("amount", filter.range.field);
    try testing.expectEqual(@as(?f64, 10.0), filter.range.min_val);
    try testing.expectEqual(@as(?f64, 40.0), filter.range.max_val);
    try testing.expect(filter.range.inclusive_min);
    try testing.expect(filter.range.inclusive_max);
}

test "undeclared and non-typed columns keep term behaviour" {
    const parser = QueryStringParser{ .typed_columns = &.{
        .{ .name = "amount", .kind = .numeric },
    } };

    // Undeclared field stays a term filter.
    const term = try parser.parse(testing.allocator, "title:hello");
    try testing.expect(term == .term);

    // Declared numeric column with a non-numeric value falls back to a term.
    const fallback = try parser.parse(testing.allocator, "amount:abc");
    try testing.expect(fallback == .term);

    // No catalog at all: alphabetic range stays a term_range (unchanged).
    const plain = QueryStringParser{};
    const plain_range = try plain.parse(testing.allocator, "title:[alpha TO omega]");
    try testing.expect(plain_range == .term_range);
}

test "query string: range delimiters are independent (Lucene)" {
    const parser = QueryStringParser{};

    // [ / ] inclusive, { / } exclusive; bounds independent, mixing allowed.
    const incl_incl = try parser.parse(testing.allocator, "age:[10 TO 20]");
    try testing.expect(incl_incl == .range);
    try testing.expect(incl_incl.range.inclusive_min);
    try testing.expect(incl_incl.range.inclusive_max);

    const incl_excl = try parser.parse(testing.allocator, "age:[10 TO 20}");
    try testing.expect(incl_excl == .range);
    try testing.expect(incl_excl.range.inclusive_min);
    try testing.expect(!incl_excl.range.inclusive_max);

    const excl_incl = try parser.parse(testing.allocator, "age:{10 TO 20]");
    try testing.expect(excl_incl == .range);
    try testing.expect(!excl_incl.range.inclusive_min);
    try testing.expect(excl_incl.range.inclusive_max);

    const excl_excl = try parser.parse(testing.allocator, "age:{10 TO 20}");
    try testing.expect(excl_excl == .range);
    try testing.expect(!excl_excl.range.inclusive_min);
    try testing.expect(!excl_excl.range.inclusive_max);
}
