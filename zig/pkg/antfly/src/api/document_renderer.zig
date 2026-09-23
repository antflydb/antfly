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

//! Renders retrieved documents for the retrieval agent's generation prompt.
//!
//! Without a `document_renderer` template, each hit's source fields are
//! encoded as TOON (Token-Oriented Object Notation), which carries the same
//! structure as JSON in fewer prompt tokens. A `document_renderer` is a
//! Handlebars template rendered once per hit against
//! `{ id, score, fields }`, with an `encodeToon` helper:
//!
//!   {{encodeToon this.fields}}
//!   {{encodeToon this.fields indent=4 delimiter="tab"}}
//!
//! `delimiter` accepts `comma` (default), `tab`, or `pipe` (or the literal
//! characters). `indent` must be at least 1.

const std = @import("std");
const Allocator = std.mem.Allocator;
const hbs = @import("handlebars");
const toon = @import("antfly_toon");
const template = @import("../template.zig");
const Expression = @FieldType(hbs.Node, "expression");

/// Source keys that carry retrieval metadata the prompt already states
/// elsewhere, so the default rendering omits them.
const omitted_default_keys = [_][]const u8{"_tree"};

/// Default rendering: the hit's source fields as TOON.
pub fn renderDefault(alloc: Allocator, source: std.json.ObjectMap) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var fields: std.json.ObjectMap = .empty;
    var it = source.iterator();
    next: while (it.next()) |entry| {
        for (omitted_default_keys) |omitted| {
            if (std.mem.eql(u8, entry.key_ptr.*, omitted)) continue :next;
        }
        try fields.put(arena, entry.key_ptr.*, entry.value_ptr.*);
    }
    return try toon.encodeValueAlloc(alloc, .{ .object = fields }, .{});
}

/// Render one hit through a caller-supplied `document_renderer` template.
pub fn renderTemplate(
    alloc: Allocator,
    template_source: []const u8,
    id: []const u8,
    score: f32,
    source: ?std.json.ObjectMap,
) ![]const u8 {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var root: std.json.ObjectMap = .empty;
    try root.put(arena, "id", .{ .string = id });
    try root.put(arena, "score", .{ .float = score });
    try root.put(arena, "fields", .{ .object = source orelse .empty });

    var json: std.Io.Writer.Allocating = .init(arena);
    try std.json.Stringify.value(std.json.Value{ .object = root }, .{}, &json.writer);

    // The evaluator renders a failed helper as empty text, so the helper
    // records its failure here for this call to report.
    var state: HelperState = .{};
    var helpers: hbs.HelperMap = .{};
    try helpers.put(arena, "encodeToon", hbs.Helper.withData(&encodeToonHelper, &state));
    const rendered = try template.renderDocumentWithHelpers(alloc, template_source, json.written(), &helpers);
    if (state.failure) |err| {
        alloc.free(rendered);
        return err;
    }
    return rendered;
}

const HelperState = struct {
    failure: ?anyerror = null,
};

/// Reject a template that cannot parse or whose helper arguments are invalid
/// before any retrieval work runs. `encodeToon` options are checked on every
/// call site in the syntax tree, including branches that sample data would
/// skip, and must be literals so a valid template cannot fail per hit.
pub fn validateTemplate(alloc: Allocator, template_source: []const u8) !void {
    {
        var arena_state = std.heap.ArenaAllocator.init(alloc);
        defer arena_state.deinit();
        const program = hbs.Parser.parse(template_source, arena_state.allocator()) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidDocumentRenderer,
        };
        try validateNode(program);
    }
    const rendered = renderTemplate(alloc, template_source, "", 0, null) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDocumentRenderer,
    };
    alloc.free(rendered);
}

fn validateNode(node: *const hbs.Node) error{InvalidDocumentRenderer}!void {
    switch (node.*) {
        .program => |program| for (program.body) |child| try validateNode(child),
        .mustache => |mustache| try validateNode(mustache.expression),
        .block => |block| {
            try validateNode(block.expression);
            try validateNode(block.program);
            if (block.inverse) |inverse| try validateNode(inverse);
        },
        .partial => |partial| {
            try validateNode(partial.name);
            for (partial.params) |param| try validateNode(param);
            if (partial.hash) |hash| try validateNode(hash);
        },
        .partial_block => |partial| {
            try validateNode(partial.name);
            for (partial.params) |param| try validateNode(param);
            if (partial.hash) |hash| try validateNode(hash);
            try validateNode(partial.program);
        },
        .inline_partial => |partial| try validateNode(partial.program),
        .expression => |expression| {
            if (isEncodeToonCall(expression)) try validateEncodeToonOptions(expression);
            try validateNode(expression.path);
            for (expression.params) |param| try validateNode(param);
            if (expression.hash) |hash| try validateNode(hash);
        },
        .sub_expression => |sub| try validateNode(sub.expression),
        .hash => |hash| for (hash.pairs) |pair| try validateNode(pair),
        .hash_pair => |pair| try validateNode(pair.value),
        .content, .comment, .path, .string_literal, .boolean_literal, .number_literal => {},
    }
}

fn isEncodeToonCall(expression: Expression) bool {
    return switch (expression.path.*) {
        .path => |path| !path.data and path.parts.len == 1 and std.mem.eql(u8, path.parts[0], "encodeToon"),
        else => false,
    };
}

fn validateEncodeToonOptions(expression: Expression) error{InvalidDocumentRenderer}!void {
    const hash = expression.hash orelse return;
    for (hash.hash.pairs) |pair_node| {
        const pair = pair_node.hash_pair;
        if (std.mem.eql(u8, pair.key, "indent")) {
            const literal = switch (pair.value.*) {
                .number_literal => |number| number,
                else => return error.InvalidDocumentRenderer,
            };
            if (!literal.is_int or literal.value < 1) return error.InvalidDocumentRenderer;
        } else if (std.mem.eql(u8, pair.key, "delimiter")) {
            const literal = switch (pair.value.*) {
                .string_literal => |string| string,
                else => return error.InvalidDocumentRenderer,
            };
            _ = parseDelimiter(literal.value) catch return error.InvalidDocumentRenderer;
        } else return error.InvalidDocumentRenderer;
    }
}

fn encodeToonHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    return encodeToon(ctx) catch |err| {
        if (ctx.userdata) |userdata| {
            const state: *HelperState = @ptrCast(@alignCast(userdata));
            if (state.failure == null) state.failure = err;
        }
        return err;
    };
}

fn encodeToon(ctx: hbs.HelperContext) anyerror!hbs.Value {
    if (ctx.params.len == 0) return .{ .safe_string = "" };
    var options: toon.EncodeOptions = .{};
    if (ctx.hashGet("indent")) |value| options.indent = switch (value) {
        .integer => |indent| if (indent >= 1) @intCast(indent) else return error.InvalidToonIndent,
        else => return error.InvalidToonIndent,
    };
    if (ctx.hashGet("delimiter")) |value| options.delimiter = switch (value) {
        .string, .safe_string => |name| try parseDelimiter(name),
        else => return error.InvalidToonDelimiter,
    };
    for (ctx.hash.keys()) |key| {
        if (!std.mem.eql(u8, key, "indent") and !std.mem.eql(u8, key, "delimiter")) return error.InvalidToonOption;
    }
    const encoded = try toon.encodeValueAlloc(ctx.arena, try toJson(ctx.arena, ctx.params[0]), options);
    // TOON is plain text for the prompt; HTML-escaping it would corrupt quotes.
    return .{ .safe_string = encoded };
}

fn parseDelimiter(name: []const u8) !toon.Delimiter {
    if (std.mem.eql(u8, name, "comma") or std.mem.eql(u8, name, ",")) return .comma;
    if (std.mem.eql(u8, name, "tab") or std.mem.eql(u8, name, "\t") or std.mem.eql(u8, name, "\\t")) return .tab;
    if (std.mem.eql(u8, name, "pipe") or std.mem.eql(u8, name, "|")) return .pipe;
    return error.InvalidToonDelimiter;
}

fn toJson(arena: Allocator, value: hbs.Value) Allocator.Error!std.json.Value {
    return switch (value) {
        .null, .undefined => .null,
        .boolean => |b| .{ .bool = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .string, .safe_string => |s| .{ .string = s },
        .array => |items| blk: {
            var array = std.json.Array.init(arena);
            try array.ensureTotalCapacityPrecise(items.len);
            for (items) |item| array.appendAssumeCapacity(try toJson(arena, item));
            break :blk .{ .array = array };
        },
        .map => |map| blk: {
            var object: std.json.ObjectMap = .empty;
            try object.ensureTotalCapacity(arena, map.count());
            var it = map.iterator();
            while (it.next()) |entry| object.putAssumeCapacity(entry.key_ptr.*, try toJson(arena, entry.value_ptr.*));
            break :blk .{ .object = object };
        },
    };
}

fn testSource(alloc: Allocator) !std.json.ObjectMap {
    var source: std.json.ObjectMap = .empty;
    try source.put(alloc, "title", .{ .string = "Lighthouse \"keeper\"" });
    try source.put(alloc, "year", .{ .integer = 1906 });
    var tree: std.json.ObjectMap = .empty;
    try tree.put(alloc, "depth", .{ .integer = 1 });
    try source.put(alloc, "_tree", .{ .object = tree });
    return source;
}

test "default rendering encodes source fields as TOON without tree metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const source = try testSource(arena.allocator());

    const rendered = try renderDefault(std.testing.allocator, source);
    defer std.testing.allocator.free(rendered);
    try std.testing.expectEqualStrings("title: \"Lighthouse \\\"keeper\\\"\"\nyear: 1906", rendered);
}

test "document_renderer template exposes id, score, and fields with encodeToon" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const source = try testSource(arena.allocator());

    const rendered = try renderTemplate(
        std.testing.allocator,
        "[{{this.id}}] {{{this.fields.title}}}\n{{encodeToon this.fields}}",
        "doc:1",
        0.5,
        source,
    );
    defer std.testing.allocator.free(rendered);
    try std.testing.expectEqualStrings("[doc:1] Lighthouse \"keeper\"\ntitle: \"Lighthouse \\\"keeper\\\"\"\nyear: 1906", rendered);
}

test "encodeToon honors indent and delimiter options" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var source: std.json.ObjectMap = .empty;
    var tags = std.json.Array.init(alloc);
    try tags.append(.{ .string = "a" });
    try tags.append(.{ .string = "b" });
    try source.put(alloc, "tags", .{ .array = tags });

    const rendered = try renderTemplate(std.testing.allocator, "{{encodeToon this.fields delimiter=\"pipe\"}}", "doc:1", 1, source);
    defer std.testing.allocator.free(rendered);
    try std.testing.expectEqualStrings("tags[2|]: a|b", rendered);
}

test "validateTemplate rejects invalid templates and helper options" {
    try validateTemplate(std.testing.allocator, "{{encodeToon this.fields}}");
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{#if}}"));
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{encodeToon this.fields indent=0}}"));
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{encodeToon this.fields delimiter=\"semicolon\"}}"));
    // Options are checked where sample data would skip the branch.
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{#if this.fields.title}}{{encodeToon this.fields indent=0}}{{/if}}"));
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{#each this.fields.items}}{{else}}{{encodeToon this indent=2.5}}{{/each}}"));
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{#if this.fields.title}}{{eq (encodeToon this.fields delimiter=\"semicolon\") \"x\"}}{{/if}}"));
    // Options must be literals, and unknown options are rejected.
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{encodeToon this.fields indent=this.fields.indent}}"));
    try std.testing.expectError(error.InvalidDocumentRenderer, validateTemplate(std.testing.allocator, "{{encodeToon this.fields lengthMarker=false}}"));
    try validateTemplate(std.testing.allocator, "{{#if this.fields.title}}{{encodeToon this.fields indent=4 delimiter=\"tab\"}}{{/if}}");
}
