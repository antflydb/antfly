// Copyright 2026 Antfly, Inc.
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
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");
const eval_mod = @import("eval.zig");
const Allocator = std.mem.Allocator;

// Re-exports
pub const Value = eval_mod.Value;
pub const ValueMap = eval_mod.ValueMap;
pub const HelperFn = eval_mod.HelperFn;
pub const Helper = eval_mod.Helper;
pub const HelperContext = eval_mod.HelperContext;
pub const HelperMap = eval_mod.HelperMap;
pub const Parser = parser_mod.Parser;
pub const ParseError = parser_mod.ParseError;
pub const Eval = eval_mod.Eval;
pub const DataFrame = eval_mod.DataFrame;
pub const htmlEscape = eval_mod.htmlEscape;
pub const mapFromEntries = eval_mod.mapFromEntries;
pub const Lexer = @import("lexer.zig").Lexer;
pub const Token = @import("lexer.zig").Token;
pub const TokenKind = @import("lexer.zig").TokenKind;
pub const Node = ast.Node;

// -----------------------------------------------------------------------
// Template
// -----------------------------------------------------------------------

/// A parsed Handlebars template ready for rendering.
pub const Template = struct {
    source: []const u8,
    program: *ast.Node,
    parse_arena: std.heap.ArenaAllocator,

    /// Parse a template string. The template owns the AST memory.
    pub fn init(backing_allocator: Allocator, source: []const u8) !Template {
        var parse_arena = std.heap.ArenaAllocator.init(backing_allocator);
        errdefer parse_arena.deinit();

        const program = try Parser.parse(source, parse_arena.allocator());
        return .{
            .source = source,
            .program = program,
            .parse_arena = parse_arena,
        };
    }

    pub fn deinit(self: *Template) void {
        self.parse_arena.deinit();
    }

    /// Render the template with the given context.
    /// The returned string is allocated from the provided arena.
    pub fn render(
        self: *const Template,
        arena: Allocator,
        context: Value,
        helpers: *const HelperMap,
        partials: *const std.StringArrayHashMapUnmanaged([]const u8),
    ) ![]const u8 {
        var eval_state = Eval.init(arena, context, helpers, partials);
        return eval_state.exec(self.program);
    }

    /// Render with default empty helpers and partials.
    pub fn renderSimple(self: *const Template, arena: Allocator, context: Value) ![]const u8 {
        const helpers: HelperMap = .{};
        const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
        var eval_state = Eval.init(arena, context, &helpers, &partials);
        return eval_state.exec(self.program);
    }
};

// -----------------------------------------------------------------------
// One-shot render
// -----------------------------------------------------------------------

/// Parse and render a template in one call.
/// Uses the provided arena for both parsing and rendering.
pub fn render(
    arena: Allocator,
    source: []const u8,
    context: Value,
    helpers: *const HelperMap,
    partials: *const std.StringArrayHashMapUnmanaged([]const u8),
) ![]const u8 {
    const program = try Parser.parse(source, arena);
    var eval_state = Eval.init(arena, context, helpers, partials);
    return eval_state.exec(program);
}

/// Parse and render with default empty helpers and partials.
pub fn renderSimple(arena: Allocator, source: []const u8, context: Value) ![]const u8 {
    const helpers: HelperMap = .{};
    const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    const program = try Parser.parse(source, arena);
    var eval_state = Eval.init(arena, context, &helpers, &partials);
    return eval_state.exec(program);
}

// -----------------------------------------------------------------------
// Field extraction (AST visitor, matches antfly2 extractor.go)
// -----------------------------------------------------------------------

/// Extract all field paths referenced in a template.
/// Returns paths like [["title"], ["user", "name"]].
pub fn extractFieldPaths(arena: Allocator, source: []const u8) ![]const []const []const u8 {
    const program = try Parser.parse(source, arena);
    var extractor = FieldExtractor{};
    extractor.visitNode(program);
    return arena.dupe([]const []const u8, extractor.fields.items);
}

/// Check if a specific field path is referenced in the template.
pub fn isFieldReferenced(arena: Allocator, source: []const u8, field_path: []const []const u8) !bool {
    const paths = try extractFieldPaths(arena, source);
    for (paths) |path| {
        if (pathsEqual(path, field_path)) return true;
    }
    return false;
}

/// Remove common prefixes ("this", "fields") for schema matching.
pub fn normalizeFieldPath(path: []const []const u8) []const []const u8 {
    var result = path;
    if (result.len > 0 and std.mem.eql(u8, result[0], "this")) {
        result = result[1..];
    }
    if (result.len > 0 and std.mem.eql(u8, result[0], "fields")) {
        result = result[1..];
    }
    return result;
}

// -----------------------------------------------------------------------
// Field extractor
// -----------------------------------------------------------------------

const builtin_helpers = std.StaticStringMap(void).initComptime(.{
    .{ "if", {} },
    .{ "unless", {} },
    .{ "each", {} },
    .{ "with", {} },
    .{ "lookup", {} },
    .{ "log", {} },
    .{ "equal", {} },
    .{ "media", {} },
});

const FieldExtractor = struct {
    fields: std.ArrayListUnmanaged([]const []const u8) = .empty,
    seen: std.StringArrayHashMapUnmanaged(void) = .{},

    fn visitNode(self: *FieldExtractor, node: *const ast.Node) void {
        switch (node.*) {
            .program => |p| {
                for (p.body) |stmt| self.visitNode(stmt);
            },
            .mustache => |m| self.visitNode(m.expression),
            .block => |b| {
                self.visitNode(b.expression);
                self.visitNode(b.program);
                if (b.inverse) |inv| self.visitNode(inv);
            },
            .partial => |p| {
                for (p.params) |param| self.visitNode(param);
                if (p.hash) |h| self.visitNode(h);
            },
            .partial_block => |pb| {
                for (pb.params) |param| self.visitNode(param);
                if (pb.hash) |h| self.visitNode(h);
                self.visitNode(pb.program);
            },
            .inline_partial => |ip| {
                self.visitNode(ip.program);
            },
            .expression => |e| {
                self.visitNode(e.path);
                for (e.params) |param| self.visitNode(param);
                if (e.hash) |h| self.visitNode(h);
            },
            .sub_expression => |s| self.visitNode(s.expression),
            .path => |p| self.visitPath(&p),
            .hash => |h| {
                for (h.pairs) |pair| self.visitNode(pair);
            },
            .hash_pair => |hp| self.visitNode(hp.value),
            .content, .comment, .string_literal, .boolean_literal, .number_literal => {},
        }
    }

    fn visitPath(self: *FieldExtractor, path: *const ast.Path) void {
        if (path.data) return; // skip @data variables
        if (path.parts.len == 0) return;
        if (path.parts.len == 1 and builtin_helpers.has(path.parts[0])) return;
        if (path.parts.len == 1 and std.mem.eql(u8, path.parts[0], "this")) return;

        // Deduplicate by joining path parts
        const key = std.mem.join(std.heap.page_allocator, ".", path.parts) catch return;
        if (self.seen.contains(key)) return;
        self.seen.put(std.heap.page_allocator, key, {}) catch return;
        self.fields.append(std.heap.page_allocator, path.parts) catch return;
    }
};

fn pathsEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!std.mem.eql(u8, x, y)) return false;
    }
    return true;
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

test "render simple template" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const result = try renderSimple(arena, "Hello, {{name}}!", .{ .map = m });
    try std.testing.expectEqualStrings("Hello, World!", result);
}

test "render multiple substitutions" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "greeting", Value.str("Hello"));
    try m.put(arena, "name", Value.str("Alice"));
    try m.put(arena, "day", Value.str("Monday"));

    const result = try renderSimple(arena, "{{greeting}}, {{name}}! Today is {{day}}.", .{ .map = m });
    try std.testing.expectEqualStrings("Hello, Alice! Today is Monday.", result);
}

test "render conditional true" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("Bob"));
    try m.put(arena, "premium", Value.bln(true));

    const result = try renderSimple(arena, "{{#if premium}}Premium: {{name}}{{else}}Standard: {{name}}{{/if}}", .{ .map = m });
    try std.testing.expectEqualStrings("Premium: Bob", result);
}

test "render each loop" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("apple"), Value.str("banana"), Value.str("cherry") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const result = try renderSimple(arena, "Items: {{#each items}}{{this}}, {{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("Items: apple, banana, cherry, ", result);
}

test "render nested map" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var user: ValueMap = .{};
    try user.put(arena, "name", Value.str("John Doe"));
    try user.put(arena, "email", Value.str("john@example.com"));

    var m: ValueMap = .{};
    try m.put(arena, "user", .{ .map = user });

    const result = try renderSimple(arena, "User: {{user.name}} ({{user.email}})", .{ .map = m });
    try std.testing.expectEqualStrings("User: John Doe (john@example.com)", result);
}

test "render integer" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "count", Value.int(5));

    const result = try renderSimple(arena, "You have {{count}} new messages", .{ .map = m });
    try std.testing.expectEqualStrings("You have 5 new messages", result);
}

test "render empty context" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    const result = try renderSimple(arena, "Static text without variables", .{ .map = m });
    try std.testing.expectEqualStrings("Static text without variables", result);
}

test "render missing variable" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    const result = try renderSimple(arena, "Hello, {{name}}!", .{ .map = m });
    try std.testing.expectEqualStrings("Hello, !", result);
}

test "render multiline" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("Customer"));
    try m.put(arena, "orderID", Value.str("12345"));
    try m.put(arena, "total", Value.str("99.99"));

    const tmpl =
        \\Dear {{name}},
        \\
        \\Thank you for your order #{{orderID}}.
        \\Your total is: ${{total}}
    ;
    const expected =
        \\Dear Customer,
        \\
        \\Thank you for your order #12345.
        \\Your total is: $99.99
    ;
    const result = try renderSimple(arena, tmpl, .{ .map = m });
    try std.testing.expectEqualStrings(expected, result);
}

test "render unless" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "logged_in", Value.bln(false));

    const result = try renderSimple(arena, "{{#unless logged_in}}Please log in{{/unless}}", .{ .map = m });
    try std.testing.expectEqualStrings("Please log in", result);
}

test "render each with @index" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("first"), Value.str("second"), Value.str("third") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const result = try renderSimple(arena, "{{#each items}}{{@index}}: {{this}} {{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("0: first 1: second 2: third ", result);
}

test "render with sub-expression helper" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const eq = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const a = try ctx.param(0).toString(ctx.arena);
            const b = try ctx.param(1).toString(ctx.arena);
            return Value.bln(std.mem.eql(u8, a, b));
        }
    }.f;

    var helpers: HelperMap = .{};
    try helpers.put(arena, "eq", Helper.from(eq));
    const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};

    var m: ValueMap = .{};
    try m.put(arena, "status", Value.str("active"));

    const result = try render(arena, "{{#if (eq status \"active\")}}Active!{{else}}Inactive{{/if}}", .{ .map = m }, &helpers, &partials);
    try std.testing.expectEqualStrings("Active!", result);
}

test "Template struct" {
    var tpl = try Template.init(std.testing.allocator, "Hello, {{name}}!");
    defer tpl.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const result = try tpl.renderSimple(arena, .{ .map = m });
    try std.testing.expectEqualStrings("Hello, World!", result);
}

test "extract field paths" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const paths = try extractFieldPaths(arena, "{{title}} by {{author.name}}");
    try std.testing.expectEqual(2, paths.len);
    try std.testing.expectEqualStrings("title", paths[0][0]);
    try std.testing.expectEqual(2, paths[1].len);
    try std.testing.expectEqualStrings("author", paths[1][0]);
    try std.testing.expectEqualStrings("name", paths[1][1]);
}

test "is field referenced" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    try std.testing.expect(try isFieldReferenced(arena, "{{author.name}}", &.{ "author", "name" }));
    try std.testing.expect(!try isFieldReferenced(arena, "{{title}}", &.{ "author", "name" }));
}

test "normalize field path" {
    const path1 = normalizeFieldPath(&.{ "this", "author", "name" });
    try std.testing.expectEqual(2, path1.len);
    try std.testing.expectEqualStrings("author", path1[0]);

    const path2 = normalizeFieldPath(&.{ "fields", "title" });
    try std.testing.expectEqual(1, path2.len);
    try std.testing.expectEqualStrings("title", path2[0]);
}

test "render else if" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "role", Value.str("editor"));

    const eq = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const a = try ctx.param(0).toString(ctx.arena);
            const b = try ctx.param(1).toString(ctx.arena);
            return Value.bln(std.mem.eql(u8, a, b));
        }
    }.f;

    var helpers: HelperMap = .{};
    try helpers.put(arena, "eq", Helper.from(eq));
    const partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};

    const result = try render(
        arena,
        "{{#if (eq role \"admin\")}}Admin{{else if (eq role \"editor\")}}Editor{{else}}User{{/if}}",
        .{ .map = m },
        &helpers,
        &partials,
    );
    try std.testing.expectEqualStrings("Editor", result);
}

test "render with partial" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var user: ValueMap = .{};
    try user.put(arena, "name", Value.str("Alice"));
    try user.put(arena, "email", Value.str("alice@example.com"));
    var m: ValueMap = .{};
    try m.put(arena, "user", .{ .map = user });

    const helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "userCard", "{{user.name}} <{{user.email}}>");

    const result = try render(arena, "Contact: {{> userCard}}", .{ .map = m }, &helpers, &partials);
    try std.testing.expectEqualStrings("Contact: Alice <alice@example.com>", result);
}

test "render ampersand unescaped" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "content", Value.str("<p>Hello & welcome</p>"));

    const result = try renderSimple(arena, "{{&content}}", .{ .map = m });
    try std.testing.expectEqualStrings("<p>Hello & welcome</p>", result);
}

test "render equal helper" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "lang", Value.str("en"));

    const result = try renderSimple(arena, "{{#equal lang \"en\"}}English{{else}}Other{{/equal}}", .{ .map = m });
    try std.testing.expectEqualStrings("English", result);
}

test "render each nested objects" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var user_a: ValueMap = .{};
    try user_a.put(arena, "Name", Value.str("Alice"));
    try user_a.put(arena, "Email", Value.str("alice@example.com"));
    try user_a.put(arena, "Age", Value.int(30));

    var user_b: ValueMap = .{};
    try user_b.put(arena, "Name", Value.str("Bob"));
    try user_b.put(arena, "Email", Value.str("bob@example.com"));
    try user_b.put(arena, "Age", Value.int(25));

    const users = [_]Value{ .{ .map = user_a }, .{ .map = user_b } };
    var config: ValueMap = .{};
    try config.put(arena, "debug", Value.bln(true));
    try config.put(arena, "version", Value.str("1.2.3"));

    var m: ValueMap = .{};
    try m.put(arena, "users", .{ .array = &users });
    try m.put(arena, "config", .{ .map = config });

    const tmpl =
        \\Users:
        \\{{#each users}}- {{Name}} ({{Email}}, age: {{Age}})
        \\{{/each}}Debug mode: {{config.debug}}
        \\Version: {{config.version}}
    ;
    const expected =
        \\Users:
        \\- Alice (alice@example.com, age: 30)
        \\- Bob (bob@example.com, age: 25)
        \\Debug mode: true
        \\Version: 1.2.3
    ;
    const result = try renderSimple(arena, tmpl, .{ .map = m });
    try std.testing.expectEqualStrings(expected, result);
}
