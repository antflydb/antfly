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

//! Native grammar index with bounded token fallback for other languages;
//! declarations retain ranges, names, nesting and exact call locations.
const std = @import("std");
const A = std.mem.Allocator;
pub const Declaration = struct { name: []const u8, kind: []const u8, start: usize, end: usize, label_line: usize, start_byte: usize, end_byte: usize, parent: ?usize = null };
pub const Call = struct { kind: []const u8 = "call", name: []const u8, line: usize, column: usize, start_byte: usize, end_byte: usize };
pub const Index = struct {
    declarations: []Declaration,
    calls: []Call,
    complete: bool,
    pub fn deinit(self: *Index, a: A) void {
        a.free(self.declarations);
        a.free(self.calls);
    }
};
// The compiler AST is authoritative for Zig. Token offsets are projected
// back into the caller's source, so no returned slice borrows the temporary AST.
fn parseZig(a: A, bytes: []const u8) !Index {
    const source = try a.dupeSentinel(u8, bytes, 0);
    defer a.free(source);
    var tree = try std.zig.Ast.parse(a, source, .{});
    defer tree.deinit(a);
    if (tree.errors.len != 0) return .{ .declarations = &.{}, .calls = &.{}, .complete = false };
    var starts: std.ArrayList(usize) = .empty;
    defer starts.deinit(a);
    try starts.append(a, 0);
    for (bytes, 0..) |ch, i| if (ch == '\n') {
        try starts.append(a, i + 1);
    };
    var declarations: std.ArrayList(Declaration) = .empty;
    errdefer declarations.deinit(a);
    var calls: std.ArrayList(Call) = .empty;
    errdefer calls.deinit(a);
    const offsets = tree.tokens.items(.start);
    const owned_prototypes = try a.alloc(bool, tree.nodes.len);
    defer a.free(owned_prototypes);
    @memset(owned_prototypes, false);
    for (1..tree.nodes.len) |i| {
        const node: std.zig.Ast.Node.Index = @fromBackingInt(@intCast(i));
        if (tree.nodeTag(node) == .fn_decl) owned_prototypes[@backingInt(tree.nodeData(node).node_and_node[0])] = true;
    }
    for (1..tree.nodes.len) |i| {
        const node: std.zig.Ast.Node.Index = @fromBackingInt(@intCast(i));
        var proto_buffer: [1]std.zig.Ast.Node.Index = undefined;
        var container_buffer: [2]std.zig.Ast.Node.Index = undefined;
        var name_token: ?std.zig.Ast.TokenIndex = null;
        var kind: []const u8 = "function";
        var first = tree.firstToken(node);
        const last = tree.lastToken(node);
        if (tree.fullFnProto(&proto_buffer, node)) |proto| {
            if (owned_prototypes[i]) continue;
            name_token = proto.name_token;
            first = proto.firstToken();
        } else if (tree.fullVarDecl(node)) |variable| {
            const init = variable.ast.init_node.unwrap() orelse continue;
            const container = tree.fullContainerDecl(&container_buffer, init) orelse continue;
            kind = tree.tokenSlice(container.ast.main_token);
            name_token = variable.ast.mut_token + 1;
            first = variable.firstToken();
        } else if (tree.fullCall(&proto_buffer, node)) |call| {
            const callee = call.ast.fn_expr;
            const token = tree.nodeMainToken(callee);
            const tag = tree.nodeTag(callee);
            if (tag != .identifier and tag != .field_access) continue;
            const selected = if (tag == .field_access) token + 1 else token;
            const start = offsets[tree.firstToken(callee)];
            const name_end = offsets[selected] + tree.tokenSlice(selected).len;
            const loc = sourceLocation(starts.items, start);
            const call_end = offsets[last] + tree.tokenSlice(last).len;
            try calls.append(a, .{ .name = bytes[start..name_end], .line = loc.line, .column = loc.column, .start_byte = start, .end_byte = call_end });
            continue;
        } else continue;
        const token = name_token orelse continue;
        const name_start = offsets[token];
        const name_len = tree.tokenSlice(token).len;
        const begin = offsets[first];
        const end = offsets[last] + tree.tokenSlice(last).len;
        const name = bytes[name_start..][0..name_len];
        try declarations.append(a, .{ .name = name, .kind = kind, .start = sourceLocation(starts.items, begin).line, .end = sourceLocation(starts.items, end -| 1).line, .label_line = sourceLocation(starts.items, name_start).line, .start_byte = begin, .end_byte = end });
    }
    try assignParents(a, declarations.items);
    const owned_declarations = try declarations.toOwnedSlice(a);
    errdefer a.free(owned_declarations);
    return .{ .declarations = owned_declarations, .calls = try calls.toOwnedSlice(a), .complete = true };
}
fn sourceLocation(starts: []const usize, offset: usize) struct { line: usize, column: usize } {
    var lo: usize = 0;
    var hi = starts.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (starts[mid] <= offset) lo = mid + 1 else hi = mid;
    }
    const row = lo - 1;
    return .{ .line = row + 1, .column = offset - starts[row] + 1 };
}

test "compiler AST handles Zig escaped names, containers and calls" {
    const a = std.testing.allocator;
    const code = "const Thing = struct {\n    pub fn @\"a strange name\"() void {\n        other.run();\n        _ = @as(u8, 1);\n    }\n};\n";
    var index = try parse(a, "thing.zig", code);
    defer index.deinit(a);
    try std.testing.expect(index.complete);
    try std.testing.expectEqual(@as(usize, 2), index.declarations.len);
    try std.testing.expectEqualStrings("Thing", index.declarations[0].name);
    try std.testing.expectEqualStrings("@\"a strange name\"", index.declarations[1].name);
    try std.testing.expectEqual(@as(?usize, 0), index.declarations[1].parent);
    try std.testing.expectEqual(@as(usize, 1), index.calls.len);
    try std.testing.expectEqualStrings("other.run", index.calls[0].name);
    try std.testing.expectEqualStrings("other.run()", code[index.calls[0].start_byte..index.calls[0].end_byte]);
    try std.testing.expectEqual(@as(usize, 3), index.calls[0].line);
}

// Grammar-backed JS/TS analysis. Returned names borrow the caller's source;
// the temporary parser arena and syntax tree never escape this function.
fn parseScript(a: A, path: []const u8, bytes: []const u8) !Index {
    const syntax = @import("antfly_syntax");
    const ast = syntax.ast;
    if (bytes.len > std.math.maxInt(u32)) return .{ .declarations = &.{}, .calls = &.{}, .complete = false };
    const declaration_file = std.mem.endsWith(u8, path, ".d.ts") or std.mem.endsWith(u8, path, ".d.mts") or std.mem.endsWith(u8, path, ".d.cts");
    const ts = std.mem.endsWith(u8, path, ".ts") or std.mem.endsWith(u8, path, ".mts") or std.mem.endsWith(u8, path, ".cts");
    const lang: ast.Lang = if (declaration_file) .dts else if (std.mem.endsWith(u8, path, ".tsx")) .tsx else if (std.mem.endsWith(u8, path, ".jsx")) .jsx else if (ts) .ts else .js;
    const cjs = std.mem.endsWith(u8, path, ".cjs") or std.mem.endsWith(u8, path, ".cts");
    var tree = try syntax.parse(a, bytes, .{ .lang = lang, .source_type = if (cjs) .commonjs else .module, .comments = .none });
    defer tree.deinit();
    if (tree.hasErrors() and lang == .js and !cjs and !std.mem.endsWith(u8, path, ".mjs")) {
        var script = try syntax.parse(a, bytes, .{ .lang = lang, .source_type = .commonjs, .comments = .none });
        if (!script.hasErrors()) {
            tree.deinit();
            tree = script;
        } else script.deinit();
    }
    var starts: std.ArrayList(usize) = .empty;
    defer starts.deinit(a);
    try starts.append(a, 0);
    for (bytes, 0..) |ch, i| if (ch == '\n') {
        try starts.append(a, i + 1);
    };
    var declarations: std.ArrayList(Declaration) = .empty;
    errdefer declarations.deinit(a);
    var calls: std.ArrayList(Call) = .empty;
    errdefer calls.deinit(a);
    for (tree.nodes.items(.data), tree.nodes.items(.span)) |data, span| {
        if (span.end > bytes.len or span.start > span.end) continue;
        var name: ast.NodeIndex = .null;
        var kind: []const u8 = "function";
        var callee: ast.NodeIndex = .null;
        switch (data) {
            .function => |f| name = f.id,
            .class => |c| {
                name = c.id;
                kind = "class";
            },
            .method_definition => |m| {
                name = m.key;
                kind = "method";
            },
            .variable_declarator => |v| if (v.init != .null) {
                switch (tree.data(v.init)) {
                    .function, .arrow_function_expression => name = v.id,
                    .class => {
                        name = v.id;
                        kind = "class";
                    },
                    else => {},
                }
            },
            .object_property => |property| if (property.value != .null) {
                switch (tree.data(property.value)) {
                    .function, .arrow_function_expression => {
                        name = property.key;
                        kind = "method";
                    },
                    else => {},
                }
            },
            .property_definition => |property| if (property.value != .null) {
                switch (tree.data(property.value)) {
                    .function, .arrow_function_expression => {
                        name = property.key;
                        kind = "method";
                    },
                    else => {},
                }
            },
            .ts_interface_declaration => |d| {
                name = d.id;
                kind = "interface";
            },
            .ts_type_alias_declaration => |d| {
                name = d.id;
                kind = "type";
            },
            .ts_enum_declaration => |d| {
                name = d.id;
                kind = "enum";
            },
            .ts_module_declaration => |d| {
                name = d.id;
                kind = "namespace";
            },
            .call_expression => |call| callee = call.callee,
            .new_expression => |call| callee = call.callee,
            .tagged_template_expression => |call| callee = call.tag,
            else => {},
        }
        if (name != .null) {
            const named = tree.span(name);
            if (named.start > named.end or named.end > bytes.len) continue;
            try declarations.append(a, .{ .name = bytes[named.start..named.end], .kind = kind, .start = sourceLocation(starts.items, span.start).line, .end = sourceLocation(starts.items, span.end -| 1).line, .label_line = sourceLocation(starts.items, named.start).line, .start_byte = span.start, .end_byte = span.end });
        }
        if (callee != .null) {
            const named = tree.span(callee);
            if (named.start > named.end or named.end > bytes.len) continue;
            const loc = sourceLocation(starts.items, named.start);
            try calls.append(a, .{ .name = bytes[named.start..named.end], .line = loc.line, .column = loc.column, .start_byte = span.start, .end_byte = span.end });
        }
    }
    try assignParents(a, declarations.items);
    std.mem.sort(Call, calls.items, {}, struct {
        fn less(_: void, x: Call, y: Call) bool {
            return x.start_byte < y.start_byte or (x.start_byte == y.start_byte and x.end_byte > y.end_byte);
        }
    }.less);
    const decls = try declarations.toOwnedSlice(a);
    errdefer a.free(decls);
    return .{ .declarations = decls, .calls = try calls.toOwnedSlice(a), .complete = !tree.hasErrors() };
}

const Token = struct { text: []const u8, start: usize, end: usize, line: usize, column: usize, pair: ?usize = null, identifier: bool = false };
// Project the selected grammar tree, including calls inside interpolated strings,
// decorators, closures and nested macro token trees when represented as syntax.
fn parseGrammar(a: A, grammar: *const @import("antfly_syntax").grammar.Grammar, bytes: []const u8) !Index {
    const syntax = @import("antfly_syntax").grammar;
    var tree = syntax.parse(a, grammar, bytes) catch |err| switch (err) {
        error.ParseBudgetExceeded => return .{ .declarations = &.{}, .calls = &.{}, .complete = false },
        else => return err,
    };
    defer tree.deinit();
    const root = tree.root orelse return .{ .declarations = &.{}, .calls = &.{}, .complete = false };
    var starts: std.ArrayList(usize) = .empty;
    defer starts.deinit(a);
    try starts.append(a, 0);
    for (bytes, 0..) |ch, i| if (ch == '\n') {
        try starts.append(a, i + 1);
    };
    var declarations: std.ArrayList(Declaration) = .empty;
    errdefer declarations.deinit(a);
    var calls: std.ArrayList(Call) = .empty;
    errdefer calls.deinit(a);
    const Visit = struct { ref: syntax.Ref, decorated_start: ?usize = null };
    var pending: std.ArrayList(Visit) = .empty;
    defer pending.deinit(a);
    try pending.append(a, .{ .ref = root });
    while (pending.pop()) |visit| {
        const node = tree.node(visit.ref);
        const kind = tree.kind(visit.ref);
        const declaration_kind: ?[]const u8 = blk: {
            for ([_][]const u8{ "function_definition", "function_declaration", "function_item", "function_signature_item" }) |name| if (std.mem.eql(u8, kind, name)) break :blk "function";
            for ([_][]const u8{ "method_declaration", "method_elem" }) |name| if (std.mem.eql(u8, kind, name)) break :blk "method";
            for ([_][2][]const u8{ .{ "class_definition", "class" }, .{ "struct_item", "struct" }, .{ "enum_item", "enum" }, .{ "trait_item", "trait" }, .{ "impl_item", "impl" }, .{ "type_item", "type" }, .{ "type_spec", "type" }, .{ "type_alias", "type" }, .{ "mod_item", "module" }, .{ "macro_definition", "macro" } }) |pair| if (std.mem.eql(u8, kind, pair[0])) break :blk pair[1];
            break :blk null;
        };
        if (declaration_kind) |label| {
            if (tree.field(visit.ref, "name") orelse (if (std.mem.eql(u8, kind, "impl_item")) tree.field(visit.ref, "type") else null)) |name_ref| {
                const name_node = tree.node(name_ref);
                const begin = visit.decorated_start orelse node.start;
                if (name_node.end <= bytes.len and node.end <= bytes.len) try declarations.append(a, .{ .name = bytes[name_node.start..name_node.end], .kind = label, .start = sourceLocation(starts.items, begin).line, .end = sourceLocation(starts.items, node.end -| 1).line, .label_line = sourceLocation(starts.items, name_node.start).line, .start_byte = begin, .end_byte = node.end });
            }
        }
        if (std.mem.eql(u8, kind, "call") or std.mem.eql(u8, kind, "call_expression") or std.mem.eql(u8, kind, "macro_invocation") or std.mem.eql(u8, kind, "type_conversion_expression")) {
            if (tree.field(visit.ref, "function") orelse tree.field(visit.ref, "macro") orelse tree.field(visit.ref, "type")) |callee| {
                const name_node = tree.node(callee);
                const loc = sourceLocation(starts.items, name_node.start);
                if (name_node.end <= bytes.len and node.end <= bytes.len) try calls.append(a, .{ .kind = if (std.mem.eql(u8, kind, "macro_invocation")) "macro" else if (std.mem.eql(u8, kind, "type_conversion_expression")) "conversion" else "call", .name = bytes[name_node.start..name_node.end], .line = loc.line, .column = loc.column, .start_byte = node.start, .end_byte = node.end });
            }
        }
        var i = node.children.len;
        while (i > 0) {
            i -= 1;
            const child = tree.child(visit.ref, i);
            const decorated_start: ?usize = if (std.mem.eql(u8, kind, "decorated_definition") and tree.node(child).symbol >= grammar.tokens) node.start else null;
            try pending.append(a, .{ .ref = child, .decorated_start = decorated_start });
        }
    }
    try assignParents(a, declarations.items);
    const owned_declarations = try declarations.toOwnedSlice(a);
    errdefer a.free(owned_declarations);
    return .{ .declarations = owned_declarations, .calls = try calls.toOwnedSlice(a), .complete = tree.complete };
}

pub fn parse(a: A, path: []const u8, bytes: []const u8) !Index {
    if (std.mem.endsWith(u8, path, ".zig")) return parseZig(a, bytes);
    for ([_][]const u8{ ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs", ".mts", ".cts" }) |extension| if (std.mem.endsWith(u8, path, extension)) return parseScript(a, path, bytes);
    if (std.mem.endsWith(u8, path, ".go")) return parseGrammar(a, @import("antfly_syntax").go, bytes);
    if (std.mem.endsWith(u8, path, ".rs")) return parseGrammar(a, @import("antfly_syntax").rust, bytes);
    if (std.mem.endsWith(u8, path, ".py") or std.mem.endsWith(u8, path, ".pyi")) return parseGrammar(a, @import("antfly_syntax").python, bytes);
    const python = std.mem.endsWith(u8, path, ".py") or std.mem.endsWith(u8, path, ".pyi");
    const rust = std.mem.endsWith(u8, path, ".rs");
    const script = std.mem.endsWith(u8, path, ".js") or std.mem.endsWith(u8, path, ".jsx") or std.mem.endsWith(u8, path, ".ts") or std.mem.endsWith(u8, path, ".tsx") or std.mem.endsWith(u8, path, ".mjs") or std.mem.endsWith(u8, path, ".cjs");
    var template_text = false;
    var template_depths: std.ArrayList(usize) = .empty;
    var tokens: std.ArrayList(Token) = .empty;
    defer tokens.deinit(a);
    defer template_depths.deinit(a);
    var delimiters: std.ArrayList(usize) = .empty;
    defer delimiters.deinit(a);
    var complete = true;
    var i: usize = 0;
    var line: usize = 1;
    var column: usize = 1;
    while (i < bytes.len) {
        if (template_text) {
            while (i < bytes.len) {
                const ch = bytes[i];
                if (ch == '\\' and i + 1 < bytes.len) {
                    i += 2;
                    column += 2;
                    continue;
                }
                if (ch == '`') {
                    template_text = false;
                    i += 1;
                    column += 1;
                    break;
                }
                if (ch == '$' and i + 1 < bytes.len and bytes[i + 1] == '{') {
                    try template_depths.append(a, delimiters.items.len);
                    template_text = false;
                    i += 2;
                    column += 2;
                    break;
                }
                if (ch == '\n') {
                    line += 1;
                    column = 0;
                }
                i += 1;
                column += 1;
            }
            if (i == bytes.len and template_text) complete = false;
            continue;
        }
        if (script and bytes[i] == '`') {
            template_text = true;
            i += 1;
            column += 1;
            continue;
        }
        if (script and bytes[i] == '}' and template_depths.items.len > 0 and template_depths.items[template_depths.items.len - 1] == delimiters.items.len) {
            _ = template_depths.pop();
            template_text = true;
            i += 1;
            column += 1;
            continue;
        }
        const start = i;
        const row = line;
        const col = column;
        const ch = bytes[i];
        if (std.ascii.isWhitespace(ch)) {
            if (ch == '\n') {
                line += 1;
                column = 1;
            } else column += 1;
            i += 1;
            continue;
        }
        if ((python and ch == '#') or (!python and ch == '/' and i + 1 < bytes.len and bytes[i + 1] == '/')) {
            while (i < bytes.len and bytes[i] != '\n') i += 1;
            column += i - start;
            continue;
        }
        if (!python and ch == '/' and i + 1 < bytes.len and bytes[i + 1] == '*') {
            i += 2;
            var depth: usize = 1;
            while (i < bytes.len and depth > 0) {
                if (i + 1 < bytes.len and bytes[i] == '/' and bytes[i + 1] == '*') {
                    depth += 1;
                    i += 2;
                } else if (i + 1 < bytes.len and bytes[i] == '*' and bytes[i + 1] == '/') {
                    depth -= 1;
                    i += 2;
                } else {
                    if (bytes[i] == '\n') {
                        line += 1;
                        column = 0;
                    }
                    i += 1;
                    column += 1;
                }
            }
            if (depth != 0) complete = false;
            continue;
        }
        if (script and ch == '/' and expressionExpected(tokens.items)) {
            var end = i + 1;
            var in_class = false;
            while (end < bytes.len and bytes[end] != '\n') : (end += 1) {
                if (bytes[end] == '\\') {
                    end += 1;
                    continue;
                }
                if (bytes[end] == '[') in_class = true else if (bytes[end] == ']') in_class = false else if (bytes[end] == '/' and !in_class) break;
            }
            if (end < bytes.len and bytes[end] == '/') {
                end += 1;
                while (end < bytes.len and std.ascii.isAlphabetic(bytes[end])) end += 1;
                column += end - i;
                i = end;
                continue;
            }
        }
        // Rust raw strings have zero or more hashes around a quoted body.
        var hashes: usize = 0;
        if (rust and ch == 'r') {
            var q = i + 1;
            while (q < bytes.len and bytes[q] == '#') {
                q += 1;
                hashes += 1;
            }
            if (q >= bytes.len or bytes[q] != '"') hashes = 0 else if (hashes > 0) {
                i = q + 1;
                while (i < bytes.len) {
                    if (bytes[i] == '"' and i + hashes < bytes.len) {
                        var valid = true;
                        for (bytes[i + 1 ..][0..hashes]) |h| if (h != '#') {
                            valid = false;
                        };
                        if (valid) {
                            i += hashes + 1;
                            break;
                        }
                    }
                    if (bytes[i] == '\n') {
                        line += 1;
                        column = 0;
                    }
                    i += 1;
                    column += 1;
                }
                continue;
            }
        }
        if (ch == '"' or ch == '`' or (ch == '\'' and (!rust or rustCharacter(bytes, i)))) {
            const triple = python and i + 2 < bytes.len and bytes[i + 1] == ch and bytes[i + 2] == ch;
            i += if (triple) @as(usize, 3) else 1;
            var closed = false;
            while (i < bytes.len) {
                if (bytes[i] == '\\' and ch != '`') {
                    i += @min(2, bytes.len - i);
                    continue;
                }
                if (bytes[i] == ch) {
                    if (!triple) {
                        i += 1;
                        closed = true;
                        break;
                    }
                    if (i + 2 < bytes.len and bytes[i + 1] == ch and bytes[i + 2] == ch) {
                        i += 3;
                        closed = true;
                        break;
                    }
                }
                if (bytes[i] == '\n') {
                    line += 1;
                    column = 0;
                }
                i += 1;
                column += 1;
            }
            if (!closed) complete = false;
            column += 1;
            continue;
        }
        const identifier = identStart(ch);
        i += 1;
        if (identifier) while (i < bytes.len and identContinue(bytes[i])) {
            i += 1;
        };
        column += i - start;
        const n = tokens.items.len;
        try tokens.append(a, .{ .text = bytes[start..i], .start = start, .end = i, .line = row, .column = col, .identifier = identifier });
        if (std.mem.indexOfScalar(u8, "([{", ch) != null and !identifier) try delimiters.append(a, n) else if (std.mem.indexOfScalar(u8, ")]}", ch) != null and !identifier) {
            if (delimiters.pop()) |open| {
                const want: u8 = switch (tokens.items[open].text[0]) {
                    '(' => ')',
                    '[' => ']',
                    else => '}',
                };
                if (want != ch) complete = false else {
                    tokens.items[open].pair = n;
                    tokens.items[n].pair = open;
                }
            } else complete = false;
        }
    }
    if (delimiters.items.len > 0 or template_text or template_depths.items.len > 0) complete = false;
    var declarations: std.ArrayList(Declaration) = .empty;
    errdefer declarations.deinit(a);
    const declared = try a.alloc(bool, tokens.items.len);
    defer a.free(declared);
    @memset(declared, false);
    for (tokens.items, 0..) |token, k| {
        var name_index: ?usize = null;
        var body: ?usize = null;
        var expression: ?usize = null;
        var kind: []const u8 = "function";
        if (eq(token, "fn") or eq(token, "func") or eq(token, "def") or eq(token, "function")) {
            var next = k + 1;
            if (next < tokens.items.len and eq(tokens.items[next], "*")) next += 1;
            if (next < tokens.items.len and eq(token, "func") and eq(tokens.items[next], "(")) next = (tokens.items[next].pair orelse continue) + 1;
            if (next < tokens.items.len and tokens.items[next].identifier) name_index = next else continue;
        } else if (eq(token, "class") or eq(token, "struct") or eq(token, "enum") or eq(token, "interface") or eq(token, "trait") or eq(token, "impl") or eq(token, "type")) {
            kind = token.text;
            var next = k + 1;
            if (eq(token, "impl") and next < tokens.items.len and eq(tokens.items[next], "<")) {
                var depth: usize = 0;
                while (next < tokens.items.len) : (next += 1) {
                    if (eq(tokens.items[next], "<")) depth += 1 else if (eq(tokens.items[next], ">")) {
                        depth -= 1;
                        if (depth == 0) {
                            next += 1;
                            break;
                        }
                    }
                }
            }
            if (next < tokens.items.len and tokens.items[next].identifier) name_index = next else continue;
        } else if ((eq(token, "const") or eq(token, "let")) and k + 1 < tokens.items.len and tokens.items[k + 1].identifier) {
            // Arrow functions and Zig struct-valued declarations.
            var n = k + 2;
            while (n < tokens.items.len and tokens.items[n].line <= token.line + 8 and !eq(tokens.items[n], ";")) : (n += 1) {
                if (eq(tokens.items[n], "(") and tokens.items[n].pair != null) {
                    n = tokens.items[n].pair.?;
                    continue;
                }
                if (eq(tokens.items[n], "=") and n + 1 < tokens.items.len and eq(tokens.items[n + 1], ">")) {
                    name_index = k + 1;
                    if (script and n + 2 < tokens.items.len and !eq(tokens.items[n + 2], "{")) expression = n + 2;
                    break;
                }
                if (eq(tokens.items[n], "struct") or eq(tokens.items[n], "enum") or eq(tokens.items[n], "union")) {
                    name_index = k + 1;
                    kind = tokens.items[n].text;
                    break;
                }
                if (eq(tokens.items[n], "{")) break;
            }
        } else if (!python and !declared[k] and token.identifier and !control(token.text)) {
            if (parametersAfter(tokens.items, k + 1, script)) |parameters| {
                const close = tokens.items[parameters].pair.?;
                var n = close + 1;
                if (script and n < tokens.items.len and eq(tokens.items[n], ":")) {
                    n += 1;
                    while (n < tokens.items.len and tokens.items[n].line <= token.line + 8 and !eq(tokens.items[n], ";")) : (n += 1) {
                        if (eq(tokens.items[n], "{")) {
                            // Object return types can precede the actual body.
                            const type_end = tokens.items[n].pair orelse break;
                            if (type_end + 1 < tokens.items.len and eq(tokens.items[type_end + 1], "{")) n = type_end + 1;
                            break;
                        }
                        if (tokens.items[n].pair) |pair| if (pair > n) {
                            n = pair;
                        };
                    }
                }
                if (n < tokens.items.len and eq(tokens.items[n], "{")) {
                    name_index = k;
                    body = n;
                    kind = "method";
                }
            }
        }
        const ni = name_index orelse continue;
        var declaration_start = token.line;
        var declaration_byte = token.start;
        var end: usize = token.line;
        var end_byte = token.end;
        if (python) {
            // Ignore newlines nested inside parameters/type expressions.
            var n = ni + 1;
            while (n < tokens.items.len and !eq(tokens.items[n], ":")) : (n += 1) if (tokens.items[n].pair) |close| {
                if (close > n) n = close;
            };
            if (n == tokens.items.len) continue;
            const colon = n;
            const indent = indentAt(bytes, token.start);
            var previous = k;
            while (previous > 0 and tokens.items[previous - 1].line == token.line) previous -= 1;
            declaration_byte = tokens.items[previous].start;
            while (previous > 0) {
                var prior = previous - 1;
                if (tokens.items[prior].pair) |open| if (open < prior) {
                    prior = open;
                };
                while (prior > 0 and tokens.items[prior - 1].line == tokens.items[prior].line) prior -= 1;
                if (!eq(tokens.items[prior], "@") or indentAt(bytes, tokens.items[prior].start) != indent) break;
                declaration_start = tokens.items[prior].line;
                declaration_byte = tokens.items[prior].start;
                previous = prior;
            }
            n += 1;
            while (n < tokens.items.len) : (n += 1) {
                if (tokens.items[n].line > tokens.items[colon].line and indentAt(bytes, tokens.items[n].start) <= indent) break;
                if (tokens.items[n].pair) |close| {
                    if (close > n) n = close;
                }
                end = tokens.items[n].line;
                end_byte = tokens.items[n].end;
            }
        } else if (expression) |first| {
            var n = first;
            while (n < tokens.items.len) : (n += 1) {
                if (eq(tokens.items[n], ";") or eq(tokens.items[n], ",")) break;
                if (tokens.items[n].pair) |close| if (close > n) {
                    n = close;
                };
                end = tokens.items[n].line;
                end_byte = tokens.items[n].end;
                if (n + 1 < tokens.items.len and tokens.items[n + 1].line > end and !continuesExpression(tokens.items[n].text) and !continuesExpression(tokens.items[n + 1].text)) break;
            }
        } else {
            if (body == null) {
                var n = ni + 1;
                while (n < tokens.items.len and tokens.items[n].line <= token.line + 32) : (n += 1) {
                    if (eq(tokens.items[n], ";")) break;
                    if (eq(tokens.items[n], "{")) {
                        body = n;
                        break;
                    }
                    if (tokens.items[n].pair) |close| if (close > n) {
                        n = close;
                    };
                }
            }
            const open = body orelse continue;
            const close = tokens.items[open].pair orelse continue;
            end = tokens.items[close].line;
            end_byte = tokens.items[close].end;
        }
        if (declared[ni]) continue;
        declared[ni] = true;
        try declarations.append(a, .{ .name = tokens.items[ni].text, .kind = kind, .start = declaration_start, .end = end, .label_line = token.line, .start_byte = declaration_byte, .end_byte = end_byte });
    }
    try assignParents(a, declarations.items);
    var calls: std.ArrayList(Call) = .empty;
    errdefer calls.deinit(a);
    for (tokens.items, 0..) |token, k| {
        if (!token.identifier or declared[k] or control(token.text)) continue;
        const parameters = parametersAfter(tokens.items, k + 1, script) orelse continue;
        const close = tokens.items[parameters].pair orelse continue;
        var start = k;
        while (start >= 2 and (eq(tokens.items[start - 1], ".") or eq(tokens.items[start - 1], ":")) and tokens.items[start - 2].identifier) start -= 2;
        try calls.append(a, .{ .name = bytes[tokens.items[start].start..token.end], .line = token.line, .column = tokens.items[start].column, .start_byte = tokens.items[start].start, .end_byte = tokens.items[close].end });
    }
    const decls = try declarations.toOwnedSlice(a);
    errdefer a.free(decls);
    return .{ .declarations = decls, .calls = try calls.toOwnedSlice(a), .complete = complete };
}
// Declaration order is canonical across parsers. A stack of enclosing byte
// ranges avoids comparing every pair, and handles same-line nested functions.
fn assignParents(a: A, declarations: []Declaration) !void {
    std.mem.sort(Declaration, declarations, {}, struct {
        fn less(_: void, x: Declaration, y: Declaration) bool {
            return x.start_byte < y.start_byte or (x.start_byte == y.start_byte and x.end_byte > y.end_byte);
        }
    }.less);
    var parents: std.ArrayList(usize) = .empty;
    defer parents.deinit(a);
    for (declarations, 0..) |*decl, i| {
        decl.parent = null;
        while (parents.items.len > 0) {
            const parent = declarations[parents.items[parents.items.len - 1]];
            if (parent.end_byte >= decl.end_byte and parent.start_byte <= decl.start_byte and (parent.start_byte < decl.start_byte or parent.end_byte > decl.end_byte)) break;
            _ = parents.pop();
        }
        if (parents.items.len > 0) decl.parent = parents.items[parents.items.len - 1];
        try parents.append(a, i);
    }
}
fn parametersAfter(tokens: []const Token, first: usize, generic: bool) ?usize {
    var at = first;
    if (generic and at < tokens.len and eq(tokens[at], "<")) {
        var depth: usize = 0;
        while (at < tokens.len and at - first < 256) : (at += 1) {
            if (eq(tokens[at], "<")) depth += 1 else if (eq(tokens[at], ">")) {
                depth -= 1;
                if (depth == 0) {
                    at += 1;
                    break;
                }
            }
        }
    }
    return if (at < tokens.len and eq(tokens[at], "(") and tokens[at].pair != null) at else null;
}
fn continuesExpression(text: []const u8) bool {
    for ([_][]const u8{ ".", "?", ":", "+", "-", "*", "/", "=", "&", "|", "," }) |operator| if (std.mem.eql(u8, text, operator)) return true;
    return false;
}
fn indentAt(bytes: []const u8, offset: usize) usize {
    var start = offset;
    while (start > 0 and bytes[start - 1] != '\n') start -= 1;
    var indent: usize = 0;
    while (start < bytes.len) : (start += 1) {
        if (bytes[start] == ' ') indent += 1 else if (bytes[start] == '\t') indent = (indent / 8 + 1) * 8 else break;
    }
    return indent;
}

fn expressionExpected(tokens: []const Token) bool {
    if (tokens.len == 0) return true;
    const last = tokens[tokens.len - 1];
    for ([_][]const u8{ "=", "(", "[", ":", ",", "!", "&", "|", "?", "return", "yield", "throw", "case" }) |text| if (eq(last, text)) return true;
    return tokens.len >= 2 and eq(last, ">") and eq(tokens[tokens.len - 2], "=");
}
fn rustCharacter(bytes: []const u8, start: usize) bool {
    var i = start + 1;
    while (i < @min(bytes.len, start + 16)) : (i += 1) {
        if (bytes[i] == '\'') return true;
        if (std.ascii.isWhitespace(bytes[i])) return false;
        if (bytes[i] == '\\' and i + 1 < bytes.len) i += 1;
    }
    return false;
}

fn identStart(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or ch == '_' or ch == '$' or ch >= 128;
}
fn identContinue(ch: u8) bool {
    return identStart(ch) or std.ascii.isDigit(ch);
}
fn eq(token: Token, text: []const u8) bool {
    return std.mem.eql(u8, token.text, text);
}
fn control(text: []const u8) bool {
    for ([_][]const u8{ "if", "for", "while", "switch", "catch", "with", "match", "return", "sizeof", "typeof", "synchronized", "assert", "fn", "func", "function", "def", "class", "new" }) |word| if (std.mem.eql(u8, text, word)) return true;
    return false;
}

test "Python async decorators and continued expressions preserve declaration ranges" {
    const text = "@decorate(\n    flag=True\n)\nasync def retry(\n    count: int,\n):\n    value = (\n        STOP(count)\n)\n    return value\n\ndef next():\n    pass\n";
    var index = try parse(std.testing.allocator, "a.py", text);
    defer index.deinit(std.testing.allocator);
    try std.testing.expect(index.complete);
    try std.testing.expectEqual(@as(usize, 2), index.declarations.len);
    try std.testing.expectEqual(@as(usize, 1), index.declarations[0].start);
    try std.testing.expectEqual(@as(usize, 4), index.declarations[0].label_line);
    try std.testing.expectEqual(@as(usize, 10), index.declarations[0].end);
    try std.testing.expectEqualStrings("STOP", index.calls[1].name);
}
test "JavaScript regex and template interpolation cannot corrupt body delimiters" {
    const text = "class Worker {\n retry(arg) {\n  const pattern = /[}]/g;\n  const msg = `hello ${arg.name()}`;\n  STOP(msg);\n }\n}\n";
    var index = try parse(std.testing.allocator, "a.ts", text);
    defer index.deinit(std.testing.allocator);
    try std.testing.expect(index.complete);
    try std.testing.expectEqual(@as(usize, 2), index.declarations.len);
    try std.testing.expectEqual(@as(usize, 7), index.declarations[0].end);
    try std.testing.expectEqual(@as(usize, 6), index.declarations[1].end);
    try std.testing.expectEqualStrings("arg.name", index.calls[0].name);
    try std.testing.expectEqualStrings("STOP", index.calls[1].name);
}

test "TypeScript generic methods and expression arrows retain source boundaries" {
    const a = std.testing.allocator;
    const text = "class Retry {\n  run<T>(arg: T): Promise<T> {\n    return stop<T>(arg);\n  }\n}\nconst adapt = (x: Value) =>\n  convert<Value>(x);\nconst other = 1;\n";
    var index = try parse(a, "retry.ts", text);
    defer index.deinit(a);
    try std.testing.expect(index.complete);
    var methods: usize = 0;
    var arrows: usize = 0;
    for (index.declarations) |declaration| {
        if (std.mem.eql(u8, declaration.name, "run")) {
            try std.testing.expectEqual(@as(usize, 4), declaration.end);
            methods += 1;
        }
        if (std.mem.eql(u8, declaration.name, "adapt")) {
            try std.testing.expectEqual(@as(usize, 7), declaration.end);
            arrows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), methods);
    try std.testing.expectEqual(@as(usize, 1), arrows);
    try std.testing.expectEqual(@as(usize, 2), index.calls.len);
}

test "native AST records extern prototypes without duplicating function headers" {
    const a = std.testing.allocator;
    const code = "extern fn remote(value: u8) void;\npub fn local() void { remote(1); }\n";
    var index = try parse(a, "a.zig", code);
    defer index.deinit(a);
    try std.testing.expect(index.complete);
    try std.testing.expectEqual(@as(usize, 2), index.declarations.len);
    try std.testing.expectEqualStrings("remote", index.declarations[0].name);
    try std.testing.expectEqualStrings("local", index.declarations[1].name);
    try std.testing.expectEqualStrings("remote(1)", code[index.calls[0].start_byte..index.calls[0].end_byte]);
}

test "native TypeScript grammar indexes mapped types decorators optional calls and TSX" {
    const a = std.testing.allocator;
    const code =
        \\interface Box<T> { value: T }
        \\type Wrapped<T> = { [K in keyof T]?: T[K] };
        \\@decorate
        \\class Worker<T> {
        \\  #state!: T;
        \\  constructor(public value: T) {}
        \\  async retry<U extends T>(value: U): Promise<U> {
        \\    return await client?.run<U>(value);
        \\  }
        \\}
        \\const render = <T,>(value: T) => <div>{show(value)}</div>;
    ;
    var index = try parse(a, "fixture.tsx", code);
    defer index.deinit(a);
    try std.testing.expect(index.complete);
    for ([_][]const u8{ "Box", "Wrapped", "Worker", "constructor", "retry", "render" }) |name| {
        var found = false;
        for (index.declarations) |decl| if (std.mem.eql(u8, decl.name, name)) {
            found = true;
            break;
        };
        try std.testing.expect(found);
    }
    try std.testing.expectEqual(@as(usize, 2), index.calls.len);
    try std.testing.expectEqualStrings("client?.run", index.calls[0].name);
    try std.testing.expectEqualStrings("client?.run<U>(value)", code[index.calls[0].start_byte..index.calls[0].end_byte]);
    try std.testing.expectEqualStrings("show", index.calls[1].name);
}

test "native script grammar keeps overloads and supports CommonJS top-level return" {
    const a = std.testing.allocator;
    const code = "declare function remote<T>(value: T): T;\nclass Service { run(value: string): string; run(value: number): number; run(value: unknown): unknown { return remote(value); } }\n";
    var index = try parse(a, "a.ts", code);
    defer index.deinit(a);
    try std.testing.expect(index.complete);
    try std.testing.expectEqual(@as(usize, 5), index.declarations.len);
    try std.testing.expectEqualStrings("remote", index.calls[0].name);
    var cjs = try parse(a, "a.cjs", "return factory();\n");
    defer cjs.deinit(a);
    try std.testing.expect(cjs.complete);
    try std.testing.expectEqualStrings("factory", cjs.calls[0].name);
}

test "native Python Go Rust grammars preserve declarations and calls across full syntax" {
    const cases = [_]struct { path: []const u8, source: []const u8, names: []const []const u8, calls: []const []const u8 }{
        .{ .path = "a.py", .source = "@decorate(flag=True)\nclass Worker[T]:\n    async def retry[U](self, value: U):\n        match value:\n            case {'key': item} if guard(item):\n                return f'{STOP(item)=:{width()}}'\n", .names = &.{ "Worker", "retry" }, .calls = &.{ "decorate", "guard", "STOP", "width" } },
        .{ .path = "a.go", .source = "package p\ntype Pair[T any] struct { Value T }\nfunc (p *Pair[T]) Retry(v T) T { f := func() { STOP(v) }; f(); return p.Value }\nfunc call() { p.Retry(instantiate[int](1)) }\n", .names = &.{ "Pair", "Retry", "call" }, .calls = &.{ "STOP", "f", "p.Retry", "instantiate[int]" } },
        .{ .path = "a.rs", .source = "#[derive(Debug)]\nstruct Worker<T> { value: T }\nimpl<T: Copy> Worker<T> {\n    pub async fn retry<'a>(&'a self) -> T {\n        let raw = r###\"fn fake() { bad() }\"###;\n        /* nested /* comment */ STOP(fake) */\n        let f = || STOP(self.value);\n        f()\n    }\n}\n", .names = &.{ "Worker", "Worker<T>", "retry" }, .calls = &.{ "STOP", "f" } },
    };
    for (cases) |case| {
        var index = try parse(std.testing.allocator, case.path, case.source);
        defer index.deinit(std.testing.allocator);
        try std.testing.expect(index.complete);
        try std.testing.expectEqual(case.names.len, index.declarations.len);
        for (case.names, index.declarations) |expected, declaration| try std.testing.expectEqualStrings(expected, declaration.name);
        try std.testing.expectEqual(case.calls.len, index.calls.len);
        for (case.calls, index.calls) |expected, call| {
            try std.testing.expectEqualStrings(expected, call.name);
            try std.testing.expect(std.mem.indexOf(u8, case.source[call.start_byte..call.end_byte], expected) != null);
        }
    }
}
