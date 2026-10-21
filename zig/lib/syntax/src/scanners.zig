// Native Zig adaptation of the upstream Python and Rust external scanners.
// tree-sitter-python 0.25.0
// The MIT License (MIT)
//
// Copyright (c) 2016 Max Brunsfeld
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// tree-sitter-rust 0.24.2
// The MIT License (MIT)
//
// Copyright (c) 2017 Maxim Sokolov
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

const std = @import("std");
const Lexer = @import("grammar.zig").Lexer;
pub const Kind = enum { none, python, rust };
pub const State = struct { indent: u32 = 0, delimiter: ?u32 = null, interpolated: bool = false, hashes: u16 = 0 };
const Link = struct { previous: ?u32, value: u16 };
pub const Store = struct {
    a: std.mem.Allocator,
    indents: std.ArrayList(Link) = .empty,
    delimiters: std.ArrayList(Link) = .empty,
    interned: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    pub fn init(a: std.mem.Allocator) !Store {
        var result = Store{ .a = a };
        try result.indents.append(a, .{ .previous = null, .value = 0 });
        return result;
    }
    fn push(self: *Store, list: *std.ArrayList(Link), previous: ?u32, value: u16) !u32 {
        const key = (@as(u64, previous orelse std.math.maxInt(u32)) << 16) | value | (if (list == &self.delimiters) @as(u64, 1) << 63 else 0);
        const entry = try self.interned.getOrPut(self.a, key);
        if (entry.found_existing) return entry.value_ptr.*;
        const index: u32 = @intCast(list.items.len);
        try list.append(self.a, .{ .previous = previous, .value = value });
        entry.value_ptr.* = index;
        return index;
    }
};
fn valid(mask: u32, token: u5) bool {
    return mask & (@as(u32, 1) << token) != 0;
}
fn cp(lex: *const Lexer) u32 {
    return lex.codepoint();
}
fn step(lex: *Lexer) void {
    lex.advance(false);
}
fn skip(lex: *Lexer) void {
    lex.advance(true);
}
fn mark(lex: *Lexer) void {
    lex.mark = lex.at;
}
fn eof(lex: *const Lexer) bool {
    return lex.at == lex.bytes.len;
}
const single: u16 = 1;
const double: u16 = 2;
const back: u16 = 4;
const raw: u16 = 8;
const format: u16 = 16;
const triple: u16 = 32;
const bytes_flag: u16 = 64;
fn endChar(flags: u16) u32 {
    return if (flags & single != 0) '\'' else if (flags & double != 0) '"' else if (flags & back != 0) '`' else 0;
}
pub fn scan(kind: Kind, store: *Store, state: *State, lex: *Lexer, mask: u32) !?u16 {
    return switch (kind) {
        .none => null,
        .python => python(store, state, lex, mask),
        .rust => rust(state, lex, mask),
    };
}
fn python(store: *Store, state: *State, lex: *Lexer, mask: u32) !?u16 {
    const recovery = valid(mask, 4) and valid(mask, 1);
    const brackets = valid(mask, 8) or valid(mask, 9) or valid(mask, 10);
    var advanced = false;
    if (valid(mask, 5) and state.delimiter != null and (cp(lex) == '{' or cp(lex) == '}') and !recovery) {
        const flags = store.delimiters.items[state.delimiter.?].value;
        if (flags & format != 0) {
            mark(lex);
            const left = cp(lex) == '{';
            step(lex);
            advanced = true;
            if ((cp(lex) == '{' and left) or (cp(lex) == '}' and !left)) {
                step(lex);
                mark(lex);
                return 5;
            }
            return null;
        }
    }
    if (valid(mask, 4) and state.delimiter != null and !recovery) {
        const delimiter = store.delimiters.items[state.delimiter.?];
        const flags = delimiter.value;
        const end = endChar(flags);
        var content = advanced;
        while (cp(lex) != 0) {
            if ((advanced or cp(lex) == '{' or cp(lex) == '}') and flags & format != 0) {
                mark(lex);
                return if (content) 4 else null;
            }
            if (cp(lex) == '\\') {
                if (flags & raw != 0) {
                    step(lex);
                    if (cp(lex) == end or cp(lex) == '\\') step(lex);
                    if (cp(lex) == '\r') {
                        step(lex);
                        if (cp(lex) == '\n') step(lex);
                    } else if (cp(lex) == '\n') step(lex);
                    continue;
                }
                if (flags & bytes_flag != 0) {
                    mark(lex);
                    step(lex);
                    if (cp(lex) == 'N' or cp(lex) == 'u' or cp(lex) == 'U') {
                        step(lex);
                    } else return if (content) 4 else null;
                } else {
                    mark(lex);
                    return if (content) 4 else null;
                }
            } else if (cp(lex) == end) {
                if (flags & triple != 0) {
                    mark(lex);
                    step(lex);
                    if (cp(lex) == end) {
                        step(lex);
                        if (cp(lex) == end) {
                            if (content) return 4;
                            step(lex);
                            mark(lex);
                            state.delimiter = delimiter.previous;
                            state.interpolated = false;
                            return 6;
                        }
                        mark(lex);
                        return 4;
                    }
                    mark(lex);
                    return 4;
                }
                if (content) {
                    mark(lex);
                    return 4;
                }
                step(lex);
                state.delimiter = delimiter.previous;
                state.interpolated = false;
                mark(lex);
                return 6;
            } else if (cp(lex) == '\n' and content and flags & triple == 0) return null;
            step(lex);
            content = true;
        }
    }
    mark(lex);
    var newline = false;
    var indent: u16 = 0;
    var first_comment: i32 = -1;
    while (true) {
        switch (cp(lex)) {
            '\n' => {
                newline = true;
                indent = 0;
                skip(lex);
            },
            ' ' => {
                indent +|= 1;
                skip(lex);
            },
            '\r', '\x0c' => {
                indent = 0;
                skip(lex);
            },
            '\t' => {
                indent +|= 8;
                skip(lex);
            },
            '#' => {
                if (!(valid(mask, 1) or valid(mask, 2) or valid(mask, 0) or valid(mask, 11))) break;
                if (!newline) return null;
                if (first_comment == -1) first_comment = indent;
                while (cp(lex) != 0 and cp(lex) != '\n') skip(lex);
                skip(lex);
                indent = 0;
            },
            '\\' => {
                skip(lex);
                if (cp(lex) == '\r') skip(lex);
                if (cp(lex) == '\n' or eof(lex)) skip(lex) else return null;
            },
            else => {
                if (eof(lex)) {
                    indent = 0;
                    newline = true;
                }
                break;
            },
        }
    }
    if (newline) {
        const current = store.indents.items[state.indent];
        if (valid(mask, 1) and indent > current.value) {
            state.indent = try store.push(&store.indents, state.indent, indent);
            return 1;
        }
        const next_string = cp(lex) == '"' or cp(lex) == '\'' or cp(lex) == '`';
        if ((valid(mask, 2) or (!valid(mask, 0) and !(valid(mask, 3) and next_string) and !brackets)) and indent < current.value and !state.interpolated and first_comment < current.value) {
            state.indent = current.previous orelse 0;
            return 2;
        }
        if (valid(mask, 0) and !recovery) return 0;
    }
    if (first_comment == -1 and valid(mask, 3)) {
        var flags: u16 = 0;
        while (cp(lex) != 0) {
            switch (cp(lex)) {
                'f', 'F', 't', 'T' => flags |= format,
                'r', 'R' => flags |= raw,
                'b', 'B' => flags |= bytes_flag,
                'u', 'U' => {},
                else => break,
            }
            step(lex);
        }
        const quote = cp(lex);
        if (quote == '\'' or quote == '"' or quote == '`') {
            flags |= if (quote == '\'') single else if (quote == '"') double else back;
            step(lex);
            mark(lex);
            if (quote != '`' and cp(lex) == quote) {
                step(lex);
                if (cp(lex) == quote) {
                    step(lex);
                    mark(lex);
                    flags |= triple;
                }
            }
            state.delimiter = try store.push(&store.delimiters, state.delimiter, flags);
            state.interpolated = flags & format != 0;
            return 3;
        }
    }
    return null;
}
fn digit(c: u32) bool {
    return c >= '0' and c <= '9';
}
fn numeric(c: u32) bool {
    return c == '_' or digit(c);
}
fn alpha(c: u32) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c >= 128;
}
fn whitespace(c: u32) bool {
    return c == ' ' or (c >= 9 and c <= 13) or c == 0x85 or c == 0xa0 or c == 0x1680 or (c >= 0x2000 and c <= 0x200a) or c == 0x2028 or c == 0x2029 or c == 0x202f or c == 0x205f or c == 0x3000;
}
fn rust(state: *State, lex: *Lexer, mask: u32) ?u16 {
    if (valid(mask, 10)) return null;
    if (valid(mask, 8) or valid(mask, 7) or valid(mask, 6)) return rustComment(lex, mask);
    if (valid(mask, 0) and !valid(mask, 5)) {
        var content = false;
        while (cp(lex) != '"' and cp(lex) != '\\') {
            if (eof(lex)) return null;
            content = true;
            step(lex);
        }
        mark(lex);
        if (content) {
            mark(lex);
            return 0;
        }
    }
    if (valid(mask, 1) and cp(lex) == '"') {
        step(lex);
        mark(lex);
        return 1;
    }
    if (valid(mask, 9)) {
        while (!eof(lex)) {
            const c = cp(lex);
            step(lex);
            if (c == '\n') break;
        }
        return 9;
    }
    while (whitespace(cp(lex))) skip(lex);
    if (valid(mask, 2) and (cp(lex) == 'r' or cp(lex) == 'b' or cp(lex) == 'c')) {
        if (cp(lex) != 'r') step(lex);
        if (cp(lex) != 'r') return null;
        step(lex);
        var hashes: u16 = 0;
        while (cp(lex) == '#') {
            step(lex);
            hashes +|= 1;
        }
        if (cp(lex) != '"' or hashes > 255) return null;
        step(lex);
        state.hashes = hashes;
        return 2;
    }
    if (valid(mask, 3)) {
        while (!eof(lex)) {
            if (cp(lex) == '"') {
                mark(lex);
                step(lex);
                var hashes: u16 = 0;
                while (cp(lex) == '#' and hashes < state.hashes) {
                    step(lex);
                    hashes += 1;
                }
                if (hashes == state.hashes) return 3;
            } else step(lex);
        }
        return null;
    }
    if (valid(mask, 4) and cp(lex) == '"') {
        step(lex);
        for (0..state.hashes) |_| step(lex);
        return 4;
    }
    if (valid(mask, 5) and digit(cp(lex))) {
        step(lex);
        while (numeric(cp(lex))) step(lex);
        var fraction = false;
        var exponent = false;
        if (cp(lex) == '.') {
            fraction = true;
            step(lex);
            if (alpha(cp(lex)) or cp(lex) == '.') return null;
            while (numeric(cp(lex))) step(lex);
        }
        mark(lex);
        if (cp(lex) == 'e' or cp(lex) == 'E') {
            exponent = true;
            step(lex);
            if (cp(lex) == '+' or cp(lex) == '-') step(lex);
            if (!numeric(cp(lex))) return 5;
            step(lex);
            while (numeric(cp(lex))) step(lex);
            mark(lex);
        }
        if (!fraction and !exponent) return null;
        if (cp(lex) != 'u' and cp(lex) != 'i' and cp(lex) != 'f') return 5;
        step(lex);
        if (!digit(cp(lex))) return 5;
        while (digit(cp(lex))) step(lex);
        mark(lex);
        return 5;
    }
    return null;
}
fn rustComment(lex: *Lexer, mask: u32) ?u16 {
    var first = cp(lex);
    if (valid(mask, 7) and first == '!') {
        step(lex);
        return 7;
    }
    if (valid(mask, 6) and first == '*') {
        step(lex);
        mark(lex);
        if (cp(lex) == '/') return null;
        if (cp(lex) != '*') return 6;
    } else step(lex);
    if (!valid(mask, 8)) return null;
    var depth: usize = 1;
    var mode: enum { slash, star, normal } = switch (first) {
        '/' => .slash,
        '*' => .star,
        else => .normal,
    };
    if (first == '*' and cp(lex) == '/') return null;
    while (!eof(lex) and depth != 0) {
        first = cp(lex);
        switch (mode) {
            .slash => {
                if (first == '*') depth += 1;
                mode = .normal;
            },
            .star => {
                if (first == '*') {
                    mark(lex);
                } else {
                    if (first == '/') depth -= 1;
                    mode = .normal;
                }
            },
            .normal => {
                mark(lex);
                mode = switch (first) {
                    '/' => .slash,
                    '*' => .star,
                    else => .normal,
                };
            },
        }
        step(lex);
        if (first == '/' and depth != 0) mark(lex);
    }
    return 8;
}
