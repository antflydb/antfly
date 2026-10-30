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

//! Folds generated CUDA kernels that differ only in integer constants.
//!
//! Plan renderers bake every shape constant (head dim, warp count, split
//! count, ...) into the kernel text, so a family of plans renders the same
//! body many times. `fold` finds `extern "C" __global__` kernels with the
//! same parameter list whose bodies differ only at integer literals, moves
//! one copy of the body into a `__device__ __forceinline__` template, and
//! turns every kernel into a one-line wrapper that keeps its exact
//! signature. `unfold` reverses it, and `fold` refuses to return output that
//! does not unfold to its input byte for byte.

const std = @import("std");

const Allocator = std.mem.Allocator;

/// First line of every folded body; `unfold` keys on it.
pub const marker = "// cuda-kernel-fold: shared body, instantiated by the wrappers below\n";

const kernel_prefix = "extern \"C\" __global__ void ";

const TokenKind = enum { int_signed, int_unsigned, other };

const Token = struct {
    text: []const u8,
    kind: TokenKind,
};

/// Splits CUDA source into tokens: identifiers, numbers, comments, string
/// and character literals, whitespace runs and single punctuation. Only
/// plain decimal integer literals (optionally `u`) are marked foldable.
fn tokenize(allocator: Allocator, src: []const u8) ![]Token {
    var out = std.ArrayListUnmanaged(Token).empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < src.len) {
        const start = i;
        const c = src[i];
        var kind: TokenKind = .other;
        if (std.ascii.isAlphabetic(c) or c == '_') {
            while (i < src.len and (std.ascii.isAlphanumeric(src[i]) or src[i] == '_')) i += 1;
        } else if (std.ascii.isDigit(c)) {
            while (i < src.len) {
                const d = src[i];
                if (std.ascii.isAlphanumeric(d) or d == '_' or d == '.') {
                    i += 1;
                } else if ((d == '+' or d == '-') and (src[i - 1] == 'e' or src[i - 1] == 'E') and !std.mem.startsWith(u8, src[start..], "0x")) {
                    i += 1;
                } else break;
            }
            const lit = src[start..i];
            var digits = lit;
            const unsigned = lit[lit.len - 1] == 'u' or lit[lit.len - 1] == 'U';
            if (unsigned) digits = lit[0 .. lit.len - 1];
            const all_digits = for (digits) |d| {
                if (!std.ascii.isDigit(d)) break false;
            } else true;
            if (all_digits and digits.len > 0 and digits.len <= 9) kind = if (unsigned) .int_unsigned else .int_signed;
        } else if (c == '/' and i + 1 < src.len and src[i + 1] == '/') {
            while (i < src.len and src[i] != '\n') i += 1;
        } else if (c == '/' and i + 1 < src.len and src[i + 1] == '*') {
            const end = std.mem.indexOfPos(u8, src, i + 2, "*/") orelse return error.UnterminatedComment;
            i = end + 2;
        } else if (c == '"' or c == '\'') {
            i += 1;
            while (i < src.len and src[i] != c) i += if (src[i] == '\\') 2 else 1;
            if (i >= src.len) return error.UnterminatedLiteral;
            i += 1;
        } else if (std.ascii.isWhitespace(c)) {
            while (i < src.len and std.ascii.isWhitespace(src[i])) i += 1;
        } else {
            i += 1;
        }
        try out.append(allocator, .{ .text = src[start..i], .kind = kind });
    }
    return out.toOwnedSlice(allocator);
}

/// Index just past the bracket that closes `open` at `start`, skipping
/// comments and literals.
fn matchClose(src: []const u8, start: usize, open: u8, close: u8) !usize {
    var depth: usize = 0;
    var i = start;
    while (i < src.len) {
        const c = src[i];
        if (c == '/' and i + 1 < src.len and src[i + 1] == '/') {
            i = std.mem.indexOfScalarPos(u8, src, i, '\n') orelse return error.UnbalancedBrackets;
            continue;
        }
        if (c == '/' and i + 1 < src.len and src[i + 1] == '*') {
            i = (std.mem.indexOfPos(u8, src, i + 2, "*/") orelse return error.UnbalancedBrackets) + 2;
            continue;
        }
        if (c == '"' or c == '\'') {
            i += 1;
            while (i < src.len and src[i] != c) i += if (src[i] == '\\') 2 else 1;
            i += 1;
            continue;
        }
        if (c == open) depth += 1;
        if (c == close) {
            depth -= 1;
            if (depth == 0) return i + 1;
        }
        i += 1;
    }
    return error.UnbalancedBrackets;
}

const Kernel = struct {
    /// `extern "C" ...` through the whitespace before the body's `{`.
    start: usize,
    body_start: usize,
    end: usize,
    name: []const u8,
    /// Text between the parameter parentheses.
    params: []const u8,
};

fn findKernels(allocator: Allocator, src: []const u8) ![]Kernel {
    var out = std.ArrayListUnmanaged(Kernel).empty;
    errdefer out.deinit(allocator);
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, src, pos, kernel_prefix)) |start| {
        pos = start + kernel_prefix.len;
        if (start != 0 and src[start - 1] != '\n') continue;
        var name_end = pos;
        while (name_end < src.len and (std.ascii.isAlphanumeric(src[name_end]) or src[name_end] == '_')) name_end += 1;
        if (name_end == pos or name_end >= src.len or src[name_end] != '(') continue;
        const params_end = try matchClose(src, name_end, '(', ')');
        var b = params_end;
        while (b < src.len and std.ascii.isWhitespace(src[b])) b += 1;
        if (b >= src.len or src[b] != '{') continue;
        const end = try matchClose(src, b, '{', '}');
        try out.append(allocator, .{
            .start = start,
            .body_start = b,
            .end = end,
            .name = src[pos..name_end],
            .params = src[name_end + 1 .. params_end - 1],
        });
        pos = end;
    }
    return out.toOwnedSlice(allocator);
}

/// Parameter names in order, from a parameter list.
fn paramNames(allocator: Allocator, params: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(allocator);
    var it = std.mem.splitScalar(u8, params, ',');
    while (it.next()) |raw| {
        const p = std.mem.trim(u8, raw, " \t\r\n");
        if (p.len == 0) continue;
        const e = p.len;
        var s = e;
        while (s > 0 and (std.ascii.isAlphanumeric(p[s - 1]) or p[s - 1] == '_')) s -= 1;
        if (s == e) return error.UnnamedParameter;
        try out.append(allocator, p[s..e]);
    }
    return out.toOwnedSlice(allocator);
}

fn commonPrefixLen(names: []const []const u8) usize {
    var n = names[0].len;
    for (names[1..]) |name| {
        var k: usize = 0;
        while (k < n and k < name.len and name[k] == names[0][k]) k += 1;
        n = k;
    }
    return n;
}

fn commonSuffixLen(names: []const []const u8) usize {
    var n = names[0].len;
    for (names[1..]) |name| {
        var k: usize = 0;
        while (k < n and k < name.len and name[name.len - 1 - k] == names[0][names[0].len - 1 - k]) k += 1;
        n = k;
    }
    return n;
}

/// `prefix_..._suffix` from the members' names, cut at underscores.
fn stemFor(allocator: Allocator, names: []const []const u8) ![]u8 {
    const first = names[0];
    var p = commonPrefixLen(names);
    while (p > 0 and first[p - 1] != '_') p -= 1;
    var s = commonSuffixLen(names);
    if (s > first.len - p) s = first.len - p;
    while (s > 0 and first[first.len - s] != '_') s -= 1;
    if (p == 0) return allocator.dupe(u8, first);
    const prefix = std.mem.trimEnd(u8, first[0..p], "_");
    const suffix = first[first.len - s ..];
    return std.mem.concat(allocator, u8, &.{ prefix, suffix });
}

fn containsIdentifier(tokens: []const Token, name: []const u8) bool {
    for (tokens) |t| if (std.mem.eql(u8, t.text, name)) return true;
    return false;
}

const Group = struct {
    members: std.ArrayListUnmanaged(usize) = .empty,
};

/// Folds every group of two or more kernels whose parameter lists match and
/// whose bodies differ only in integer literals of the same signedness.
pub fn fold(allocator: Allocator, src: []const u8) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const kernels = try findKernels(arena, src);
    const bodies = try arena.alloc([]Token, kernels.len);
    for (kernels, 0..) |k, i| bodies[i] = try tokenize(arena, src[k.body_start..k.end]);

    // Group by parameter list plus body shape (literals reduced to their kind).
    var groups = std.StringArrayHashMapUnmanaged(Group).empty;
    for (kernels, 0..) |k, i| {
        var key = std.ArrayListUnmanaged(u8).empty;
        try key.appendSlice(arena, k.params);
        try key.append(arena, 0);
        for (bodies[i]) |t| switch (t.kind) {
            .int_signed => try key.appendSlice(arena, "\x01i\x00"),
            .int_unsigned => try key.appendSlice(arena, "\x01u\x00"),
            .other => {
                try key.appendSlice(arena, t.text);
                try key.append(arena, 0);
            },
        };
        const gop = try groups.getOrPut(arena, key.items);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        try gop.value_ptr.members.append(arena, i);
    }

    const Edit = struct { start: usize, end: usize, text: []const u8 };
    var edits = std.ArrayListUnmanaged(Edit).empty;
    var used_stems = std.StringHashMapUnmanaged(void).empty;

    for (groups.values()) |g| {
        const ms = g.members.items;
        if (ms.len < 2) continue;
        const base = bodies[ms[0]];
        // Slot per distinct tuple of values across members.
        var slot_of = std.StringArrayHashMapUnmanaged(usize).empty;
        var slot_kind = std.ArrayListUnmanaged(TokenKind).empty;
        const pos_slot = try arena.alloc(?usize, base.len);
        @memset(pos_slot, null);
        for (base, 0..) |t, p| {
            if (t.kind == .other) continue;
            var differs = false;
            for (ms[1..]) |m| {
                if (!std.mem.eql(u8, bodies[m][p].text, t.text)) differs = true;
            }
            if (!differs) continue;
            var tuple = std.ArrayListUnmanaged(u8).empty;
            for (ms) |m| {
                try tuple.appendSlice(arena, bodies[m][p].text);
                try tuple.append(arena, 0);
            }
            const gop = try slot_of.getOrPut(arena, tuple.items);
            if (!gop.found_existing) {
                gop.value_ptr.* = slot_kind.items.len;
                try slot_kind.append(arena, t.kind);
            }
            pos_slot[p] = gop.value_ptr.*;
        }

        // Template parameter names that appear nowhere in the body.
        const pnames = try arena.alloc([]const u8, slot_kind.items.len);
        for (pnames, 0..) |*n, s| {
            n.* = try std.fmt.allocPrint(arena, "N{d}", .{s});
            if (containsIdentifier(base, n.*)) n.* = try std.fmt.allocPrint(arena, "AntflyFoldN{d}", .{s});
            if (containsIdentifier(base, n.*)) return error.FoldParameterNameTaken;
        }

        const names = try arena.alloc([]const u8, ms.len);
        for (ms, 0..) |m, j| names[j] = kernels[m].name;
        const base_stem = try stemFor(arena, names);
        var stem = base_stem;
        var impl = try std.fmt.allocPrint(arena, "{s}_impl", .{stem});
        var suffix_n: usize = 2;
        while (used_stems.contains(stem) or std.mem.indexOf(u8, src, impl) != null) : (suffix_n += 1) {
            stem = try std.fmt.allocPrint(arena, "{s}_{d}", .{ base_stem, suffix_n });
            impl = try std.fmt.allocPrint(arena, "{s}_impl", .{stem});
        }
        try used_stems.put(arena, stem, {});

        var tmpl = std.ArrayListUnmanaged(u8).empty;
        try tmpl.appendSlice(arena, marker);
        // Identical bodies share a plain function; otherwise a template.
        if (pnames.len != 0) {
            try tmpl.appendSlice(arena, "template <");
            for (pnames, 0..) |n, s| {
                if (s != 0) try tmpl.appendSlice(arena, ", ");
                try tmpl.appendSlice(arena, if (slot_kind.items[s] == .int_unsigned) "unsigned int " else "int ");
                try tmpl.appendSlice(arena, n);
            }
            try tmpl.appendSlice(arena, ">\n");
        }
        try tmpl.appendSlice(arena, "__device__ __forceinline__ void ");
        try tmpl.appendSlice(arena, impl);
        try tmpl.append(arena, '(');
        try tmpl.appendSlice(arena, kernels[ms[0]].params);
        try tmpl.appendSlice(arena, ") ");
        for (base, 0..) |t, p| try tmpl.appendSlice(arena, if (pos_slot[p]) |s| pnames[s] else t.text);
        try tmpl.append(arena, '\n');

        const call_args = try paramNames(arena, kernels[ms[0]].params);
        for (ms, 0..) |m, j| {
            const k = kernels[m];
            var w = std.ArrayListUnmanaged(u8).empty;
            if (j == 0) try w.appendSlice(arena, tmpl.items);
            try w.appendSlice(arena, src[k.start..k.body_start]);
            try w.appendSlice(arena, "{ ");
            try w.appendSlice(arena, impl);
            if (pnames.len != 0) {
                try w.append(arena, '<');
                for (pnames, 0..) |_, s| {
                    if (s != 0) try w.appendSlice(arena, ", ");
                    const p = std.mem.indexOfScalar(?usize, pos_slot, s).?;
                    try w.appendSlice(arena, bodies[m][p].text);
                }
                try w.append(arena, '>');
            }
            try w.append(arena, '(');
            for (call_args, 0..) |a, ai| {
                if (ai != 0) try w.appendSlice(arena, ", ");
                try w.appendSlice(arena, a);
            }
            try w.appendSlice(arena, "); }");
            try edits.append(arena, .{ .start = k.start, .end = k.end, .text = w.items });
        }
    }

    std.mem.sort(Edit, edits.items, {}, struct {
        fn lessThan(_: void, a: Edit, b: Edit) bool {
            return a.start < b.start;
        }
    }.lessThan);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var pos: usize = 0;
    for (edits.items) |e| {
        try out.appendSlice(allocator, src[pos..e.start]);
        try out.appendSlice(allocator, e.text);
        pos = e.end;
    }
    try out.appendSlice(allocator, src[pos..]);

    const back = try unfold(arena, out.items);
    if (!std.mem.eql(u8, back, src)) return error.FoldRoundTripMismatch;
    return out.toOwnedSlice(allocator);
}

const Folded = struct {
    params: []const u8,
    names: []const []const u8,
    body: []Token,
};

/// Expands every folded body back into its wrappers and drops the shared
/// template, restoring the text `fold` was given.
pub fn unfold(allocator: Allocator, src: []const u8) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var impls = std.StringHashMapUnmanaged(Folded).empty;
    var removed = std.ArrayListUnmanaged([2]usize).empty;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, src, pos, marker)) |m| {
        const tl = m + marker.len;
        var names = std.ArrayListUnmanaged([]const u8).empty;
        var f = tl;
        if (std.mem.startsWith(u8, src[tl..], "template <")) {
            const tl_end = std.mem.indexOfScalarPos(u8, src, tl, '\n') orelse return error.MalformedFold;
            var it = std.mem.splitSequence(u8, src[tl + "template <".len .. tl_end - 1], ", ");
            while (it.next()) |decl| {
                const sp = std.mem.lastIndexOfScalar(u8, decl, ' ') orelse return error.MalformedFold;
                try names.append(arena, decl[sp + 1 ..]);
            }
            f = tl_end + 1;
        }
        const fn_prefix = "__device__ __forceinline__ void ";
        if (!std.mem.startsWith(u8, src[f..], fn_prefix)) return error.MalformedFold;
        const name_start = f + fn_prefix.len;
        const paren = std.mem.indexOfScalarPos(u8, src, name_start, '(') orelse return error.MalformedFold;
        const params_end = try matchClose(src, paren, '(', ')');
        const body_start = params_end + 1;
        if (src[params_end] != ' ' or src[body_start] != '{') return error.MalformedFold;
        const body_end = try matchClose(src, body_start, '{', '}');
        if (body_end >= src.len or src[body_end] != '\n') return error.MalformedFold;
        try impls.put(arena, src[name_start..paren], .{
            .params = src[paren + 1 .. params_end - 1],
            .names = names.items,
            .body = try tokenize(arena, src[body_start..body_end]),
        });
        try removed.append(arena, .{ m, body_end + 1 });
        pos = body_end + 1;
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var cursor: usize = 0;
    var r: usize = 0;
    const kernels = try findKernels(arena, src);
    for (kernels) |k| {
        while (r < removed.items.len and removed.items[r][1] <= k.start) : (r += 1) {
            try out.appendSlice(allocator, src[cursor..removed.items[r][0]]);
            cursor = removed.items[r][1];
        }
        const body = src[k.body_start..k.end];
        if (!std.mem.startsWith(u8, body, "{ ") or !std.mem.endsWith(u8, body, "); }")) continue;
        var name_end: usize = 2;
        while (name_end < body.len and (std.ascii.isAlphanumeric(body[name_end]) or body[name_end] == '_')) name_end += 1;
        const folded = impls.get(body[2..name_end]) orelse continue;
        var args = std.ArrayListUnmanaged([]const u8).empty;
        if (folded.names.len != 0) {
            if (body[name_end] != '<') return error.MalformedFold;
            const gt = std.mem.indexOfScalarPos(u8, body, name_end, '>') orelse return error.MalformedFold;
            var it = std.mem.splitSequence(u8, body[name_end + 1 .. gt], ", ");
            while (it.next()) |a| try args.append(arena, a);
        }
        if (args.items.len != folded.names.len) return error.MalformedFold;
        if (!std.mem.eql(u8, folded.params, k.params)) return error.MalformedFold;

        try out.appendSlice(allocator, src[cursor..k.body_start]);
        for (folded.body) |t| {
            const replacement = for (folded.names, 0..) |n, s| {
                if (std.mem.eql(u8, t.text, n)) break args.items[s];
            } else t.text;
            try out.appendSlice(allocator, replacement);
        }
        cursor = k.end;
    }
    while (r < removed.items.len) : (r += 1) {
        try out.appendSlice(allocator, src[cursor..removed.items[r][0]]);
        cursor = removed.items[r][1];
    }
    try out.appendSlice(allocator, src[cursor..]);
    return out.toOwnedSlice(allocator);
}

test "fold shares a body across kernels that differ in integer constants" {
    const src =
        \\// header
        \\extern "C" __global__ void antfly_k_hd256_f32_v1(
        \\    float* y,
        \\    const float* x
        \\) {
        \\    __shared__ float s[8];
        \\    if (blockDim.x != 256u) return; // 256 threads
        \\    y[threadIdx.x] = x[threadIdx.x] * 2.0f + s[7 - 7];
        \\}
        \\
        \\extern "C" __global__ void antfly_k_hd512_f32_v1(
        \\    float* y,
        \\    const float* x
        \\) {
        \\    __shared__ float s[16];
        \\    if (blockDim.x != 512u) return; // 256 threads
        \\    y[threadIdx.x] = x[threadIdx.x] * 2.0f + s[7 - 7];
        \\}
        \\
        \\extern "C" __global__ void antfly_other(float* y) {
        \\    y[0] = 1.0f;
        \\}
        \\
    ;
    const folded = try fold(std.testing.allocator, src);
    defer std.testing.allocator.free(folded);
    try std.testing.expect(std.mem.indexOf(u8, folded, "template <int N0, unsigned int N1>\n__device__ __forceinline__ void antfly_k_f32_v1_impl(") != null);
    try std.testing.expect(std.mem.indexOf(u8, folded, "{ antfly_k_f32_v1_impl<8, 256u>(y, x); }") != null);
    try std.testing.expect(std.mem.indexOf(u8, folded, "{ antfly_k_f32_v1_impl<16, 512u>(y, x); }") != null);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, folded, "* 2.0f"));
    // The unrelated kernel and comments stay as they were.
    try std.testing.expect(std.mem.indexOf(u8, folded, "extern \"C\" __global__ void antfly_other(float* y) {\n    y[0] = 1.0f;\n}") != null);
    const back = try unfold(std.testing.allocator, folded);
    defer std.testing.allocator.free(back);
    try std.testing.expectEqualStrings(src, back);
}

test "fold leaves kernels alone when anything but an integer differs" {
    const src =
        \\extern "C" __global__ void a_v1(float* y) {
        \\    y[0] = 1.0f;
        \\}
        \\extern "C" __global__ void b_v1(float* y) {
        \\    y[0] = 2.0f;
        \\}
        \\extern "C" __global__ void c_v1(float* y) {
        \\    y[1] = 1.0f;
        \\}
        \\extern "C" __global__ void d_v1(float* z) {
        \\    z[2] = 1.0f;
        \\}
        \\
    ;
    const folded = try fold(std.testing.allocator, src);
    defer std.testing.allocator.free(folded);
    // a/b differ in a float literal, d has another parameter list; only
    // a_v1 and c_v1 share a shape and fold together.
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, folded, marker));
    try std.testing.expect(std.mem.indexOf(u8, folded, "void b_v1(float* y) {\n    y[0] = 2.0f;\n}") != null);
    try std.testing.expect(std.mem.indexOf(u8, folded, "void d_v1(float* z) {\n    z[2] = 1.0f;\n}") != null);
    const back = try unfold(std.testing.allocator, folded);
    defer std.testing.allocator.free(back);
    try std.testing.expectEqualStrings(src, back);
}

test "fold shares identical bodies through a plain function" {
    const src =
        \\extern "C" __global__ void antfly_k_e2b_6144_v1(float* y) {
        \\    y[0] = 1.0f;
        \\}
        \\extern "C" __global__ void antfly_k_e2b_12288_v1(float* y) {
        \\    y[0] = 1.0f;
        \\}
        \\
    ;
    const folded = try fold(std.testing.allocator, src);
    defer std.testing.allocator.free(folded);
    try std.testing.expect(std.mem.indexOf(u8, folded, marker ++ "__device__ __forceinline__ void antfly_k_e2b_v1_impl(float* y) {") != null);
    try std.testing.expect(std.mem.indexOf(u8, folded, "{ antfly_k_e2b_v1_impl(y); }") != null);
    const back = try unfold(std.testing.allocator, folded);
    defer std.testing.allocator.free(back);
    try std.testing.expectEqualStrings(src, back);
}
