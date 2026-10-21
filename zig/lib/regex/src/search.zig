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

//! Ordered Thompson/Pike search VM. Separate from the byte/FST schema dialect:
//! UTF-8, Unicode 16 properties and simple folding, captures, assertions, flags,
//! and greedy/lazy priorities. A separate bounded native backtracking VM handles
//! nonregular compatibility features without foreign runtime dependencies.
const std = @import("std");
const unicode = @import("unicode.zig");
pub const literal = @import("literal.zig");
pub const Dfa = @import("dfa.zig").Dfa;
pub const prefilter = @import("prefilter.zig");
const native_jit = @import("jit.zig");
const Allocator = std.mem.Allocator;
pub const Error = error{ InvalidRegex, UnsupportedRegex, RegexTooLarge } || Allocator.Error;
pub const Flags = struct { insensitive: bool = false, multiline: bool = false, dotall: bool = false, unicode: bool = true, ungreedy: bool = false, extended: bool = false, crlf: bool = false, whole_word: bool = false, whole_line: bool = false, no_auto_capture: bool = false, duplicate_names: bool = false, pcre: bool = false, extended_more: bool = false, caseless_restrict: bool = false, ascii_digit: bool = false, ascii_posix: bool = false, ascii_bsd: bool = false, ascii_bss: bool = false, ascii_bsw: bool = false };
pub const Span = struct { start: usize, end: usize };
pub const CaptureName = struct { name: []const u8, index: usize };
pub const Match = struct { span: Span, captures: []const ?Span };
const Assert = enum { start, end, absolute_start, absolute_end, word, not_word, word_start, word_end, word_left, word_right, soft_end, previous_match };
const Class = union(enum) {
    range: unicode.Range,
    property: []const unicode.Range,
    exact_property: []const unicode.Range,
    ascii_property: []const unicode.Range,
    either: struct { a: *const Class, b: *const Class },
    intersect: struct { a: *const Class, b: *const Class },
    difference: struct { a: *const Class, b: *const Class },
    negate: *const Class,
    folded: struct { child: *const Class, utf8: bool, restrict: bool },
    fn matches(self: *const Class, cp: u32) bool {
        if (cp > 0x10ffff) return false;
        return switch (self.*) {
            .range => |r| cp >= r.lo and cp <= r.hi,
            .property, .exact_property => |p| unicode.contains(p, cp),
            .ascii_property => |p| cp < 128 and unicode.contains(p, cp),
            .either => |v| v.a.matches(cp) or v.b.matches(cp),
            .intersect => |v| v.a.matches(cp) and v.b.matches(cp),
            .difference => |v| v.a.matches(cp) and !v.b.matches(cp),
            .negate => |v| !v.matches(cp),
            .folded => |v| v.child.matchesCase(cp, v.utf8, v.restrict),
        };
    }
    fn matchesCase(self: *const Class, cp: u32, utf8: bool, restrict: bool) bool {
        if (cp > 0x10ffff) return false;
        switch (self.*) {
            .exact_property => return self.matches(cp),
            .ascii_property => |ranges| return cp < 128 and (unicode.contains(ranges, cp) or (std.ascii.isAlphabetic(@intCast(cp)) and unicode.contains(ranges, cp ^ 32))),
            .negate => |child| return !child.matchesCase(cp, utf8, restrict),
            .either => |v| return v.a.matchesCase(cp, utf8, restrict) or v.b.matchesCase(cp, utf8, restrict),
            .intersect => |v| return v.a.matchesCase(cp, utf8, restrict) and v.b.matchesCase(cp, utf8, restrict),
            .difference => |v| return v.a.matchesCase(cp, utf8, restrict) and !v.b.matchesCase(cp, utf8, restrict),
            .folded => return self.matches(cp),
            else => {},
        }
        var variants: [5]u32 = undefined;
        const folded = if (utf8) unicode.equivalents(cp, &variants) else ascii: {
            variants[0] = cp;
            if (cp < 128 and std.ascii.isAlphabetic(@intCast(cp))) {
                variants[1] = cp ^ 32;
                break :ascii variants[0..2];
            }
            break :ascii variants[0..1];
        };
        for (folded) |variant| if ((!restrict or (variant < 128) == (cp < 128)) and self.matches(variant)) return true;
        return false;
    }
};
const Condition = enum { group, recursion, define, version };
const GroupRef = struct { group: usize = 0, name: ?[]const u8 = null };
const Node = struct {
    scan_refs: []const GroupRef = &.{},
    scan_groups: []const usize = &.{},
    condition: Condition = .group,
    tag: enum { empty, literal, any, class, assertion, seq, alt, repeat, capture, look, reference, atomic, reset, subroutine, conditional, grapheme, verb, callout, script_run, scan },
    flags: Flags,
    negative: bool = false,
    non_atomic: bool = false,
    recursion_condition: bool = false,
    behind: bool = false,
    forbid_lookbehind: bool = false,
    ref_name: ?[]const u8 = null,
    cp: u32 = 0,
    class: ?*const Class = null,
    assertion: Assert = .start,
    children: []const *const Node = &.{},
    child: ?*const Node = null,
    min: usize = 0,
    max: ?usize = null,
    greedy: bool = true,
    group: usize = 0,
};
const Verb = enum(u32) { fail, accept, skip, prune, commit, mark, then };
const State = struct { ascii_mask: [2]u64 = .{ 0, 0 }, tag: enum { consume, repeat_consume, look_shift, split, save, assertion, accept, look, reference, atomic_start, atomic_end, reset, progress, call, conditional, grapheme, verb, callout, alt_start, alt_end, script_start, script_end, scan }, first: u32 = 0, second: u32 = 0, slot: usize = 0, node: ?*const Node = null };
pub const Pattern = struct {
    arena: std.heap.ArenaAllocator,
    states: []const State,
    start: u32,
    groups: usize,
    names: []const CaptureName,
    flags: Flags,
    prefix: []const u8,
    prefilter: []const u8,
    simple: ?SimpleRun,
    pub fn init(a: Allocator, text: []const u8, flags: Flags) Error!Pattern {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        var parser = Parser{ .a = arena.allocator(), .text = text, .flags = flags, .unicode = flags.unicode };
        var root = try parser.expr();
        parser.skip();
        if (parser.pos != text.len) return error.InvalidRegex;
        if (flags.whole_word or flags.whole_line) {
            const left = try parser.node(.{ .tag = .assertion, .flags = flags, .assertion = if (flags.whole_line) .start else .word_left });
            const right = try parser.node(.{ .tag = .assertion, .flags = flags, .assertion = if (flags.whole_line) .end else .word_right });
            const children = try arena.allocator().alloc(*const Node, 3);
            children[0] = left;
            children[1] = root;
            children[2] = right;
            root = try parser.node(.{ .tag = .seq, .flags = flags, .children = children });
        }
        var compiler = Compiler{ .a = arena.allocator() };
        const accept = try compiler.emit(.{ .tag = .accept });
        const start = try compiler.lower(root, accept);
        const prefix = try literalPrefix(parser.a, root);
        const required_literal = try requiredLiteral(parser.a, root);
        return .{ .arena = arena, .states = compiler.states.items, .start = start, .groups = parser.groups, .names = parser.names.items, .flags = flags, .prefix = prefix, .prefilter = required_literal, .simple = simpleRun(root) };
    }
    pub fn deinit(self: *Pattern) void {
        self.arena.deinit();
    }
};
fn literalBytes(a: Allocator, node: *const Node) Error![]const u8 {
    if (node.tag != .literal or node.flags.insensitive) return "";
    if (!node.flags.unicode) {
        const bytes = try a.alloc(u8, 1);
        bytes[0] = @intCast(node.cp);
        return bytes;
    }
    var buffer: [4]u8 = undefined;
    const n = std.unicode.utf8Encode(@intCast(node.cp), &buffer) catch return error.InvalidRegex;
    return a.dupe(u8, buffer[0..n]);
}
fn literalPrefix(a: Allocator, node: *const Node) Error![]const u8 {
    return switch (node.tag) {
        .literal => literalBytes(a, node),
        .capture => literalPrefix(a, node.child.?),
        .repeat => if (node.min > 0) literalPrefix(a, node.child.?) else "",
        .seq => blk: {
            var out: std.ArrayList(u8) = .empty;
            for (node.children) |child| {
                if (child.tag == .empty or child.tag == .assertion) continue;
                const prefix = try literalPrefix(a, child);
                try out.appendSlice(a, prefix);
                if (child.tag != .literal or prefix.len == 0) break;
            }
            break :blk out.items;
        },
        .alt => blk: {
            var prefix = try literalPrefix(a, node.children[0]);
            for (node.children[1..]) |child| {
                const other = try literalPrefix(a, child);
                var n: usize = 0;
                while (n < @min(prefix.len, other.len) and prefix[n] == other[n]) n += 1;
                prefix = prefix[0..n];
            }
            break :blk prefix;
        },
        else => "",
    };
}
fn requiredLiteral(a: Allocator, node: *const Node) Error![]const u8 {
    var best = try literalPrefix(a, node);
    switch (node.tag) {
        .capture => return requiredLiteral(a, node.child.?),
        .repeat => if (node.min > 0) {
            return requiredLiteral(a, node.child.?);
        },
        .seq => {
            var run: std.ArrayList(u8) = .empty;
            for (node.children) |child| {
                if (child.tag == .literal and !child.flags.insensitive) {
                    try run.appendSlice(a, try literalBytes(a, child));
                    if (run.items.len > best.len) best = try a.dupe(u8, run.items);
                } else {
                    run.clearRetainingCapacity();
                    const candidate = try requiredLiteral(a, child);
                    if (candidate.len > best.len) best = candidate;
                }
            }
        },
        else => {},
    }
    return best;
}

pub fn hasAbsoluteStart(states: []const State) bool {
    for (states) |state| if (state.tag == .assertion and state.node.?.assertion == .absolute_start) return true;
    return false;
}
pub fn hasLineAnchors(states: []const State) bool {
    for (states) |state| if (state.tag == .assertion and (state.node.?.assertion == .start or state.node.?.assertion == .end)) return true;
    return false;
}

const Compiler = struct {
    a: Allocator,
    states: std.ArrayList(State) = .empty,
    steps: usize = 0,
    scratch_slots: usize = 0,
    advanced: bool = false,
    names: []const CaptureName = &.{},
    groups: usize = 0,
    group_nodes: []const ?*const Node = &.{},
    utf8: bool = true,
    fn emit(self: *Compiler, s: State) Error!u32 {
        if (self.states.items.len >= 8192) return error.RegexTooLarge;
        const index: u32 = @intCast(self.states.items.len);
        var state = s;
        if (state.tag == .progress or state.tag == .atomic_start or state.tag == .script_start) {
            state.slot = self.scratch_slots;
            self.scratch_slots += if (state.tag == .atomic_start) @as(usize, 3) else 1;
        }
        try self.states.append(self.a, state);
        return index;
    }
    fn referenceGroup(self: *Compiler, n: *const Node, allow_zero: bool) Error!usize {
        var group = n.group;
        if (n.ref_name) |name| {
            group = std.math.maxInt(usize);
            for (self.names) |entry| if (std.mem.eql(u8, name, entry.name)) {
                group = entry.index;
                break;
            };
            if (group == std.math.maxInt(usize)) return error.InvalidRegex;
        }
        if ((!allow_zero and group == 0) or group > self.groups) return error.InvalidRegex;
        return group;
    }
    const Bounds = struct { min: usize, max: usize };
    fn bounds(self: *Compiler, n: *const Node, visiting: *[129]bool) Error!Bounds {
        if (n.forbid_lookbehind) return error.InvalidRegex;
        return switch (n.tag) {
            .empty, .assertion, .look, .scan, .reset, .verb, .callout => .{ .min = 0, .max = 0 },
            .literal, .class => .{ .min = 1, .max = 1 },
            .any => if (self.utf8 and !n.flags.unicode) error.InvalidRegex else .{ .min = 1, .max = 1 },
            .grapheme => error.InvalidRegex,
            .capture, .atomic, .script_run => self.bounds(n.child.?, visiting),
            .reference, .subroutine => blk: {
                const group = try self.referenceGroup(n, false);
                if (group >= visiting.len or visiting[group]) return error.InvalidRegex;
                if (n.ref_name) |name| {
                    var count: usize = 0;
                    for (self.names) |entry| if (std.mem.eql(u8, name, entry.name)) {
                        count += 1;
                    };
                    if (count != 1) return error.InvalidRegex;
                }
                const child = self.group_nodes[group] orelse return error.InvalidRegex;
                visiting[group] = true;
                defer visiting[group] = false;
                break :blk try self.bounds(child, visiting);
            },
            .repeat => blk: {
                if (n.max == 0) break :blk .{ .min = 0, .max = 0 };
                const child = try self.bounds(n.child.?, visiting);
                const maximum = n.max orelse if (child.max == 0) 0 else return error.InvalidRegex;
                break :blk .{ .min = std.math.mul(usize, child.min, n.min) catch return error.RegexTooLarge, .max = std.math.mul(usize, child.max, maximum) catch return error.RegexTooLarge };
            },
            .seq, .alt, .conditional => blk: {
                var result = Bounds{ .min = if (n.tag == .seq) 0 else std.math.maxInt(usize), .max = 0 };
                for (n.children) |child| {
                    const value = try self.bounds(child, visiting);
                    if (n.tag == .seq) {
                        result.min = std.math.add(usize, result.min, value.min) catch return error.RegexTooLarge;
                        result.max = std.math.add(usize, result.max, value.max) catch return error.RegexTooLarge;
                    } else {
                        result.min = @min(result.min, value.min);
                        result.max = @max(result.max, value.max);
                    }
                }
                break :blk result;
            },
        };
    }
    fn behindBranch(self: *Compiler, branch: *const Node, cont: u32) Error!u32 {
        var visiting: [129]bool = @splat(false);
        const width = try self.bounds(branch, &visiting);
        if (width.max > (if (width.min == width.max) @as(usize, 65535) else 255)) return error.InvalidRegex;
        const body = try self.lower(branch, cont);
        return self.emit(.{ .tag = .look_shift, .first = body, .slot = width.min, .second = @intCast(width.max), .node = branch });
    }
    fn lowerBehind(self: *Compiler, child: *const Node) Error!u32 {
        const accept = try self.emit(.{ .tag = .accept });
        if (child.tag != .alt or child.children.len < 2) return self.behindBranch(child, accept);
        const begin = try self.emit(.{ .tag = .alt_start });
        const end = try self.emit(.{ .tag = .alt_end, .first = accept, .slot = begin });
        var i = child.children.len - 1;
        var next = try self.behindBranch(child.children[i], end);
        while (i > 0) {
            i -= 1;
            const branch = try self.behindBranch(child.children[i], end);
            next = try self.emit(.{ .tag = .split, .first = branch, .second = next, .slot = begin + 1 });
        }
        self.states.items[begin].first = next;
        return begin;
    }
    fn lower(self: *Compiler, n: *const Node, cont: u32) Error!u32 {
        self.steps += 1;
        if (self.steps > 32768) return error.RegexTooLarge;
        return switch (n.tag) {
            .script_run => if (self.advanced) blk: {
                const begin = try self.emit(.{ .tag = .script_start });
                const end = try self.emit(.{ .tag = .script_end, .slot = self.states.items[begin].slot, .first = cont, .node = n });
                const body = try self.lower(n.child.?, end);
                self.states.items[begin].first = body;
                break :blk begin;
            } else error.UnsupportedRegex,
            .scan => if (self.advanced) blk: {
                const prepared = try self.a.create(Node);
                prepared.* = n.*;
                var groups: std.ArrayList(usize) = .empty;
                for (n.scan_refs) |ref| {
                    if (ref.name) |name| {
                        const first = groups.items.len;
                        for (self.names) |entry| if (std.mem.eql(u8, name, entry.name)) {
                            try groups.append(self.a, entry.index);
                        };
                        if (groups.items.len == first) return error.InvalidRegex;
                        std.mem.sort(usize, groups.items[first..], {}, struct {
                            fn less(_: void, left: usize, right: usize) bool {
                                return left < right;
                            }
                        }.less);
                    } else {
                        if (ref.group == 0 or ref.group > self.groups) return error.InvalidRegex;
                        try groups.append(self.a, ref.group);
                    }
                }
                prepared.scan_groups = groups.items;
                break :blk try self.emit(.{ .tag = .scan, .first = cont, .second = try self.lower(n.child.?, try self.emit(.{ .tag = .accept })), .node = prepared });
            } else error.UnsupportedRegex,
            .look => if (self.advanced) try self.emit(.{ .tag = .look, .first = cont, .second = if (n.behind) try self.lowerBehind(n.child.?) else try self.lower(n.child.?, try self.emit(.{ .tag = .accept })), .node = n }) else error.UnsupportedRegex,
            .reference => if (self.advanced) blk: {
                var group = n.group;
                if (n.ref_name) |name| {
                    group = 0;
                    for (self.names) |entry| if (std.mem.eql(u8, name, entry.name)) {
                        group = entry.index;
                        break;
                    };
                }
                if (group == 0 or group > self.groups) return error.InvalidRegex;
                break :blk try self.emit(.{ .tag = .reference, .first = cont, .slot = group, .node = n });
            } else error.UnsupportedRegex,
            .callout => if (self.advanced) try self.emit(.{ .tag = .callout, .first = cont, .node = n }) else error.UnsupportedRegex,
            .verb => if (self.advanced) try self.emit(.{ .tag = .verb, .first = cont, .slot = n.cp, .node = n }) else error.UnsupportedRegex,
            .grapheme => if (self.advanced) try self.emit(.{ .tag = .grapheme, .first = cont }) else error.UnsupportedRegex,
            .subroutine => if (self.advanced) blk: {
                const group = try self.referenceGroup(n, true);
                // A single predicate has no observable capture/control effects:
                // PCRE restores the called group's capture on return. Inline it
                // so generated chains avoid allocating a return snapshot.
                if (group > 0) if (self.group_nodes[group]) |definition| {
                    const body = definition.child.?;
                    if (body.tag == .literal or body.tag == .class or body.tag == .any)
                        break :blk try self.lower(body, cont);
                };
                break :blk try self.emit(.{ .tag = .call, .first = cont, .slot = group });
            } else error.UnsupportedRegex,
            .conditional => if (self.advanced) blk: {
                var condition = n;
                if (n.condition == .recursion) for (self.names) |entry| if (std.mem.eql(u8, entry.name, "R")) {
                    const named = try self.a.create(Node);
                    named.* = n.*;
                    named.condition = .group;
                    named.group = entry.index;
                    named.ref_name = "R";
                    condition = named;
                    break;
                };
                break :blk try self.emit(.{ .tag = .conditional, .first = try self.lower(n.children[0], cont), .second = try self.lower(n.children[1], cont), .slot = if (n.child) |child| (if (n.behind) try self.lowerBehind(child) else try self.lower(child, try self.emit(.{ .tag = .accept }))) else if (condition.condition != .group) 0 else try self.referenceGroup(condition, condition.recursion_condition), .node = condition });
            } else error.UnsupportedRegex,
            .reset => if (self.advanced) try self.emit(.{ .tag = .reset, .first = cont }) else error.UnsupportedRegex,
            .atomic => if (self.advanced) blk: {
                const begin = try self.emit(.{ .tag = .atomic_start });
                const end = try self.emit(.{ .tag = .atomic_end, .first = cont, .slot = self.states.items[begin].slot });
                const child = try self.lower(n.child.?, end);
                self.states.items[begin].first = child;
                break :blk begin;
            } else error.UnsupportedRegex,
            .empty => cont,
            .literal, .any, .class => try self.emit(.{ .tag = .consume, .node = n, .first = cont, .ascii_mask = asciiMask(n) }),
            .assertion => try self.emit(.{ .tag = .assertion, .node = n, .first = cont }),
            .capture => blk: {
                const end = try self.emit(.{ .tag = .save, .slot = n.group * 2 + 1, .first = cont });
                break :blk try self.emit(.{ .tag = .save, .slot = n.group * 2, .first = try self.lower(n.child.?, end) });
            },
            .seq => blk: {
                var next = cont;
                var i = n.children.len;
                while (i > 0) {
                    i -= 1;
                    next = try self.lower(n.children[i], next);
                }
                break :blk next;
            },
            .alt => blk: {
                const begin = if (self.advanced and n.children.len > 1) try self.emit(.{ .tag = .alt_start }) else null;
                const end = if (begin) |id| try self.emit(.{ .tag = .alt_end, .first = cont, .slot = id }) else cont;
                var i = n.children.len - 1;
                var next = try self.lower(n.children[i], end);
                while (i > 0) {
                    i -= 1;
                    next = try self.emit(.{ .tag = .split, .first = try self.lower(n.children[i], end), .second = next, .slot = if (begin) |id| id + 1 else 0 });
                }
                if (begin) |id| {
                    self.states.items[id].first = next;
                    break :blk id;
                }
                break :blk next;
            },
            .repeat => blk: {
                if (self.advanced and (n.child.?.tag == .literal or n.child.?.tag == .class or n.child.?.tag == .any)) {
                    if (n.min > 65535 or (n.max != null and n.max.? > 65535)) return error.RegexTooLarge;
                    break :blk try self.emit(.{ .tag = .repeat_consume, .first = cont, .node = n, .ascii_mask = asciiMask(n.child.?) });
                }
                if (n.min > 8192 or (n.max != null and n.max.? > 8192)) return error.RegexTooLarge;
                var next = cont;
                if (n.max) |max| {
                    for (n.min..max) |_| {
                        const child = try self.lower(n.child.?, next);
                        next = try self.emit(.{ .tag = .split, .first = if (n.greedy) child else next, .second = if (n.greedy) next else child });
                    }
                } else {
                    const split = try self.emit(.{ .tag = .split });
                    const loop = if (self.advanced) try self.emit(.{ .tag = .progress, .first = split, .second = cont }) else split;
                    const child = try self.lower(n.child.?, loop);
                    self.states.items[split].first = if (n.greedy) child else cont;
                    self.states.items[split].second = if (n.greedy) cont else child;
                    next = split;
                }
                for (0..n.min) |_| next = try self.lower(n.child.?, next);
                break :blk next;
            },
        };
    }
};
const List = struct { pcs: []u32, caps: []usize, len: usize = 0 };
pub const Runner = struct {
    a: Allocator,
    p: *const Pattern,
    slots: usize,
    current: List = .{ .pcs = &.{}, .caps = &.{} },
    next: List = .{ .pcs = &.{}, .caps = &.{} },
    stack: List = .{ .pcs = &.{}, .caps = &.{} },
    seen: []bool = &.{},
    initial: []usize = &.{},
    best: []usize = &.{},
    work: []usize = &.{},
    result: []?Span,
    storage_ready: bool = false,
    absolute_start_allowed: bool = true,
    failure: ?Error = null,
    pub const CaptureMode = enum { full, span };
    pub fn init(a: Allocator, p: *const Pattern) Error!Runner {
        return initMode(a, p, .full);
    }
    pub fn initMode(a: Allocator, p: *const Pattern, mode: CaptureMode) Error!Runner {
        const captures = if (mode == .full) p.groups + 1 else 1;
        const slots = captures * 2;
        if (p.simple == null and p.states.len * slots * 3 * @sizeOf(usize) > 64 * 1024 * 1024) return error.RegexTooLarge;
        return .{ .a = a, .p = p, .slots = slots, .result = try a.alloc(?Span, captures) };
    }
    fn ensureStorage(self: *Runner) Error!void {
        if (self.storage_ready) return;
        const a = self.a;
        const p = self.p;
        const s = p.states.len;
        const slots = self.slots;
        const pcs = try a.alloc(u32, s * 3);
        errdefer a.free(pcs);
        const caps = try a.alloc(usize, s * slots * 3);
        errdefer a.free(caps);
        const seen = try a.alloc(bool, s);
        errdefer a.free(seen);
        const tags = try a.alloc(usize, slots * 3);
        self.current = .{ .pcs = pcs[0..s], .caps = caps[0 .. s * slots] };
        self.next = .{ .pcs = pcs[s .. s * 2], .caps = caps[s * slots .. s * slots * 2] };
        self.stack = .{ .pcs = pcs[s * 2 ..], .caps = caps[s * slots * 2 ..] };
        self.seen = seen;
        self.initial = tags[0..slots];
        self.best = tags[slots .. slots * 2];
        self.work = tags[slots * 2 ..];
        self.storage_ready = true;
    }
    pub fn deinit(self: *Runner) void {
        if (self.storage_ready) {
            // Lists swap while executing; recover allocation starts from the minimum.
            const pcs = if (@intFromPtr(self.current.pcs.ptr) < @intFromPtr(self.next.pcs.ptr)) self.current.pcs else self.next.pcs;
            const caps = if (@intFromPtr(self.current.caps.ptr) < @intFromPtr(self.next.caps.ptr)) self.current.caps else self.next.caps;
            self.a.free(pcs.ptr[0 .. self.p.states.len * 3]);
            self.a.free(caps.ptr[0 .. self.p.states.len * self.slots * 3]);
            self.a.free(self.seen);
            self.a.free(self.initial.ptr[0 .. self.slots * 3]);
        }
        self.a.free(self.result);
    }
    // Returned capture storage is borrowed until the next find call.
    pub fn find(self: *Runner, text: []const u8, offset: usize) ?Match {
        self.failure = null;
        if (offset > text.len) return null;
        if (self.p.simple) |simple| return self.findSimple(simple, text, offset);
        if (self.p.prefilter.len > 0 and literal.findExact(text, self.p.prefilter, offset) == null) return null;
        self.ensureStorage() catch |err| {
            self.failure = err;
            return null;
        };
        self.current.len = 0;
        self.next.len = 0;
        @memset(self.seen, false);
        @memset(self.initial, std.math.maxInt(usize));
        var pos = offset;
        var matched = false;
        while (true) {
            if (!matched and (self.p.prefix.len == 0 or std.mem.startsWith(u8, text[pos..], self.p.prefix) or self.current.len == 0)) {
                if (self.p.prefix.len > 0 and self.current.len == 0) {
                    pos = literal.findExact(text, self.p.prefix, pos) orelse break;
                    @memset(self.seen, false);
                }
                self.initial[0] = pos;
                self.expand(&self.current, self.p.start, self.initial, text, pos);
            }
            @memset(self.seen, false);
            self.next.len = 0;
            const d = if (pos < text.len) unicode.decode(text, pos, self.p.flags.unicode) else unicode.Decoded{ .cp = 0x110000, .len = 0 };
            for (self.current.pcs[0..self.current.len], 0..) |pc, i| {
                const state = self.p.states[pc];
                const caps = self.current.caps[i * self.slots ..][0..self.slots];
                if (state.tag == .accept) {
                    @memcpy(self.best, caps);
                    self.best[1] = pos;
                    matched = true;
                    break;
                }
                if (pos < text.len and stateMatches(state, d.cp)) self.expand(&self.next, state.first, caps, text, pos + d.len);
            }
            if (pos == text.len or (matched and self.next.len == 0)) break;
            std.mem.swap(List, &self.current, &self.next);
            pos += d.len;
        }
        if (!matched) return null;
        for (self.result, 0..) |*value, i| value.* = if (self.best[i * 2] == std.math.maxInt(usize) or self.best[i * 2 + 1] == std.math.maxInt(usize)) null else .{ .start = self.best[i * 2], .end = self.best[i * 2 + 1] };
        return .{ .span = self.result[0].?, .captures = self.result };
    }
    fn findSimple(self: *Runner, simple: SimpleRun, text: []const u8, offset: usize) ?Match {
        var start = offset;
        while (start <= text.len) {
            var end = start;
            var count: usize = 0;
            const target = if (simple.greedy) simple.max else simple.min;
            while (end < text.len and count < target) {
                const d = unicode.decode(text, end, simple.node.flags.unicode);
                if (!if (d.cp < 128) simple.ascii_mask[d.cp / 64] & (@as(u64, 1) << @intCast(d.cp % 64)) != 0 else consume(simple.node, d.cp)) break;
                count += 1;
                end += d.len;
            }
            if (count >= simple.min) {
                self.result[0] = .{ .start = start, .end = end };
                return .{ .span = self.result[0].?, .captures = self.result };
            }
            if (start == text.len) break;
            // All interior suffixes of this failed run are too short as well.
            start = if (end > start) end else start + unicode.decode(text, start, simple.node.flags.unicode).len;
        }
        return null;
    }
    fn expand(self: *Runner, out: *List, start: u32, tags: []const usize, text: []const u8, pos: usize) void {
        self.stack.len = 1;
        self.stack.pcs[0] = start;
        @memcpy(self.stack.caps[0..self.slots], tags);
        while (self.stack.len > 0) {
            self.stack.len -= 1;
            const j = self.stack.len;
            var pc = self.stack.pcs[j];
            @memcpy(self.work, self.stack.caps[j * self.slots ..][0..self.slots]);
            const caps = self.work;
            while (!self.seen[pc]) {
                self.seen[pc] = true;
                const s = self.p.states[pc];
                switch (s.tag) {
                    .consume, .accept => {
                        out.pcs[out.len] = pc;
                        @memcpy(out.caps[out.len * self.slots ..][0..self.slots], caps);
                        out.len += 1;
                        break;
                    },
                    .save => if (s.slot < caps.len) {
                        caps[s.slot] = pos;
                    },
                    .assertion => if ((s.node.?.assertion == .absolute_start and !self.absolute_start_allowed) or !assertion(s.node.?, text, pos, self.p.flags.unicode)) break,
                    else => unreachable,
                    .split => {
                        const k = self.stack.len;
                        self.stack.pcs[k] = s.second;
                        @memcpy(self.stack.caps[k * self.slots ..][0..self.slots], caps);
                        self.stack.len += 1;
                    },
                }
                pc = s.first;
            }
        }
    }
};
const SimpleRun = struct { node: *const Node, min: usize, max: usize, greedy: bool, ascii_mask: [2]u64 };
fn simpleRun(root: *const Node) ?SimpleRun {
    if (root.tag == .seq) {
        var meaningful: ?*const Node = null;
        for (root.children) |child| {
            if (child.tag == .empty) continue;
            if (meaningful != null) return null;
            meaningful = child;
        }
        return if (meaningful) |child| simpleRun(child) else null;
    }
    const node = if (root.tag == .repeat) root.child.? else root;
    if (node.tag != .literal and node.tag != .class and node.tag != .any) return null;
    return .{ .node = node, .min = if (root.tag == .repeat) root.min else 1, .max = if (root.tag == .repeat) root.max orelse std.math.maxInt(usize) else 1, .greedy = root.greedy, .ascii_mask = asciiMask(node) };
}
fn asciiMask(node: *const Node) [2]u64 {
    var mask: [2]u64 = .{ 0, 0 };
    for (0..128) |cp| if (consume(node, @intCast(cp))) {
        mask[cp / 64] |= @as(u64, 1) << @intCast(cp % 64);
    };
    return mask;
}
pub fn stateMatches(state: State, cp: u32) bool {
    return if (cp < 128) state.ascii_mask[cp / 64] & (@as(u64, 1) << @intCast(cp % 64)) != 0 else consume(if (state.tag == .repeat_consume) state.node.?.child.? else state.node.?, cp);
}
fn foldCodepoint(cp: u32, utf8: bool) u32 {
    return if (utf8) unicode.fold(cp) else if (cp < 128) std.ascii.toLower(@as(u8, @intCast(cp))) else cp;
}
fn consume(n: *const Node, cp: u32) bool {
    if (cp > (if (n.flags.unicode) @as(u32, 0x10ffff) else 255)) return false;
    return switch (n.tag) {
        .literal => if (n.flags.insensitive) (!n.flags.caseless_restrict or (cp < 128) == (n.cp < 128)) and foldCodepoint(cp, n.flags.unicode) == foldCodepoint(n.cp, n.flags.unicode) else cp == n.cp,
        .any => n.flags.dotall or cp != '\n',
        .class => if (n.flags.insensitive) n.class.?.matchesCase(cp, n.flags.unicode, n.flags.caseless_restrict) else n.class.?.matches(cp),
        else => unreachable,
    };
}
fn assertion(n: *const Node, text: []const u8, pos: usize, utf8: bool) bool {
    const prev = unicode.previous(text, pos, utf8);
    const next = if (pos < text.len) unicode.decode(text, pos, utf8).cp else 0x110000;
    if (n.flags.unicode and pos < text.len and text[pos] & 0xc0 == 0x80) switch (n.assertion) {
        .word, .not_word, .word_start, .word_end, .word_left, .word_right => return false,
        else => {},
    };
    const before = if (n.flags.unicode and !n.flags.ascii_bsw) (if (n.flags.pcre) unicode.pcreWord(prev) else unicode.isWord(prev)) else prev < 128 and (std.ascii.isAlphanumeric(@intCast(prev)) or prev == '_');
    const after = if (n.flags.unicode and !n.flags.ascii_bsw) (if (n.flags.pcre) unicode.pcreWord(next) else unicode.isWord(next)) else next < 128 and (std.ascii.isAlphanumeric(@intCast(next)) or next == '_');
    return switch (n.assertion) {
        .absolute_start => pos == 0,
        .absolute_end => pos == text.len,
        .soft_end => pos == text.len or (pos + 1 == text.len and next == '\n'),
        .previous_match => pos == 0,
        .start => pos == 0 or (n.flags.multiline and prev == '\n' and !(n.flags.pcre and pos == text.len)),
        .end => pos == text.len or (n.flags.multiline and (next == '\n' or (n.flags.crlf and next == '\r' and pos + 1 < text.len and text[pos + 1] == '\n'))),
        .word => before != after,
        .not_word => before == after,
        .word_start => !before and after,
        .word_end => before and !after,
        .word_left => !before,
        .word_right => !after,
    };
}
const Parser = struct {
    a: Allocator,
    text: []const u8,
    flags: Flags,
    unicode: bool,
    pos: usize = 0,
    depth: usize = 0,
    nodes: usize = 0,
    groups: usize = 0,
    names: std.ArrayList(CaptureName) = .empty,
    advanced: bool = false,
    fn node(self: *Parser, value: Node) Error!*const Node {
        if (!self.advanced and self.unicode and !value.flags.unicode) {
            if (value.tag == .any or (value.tag == .literal and value.cp > 127)) return error.UnsupportedRegex;
            if (value.tag == .class) for (128..256) |cp| {
                if (value.class.?.matches(@intCast(cp))) return error.UnsupportedRegex;
            };
        }
        self.nodes += 1;
        if (self.nodes > 16384) return error.RegexTooLarge;
        const p = try self.a.create(Node);
        p.* = value;
        if (value.ref_name) |name| p.ref_name = try self.a.dupe(u8, name);
        return p;
    }
    fn cls(self: *Parser, value: Class) Error!*const Class {
        self.nodes += 1;
        if (self.nodes > 16384) return error.RegexTooLarge;
        const p = try self.a.create(Class);
        p.* = value;
        return p;
    }
    fn skip(self: *Parser) void {
        if (!self.flags.extended) return;
        while (self.pos < self.text.len) {
            if (std.ascii.isWhitespace(self.text[self.pos])) {
                self.pos += 1;
                continue;
            }
            if (self.text[self.pos] == '#') {
                while (self.pos < self.text.len and self.text[self.pos] != '\n') self.pos += 1;
                continue;
            }
            break;
        }
    }
    fn expr(self: *Parser) Error!*const Node {
        if (self.depth >= 128) return error.RegexTooLarge;
        self.depth += 1;
        defer self.depth -= 1;
        var alts: std.ArrayList(*const Node) = .empty;
        try alts.append(self.a, try self.concat());
        self.skip();
        while (self.pos < self.text.len and self.text[self.pos] == '|') {
            self.pos += 1;
            try alts.append(self.a, try self.concat());
            self.skip();
        }
        if (alts.items.len == 1) return alts.items[0];
        return self.node(.{ .tag = .alt, .flags = self.flags, .children = alts.items });
    }
    fn concat(self: *Parser) Error!*const Node {
        var nodes: std.ArrayList(*const Node) = .empty;
        while (true) {
            self.skip();
            if (self.pos == self.text.len or self.text[self.pos] == ')' or self.text[self.pos] == '|') break;
            try nodes.append(self.a, try self.quantified());
        }
        if (nodes.items.len == 0) return self.node(.{ .tag = .empty, .flags = self.flags });
        if (nodes.items.len == 1) return nodes.items[0];
        return self.node(.{ .tag = .seq, .flags = self.flags, .children = nodes.items });
    }
    fn quantified(self: *Parser) Error!*const Node {
        const term = try self.atom();
        self.skip();
        if (self.pos == self.text.len) return term;
        var min: usize = 0;
        var max: ?usize = null;
        switch (self.text[self.pos]) {
            '*' => self.pos += 1,
            '+' => {
                min = 1;
                self.pos += 1;
            },
            '?' => {
                max = 1;
                self.pos += 1;
            },
            '{' => {
                if (self.advanced) {
                    const count_range = try self.bracedQuantifier(self.pos) orelse return term;
                    min = count_range.min;
                    max = count_range.max;
                    self.pos = count_range.end;
                } else {
                    self.pos += 1;
                    min = try self.count();
                    max = min;
                    if (self.pos < self.text.len and self.text[self.pos] == ',') {
                        self.pos += 1;
                        max = if (self.pos < self.text.len and self.text[self.pos] != '}') try self.count() else null;
                    }
                    if (self.pos == self.text.len or self.text[self.pos] != '}' or (max != null and max.? < min)) return error.InvalidRegex;
                    self.pos += 1;
                }
            },
            else => return term,
        }
        if (term.tag == .assertion) return error.InvalidRegex;
        var greedy = !self.flags.ungreedy;
        if (self.pos < self.text.len and self.text[self.pos] == '?') {
            self.pos += 1;
            greedy = !greedy;
        }
        if (self.advanced and self.pos < self.text.len and self.text[self.pos] == '+') {
            self.pos += 1;
            const repeat = try self.node(.{ .tag = .repeat, .flags = self.flags, .child = term, .min = min, .max = max, .greedy = true });
            return self.node(.{ .tag = .atomic, .flags = self.flags, .child = repeat });
        }
        if (self.pos < self.text.len and std.mem.indexOfScalar(u8, "*+?{", self.text[self.pos]) != null) {
            if (!self.advanced or self.text[self.pos] != '{' or try self.bracedQuantifier(self.pos) != null) return error.InvalidRegex;
        }
        return self.node(.{ .tag = .repeat, .flags = self.flags, .child = term, .min = min, .max = max, .greedy = greedy });
    }
    const Quantifier = struct { min: usize, max: ?usize, end: usize };
    fn bracedQuantifier(self: *Parser, start: usize) Error!?Quantifier {
        var at = start + 1;
        while (at < self.text.len and (self.text[at] == ' ' or self.text[at] == '\t')) at += 1;
        const first = at;
        while (at < self.text.len and std.ascii.isDigit(self.text[at])) at += 1;
        const lower_end = at;
        while (at < self.text.len and (self.text[at] == ' ' or self.text[at] == '\t')) at += 1;
        const comma = at < self.text.len and self.text[at] == ',';
        if (!comma) {
            if (first == lower_end or at == self.text.len or self.text[at] != '}') return null;
            const n = std.fmt.parseInt(usize, self.text[first..lower_end], 10) catch return error.RegexTooLarge;
            if (n > 65535) return error.RegexTooLarge;
            return .{ .min = n, .max = n, .end = at + 1 };
        }
        at += 1;
        while (at < self.text.len and (self.text[at] == ' ' or self.text[at] == '\t')) at += 1;
        const upper = at;
        while (at < self.text.len and std.ascii.isDigit(self.text[at])) at += 1;
        const upper_end = at;
        while (at < self.text.len and (self.text[at] == ' ' or self.text[at] == '\t')) at += 1;
        if (at == self.text.len or self.text[at] != '}' or (lower_end == first and upper_end == upper)) return null;
        const lower = if (lower_end == first) 0 else std.fmt.parseInt(usize, self.text[first..lower_end], 10) catch return error.RegexTooLarge;
        const limit: ?usize = if (upper_end == upper) null else std.fmt.parseInt(usize, self.text[upper..upper_end], 10) catch return error.RegexTooLarge;
        if (lower > 65535 or (limit != null and limit.? > 65535)) return error.RegexTooLarge;
        if (limit != null and limit.? < lower) return error.InvalidRegex;
        return .{ .min = lower, .max = limit, .end = at + 1 };
    }
    fn count(self: *Parser) Error!usize {
        const start = self.pos;
        while (self.pos < self.text.len and std.ascii.isDigit(self.text[self.pos])) self.pos += 1;
        if (start == self.pos) return error.InvalidRegex;
        return std.fmt.parseInt(usize, self.text[start..self.pos], 10) catch error.RegexTooLarge;
    }
    fn atom(self: *Parser) Error!*const Node {
        self.skip();
        if (self.pos == self.text.len) return error.InvalidRegex;
        const c = self.text[self.pos];
        self.pos += 1;
        switch (c) {
            '(' => return self.group(),
            '[' => {
                if (self.advanced and (std.mem.startsWith(u8, self.text[self.pos..], "[:<:]]") or std.mem.startsWith(u8, self.text[self.pos..], "[:>:]]"))) {
                    const start_word = self.text[self.pos + 2] == '<';
                    self.pos += 6;
                    return self.node(.{ .tag = .assertion, .flags = self.flags, .assertion = if (start_word) .word_start else .word_end });
                }
                return self.node(.{ .tag = .class, .flags = self.flags, .class = try self.class() });
            },
            '.' => return self.node(.{ .tag = .any, .flags = self.flags }),
            '^' => return self.node(.{ .tag = .assertion, .flags = self.flags, .assertion = .start }),
            '$' => return self.node(.{ .tag = .assertion, .flags = self.flags, .assertion = .end }),
            '\\' => return self.escape(false),
            '*', '+', '?' => return error.InvalidRegex,
            '{' => {
                if (!self.advanced or try self.bracedQuantifier(self.pos - 1) != null) return error.InvalidRegex;
                return self.scalar('{');
            },
            '}' => {
                if (!self.advanced) return error.InvalidRegex;
                return self.scalar('}');
            },
            else => {
                self.pos -= 1;
                const cp = try self.codepoint();
                return self.node(.{ .tag = .literal, .flags = self.flags, .cp = cp });
            },
        }
    }
    fn group(self: *Parser) Error!*const Node {
        const old = self.flags;
        if (self.advanced and self.pos < self.text.len and self.text[self.pos] == '*') {
            self.pos += 1;
            const first = self.pos;
            while (self.pos < self.text.len and self.text[self.pos] != ')' and self.text[self.pos] != ':') self.pos += 1;
            if (self.pos < self.text.len and self.text[self.pos] == ':') {
                const name = self.text[first..self.pos];
                const script_run = std.mem.eql(u8, name, "sr") or std.mem.eql(u8, name, "script_run") or std.mem.eql(u8, name, "asr") or std.mem.eql(u8, name, "atomic_script_run");
                const scan = std.mem.eql(u8, name, "scs") or std.mem.eql(u8, name, "scan_substring");
                if (script_run or scan) {
                    self.pos += 1;
                    var refs: std.ArrayList(GroupRef) = .empty;
                    if (scan) {
                        if (self.pos == self.text.len or self.text[self.pos] != '(') return error.InvalidRegex;
                        self.pos += 1;
                        while (true) {
                            while (self.pos < self.text.len and (self.text[self.pos] == ' ' or self.text[self.pos] == '\t')) self.pos += 1;
                            if (self.pos == self.text.len) return error.InvalidRegex;
                            const quote = self.text[self.pos];
                            if (quote == '\'' or quote == '<') {
                                self.pos += 1;
                                const begin = self.pos;
                                const close: u8 = if (quote == '<') '>' else '\'';
                                while (self.pos < self.text.len and self.text[self.pos] != close) self.pos += 1;
                                if (self.pos == self.text.len or self.pos == begin) return error.InvalidRegex;
                                try refs.append(self.a, .{ .name = try self.a.dupe(u8, self.text[begin..self.pos]) });
                                self.pos += 1;
                            } else {
                                const begin = self.pos;
                                if (quote == '-' or quote == '+') self.pos += 1;
                                const digits = self.pos;
                                while (self.pos < self.text.len and std.ascii.isDigit(self.text[self.pos])) self.pos += 1;
                                if (digits == self.pos) return error.InvalidRegex;
                                const ref = try self.reference(self.text[begin..self.pos]);
                                try refs.append(self.a, .{ .group = ref.group });
                            }
                            while (self.pos < self.text.len and (self.text[self.pos] == ' ' or self.text[self.pos] == '\t')) self.pos += 1;
                            if (self.pos == self.text.len) return error.InvalidRegex;
                            const separator = self.text[self.pos];
                            self.pos += 1;
                            if (separator == ')') break;
                            if (separator != ',') return error.InvalidRegex;
                        }
                    }
                    var child = try self.expr();
                    self.skip();
                    if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
                    self.pos += 1;
                    self.flags = old;
                    if (std.mem.eql(u8, name, "asr") or std.mem.eql(u8, name, "atomic_script_run")) child = try self.node(.{ .tag = .atomic, .flags = old, .child = child });
                    return self.node(.{ .tag = if (scan) .scan else .script_run, .flags = old, .child = child, .scan_refs = refs.items });
                }
                const Alias = struct { name: []const u8, behind: bool = false, negative: bool = false, atomic: bool = false, non_atomic: bool = false };
                const aliases = [_]Alias{ .{ .name = "pla" }, .{ .name = "positive_lookahead" }, .{ .name = "nla", .negative = true }, .{ .name = "negative_lookahead", .negative = true }, .{ .name = "plb", .behind = true }, .{ .name = "positive_lookbehind", .behind = true }, .{ .name = "nlb", .behind = true, .negative = true }, .{ .name = "negative_lookbehind", .behind = true, .negative = true }, .{ .name = "atomic", .atomic = true }, .{ .name = "napla", .non_atomic = true }, .{ .name = "non_atomic_positive_lookahead", .non_atomic = true }, .{ .name = "naplb", .behind = true, .non_atomic = true }, .{ .name = "non_atomic_positive_lookbehind", .behind = true, .non_atomic = true } };
                for (aliases) |alias| if (std.mem.eql(u8, name, alias.name)) {
                    self.pos += 1;
                    const child = try self.expr();
                    self.skip();
                    if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
                    self.pos += 1;
                    self.flags = old;
                    return self.node(.{ .tag = if (alias.atomic) .atomic else .look, .flags = old, .child = child, .behind = alias.behind, .negative = alias.negative, .non_atomic = alias.non_atomic });
                };
            }
            self.pos = first;
            while (self.pos < self.text.len and self.text[self.pos] != ')') self.pos += 1;
            if (self.pos == self.text.len) return error.InvalidRegex;
            const raw = self.text[first..self.pos];
            self.pos += 1;
            const colon = std.mem.indexOfScalar(u8, raw, ':');
            const name = if (colon) |i| raw[0..i] else raw;
            const argument = if (colon) |i| raw[i + 1 ..] else null;
            const verb: Verb = if (std.mem.eql(u8, name, "FAIL") or std.mem.eql(u8, name, "F")) .fail else if (std.mem.eql(u8, name, "ACCEPT")) .accept else if (std.mem.eql(u8, name, "SKIP")) .skip else if (std.mem.eql(u8, name, "PRUNE")) .prune else if (std.mem.eql(u8, name, "COMMIT")) .commit else if (std.mem.eql(u8, name, "THEN")) .then else if (std.mem.eql(u8, name, "MARK") or (name.len == 0 and argument != null)) .mark else return error.UnsupportedRegex;
            if ((verb == .mark and argument == null) or (argument != null and argument.?.len == 0)) return error.InvalidRegex;
            return self.node(.{ .tag = .verb, .flags = old, .cp = @backingInt(verb), .ref_name = argument });
        }
        if (self.advanced and self.pos < self.text.len and self.text[self.pos] == '?') {
            const tail = self.text[self.pos..];
            if (std.mem.startsWith(u8, tail, "?[")) {
                self.pos += 2;
                self.flags.extended = true;
                self.flags.extended_more = true;
                const value = try self.setExpression(0);
                self.skip();
                if (!std.mem.startsWith(u8, self.text[self.pos..], "])")) return error.InvalidRegex;
                self.pos += 2;
                self.flags = old;
                var node_flags = old;
                node_flags.insensitive = false;
                return self.node(.{ .tag = .class, .flags = node_flags, .class = value });
            }
            if (std.mem.startsWith(u8, tail, "?C")) {
                self.pos += 2;
                var number: u8 = 0;
                var string: ?[]const u8 = null;
                if (self.pos < self.text.len and std.mem.indexOfScalar(u8, "`'\"^%#${", self.text[self.pos]) != null) {
                    const open = self.text[self.pos];
                    const close: u8 = if (open == '{') '}' else open;
                    self.pos += 1;
                    var value: std.ArrayList(u8) = .empty;
                    while (true) {
                        if (self.pos == self.text.len) return error.InvalidRegex;
                        const ch = self.text[self.pos];
                        self.pos += 1;
                        if (ch == close) {
                            if (self.pos == self.text.len or self.text[self.pos] != close) break;
                            self.pos += 1;
                        }
                        try value.append(self.a, ch);
                    }
                    string = value.items;
                } else {
                    const first = self.pos;
                    while (self.pos < self.text.len and std.ascii.isDigit(self.text[self.pos])) self.pos += 1;
                    if (self.pos != first) number = std.fmt.parseInt(u8, self.text[first..self.pos], 10) catch return error.InvalidRegex;
                }
                if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
                self.pos += 1;
                return self.node(.{ .tag = .callout, .flags = old, .cp = number, .ref_name = string, .group = self.pos });
            }
            if (std.mem.startsWith(u8, tail, "?|")) {
                self.pos += 2;
                const base = self.groups;
                var maximum = base;
                var alternatives: std.ArrayList(*const Node) = .empty;
                while (true) {
                    self.groups = base;
                    try alternatives.append(self.a, try self.concat());
                    maximum = @max(maximum, self.groups);
                    self.skip();
                    if (self.pos == self.text.len) return error.InvalidRegex;
                    const terminator = self.text[self.pos];
                    self.pos += 1;
                    if (terminator == ')') break;
                    if (terminator != '|') return error.InvalidRegex;
                }
                self.groups = maximum;
                self.flags = old;
                return self.node(.{ .tag = .alt, .flags = old, .children = alternatives.items });
            }
            if (std.mem.startsWith(u8, tail, "?R)") or std.mem.startsWith(u8, tail, "?0)")) {
                self.pos += 3;
                return self.node(.{ .tag = .subroutine, .flags = old, .group = 0 });
            }
            if (tail.len > 1 and (std.ascii.isDigit(tail[1]) or tail[1] == '-' or tail[1] == '+' or tail[1] == '&' or std.mem.startsWith(u8, tail, "?P>"))) {
                self.pos += if (std.mem.startsWith(u8, tail, "?P>")) @as(usize, 3) else if (tail[1] == '&') 2 else 1;
                const first = self.pos;
                while (self.pos < self.text.len and self.text[self.pos] != ')') self.pos += 1;
                if (self.pos == self.text.len) return error.InvalidRegex;
                const name = self.text[first..self.pos];
                self.pos += 1;
                const relative = std.fmt.parseInt(isize, name, 10) catch null;
                const target_group: usize = if (relative) |n| if (n < 0) std.math.cast(usize, @as(isize, @intCast(self.groups)) + n + 1) orelse return error.InvalidRegex else if (name.len > 0 and name[0] == '+') std.math.add(usize, self.groups, @intCast(n)) catch return error.InvalidRegex else @intCast(n) else 0;
                return self.node(.{ .tag = .subroutine, .flags = old, .group = target_group, .ref_name = if (relative == null) name else null });
            }
            if (std.mem.startsWith(u8, tail, "?(")) {
                self.pos += 2;
                const assertion_condition = std.mem.startsWith(u8, self.text[self.pos..], "?=") or std.mem.startsWith(u8, self.text[self.pos..], "?!") or std.mem.startsWith(u8, self.text[self.pos..], "?<=") or std.mem.startsWith(u8, self.text[self.pos..], "?<!");
                const look = if (assertion_condition) try self.group() else null;
                const groups_before = self.groups;
                var condition: []const u8 = "";
                if (look == null) {
                    const first = self.pos;
                    while (self.pos < self.text.len and self.text[self.pos] != ')') self.pos += 1;
                    if (self.pos == self.text.len) return error.InvalidRegex;
                    condition = self.text[first..self.pos];
                    if (self.flags.extended) condition = std.mem.trim(u8, condition, " \t\r\n");
                    self.pos += 1;
                    if (std.mem.startsWith(u8, condition, "R&")) condition = condition[1..];
                    if (condition.len >= 2 and ((condition[0] == '<' and condition[condition.len - 1] == '>') or (condition[0] == '\'' and condition[condition.len - 1] == '\''))) condition = condition[1 .. condition.len - 1];
                }
                const branches = try self.expr();
                if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
                self.pos += 1;
                const children = try self.a.alloc(*const Node, 2);
                if (branches.tag == .alt) {
                    if (branches.children.len != 2) return error.InvalidRegex;
                    @memcpy(children, branches.children);
                } else {
                    children[0] = branches;
                    children[1] = try self.node(.{ .tag = .empty, .flags = old });
                }
                const recursive_group = condition.len > 1 and ((condition[0] == 'R' and std.ascii.isDigit(condition[1])) or condition[0] == '&');
                if (recursive_group) condition = condition[1..];
                const relative = condition.len > 0 and (condition[0] == '+' or condition[0] == '-');
                const number: ?usize = if (relative) blk: {
                    const n = std.fmt.parseInt(isize, condition, 10) catch return error.InvalidRegex;
                    const target = std.math.add(isize, @intCast(groups_before), n) catch return error.InvalidRegex;
                    break :blk std.math.cast(usize, target + @as(isize, if (n < 0) 1 else 0)) orelse return error.InvalidRegex;
                } else std.fmt.parseInt(usize, condition, 10) catch null;
                const version: ?bool = if (!recursive_group and (std.mem.startsWith(u8, condition, "VERSION=") or std.mem.startsWith(u8, condition, "VERSION>="))) blk: {
                    const comparison = condition[7..];
                    const greater = std.mem.startsWith(u8, comparison, ">=");
                    if (!greater and !std.mem.startsWith(u8, comparison, "=")) return error.InvalidRegex;
                    var parts = std.mem.splitScalar(u8, comparison[if (greater) @as(usize, 2) else 1..], '.');
                    const major = std.fmt.parseInt(u16, parts.next() orelse return error.InvalidRegex, 10) catch return error.InvalidRegex;
                    const minor = std.fmt.parseInt(u16, parts.next() orelse return error.InvalidRegex, 10) catch return error.InvalidRegex;
                    if (parts.next() != null or major > 255 or minor > 99) return error.InvalidRegex;
                    const requested = @as(u32, major) * 100 + minor;
                    break :blk if (greater) 1045 >= requested else 1045 == requested;
                } else null;
                const special = !recursive_group and (version != null or std.mem.eql(u8, condition, "R") or std.mem.eql(u8, condition, "DEFINE"));
                const condition_kind: Condition = if (recursive_group) .group else if (version != null) .version else if (std.mem.eql(u8, condition, "R")) .recursion else if (std.mem.eql(u8, condition, "DEFINE")) .define else .group;
                const target_group = if (special) 0 else number orelse 0;
                if (condition_kind == .define and branches.tag == .alt) return error.InvalidRegex;
                self.flags = old;
                return self.node(.{ .tag = .conditional, .condition = condition_kind, .recursion_condition = recursive_group, .flags = old, .cp = if (version orelse false) 1 else 0, .group = target_group, .ref_name = if (number == null and !special and look == null) condition else null, .child = if (look) |l| l.child else null, .negative = if (look) |l| l.negative else false, .behind = if (look) |l| l.behind else false, .children = children });
            }

            if (std.mem.startsWith(u8, tail, "?#")) {
                while (self.pos < self.text.len and self.text[self.pos] != ')') self.pos += 1;
                if (self.pos == self.text.len) return error.InvalidRegex;
                self.pos += 1;
                return self.node(.{ .tag = .empty, .flags = old });
            }
            const behind = std.mem.startsWith(u8, tail, "?<=") or std.mem.startsWith(u8, tail, "?<!") or std.mem.startsWith(u8, tail, "?<*");
            const non_atomic = std.mem.startsWith(u8, tail, "?*") or std.mem.startsWith(u8, tail, "?<*");
            const look = behind or non_atomic or std.mem.startsWith(u8, tail, "?=") or std.mem.startsWith(u8, tail, "?!");
            const atomic = std.mem.startsWith(u8, tail, "?>");
            if (look or atomic) {
                const negative = if (behind) tail[2] == '!' else tail[1] == '!';
                self.pos += if (behind) @as(usize, 3) else 2;
                const child = try self.expr();
                self.skip();
                if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
                self.pos += 1;
                self.flags = old;
                return self.node(.{ .tag = if (atomic) .atomic else .look, .flags = old, .child = child, .negative = negative, .behind = behind, .non_atomic = non_atomic });
            }
            if (std.mem.startsWith(u8, tail, "?P=")) {
                self.pos += 3;
                const start = self.pos;
                while (self.pos < self.text.len and self.text[self.pos] != ')') self.pos += 1;
                if (self.pos == self.text.len) return error.InvalidRegex;
                const name = self.text[start..self.pos];
                self.pos += 1;
                return self.reference(name);
            }
        }
        var capture = !self.flags.no_auto_capture;
        var name: ?[]const u8 = null;
        if (self.pos < self.text.len and self.text[self.pos] == '?') {
            self.pos += 1;
            if (self.pos == self.text.len) return error.InvalidRegex;
            if (self.text[self.pos] == ':') {
                capture = false;
                self.pos += 1;
            } else if (self.text[self.pos] == 'P' or self.text[self.pos] == '<' or (self.advanced and self.text[self.pos] == '\'')) {
                if (self.text[self.pos] == 'P') self.pos += 1;
                if (self.pos == self.text.len) return error.InvalidRegex;
                const closer: u8 = if (self.text[self.pos] == '\'') '\'' else '>';
                if (self.text[self.pos] != '<' and self.text[self.pos] != '\'') return error.InvalidRegex;
                capture = true;
                self.pos += 1;
                const start = self.pos;
                while (self.pos < self.text.len and self.text[self.pos] != closer) self.pos += 1;
                if (self.pos == self.text.len or self.pos == start) return error.InvalidRegex;
                name = self.text[start..self.pos];
                self.pos += 1;
                var name_offset: usize = 0;
                while (name_offset < name.?.len) {
                    const decoded = unicode.decode(name.?, name_offset, self.advanced and self.flags.unicode);
                    if (self.advanced and self.flags.unicode) {
                        if (decoded.cp != '_' and !unicode.contains(unicode.property("L").?, decoded.cp) and !(name_offset > 0 and unicode.contains(unicode.property("Nd").?, decoded.cp))) return error.InvalidRegex;
                    } else if (decoded.cp > 127 or !(std.ascii.isAlphanumeric(@intCast(decoded.cp)) or decoded.cp == '_') or (name_offset == 0 and std.ascii.isDigit(@intCast(decoded.cp)))) return error.InvalidRegex;
                    name_offset += decoded.len;
                }
                for (self.names.items) |existing| if (std.mem.eql(u8, existing.name, name.?) and existing.index != self.groups + 1 and !self.flags.duplicate_names) return error.InvalidRegex;
            } else {
                capture = false;
                var enabled = true;
                var any = false;
                var reset = false;
                if (self.advanced and self.text[self.pos] == '^') {
                    self.pos += 1;
                    reset = true;
                    any = true;
                    self.flags.insensitive = false;
                    self.flags.multiline = false;
                    self.flags.no_auto_capture = false;
                    self.flags.caseless_restrict = false;
                    self.flags.dotall = false;
                    self.flags.extended = false;
                    self.flags.extended_more = false;
                }
                while (self.pos < self.text.len and self.text[self.pos] != ':' and self.text[self.pos] != ')') {
                    const f = self.text[self.pos];
                    self.pos += 1;
                    if (f == '-') {
                        if (self.advanced and (!enabled or reset)) return error.InvalidRegex;
                        enabled = false;
                        continue;
                    }
                    any = true;
                    switch (f) {
                        'a' => {
                            if (!self.advanced) return error.UnsupportedRegex;
                            const option = if (self.pos < self.text.len) self.text[self.pos] else 0;
                            if (std.mem.indexOfScalar(u8, "DSWPT", option) != null) {
                                self.pos += 1;
                                switch (option) {
                                    'D' => self.flags.ascii_bsd = enabled,
                                    'S' => self.flags.ascii_bss = enabled,
                                    'W' => self.flags.ascii_bsw = enabled,
                                    'P' => {
                                        self.flags.ascii_posix = enabled;
                                        self.flags.ascii_digit = enabled;
                                    },
                                    'T' => self.flags.ascii_digit = enabled,
                                    else => unreachable,
                                }
                            } else {
                                self.flags.ascii_bsd = enabled;
                                self.flags.ascii_bss = enabled;
                                self.flags.ascii_bsw = enabled;
                                self.flags.ascii_posix = enabled;
                                self.flags.ascii_digit = enabled;
                            }
                        },
                        'r' => {
                            if (!self.advanced) return error.UnsupportedRegex;
                            self.flags.caseless_restrict = enabled;
                        },
                        'J' => {
                            if (!self.advanced) return error.UnsupportedRegex;
                            self.flags.duplicate_names = enabled;
                        },
                        'n' => {
                            if (!self.advanced) return error.UnsupportedRegex;
                            self.flags.no_auto_capture = enabled;
                        },
                        'i' => self.flags.insensitive = enabled,
                        'm' => self.flags.multiline = enabled,
                        's' => self.flags.dotall = enabled,
                        'U' => self.flags.ungreedy = enabled,
                        'x' => {
                            self.flags.extended = enabled;
                            if (!enabled) self.flags.extended_more = false else if (self.advanced and self.pos < self.text.len and self.text[self.pos] == 'x') {
                                self.pos += 1;
                                self.flags.extended_more = true;
                            }
                        },
                        'R' => self.flags.crlf = enabled,
                        'u' => {
                            if (self.advanced) return error.UnsupportedRegex;
                            if (enabled and !self.unicode) return error.UnsupportedRegex;
                            self.flags.unicode = enabled;
                        },
                        else => return error.UnsupportedRegex,
                    }
                }
                if ((!any and !self.advanced) or self.pos == self.text.len) return error.InvalidRegex;
                if (self.text[self.pos] == ')') {
                    self.pos += 1;
                    return self.node(.{ .tag = .empty, .flags = self.flags });
                }
                self.pos += 1;
            }
        }
        const index = if (capture) blk: {
            self.groups += 1;
            if (self.groups > 128) return error.RegexTooLarge;
            break :blk self.groups;
        } else 0;
        if (name) |n| try self.names.append(self.a, .{ .name = try self.a.dupe(u8, n), .index = index });
        const child = try self.expr();
        self.skip();
        if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
        self.pos += 1;
        self.flags = old;
        return if (capture) self.node(.{ .tag = .capture, .flags = old, .child = child, .group = index }) else child;
    }
    fn codepoint(self: *Parser) Error!u32 {
        if (self.pos == self.text.len) return error.InvalidRegex;
        const d = unicode.decode(self.text, self.pos, self.unicode);
        if (d.cp > 0x10ffff) return error.InvalidRegex;
        self.pos += d.len;
        return d.cp;
    }
    fn propertyClass(self: *Parser, negate: bool) Error!*const Class {
        if (!self.flags.unicode and !self.advanced) return error.InvalidRegex;
        var raw: []const u8 = undefined;
        if (self.pos < self.text.len and self.text[self.pos] == '{') {
            self.pos += 1;
            const start = self.pos;
            while (self.pos < self.text.len and self.text[self.pos] != '}') self.pos += 1;
            if (self.pos == self.text.len) return error.InvalidRegex;
            raw = self.text[start..self.pos];
            self.pos += 1;
        } else {
            const start = self.pos;
            _ = try self.codepoint();
            raw = self.text[start..self.pos];
        }
        var negative = negate;
        if (self.advanced and raw.len > 0 and raw[0] == '^') {
            raw = raw[1..];
            negative = !negative;
        }
        var normalized: [256]u8 = undefined;
        if (std.mem.indexOf(u8, raw, "!=")) |at| {
            if (raw.len - 1 > normalized.len) return error.InvalidRegex;
            @memcpy(normalized[0..at], raw[0..at]);
            @memcpy(normalized[at..][0 .. raw.len - at - 1], raw[at + 1 ..]);
            raw = normalized[0 .. raw.len - 1];
            negative = !negative;
        }
        const result = if (self.advanced) try self.pcreProperty(raw) else try self.cls(.{ .property = unicode.property(raw) orelse return error.InvalidRegex });
        return if (negative) self.cls(.{ .negate = result }) else result;
    }
    fn pcreProperty(self: *Parser, raw: []const u8) Error!*const Class {
        if (std.ascii.eqlIgnoreCase(raw, "Xan") or std.ascii.eqlIgnoreCase(raw, "Xwd")) {
            var result = try self.cls(.{ .either = .{ .a = try self.cls(.{ .exact_property = unicode.property("L").? }), .b = try self.cls(.{ .exact_property = unicode.property("N").? }) } });
            if (std.ascii.eqlIgnoreCase(raw, "Xwd")) for ([_][]const u8{ "Mn", "Pc" }) |name| {
                result = try self.cls(.{ .either = .{ .a = result, .b = try self.cls(.{ .exact_property = unicode.property(name).? }) } });
            };
            return result;
        }
        if (std.ascii.eqlIgnoreCase(raw, "Xps") or std.ascii.eqlIgnoreCase(raw, "Xsp")) return self.cls(.{ .exact_property = unicode.property("space").? });
        if (std.ascii.eqlIgnoreCase(raw, "Xuc")) return self.cls(.{ .exact_property = &.{ .{ .lo = '$', .hi = '$' }, .{ .lo = '@', .hi = '@' }, .{ .lo = '`', .hi = '`' }, .{ .lo = 0xa0, .hi = 0x10ffff } } });
        if (std.mem.indexOfAny(u8, raw, "=:") == null) {
            var script: [128]u8 = undefined;
            if (raw.len <= script.len - 4) {
                @memcpy(script[0..4], "scx:");
                @memcpy(script[4..][0..raw.len], raw);
                if (unicode.property(script[0 .. raw.len + 4])) |ranges| return self.cls(.{ .exact_property = ranges });
            }
        }
        const ranges = unicode.property(if (std.ascii.eqlIgnoreCase(raw, "L&")) "LC" else raw) orelse return error.InvalidRegex;
        if (self.flags.insensitive) for ([_][]const u8{ "Lu", "Ll", "Lt" }) |name| {
            const category = unicode.property(name).?;
            if (ranges.ptr == category.ptr and ranges.len == category.len) return self.cls(.{ .exact_property = unicode.property("LC").? });
        };
        return self.cls(.{ .exact_property = ranges });
    }
    fn scalar(self: *Parser, value: u32) Error!*const Node {
        if (value > 0x10ffff or (value >= 0xd800 and value <= 0xdfff) or (!self.flags.unicode and value > 255)) return error.InvalidRegex;
        return self.node(.{ .tag = .literal, .flags = self.flags, .cp = value });
    }
    fn shorthand(self: *Parser, c: u8) Error!*const Class {
        const key: []const u8 = switch (std.ascii.toLower(c)) {
            'd' => "digit",
            'w' => "word",
            's' => "space",
            else => unreachable,
        };
        var value = if (self.advanced and c != 's' and c != 'S' and c != 'd' and c != 'D') try self.pcreProperty("Xwd") else try self.cls(.{ .property = unicode.property(key).? });
        const ascii = switch (std.ascii.toLower(c)) {
            'd' => self.flags.ascii_bsd,
            's' => self.flags.ascii_bss,
            'w' => self.flags.ascii_bsw,
            else => false,
        };
        if (self.advanced and (!self.flags.unicode or ascii)) {
            const ranges: []const unicode.Range = switch (std.ascii.toLower(c)) {
                'd' => &.{.{ .lo = '0', .hi = '9' }},
                's' => &.{ .{ .lo = 9, .hi = 13 }, .{ .lo = 32, .hi = 32 } },
                'w' => &.{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'Z' }, .{ .lo = '_', .hi = '_' }, .{ .lo = 'a', .hi = 'z' } },
                else => unreachable,
            };
            value = try self.cls(.{ .ascii_property = ranges });
        } else if (!self.flags.unicode or ascii) value = try self.cls(.{ .intersect = .{ .a = value, .b = try self.cls(.{ .range = .{ .lo = 0, .hi = 127 } }) } });
        return if (std.ascii.isUpper(c)) self.cls(.{ .negate = value }) else value;
    }
    fn escape(self: *Parser, in_class: bool) Error!*const Node {
        if (self.pos == self.text.len) return error.InvalidRegex;
        const c = self.text[self.pos];
        self.pos += 1;
        if (self.advanced) {
            if (c == 'u' or c == 'U') return error.UnsupportedRegex;
            if (!in_class and c == 'E') return self.node(.{ .tag = .empty, .flags = self.flags });
            if (c == 'o' or (c == 'N' and self.pos < self.text.len and self.text[self.pos] == '{')) {
                const octal = c == 'o';
                if (self.pos == self.text.len or self.text[self.pos] != '{') return error.InvalidRegex;
                self.pos += 1;
                while (self.pos < self.text.len and (self.text[self.pos] == ' ' or self.text[self.pos] == '\t')) self.pos += 1;
                if (!octal) {
                    if (!std.mem.startsWith(u8, self.text[self.pos..], "U+")) return error.UnsupportedRegex;
                    self.pos += 2;
                }
                const begin = self.pos;
                while (self.pos < self.text.len and self.text[self.pos] != '}') self.pos += 1;
                if (self.pos == self.text.len) return error.InvalidRegex;
                const raw = std.mem.trim(u8, self.text[begin..self.pos], " \t");
                if (raw.len == 0) return error.InvalidRegex;
                const value = std.fmt.parseInt(u32, raw, if (octal) 8 else 16) catch return error.InvalidRegex;
                self.pos += 1;
                return self.scalar(value);
            }
            if (in_class and c >= '1' and c <= '9') {
                if (c > '7') return error.InvalidRegex;
                var value: u32 = c - '0';
                var n: usize = 1;
                while (self.pos < self.text.len and n < 3 and self.text[self.pos] >= '0' and self.text[self.pos] <= '7') : (n += 1) {
                    value = value * 8 + self.text[self.pos] - '0';
                    self.pos += 1;
                }
                return self.scalar(value);
            }
            if (c == '0') {
                var value: u32 = 0;
                var n: usize = 0;
                while (self.pos < self.text.len and n < 2 and self.text[self.pos] >= '0' and self.text[self.pos] <= '7') : (n += 1) {
                    value = value * 8 + self.text[self.pos] - '0';
                    self.pos += 1;
                }
                return self.node(.{ .tag = .literal, .flags = self.flags, .cp = value });
            }
            if (c == 'e' or (c == 'b' and in_class)) return self.node(.{ .tag = .literal, .flags = self.flags, .cp = if (c == 'e') 27 else 8 });
            if (c == 'c') {
                if (self.pos == self.text.len) return error.InvalidRegex;
                const ch = self.text[self.pos];
                self.pos += 1;
                return self.node(.{ .tag = .literal, .flags = self.flags, .cp = std.ascii.toUpper(ch) ^ 64 });
            }
            if (std.mem.indexOfScalar(u8, "hHvV", c) != null) {
                const vertical = c == 'v' or c == 'V';
                var cls_node = try self.cls(.{ .property = if (vertical) &.{ .{ .lo = 10, .hi = 13 }, .{ .lo = 0x85, .hi = 0x85 }, .{ .lo = 0x2028, .hi = 0x2029 } } else &.{ .{ .lo = 9, .hi = 9 }, .{ .lo = 32, .hi = 32 }, .{ .lo = 0xa0, .hi = 0xa0 }, .{ .lo = 0x1680, .hi = 0x1680 }, .{ .lo = 0x180e, .hi = 0x180e }, .{ .lo = 0x2000, .hi = 0x200a }, .{ .lo = 0x202f, .hi = 0x202f }, .{ .lo = 0x205f, .hi = 0x205f }, .{ .lo = 0x3000, .hi = 0x3000 } } });
                if (std.ascii.isUpper(c)) cls_node = try self.cls(.{ .negate = cls_node });
                return self.node(.{ .tag = .class, .flags = self.flags, .class = cls_node });
            }
            if (!in_class and c == 'X') return self.node(.{ .tag = .grapheme, .flags = self.flags });
            if (!in_class and (c == 'Z' or c == 'G')) return self.node(.{ .tag = .assertion, .flags = self.flags, .assertion = if (c == 'Z') .soft_end else .previous_match });
            if (!in_class and (c == 'N' or c == 'C')) {
                var flags = self.flags;
                if (c == 'N') flags.dotall = false else {
                    flags.unicode = false;
                    flags.dotall = true;
                }
                return self.node(.{ .tag = .any, .flags = flags });
            }
            if (!in_class and c == 'R') {
                const pair = try self.a.alloc(*const Node, 2);
                pair[0] = try self.node(.{ .tag = .literal, .flags = self.flags, .cp = 13 });
                pair[1] = try self.node(.{ .tag = .literal, .flags = self.flags, .cp = 10 });
                const alts = try self.a.alloc(*const Node, 2);
                alts[0] = try self.node(.{ .tag = .seq, .flags = self.flags, .children = pair });
                alts[1] = try self.node(.{ .tag = .class, .flags = self.flags, .class = try self.cls(.{ .property = &.{ .{ .lo = 10, .hi = 13 }, .{ .lo = 0x85, .hi = 0x85 }, .{ .lo = 0x2028, .hi = 0x2029 } } }) });
                return self.node(.{ .tag = .atomic, .flags = self.flags, .child = try self.node(.{ .tag = .alt, .flags = self.flags, .children = alts }) });
            }
        }
        if (self.advanced and !in_class) {
            if (c >= '1' and c <= '9') {
                const start = self.pos - 1;
                while (self.pos < self.text.len and std.ascii.isDigit(self.text[self.pos])) self.pos += 1;
                const number = std.fmt.parseInt(usize, self.text[start..self.pos], 10) catch return error.InvalidRegex;
                if (number < 10 or number <= self.groups) return self.node(.{ .tag = .reference, .flags = self.flags, .group = number });
                if (c > '7') return error.InvalidRegex;
                self.pos = start;
                var value: u32 = 0;
                var n: usize = 0;
                while (self.pos < self.text.len and n < 3 and self.text[self.pos] >= '0' and self.text[self.pos] <= '7') : (n += 1) {
                    value = value * 8 + self.text[self.pos] - '0';
                    self.pos += 1;
                }
                return self.scalar(value);
            }
            if (c == 'k' or c == 'g') {
                if (self.pos == self.text.len) return error.InvalidRegex;
                const end: u8 = switch (self.text[self.pos]) {
                    '<' => '>',
                    '{' => '}',
                    '\'' => '\'',
                    else => return error.InvalidRegex,
                };
                self.pos += 1;
                const start = self.pos;
                while (self.pos < self.text.len and self.text[self.pos] != end) self.pos += 1;
                if (self.pos == self.text.len) return error.InvalidRegex;
                const raw_name = self.text[start..self.pos];
                const name = if (end == '}') std.mem.trim(u8, raw_name, " \t") else raw_name;
                self.pos += 1;
                const ref = try self.reference(name);
                if (c == 'g' and end != '}') {
                    var value = ref.*;
                    value.tag = .subroutine;
                    return self.node(value);
                }
                return ref;
            }
            if (c == 'K') return self.node(.{ .tag = .reset, .flags = self.flags });
            if (c == 'Q') {
                var children: std.ArrayList(*const Node) = .empty;
                while (self.pos < self.text.len and !std.mem.startsWith(u8, self.text[self.pos..], "\\E")) try children.append(self.a, try self.node(.{ .tag = .literal, .flags = self.flags, .cp = try self.codepoint() }));
                if (self.pos < self.text.len) self.pos += 2;
                return self.node(.{ .tag = .seq, .flags = self.flags, .children = children.items });
            }
        }
        if (std.mem.indexOfScalar(u8, "dDwWsS", c) != null) return self.node(.{ .tag = .class, .flags = self.flags, .class = try self.shorthand(c) });
        if (c == 'p' or c == 'P') return self.node(.{ .tag = .class, .flags = self.flags, .class = try self.propertyClass(c == 'P') });
        if (!in_class and std.mem.indexOfScalar(u8, "bBAz", c) != null) {
            const kind: Assert = switch (c) {
                'b' => .word,
                'B' => .not_word,
                'A' => .absolute_start,
                'z' => .absolute_end,
                else => unreachable,
            };
            return self.node(.{ .tag = .assertion, .flags = self.flags, .assertion = kind });
        }
        const cp: u32 = switch (c) {
            't' => '\t',
            'r' => '\r',
            'n' => '\n',
            'f' => 12,
            'v' => 11,
            'a' => 7,
            'x', 'u', 'U' => blk: {
                var digits: usize = if (c == 'x') 2 else if (c == 'u') 4 else 8;
                var braced = false;
                if (self.pos < self.text.len and self.text[self.pos] == '{') {
                    braced = true;
                    self.pos += 1;
                    digits = 0;
                    while (self.pos + digits < self.text.len and self.text[self.pos + digits] != '}') digits += 1;
                }
                if (digits > self.text.len - self.pos) return error.InvalidRegex;
                const raw = self.text[self.pos..][0..digits];
                const hexadecimal = if (self.advanced and braced) std.mem.trim(u8, raw, " \t") else raw;
                if (hexadecimal.len == 0 or hexadecimal.len > 8) return error.InvalidRegex;
                const value = std.fmt.parseInt(u32, hexadecimal, 16) catch return error.InvalidRegex;
                self.pos += digits;
                if (braced) {
                    if (self.pos == self.text.len or self.text[self.pos] != '}') return error.InvalidRegex;
                    self.pos += 1;
                }
                if (value > 0x10ffff or (value >= 0xd800 and value <= 0xdfff) or (!self.flags.unicode and value > 255)) return error.InvalidRegex;
                break :blk value;
            },
            else => if (std.ascii.isAlphanumeric(c)) return error.UnsupportedRegex else c,
        };
        return self.node(.{ .tag = .literal, .flags = self.flags, .cp = cp });
    }
    fn reference(self: *Parser, name: []const u8) Error!*const Node {
        var number = std.fmt.parseInt(usize, name, 10) catch null;
        if (name.len > 0 and (name[0] == '-' or name[0] == '+')) {
            const relative = std.fmt.parseInt(isize, name, 10) catch return error.InvalidRegex;
            const target = std.math.add(isize, @intCast(self.groups), relative) catch return error.InvalidRegex;
            number = std.math.cast(usize, target + @as(isize, if (relative < 0) 1 else 0)) orelse return error.InvalidRegex;
        }
        return self.node(.{ .tag = .reference, .flags = self.flags, .group = number orelse 0, .ref_name = if (number == null) name else null });
    }
    fn posixUnicode(self: *Parser, key: []const u8, ascii: *const Class) Error!*const Class {
        if (!self.advanced) return ascii;
        const restricted = try self.cls(.{ .ascii_property = ascii.property });
        if (!self.flags.unicode or self.flags.ascii_posix or std.mem.eql(u8, key, "ascii")) return restricted;
        const digit = std.mem.eql(u8, key, "digit");
        const hex = std.mem.eql(u8, key, "xdigit");
        if ((digit or hex) and self.flags.ascii_digit) return restricted;
        if (std.mem.eql(u8, key, "lower") or std.mem.eql(u8, key, "upper")) return self.cls(.{ .exact_property = unicode.property(if (std.mem.eql(u8, key, "lower")) "Ll" else "Lu").? });
        for ([_][2][]const u8{ .{ "alpha", "L" }, .{ "alnum", "Xan" }, .{ "cntrl", "Cc" }, .{ "digit", "Nd" }, .{ "lower", "Ll" }, .{ "upper", "Lu" }, .{ "space", "Xps" }, .{ "word", "Xwd" } }) |pair| if (std.mem.eql(u8, key, pair[0])) return self.pcreProperty(pair[1]);
        if (hex) return self.cls(.{ .either = .{ .a = ascii, .b = try self.cls(.{ .exact_property = &.{ .{ .lo = 0xff10, .hi = 0xff19 }, .{ .lo = 0xff21, .hi = 0xff26 }, .{ .lo = 0xff41, .hi = 0xff46 } } }) } });
        if (std.mem.eql(u8, key, "blank")) return self.cls(.{ .exact_property = &.{ .{ .lo = 9, .hi = 9 }, .{ .lo = 32, .hi = 32 }, .{ .lo = 0xa0, .hi = 0xa0 }, .{ .lo = 0x1680, .hi = 0x1680 }, .{ .lo = 0x180e, .hi = 0x180e }, .{ .lo = 0x2000, .hi = 0x200a }, .{ .lo = 0x202f, .hi = 0x202f }, .{ .lo = 0x205f, .hi = 0x205f }, .{ .lo = 0x3000, .hi = 0x3000 } } });
        if (std.mem.eql(u8, key, "punct")) return self.cls(.{ .either = .{ .a = try self.pcreProperty("P"), .b = try self.cls(.{ .intersect = .{ .a = try self.pcreProperty("S"), .b = try self.cls(.{ .range = .{ .lo = 0, .hi = 255 } }) } }) } });
        if (std.mem.eql(u8, key, "graph") or std.mem.eql(u8, key, "print")) {
            var value = try self.pcreProperty("L");
            for ([_][]const u8{ "M", "N", "P", "S", "Cf" }) |name| value = try self.cls(.{ .either = .{ .a = value, .b = try self.pcreProperty(name) } });
            value = try self.cls(.{ .difference = .{ .a = value, .b = try self.cls(.{ .exact_property = &.{ .{ .lo = 0x61c, .hi = 0x61c }, .{ .lo = 0x180e, .hi = 0x180e }, .{ .lo = 0x2066, .hi = 0x2069 } } }) } });
            if (std.mem.eql(u8, key, "print")) value = try self.cls(.{ .either = .{ .a = value, .b = try self.pcreProperty("Zs") } });
            return value;
        }
        return ascii;
    }
    fn classItem(self: *Parser) Error!*const Class {
        if (self.pos == self.text.len) return error.InvalidRegex;
        if (!self.advanced and self.text[self.pos] == '[') {
            self.pos += 1;
            return self.class();
        }
        const n = if (self.text[self.pos] == '\\') blk: {
            self.pos += 1;
            break :blk try self.escape(true);
        } else try self.node(.{ .tag = .literal, .flags = self.flags, .cp = try self.codepoint() });
        if (n.tag == .class) return n.class.?;
        if (n.tag != .literal) return error.InvalidRegex;
        if (self.pos + 1 < self.text.len and self.text[self.pos] == '-' and self.text[self.pos + 1] != ']' and (self.advanced or self.text[self.pos + 1] != '-')) {
            self.pos += 1;
            const end = if (self.text[self.pos] == '\\') blk: {
                self.pos += 1;
                const e = try self.escape(true);
                if (e.tag != .literal) return error.InvalidRegex;
                break :blk e.cp;
            } else try self.codepoint();
            if (end < n.cp) return error.InvalidRegex;
            return self.cls(.{ .range = .{ .lo = @intCast(n.cp), .hi = @intCast(end) } });
        }
        return self.cls(.{ .range = .{ .lo = @intCast(n.cp), .hi = @intCast(n.cp) } });
    }
    fn posixAtom(self: *Parser) Error!*const Class {
        self.pos += 2;
        const start = self.pos;
        while (self.pos + 1 < self.text.len and !std.mem.startsWith(u8, self.text[self.pos..], ":]")) self.pos += 1;
        if (self.pos + 1 >= self.text.len) return error.InvalidRegex;
        var key = self.text[start..self.pos];
        const negative = key.len > 0 and key[0] == '^';
        if (negative) key = key[1..];
        self.pos += 2;
        var raw: []const unicode.Range = &.{};
        if (std.mem.eql(u8, key, "digit")) raw = &.{.{ .lo = '0', .hi = '9' }} else if (std.mem.eql(u8, key, "alpha")) raw = &.{ .{ .lo = 'A', .hi = 'Z' }, .{ .lo = 'a', .hi = 'z' } } else if (std.mem.eql(u8, key, "alnum")) raw = &.{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'Z' }, .{ .lo = 'a', .hi = 'z' } } else if (std.mem.eql(u8, key, "word")) raw = &.{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'Z' }, .{ .lo = '_', .hi = '_' }, .{ .lo = 'a', .hi = 'z' } } else if (std.mem.eql(u8, key, "space")) raw = &.{ .{ .lo = 9, .hi = 13 }, .{ .lo = 32, .hi = 32 } } else if (std.mem.eql(u8, key, "blank")) raw = &.{ .{ .lo = 9, .hi = 9 }, .{ .lo = 32, .hi = 32 } } else if (std.mem.eql(u8, key, "lower")) raw = &.{.{ .lo = 'a', .hi = 'z' }} else if (std.mem.eql(u8, key, "upper")) raw = &.{.{ .lo = 'A', .hi = 'Z' }} else if (std.mem.eql(u8, key, "xdigit")) raw = &.{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'F' }, .{ .lo = 'a', .hi = 'f' } } else if (std.mem.eql(u8, key, "ascii")) raw = &.{.{ .lo = 0, .hi = 127 }} else if (std.mem.eql(u8, key, "cntrl")) raw = &.{ .{ .lo = 0, .hi = 31 }, .{ .lo = 127, .hi = 127 } } else if (std.mem.eql(u8, key, "graph")) raw = &.{.{ .lo = 33, .hi = 126 }} else if (std.mem.eql(u8, key, "print")) raw = &.{.{ .lo = 32, .hi = 126 }} else if (std.mem.eql(u8, key, "punct")) raw = &.{ .{ .lo = 33, .hi = 47 }, .{ .lo = 58, .hi = 64 }, .{ .lo = 91, .hi = 96 }, .{ .lo = 123, .hi = 126 } } else return error.InvalidRegex;
        var item = try self.posixUnicode(key, try self.cls(.{ .property = raw }));
        if (negative) item = try self.cls(.{ .negate = item });

        return item;
    }
    fn setAtom(self: *Parser) Error!*const Class {
        if (self.depth >= 128) return error.RegexTooLarge;
        self.depth += 1;
        defer self.depth -= 1;
        self.skip();
        if (self.pos == self.text.len) return error.InvalidRegex;
        if (self.text[self.pos] == '!') {
            self.pos += 1;
            return self.cls(.{ .negate = try self.setAtom() });
        }
        if (self.text[self.pos] == '(') {
            self.pos += 1;
            const value = try self.setExpression(0);
            self.skip();
            if (self.pos == self.text.len or self.text[self.pos] != ')') return error.InvalidRegex;
            self.pos += 1;
            return value;
        }
        const value = if (std.mem.startsWith(u8, self.text[self.pos..], "[:")) try self.posixAtom() else if (self.text[self.pos] == '[') blk: {
            self.pos += 1;
            break :blk try self.class();
        } else if (self.text[self.pos] == '\\') blk: {
            self.pos += 1;
            const escaped = try self.escape(true);
            if (escaped.tag == .class) break :blk escaped.class.?;
            if (escaped.tag != .literal) return error.InvalidRegex;
            break :blk try self.cls(.{ .range = .{ .lo = @intCast(escaped.cp), .hi = @intCast(escaped.cp) } });
        } else return error.InvalidRegex;
        return if (self.flags.insensitive) self.cls(.{ .folded = .{ .child = value, .utf8 = self.flags.unicode, .restrict = self.flags.caseless_restrict } }) else value;
    }
    fn setExpression(self: *Parser, minimum: u8) Error!*const Class {
        var value = try self.setAtom();
        while (true) {
            self.skip();
            if (self.pos == self.text.len) return error.InvalidRegex;
            const operation = self.text[self.pos];
            const precedence: u8 = switch (operation) {
                '&' => 2,
                '|', '+', '-', '^' => 1,
                else => break,
            };
            if (precedence < minimum) break;
            self.pos += 1;
            const right = try self.setExpression(precedence + 1);
            value = switch (operation) {
                '&' => try self.cls(.{ .intersect = .{ .a = value, .b = right } }),
                '|', '+' => try self.cls(.{ .either = .{ .a = value, .b = right } }),
                '-' => try self.cls(.{ .difference = .{ .a = value, .b = right } }),
                '^' => try self.cls(.{ .either = .{ .a = try self.cls(.{ .difference = .{ .a = value, .b = right } }), .b = try self.cls(.{ .difference = .{ .a = right, .b = value } }) } }),
                else => unreachable,
            };
        }
        return value;
    }
    fn class(self: *Parser) Error!*const Class {
        if (self.depth >= 128) return error.RegexTooLarge;
        self.depth += 1;
        defer self.depth -= 1;
        var negate = false;
        if (self.pos < self.text.len and self.text[self.pos] == '^') {
            negate = true;
            self.pos += 1;
        }
        var value: ?*const Class = null;
        var first = true;
        while (self.pos < self.text.len and (first or self.text[self.pos] != ']')) {
            if (self.flags.extended_more and (self.text[self.pos] == ' ' or self.text[self.pos] == '\t')) {
                self.pos += 1;
                continue;
            }
            if (self.advanced and std.mem.startsWith(u8, self.text[self.pos..], "\\E")) {
                self.pos += 2;
                continue;
            }
            if (self.advanced and std.mem.startsWith(u8, self.text[self.pos..], "\\Q")) {
                self.pos += 2;
                while (self.pos < self.text.len and !std.mem.startsWith(u8, self.text[self.pos..], "\\E")) {
                    const cp = try self.codepoint();
                    const item = try self.cls(.{ .range = .{ .lo = @intCast(cp), .hi = @intCast(cp) } });
                    value = if (value) |left| try self.cls(.{ .either = .{ .a = left, .b = item } }) else item;
                    first = false;
                }
                if (self.pos < self.text.len) self.pos += 2;
                continue;
            }
            first = false;
            // POSIX classes use Unicode properties in PCRE UCP mode.
            var item: *const Class = undefined;
            if (std.mem.startsWith(u8, self.text[self.pos..], "[:")) {
                item = try self.posixAtom();
            } else item = try self.classItem();
            value = if (value) |v| try self.cls(.{ .either = .{ .a = v, .b = item } }) else item;
            if (!self.advanced and self.pos + 1 < self.text.len and (std.mem.startsWith(u8, self.text[self.pos..], "&&") or std.mem.startsWith(u8, self.text[self.pos..], "--"))) {
                const diff = self.text[self.pos] == '-';
                self.pos += 2;
                const rhs = try self.classItem();
                value = if (diff) try self.cls(.{ .difference = .{ .a = value.?, .b = rhs } }) else try self.cls(.{ .intersect = .{ .a = value.?, .b = rhs } });
            }
        }
        if (self.pos == self.text.len or value == null) return error.InvalidRegex;
        self.pos += 1;
        return if (negate) self.cls(.{ .negate = value.? }) else value.?;
    }
};

test "search VM preserves leftmost alternation, greedy/lazy and capture priorities" {
    const a = std.testing.allocator;
    var p = try Pattern.init(a, "(?P<name>a+?)(b|bb)", .{});
    defer p.deinit();
    var runner = try Runner.init(a, &p);
    defer runner.deinit();
    const m = runner.find("xxaaabb!", 0).?;
    try std.testing.expectEqual(Span{ .start = 2, .end = 6 }, m.span);
    try std.testing.expectEqual(Span{ .start = 2, .end = 5 }, m.captures[1].?);
    var q = try Pattern.init(a, "a|aa", .{});
    defer q.deinit();
    var r = try Runner.init(a, &q);
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 1), r.find("aa", 0).?.span.end);
}
test "Unicode word boundaries properties folding and multiline assertions" {
    const a = std.testing.allocator;
    var p = try Pattern.init(a, "(?i)\\b(\\p{Greek}+)\\b", .{});
    defer p.deinit();
    var r = try Runner.init(a, &p);
    defer r.deinit();
    const text = "x Σίσυφος!";
    const m = r.find(text, 0).?;
    try std.testing.expectEqualStrings("Σίσυφος", text[m.span.start..m.span.end]);
    var q = try Pattern.init(a, "(?im)^é+$", .{});
    defer q.deinit();
    var s = try Runner.init(a, &q);
    defer s.deinit();
    try std.testing.expectEqual(Span{ .start = 2, .end = 6 }, s.find("x\nÉé\ny", 0).?.span);
}

/// Native compatibility engine for nonregular features. Kept separate from the
/// linear-time default runner. Both compiled state and backtracking work are bounded.
pub const BacktrackingPattern = struct {
    arena: std.heap.ArenaAllocator,
    states: []const State,
    start: u32,
    groups: usize,
    scratch_slots: usize,
    names: []const CaptureName,
    flags: Flags,
    subprograms: []u32,
    candidate_prefix: []const u8,
    prefix_steps: usize,
    jit: ?native_jit.Program = null,
    pub fn init(a: Allocator, text: []const u8, flags: Flags) Error!BacktrackingPattern {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        var dialect = flags;
        dialect.pcre = true;
        var parser = Parser{ .a = arena.allocator(), .text = text, .flags = dialect, .unicode = flags.unicode, .advanced = true };
        var root = try parser.expr();
        parser.skip();
        if (parser.pos != text.len) return error.InvalidRegex;
        if (flags.whole_word or flags.whole_line) {
            const nodes = try parser.a.alloc(*const Node, 3);
            nodes[0] = try parser.node(.{ .tag = .assertion, .flags = dialect, .assertion = if (flags.whole_line) .start else .word_left });
            nodes[1] = root;
            nodes[2] = try parser.node(.{ .tag = .assertion, .flags = dialect, .assertion = if (flags.whole_line) .end else .word_right });
            root = try parser.node(.{ .tag = .seq, .flags = dialect, .children = nodes });
        }
        const group_nodes = try parser.a.alloc(?*const Node, parser.groups + 1);
        @memset(group_nodes, null);
        collectGroups(root, group_nodes);
        var compiler = Compiler{ .a = parser.a, .advanced = true, .names = parser.names.items, .groups = parser.groups, .group_nodes = group_nodes, .utf8 = flags.unicode };
        const accept = try compiler.emit(.{ .tag = .accept });
        const start = try compiler.lower(root, accept);
        const entries = try parser.a.alloc(u32, parser.groups + 1);
        entries[0] = start;
        @memset(entries[1..], accept);
        // Compile only referenced subprograms, avoiding quadratic expansion of
        // ordinary nested captures. New calls can appear while lowering a body.
        const compiled = try parser.a.alloc(bool, entries.len);
        @memset(compiled, false);
        compiled[0] = true;
        var pc: usize = 0;
        while (pc < compiler.states.items.len) : (pc += 1) {
            const state = compiler.states.items[pc];
            if (state.tag != .call or compiled[state.slot]) continue;
            const body = group_nodes[state.slot] orelse return error.InvalidRegex;
            compiled[state.slot] = true;
            entries[state.slot] = try compiler.lower(body, accept);
        }
        const prefix = try candidatePrefix(parser.a, compiler.states.items, start);
        return .{ .arena = arena, .states = compiler.states.items, .start = start, .groups = parser.groups, .scratch_slots = compiler.scratch_slots, .names = parser.names.items, .flags = dialect, .subprograms = entries, .candidate_prefix = prefix.bytes, .prefix_steps = prefix.steps, .jit = native_jit.Program.compile(compiler.states.items) };
    }
    pub fn deinit(self: *BacktrackingPattern) void {
        if (self.jit) |*program| program.deinit();
        self.arena.deinit();
    }
};
// Only traverse a straight prefix of capture saves and case-sensitive ASCII
// literals. Assertions, branches, calls and control effects stop extraction.
// The counted VM steps let low match budgets retain their exact error behavior.
fn candidatePrefix(a: Allocator, states: []const State, start: u32) Error!struct { bytes: []const u8, steps: usize } {
    var bytes: [64]u8 = undefined;
    var len: usize = 0;
    var steps: usize = 0;
    var pc = start;
    while (steps < states.len and len < bytes.len) {
        const state = states[pc];
        switch (state.tag) {
            .save => {},
            .consume => {
                const node = state.node.?;
                if (node.tag != .literal or node.flags.insensitive or node.cp >= 128) break;
                bytes[len] = @intCast(node.cp);
                len += 1;
            },
            else => break,
        }
        steps += 1;
        pc = state.first;
    }
    return .{ .bytes = try a.dupe(u8, bytes[0..len]), .steps = steps };
}
fn collectGroups(node: *const Node, out: []?*const Node) void {
    if (node.tag == .capture and out[node.group] == null) out[node.group] = node;
    if (node.child) |child| collectGroups(child, out);
    for (node.children) |child| collectGroups(child, out);
}
pub const Callout = struct {
    number: u8,
    string: ?[]const u8,
    pattern_position: usize,
    subject: []const u8,
    offset: usize,
    start: usize,
    /// Capture offset pairs. Unset slots contain maxInt(usize).
    captures: []const usize,
    mark: ?[]const u8,
};
pub const CalloutAction = enum { proceed, reject, abort };
// Called only for non-ASCII input. ASCII chains remain entirely in generated
// code; Unicode classes/folding share the validated native predicate machinery.
fn jitConsume(context: *native_jit.Context, pc: u32) callconv(.c) u32 {
    const pattern: *const BacktrackingPattern = @ptrCast(@alignCast(context.user));
    const state = pattern.states[pc];
    const text = context.text[0..context.length];
    if (context.position == text.len) return 0;
    const decoded = unicode.decode(text, context.position, state.node.?.flags.unicode);
    if (!stateMatches(state, decoded.cp)) return 0;
    context.position += decoded.len;
    return 1;
}

pub const ExecutionProfile = struct {
    states: [256]u64 = @splat(0),
    jit_entries: u64 = 0,
};
// Instrumentation is an explicit executable opt-in. Production runners carry
// neither a profiling pointer nor a conditional in the dispatch loop.
pub const execution_profiling = if (@hasDecl(@import("root"), "antfly_regex_profiling")) @import("root").antfly_regex_profiling else false;
pub const BacktrackingRunner = struct {
    profile: if (execution_profiling) ?*ExecutionProfile else void = if (execution_profiling) null else {},
    a: Allocator,
    p: *const BacktrackingPattern,
    result: []?Span,
    steps: usize = 0,
    search_offset: usize = 0,
    resume_offset: usize = 0,
    committed: bool = false,
    absolute_start_allowed: bool = true,
    jit_enabled: bool = true,
    ascii_repeat_enabled: bool = true,
    callout: ?*const fn (?*anyopaque, Callout) CalloutAction = null,
    callout_context: ?*anyopaque = null,
    max_steps: usize = 10_000_000,
    live_bytes: usize = 0,
    mark: ?[]const u8 = null,
    marks: std.ArrayList(Mark) = .empty,
    alternatives: std.ArrayList(Alternative) = .empty,
    free_caps: std.ArrayList([]usize) = .empty,
    cached_bytes: usize = 0,
    call_links: std.ArrayList(CallLink) = .empty,
    free_call: ?u32 = null,
    call_snapshots: std.ArrayList([]usize) = .empty,
    snapshot_cache_bytes: usize = 0,
    live_snapshot_bytes: usize = 0,
    execution: ?*Execution = null,
    const CallLink = struct { parent: ?u32, pc: u32, group: u16, depth: u16, references: u32, snapshot: []usize, alternative: ?usize, branch_marker: ?u32, owner: *Execution, base: usize, generation: usize };
    const Mark = struct { name: []const u8, position: usize, parent: ?usize, skip_parent: ?usize, skippable: bool };
    const Execution = struct { parent: ?*Execution, stack: *std.ArrayList(Frame), scope_depth: usize, assertion_scope: bool, opaque_cut: bool, generation: usize = 0 };
    const Alternative = struct { marker: u32, base: usize, parent: ?usize, owner: *Execution, generation: usize };
    const Frame = struct { pc: u32, pos: usize, caps: []usize, call: ?u32 = null, return_boundary: ?u32 = null, mark: ?usize = null, skip_mark: ?usize = null, alternative: ?usize = null, branch_marker: ?u32 = null, barrier: bool = false, accepted: bool = false, repeat_retry: bool = false, repeat_min_end: usize = 0, repeat_count: usize = 0 };
    const Continuation = struct { subject: []const u8, saved_frame: Frame, pc: u32, required_end: ?usize, scope_depth: usize, next: ?*const Continuation };
    const Failure = Error || error{ MatchLimitExceeded, DepthLimitExceeded, CalloutAborted };
    pub fn init(a: Allocator, p: *const BacktrackingPattern) !BacktrackingRunner {
        return .{ .a = a, .p = p, .result = try a.alloc(?Span, p.groups + 1) };
    }
    pub fn deinit(self: *BacktrackingRunner) void {
        self.marks.deinit(self.a);
        self.alternatives.deinit(self.a);
        for (self.free_caps.items) |caps| self.a.free(caps);
        self.free_caps.deinit(self.a);
        self.a.free(self.result);
        for (self.call_links.items) |link| if (link.references != 0) self.a.free(link.snapshot);
        self.call_links.deinit(self.a);
        for (self.call_snapshots.items) |snapshot| self.a.free(snapshot);
        self.call_snapshots.deinit(self.a);
    }
    fn retainCall(self: *BacktrackingRunner, index: ?u32) ?u32 {
        if (index) |i| self.call_links.items[i].references += 1;
        return index;
    }
    fn releaseCall(self: *BacktrackingRunner, index: ?u32) void {
        var current = index;
        while (current) |i| {
            const link = &self.call_links.items[i];
            link.references -= 1;
            if (link.references != 0) return;
            const parent = link.parent;
            const bytes = link.snapshot.len * @sizeOf(usize);
            self.live_snapshot_bytes -= bytes;
            if (bytes <= 256 * 1024 - self.snapshot_cache_bytes) {
                if (self.call_snapshots.append(self.a, link.snapshot)) |_| {
                    self.snapshot_cache_bytes += bytes;
                } else |_| self.a.free(link.snapshot);
            } else self.a.free(link.snapshot);
            link.snapshot = &.{};
            link.parent = self.free_call;
            self.free_call = i;
            current = parent;
        }
    }
    fn cloneFrame(self: *BacktrackingRunner, frame: Frame) Failure!Frame {
        var copy = frame;
        copy.caps = try self.clone(frame.caps);
        copy.call = self.retainCall(frame.call);
        return copy;
    }
    fn releaseFrame(self: *BacktrackingRunner, frame: Frame) void {
        self.release(frame.caps);
        self.releaseCall(frame.call);
    }
    fn enterCall(self: *BacktrackingRunner, frame: *Frame, group: usize, pc: u32) Failure!void {
        const depth = if (frame.call) |i| self.call_links.items[i].depth else 0;
        if (depth >= 64) return error.DepthLimitExceeded;
        const count = frame.caps.len - 2;
        const bytes = count * @sizeOf(usize);
        if (bytes > 8 * 1024 * 1024 - self.live_snapshot_bytes) return error.MatchLimitExceeded;
        const snapshot = if (self.call_snapshots.pop()) |cached| blk: {
            self.snapshot_cache_bytes -= cached.len * @sizeOf(usize);
            break :blk cached;
        } else try self.a.alloc(usize, count);
        errdefer self.a.free(snapshot);
        @memcpy(snapshot, frame.caps[2..]);
        const index: u32 = if (self.free_call) |i| blk: {
            self.free_call = self.call_links.items[i].parent;
            break :blk i;
        } else blk: {
            const i: u32 = @intCast(self.call_links.items.len);
            try self.call_links.append(self.a, undefined);
            break :blk i;
        };
        self.call_links.items[index] = .{ .parent = frame.call, .pc = pc, .group = @intCast(group), .depth = depth + 1, .references = 1, .snapshot = snapshot, .alternative = frame.alternative, .branch_marker = frame.branch_marker, .owner = self.execution.?, .base = self.execution.?.stack.items.len, .generation = self.execution.?.generation };
        self.live_snapshot_bytes += bytes;
        frame.call = index;
    }
    fn lookFrame(self: *BacktrackingRunner, frame: Frame, pc: u32, pos: usize) Failure!Frame {
        var copy = try self.cloneFrame(frame);
        copy.pc = pc;
        copy.pos = pos;
        copy.return_boundary = frame.call;
        copy.alternative = null;
        copy.branch_marker = null;
        return copy;
    }
    fn clearExecution(self: *BacktrackingRunner, execution: *Execution) void {
        for (execution.stack.items) |frame| self.releaseFrame(frame);
        execution.stack.clearRetainingCapacity();
        execution.generation += 1;
    }
    // Assertion continuations are native call frames, but cuts still apply to
    // the lexical backtracking scope. Prune paused ancestor stacks immediately,
    // so later cuts cannot overwrite an earlier barrier.
    fn cut(self: *BacktrackingRunner, scope_depth: usize, original_alternative: ?usize, call: ?u32, then: bool) bool {
        const limit = if (call) |i| self.call_links.items[i] else null;
        const alternative = if (limit != null and original_alternative == limit.?.alternative) null else original_alternative;
        var current = self.execution;
        while (current) |execution| {
            if (alternative) |i| {
                const active = self.alternatives.items[i];
                if (active.owner == execution) {
                    const base = if (active.generation == execution.generation) active.base else 0;
                    while (execution.stack.items.len > base) {
                        if (execution.stack.items[execution.stack.items.len - 1].branch_marker == active.marker) break;
                        self.releaseFrame(execution.stack.pop().?);
                    }
                    return false;
                }
            }
            if (limit) |link| if (link.owner == execution) {
                const base = if (link.generation == execution.generation) link.base else 0;
                while (execution.stack.items.len > base) self.releaseFrame(execution.stack.pop().?);
                return false;
            };
            const local = execution.scope_depth <= scope_depth and (execution.opaque_cut or (then and execution.assertion_scope));
            self.clearExecution(execution);
            if (local) return false;
            current = execution.parent;
        }
        return true;
    }
    fn acquire(self: *BacktrackingRunner, slots: usize) Failure![]usize {
        const bytes = slots * @sizeOf(usize);
        if (self.live_bytes + bytes > 64 * 1024 * 1024) return error.MatchLimitExceeded;
        const copy = if (self.free_caps.pop()) |cached| blk: {
            self.cached_bytes -= cached.len * @sizeOf(usize);
            break :blk cached;
        } else try self.a.alloc(usize, slots);
        self.live_bytes += bytes;
        return copy;
    }
    fn clone(self: *BacktrackingRunner, caps: []const usize) Failure![]usize {
        const copy = try self.acquire(caps.len);
        @memcpy(copy, caps);
        return copy;
    }
    fn release(self: *BacktrackingRunner, caps: []usize) void {
        const bytes = caps.len * @sizeOf(usize);
        self.live_bytes -= bytes;
        if (bytes <= 256 * 1024 - self.cached_bytes) {
            self.free_caps.append(self.a, caps) catch {
                self.a.free(caps);
                return;
            };
            self.cached_bytes += bytes;
        } else self.a.free(caps);
    }
    pub fn find(self: *BacktrackingRunner, text: []const u8, offset: usize) Failure!?Match {
        if (offset > text.len) return null;
        self.search_offset = offset;
        self.resume_offset = offset;
        self.committed = false;
        self.mark = null;
        self.marks.clearRetainingCapacity();
        self.alternatives.clearRetainingCapacity();
        self.steps = self.max_steps;
        const slots = (self.p.groups + 1) * 2 + self.p.scratch_slots;
        var pos = offset;
        while (pos <= text.len) {
            if (self.p.candidate_prefix.len > 0 and self.max_steps >= self.p.prefix_steps) pos = literal.findExact(text, self.p.candidate_prefix, pos) orelse break;
            // PCRE2's match limit bounds each candidate start, rather than the
            // linear scan through an unanchored subject. Metadata from earlier
            // failed starts is also unreachable and can be recycled immediately.
            self.steps = self.max_steps;
            self.marks.clearRetainingCapacity();
            self.alternatives.clearRetainingCapacity();
            // Initialize the owned frame directly. Failed candidate starts reuse
            // this buffer without filling and then cloning a second scratch copy.
            const initial = try self.acquire(slots);
            @memset(initial, std.math.maxInt(usize));
            initial[0] = pos;
            if (try self.executeFrame(text, .{ .pc = self.p.start, .pos = pos, .caps = initial }, null, 0, 0, null, false, false)) |found| {
                defer self.releaseFrame(found);
                self.mark = if (found.mark) |i| self.marks.items[i].name else null;
                found.caps[1] = found.pos;
                for (self.result, 0..) |*span, i| span.* = if (found.caps[2 * i] == std.math.maxInt(usize) or found.caps[2 * i + 1] == std.math.maxInt(usize)) null else .{ .start = found.caps[2 * i], .end = found.caps[2 * i + 1] };
                return .{ .span = self.result[0].?, .captures = self.result };
            }
            if (self.committed or pos == text.len) break;
            pos = @max(self.resume_offset, pos + unicode.decode(text, pos, self.p.flags.unicode).len);
        }
        return null;
    }
    fn evaluateLook(self: *BacktrackingRunner, text: []const u8, start: u32, frame: Frame, node: *const Node, depth: usize, scope_depth: usize, conditional: bool) Failure!?Frame {
        return self.evaluateLookWith(text, start, frame, node, depth, scope_depth, conditional, null);
    }
    fn evaluateLookWith(self: *BacktrackingRunner, text: []const u8, start: u32, frame: Frame, node: *const Node, depth: usize, scope_depth: usize, conditional: bool, continuation: ?*const Continuation) Failure!?Frame {
        return self.executeFrame(text, try self.lookFrame(frame, start, frame.pos), if (node.behind) frame.pos else null, depth + 1, scope_depth + 1, continuation, true, conditional or node.negative);
    }
    // Owns initial.caps even on admission failure. Assertion continuations retain
    // their caller's position/progress and copy only successful capture values.
    fn executeFrame(self: *BacktrackingRunner, text: []const u8, initial_frame: Frame, required_end: ?usize, depth: usize, scope_depth: usize, continuation: ?*const Continuation, assertion_scope: bool, opaque_cut: bool) Failure!?Frame {
        var frame = initial_frame;
        var owns = true;
        defer if (owns) self.releaseFrame(frame);
        if (depth > 64) return error.DepthLimitExceeded;
        const capture_slots = (self.p.groups + 1) * 2;
        var stack: std.ArrayList(Frame) = .empty;
        var execution = Execution{ .parent = self.execution, .stack = &stack, .scope_depth = scope_depth, .assertion_scope = assertion_scope, .opaque_cut = opaque_cut };
        self.execution = &execution;
        defer self.execution = execution.parent;
        defer {
            for (stack.items) |f| self.releaseFrame(f);
            stack.deinit(self.a);
        }
        run: while (true) {
            frame.branch_marker = null;
            var state = self.p.states[frame.pc];
            if (self.jit_enabled and state.tag == .consume) if (self.p.jit) |*program| {
                if (comptime execution_profiling) {
                    if (self.profile) |profile| profile.jit_entries += 1;
                }
                var context = native_jit.Context{ .text = text.ptr, .length = text.len, .position = frame.pos, .steps = self.steps, .pc = frame.pc, .slow = jitConsume, .user = @ptrCast(self.p), .captures = frame.caps.ptr };
                const result = program.run(&context);
                self.steps = context.steps;
                frame.pos = context.position;
                frame.pc = context.pc;
                switch (result) {
                    .limit => return error.MatchLimitExceeded,
                    .fail => {
                        self.releaseFrame(frame);
                        if (stack.pop()) |next| {
                            frame = next;
                            continue :run;
                        }
                        owns = false;
                        return null;
                    },
                    .fallback => state = self.p.states[frame.pc],
                }
            };
            if (self.steps == 0) return error.MatchLimitExceeded;
            self.steps -= 1;
            if (comptime execution_profiling) {
                if (self.profile) |profile| profile.states[@backingInt(state.tag)] += 1;
            }
            switch (state.tag) {
                .alt_start => {
                    if ((self.alternatives.items.len + 1) * @sizeOf(Alternative) > 8 * 1024 * 1024) return error.MatchLimitExceeded;
                    const index = self.alternatives.items.len;
                    try self.alternatives.append(self.a, .{ .marker = frame.pc, .base = stack.items.len, .parent = frame.alternative, .owner = &execution, .generation = execution.generation });
                    frame.alternative = index;
                    frame.pc = state.first;
                    continue :run;
                },
                .alt_end => {
                    const active = self.alternatives.items[frame.alternative.?];
                    std.debug.assert(active.marker == state.slot);
                    frame.alternative = active.parent;
                    frame.pc = state.first;
                    continue :run;
                },
                .callout => {
                    const node = state.node.?;
                    const action = if (self.callout) |callback| callback(self.callout_context, .{ .number = @intCast(node.cp), .string = node.ref_name, .pattern_position = node.group, .subject = text, .offset = frame.pos, .start = frame.caps[0], .captures = frame.caps[0..capture_slots], .mark = if (frame.mark) |i| self.marks.items[i].name else null }) else .proceed;
                    switch (action) {
                        .proceed => {
                            frame.pc = state.first;
                            continue :run;
                        },
                        .abort => return error.CalloutAborted,
                        .reject => {},
                    }
                },
                .verb => {
                    const verb: Verb = @fromBackingInt(@as(u32, @intCast(state.slot)));
                    const triggered = frame.barrier;
                    frame.barrier = false;
                    if (!triggered) {
                        if (state.node.?.ref_name) |name| if (verb != .skip) {
                            if ((self.marks.items.len + 1) * @sizeOf(Mark) > 8 * 1024 * 1024) return error.MatchLimitExceeded;
                            const index = self.marks.items.len;
                            try self.marks.append(self.a, .{ .name = name, .position = frame.pos, .parent = frame.mark, .skip_parent = frame.skip_mark, .skippable = verb == .mark });
                            frame.mark = index;
                            if (verb == .mark) frame.skip_mark = index;
                        };
                        if (verb == .mark) {
                            frame.pc = state.first;
                            continue :run;
                        }
                        if (verb == .accept) {
                            for (0..self.p.groups + 1) |i| if (frame.caps[2 * i] != std.math.maxInt(usize) and frame.caps[2 * i + 1] == std.math.maxInt(usize)) {
                                frame.caps[2 * i + 1] = frame.pos;
                            };
                            frame.pc = 0;
                            frame.accepted = true;
                            continue :run;
                        }
                        if (verb != .fail) {
                            var barrier = try self.cloneFrame(frame);
                            errdefer self.releaseFrame(barrier);
                            barrier.barrier = true;
                            try stack.append(self.a, barrier);
                            frame.pc = state.first;
                            continue :run;
                        }
                    } else {
                        var skip_position = frame.pos;
                        var skip_found = true;
                        if (verb == .skip) if (state.node.?.ref_name) |name| {
                            skip_found = false;
                            var mark_index = frame.skip_mark;
                            while (mark_index) |i| {
                                const point = self.marks.items[i];
                                if (point.skippable and std.mem.eql(u8, point.name, name)) {
                                    skip_position = point.position;
                                    skip_found = true;
                                    break;
                                }
                                mark_index = point.skip_parent;
                            }
                        };
                        if (skip_found) {
                            const global = self.cut(scope_depth, if (verb == .then) frame.alternative else null, frame.call, verb == .then);
                            if (global) {
                                if (verb == .commit) self.committed = true;
                                if (verb == .skip) self.resume_offset = skip_position;
                            }
                        }
                    }
                },
                .grapheme => if (frame.pos < text.len) {
                    if (self.p.flags.unicode) {
                        var iterator = @import("antfly_grapheme").Iterator{ .text = text, .position = frame.pos };
                        _ = iterator.next() catch return error.InvalidRegex;
                        frame.pos = iterator.position;
                    } else frame.pos += if (text[frame.pos] == '\r' and frame.pos + 1 < text.len and text[frame.pos + 1] == '\n') @as(usize, 2) else 1;
                    frame.pc = state.first;
                    continue :run;
                },
                .call => {
                    if (state.slot >= self.p.subprograms.len) return error.InvalidRegex;
                    try self.enterCall(&frame, state.slot, state.first);
                    frame.pc = self.p.subprograms[state.slot];
                    continue :run;
                },
                .conditional => {
                    const node = state.node.?;
                    var yes = if (node.child != null) blk: {
                        const found = try self.evaluateLook(text, @intCast(state.slot), frame, node, depth, scope_depth, true);
                        if (found) |f| {
                            if (!node.negative or f.accepted) {
                                @memcpy(frame.caps[2..capture_slots], f.caps[2..capture_slots]);
                                frame.mark = f.mark;
                            }
                            self.releaseFrame(f);
                        }
                        break :blk (found != null) != node.negative;
                    } else switch (node.condition) {
                        .recursion => frame.call != null,
                        .define => false,
                        .version => node.cp != 0,
                        .group => frame.caps[2 * state.slot] != std.math.maxInt(usize) and frame.caps[2 * state.slot + 1] != std.math.maxInt(usize),
                    };
                    if (node.recursion_condition) yes = if (frame.call) |i| self.call_links.items[i].group == state.slot else false;
                    if (!node.recursion_condition) if (node.ref_name) |name| {
                        yes = false;
                        for (self.p.names) |entry| if (std.mem.eql(u8, name, entry.name) and frame.caps[2 * entry.index] != std.math.maxInt(usize) and frame.caps[2 * entry.index + 1] != std.math.maxInt(usize)) {
                            yes = true;
                            break;
                        };
                    };
                    frame.pc = if (yes) state.first else state.second;
                    continue :run;
                },
                .accept => if (frame.call != frame.return_boundary) {
                    const index = frame.call.?;
                    const link = self.call_links.items[index];
                    @memcpy(frame.caps[2..], link.snapshot);
                    frame.pc = link.pc;
                    frame.alternative = link.alternative;
                    frame.branch_marker = link.branch_marker;
                    frame.accepted = false;
                    frame.call = self.retainCall(link.parent);
                    self.releaseCall(index);
                    continue :run;
                } else if (frame.accepted or required_end == null or frame.pos == required_end.?) {
                    if (continuation) |next| {
                        var saved_frame = try self.cloneFrame(next.saved_frame);
                        saved_frame.pc = next.pc;
                        @memcpy(saved_frame.caps[2..capture_slots], frame.caps[2..capture_slots]);
                        saved_frame.mark = frame.mark;
                        saved_frame.skip_mark = frame.skip_mark;
                        if (try self.executeFrame(next.subject, saved_frame, next.required_end, depth + 1, next.scope_depth, next.next, false, false)) |found| return found;
                    } else {
                        owns = false;
                        return frame;
                    }
                },
                .save => {
                    frame.caps[state.slot] = frame.pos;
                    frame.pc = state.first;
                    continue :run;
                },
                .reset => {
                    frame.caps[0] = frame.pos;
                    frame.pc = state.first;
                    continue :run;
                },
                .split => {
                    var copy = try self.cloneFrame(frame);
                    errdefer self.releaseFrame(copy);
                    copy.pc = state.second;
                    copy.branch_marker = if (state.slot > 0) @intCast(state.slot - 1) else null;
                    try stack.append(self.a, copy);
                    frame.pc = state.first;
                    continue :run;
                },
                .progress => {
                    const slot = capture_slots + state.slot;
                    if (frame.caps[slot] == frame.pos) frame.pc = state.second else {
                        frame.caps[slot] = frame.pos;
                        frame.pc = state.first;
                    }
                    continue :run;
                },
                .atomic_start => {
                    frame.caps[capture_slots + state.slot] = stack.items.len;
                    frame.caps[capture_slots + state.slot + 1] = execution.generation;
                    frame.caps[capture_slots + state.slot + 2] = frame.skip_mark orelse std.math.maxInt(usize);
                    frame.pc = state.first;
                    continue :run;
                },
                .atomic_end => {
                    const target = if (frame.caps[capture_slots + state.slot + 1] == execution.generation) @min(stack.items.len, frame.caps[capture_slots + state.slot]) else 0;
                    while (stack.items.len > target) self.releaseFrame(stack.pop().?);
                    const saved_mark = frame.caps[capture_slots + state.slot + 2];
                    frame.skip_mark = if (saved_mark == std.math.maxInt(usize)) null else saved_mark;
                    frame.pc = state.first;
                    continue :run;
                },
                .assertion => if ((state.node.?.assertion != .absolute_start or self.absolute_start_allowed) and (if (state.node.?.assertion == .previous_match) frame.pos == self.search_offset else assertion(state.node.?, text, frame.pos, self.p.flags.unicode))) {
                    frame.pc = state.first;
                    continue :run;
                },
                .consume => if (frame.pos < text.len) {
                    const d = unicode.decode(text, frame.pos, state.node.?.flags.unicode);
                    if (stateMatches(state, d.cp)) {
                        frame.pos += d.len;
                        frame.pc = state.first;
                        continue :run;
                    }
                },
                .script_start => {
                    frame.caps[capture_slots + state.slot] = frame.pos;
                    frame.pc = state.first;
                    continue :run;
                },
                .script_end => {
                    const begin = frame.caps[capture_slots + state.slot];
                    if (begin <= frame.pos and unicode.scriptRun(text[begin..frame.pos], state.node.?.flags.unicode)) {
                        frame.pc = state.first;
                        continue :run;
                    }
                },
                .scan => {
                    var selected: ?Span = null;
                    for (state.node.?.scan_groups) |group| {
                        const begin = frame.caps[2 * group];
                        const end = frame.caps[2 * group + 1];
                        if (begin != std.math.maxInt(usize) and end != std.math.maxInt(usize)) {
                            selected = .{ .start = begin, .end = end };
                            break;
                        }
                    }
                    if (selected) |span| {
                        const next = Continuation{ .subject = text, .saved_frame = frame, .pc = state.first, .required_end = required_end, .scope_depth = scope_depth, .next = continuation };
                        if (span.start <= span.end and span.end <= text.len) if (try self.executeFrame(text[0..span.end], try self.lookFrame(frame, state.second, span.start), null, depth + 1, scope_depth + 1, &next, true, false)) |found| return found;
                    }
                },
                .look_shift => shift: {
                    if (!frame.repeat_retry) {
                        frame.repeat_min_end = frame.pos;
                        frame.repeat_count = 0;
                        while (frame.pos > 0 and frame.repeat_count < state.second) {
                            frame.pos = previousStart(text, frame.pos, state.node.?.flags.unicode);
                            frame.repeat_count += 1;
                        }
                        if (frame.repeat_count < state.slot) break :shift;
                    } else {
                        frame.repeat_retry = false;
                        if (frame.repeat_count == state.slot) break :shift;
                        frame.pos += unicode.decode(text, frame.pos, state.node.?.flags.unicode).len;
                        frame.repeat_count -= 1;
                    }
                    if (frame.repeat_count > state.slot) {
                        var backup = try self.cloneFrame(frame);
                        errdefer self.releaseFrame(backup);
                        backup.repeat_retry = true;
                        try stack.append(self.a, backup);
                    }
                    frame.repeat_retry = false;
                    frame.pc = state.first;
                    continue :run;
                },
                .repeat_consume => repeat: {
                    const node = state.node.?;
                    const utf8 = node.child.?.flags.unicode;
                    if (!frame.repeat_retry and node.greedy and self.ascii_repeat_enabled and node.child.?.tag == .literal and !node.child.?.flags.insensitive and node.child.?.cp < 128) {
                        const begin = frame.pos;
                        const limit = @min(text.len - begin, node.max orelse std.math.maxInt(usize));
                        const count = literal.equalPrefix(text[begin..][0..limit], @intCast(node.child.?.cp));
                        if (count > self.steps) {
                            self.steps = 0;
                            return error.MatchLimitExceeded;
                        }
                        self.steps -= count;
                        frame.pos += count;
                        frame.repeat_count = count;
                        if (count < node.min) break :repeat;
                        frame.repeat_min_end = begin + node.min;
                    } else if (!frame.repeat_retry) {
                        frame.repeat_count = 0;
                        while (frame.repeat_count < node.min) {
                            if (frame.pos == text.len) break :repeat;
                            const decoded = unicode.decode(text, frame.pos, utf8);
                            if (!stateMatches(state, decoded.cp)) break :repeat;
                            if (self.steps == 0) return error.MatchLimitExceeded;
                            self.steps -= 1;
                            frame.pos += decoded.len;
                            frame.repeat_count += 1;
                        }
                        frame.repeat_min_end = frame.pos;
                        if (node.greedy) while (frame.pos < text.len and (node.max == null or frame.repeat_count < node.max.?)) {
                            const decoded = unicode.decode(text, frame.pos, utf8);
                            if (!stateMatches(state, decoded.cp)) break;
                            if (self.steps == 0) return error.MatchLimitExceeded;
                            self.steps -= 1;
                            frame.pos += decoded.len;
                            frame.repeat_count += 1;
                        };
                    } else if (node.greedy) {
                        frame.repeat_retry = false;
                        if (frame.pos == frame.repeat_min_end) break :repeat;
                        var previous = frame.pos - 1;
                        if (utf8 and text[previous] & 0xc0 == 0x80) {
                            const end = frame.pos;
                            var distance: usize = 2;
                            while (distance <= 4 and distance <= end - frame.repeat_min_end) : (distance += 1) {
                                const candidate = end - distance;
                                if (unicode.decode(text, candidate, true).len == distance) {
                                    previous = candidate;
                                    break;
                                }
                            }
                        }
                        frame.pos = previous;
                        frame.repeat_count -= 1;
                    } else {
                        frame.repeat_retry = false;
                        if (frame.pos == text.len or (node.max != null and frame.repeat_count == node.max.?)) break :repeat;
                        const decoded = unicode.decode(text, frame.pos, utf8);
                        if (!stateMatches(state, decoded.cp)) break :repeat;
                        frame.pos += decoded.len;
                        frame.repeat_count += 1;
                    }
                    const more = if (node.greedy) frame.pos > frame.repeat_min_end else frame.pos < text.len and (node.max == null or frame.repeat_count < node.max.?);
                    if (more) {
                        var backup = try self.cloneFrame(frame);
                        errdefer self.releaseFrame(backup);
                        backup.repeat_retry = true;
                        try stack.append(self.a, backup);
                    }
                    frame.repeat_retry = false;
                    frame.pc = state.first;
                    continue :run;
                },
                .reference => {
                    var group = state.slot;
                    if (state.node.?.ref_name) |name| {
                        for (self.p.names) |entry| if (std.mem.eql(u8, name, entry.name) and frame.caps[2 * entry.index] != std.math.maxInt(usize) and frame.caps[2 * entry.index + 1] != std.math.maxInt(usize)) {
                            group = entry.index;
                            break;
                        };
                    }
                    const begin = frame.caps[group * 2];
                    const end = frame.caps[group * 2 + 1];
                    if (begin != std.math.maxInt(usize) and end != std.math.maxInt(usize)) {
                        var left = begin;
                        var right = frame.pos;
                        while (left < end and right < text.len) {
                            const x = unicode.decode(text, left, state.node.?.flags.unicode);
                            const y = unicode.decode(text, right, state.node.?.flags.unicode);
                            if (if (state.node.?.flags.insensitive) (state.node.?.flags.caseless_restrict and (x.cp < 128) != (y.cp < 128)) or foldCodepoint(x.cp, state.node.?.flags.unicode) != foldCodepoint(y.cp, state.node.?.flags.unicode) else x.cp != y.cp) break;
                            left += x.len;
                            right += y.len;
                        }
                        if (left == end) {
                            frame.pos = right;
                            frame.pc = state.first;
                            continue :run;
                        }
                    }
                },
                .look => {
                    const node = state.node.?;
                    if (node.non_atomic) {
                        const next = Continuation{ .subject = text, .saved_frame = frame, .pc = state.first, .required_end = required_end, .scope_depth = scope_depth, .next = continuation };
                        if (try self.evaluateLookWith(text, state.second, frame, node, depth, scope_depth, false, &next)) |found| return found;
                    } else {
                        const found = try self.evaluateLook(text, state.second, frame, node, depth, scope_depth, false);
                        const yes = (found != null) != node.negative;
                        if (found) |f| {
                            if (!node.negative) {
                                @memcpy(frame.caps[2..capture_slots], f.caps[2..capture_slots]);
                                frame.mark = f.mark;
                            }
                            self.releaseFrame(f);
                        }
                        if (yes) {
                            frame.pc = state.first;
                            continue :run;
                        }
                    }
                },
            }
            self.releaseFrame(frame);
            if (stack.pop()) |f| frame = f else {
                owns = false;
                return null;
            }
        }
    }
};

/// Smart-case examines literals/classes, excluding group names and property names.
pub fn hasUpperLiteral(a: Allocator, text: []const u8, is_literal: bool, utf8: bool) bool {
    if (is_literal) {
        var i: usize = 0;
        while (i < text.len) {
            const d = unicode.decode(text, i, utf8);
            if (unicode.contains(unicode.property("Uppercase").?, d.cp)) return true;
            i += d.len;
        }
        return false;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var parser = Parser{ .a = arena.allocator(), .text = text, .flags = .{ .unicode = utf8 }, .unicode = utf8, .advanced = true };
    const root = parser.expr() catch return false;
    return nodeHasUpper(root);
}
fn nodeHasUpper(node: *const Node) bool {
    if (node.tag == .literal and unicode.contains(unicode.property("Uppercase").?, node.cp)) return true;
    if (node.tag == .class and classHasUpper(node.class.?)) return true;
    if (node.child) |child| if (nodeHasUpper(child)) return true;
    for (node.children) |child| if (nodeHasUpper(child)) return true;
    return false;
}
fn classHasUpper(cls: *const Class) bool {
    return switch (cls.*) {
        .range => |r| blk: {
            for (unicode.property("Uppercase").?) |upper| if (r.lo <= upper.hi and r.hi >= upper.lo) break :blk true;
            break :blk false;
        },
        .either => |v| classHasUpper(v.a) or classHasUpper(v.b),
        .intersect => |v| classHasUpper(v.a) or classHasUpper(v.b),
        .difference => |v| classHasUpper(v.a) or classHasUpper(v.b),
        .negate => |v| classHasUpper(v),
        .folded => |v| classHasUpper(v.child),
        .property, .exact_property, .ascii_property => false,
    };
}

test "specialized class runs agree with the capture VM at every offset" {
    const a = std.testing.allocator;
    const patterns = [_][]const u8{ "a", "a*", "a+?", "a{2,4}", "a{2,4}?", "[a-z]+", "(?i)é+", "\\p{Greek}+", ".*", "(?s).*", "[^a]{2,}", "\\w*?", "[[:alpha:]]+" };
    const subjects = [_][]const u8{ "", "xaaaay", "a\naaaa", "Éé Été Σίσυφος!", "\xffaa\xc3", "123ab12" };
    for (patterns) |pattern| {
        var p = try Pattern.init(a, pattern, .{});
        defer p.deinit();
        const simple = p.simple;
        try std.testing.expect(simple != null);
        var optimized = try Runner.init(a, &p);
        defer optimized.deinit();
        var reference = try Runner.init(a, &p);
        defer reference.deinit();
        for (subjects) |subject| for (0..subject.len + 1) |offset| {
            p.simple = simple;
            const fast = if (optimized.find(subject, offset)) |match| match.span else null;
            p.simple = null;
            const regular = if (reference.find(subject, offset)) |match| match.span else null;
            try std.testing.expectEqual(regular, fast);
        };
    }
}

test "native backtracking resource limits and invalid named conditions" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidRegex, BacktrackingPattern.init(a, "(?(missing)a|b)", .{}));
    var p = try BacktrackingPattern.init(a, "(a+)+$", .{});
    defer p.deinit();
    var r = try BacktrackingRunner.init(a, &p);
    defer r.deinit();
    r.max_steps = 100;
    try std.testing.expectError(error.MatchLimitExceeded, r.find("aaaaaaaaaaaaaaaaaaaa!", 0));
    try std.testing.expectEqual(@as(usize, 0), r.live_bytes);
}

test "duplicate names select participating captures and callouts can reject or abort" {
    const a = std.testing.allocator;
    var pattern = try BacktrackingPattern.init(a, "(?J)(?:(?<n>a)|(?<n>b))\\k<n>(?(n)c|d)(?C7)", .{});
    defer pattern.deinit();
    var runner = try BacktrackingRunner.init(a, &pattern);
    defer runner.deinit();
    try std.testing.expect(try runner.find("aac", 0) != null);
    try std.testing.expect(try runner.find("bbc", 0) != null);
    try std.testing.expect(try runner.find("abc", 0) == null);
    const Callback = struct {
        fn reject(_: ?*anyopaque, value: Callout) CalloutAction {
            std.debug.assert(value.number == 7);
            return .reject;
        }
        fn abort(_: ?*anyopaque, _: Callout) CalloutAction {
            return .abort;
        }
    };
    runner.callout = Callback.reject;
    try std.testing.expect(try runner.find("bbc", 0) == null);
    runner.callout = Callback.abort;
    try std.testing.expectError(error.CalloutAborted, runner.find("bbc", 0));
    try std.testing.expectEqual(@as(usize, 0), runner.live_bytes);
    var quoted = try BacktrackingPattern.init(a, "a(?C{some }} text})b", .{});
    defer quoted.deinit();
    var r = try BacktrackingRunner.init(a, &quoted);
    defer r.deinit();
    try std.testing.expect(try r.find("ab", 0) != null);
}

test "vectorized repeats preserve scalar captures and exact step budgets" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "a+z", "(a{0,40})(a+)", "a{3,9}?a", "a{4}b", "a*?z", "a{0}z", "(a{1,64})a?z" }) |expression| {
        var pattern = try BacktrackingPattern.init(a, expression, .{});
        defer pattern.deinit();
        var fast = try BacktrackingRunner.init(a, &pattern);
        defer fast.deinit();
        var scalar = try BacktrackingRunner.init(a, &pattern);
        defer scalar.deinit();
        scalar.jit_enabled = false;
        scalar.ascii_repeat_enabled = false;
        for ([_][]const u8{ "", "bb", "aaaab", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaz", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaz" }) |text| for ([_]usize{ 0, 1, 2, 3, 4, 5, 8, 16, 32, 64, 128, 1024 }) |budget| {
            fast.max_steps = budget;
            scalar.max_steps = budget;
            const actual = fast.find(text, 0) catch |err| {
                try std.testing.expectError(err, scalar.find(text, 0));
                try std.testing.expectEqual(scalar.steps, fast.steps);
                continue;
            };
            const expected = try scalar.find(text, 0);
            try std.testing.expectEqual(expected != null, actual != null);
            if (actual) |match| {
                try std.testing.expectEqual(expected.?.span, match.span);
                try std.testing.expectEqualSlices(?Span, expected.?.captures, match.captures);
            }
            try std.testing.expectEqual(scalar.steps, fast.steps);
        };
    }
}

test "positive lookarounds inherit and return named marks to callouts" {
    const Callback = struct {
        fn check(_: ?*anyopaque, value: Callout) CalloutAction {
            return if (std.mem.eql(u8, value.mark orelse "", if (value.number == 1) "outer" else "inner")) .proceed else .abort;
        }
    };
    const a = std.testing.allocator;
    var pattern = try BacktrackingPattern.init(a, "(*MARK:outer)(?=(?C1)a(*MARK:inner))(?C2)a", .{});
    defer pattern.deinit();
    var runner = try BacktrackingRunner.init(a, &pattern);
    defer runner.deinit();
    runner.callout = Callback.check;
    try std.testing.expect(try runner.find("a", 0) != null);
    try std.testing.expectEqualStrings("inner", runner.mark.?);
}

test "native JIT agrees with the VM for spans, captures, Unicode chains and step budgets" {
    if (!native_jit.Program.supported()) return error.SkipZigTest;
    const a = std.testing.allocator;
    for ([_][]const u8{ "(abc|ab)+(?=!)", "(?i)x[a-z]+(\\d+)\\1", "a{0,4}b", "(?<n>é|abc)\\k<n>", "abc(*MARK:ok)(?C7)(*ACCEPT)", "((a)(b))\\K(?<n>é|c)", "(a)?(b)?(c)?", "(?=(a))a\\K(b)", "(?<=(a))(b)", "(?:(a)|(b))(*THEN)c" }) |expression| {
        var pattern = try BacktrackingPattern.init(a, expression, .{});
        defer pattern.deinit();
        try std.testing.expect(pattern.jit != null);
        var fast = try BacktrackingRunner.init(a, &pattern);
        defer fast.deinit();
        var vm = try BacktrackingRunner.init(a, &pattern);
        defer vm.deinit();
        vm.jit_enabled = false;
        for ([_][]const u8{ "", "abcab!", "Abc421421", "xAbc421421", "ab", "éé", "abcabc", "\xffabc" }) |subject| for (0..subject.len + 1) |offset| {
            const actual = try fast.find(subject, offset);
            const expected = try vm.find(subject, offset);
            try std.testing.expectEqual(expected != null, actual != null);
            if (actual) |match| {
                try std.testing.expectEqual(expected.?.span, match.span);
                try std.testing.expectEqualSlices(?Span, expected.?.captures, match.captures);
            }
            try std.testing.expectEqual(vm.steps, fast.steps);
        };
        fast.max_steps = 1;
        vm.max_steps = 1;
        try std.testing.expectError(error.MatchLimitExceeded, fast.find("abcabc!xAbc421421", 0));
        try std.testing.expectError(error.MatchLimitExceeded, vm.find("abcabc!xAbc421421", 0));
    }
}

test {
    _ = @import("literal.zig");
    _ = @import("dfa.zig");
    _ = @import("prefilter.zig");
}

test "backtracking limits restart for each subject offset and scratch stays bounded" {
    const a = std.testing.allocator;
    var pattern = try BacktrackingPattern.init(a, "a(?=b)b", .{});
    defer pattern.deinit();
    var runner = try BacktrackingRunner.init(a, &pattern);
    defer runner.deinit();
    runner.max_steps = 32;
    const subject = try a.alloc(u8, 10002);
    defer a.free(subject);
    @memset(subject, 'z');
    @memcpy(subject[10000..], "ab");
    try std.testing.expectEqual(Span{ .start = 10000, .end = 10002 }, (try runner.find(subject, 0)).?.span);
    try std.testing.expectEqual(@as(usize, 0), runner.live_bytes);
    try std.testing.expect(runner.cached_bytes <= 256 * 1024);
}

test "bounded lookbehind does not rescan an unbounded subject prefix" {
    const a = std.testing.allocator;
    const subject = try a.alloc(u8, 10003);
    defer a.free(subject);
    @memset(subject, 'z');
    @memcpy(subject[10000..], "abc");
    for ([_][]const u8{ "(?<=ab)c", "(?(?<=ab)c|q)" }) |expression| {
        var pattern = try BacktrackingPattern.init(a, expression, .{});
        defer pattern.deinit();
        var runner = try BacktrackingRunner.init(a, &pattern);
        defer runner.deinit();
        runner.max_steps = 128;
        const found = (try runner.find(subject, 0)).?;
        try std.testing.expectEqual(@as(usize, 10002), found.span.start);
        try std.testing.expectEqual(@as(usize, 10003), found.span.end);
    }
}

test "native repeats use bounded memory and recycle subroutine snapshots" {
    const a = std.testing.allocator;
    var budget = @import("bounded.zig").Budget{ .backing = a, .limit = 256 * 1024 };
    var pattern = try BacktrackingPattern.init(budget.allocator(), "a*ab", .{});
    defer pattern.deinit();
    var runner = try BacktrackingRunner.init(budget.allocator(), &pattern);
    defer runner.deinit();
    const text = try a.alloc(u8, 65536);
    defer a.free(text);
    @memset(text, 'a');
    text[text.len - 1] = 'b';
    const match = (try runner.find(text, 0)).?;
    try std.testing.expectEqual(Span{ .start = 0, .end = text.len }, match.span);
    try std.testing.expect(budget.peak <= budget.limit);
    var calls = try BacktrackingPattern.init(a, "(?<n>aa)(?&n){1000}", .{});
    defer calls.deinit();
    var call_runner = try BacktrackingRunner.init(a, &calls);
    defer call_runner.deinit();
    @memset(text, 'a');
    const called = (try call_runner.find(text[0..2002], 0)).?;
    try std.testing.expectEqual(Span{ .start = 0, .end = 2002 }, called.span);
    try std.testing.expectEqual(Span{ .start = 0, .end = 2 }, called.captures[1].?);
    try std.testing.expect(call_runner.live_snapshot_bytes == 0);
    try std.testing.expect(call_runner.call_links.items.len <= 2);
}

test "native dialect preserves non-atomic captures cuts and recursive conditions" {
    const a = std.testing.allocator;
    const cases = [_]struct { expression: []const u8, text: []const u8, span: ?Span, capture: ?Span = null }{
        .{ .expression = "(?*(a|ab))\\1c", .text = "abc", .span = .{ .start = 0, .end = 3 }, .capture = .{ .start = 0, .end = 2 } },
        .{ .expression = "(?*(a|ab))\\1(*COMMIT)c", .text = "abc", .span = null },
        .{ .expression = "(?(DEFINE)(?<n>a))(?&n)\\k<n>", .text = "aa", .span = null },
        .{ .expression = "(a|b)(?1)", .text = "ab", .span = .{ .start = 0, .end = 2 }, .capture = .{ .start = 0, .end = 1 } },
        .{ .expression = "(?<n>a(?(R&n)b|c))(?&n)", .text = "acab", .span = .{ .start = 0, .end = 4 }, .capture = .{ .start = 0, .end = 2 } },
        .{ .expression = "(?<R>a)(?(R)b|c)", .text = "ab", .span = .{ .start = 0, .end = 2 }, .capture = .{ .start = 0, .end = 1 } },
        .{ .expression = "(ab)(*scs:(1)(a))c\\2", .text = "abca", .span = .{ .start = 0, .end = 4 }, .capture = .{ .start = 0, .end = 2 } },
        .{ .expression = "(?>a(*COMMIT))b|ac", .text = "ac", .span = .{ .start = 0, .end = 2 } },
        .{ .expression = "(*sr:\\S+)", .text = "aа", .span = .{ .start = 0, .end = 1 } },
        .{ .expression = "(*asr:\\S+)", .text = "aа", .span = .{ .start = 1, .end = 3 } },
        .{ .expression = "(*sr:\\S+)", .text = "漢あカ", .span = .{ .start = 0, .end = 9 } },
        .{ .expression = "(?i)(?[[A]-[a]])", .text = "Aa", .span = null },
        .{ .expression = "(?r)(?i)k", .text = "Kk", .span = .{ .start = 3, .end = 4 } },
        .{ .expression = "(?<é>a)\\k<é>", .text = "aa", .span = .{ .start = 0, .end = 2 }, .capture = .{ .start = 0, .end = 1 } },
        .{ .expression = "a{ 1 , 3 }", .text = "aaaa", .span = .{ .start = 0, .end = 3 } },
    };
    for (cases) |case| {
        var pattern = try BacktrackingPattern.init(a, case.expression, .{});
        defer pattern.deinit();
        var runner = try BacktrackingRunner.init(a, &pattern);
        defer runner.deinit();
        const found = try runner.find(case.text, 0);
        try std.testing.expectEqual(case.span != null, found != null);
        if (found) |match| {
            try std.testing.expectEqual(case.span.?, match.span);
            if (case.capture) |capture| try std.testing.expectEqual(capture, match.captures[1].?);
        }
    }
    for ([_][]const u8{ "(?(DEFINE)a|b)", "(?(0)a)", "(?18446744073709551615)", "\\g{18446744073709551615}", "(*scs:(0)a)" }) |expression| {
        var invalid = BacktrackingPattern.init(a, expression, .{}) catch continue;
        invalid.deinit();
        return error.InvalidPatternAccepted;
    }
}

fn previousStart(text: []const u8, end: usize, utf8: bool) usize {
    if (utf8) {
        var distance: usize = 2;
        while (distance <= 4 and distance <= end) : (distance += 1) {
            const candidate = end - distance;
            if (unicode.decode(text, candidate, true).len == distance) return candidate;
        }
    }
    return end - 1;
}

test "lookbehind preserves branch priority and bounds character candidates" {
    const a = std.testing.allocator;
    var pattern = try BacktrackingPattern.init(a, "(?<=(a)|(ba))c", .{});
    defer pattern.deinit();
    var runner = try BacktrackingRunner.init(a, &pattern);
    defer runner.deinit();
    const found = (try runner.find("bac", 0)).?;
    try std.testing.expectEqual(Span{ .start = 1, .end = 2 }, found.captures[1].?);
    try std.testing.expect(found.captures[2] == null);
    for ([_][]const u8{ "(?<=a*)b", "(?<=a{1,256})b", "(?<=\\X)b", "(?<=\\C)b" }) |invalid| {
        try std.testing.expectError(error.InvalidRegex, BacktrackingPattern.init(a, invalid, .{}));
    }
    var unicode_pattern = try BacktrackingPattern.init(a, "(?<=é{1,3})x", .{});
    defer unicode_pattern.deinit();
    var unicode_runner = try BacktrackingRunner.init(a, &unicode_pattern);
    defer unicode_runner.deinit();
    try std.testing.expectEqual(Span{ .start = 4, .end = 5 }, (try unicode_runner.find("ééx", 0)).?.span);
}

test "malformed UTF does not swallow following valid characters" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(unicode.Decoded{ .cp = 0x110000, .len = 1 }, unicode.decode("\xc2a", 0, true));
    var pattern = try BacktrackingPattern.init(a, "[a]", .{});
    defer pattern.deinit();
    for ([_]bool{ true, false }) |jit_enabled| {
        var runner = try BacktrackingRunner.init(a, &pattern);
        defer runner.deinit();
        runner.jit_enabled = jit_enabled;
        try std.testing.expectEqual(Span{ .start = 1, .end = 2 }, (try runner.find("\xc2a", 0)).?.span);
    }
}
