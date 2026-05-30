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

//! NFA-based regex engine implementing the vellum Automaton interface.
//!
//! Uses Thompson's NFA construction for regex → NFA, then on-the-fly
//! DFA construction (powerset/subset construction) for efficient FST
//! traversal via FST.search(automaton).
//!
//! Supported syntax:
//!   .        any byte
//!   *        zero or more of preceding
//!   +        one or more of preceding
//!   ?        zero or one of preceding
//!   |        alternation
//!   ()       grouping
//!   [abc]    character class
//!   [a-z]    character range
//!   [^abc]   negated class
//!   \        escape next character
//!   ^        anchor start (implicit)
//!   $        anchor end (implicit)

const std = @import("std");
const Allocator = std.mem.Allocator;
const vellum = @import("antfly_vellum");

const dead_state = std.math.maxInt(usize);
const transition_unknown = dead_state - 1;

/// Maximum NFA states supported. State sets are represented as bitsets of this size.
const max_nfa_states = 256;

/// A bitset representing a set of NFA states.
const StateBitSet = std.StaticBitSet(max_nfa_states);

/// NFA state for Thompson's construction.
const NFAState = struct {
    /// For split states: two epsilon transitions. -1 means none.
    out1: i32 = -1,
    out2: i32 = -1,
    /// For match states: the byte to match. -1 means epsilon/split.
    /// -2 means "match any" (dot).
    match_byte: i16 = -1,
    /// For character class states: index into char_classes array.
    char_class_idx: i16 = -1,
    /// Is this an accepting state?
    is_match: bool = false,
};

/// Character class definition.
const CharClass = struct {
    /// Bitmap of which bytes match.
    bytes: [256]bool = @as([256]bool, @splat(false)),
    negated: bool = false,

    fn matches(self: *const CharClass, b: u8) bool {
        return self.bytes[b] != self.negated;
    }
};

/// NFA fragment used during Thompson's construction.
const Fragment = struct {
    start: u16,
    /// Index of the "dangling" out pointer to patch.
    /// This is the state whose out1 needs to be set to the next fragment's start.
    out: u16,
    /// Which output of the out state to patch (1 or 2).
    out_slot: u8 = 1,
    /// Additional dangling outputs for alternation.
    out_list: ?*PatchList = null,
};

const PatchList = struct {
    state: u16,
    slot: u8,
    next: ?*PatchList = null,
};

/// Compiled regex automaton for FST traversal.
pub const RegexAutomaton = struct {
    alloc: Allocator,
    states: []NFAState,
    num_states: u16,
    char_classes: []CharClass,
    num_classes: u16,
    epsilon_closures: []StateBitSet,
    byte_classes: [256]u8,
    byte_class_representatives: [256]u8,
    num_byte_classes: u16,
    start_state: u16,
    anchored_start: bool,
    anchored_end: bool,
    prefix_literals: [][]u8,
    prefix_first_bytes: []u8,
    prefix_check_offsets: []u8,
    /// DFA state cache: maps state set → DFA state index.
    /// Index 0 is always the start state.
    dfa_cache_keys: std.ArrayListUnmanaged(StateBitSet),
    dfa_cache_match: std.ArrayListUnmanaged(bool),
    dfa_cache_index: std.AutoHashMapUnmanaged(StateBitSet, usize),
    dfa_cache_transitions: std.ArrayListUnmanaged(usize),

    pub fn deinit(self: *RegexAutomaton) void {
        self.alloc.free(self.states);
        self.alloc.free(self.char_classes);
        self.alloc.free(self.epsilon_closures);
        for (self.prefix_literals) |prefix| self.alloc.free(prefix);
        self.alloc.free(self.prefix_literals);
        self.alloc.free(self.prefix_first_bytes);
        self.alloc.free(self.prefix_check_offsets);
        self.dfa_cache_keys.deinit(self.alloc);
        self.dfa_cache_match.deinit(self.alloc);
        self.dfa_cache_index.deinit(self.alloc);
        self.dfa_cache_transitions.deinit(self.alloc);
    }

    /// Get a vellum.Automaton interface for FST traversal.
    pub fn automaton(self: *RegexAutomaton) vellum.Automaton {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .start = @ptrCast(&startFn),
                .isMatch = @ptrCast(&isMatchFn),
                .canMatch = @ptrCast(&canMatchFn),
                .willAlwaysMatch = @ptrCast(&willAlwaysMatchFn),
                .accept = @ptrCast(&acceptFn),
            },
        };
    }

    fn epsilonClosure(self: *const RegexAutomaton, initial: StateBitSet) StateBitSet {
        var result = StateBitSet.initEmpty();
        var it = initial.iterator(.{});
        while (it.next()) |idx| {
            result.setUnion(self.epsilon_closures[idx]);
        }
        return result;
    }

    fn stepNFA(self: *const RegexAutomaton, state_set: StateBitSet, b: u8) StateBitSet {
        var next = StateBitSet.initEmpty();
        var it = state_set.iterator(.{});
        while (it.next()) |idx| {
            const s = self.states[idx];
            if (s.match_byte == -2) {
                // Dot: match any byte
                if (s.out1 >= 0) next.set(@intCast(s.out1));
            } else if (s.char_class_idx >= 0) {
                // Character class
                if (self.char_classes[@intCast(s.char_class_idx)].matches(b)) {
                    if (s.out1 >= 0) next.set(@intCast(s.out1));
                }
            } else if (s.match_byte >= 0 and @as(u8, @intCast(s.match_byte)) == b) {
                if (s.out1 >= 0) next.set(@intCast(s.out1));
            }
        }
        return self.epsilonClosure(next);
    }

    fn lookupOrInsert(self: *RegexAutomaton, set: StateBitSet) !usize {
        if (self.dfa_cache_index.get(set)) |idx| return idx;

        const idx = self.dfa_cache_keys.items.len;
        try self.dfa_cache_keys.append(self.alloc, set);

        var is_match = false;
        var it = set.iterator(.{});
        while (it.next()) |s| {
            if (self.states[s].is_match) {
                is_match = true;
                break;
            }
        }
        try self.dfa_cache_match.append(self.alloc, is_match);
        try self.dfa_cache_index.put(self.alloc, set, idx);
        try self.dfa_cache_transitions.ensureUnusedCapacity(self.alloc, self.num_byte_classes);
        for (0..self.num_byte_classes) |_| {
            self.dfa_cache_transitions.appendAssumeCapacity(transition_unknown);
        }
        return idx;
    }

    fn startFn(self: *RegexAutomaton) usize {
        // Build start state from epsilon closure of NFA start
        if (self.dfa_cache_keys.items.len > 0) return 0;

        var initial = StateBitSet.initEmpty();
        initial.set(self.start_state);
        const start_set = self.epsilonClosure(initial);
        _ = self.lookupOrInsert(start_set) catch return dead_state;
        return 0;
    }

    fn isMatchFn(self: *RegexAutomaton, state: usize) bool {
        if (state == dead_state) return false;
        if (state >= self.dfa_cache_match.items.len) return false;
        return self.dfa_cache_match.items[state];
    }

    fn canMatchFn(_: *RegexAutomaton, state: usize) bool {
        return state != dead_state;
    }

    fn willAlwaysMatchFn(_: *RegexAutomaton, _: usize) bool {
        return false;
    }

    fn acceptFn(self: *RegexAutomaton, state: usize, b: u8) usize {
        if (state == dead_state) return dead_state;
        if (state >= self.dfa_cache_keys.items.len) return dead_state;

        const class_idx = self.byte_classes[b];
        const transition_idx = state * self.num_byte_classes + class_idx;
        const cached = self.dfa_cache_transitions.items[transition_idx];
        if (cached != transition_unknown) return cached;

        const current_set = self.dfa_cache_keys.items[state];
        const next_set = self.stepNFA(current_set, self.byte_class_representatives[class_idx]);

        // Empty set → dead state
        if (next_set.count() == 0) {
            self.dfa_cache_transitions.items[transition_idx] = dead_state;
            return dead_state;
        }

        const next_state = self.lookupOrInsert(next_set) catch dead_state;
        self.dfa_cache_transitions.items[transition_idx] = next_state;
        return next_state;
    }
};

// ============================================================================
// Regex compiler (Thompson's construction)
// ============================================================================

pub fn compile(alloc: Allocator, pattern: []const u8) !RegexAutomaton {
    const analysis = try analyzePattern(alloc, pattern);
    errdefer freePrefixSet(alloc, analysis.prefix_literals);
    errdefer alloc.free(analysis.prefix_first_bytes);
    errdefer alloc.free(analysis.prefix_check_offsets);

    var compiler = Compiler{
        .alloc = alloc,
        .pattern = pattern,
        .pos = 0,
        .states = try alloc.alloc(NFAState, max_nfa_states),
        .num_states = 0,
        .char_classes = try alloc.alloc(CharClass, 64),
        .num_classes = 0,
    };
    errdefer {
        alloc.free(compiler.states);
        alloc.free(compiler.char_classes);
    }

    const frag = try compiler.parseExpr();

    // Patch final fragment to point to accepting state
    const accept_state = compiler.addState(.{ .is_match = true });
    compiler.patchFragment(frag, accept_state);
    const epsilon_closures = try precomputeEpsilonClosures(alloc, compiler.states[0..compiler.num_states]);
    errdefer alloc.free(epsilon_closures);
    const byte_class_info = buildByteClasses(
        compiler.states[0..compiler.num_states],
        compiler.char_classes[0..compiler.num_classes],
    );

    return .{
        .alloc = alloc,
        .states = compiler.states,
        .num_states = compiler.num_states,
        .char_classes = compiler.char_classes,
        .num_classes = compiler.num_classes,
        .epsilon_closures = epsilon_closures,
        .byte_classes = byte_class_info.classes,
        .byte_class_representatives = byte_class_info.representatives,
        .num_byte_classes = byte_class_info.count,
        .start_state = frag.start,
        .anchored_start = analysis.anchored_start,
        .anchored_end = analysis.anchored_end,
        .prefix_literals = analysis.prefix_literals,
        .prefix_first_bytes = analysis.prefix_first_bytes,
        .prefix_check_offsets = analysis.prefix_check_offsets,
        .dfa_cache_keys = .empty,
        .dfa_cache_match = .empty,
        .dfa_cache_index = .empty,
        .dfa_cache_transitions = .empty,
    };
}

const ByteClassInfo = struct {
    classes: [256]u8,
    representatives: [256]u8,
    count: u16,
};

fn buildByteClasses(states: []const NFAState, char_classes: []const CharClass) ByteClassInfo {
    var classes: [256]u8 = undefined;
    var representatives = @as([256]u8, @splat(0));
    var count: u16 = 0;

    for (0..256) |byte_idx| {
        const b: u8 = @intCast(byte_idx);
        var class_idx: ?u8 = null;
        for (representatives[0..count], 0..) |representative, idx| {
            if (bytesAreEquivalent(states, char_classes, representative, b)) {
                class_idx = @intCast(idx);
                break;
            }
        }

        if (class_idx) |idx| {
            classes[byte_idx] = idx;
            continue;
        }

        representatives[count] = b;
        classes[byte_idx] = @intCast(count);
        count += 1;
    }

    if (count == 0) {
        representatives[0] = 0;
        count = 1;
    }

    return .{
        .classes = classes,
        .representatives = representatives,
        .count = count,
    };
}

fn precomputeEpsilonClosures(alloc: Allocator, states: []const NFAState) ![]StateBitSet {
    const closures = try alloc.alloc(StateBitSet, states.len);
    errdefer alloc.free(closures);

    for (states, 0..) |_, idx| {
        closures[idx] = computeSingleStateEpsilonClosure(states, idx);
    }

    return closures;
}

fn computeSingleStateEpsilonClosure(states: []const NFAState, start_idx: usize) StateBitSet {
    var closure = StateBitSet.initEmpty();
    var stack: [max_nfa_states]u16 = undefined;
    var stack_len: usize = 0;

    closure.set(start_idx);
    stack[stack_len] = @intCast(start_idx);
    stack_len += 1;

    while (stack_len > 0) {
        stack_len -= 1;
        const idx = stack[stack_len];
        const state = states[idx];
        if (state.match_byte != -1 or state.char_class_idx != -1) continue;

        if (state.out1 >= 0) {
            const next: usize = @intCast(state.out1);
            if (!closure.isSet(next)) {
                closure.set(next);
                stack[stack_len] = @intCast(next);
                stack_len += 1;
            }
        }

        if (state.out2 >= 0) {
            const next: usize = @intCast(state.out2);
            if (!closure.isSet(next)) {
                closure.set(next);
                stack[stack_len] = @intCast(next);
                stack_len += 1;
            }
        }
    }

    return closure;
}

fn bytesAreEquivalent(states: []const NFAState, char_classes: []const CharClass, lhs: u8, rhs: u8) bool {
    for (states) |state| {
        if (stateMatchesByte(state, char_classes, lhs) != stateMatchesByte(state, char_classes, rhs)) {
            return false;
        }
    }
    return true;
}

fn stateMatchesByte(state: NFAState, char_classes: []const CharClass, b: u8) bool {
    if (state.match_byte == -2) return true;
    if (state.char_class_idx >= 0) {
        return char_classes[@intCast(state.char_class_idx)].matches(b);
    }
    return state.match_byte >= 0 and @as(u8, @intCast(state.match_byte)) == b;
}

const PatternAnalysis = struct {
    anchored_start: bool,
    anchored_end: bool,
    prefix_literals: [][]u8,
    prefix_first_bytes: []u8,
    prefix_check_offsets: []u8,
};

fn analyzePattern(alloc: Allocator, pattern: []const u8) !PatternAnalysis {
    const anchored_start = pattern.len > 0 and pattern[0] == '^';
    const anchored_end = hasTrailingAnchor(pattern);
    const start_idx: usize = if (anchored_start) 1 else 0;
    const stop_idx: usize = if (anchored_end and pattern.len > 0) pattern.len - 1 else pattern.len;

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var parser = PrefixParser{
        .alloc = arena.allocator(),
        .pattern = pattern[start_idx..stop_idx],
    };
    const temp_prefixes = try parser.parse();
    const prefixes = try dupePrefixSet(alloc, temp_prefixes);
    errdefer freePrefixSet(alloc, prefixes);
    const first_bytes = try computePrefixFirstBytes(alloc, prefixes);
    errdefer alloc.free(first_bytes);
    const check_offsets = try computePrefixCheckOffsets(alloc, prefixes);
    errdefer alloc.free(check_offsets);

    return .{
        .anchored_start = anchored_start,
        .anchored_end = anchored_end,
        .prefix_literals = prefixes,
        .prefix_first_bytes = first_bytes,
        .prefix_check_offsets = check_offsets,
    };
}

fn hasTrailingAnchor(pattern: []const u8) bool {
    if (pattern.len == 0 or pattern[pattern.len - 1] != '$') return false;

    var backslash_count: usize = 0;
    var idx = pattern.len - 1;
    while (idx > 0) {
        idx -= 1;
        if (pattern[idx] != '\\') break;
        backslash_count += 1;
    }
    return backslash_count % 2 == 0;
}

const max_prefix_literals = 8;
const max_prefix_len = 32;
const empty_prefix_set = [_][]const u8{};
const seed_prefix_set = [_][]const u8{""};

const PrefixParser = struct {
    alloc: Allocator,
    pattern: []const u8,
    pos: usize = 0,

    fn parse(self: *PrefixParser) Allocator.Error![]const []const u8 {
        return self.parseExpr();
    }

    fn parseExpr(self: *PrefixParser) Allocator.Error![]const []const u8 {
        var result = try self.parseConcat();
        if (result.len == 0) return &empty_prefix_set;

        while (self.pos < self.pattern.len and self.pattern[self.pos] == '|') {
            self.pos += 1;
            const branch = try self.parseConcat();
            if (branch.len == 0) return &empty_prefix_set;
            result = try unionPrefixSets(self.alloc, result, branch);
            if (result.len == 0) return &empty_prefix_set;
        }

        return result;
    }

    fn parseConcat(self: *PrefixParser) Allocator.Error![]const []const u8 {
        var current: []const []const u8 = &seed_prefix_set;
        var have_required_prefix = false;

        while (self.pos < self.pattern.len) {
            const c = self.pattern[self.pos];
            if (c == '|' or c == ')') break;

            const atom = try self.parseAtom();
            if (atom.len == 0) break;

            const quantifier = if (self.pos < self.pattern.len) self.pattern[self.pos] else 0;
            switch (quantifier) {
                '*' => {
                    self.pos += 1;
                    break;
                },
                '?' => {
                    self.pos += 1;
                    break;
                },
                '+' => {
                    self.pos += 1;
                    current = try concatPrefixSets(self.alloc, current, atom);
                    if (current.len == 0) return &empty_prefix_set;
                    have_required_prefix = true;
                    break;
                },
                else => {
                    current = try concatPrefixSets(self.alloc, current, atom);
                    if (current.len == 0) return &empty_prefix_set;
                    have_required_prefix = true;
                },
            }
        }

        if (!have_required_prefix) return &empty_prefix_set;
        return current;
    }

    fn parseAtom(self: *PrefixParser) Allocator.Error![]const []const u8 {
        if (self.pos >= self.pattern.len) return &empty_prefix_set;

        const c = self.pattern[self.pos];
        switch (c) {
            '(' => {
                self.pos += 1;
                const nested = try self.parseExpr();
                if (self.pos >= self.pattern.len or self.pattern[self.pos] != ')') return &empty_prefix_set;
                self.pos += 1;
                return nested;
            },
            '\\' => {
                self.pos += 1;
                if (self.pos >= self.pattern.len) return &empty_prefix_set;
                const escaped = self.pattern[self.pos];
                self.pos += 1;
                return try singlePrefix(self.alloc, escaped);
            },
            '.', '[', ']', '{', '}', '^', '$', '*', '+', '?' => return &empty_prefix_set,
            else => {
                self.pos += 1;
                return try singlePrefix(self.alloc, c);
            },
        }
    }
};

fn singlePrefix(alloc: Allocator, byte: u8) ![]const []const u8 {
    const literal = try alloc.alloc(u8, 1);
    literal[0] = byte;

    const set = try alloc.alloc([]const u8, 1);
    set[0] = literal;
    return set;
}

fn concatPrefixSets(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);

    for (left) |a| {
        for (right) |b| {
            if (out.items.len >= max_prefix_literals) return &empty_prefix_set;
            if (a.len + b.len > max_prefix_len) return &empty_prefix_set;

            const combined = try alloc.alloc(u8, a.len + b.len);
            @memcpy(combined[0..a.len], a);
            @memcpy(combined[a.len..], b);
            try out.append(alloc, combined);
        }
    }

    return try out.toOwnedSlice(alloc);
}

fn unionPrefixSets(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);

    for (left) |prefix| try appendUniquePrefix(alloc, &out, prefix);
    for (right) |prefix| try appendUniquePrefix(alloc, &out, prefix);

    if (out.items.len > max_prefix_literals) return &empty_prefix_set;
    return try out.toOwnedSlice(alloc);
}

fn appendUniquePrefix(alloc: Allocator, out: *std.ArrayListUnmanaged([]const u8), prefix: []const u8) !void {
    for (out.items) |existing| {
        if (std.mem.eql(u8, existing, prefix)) return;
    }

    const copy = try alloc.dupe(u8, prefix);
    try out.append(alloc, copy);
}

fn dupePrefixSet(alloc: Allocator, prefixes: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, prefixes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |prefix| alloc.free(prefix);
        alloc.free(out);
    }

    for (prefixes, 0..) |prefix, i| {
        out[i] = try alloc.dupe(u8, prefix);
        initialized += 1;
    }
    return out;
}

fn freePrefixSet(alloc: Allocator, prefixes: [][]u8) void {
    for (prefixes) |prefix| alloc.free(prefix);
    alloc.free(prefixes);
}

fn computePrefixFirstBytes(alloc: Allocator, prefixes: [][]u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (prefixes) |prefix| {
        if (prefix.len == 0) continue;
        const first_byte = prefix[0];
        var seen = false;
        for (out.items) |existing| {
            if (existing == first_byte) {
                seen = true;
                break;
            }
        }
        if (!seen) try out.append(alloc, first_byte);
    }

    return try out.toOwnedSlice(alloc);
}

fn computePrefixCheckOffsets(alloc: Allocator, prefixes: [][]u8) ![]u8 {
    const offsets = try alloc.alloc(u8, prefixes.len);
    for (prefixes, 0..) |prefix, idx| {
        offsets[idx] = @intCast(computePrefixCheckOffset(prefix));
    }
    return offsets;
}

fn computePrefixCheckOffset(prefix: []const u8) usize {
    if (prefix.len <= 1) return 0;

    var best_idx: usize = 1;
    var best_score = prefixByteRarityScore(prefix[1]);
    for (prefix[2..], 2..) |b, idx| {
        const score = prefixByteRarityScore(b);
        if (score > best_score) {
            best_idx = idx;
            best_score = score;
        }
    }
    return best_idx;
}

fn prefixByteRarityScore(b: u8) u8 {
    return switch (b) {
        0...8, 11...12, 14...31, 127...255 => 255,
        ' ', '\t', '\n', '\r' => 4,
        'a', 'e', 'i', 'o', 'n', 'r', 's', 't', 'l' => 8,
        '0'...'9' => 20,
        'A'...'Z' => 24,
        'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'm', 'p', 'q', 'u', 'v', 'w', 'x', 'y', 'z' => 32,
        else => 64,
    };
}

const Compiler = struct {
    alloc: Allocator,
    pattern: []const u8,
    pos: usize,
    states: []NFAState,
    num_states: u16,
    char_classes: []CharClass,
    num_classes: u16,

    fn addState(self: *Compiler, state: NFAState) u16 {
        const idx = self.num_states;
        self.states[idx] = state;
        self.num_states += 1;
        return idx;
    }

    fn addClass(self: *Compiler, class: CharClass) u16 {
        const idx = self.num_classes;
        self.char_classes[idx] = class;
        self.num_classes += 1;
        return idx;
    }

    fn patchFragment(self: *Compiler, frag: Fragment, target: u16) void {
        self.patchOne(frag.out, frag.out_slot, target);
        // Patch any additional outputs from alternation
        var pl = frag.out_list;
        while (pl) |p| {
            self.patchOne(p.state, p.slot, target);
            pl = p.next;
        }
    }

    fn patchOne(self: *Compiler, state: u16, slot: u8, target: u16) void {
        if (slot == 1) {
            self.states[state].out1 = @intCast(target);
        } else {
            self.states[state].out2 = @intCast(target);
        }
    }

    const ParseError = error{InvalidRegex};

    // expr → concat ('|' concat)*
    fn parseExpr(self: *Compiler) ParseError!Fragment {
        var left = try self.parseConcat();

        while (self.pos < self.pattern.len and self.pattern[self.pos] == '|') {
            self.pos += 1;
            const right = try self.parseConcat();

            // Create split state
            const split = self.addState(.{
                .out1 = @intCast(left.start),
                .out2 = @intCast(right.start),
            });

            // Both fragments' dangling outputs need to be patched together
            // We create a new fragment that collects both outputs
            // Use a simple approach: create a join state
            const join = self.addState(.{});
            self.patchFragment(left, join);
            self.patchFragment(right, join);

            left = .{
                .start = split,
                .out = join,
                .out_slot = 1,
            };
        }

        return left;
    }

    // concat → quantified+
    fn parseConcat(self: *Compiler) ParseError!Fragment {
        var result: ?Fragment = null;

        while (self.pos < self.pattern.len) {
            const c = self.pattern[self.pos];
            if (c == '|' or c == ')') break;

            const frag = try self.parseQuantified();

            if (result) |r| {
                // Concatenate: patch result's output to frag's start
                self.patchFragment(r, frag.start);
                result = .{
                    .start = r.start,
                    .out = frag.out,
                    .out_slot = frag.out_slot,
                    .out_list = frag.out_list,
                };
            } else {
                result = frag;
            }
        }

        if (result) |r| return r;

        // Empty expression: epsilon
        const s = self.addState(.{});
        return .{ .start = s, .out = s, .out_slot = 1 };
    }

    // quantified → atom ('*' | '+' | '?')?
    fn parseQuantified(self: *Compiler) ParseError!Fragment {
        const frag = try self.parseAtom();

        if (self.pos < self.pattern.len) {
            const c = self.pattern[self.pos];
            if (c == '*') {
                self.pos += 1;
                // Zero or more: split → (frag → back to split) | out
                const split = self.addState(.{
                    .out1 = @intCast(frag.start),
                    // out2 is the "skip" path, left dangling
                });
                self.patchFragment(frag, split); // loop back
                return .{ .start = split, .out = split, .out_slot = 2 };
            } else if (c == '+') {
                self.pos += 1;
                // One or more: frag → split → (frag again | out)
                const split = self.addState(.{
                    .out1 = @intCast(frag.start),
                    // out2 left dangling
                });
                self.patchFragment(frag, split);
                return .{ .start = frag.start, .out = split, .out_slot = 2 };
            } else if (c == '?') {
                self.pos += 1;
                // Zero or one: split → (frag | out)
                const split = self.addState(.{
                    .out1 = @intCast(frag.start),
                    // out2 is the "skip" path, left dangling
                });
                // Both frag's output and split's out2 need to be patched
                // Create a join point
                const join = self.addState(.{});
                self.patchFragment(frag, join);
                self.states[split].out2 = @intCast(join);
                return .{ .start = split, .out = join, .out_slot = 1 };
            }
        }

        return frag;
    }

    // atom → '(' expr ')' | '[' class ']' | '.' | '\' char | literal
    fn parseAtom(self: *Compiler) ParseError!Fragment {
        if (self.pos >= self.pattern.len) return error.InvalidRegex;

        const c = self.pattern[self.pos];

        if (c == '(') {
            self.pos += 1;
            const frag = try self.parseExpr();
            if (self.pos >= self.pattern.len or self.pattern[self.pos] != ')')
                return error.InvalidRegex;
            self.pos += 1;
            return frag;
        }

        if (c == '[') {
            return self.parseCharClass();
        }

        if (c == '.') {
            self.pos += 1;
            const s = self.addState(.{ .match_byte = -2 }); // any
            return .{ .start = s, .out = s, .out_slot = 1 };
        }

        if (c == '\\') {
            self.pos += 1;
            if (self.pos >= self.pattern.len) return error.InvalidRegex;
            const escaped = self.pattern[self.pos];
            self.pos += 1;
            const s = self.addState(.{ .match_byte = @intCast(escaped) });
            return .{ .start = s, .out = s, .out_slot = 1 };
        }

        if (c == '^' or c == '$') {
            // Anchors — implicit in FST matching (always match full key)
            self.pos += 1;
            const s = self.addState(.{}); // epsilon
            return .{ .start = s, .out = s, .out_slot = 1 };
        }

        // Literal character
        self.pos += 1;
        const s = self.addState(.{ .match_byte = @intCast(c) });
        return .{ .start = s, .out = s, .out_slot = 1 };
    }

    fn parseCharClass(self: *Compiler) ParseError!Fragment {
        self.pos += 1; // skip '['
        var class = CharClass{};

        if (self.pos < self.pattern.len and self.pattern[self.pos] == '^') {
            class.negated = true;
            self.pos += 1;
        }

        var first = true;
        while (self.pos < self.pattern.len) {
            const c = self.pattern[self.pos];
            if (c == ']' and !first) break;
            first = false;

            if (c == '\\') {
                self.pos += 1;
                if (self.pos >= self.pattern.len) return error.InvalidRegex;
                class.bytes[self.pattern[self.pos]] = true;
                self.pos += 1;
                continue;
            }

            // Check for range: a-z
            if (self.pos + 2 < self.pattern.len and self.pattern[self.pos + 1] == '-' and self.pattern[self.pos + 2] != ']') {
                const lo = c;
                const hi = self.pattern[self.pos + 2];
                if (lo > hi) return error.InvalidRegex;
                for (lo..hi + 1) |b| {
                    class.bytes[b] = true;
                }
                self.pos += 3;
                continue;
            }

            class.bytes[c] = true;
            self.pos += 1;
        }

        if (self.pos >= self.pattern.len) return error.InvalidRegex;
        self.pos += 1; // skip ']'

        const class_idx = self.addClass(class);
        const s = self.addState(.{ .char_class_idx = @intCast(class_idx) });
        return .{ .start = s, .out = s, .out_slot = 1 };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "regex literal match" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "hello");
    defer regex.deinit();
    const aut = regex.automaton();

    // Walk "hello"
    var state = aut.start();
    try std.testing.expect(aut.canMatch(state));
    for ("hello") |c| {
        state = aut.accept(state, c);
        try std.testing.expect(aut.canMatch(state));
    }
    try std.testing.expect(aut.isMatch(state));

    // "hell" should not match (incomplete)
    state = aut.start();
    for ("hell") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(!aut.isMatch(state));
}

test "regex dot matches any" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "h.llo");
    defer regex.deinit();
    const aut = regex.automaton();

    // "hello" matches
    var state = aut.start();
    for ("hello") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "hallo" matches
    state = aut.start();
    for ("hallo") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "hllo" does not match (dot requires exactly one)
    state = aut.start();
    for ("hllo") |c| state = aut.accept(state, c);
    try std.testing.expect(!aut.isMatch(state));
}

test "regex star quantifier" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "hel*o");
    defer regex.deinit();
    const aut = regex.automaton();

    // "heo" matches (zero l's)
    var state = aut.start();
    for ("heo") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "hello" matches (two l's)
    state = aut.start();
    for ("hello") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "hellllo" matches (four l's)
    state = aut.start();
    for ("hellllo") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));
}

test "regex character class" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "[a-z]+");
    defer regex.deinit();
    const aut = regex.automaton();

    // "hello" matches
    var state = aut.start();
    for ("hello") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "HELLO" does not match (uppercase)
    state = aut.start();
    for ("HELLO") |c| state = aut.accept(state, c);
    try std.testing.expect(!aut.isMatch(state));

    // empty does not match (+ requires at least one)
    state = aut.start();
    try std.testing.expect(!aut.isMatch(state));
}

test "regex alternation" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "foo|bar");
    defer regex.deinit();
    const aut = regex.automaton();

    // "foo" matches
    var state = aut.start();
    for ("foo") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "bar" matches
    state = aut.start();
    for ("bar") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "baz" does not match
    state = aut.start();
    for ("baz") |c| state = aut.accept(state, c);
    try std.testing.expect(!aut.isMatch(state));
}

test "regex dead state prunes" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "abc");
    defer regex.deinit();
    const aut = regex.automaton();

    // "x" should hit dead state immediately
    var state = aut.start();
    state = aut.accept(state, 'x');
    try std.testing.expect(!aut.canMatch(state));
    try std.testing.expectEqual(dead_state, state);
}

test "regex question mark" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "colou?r");
    defer regex.deinit();
    const aut = regex.automaton();

    // "color" matches
    var state = aut.start();
    for ("color") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "colour" matches
    state = aut.start();
    for ("colour") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));
}

test "regex negated class" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "[^0-9]+");
    defer regex.deinit();
    const aut = regex.automaton();

    // "hello" matches (no digits)
    var state = aut.start();
    for ("hello") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "123" does not match
    state = aut.start();
    for ("123") |c| state = aut.accept(state, c);
    try std.testing.expect(!aut.isMatch(state));
}

test "regex escaped special chars" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "a\\.b");
    defer regex.deinit();
    const aut = regex.automaton();

    // "a.b" matches (literal dot)
    var state = aut.start();
    for ("a.b") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "axb" does not match (dot is literal)
    state = aut.start();
    for ("axb") |c| state = aut.accept(state, c);
    try std.testing.expect(!aut.isMatch(state));
}

test "regex grouping with quantifier" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "(ab)+");
    defer regex.deinit();
    const aut = regex.automaton();

    // "ab" matches
    var state = aut.start();
    for ("ab") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "abab" matches
    state = aut.start();
    for ("abab") |c| state = aut.accept(state, c);
    try std.testing.expect(aut.isMatch(state));

    // "" does not match (+ requires at least one)
    state = aut.start();
    try std.testing.expect(!aut.isMatch(state));

    // "a" does not match (incomplete group)
    state = aut.start();
    state = aut.accept(state, 'a');
    try std.testing.expect(!aut.isMatch(state));
}

test "regex compile extracts prefix literal metadata" {
    const alloc = std.testing.allocator;

    var simple = try compile(alloc, "cat.*dog");
    defer simple.deinit();
    try std.testing.expectEqual(@as(usize, 1), simple.prefix_literals.len);
    try std.testing.expectEqualStrings("cat", simple.prefix_literals[0]);
    try std.testing.expect(!simple.anchored_start);
    try std.testing.expect(!simple.anchored_end);

    var anchored = try compile(alloc, "^foo");
    defer anchored.deinit();
    try std.testing.expectEqualStrings("foo", anchored.prefix_literals[0]);
    try std.testing.expect(anchored.anchored_start);

    var suffix = try compile(alloc, "bar$");
    defer suffix.deinit();
    try std.testing.expectEqualStrings("bar", suffix.prefix_literals[0]);
    try std.testing.expect(suffix.anchored_end);

    var plus = try compile(alloc, "a+bc");
    defer plus.deinit();
    try std.testing.expectEqualStrings("a", plus.prefix_literals[0]);

    var optional = try compile(alloc, "a*bc");
    defer optional.deinit();
    try std.testing.expectEqual(@as(usize, 0), optional.prefix_literals.len);
}

test "regex compile extracts small literal prefix sets for alternation" {
    const alloc = std.testing.allocator;

    var alt = try compile(alloc, "foo|bar");
    defer alt.deinit();
    try std.testing.expectEqual(@as(usize, 2), alt.prefix_literals.len);
    try std.testing.expectEqualStrings("foo", alt.prefix_literals[0]);
    try std.testing.expectEqualStrings("bar", alt.prefix_literals[1]);

    var grouped = try compile(alloc, "(foo|bar)baz");
    defer grouped.deinit();
    try std.testing.expectEqual(@as(usize, 2), grouped.prefix_literals.len);
    try std.testing.expectEqualStrings("foobaz", grouped.prefix_literals[0]);
    try std.testing.expectEqualStrings("barbaz", grouped.prefix_literals[1]);
    try std.testing.expectEqual(@as(usize, 2), grouped.prefix_first_bytes.len);
    try std.testing.expectEqual(@as(u8, 'f'), grouped.prefix_first_bytes[0]);
    try std.testing.expectEqual(@as(u8, 'b'), grouped.prefix_first_bytes[1]);
    try std.testing.expectEqual(@as(usize, grouped.prefix_literals.len), grouped.prefix_check_offsets.len);
}

test "regex DFA cache reuses repeated state sets" {
    const alloc = std.testing.allocator;
    var regex = try compile(alloc, "ab|ac");
    defer regex.deinit();
    const aut = regex.automaton();

    const start = aut.start();
    const after_a = aut.accept(start, 'a');
    const after_ab_1 = aut.accept(after_a, 'b');

    const start_again = aut.start();
    const after_a_again = aut.accept(start_again, 'a');
    const after_ab_2 = aut.accept(after_a_again, 'b');

    try std.testing.expectEqual(after_a, after_a_again);
    try std.testing.expectEqual(after_ab_1, after_ab_2);
}

test "regex compile builds byte equivalence classes" {
    const alloc = std.testing.allocator;

    var literal = try compile(alloc, "ab|ac");
    defer literal.deinit();
    try std.testing.expectEqual(@as(u16, 4), literal.num_byte_classes);
    try std.testing.expectEqual(literal.byte_classes['a'], literal.byte_classes['a']);
    try std.testing.expectEqual(literal.byte_classes['b'], literal.byte_classes['b']);
    try std.testing.expectEqual(literal.byte_classes['c'], literal.byte_classes['c']);
    try std.testing.expect(literal.byte_classes['b'] != literal.byte_classes['c']);
    try std.testing.expect(literal.byte_classes['x'] != literal.byte_classes['a']);

    var class_regex = try compile(alloc, "[a-z]+");
    defer class_regex.deinit();
    try std.testing.expectEqual(@as(u16, 2), class_regex.num_byte_classes);
    try std.testing.expectEqual(class_regex.byte_classes['a'], class_regex.byte_classes['z']);
    try std.testing.expectEqual(class_regex.byte_classes['A'], class_regex.byte_classes['0']);

    var dot = try compile(alloc, ".*");
    defer dot.deinit();
    try std.testing.expectEqual(@as(u16, 1), dot.num_byte_classes);
}

test "regex compile precomputes epsilon closures" {
    const alloc = std.testing.allocator;

    var regex = try compile(alloc, "colou?r");
    defer regex.deinit();

    var found_expanded = false;
    for (regex.epsilon_closures, 0..) |closure, idx| {
        try std.testing.expect(closure.isSet(idx));
        if (closure.count() > 1) found_expanded = true;
    }

    try std.testing.expect(found_expanded);
}
