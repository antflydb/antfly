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

//! Levenshtein automaton for fuzzy matching on FST traversal.
//!
//! Implements the vellum.Automaton trait so it can be used with FST.search()
//! to find all terms within a given edit distance of a target term.
//!
//! Uses on-the-fly DFA construction: each DFA state is a set of NFA states
//! (position, edits) pairs. This correctly handles insertion, deletion,
//! and substitution — all three edit operations.
//!
//! NFA state space: (term.len + 1) * (max_distance + 1) possible states.
//! State sets are cached for deduplication (powerset construction).

const std = @import("std");
const vellum = @import("antfly_vellum");

const dead_state = std.math.maxInt(usize);

/// Maximum supported term length * (max_distance+1). We use a bitset to
/// represent sets of NFA states. 128 * 4 = 512 supports terms up to 127
/// chars with distance 3, which covers all practical cases.
const max_nfa_states = 512;
const StateBitSet = std.StaticBitSet(max_nfa_states);

/// Levenshtein automaton matching strings within `max_distance` edits of `term`.
pub const LevenshteinAutomaton = struct {
    term: []const u8,
    max_distance: u8,
    /// DFA state cache
    dfa_states: std.ArrayListUnmanaged(StateBitSet) = .empty,
    dfa_match: std.ArrayListUnmanaged(bool) = .empty,
    /// We need a persistent allocator for the DFA cache
    alloc: ?std.mem.Allocator = null,

    /// Get a vellum.Automaton interface for FST traversal.
    pub fn automaton(self: *LevenshteinAutomaton) vellum.Automaton {
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

    pub fn deinit(self: *LevenshteinAutomaton) void {
        if (self.alloc) |a| {
            self.dfa_states.deinit(a);
            self.dfa_match.deinit(a);
        }
    }

    /// Encode NFA state (pos, edits) → index.
    fn nfaIndex(self: *const LevenshteinAutomaton, pos: usize, edits: usize) usize {
        return pos * (@as(usize, self.max_distance) + 1) + edits;
    }

    /// Compute epsilon closure: for deletion edits, we can advance the
    /// target position without consuming input. This is done transitively.
    fn epsilonClosure(self: *const LevenshteinAutomaton, initial: StateBitSet) StateBitSet {
        var result = initial;
        const d: usize = @as(usize, self.max_distance) + 1;
        var changed = true;
        while (changed) {
            changed = false;
            var it = result.iterator(.{});
            var to_add: [max_nfa_states]usize = undefined;
            var add_count: usize = 0;
            while (it.next()) |idx| {
                const pos = idx / d;
                const edits = idx % d;
                // Deletion: skip a character in the target (advance pos, +1 edit)
                if (pos < self.term.len and edits + 1 <= self.max_distance) {
                    const next = self.nfaIndex(pos + 1, edits + 1);
                    if (!result.isSet(next)) {
                        to_add[add_count] = next;
                        add_count += 1;
                    }
                }
            }
            for (to_add[0..add_count]) |idx| {
                result.set(idx);
                changed = true;
            }
        }
        return result;
    }

    fn lookupOrInsert(self: *LevenshteinAutomaton, set: StateBitSet) usize {
        for (self.dfa_states.items, 0..) |existing, i| {
            if (existing.eql(set)) return i;
        }
        const idx = self.dfa_states.items.len;
        const alloc = self.alloc orelse return dead_state;
        self.dfa_states.append(alloc, set) catch return dead_state;
        // Check if any NFA state in this set is a match
        const d: usize = @as(usize, self.max_distance) + 1;
        var is_match = false;
        var it = set.iterator(.{});
        while (it.next()) |s| {
            const pos = s / d;
            const edits = s % d;
            const remaining = self.term.len -| pos;
            if (edits + remaining <= self.max_distance) {
                is_match = true;
                break;
            }
        }
        self.dfa_match.append(alloc, is_match) catch return dead_state;
        return idx;
    }

    fn startFn(self: *LevenshteinAutomaton) usize {
        if (self.dfa_states.items.len > 0) return 0;
        var initial = StateBitSet.empty;
        initial.set(0); // (pos=0, edits=0)
        const start_set = self.epsilonClosure(initial);
        return self.lookupOrInsert(start_set);
    }

    fn isMatchFn(self: *LevenshteinAutomaton, state: usize) bool {
        if (state == dead_state) return false;
        if (state >= self.dfa_match.items.len) return false;
        return self.dfa_match.items[state];
    }

    fn canMatchFn(_: *LevenshteinAutomaton, state: usize) bool {
        return state != dead_state;
    }

    fn willAlwaysMatchFn(_: *LevenshteinAutomaton, _: usize) bool {
        return false;
    }

    fn acceptFn(self: *LevenshteinAutomaton, state: usize, b: u8) usize {
        if (state == dead_state) return dead_state;
        if (state >= self.dfa_states.items.len) return dead_state;

        const current = self.dfa_states.items[state];
        const d: usize = @as(usize, self.max_distance) + 1;
        var next = StateBitSet.empty;

        var it = current.iterator(.{});
        while (it.next()) |idx| {
            const pos = idx / d;
            const edits = idx % d;

            if (pos < self.term.len) {
                if (b == self.term[pos]) {
                    // Match: advance both, no edit cost
                    next.set(self.nfaIndex(pos + 1, edits));
                } else if (edits + 1 <= self.max_distance) {
                    // Substitution: advance both, +1 edit
                    next.set(self.nfaIndex(pos + 1, edits + 1));
                    // Insertion: consume input byte, don't advance target, +1 edit
                    next.set(self.nfaIndex(pos, edits + 1));
                }
            } else {
                // Past end of target: insertion (extra input bytes)
                if (edits + 1 <= self.max_distance) {
                    next.set(self.nfaIndex(pos, edits + 1));
                }
            }
        }

        // Apply epsilon closure (handles deletion transitions)
        const closed = self.epsilonClosure(next);
        if (closed.count() == 0) return dead_state;

        return self.lookupOrInsert(closed);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "levenshtein automaton exact match" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "hello" through the automaton
    var state = aut.start();
    for ("hello") |c| {
        state = aut.accept(state, c);
        try std.testing.expect(aut.canMatch(state));
    }
    try std.testing.expect(aut.isMatch(state));
}

test "levenshtein automaton substitution" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "hallo" (substitution at position 1)
    var state = aut.start();
    for ("hallo") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(aut.isMatch(state));
}

test "levenshtein automaton too many edits" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "haxlo" (2 substitutions — exceeds distance 1)
    var state = aut.start();
    for ("haxlo") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(!aut.isMatch(state));
}

test "levenshtein automaton distance 2" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 2, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "haxlo" (2 substitutions — within distance 2)
    var state = aut.start();
    for ("haxlo") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(aut.isMatch(state));
}

test "levenshtein automaton shorter input" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "hell" (deletion of last char — within distance 1)
    var state = aut.start();
    for ("hell") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(aut.isMatch(state));
}

test "levenshtein automaton longer input" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "helloo" (insertion at end — within distance 1)
    var state = aut.start();
    for ("helloo") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(aut.isMatch(state));
}

test "levenshtein automaton insertion in middle" {
    var lev = LevenshteinAutomaton{ .term = "helo", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "hello" — target "helo" + insertion of 'l' → distance 1
    var state = aut.start();
    for ("hello") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(aut.isMatch(state));
}

test "levenshtein automaton deletion in middle" {
    var lev = LevenshteinAutomaton{ .term = "hello", .max_distance = 1, .alloc = std.testing.allocator };
    defer lev.deinit();
    const aut = lev.automaton();

    // Walk "helo" — target "hello", input missing one 'l' → distance 1
    var state = aut.start();
    for ("helo") |c| {
        state = aut.accept(state, c);
    }
    try std.testing.expect(aut.isMatch(state));
}
