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

//! Bounded lazy subset DFA for existence queries. Assertions and capture
//! extraction stay with the ordered VM; cache exhaustion is a fallback signal.
const std = @import("std");
const search = @import("search.zig");
const unicode = @import("unicode.zig");
const A = std.mem.Allocator;
const Budget = @import("bounded.zig").Budget;
const unknown = std.math.maxInt(u32);
const accepting_bit: u32 = 1 << 31;
pub const Dfa = struct {
    a: A,
    memory: *Budget,
    pattern: *const search.Pattern,
    budget: usize,
    used: usize = 0,
    words: usize,
    working: []u64,
    stack: []u32,
    states: std.ArrayList(Cached) = .empty,
    index: std.AutoHashMap(u64, u32),
    wide: std.AutoHashMap(u64, u32),
    const Cached = struct { bits: []u64, ascii: [128]u32 = @splat(unknown), accepting: bool, collision: u32 = unknown };
    pub const Error = A.Error || error{ UnsupportedDfa, DfaCacheFull };
    pub fn supported(p: *const search.Pattern) bool {
        for (p.states) |state| switch (state.tag) {
            .consume, .split, .save, .accept => {},
            else => return false,
        };
        return true;
    }
    pub fn init(backing: A, p: *const search.Pattern, budget: usize) Error!Dfa {
        if (!supported(p)) return error.UnsupportedDfa;
        if (budget <= @sizeOf(Budget)) return error.DfaCacheFull;
        const memory = try backing.create(Budget);
        memory.* = .{ .backing = backing, .limit = budget - @sizeOf(Budget) };
        return initBounded(memory, p, budget) catch |err| {
            const denied = memory.denied;
            std.debug.assert(memory.live == 0);
            backing.destroy(memory);
            return if (denied) error.DfaCacheFull else err;
        };
    }
    fn initBounded(memory: *Budget, p: *const search.Pattern, budget: usize) Error!Dfa {
        const a = memory.allocator();
        const words = (p.states.len + 63) / 64;
        const working = try a.alloc(u64, words);
        const stack = a.alloc(u32, p.states.len * 2 + 1) catch |err| {
            a.free(working);
            return err;
        };
        var self = Dfa{ .a = a, .memory = memory, .pattern = p, .budget = budget, .used = memory.live + @sizeOf(Budget), .words = words, .working = working, .stack = stack, .index = .init(a), .wide = .init(a) };
        errdefer self.deinitStorage();
        @memset(working, 0);
        self.closure(p.start);
        _ = try self.intern();
        return self;
    }
    pub fn deinit(self: *Dfa) void {
        const memory = self.memory;
        self.deinitStorage();
        std.debug.assert(memory.live == 0);
        memory.backing.destroy(memory);
    }
    fn deinitStorage(self: *Dfa) void {
        for (self.states.items) |state| self.a.free(state.bits);
        self.states.deinit(self.a);
        self.index.deinit();
        self.wide.deinit();
        self.a.free(self.working);
        self.a.free(self.stack);
    }
    fn closure(self: *Dfa, start: u32) void {
        self.stack[0] = start;
        var len: usize = 1;
        while (len > 0) {
            len -= 1;
            const pc = self.stack[len];
            const mask = @as(u64, 1) << @intCast(pc % 64);
            if (self.working[pc / 64] & mask != 0) continue;
            self.working[pc / 64] |= mask;
            const state = self.pattern.states[pc];
            switch (state.tag) {
                .split => {
                    self.stack[len] = state.second;
                    len += 1;
                    self.stack[len] = state.first;
                    len += 1;
                },
                .save => {
                    self.stack[len] = state.first;
                    len += 1;
                },
                else => {},
            }
        }
    }
    fn intern(self: *Dfa) Error!u32 {
        const hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(self.working));
        const previous = self.index.get(hash) orelse unknown;
        var item = previous;
        while (item != unknown) {
            if (std.mem.eql(u64, self.states.items[item].bits, self.working)) return item;
            item = self.states.items[item].collision;
        }
        // The bounded allocator accounts for real array/hash capacities and
        // temporary buffers during growth, rather than estimated entry costs.
        var accepting = false;
        for (self.pattern.states, 0..) |state, pc| if (state.tag == .accept and self.working[pc / 64] & (@as(u64, 1) << @intCast(pc % 64)) != 0) {
            accepting = true;
            break;
        };
        const bits = try self.a.dupe(u64, self.working);
        errdefer self.a.free(bits);
        const id: u32 = @intCast(self.states.items.len);
        try self.states.append(self.a, .{ .bits = bits, .accepting = accepting, .collision = previous });
        errdefer _ = self.states.pop();
        try self.index.put(hash, id);
        self.used = self.memory.live + @sizeOf(Budget);
        return id;
    }
    fn transition(self: *Dfa, from: u32, cp: u32) Error!u32 {
        const key = (@as(u64, from) << 21) | cp;
        if (cp < 128) {
            const cached = self.states.items[from].ascii[cp];
            if (cached != unknown) return cached;
        } else if (self.wide.get(key)) |cached| return cached;
        @memset(self.working, 0);
        // The stored set contains the epsilon closure. Only consume states
        // contribute successors; save states require no capture copying here.
        const bits = self.states.items[from].bits;
        for (bits, 0..) |word, index| {
            var live = word;
            while (live != 0) {
                const pc = index * 64 + @ctz(live);
                live &= live - 1;
                if (pc >= self.pattern.states.len) continue;
                const state = self.pattern.states[pc];
                if (state.tag == .consume and search.stateMatches(state, cp)) self.closure(state.first);
            }
        }
        self.closure(self.pattern.start); // unanchored starts at each boundary
        const next = try self.intern();
        const encoded = next | (if (self.states.items[next].accepting) accepting_bit else @as(u32, 0));
        if (cp < 128) self.states.items[from].ascii[cp] = encoded else {
            try self.wide.put(key, encoded);
            self.used = self.memory.live + @sizeOf(Budget);
        }
        return encoded;
    }
    pub fn matches(self: *Dfa, text: []const u8) Error!bool {
        return self.matchesInner(text) catch |err| {
            self.used = self.memory.live + @sizeOf(Budget);
            return if (self.memory.denied) error.DfaCacheFull else err;
        };
    }
    fn matchesInner(self: *Dfa, text: []const u8) Error!bool {
        var state: u32 = 0;
        if (self.states.items[state].accepting) return true;
        var at: usize = 0;
        while (at < text.len) {
            const byte = text[at];
            const next = if (byte < 128) blk: {
                const cached = self.states.items[state].ascii[byte];
                at += 1;
                break :blk if (cached == unknown) try self.transition(state, byte) else cached;
            } else blk: {
                const decoded = unicode.decode(text, at, self.pattern.flags.unicode);
                at += decoded.len;
                break :blk try self.transition(state, decoded.cp);
            };
            if (next & accepting_bit != 0) return true;
            state = next;
        }
        return false;
    }
};
test "lazy DFA agrees with the capture VM and obeys a hard cache budget" {
    const a = std.testing.allocator;
    const patterns = [_][]const u8{ "(ab|a)+z", "a*", "a+?b", "[a-z]{2,4}", "\\p{Greek}+", "(?i:é|k)+", "x.y" };
    const subjects = [_][]const u8{ "", "abaz", "aaa!", "hello", "ÉK", "Σίσυφος", "x\ny", "xxay", "\xff" };
    for (patterns) |text| {
        var pattern = try search.Pattern.init(a, text, .{});
        defer pattern.deinit();
        var runner = try search.Runner.init(a, &pattern);
        defer runner.deinit();
        var dfa = try Dfa.init(a, &pattern, 128 * 1024);
        defer dfa.deinit();
        for (subjects) |subject| try std.testing.expectEqual(runner.find(subject, 0) != null, try dfa.matches(subject));
        try std.testing.expect(dfa.used <= dfa.budget);
        try std.testing.expect(dfa.memory.peak + @sizeOf(Budget) <= dfa.budget);
        try std.testing.expectError(error.DfaCacheFull, Dfa.init(a, &pattern, 1));
    }
}

test "DFA quota includes actual array and hash capacities" {
    const a = std.testing.allocator;
    var pattern = try search.Pattern.init(a, "a|bc", .{});
    defer pattern.deinit();
    for ([_]usize{ 512, 1024, 2048, 4096, 8192, 16384 }) |budget| {
        var dfa = Dfa.init(a, &pattern, budget) catch |err| {
            try std.testing.expectEqual(error.DfaCacheFull, err);
            continue;
        };
        defer dfa.deinit();
        _ = dfa.matches("bc") catch |err| blk: {
            try std.testing.expectEqual(error.DfaCacheFull, err);
            break :blk false;
        };
        try std.testing.expect(dfa.memory.peak + @sizeOf(Budget) <= budget);
    }
}
