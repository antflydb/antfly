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

const std = @import("std");
const A = std.mem.Allocator;
const Group = @import("enrichment_worker.zig").PendingDocumentGroup;

/// A bounded durable workset, separate from the contiguous applied watermark.
/// Tickets retain identities, clocks and budgets, never documents or credentials.
pub const max_documents = 128;
pub const max_bytes = 16 * 1024 * 1024;
pub const Budget = struct {
    fingerprint: u64,
    logical_fingerprint: u64 = 0,
    failures: u32 = 0,
    waits: u32 = 0,
    deadline_ms: u64 = 0,
    revision: u64 = 0,
};
pub const Ticket = struct {
    document: []const u8, // base64 of the opaque document key
    sequence: u64,
    source_digest: ?[32]u8 = null,
    deadline_ms: u64,
    revision: u64,
    budgets: std.ArrayListUnmanaged(Budget) = .empty,
    fn refreshDeadline(self: *Ticket) void {
        self.deadline_ms = std.math.maxInt(u64);
        for (self.budgets.items) |budget| self.deadline_ms = @min(self.deadline_ms, budget.deadline_ms);
    }
    pub fn deinit(self: *Ticket, a: A) void {
        a.free(self.document);
        self.budgets.deinit(a);
    }
};
const WireTicket = struct { document: []const u8, sequence: u64, source_digest: ?[32]u8 = null, deadline_ms: u64, revision: u64, budgets: []const Budget };
const Wire = struct { version: u32 = 1, config_hash: u64 = 0, scanned_sequence: u64 = 0, scan_document: []const u8 = "", tickets: []const WireTicket = &.{} };
pub const Token = struct { document: []const u8, sequence: u64, fingerprint: u64, logical_fingerprint: u64, revision: u64 };
pub const Due = struct {
    groups: []Group,
    tokens: []Token,
    documents: [][]const u8,
    pub fn deinit(self: Due, a: A) void {
        for (self.groups) |group| a.free(group.doc_key);
        a.free(self.groups);
        a.free(self.tokens);
        for (self.documents) |document| a.free(document);
        a.free(self.documents);
    }
};
pub const PreparedDocument = struct { delayed: []u64, changed: bool };
pub const State = struct {
    config_hash: u64 = 0,
    scanned_sequence: u64 = 0,
    scan_document: []const u8 = "",
    tickets: std.ArrayListUnmanaged(Ticket) = .empty,

    pub fn deinit(self: *State, a: A) void {
        for (self.tickets.items) |*ticket| ticket.deinit(a);
        self.tickets.deinit(a);
        if (self.scan_document.len != 0) a.free(self.scan_document);
        self.* = .{};
    }
    pub fn decode(a: A, bytes: []const u8) !State {
        if (bytes.len > max_bytes) return error.InvalidEnrichmentState;
        var parsed = try std.json.parseFromSlice(Wire, a, bytes, .{});
        defer parsed.deinit();
        if (parsed.value.version != 1 or parsed.value.tickets.len > max_documents) return error.InvalidEnrichmentState;
        var result = State{ .config_hash = parsed.value.config_hash, .scanned_sequence = parsed.value.scanned_sequence };
        if (parsed.value.scan_document.len != 0) result.scan_document = try a.dupe(u8, parsed.value.scan_document);
        errdefer result.deinit(a);
        for (parsed.value.tickets) |wire| {
            if (wire.document.len == 0 or wire.document.len > 90_000 or (wire.budgets.len == 0 or wire.budgets.len > 1024) or wire.revision == 0) return error.InvalidEnrichmentState;
            _ = std.base64.standard.Decoder.calcSizeForSlice(wire.document) catch return error.InvalidEnrichmentState;
            var ticket = Ticket{ .document = try a.dupe(u8, wire.document), .sequence = wire.sequence, .source_digest = wire.source_digest, .deadline_ms = wire.deadline_ms, .revision = wire.revision };
            errdefer ticket.deinit(a);
            try ticket.budgets.appendSlice(a, wire.budgets);
            for (ticket.budgets.items) |budget| {
                if (budget.fingerprint == 0 or budget.revision == 0 or budget.revision > ticket.revision) return error.InvalidEnrichmentState;
            }
            ticket.refreshDeadline();
            if (ticket.deadline_ms != wire.deadline_ms) return error.InvalidEnrichmentState;
            try result.tickets.append(a, ticket);
        }
        return result;
    }
    pub fn encode(self: State, a: A) ![]u8 {
        const wire = try a.alloc(WireTicket, self.tickets.items.len);
        defer a.free(wire);
        for (self.tickets.items, wire) |ticket, *dest| dest.* = .{ .document = ticket.document, .sequence = ticket.sequence, .source_digest = ticket.source_digest, .deadline_ms = ticket.deadline_ms, .revision = ticket.revision, .budgets = ticket.budgets.items };
        const bytes = try std.json.Stringify.valueAlloc(a, Wire{ .config_hash = self.config_hash, .scanned_sequence = self.scanned_sequence, .scan_document = self.scan_document, .tickets = wire }, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_bytes) return error.MemoryBudgetExceeded;
        return bytes;
    }
    pub fn clearScanDocument(self: *State, a: A) void {
        if (self.scan_document.len != 0) a.free(self.scan_document);
        self.scan_document = "";
    }
    pub fn scanDocumentAlloc(self: State, a: A) ![]u8 {
        const size = try std.base64.standard.Decoder.calcSizeForSlice(self.scan_document);
        const bytes = try a.alloc(u8, size);
        errdefer a.free(bytes);
        try std.base64.standard.Decoder.decode(bytes, self.scan_document);
        return bytes;
    }
    pub fn setScanDocument(self: *State, a: A, group: Group) !void {
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(group.doc_key.len));
        _ = std.base64.standard.Encoder.encode(encoded, group.doc_key);
        self.clearScanDocument(a);
        self.scan_document = encoded;
        self.scanned_sequence = group.sequence;
    }
    pub fn completedSequence(self: State) u64 {
        return self.scanned_sequence -| @as(u64, if (self.scan_document.len != 0) 1 else 0);
    }
    pub fn prior(self: State, fingerprint: u64) Budget {
        var result = Budget{ .fingerprint = fingerprint };
        for (self.tickets.items) |ticket| for (ticket.budgets.items) |budget| {
            if (budget.fingerprint != fingerprint) continue;
            result.failures = @max(result.failures, budget.failures);
            result.waits = @max(result.waits, budget.waits);
        };
        return result;
    }
    /// All members of a failed provider invocation receive the same episode.
    /// Each producer retains its own clock even when one document owns several
    /// tickets. Compatible batch rebucketing cannot reset logical budgets.
    pub fn park(self: *State, a: A, group: Group, deadline_ms: u64, budget: Budget, source_digest: ?[32]u8) !void {
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(group.doc_key.len));
        defer a.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, group.doc_key);
        var selected: ?*Ticket = null;
        for (self.tickets.items) |*ticket| {
            if (ticket.sequence == group.sequence and std.mem.eql(u8, ticket.document, encoded)) {
                selected = ticket;
                break;
            }
        }
        if (selected == null) {
            if (self.tickets.items.len == max_documents) return error.MemoryBudgetExceeded;
            var ticket = Ticket{ .document = try a.dupe(u8, encoded), .sequence = group.sequence, .source_digest = source_digest, .deadline_ms = deadline_ms, .revision = 1 };
            errdefer ticket.deinit(a);
            var next = budget;
            next.deadline_ms = deadline_ms;
            next.revision = 1;
            try ticket.budgets.append(a, next);
            try self.tickets.append(a, ticket);
            return;
        }
        const ticket = selected.?;
        const changed = !std.meta.eql(ticket.source_digest, source_digest);
        var old: ?usize = null;
        if (!changed) for (ticket.budgets.items, 0..) |existing, i| {
            if (if (budget.logical_fingerprint != 0) existing.logical_fingerprint == budget.logical_fingerprint else existing.fingerprint == budget.fingerprint) {
                old = i;
                break;
            }
        };
        const revision = try std.math.add(u64, ticket.revision, 1);
        if (changed) {
            try ticket.budgets.ensureTotalCapacity(a, 1);
        } else if (old == null) {
            if (ticket.budgets.items.len == 1024) return error.MemoryBudgetExceeded;
            try ticket.budgets.ensureUnusedCapacity(a, 1);
        }
        // Every allocation precedes mutation, including source replacement.
        if (changed) ticket.budgets.clearRetainingCapacity();
        ticket.source_digest = source_digest;
        ticket.revision = revision;
        var next = budget;
        next.deadline_ms = deadline_ms;
        next.revision = revision;
        if (old) |i| {
            next.deadline_ms = @max(next.deadline_ms, ticket.budgets.items[i].deadline_ms);
            next.failures = @max(next.failures, ticket.budgets.items[i].failures);
            next.waits = @max(next.waits, ticket.budgets.items[i].waits);
            ticket.budgets.items[i] = next;
        } else ticket.budgets.appendAssumeCapacity(next);
        ticket.refreshDeadline();
    }
    pub fn priorLogical(self: State, logical: u64, source_digest: ?[32]u8) Budget {
        var result = Budget{ .fingerprint = 0, .logical_fingerprint = logical };
        for (self.tickets.items) |ticket| for (ticket.budgets.items) |budget| {
            if (!std.meta.eql(ticket.source_digest, source_digest) or budget.logical_fingerprint != logical) continue;
            result.failures = @max(result.failures, budget.failures);
            result.waits = @max(result.waits, budget.waits);
        };
        return result;
    }
    pub fn deadline(self: State) u64 {
        var result: u64 = std.math.maxInt(u64);
        for (self.tickets.items) |ticket| result = @min(result, ticket.deadline_ms);
        return if (self.tickets.items.len == 0) 0 else result;
    }
    pub fn appliedThrough(self: State, scanned: u64) u64 {
        var result = scanned;
        for (self.tickets.items) |ticket| result = @min(result, ticket.sequence -| 1);
        return result;
    }
    pub fn due(self: State, a: A, now_ms: u64) !Due {
        return self.selectDue(a, now_ms, false);
    }
    pub fn replan(self: State, a: A, now_ms: u64) !Due {
        return self.selectDue(a, now_ms, true);
    }
    fn selectDue(self: State, a: A, now_ms: u64, include_waiting: bool) !Due {
        var document_count: usize = 0;
        var budget_count: usize = 0;
        for (self.tickets.items) |ticket| {
            if (!include_waiting and ticket.deadline_ms > now_ms) continue;
            document_count += 1;
            for (ticket.budgets.items) |budget| if (budget.deadline_ms <= now_ms) {
                budget_count += 1;
            };
        }
        const groups = try a.alloc(Group, document_count);
        errdefer a.free(groups);
        const tokens = try a.alloc(Token, budget_count);
        errdefer a.free(tokens);
        const documents = try a.alloc([]const u8, document_count);
        errdefer a.free(documents);
        var initialized: usize = 0;
        errdefer for (0..initialized) |i| {
            a.free(groups[i].doc_key);
            a.free(documents[i]);
        };
        var token_count: usize = 0;
        for (self.tickets.items) |ticket| {
            if (!include_waiting and ticket.deadline_ms > now_ms) continue;
            const size = try std.base64.standard.Decoder.calcSizeForSlice(ticket.document);
            const key = try a.alloc(u8, size);
            errdefer a.free(key);
            try std.base64.standard.Decoder.decode(key, ticket.document);
            documents[initialized] = try a.dupe(u8, ticket.document);
            groups[initialized] = .{ .doc_key = key, .sequence = ticket.sequence };
            for (ticket.budgets.items) |budget| {
                if (budget.deadline_ms > now_ms) continue;
                tokens[token_count] = .{ .document = documents[initialized], .sequence = ticket.sequence, .fingerprint = budget.fingerprint, .logical_fingerprint = budget.logical_fingerprint, .revision = budget.revision };
                token_count += 1;
            }
            initialized += 1;
        }
        const Order = struct {
            fn less(_: void, x: Group, y: Group) bool {
                return if (x.sequence == y.sequence) std.mem.lessThan(u8, x.doc_key, y.doc_key) else x.sequence < y.sequence;
            }
        };
        std.mem.sort(Group, groups, {}, Order.less);
        return .{ .groups = groups, .tokens = tokens, .documents = documents };
    }
    pub fn hasDocument(self: State, encoded: []const u8) bool {
        for (self.tickets.items) |ticket| if (std.mem.eql(u8, ticket.document, encoded)) return true;
        return false;
    }
    /// Called only for a currently planned primary document. Obsolete producer
    /// definitions can retire without forgiving another producer's retry clock.
    pub fn prepareDocument(self: *State, a: A, encoded: []const u8, source_digest: ?[32]u8, definitions: []const u64, now_ms: u64) !PreparedDocument {
        var changed = false;
        var delayed: std.ArrayListUnmanaged(u64) = .empty;
        defer delayed.deinit(a);
        var i: usize = 0;
        while (i < self.tickets.items.len) {
            const ticket = &self.tickets.items[i];
            if (!std.mem.eql(u8, ticket.document, encoded)) {
                i += 1;
                continue;
            }
            var j: usize = 0;
            while (j < ticket.budgets.items.len) {
                const budget = ticket.budgets.items[j];
                if (budget.logical_fingerprint != 0 and std.mem.indexOfScalar(u64, definitions, budget.logical_fingerprint) == null) {
                    _ = ticket.budgets.swapRemove(j);
                    changed = true;
                    continue;
                }
                if (budget.deadline_ms > now_ms and std.meta.eql(source_digest, ticket.source_digest) and
                    std.mem.indexOfScalar(u64, delayed.items, budget.logical_fingerprint) == null)
                    try delayed.append(a, budget.logical_fingerprint);
                j += 1;
            }
            if (ticket.budgets.items.len == 0) {
                ticket.deinit(a);
                _ = self.tickets.swapRemove(i);
            } else {
                ticket.refreshDeadline();
                i += 1;
            }
        }
        return .{ .delayed = try delayed.toOwnedSlice(a), .changed = changed };
    }
    pub fn retireSuperseded(self: *State, a: A, group: Group, encoded: []const u8, source_digest: ?[32]u8) void {
        var i: usize = 0;
        while (i < self.tickets.items.len) {
            const ticket = &self.tickets.items[i];
            if (ticket.sequence < group.sequence and std.mem.eql(u8, ticket.document, encoded) and !std.meta.eql(ticket.source_digest, source_digest)) {
                ticket.deinit(a);
                _ = self.tickets.swapRemove(i);
            } else i += 1;
        }
    }
    pub fn retire(self: *State, a: A, tokens: []const Token) void {
        for (tokens) |token| {
            for (self.tickets.items, 0..) |*ticket, i| {
                if (ticket.sequence != token.sequence or !std.mem.eql(u8, ticket.document, token.document)) continue;
                for (ticket.budgets.items, 0..) |budget, j| {
                    if (budget.fingerprint == token.fingerprint and budget.logical_fingerprint == token.logical_fingerprint and budget.revision == token.revision) {
                        _ = ticket.budgets.swapRemove(j);
                        break;
                    }
                }
                if (ticket.budgets.items.len == 0) {
                    ticket.deinit(a);
                    _ = self.tickets.swapRemove(i);
                } else ticket.refreshDeadline();
                break;
            }
        }
    }
};

test "scoped retry workset preserves full clocks budgets binary keys and publication debt" {
    const a = std.testing.allocator;
    var state = State{};
    defer state.deinit(a);
    try state.park(a, .{ .doc_key = "bad\xff", .sequence = 2 }, 3_600_000, .{ .fingerprint = 17, .failures = 2 }, null);
    state.scanned_sequence = 100;
    try std.testing.expectEqual(@as(u64, 1), state.appliedThrough(100));
    const bytes = try state.encode(a);
    defer a.free(bytes);
    var restored = try State.decode(a, bytes);
    defer restored.deinit(a);
    try std.testing.expectEqual(@as(u32, 2), restored.prior(17).failures);
    const early = try restored.due(a, 3_599_999);
    defer early.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), early.groups.len);
    const ready = try restored.due(a, 3_600_000);
    defer ready.deinit(a);
    try std.testing.expectEqualStrings("bad\xff", ready.groups[0].doc_key);
    // A failed reattempt fences an older pass's completion token.
    try restored.park(a, ready.groups[0], 4_000_000, .{ .fingerprint = 17, .failures = 3 }, null);
    restored.retire(a, ready.tokens);
    try std.testing.expectEqual(@as(usize, 1), restored.tickets.items.len);
    const last = try restored.due(a, 4_000_000);
    defer last.deinit(a);
    restored.retire(a, last.tokens);
    try std.testing.expectEqual(@as(u64, 100), restored.appliedThrough(100));
}

test "scoped retry workset keeps independent producer clocks and logical budgets" {
    const a = std.testing.allocator;
    var state = State{};
    defer state.deinit(a);
    const source: [32]u8 = @splat(1);
    const group = Group{ .doc_key = "document", .sequence = 2 };
    try state.park(a, group, 1_000, .{ .fingerprint = 11, .logical_fingerprint = 1, .failures = 1 }, source);
    try state.park(a, group, 3_600_000, .{ .fingerprint = 22, .logical_fingerprint = 2, .failures = 1 }, source);
    const ready = try state.due(a, 1_000);
    defer ready.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), ready.tokens.len);
    const delayed = try state.prepareDocument(a, ready.documents[0], source, &.{ 1, 2 }, 1_000);
    defer a.free(delayed.delayed);
    try std.testing.expectEqualSlices(u64, &.{2}, delayed.delayed);
    state.retire(a, ready.tokens);
    try std.testing.expectEqual(@as(u64, 3_600_000), state.deadline());
    // A different physical provider batch still consumes the logical budget.
    try state.park(a, group, 3_000, .{ .fingerprint = 33, .logical_fingerprint = 2, .failures = 2 }, source);
    try std.testing.expectEqual(@as(u32, 2), state.priorLogical(2, source).failures);
    try std.testing.expectEqual(@as(u32, 0), state.priorLogical(2, @as([32]u8, @splat(9))).failures);
    // Retiring an obsolete definition cannot shorten the surviving clock.
    const after_change = try state.prepareDocument(a, ready.documents[0], source, &.{2}, 2_000);
    defer a.free(after_change.delayed);
    try std.testing.expectEqualSlices(u64, &.{2}, after_change.delayed);
    const before_delete = try state.due(a, 3_000);
    defer before_delete.deinit(a);
    state.retire(a, before_delete.tokens);
    try std.testing.expectEqual(@as(u64, 3_600_000), state.deadline());
    state.retireSuperseded(a, .{ .doc_key = "document", .sequence = 3 }, ready.documents[0], null);
    try std.testing.expectEqual(@as(usize, 0), state.tickets.items.len);
}

test "scoped retry workset allocation failures preserve ownership" {
    const Harness = struct {
        fn run(a: A) !void {
            var state = State{};
            defer state.deinit(a);
            try state.park(a, .{ .doc_key = "document", .sequence = 1 }, 1_000, .{ .fingerprint = 1, .logical_fingerprint = 7 }, null);
            try state.park(a, .{ .doc_key = "document", .sequence = 1 }, 2_000, .{ .fingerprint = 2, .logical_fingerprint = 8 }, null);
            const raw = try state.encode(a);
            defer a.free(raw);
            var restored = try State.decode(a, raw);
            defer restored.deinit(a);
            const ready = try restored.due(a, 1_000);
            defer ready.deinit(a);
            const delayed = try restored.prepareDocument(a, ready.documents[0], null, &.{8}, 1_000);
            defer a.free(delayed.delayed);
            restored.retire(a, ready.tokens);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}
