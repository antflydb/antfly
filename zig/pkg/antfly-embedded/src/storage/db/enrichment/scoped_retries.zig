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
const binary = @import("scoped_retry_encoding.zig");
const Group = @import("enrichment_worker.zig").PendingDocumentGroup;

/// A bounded durable workset, separate from the contiguous applied watermark.
/// Tickets retain identities, clocks and budgets, never documents or credentials.
pub const max_documents = 128;
pub const max_bytes = 16 * 1024 * 1024;
pub const max_document_key_bytes = 67_500; // v1 allowed 90,000 base64 bytes.
pub const max_budgets = 1024;
pub const SourceStamp = struct {
    mutation_revision: u64,
    incarnation: u64,
    digest: [32]u8,
    pub fn sameInput(self: @This(), other: @This()) bool {
        return self.incarnation == other.incarnation and std.mem.eql(u8, &self.digest, &other.digest);
    }
};
const header_magic = "ESQ2";
const empty_cursor_header_magic = "ESQ3";
const referenced_cursor_header_magic = "ESQ4";
// Optional bounded envelope around an ESQ2/3/4 cursor header. Ticket rows
// and invocation budgets are unchanged; only outstanding reconsideration is new.
const replan_header_magic = "ESQ5";
const replan_mask_bytes = (max_documents + 7) / 8;
const snapshot_magic = "ESR2";
const ticket_magic = "EST2";
const referenced_ticket_magic = "EST3";
const max_references = max_documents + 1;
const Reference = [32]u8;

/// Large opaque IDs stay in separate, deduplicated durable rows. Scheduling
/// metadata keeps a SHA-256 reference, never a larger per-slot allowance.
/// '@' is outside base64, so a reference cannot alias an ordinary opaque ID.
pub fn documentIdentityAlloc(a: A, key: []const u8) ![]u8 {
    if (key.len <= max_document_key_bytes) {
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(key.len));
        _ = std.base64.standard.Encoder.encode(encoded, key);
        return encoded;
    }
    var digest: Reference = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &digest, .{});
    const encoded = try a.alloc(u8, 1 + std.base64.standard.Encoder.calcSize(digest.len));
    encoded[0] = '@';
    _ = std.base64.standard.Encoder.encode(encoded[1..], &digest);
    return encoded;
}
fn isReference(encoded: []const u8) bool {
    return encoded.len != 0 and encoded[0] == '@';
}
fn wireIdentity(encoded: []const u8) []const u8 {
    return if (isReference(encoded)) encoded[1..] else encoded;
}
fn referenceDigest(encoded: []const u8) !Reference {
    if (!isReference(encoded) or try binary.keyLength(encoded[1..]) != 32) return error.InvalidEnrichmentState;
    var digest: Reference = undefined;
    std.base64.standard.Decoder.decode(&digest, encoded[1..]) catch return error.InvalidEnrichmentState;
    return digest;
}
fn readIdentity(a: A, reader: *binary.Reader, referenced: bool) ![]const u8 {
    const encoded = try reader.key(a, if (referenced) 32 else max_document_key_bytes);
    if (!referenced) return encoded;
    defer if (encoded.len != 0) a.free(encoded);
    if (try binary.keyLength(encoded) != 32) return error.InvalidEnrichmentState;
    return std.mem.concat(a, u8, &.{ "@", encoded });
}
pub const DocumentResolver = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, Reference) anyerror![]u8,
};
const PendingDocument = struct { digest: Reference, bytes: []u8 };
pub const DocumentLocation = struct { revision_hash: [32]u8, ordinal: u32, kind: u8 };
pub const max_ticket_bytes = 4 + 4 + max_document_key_bytes + 8 + 1 + 32 + 1 + 48 + 8 + 8 + 2 + max_budgets * 40;
const max_cursor_header_bytes = 4 + 8 + 8 + 4 + max_document_key_bytes + 1 + 2 + max_documents;
const max_header_bytes = 8 + max_cursor_header_bytes + replan_mask_bytes;
comptime {
    // Every admitted combination fits, including maximum opaque keys, every
    // producer budget, both source identities, and a partial-record cursor.
    std.debug.assert(8 + max_header_bytes + max_documents * (4 + max_ticket_bytes) <= max_bytes);
}
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
    document_bytes: u64 = 0, // nonzero only for an external reference
    document_location: ?DocumentLocation = null,
    sequence: u64,
    source_digest: ?[32]u8 = null,
    source_stamp: ?SourceStamp = null,
    slot: u8 = 0,
    dirty: bool = true,
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
const WireTicket = struct { document: []const u8, sequence: u64, source_digest: ?[32]u8 = null, source_stamp: ?SourceStamp = null, deadline_ms: u64, revision: u64, budgets: []const Budget };
const Wire = struct { version: u32 = 1, config_hash: u64 = 0, scanned_sequence: u64 = 0, scan_document: []const u8 = "", tickets: []const WireTicket = &.{} };
pub const Token = struct { document: []const u8, sequence: u64, fingerprint: u64, logical_fingerprint: u64, revision: u64 };
pub const Due = struct {
    groups: []Group,
    tokens: []Token,
    documents: [][]const u8,
    replanned_slots: std.StaticBitSet(max_documents) = .empty,
    pub fn deinit(self: Due, a: A) void {
        for (self.groups) |group| a.free(group.doc_key);
        a.free(self.groups);
        a.free(self.tokens);
        for (self.documents) |document| a.free(document);
        a.free(self.documents);
    }
};
pub const PreparedDocument = struct { delayed: []u64, changed: bool };
const BudgetKey = struct { logical: u64, digest: ?[32]u8 };
const BudgetCounts = struct { failures: u32 = 0, waits: u32 = 0 };
pub const State = struct {
    config_hash: u64 = 0,
    scanned_sequence: u64 = 0,
    scan_document: []const u8 = "",
    scan_document_bytes: u64 = 0,
    empty_scan_document: bool = false,
    tickets: std.ArrayListUnmanaged(Ticket) = .empty,
    persisted_slots: std.StaticBitSet(max_documents) = .empty,
    // Configuration reconsideration is independent of invocation deadlines.
    // Slot identities survive swapRemove and incremental persistence.
    pending_replan: std.StaticBitSet(max_documents) = .empty,
    persisted_documents: [max_references]Reference = undefined,
    persisted_document_count: usize = 0,
    pending_documents: std.ArrayListUnmanaged(PendingDocument) = .empty,
    logical_budgets: std.AutoHashMapUnmanaged(BudgetKey, BudgetCounts) = .empty,
    logical_present: std.AutoHashMapUnmanaged(u64, void) = .empty,
    budget_index_valid: bool = false,

    /// Derived metadata only: rebuilt once after decode/removal, then updated
    /// incrementally as failures are parked. Never serialized or pointer-based.
    fn invalidateBudgetIndex(self: *State) void {
        self.budget_index_valid = false;
    }
    pub fn removeTicket(self: *State, a: A, index: usize) void {
        self.invalidateBudgetIndex();
        self.pending_replan.unset(self.tickets.items[index].slot);
        self.tickets.items[index].deinit(a);
        _ = self.tickets.swapRemove(index);
    }
    pub fn beginReplan(self: *State) void {
        for (self.tickets.items) |ticket| self.pending_replan.set(ticket.slot);
    }
    pub fn completeReplan(self: *State, due_work: Due) void {
        // Called only after the entire selected page has published/drained.
        // New retries retain their clocks; they need no second reconsideration.
        self.pending_replan = self.pending_replan.differenceWith(due_work.replanned_slots);
    }
    fn mergeBudget(self: *State, digest: ?[32]u8, budget: Budget) void {
        const key = BudgetKey{ .logical = budget.logical_fingerprint, .digest = digest };
        const entry = self.logical_budgets.getOrPutAssumeCapacity(key);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        entry.value_ptr.failures = @max(entry.value_ptr.failures, budget.failures);
        entry.value_ptr.waits = @max(entry.value_ptr.waits, budget.waits);
        self.logical_present.putAssumeCapacity(key.logical, {});
    }
    fn ensureBudgetIndex(self: *State, a: A) !void {
        if (self.budget_index_valid) return;
        self.logical_budgets.clearRetainingCapacity();
        self.logical_present.clearRetainingCapacity();
        var count: usize = 0;
        for (self.tickets.items) |ticket| count += ticket.budgets.items.len;
        try self.logical_budgets.ensureTotalCapacity(a, @intCast(count));
        try self.logical_present.ensureTotalCapacity(a, @intCast(count));
        for (self.tickets.items) |ticket| for (ticket.budgets.items) |budget| self.mergeBudget(ticket.source_digest, budget);
        self.budget_index_valid = true;
    }
    pub fn hasLogical(self: *State, a: A, logical: u64) !bool {
        try self.ensureBudgetIndex(a);
        return self.logical_present.contains(logical);
    }

    pub fn deinit(self: *State, a: A) void {
        for (self.tickets.items) |*ticket| ticket.deinit(a);
        self.tickets.deinit(a);
        self.logical_budgets.deinit(a);
        self.logical_present.deinit(a);
        self.releasePersistedDocuments(a);
        self.pending_documents.deinit(a);
        if (self.scan_document.len != 0) a.free(self.scan_document);
        self.* = .{};
    }
    pub fn releasePersistedDocuments(self: *State, a: A) void {
        for (self.pending_documents.items) |document| a.free(document.bytes);
        self.pending_documents.clearRetainingCapacity();
    }
    fn rememberDocument(self: *State, a: A, identity: []const u8, bytes: []const u8) !void {
        if (!isReference(identity)) return;
        const digest = try referenceDigest(identity);
        for (self.persisted_documents[0..self.persisted_document_count]) |existing|
            if (std.mem.eql(u8, &existing, &digest)) return;
        var pending_bytes: usize = 0;
        for (self.pending_documents.items) |existing| {
            if (std.mem.eql(u8, &existing.digest, &digest)) return;
            pending_bytes +|= existing.bytes.len;
        }
        // The replay source admits one oversized record to make progress.
        // Persist it before admitting another, rather than multiplying an
        // oversized allowance by the scheduling slot count.
        if (self.pending_documents.items.len != 0 and bytes.len > max_bytes -| pending_bytes)
            return error.MemoryBudgetExceeded;
        const copy = try a.dupe(u8, bytes);
        errdefer a.free(copy);
        try self.pending_documents.append(a, .{ .digest = digest, .bytes = copy });
    }
    pub fn parkBounded(self: *State, a: A, group: Group, deadline_ms: u64, budget: Budget, source_digest: ?[32]u8, stamp: ?SourceStamp, location: ?DocumentLocation, pending_limit: usize, publisher: anytype) !void {
        var retained: usize = 0;
        for (self.pending_documents.items) |document| retained +|= document.bytes.len;
        if (retained != 0 and group.doc_key.len > max_document_key_bytes and
            group.doc_key.len > @min(pending_limit, max_bytes) -| retained)
        {
            var digest: Reference = undefined;
            std.crypto.hash.sha2.Sha256.hash(group.doc_key, &digest, .{});
            const known = known: {
                for (self.persisted_documents[0..self.persisted_document_count]) |value|
                    if (std.mem.eql(u8, &value, &digest)) break :known true;
                for (self.pending_documents.items) |value|
                    if (std.mem.eql(u8, &value.digest, &digest)) break :known true;
                break :known false;
            };
            // Publish the first large identity before retaining another. The
            // caller supplies its existing owner/fence; never reacquire a
            // writer while that publication authority is already held.
            if (!known) try publisher.save();
        }
        return self.parkLocated(a, group, deadline_ms, budget, source_digest, stamp, location);
    }
    fn referencedDocuments(self: State, out: *[max_references]Reference) usize {
        var count: usize = 0;
        if (isReference(self.scan_document)) {
            out[count] = referenceDigest(self.scan_document) catch unreachable;
            count += 1;
        }
        for (self.tickets.items) |ticket| {
            if (!isReference(ticket.document)) continue;
            const digest = referenceDigest(ticket.document) catch unreachable;
            for (out[0..count]) |existing| {
                if (std.mem.eql(u8, &existing, &digest)) break;
            } else {
                out[count] = digest;
                count += 1;
            }
        }
        return count;
    }
    pub fn documentAlloc(self: State, a: A, identity: []const u8, expected_bytes: u64, resolver: ?DocumentResolver) ![]u8 {
        if (!isReference(identity)) {
            const n = try binary.keyLength(identity);
            const key = try a.alloc(u8, n);
            errdefer a.free(key);
            try std.base64.standard.Decoder.decode(key, identity);
            return key;
        }
        const digest = try referenceDigest(identity);
        const key = pending: {
            for (self.pending_documents.items) |document|
                if (std.mem.eql(u8, &document.digest, &digest)) break :pending try a.dupe(u8, document.bytes);
            const port = resolver orelse return error.InvalidEnrichmentState;
            break :pending try port.get(port.ptr, a, digest);
        };
        errdefer a.free(key);
        var actual: Reference = undefined;
        std.crypto.hash.sha2.Sha256.hash(key, &actual, .{});
        if (key.len != expected_bytes or !std.mem.eql(u8, &digest, &actual)) return error.InvalidEnrichmentState;
        return key;
    }
    pub fn decode(a: A, bytes: []const u8) !State {
        if (bytes.len > max_bytes) return error.InvalidEnrichmentState;
        if (!std.mem.startsWith(u8, bytes, snapshot_magic)) return decodeLegacy(a, bytes);
        var reader = binary.Reader{ .bytes = bytes };
        _ = try reader.take(4);
        const header = try reader.take(try reader.int(u32));
        var decoded = try decodeHeader(a, header);
        errdefer decoded.state.deinit(a);
        for (decoded.slots[0..decoded.count]) |slot| {
            const raw = try reader.take(try reader.int(u32));
            var ticket = try decodeTicket(a, raw, slot);
            errdefer ticket.deinit(a);
            try decoded.state.tickets.append(a, ticket);
        }
        try reader.end();
        decoded.state.persisted_document_count = decoded.state.referencedDocuments(&decoded.state.persisted_documents);
        return decoded.state;
    }
    fn decodeLegacy(a: A, bytes: []const u8) !State {
        if (bytes.len > max_bytes) return error.InvalidEnrichmentState;
        var parsed = try std.json.parseFromSlice(Wire, a, bytes, .{});
        defer parsed.deinit();
        if (parsed.value.version != 1 or parsed.value.tickets.len > max_documents) return error.InvalidEnrichmentState;
        var result = State{ .config_hash = parsed.value.config_hash, .scanned_sequence = parsed.value.scanned_sequence };
        if (parsed.value.scan_document.len != 0) result.scan_document = try a.dupe(u8, parsed.value.scan_document);
        errdefer result.deinit(a);
        for (parsed.value.tickets, 0..) |wire, slot| {
            if (wire.document.len > 90_000 or (wire.budgets.len == 0 or wire.budgets.len > max_budgets) or wire.revision == 0) return error.InvalidEnrichmentState;
            _ = std.base64.standard.Decoder.calcSizeForSlice(wire.document) catch return error.InvalidEnrichmentState;
            var ticket = Ticket{ .document = try a.dupe(u8, wire.document), .sequence = wire.sequence, .source_digest = wire.source_digest, .source_stamp = wire.source_stamp, .slot = @intCast(slot), .deadline_ms = wire.deadline_ms, .revision = wire.revision };
            errdefer ticket.deinit(a);
            try ticket.budgets.appendSlice(a, wire.budgets);
            for (ticket.budgets.items) |budget| {
                if (budget.fingerprint == 0 or budget.revision == 0 or budget.revision > ticket.revision) return error.InvalidEnrichmentState;
            }
            ticket.refreshDeadline();
            if (ticket.deadline_ms != wire.deadline_ms) return error.InvalidEnrichmentState;
            try validateTicket(ticket);
            try result.tickets.append(a, ticket);
        }
        return result;
    }
    pub fn encode(self: State, a: A) ![]u8 {
        const plan = try self.preparePersistence(a, true);
        defer plan.deinit(a);
        const header = plan.header;
        var size: usize = 8 + header.len;
        for (plan.rows) |row| size += 4 + row.bytes.len;
        if (size > max_bytes) return error.InvalidEnrichmentState;
        const bytes = try a.alloc(u8, size);
        var writer = binary.Writer{ .bytes = bytes };
        writer.put(snapshot_magic);
        writer.int(u32, @intCast(header.len));
        writer.put(header);
        for (plan.rows) |row| {
            writer.int(u32, @intCast(row.bytes.len));
            writer.put(row.bytes);
        }
        std.debug.assert(writer.offset == bytes.len);
        return bytes;
    }
    pub fn isHeader(bytes: []const u8) bool {
        return std.mem.startsWith(u8, bytes, replan_header_magic) or std.mem.startsWith(u8, bytes, header_magic) or std.mem.startsWith(u8, bytes, empty_cursor_header_magic) or std.mem.startsWith(u8, bytes, referenced_cursor_header_magic);
    }
    pub const Header = struct { state: State, slots: [max_documents]u8 = undefined, count: usize };
    pub fn decodeHeader(a: A, raw: []const u8) !Header {
        if (!isHeader(raw) or raw.len > max_header_bytes) return error.InvalidEnrichmentState;
        var reader = binary.Reader{ .bytes = raw };
        _ = try reader.take(4);
        if (std.mem.startsWith(u8, raw, replan_header_magic)) {
            const base = try reader.take(try reader.int(u32));
            // Exactly one envelope; reject nesting before decoding the cursor.
            if (!isHeader(base) or std.mem.startsWith(u8, base, replan_header_magic)) return error.InvalidEnrichmentState;
            var result = try decodeCursorHeader(a, base);
            errdefer result.state.deinit(a);
            const mask = try reader.take(replan_mask_bytes);
            var used: std.StaticBitSet(max_documents) = .empty;
            for (result.slots[0..result.count]) |slot| used.set(slot);
            for (0..max_documents) |slot| if (mask[slot / 8] & (@as(u8, 1) << @intCast(slot % 8)) != 0) {
                if (!used.isSet(slot)) return error.InvalidEnrichmentState;
                result.state.pending_replan.set(slot);
            };
            try reader.end();
            return result;
        }
        return decodeCursorHeader(a, raw);
    }
    fn decodeCursorHeader(a: A, raw: []const u8) !Header {
        var reader = binary.Reader{ .bytes = raw };
        _ = try reader.take(4);
        var result = Header{ .state = .{}, .count = 0 };
        errdefer result.state.deinit(a);
        result.state.config_hash = try reader.int(u64);
        result.state.scanned_sequence = try reader.int(u64);
        const referenced = std.mem.startsWith(u8, raw, referenced_cursor_header_magic);
        result.state.scan_document = try readIdentity(a, &reader, referenced);
        if (referenced) {
            result.state.scan_document_bytes = try reader.int(u64);
            if (result.state.scan_document_bytes <= max_document_key_bytes) return error.InvalidEnrichmentState;
        }
        if (std.mem.startsWith(u8, raw, empty_cursor_header_magic)) {
            if (try reader.int(u8) != 1 or result.state.scan_document.len != 0) return error.InvalidEnrichmentState;
            result.state.empty_scan_document = true;
        }
        result.count = try reader.int(u16);
        if (result.count > max_documents) return error.InvalidEnrichmentState;
        var used: std.StaticBitSet(max_documents) = .empty;
        for (result.slots[0..result.count]) |*slot| {
            slot.* = try reader.int(u8);
            if (slot.* >= max_documents or used.isSet(slot.*)) return error.InvalidEnrichmentState;
            used.set(slot.*);
        }
        try reader.end();
        return result;
    }
    fn encodeHeader(self: State, a: A) ![]u8 {
        const base = try self.encodeCursorHeader(a);
        if (self.pending_replan.count() == 0) return base;
        defer a.free(base);
        const bytes = try a.alloc(u8, 8 + base.len + replan_mask_bytes);
        errdefer a.free(bytes);
        var writer = binary.Writer{ .bytes = bytes };
        writer.put(replan_header_magic);
        writer.int(u32, @intCast(base.len));
        writer.put(base);
        var mask: [replan_mask_bytes]u8 = @splat(0);
        for (0..max_documents) |slot| if (self.pending_replan.isSet(slot)) {
            mask[slot / 8] |= @as(u8, 1) << @intCast(slot % 8);
        };
        writer.put(&mask);
        return bytes;
    }
    fn encodeCursorHeader(self: State, a: A) ![]u8 {
        if (self.tickets.items.len > max_documents) return error.InvalidEnrichmentState;
        const cursor_len = try binary.keyLength(wireIdentity(self.scan_document));
        if (cursor_len > max_document_key_bytes) return error.InvalidEnrichmentState;
        const referenced = isReference(self.scan_document);
        if (referenced and (cursor_len != 32 or self.scan_document_bytes <= max_document_key_bytes)) return error.InvalidEnrichmentState;
        const empty_cursor = self.empty_scan_document and self.scan_document.len == 0;
        const bytes = try a.alloc(u8, 4 + 8 + 8 + 4 + cursor_len + @as(usize, if (referenced) 8 else 0) + @intFromBool(empty_cursor) + 2 + self.tickets.items.len);
        errdefer a.free(bytes);
        var writer = binary.Writer{ .bytes = bytes };
        writer.put(if (referenced) referenced_cursor_header_magic else if (empty_cursor) empty_cursor_header_magic else header_magic);
        writer.int(u64, self.config_hash);
        writer.int(u64, self.scanned_sequence);
        try writer.key(wireIdentity(self.scan_document));
        if (referenced) writer.int(u64, self.scan_document_bytes);
        if (empty_cursor) writer.int(u8, 1);
        writer.int(u16, @intCast(self.tickets.items.len));
        var used: std.StaticBitSet(max_documents) = .empty;
        for (self.tickets.items) |ticket| {
            if (ticket.slot >= max_documents or used.isSet(ticket.slot)) return error.InvalidEnrichmentState;
            used.set(ticket.slot);
            writer.int(u8, ticket.slot);
        }
        return bytes;
    }
    fn validateTicket(ticket: Ticket) !void {
        const n = try binary.keyLength(wireIdentity(ticket.document));
        if (n > max_document_key_bytes or ticket.revision == 0 or ticket.budgets.items.len == 0 or ticket.budgets.items.len > max_budgets) return error.InvalidEnrichmentState;
        if (isReference(ticket.document)) {
            if (n != 32 or ticket.document_bytes <= max_document_key_bytes) return error.InvalidEnrichmentState;
            if (ticket.document_location) |location| if (location.ordinal == 0) return error.InvalidEnrichmentState;
        } else if (ticket.document_bytes != 0 or ticket.document_location != null) return error.InvalidEnrichmentState;
        var earliest: u64 = std.math.maxInt(u64);
        for (ticket.budgets.items) |budget| {
            if (budget.fingerprint == 0 or budget.revision == 0 or budget.revision > ticket.revision) return error.InvalidEnrichmentState;
            earliest = @min(earliest, budget.deadline_ms);
        }
        if (earliest != ticket.deadline_ms) return error.InvalidEnrichmentState;
    }
    pub fn decodeTicket(a: A, raw: []const u8, slot: u8) !Ticket {
        const referenced = std.mem.startsWith(u8, raw, referenced_ticket_magic);
        if (raw.len > max_ticket_bytes or (!referenced and !std.mem.startsWith(u8, raw, ticket_magic)) or slot >= max_documents) return error.InvalidEnrichmentState;
        var reader = binary.Reader{ .bytes = raw };
        _ = try reader.take(4);
        var ticket = Ticket{ .document = try readIdentity(a, &reader, referenced), .sequence = 0, .deadline_ms = 0, .revision = 0, .slot = slot };
        errdefer ticket.deinit(a);
        if (referenced) {
            ticket.document_bytes = try reader.int(u64);
            if (try reader.flag()) ticket.document_location = .{ .revision_hash = (try reader.take(32))[0..32].*, .ordinal = try reader.int(u32), .kind = try reader.int(u8) };
        }
        ticket.sequence = try reader.int(u64);
        if (try reader.flag()) ticket.source_digest = (try reader.take(32))[0..32].*;
        if (try reader.flag()) ticket.source_stamp = .{ .mutation_revision = try reader.int(u64), .incarnation = try reader.int(u64), .digest = (try reader.take(32))[0..32].* };
        ticket.deadline_ms = try reader.int(u64);
        ticket.revision = try reader.int(u64);
        const count = try reader.int(u16);
        if (count == 0 or count > max_budgets) return error.InvalidEnrichmentState;
        try ticket.budgets.ensureTotalCapacity(a, count);
        for (0..count) |_| ticket.budgets.appendAssumeCapacity(.{ .fingerprint = try reader.int(u64), .logical_fingerprint = try reader.int(u64), .failures = try reader.int(u32), .waits = try reader.int(u32), .deadline_ms = try reader.int(u64), .revision = try reader.int(u64) });
        try reader.end();
        try validateTicket(ticket);
        return ticket;
    }
    fn encodeTicket(ticket: Ticket, a: A) ![]u8 {
        try validateTicket(ticket);
        const referenced = isReference(ticket.document);
        const size = 4 + 4 + try binary.keyLength(wireIdentity(ticket.document)) + @as(usize, if (referenced) 9 + @as(usize, if (ticket.document_location != null) 37 else 0) else 0) + 8 + 1 + @as(usize, if (ticket.source_digest != null) 32 else 0) + 1 + @as(usize, if (ticket.source_stamp != null) 48 else 0) + 8 + 8 + 2 + ticket.budgets.items.len * 40;
        const bytes = try a.alloc(u8, size);
        errdefer a.free(bytes);
        var writer = binary.Writer{ .bytes = bytes };
        writer.put(if (referenced) referenced_ticket_magic else ticket_magic);
        try writer.key(wireIdentity(ticket.document));
        if (referenced) {
            writer.int(u64, ticket.document_bytes);
            writer.int(u8, @intFromBool(ticket.document_location != null));
            if (ticket.document_location) |location| {
                writer.put(&location.revision_hash);
                writer.int(u32, location.ordinal);
                writer.int(u8, location.kind);
            }
        }
        writer.int(u64, ticket.sequence);
        writer.int(u8, @intFromBool(ticket.source_digest != null));
        if (ticket.source_digest) |digest| writer.put(&digest);
        writer.int(u8, @intFromBool(ticket.source_stamp != null));
        if (ticket.source_stamp) |stamp| {
            writer.int(u64, stamp.mutation_revision);
            writer.int(u64, stamp.incarnation);
            writer.put(&stamp.digest);
        }
        writer.int(u64, ticket.deadline_ms);
        writer.int(u64, ticket.revision);
        writer.int(u16, @intCast(ticket.budgets.items.len));
        for (ticket.budgets.items) |budget| {
            writer.int(u64, budget.fingerprint);
            writer.int(u64, budget.logical_fingerprint);
            writer.int(u32, budget.failures);
            writer.int(u32, budget.waits);
            writer.int(u64, budget.deadline_ms);
            writer.int(u64, budget.revision);
        }
        std.debug.assert(writer.offset == bytes.len);
        return bytes;
    }
    pub const Persistence = struct {
        pub const Row = struct { slot: u8, bytes: []u8 };
        header: []u8,
        rows: []Row,
        documents: []PendingDocument,
        removed_documents: [max_references]Reference = undefined,
        removed_document_count: usize = 0,
        removed: [max_documents]u8 = undefined,
        removed_len: usize = 0,
        pub fn deinit(self: @This(), a: A) void {
            a.free(self.header);
            for (self.rows) |row| a.free(row.bytes);
            a.free(self.rows);
            a.free(self.documents);
        }
    };
    pub fn preparePersistence(self: State, a: A, all: bool) !Persistence {
        const header = try self.encodeHeader(a);
        errdefer a.free(header);
        var rows: std.ArrayListUnmanaged(Persistence.Row) = .empty;
        errdefer {
            for (rows.items) |row| a.free(row.bytes);
            rows.deinit(a);
        }
        var used: std.StaticBitSet(max_documents) = .empty;
        for (self.tickets.items) |ticket| {
            used.set(ticket.slot);
            if (!all and !ticket.dirty and self.persisted_slots.isSet(ticket.slot)) continue;
            const bytes = try encodeTicket(ticket, a);
            errdefer a.free(bytes);
            try rows.append(a, .{ .slot = ticket.slot, .bytes = bytes });
        }
        var wanted: [max_references]Reference = undefined;
        const wanted_count = self.referencedDocuments(&wanted);
        var documents: std.ArrayListUnmanaged(PendingDocument) = .empty;
        defer documents.deinit(a);
        for (self.pending_documents.items) |document| for (wanted[0..wanted_count]) |digest| {
            if (std.mem.eql(u8, &document.digest, &digest)) {
                try documents.append(a, document);
                break;
            }
        };
        const owned_documents = try documents.toOwnedSlice(a);
        errdefer a.free(owned_documents);
        var result = Persistence{ .header = header, .rows = try rows.toOwnedSlice(a), .documents = owned_documents };
        for (self.persisted_documents[0..self.persisted_document_count]) |digest| {
            for (wanted[0..wanted_count]) |current| {
                if (std.mem.eql(u8, &digest, &current)) break;
            } else {
                result.removed_documents[result.removed_document_count] = digest;
                result.removed_document_count += 1;
            }
        }
        for (0..max_documents) |slot| if (self.persisted_slots.isSet(slot) and !used.isSet(slot)) {
            result.removed[result.removed_len] = @intCast(slot);
            result.removed_len += 1;
        };
        return result;
    }
    pub fn markPersisted(self: *State) void {
        self.persisted_slots = .empty;
        for (self.tickets.items) |*ticket| {
            ticket.dirty = false;
            self.persisted_slots.set(ticket.slot);
        }
        self.persisted_document_count = self.referencedDocuments(&self.persisted_documents);
    }
    pub fn clearScanDocument(self: *State, a: A) void {
        if (self.scan_document.len != 0) a.free(self.scan_document);
        self.scan_document = "";
        self.scan_document_bytes = 0;
        self.empty_scan_document = false;
    }
    pub fn scanDocumentAlloc(self: State, a: A) ![]u8 {
        return self.scanDocumentAllocWithReferences(a, null);
    }
    pub fn scanDocumentAllocWithReferences(self: State, a: A, resolver: ?DocumentResolver) ![]u8 {
        return self.documentAlloc(a, self.scan_document, self.scan_document_bytes, resolver);
    }
    pub fn setScanDocument(self: *State, a: A, group: Group) !void {
        const encoded = try documentIdentityAlloc(a, group.doc_key);
        errdefer a.free(encoded);
        try self.rememberDocument(a, encoded, group.doc_key);
        self.clearScanDocument(a);
        self.scan_document = encoded;
        self.scan_document_bytes = if (isReference(encoded)) group.doc_key.len else 0;
        self.empty_scan_document = group.doc_key.len == 0;
        self.scanned_sequence = group.sequence;
    }
    pub fn hasScanDocument(self: State) bool {
        return self.empty_scan_document or self.scan_document.len != 0;
    }
    pub fn completedSequence(self: State) u64 {
        return self.scanned_sequence -| @as(u64, if (self.hasScanDocument()) 1 else 0);
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
        return self.parkStamped(a, group, deadline_ms, budget, source_digest, null);
    }
    pub fn parkStamped(self: *State, a: A, group: Group, deadline_ms: u64, budget: Budget, source_digest: ?[32]u8, stamp: ?SourceStamp) !void {
        return self.parkLocated(a, group, deadline_ms, budget, source_digest, stamp, null);
    }
    pub fn parkLocated(self: *State, a: A, group: Group, deadline_ms: u64, budget: Budget, source_digest: ?[32]u8, stamp: ?SourceStamp, location: ?DocumentLocation) !void {
        if (self.budget_index_valid) {
            try self.logical_budgets.ensureUnusedCapacity(a, 1);
            try self.logical_present.ensureUnusedCapacity(a, 1);
        }
        const encoded = try documentIdentityAlloc(a, group.doc_key);
        defer a.free(encoded);
        try self.rememberDocument(a, encoded, group.doc_key);
        var selected: ?*Ticket = null;
        for (self.tickets.items) |*ticket| {
            if (ticket.sequence == group.sequence and std.mem.eql(u8, ticket.document, encoded)) {
                selected = ticket;
                break;
            }
        }
        if (selected == null) {
            if (self.tickets.items.len == max_documents) return error.MemoryBudgetExceeded;
            var used: std.StaticBitSet(max_documents) = .empty;
            for (self.tickets.items) |existing| used.set(existing.slot);
            var slot: usize = 0;
            while (used.isSet(slot)) : (slot += 1) {}
            var ticket = Ticket{ .slot = @intCast(slot), .source_stamp = stamp, .document = try a.dupe(u8, encoded), .document_bytes = if (isReference(encoded)) group.doc_key.len else 0, .document_location = if (isReference(encoded)) location else null, .sequence = group.sequence, .source_digest = source_digest, .deadline_ms = deadline_ms, .revision = 1 };
            errdefer ticket.deinit(a);
            var next = budget;
            next.deadline_ms = deadline_ms;
            next.revision = 1;
            try ticket.budgets.append(a, next);
            try self.tickets.append(a, ticket);
            if (self.budget_index_valid) self.mergeBudget(source_digest, next);
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
            if (ticket.budgets.items.len == max_budgets) return error.MemoryBudgetExceeded;
            try ticket.budgets.ensureUnusedCapacity(a, 1);
        }
        // Every allocation precedes mutation, including source replacement.
        if (changed) {
            ticket.budgets.clearRetainingCapacity();
            self.invalidateBudgetIndex();
        }
        ticket.source_digest = source_digest;
        ticket.source_stamp = stamp;
        if (isReference(ticket.document) and location != null) ticket.document_location = location;
        ticket.dirty = true;
        ticket.revision = revision;
        var next = budget;
        next.deadline_ms = deadline_ms;
        next.revision = revision;
        if (old) |i| {
            if (next.logical_fingerprint != ticket.budgets.items[i].logical_fingerprint) self.invalidateBudgetIndex();
            next.deadline_ms = @max(next.deadline_ms, ticket.budgets.items[i].deadline_ms);
            next.failures = @max(next.failures, ticket.budgets.items[i].failures);
            next.waits = @max(next.waits, ticket.budgets.items[i].waits);
            ticket.budgets.items[i] = next;
        } else ticket.budgets.appendAssumeCapacity(next);
        ticket.refreshDeadline();
        if (self.budget_index_valid) self.mergeBudget(source_digest, next);
    }
    pub fn priorLogical(self: *State, a: A, logical: u64, source_digest: ?[32]u8) !Budget {
        try self.ensureBudgetIndex(a);
        const counts = self.logical_budgets.get(.{ .logical = logical, .digest = source_digest }) orelse BudgetCounts{};
        return .{ .fingerprint = 0, .logical_fingerprint = logical, .failures = counts.failures, .waits = counts.waits };
    }
    pub fn deadline(self: State) u64 {
        if (self.pending_replan.count() != 0) return 0;
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
        return self.selectDue(a, now_ms, false, null, 0);
    }
    pub fn replan(self: State, a: A, now_ms: u64) !Due {
        return self.selectDue(a, now_ms, true, null, 0);
    }
    pub fn dueWithReferences(self: State, a: A, now_ms: u64, resolver: DocumentResolver, max_key_bytes: usize) !Due {
        return self.selectDue(a, now_ms, false, resolver, max_key_bytes);
    }
    pub fn replanWithReferences(self: State, a: A, now_ms: u64, resolver: DocumentResolver, max_key_bytes: usize) !Due {
        return self.selectDue(a, now_ms, true, resolver, max_key_bytes);
    }
    fn selectDue(self: State, a: A, now_ms: u64, include_waiting: bool, resolver: ?DocumentResolver, max_key_bytes: usize) !Due {
        var document_count: usize = 0;
        var budget_count: usize = 0;
        var key_bytes: usize = 0;
        // Finish finite configuration work before ordinary due retries. A
        // quickly failing head ticket must not starve later replan pages.
        const replanning = self.pending_replan.count() != 0;
        for (self.tickets.items) |ticket| {
            if (if (replanning) !self.pending_replan.isSet(ticket.slot) else (!include_waiting and ticket.deadline_ms > now_ms)) continue;
            const n = if (isReference(ticket.document)) std.math.cast(usize, ticket.document_bytes) orelse return error.InvalidEnrichmentState else try binary.keyLength(ticket.document);
            if (document_count != 0 and max_key_bytes != 0 and n > max_key_bytes -| key_bytes) break;
            key_bytes +|= n;
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
        var replanned_slots: std.StaticBitSet(max_documents) = .empty;
        for (self.tickets.items) |ticket| {
            if (if (replanning) !self.pending_replan.isSet(ticket.slot) else (!include_waiting and ticket.deadline_ms > now_ms)) continue;
            if (initialized == document_count) break;
            if (self.pending_replan.isSet(ticket.slot)) replanned_slots.set(ticket.slot);
            const key = try self.documentAlloc(a, ticket.document, ticket.document_bytes, resolver);
            errdefer a.free(key);
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
        return .{ .groups = groups, .tokens = tokens, .documents = documents, .replanned_slots = replanned_slots };
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
                    self.invalidateBudgetIndex();
                    _ = ticket.budgets.swapRemove(j);
                    ticket.dirty = true;
                    changed = true;
                    continue;
                }
                if (budget.deadline_ms > now_ms and std.meta.eql(source_digest, ticket.source_digest) and
                    std.mem.indexOfScalar(u64, delayed.items, budget.logical_fingerprint) == null)
                    try delayed.append(a, budget.logical_fingerprint);
                j += 1;
            }
            if (ticket.budgets.items.len == 0) {
                self.removeTicket(a, i);
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
                self.invalidateBudgetIndex();
                self.removeTicket(a, i);
            } else i += 1;
        }
    }
    pub fn retire(self: *State, a: A, tokens: []const Token) void {
        for (tokens) |token| {
            for (self.tickets.items, 0..) |*ticket, i| {
                if (ticket.sequence != token.sequence or !std.mem.eql(u8, ticket.document, token.document)) continue;
                for (ticket.budgets.items, 0..) |budget, j| {
                    if (budget.fingerprint == token.fingerprint and budget.logical_fingerprint == token.logical_fingerprint and budget.revision == token.revision) {
                        self.invalidateBudgetIndex();
                        _ = ticket.budgets.swapRemove(j);
                        ticket.dirty = true;
                        break;
                    }
                }
                if (ticket.budgets.items.len == 0) {
                    self.removeTicket(a, i);
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
    try std.testing.expectEqual(@as(u32, 2), (try state.priorLogical(a, 2, source)).failures);
    try std.testing.expectEqual(@as(u32, 0), (try state.priorLogical(a, 2, @as([32]u8, @splat(9)))).failures);
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

test "scoped retry workset replan headers retain every cursor form and reject invalid slots" {
    const a = std.testing.allocator;
    const large = try a.alloc(u8, max_document_key_bytes + 1);
    defer a.free(large);
    @memset(large, 'x');
    for ([_][]const u8{ "", "cursor", large }) |key| {
        var state: State = .{};
        defer state.deinit(a);
        try state.park(a, .{ .doc_key = key, .sequence = 7 }, 1000, .{ .fingerprint = 1 }, null);
        try state.setScanDocument(a, .{ .doc_key = key, .sequence = 7 });
        // Existing ESQ2/3/4 cursor headers remain readable without migration.
        const legacy = try state.encodeHeader(a);
        defer a.free(legacy);
        var old = try State.decodeHeader(a, legacy);
        defer old.state.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), old.state.pending_replan.count());
        state.beginReplan();
        const bytes = try state.encodeHeader(a);
        defer a.free(bytes);
        var restored = try State.decodeHeader(a, bytes);
        defer restored.state.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), restored.state.pending_replan.count());
        try std.testing.expectEqualStrings(state.scan_document, restored.state.scan_document);
        try std.testing.expectEqual(state.scan_document_bytes, restored.state.scan_document_bytes);
        try std.testing.expectEqual(state.empty_scan_document, restored.state.empty_scan_document);
        // An unowned slot is corruption, never an endless immediate wakeup.
        bytes[bytes.len - 1] |= 0x80;
        try std.testing.expectError(error.InvalidEnrichmentState, State.decodeHeader(a, bytes));
    }
}

test "scoped retry workset allocation failures preserve ownership" {
    const Harness = struct {
        fn run(a: A) !void {
            var state = State{};
            defer state.deinit(a);
            try state.park(a, .{ .doc_key = "document", .sequence = 1 }, 1_000, .{ .fingerprint = 1, .logical_fingerprint = 7 }, null);
            try state.park(a, .{ .doc_key = "document", .sequence = 1 }, 2_000, .{ .fingerprint = 2, .logical_fingerprint = 8 }, null);
            state.beginReplan();
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

test "scoped retry workset oversized identity allocation failures preserve ownership" {
    const key = try std.testing.allocator.alloc(u8, max_document_key_bytes + 1);
    defer std.testing.allocator.free(key);
    @memset(key, 'x');
    const Harness = struct {
        fn run(a: A, document: []const u8) !void {
            var state: State = .{};
            defer state.deinit(a);
            try state.park(a, .{ .doc_key = document, .sequence = 1 }, 10, .{ .fingerprint = 1 }, null);
            try state.setScanDocument(a, .{ .doc_key = document, .sequence = 1 });
            state.beginReplan();
            const raw = try state.encode(a);
            defer a.free(raw);
            var restored = try State.decode(a, raw);
            defer restored.deinit(a);
            // Snapshot decoding must retain body ownership for later GC,
            // just as the incremental loader does.
            try std.testing.expectEqual(@as(usize, 1), restored.persisted_document_count);
            while (restored.tickets.items.len != 0) restored.removeTicket(a, 0);
            restored.clearScanDocument(a);
            const retirement = try restored.preparePersistence(a, false);
            defer retirement.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), retirement.removed_document_count);
            const ready = try state.due(a, 10);
            defer ready.deinit(a);
            try std.testing.expectEqualSlices(u8, document, ready.groups[0].doc_key);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{key});
}

test "scoped retry workset maximum metadata fits binary and incremental persistence" {
    const a = std.testing.allocator;
    var state = State{};
    defer state.deinit(a);
    for (0..128) |doc| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "doc-{d}", .{doc});
        for (0..1024) |producer| {
            const fp = 10_000_000_000_000_000_000 + doc * 1024 + producer;
            try state.park(a, .{ .doc_key = key, .sequence = doc + 1 }, 1_800_000_000_000, .{ .fingerprint = fp, .logical_fingerprint = fp, .failures = 1 }, null);
        }
    }
    try std.testing.expectEqual(@as(usize, 128), state.tickets.items.len);
    try std.testing.expect(try state.hasLogical(a, 10_000_000_000_000_000_000));
    // Warm lookups need no allocation, rebuild, or traversal of other budgets.
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    for (0..128 * 1024) |i| {
        const fp = 10_000_000_000_000_000_000 + i;
        try std.testing.expect(try state.hasLogical(denied.allocator(), fp));
        try std.testing.expectEqual(@as(u32, 1), (try state.priorLogical(denied.allocator(), fp, null)).failures);
    }

    const initial = try state.preparePersistence(a, false);
    defer initial.deinit(a);
    try std.testing.expectEqual(@as(usize, 128), initial.rows.len);
    state.markPersisted();
    try state.park(a, .{ .doc_key = "doc-0", .sequence = 1 }, 1_900_000_000_000, .{ .fingerprint = 10_000_000_000_000_000_000, .logical_fingerprint = 10_000_000_000_000_000_000, .failures = 2 }, null);
    const increment = try state.preparePersistence(a, false);
    defer increment.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), increment.rows.len);
    try std.testing.expect(increment.rows[0].bytes.len <= max_ticket_bytes);
    // Exercise the full supported Cartesian product, including longest keys
    // and both identities. This is a codec bound, independent of JSON digits.
    const key = try a.alloc(u8, max_document_key_bytes);
    defer a.free(key);
    @memset(key, 0xff);
    for (state.tickets.items) |*ticket| {
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(key.len));
        _ = std.base64.standard.Encoder.encode(encoded, key);
        a.free(ticket.document);
        ticket.document = encoded;
        ticket.source_digest = @splat(255);
        ticket.source_stamp = .{ .mutation_revision = std.math.maxInt(u64), .incarnation = 1, .digest = @splat(255) };
    }
    const raw = try state.encode(a);
    defer a.free(raw);
    try std.testing.expect(raw.len <= max_bytes);
    var restored = try State.decode(a, raw);
    defer restored.deinit(a);
    try std.testing.expectEqual(@as(usize, 128), restored.tickets.items.len);
    try std.testing.expectEqual(@as(usize, 1024), restored.tickets.items[127].budgets.items.len);
}

test "scoped retry workset indexed budgets survive replacement retirement and allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var state = State{};
            defer state.deinit(a);
            const source: [32]u8 = @splat(1);
            try state.park(a, .{ .doc_key = "doc", .sequence = 1 }, 100, .{ .fingerprint = 1, .logical_fingerprint = 7, .failures = 2 }, source);
            try std.testing.expect(try state.hasLogical(a, 7));
            try std.testing.expectEqual(@as(u32, 2), (try state.priorLogical(a, 7, source)).failures);
            // Duplicate journal notifications aggregate maxima across tickets.
            try state.park(a, .{ .doc_key = "doc", .sequence = 2 }, 200, .{ .fingerprint = 2, .logical_fingerprint = 7, .failures = 1 }, source);
            try std.testing.expectEqual(@as(u32, 2), (try state.priorLogical(a, 7, source)).failures);
            const due = try state.due(a, 100);
            defer due.deinit(a);
            state.retire(a, due.tokens);
            try std.testing.expectEqual(@as(u32, 1), (try state.priorLogical(a, 7, source)).failures);
            // A lower counter cannot forgive the same producer's old clock.
            try state.park(a, .{ .doc_key = "doc", .sequence = 2 }, 300, .{ .fingerprint = 3, .logical_fingerprint = 7 }, source);
            try std.testing.expectEqual(@as(u32, 1), (try state.priorLogical(a, 7, source)).failures);
            try state.park(a, .{ .doc_key = "doc", .sequence = 2 }, 400, .{ .fingerprint = 4, .logical_fingerprint = 8, .waits = 3 }, @splat(2));
            try std.testing.expect(!try state.hasLogical(a, 7));
            try std.testing.expectEqual(@as(u32, 0), (try state.priorLogical(a, 7, source)).failures);
            try std.testing.expectEqual(@as(u32, 3), (try state.priorLogical(a, 8, @splat(2))).waits);
            const encoded = try state.encode(a);
            defer a.free(encoded);
            var restored = try State.decode(a, encoded);
            defer restored.deinit(a);
            try std.testing.expectEqual(@as(u32, 3), (try restored.priorLogical(a, 8, @splat(2))).waits);
            const definitions = try restored.prepareDocument(a, restored.tickets.items[0].document, @splat(2), &.{9}, 0);
            defer a.free(definitions.delayed);
            try std.testing.expect(!try restored.hasLogical(a, 8));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "privacy retry workset preserves empty document tickets and exclusive cursors" {
    const a = std.testing.allocator;
    var state = State{};
    defer state.deinit(a);
    try state.park(a, .{ .doc_key = "", .sequence = 2 }, 100, .{ .fingerprint = 17, .failures = 1 }, null);
    try state.setScanDocument(a, .{ .doc_key = "", .sequence = 3 });
    try std.testing.expect(state.hasScanDocument());
    try std.testing.expectEqual(@as(u64, 2), state.completedSequence());
    const bytes = try state.encode(a);
    defer a.free(bytes);
    var restored = try State.decode(a, bytes);
    defer restored.deinit(a);
    try std.testing.expect(restored.hasScanDocument());
    try std.testing.expectEqual(@as(u64, 2), restored.completedSequence());
    const due = try restored.due(a, 100);
    defer due.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), due.groups.len);
    try std.testing.expectEqualStrings("", due.groups[0].doc_key);
    restored.clearScanDocument(a);
    try std.testing.expect(!restored.hasScanDocument());
    try std.testing.expectEqual(@as(u64, 3), restored.completedSequence());
    const legacy = try restored.encode(a);
    defer a.free(legacy);
    var roundtrip = try State.decode(a, legacy);
    defer roundtrip.deinit(a);
    try std.testing.expect(!roundtrip.hasScanDocument());
}
