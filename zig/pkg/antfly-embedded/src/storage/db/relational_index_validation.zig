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

//! Read-only verification of an unpublished, fenced index generation. This is
//! physical progress, not metadata authentication or permission to publish.
//! Primary coverage and both generation-local passes share the repair path's
//! typed projection, but never repair a mismatch while claiming validation.
const std = @import("std");
const jobs = @import("relational_index_jobs.zig");
const owner = @import("relational_index_owner_job.zig");
const claims = @import("relational_index_private_claims.zig");
const catalog = @import("relational_index_catalog.zig");
const records = @import("relational_index_records.zig");
const internal = @import("../internal_keys.zig");
const range_state = @import("range_state.zig");
const authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
const Context = @import("../../api/operation.zig").RequestContext;
const Allocator = std.mem.Allocator;
const Digest = [32]u8;
const Expected = claims.TargetExpectation;
const prefix = owner.key ++ ":generation-validation:";
const max_cursor = @import("relational_index_limits.zig").max_cursor_key_bytes;
const max_summary = 8192;
const max_record = max_cursor + max_summary + 48;
pub const max_wire_bytes = 2 * max_cursor + 64 * 1024;
pub const Phase = enum { primary, forward, ownership, ready, failed };
pub const Failure = enum { none, malformed_key, missing_primary, invalid_row, tuple_mismatch, missing_forward, invalid_payload, missing_reverse, invalid_ownership, out_of_range, count_mismatch };

fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn equal(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn nonzero(value: Digest) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn optional(txn: anytype, key_: []const u8) !?[]const u8 {
    return txn.get(key_) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn key(binding: owner.Binding) [prefix.len + 16]u8 {
    var result: [prefix.len + 16]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &binding.id);
    return result;
}
pub const Fields = struct {
    phase: Phase = .primary,
    failure: Failure = .none,
    counts: [3]u64 = @splat(0),
    chain: Digest,
    cursor_digest: Digest,
    pub fn validate(self: Fields, source_members: u64) !void {
        if (!nonzero(self.chain) or !nonzero(self.cursor_digest) or (self.phase == .failed) != (self.failure != .none)) return error.InvalidRelationalIndexValidation;
        if (self.phase != .failed) {
            for (self.counts) |count| if (count > source_members) return error.InvalidRelationalIndexValidation;
            if (self.phase != .primary and self.counts[0] != source_members) return error.InvalidRelationalIndexValidation;
            if ((self.phase == .ownership or self.phase == .ready) and self.counts[1] != source_members) return error.InvalidRelationalIndexValidation;
            if (self.phase == .ready and (self.counts[2] != source_members or !equal(self.cursor_digest, hash("")))) return error.InvalidRelationalIndexValidation;
        }
    }
};
/// Fixed response/recovery identity. Large legal forward keys stay in a
/// separately framed cursor, never in the public/private progress response.
pub const Summary = struct {
    expected: Expected,
    inventory_digest: Digest,
    source_members: u64,
    fields: Fields,
    position: Native,
    page_digest: Digest,
    pub fn validate(self: Summary) !void {
        try self.expected.validate();
        try self.fields.validate(self.source_members);
        try self.position.validate();
        if (!nonzero(self.inventory_digest) or !nonzero(self.page_digest)) return error.InvalidRelationalIndexValidation;
    }
};
pub const Loaded = struct {
    alloc: Allocator,
    summary: Summary,
    cursor: []u8,
    digest: Digest,
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.cursor);
        self.* = undefined;
    }
};
fn encodeRecord(alloc: Allocator, summary: Summary, cursor: []const u8) ![]u8 {
    try summary.validate();
    if (cursor.len > max_cursor or !equal(hash(cursor), summary.fields.cursor_digest)) return error.InvalidRelationalIndexValidation;
    const json = try std.json.Stringify.valueAlloc(alloc, summary, .{});
    defer alloc.free(json);
    if (json.len > max_summary) return error.InvalidRelationalIndexValidation;
    const bytes = try alloc.alloc(u8, 48 + json.len + cursor.len);
    @memcpy(bytes[0..8], "AIGV\x01\x00\x00\x00");
    std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(cursor.len), .little);
    @memcpy(bytes[16..][0..json.len], json);
    @memcpy(bytes[16 + json.len ..][0..cursor.len], cursor);
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    return bytes;
}
fn decodeRecord(alloc: Allocator, bytes: []const u8) !Loaded {
    if (bytes.len < 48 or bytes.len > max_record or !std.mem.eql(u8, bytes[0..8], "AIGV\x01\x00\x00\x00") or !equal(bytes[bytes.len - 32 ..][0..32].*, hash(bytes[0 .. bytes.len - 32]))) return error.InvalidRelationalIndexValidation;
    const json_len = std.mem.readInt(u32, bytes[8..12], .little);
    const cursor_len = std.mem.readInt(u32, bytes[12..16], .little);
    if (json_len > max_summary or cursor_len > max_cursor or @as(u64, json_len) + cursor_len + 48 != bytes.len) return error.InvalidRelationalIndexValidation;
    var parsed = try std.json.parseFromSlice(Summary, alloc, bytes[16..][0..json_len], .{ .ignore_unknown_fields = false, .max_value_len = 128 });
    defer parsed.deinit();
    const summary = parsed.value;
    try summary.validate();
    const canonical = try std.json.Stringify.valueAlloc(alloc, summary, .{});
    defer alloc.free(canonical);
    const raw_cursor = bytes[16 + json_len ..][0..cursor_len];
    if (!std.mem.eql(u8, canonical, bytes[16..][0..json_len]) or !equal(hash(raw_cursor), summary.fields.cursor_digest)) return error.InvalidRelationalIndexValidation;
    const digest = hash(bytes);
    const copied = try alloc.dupe(u8, raw_cursor);
    errdefer alloc.free(copied);
    return .{ .alloc = alloc, .summary = summary, .cursor = copied, .digest = digest };
}
pub fn load(alloc: Allocator, txn: anytype, binding: owner.Binding) !?Loaded {
    const bytes = (try optional(txn, &key(binding))) orelse return null;
    var loaded = try decodeRecord(alloc, bytes);
    errdefer loaded.deinit();
    try loaded.summary.expected.requireBinding(binding);
    const current = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
    if (!loaded.summary.position.namespace.eql(binding.fence.namespace) or loaded.summary.position.sequence > current.sequence) return error.InvalidRelationalIndexValidation;
    return loaded;
}
pub const Command = struct {
    binding: owner.Binding,
    expected: Expected,
    inventory_digest: Digest,
    source_members: u64,
    previous: Digest,
    scanned: u16,
    finish: bool,
    next: Fields,
    cursor_b64: []const u8,
    pub fn validate(self: Command) !void {
        try self.expected.requireBinding(self.binding);
        try self.next.validate(self.source_members);
        if (!nonzero(self.inventory_digest) or self.scanned > claims.max_claims or self.cursor_b64.len > std.base64.standard.Encoder.calcSize(max_cursor)) return error.InvalidRelationalIndexValidation;
        if (try std.base64.standard.Decoder.calcSizeForSlice(self.cursor_b64) > max_cursor) return error.InvalidRelationalIndexValidation;
    }
    pub fn digest(self: Command) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-generation-page/v1");
        state.update(&try self.binding.encode());
        state.update(&try self.expected.digest());
        inline for (.{ "inventory_digest", "previous" }) |field| state.update(&@field(self, field));
        var numbers: [35]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.source_members, .little);
        std.mem.writeInt(u16, numbers[8..10], self.scanned, .little);
        numbers[10] = @intFromBool(self.finish);
        for (self.next.counts, 0..) |count, i| std.mem.writeInt(u64, numbers[11 + i * 8 ..][0..8], count, .little);
        state.update(&numbers);
        state.update(&.{ @backingInt(self.next.phase), @backingInt(self.next.failure) });
        state.update(&self.next.chain);
        state.update(&self.next.cursor_digest);
        state.update(self.cursor_b64);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
};

/// Structural recovery only. Restore/adoption still requires a complete
/// physical rescan before this progress can become authenticated publication
/// evidence; a retained cursor is not an audit of its already-scanned prefix.
pub fn verifyRecovery(alloc: Allocator, txn: anytype, binding: owner.Binding) !void {
    var loaded = (try load(alloc, txn, binding)) orelse return;
    defer loaded.deinit();
    if (try @import("relational_index_cancellation_intent.zig").load(txn)) |closed| if (closed.intent.fence.admission_epoch == binding.fence.admission_epoch) return;
    const frozen = try claims.InventoryValidation.requireReady(alloc, txn, binding, loaded.summary.expected);
    if (!equal(frozen.digest, loaded.summary.inventory_digest) or frozen.source_members != loaded.summary.source_members or loaded.summary.position.sequence <= frozen.position.sequence) return error.InvalidRelationalIndexValidation;
}
fn cursorEnd(alloc: Allocator, value: []const u8) ![]u8 {
    var end = value.len;
    while (end != 0) {
        end -= 1;
        if (value[end] != 255) {
            const result = try alloc.dupe(u8, value[0 .. end + 1]);
            result[end] += 1;
            return result;
        }
    }
    return error.InvalidRelationalIndexValidation;
}
fn observe(chain: *Digest, key_: []const u8, value: ?[]const u8) void {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/private-index-generation-record/v1");
    state.update(chain);
    state.update(&hash(key_));
    state.update(&.{@intFromBool(value != null)});
    if (value) |bytes| state.update(&hash(bytes));
    state.final(chain);
}
fn Probe(comptime Txn: type) type {
    return struct {
        txn: Txn,
        chain: *Digest,
        bytes: usize = 0,
        observed_key: []const u8 = "",
        observed_value: []const u8 = "",
        fn get(self: *@This(), key_: []const u8) !?[]const u8 {
            if (self.observed_key.len != 0 and std.mem.eql(u8, key_, self.observed_key)) return self.observed_value;
            const value = try optional(self.txn, key_);
            self.bytes +|= key_.len +| (if (value) |raw| raw.len else @as(usize, 0));
            observe(self.chain, key_, value);
            return value;
        }
    };
}
fn verifyDocument(alloc: Allocator, core: anytype, pinned: catalog.WriteSnapshot, index: @import("relational_index_plan.zig").BoundIndex, phase: Phase, document: []const u8, projection: *jobs.RowProjection, probe: anytype, next: *Fields) !Failure {
    const primary = try internal.relationalRowKeyAlloc(alloc, document);
    const row = try probe.get(primary);
    var reverse: std.ArrayList(u8) = .empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, index.id(), document);
    var ownership_key: std.ArrayList(u8) = .empty;
    defer ownership_key.deinit(alloc);
    try records.appendOwnershipFromReverse(alloc, &ownership_key, reverse.items);
    const projected = if (row) |raw| projection.prepare(alloc, core, pinned, index, raw, document) catch |err| {
        if (jobs.classifyRowFailure(err) != null) return .invalid_row;
        return err;
    } else null;
    if (projected == null) {
        if (phase != .primary) return if (row == null) .missing_primary else .tuple_mismatch;
        if (try probe.get(reverse.items) != null) return if (row == null) .missing_primary else .tuple_mismatch;
        if (try probe.get(ownership_key.items) != null) return .invalid_ownership;
        return .none;
    }
    const value = projected.?;
    if (phase == .primary) next.counts[0] = std.math.add(u64, next.counts[0], 1) catch return error.InvalidRelationalIndexValidation;
    var reverse_value: std.ArrayList(u8) = .empty;
    defer reverse_value.deinit(alloc);
    try records.appendReverseValue(alloc, &reverse_value, reverse.items, value.tuple);
    var forward: std.ArrayList(u8) = .empty;
    defer forward.deinit(alloc);
    try records.appendForwardFromReverse(alloc, &forward, reverse.items, reverse_value.items);
    if (phase == .forward and !std.mem.eql(u8, probe.observed_key, forward.items)) return .tuple_mismatch;
    if (phase == .ownership and !std.mem.eql(u8, probe.observed_key, ownership_key.items)) return .invalid_ownership;
    const actual_reverse = (try probe.get(reverse.items)) orelse return .missing_reverse;
    if (!std.mem.eql(u8, actual_reverse, reverse_value.items)) return .tuple_mismatch;
    const actual_forward = (try probe.get(forward.items)) orelse return .missing_forward;
    if ((value.payload.len == 0 and actual_forward.len != 0) or (value.payload.len != 0 and actual_forward.len != value.payload.len + 4)) return .invalid_payload;
    const payload = records.forwardPayload(forward.items, actual_forward) catch return .invalid_payload;
    if (!std.mem.eql(u8, payload, value.payload)) return .invalid_payload;
    const actual_ownership = (try probe.get(ownership_key.items)) orelse return .invalid_ownership;
    if (actual_ownership.len != 0) return .invalid_ownership;
    return .none;
}
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    command: Command,
    context: Context,
    namespace_generation: u64,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn prepare(alloc: Allocator, core: anytype, binding: owner.Binding, expected: Expected, addition: *const catalog.PrivateAddition, budget: jobs.Budget, context: Context) !?Prepared {
        return prepareImpl(alloc, core, binding, expected, addition, budget, context, null);
    }
    pub fn replay(alloc: Allocator, core: anytype, addition: *const catalog.PrivateAddition, command: Command) !Prepared {
        try command.validate();
        var result = (try prepareImpl(alloc, core, command.binding, command.expected, addition, .{ .records = @max(1, command.scanned), .bytes = 16 * 1024 * 1024, .time_ns = std.time.ns_per_s }, .{}, command)) orelse return error.InvalidRelationalIndexValidation;
        errdefer result.deinit();
        if (!equal(try result.command.digest(), try command.digest())) return error.InvalidRelationalIndexValidation;
        return result;
    }
    fn prepareImpl(alloc: Allocator, core: anytype, binding: owner.Binding, expected: Expected, addition: *const catalog.PrivateAddition, budget: jobs.Budget, context: Context, replay_command: ?Command) !?Prepared {
        try context.ensureActive();
        try budget.validate();
        try expected.requireBinding(binding);
        if (!addition.candidate.head.eql(binding.physical_candidate) or !addition.reservation.head.eql(binding.physical_reserved) or addition.index.mapKey() != binding.index.mapKey()) return error.PrivateIndexOwnerJobChanged;
        const namespace_generation = core.schemaNamespaceGeneration();
        const pinned: catalog.WriteSnapshot = .{ .head = addition.candidate.head, .plan = addition.plan.clone() };
        defer {
            var copy = pinned;
            copy.deinit();
        }
        const index = for (pinned.plan.boundIndexes()) |value| {
            if (value.id().mapKey() == binding.index.mapKey()) break value;
        } else return error.IndexNotFound;
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const frozen = try claims.InventoryValidation.requireReady(alloc, &read, binding, expected);
        var loaded = try load(alloc, &read, binding);
        defer if (loaded) |*value| value.deinit();
        if (loaded) |value| {
            if (!equal(try value.summary.expected.digest(), try expected.digest()) or !equal(value.summary.inventory_digest, frozen.digest) or value.summary.source_members != frozen.source_members or value.summary.position.sequence <= frozen.position.sequence) return error.PrivateIndexOwnerJobChanged;
        }
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        if (replay_command) |command| if (loaded) |value| if (equal(value.summary.page_digest, try command.digest())) {
            var owned = command;
            owned.cursor_b64 = try a.dupe(u8, command.cursor_b64);
            return .{ .arena = arena, .command = owned, .context = context, .namespace_generation = namespace_generation };
        };
        if (loaded) |value| if (value.summary.fields.phase == .ready or value.summary.fields.phase == .failed) {
            arena.deinit();
            return null;
        };
        const previous = if (loaded) |value| value.digest else @as(Digest, @splat(0));
        var next: Fields = if (loaded) |value| value.summary.fields else initial: {
            var state = std.crypto.hash.Blake3.init(.{});
            state.update("antfly/private-index-generation/v1");
            state.update(&try expected.digest());
            state.update(&frozen.digest);
            var chain: Digest = undefined;
            state.final(&chain);
            break :initial .{ .chain = chain, .cursor_digest = hash("") };
        };
        const phase = next.phase;
        const range = try range_state.decodeRangeAlloc(a, (try optional(&read, range_state.range_key)) orelse &@as([8]u8, @splat(0)));
        const lower: []const u8 = switch (phase) {
            .primary => try internal.documentExactPrefixAlloc(a, range.start),
            .forward => try a.dupe(u8, &try records.forwardPrefix(binding.index)),
            .ownership => try a.dupe(u8, &try records.ownershipPrefix(binding.index)),
            else => unreachable,
        };
        const upper: []const u8 = if (phase == .primary) (if (range.end.len != 0) try internal.documentExactPrefixAlloc(a, range.end) else &.{internal.user_namespace + 1}) else try cursorEnd(a, lower);
        var after: std.ArrayList(u8) = .empty;
        defer after.deinit(alloc);
        if (loaded) |value| try after.appendSlice(alloc, value.cursor);
        if (after.items.len != 0 and (std.mem.order(u8, after.items, lower) == .lt or std.mem.order(u8, after.items, upper) == .gt)) return error.InvalidRelationalIndexValidation;
        var cursor = try read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(upper);
        var row_arena = std.heap.ArenaAllocator.init(alloc);
        defer row_arena.deinit();
        var projection: jobs.RowProjection = .{ .alloc = alloc };
        defer projection.deinit();
        var scanned: u16 = 0;
        var bytes: usize = 0;
        const start = @import("antfly_platform").time.monotonicNs();
        var entry = try cursor.seekAtOrAfter(if (after.items.len == 0) lower else after.items);
        if (phase != .primary and after.items.len != 0) if (entry) |kv| if (std.mem.eql(u8, kv.key, after.items)) {
            entry = try cursor.next();
        };
        while (entry) |kv| {
            if (std.mem.order(u8, kv.key, upper) != .lt) break;
            if (replay_command) |command| if (scanned >= command.scanned) break;
            try context.ensureActive();
            _ = row_arena.reset(.retain_capacity);
            const row_alloc = row_arena.allocator();
            if (kv.key.len > max_cursor) return error.InvalidRelationalIndexValidation;
            const observed = try row_alloc.dupe(u8, kv.key);
            var probe: Probe(@TypeOf(&read)) = .{ .txn = &read, .chain = &next.chain, .bytes = observed.len };
            var document: ?[]const u8 = null;
            var continuation: []const u8 = observed;
            var failure: Failure = .none;
            if (phase == .primary) {
                if (internal.findComponentTerminator(observed, 1)) |term| {
                    continuation = try cursorEnd(row_alloc, observed[0 .. term + 2]);
                    document = try internal.decodeDocumentComponentAlloc(row_alloc, observed);
                    observe(&next.chain, observed[0 .. term + 2], null);
                } else failure = .malformed_key;
            } else {
                probe.observed_key = observed;
                probe.observed_value = try row_alloc.dupe(u8, kv.value);
                probe.bytes +|= kv.value.len;
                observe(&next.chain, observed, probe.observed_value);
                const component = if (phase == .forward) component: {
                    const parsed = records.forwardOwnership(observed) catch {
                        failure = .malformed_key;
                        break :component null;
                    };
                    break :component parsed.document_component;
                } else records.ownershipDocument(observed) catch component: {
                    failure = .malformed_key;
                    break :component null;
                };
                if (component) |value| {
                    const raw_key = try std.mem.concat(row_alloc, u8, &.{ &.{internal.user_namespace}, value });
                    document = try internal.decodeDocumentComponentAlloc(row_alloc, raw_key);
                }
                const slot: usize = if (phase == .forward) 1 else 2;
                next.counts[slot] = std.math.add(u64, next.counts[slot], 1) catch return error.InvalidRelationalIndexValidation;
            }
            if (failure == .none) {
                if (document) |value| {
                    failure = if (!range.contains(value)) .out_of_range else try verifyDocument(row_alloc, core, pinned, index, phase, value, &projection, &probe, &next);
                } else failure = .malformed_key;
            }
            after.clearRetainingCapacity();
            try after.appendSlice(alloc, continuation);
            scanned += 1;
            bytes +|= probe.bytes;
            if (failure != .none) {
                next.phase = .failed;
                next.failure = failure;
                break;
            }
            if (next.counts[@as(usize, if (phase == .primary) 0 else if (phase == .forward) 1 else 2)] > frozen.source_members) {
                next.phase = .failed;
                next.failure = .count_mismatch;
                break;
            }
            entry = if (phase == .primary) try cursor.seekAtOrAfter(after.items) else try cursor.next();
            // A byte/time budget is soft for one indivisible row, like build
            // pages. It must not make a previously admitted large row unusable.
            if (scanned >= @min(budget.records, claims.max_claims) or bytes >= budget.bytes or (replay_command == null and @import("antfly_platform").time.monotonicNs() -| start >= budget.time_ns)) break;
        }
        if (replay_command) |command| if (scanned != command.scanned) return error.InvalidRelationalIndexValidation;
        const exhausted = next.phase != .failed and (entry == null or std.mem.order(u8, entry.?.key, upper) != .lt);
        if (next.phase != .failed and exhausted and (replay_command == null or replay_command.?.finish)) {
            const slot: usize = if (phase == .primary) 0 else if (phase == .forward) 1 else 2;
            if (next.counts[slot] != frozen.source_members) {
                next.phase = .failed;
                next.failure = .count_mismatch;
            } else {
                next.phase = switch (phase) {
                    .primary => .forward,
                    .forward => .ownership,
                    .ownership => .ready,
                    else => unreachable,
                };
                after.clearRetainingCapacity();
            }
        }
        const finish = phase != next.phase;
        next.cursor_digest = hash(after.items);
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(after.items.len));
        _ = std.base64.standard.Encoder.encode(encoded, after.items);
        const command: Command = .{ .binding = binding, .expected = expected, .inventory_digest = frozen.digest, .source_members = frozen.source_members, .previous = previous, .scanned = scanned, .finish = finish, .next = next, .cursor_b64 = encoded };
        try command.validate();
        return .{ .arena = arena, .command = command, .context = context, .namespace_generation = namespace_generation };
    }
    /// Caller commits progress with its ordered receipt/outbox and MUST abort
    /// the whole transaction on every error, including encode/allocation failure.
    pub fn stage(self: *@This(), core: anytype, txn: anytype, replay_position: ?Native) !struct { summary: Summary, duplicate: bool } {
        try self.context.ensureActive();
        try self.command.validate();
        if (core.schemaNamespaceGeneration() != self.namespace_generation) return error.PreparedGenerationChanged;
        const frozen = try claims.InventoryValidation.requireReady(self.arena.allocator(), txn, self.command.binding, self.command.expected);
        if (!equal(frozen.digest, self.command.inventory_digest) or frozen.source_members != self.command.source_members) return error.PrivateIndexOwnerJobChanged;
        var loaded = try load(self.arena.allocator(), txn, self.command.binding);
        defer if (loaded) |*value| value.deinit();
        const digest = try self.command.digest();
        if (loaded) |value| if (equal(value.summary.page_digest, digest)) {
            if (replay_position) |position| if (!std.meta.eql(position, value.summary.position)) return error.InvalidPrivateIndexNativeReceipt;
            return .{ .summary = value.summary, .duplicate = true };
        };
        const previous = if (loaded) |value| value.digest else @as(Digest, @splat(0));
        if (!equal(previous, self.command.previous)) return error.VersionConflict;
        if (replay_position) |position| if (!position.namespace.eql(self.command.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        const a = self.arena.allocator();
        const raw_cursor = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(self.command.cursor_b64));
        try std.base64.standard.Decoder.decode(raw_cursor, self.command.cursor_b64);
        if (!equal(hash(raw_cursor), self.command.next.cursor_digest)) return error.InvalidRelationalIndexValidation;
        try self.context.ensureActive();
        const position: Native = .{ .namespace = self.command.binding.fence.namespace, .sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(self.command.binding.fence.namespace), if (replay_position) |value| value.sequence else null) };
        const summary: Summary = .{ .expected = self.command.expected, .inventory_digest = frozen.digest, .source_members = frozen.source_members, .fields = self.command.next, .position = position, .page_digest = digest };
        const bytes = try encodeRecord(a, summary, raw_cursor);
        try txn.put(&key(self.command.binding), bytes);
        return .{ .summary = summary, .duplicate = false };
    }
};

test "relational index build pages generation checkpoint keeps maximum cursors out of fixed summaries" {
    const alloc = std.testing.allocator;
    const cursor = try alloc.alloc(u8, max_cursor);
    defer alloc.free(cursor);
    @memset(cursor, 255);
    const summary: Summary = .{
        .expected = .{ .metadata_incarnation = @splat('1'), .binding_digest = @splat(1), .generation = @splat(2), .completion_revision = 1, .source_consumption_digest = @splat(3), .source_progress_digest = @splat(4), .table_name_digest = @splat(5), .rows = 0, .inventory_xor = @splat(0), .provenance = @splat(0) },
        .inventory_digest = @splat(6),
        .source_members = 0,
        .fields = .{ .chain = @splat(7), .cursor_digest = hash(cursor) },
        .position = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .sequence = 1 },
        .page_digest = @splat(8),
    };
    const bytes = try encodeRecord(alloc, summary, cursor);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len <= max_record);
    const response = try std.json.Stringify.valueAlloc(alloc, summary, .{});
    defer alloc.free(response);
    try std.testing.expect(response.len < max_summary);
    var loaded = try decodeRecord(alloc, bytes);
    defer loaded.deinit();
    try std.testing.expectEqualDeep(summary, loaded.summary);
    try std.testing.expectEqualSlices(u8, cursor, loaded.cursor);
    // A source may produce 32 local index rows while all UNIQUE claims route
    // to other owners. Physical index counts bind the consumed source rows,
    // not the independently routed target-inventory cardinality.
    var different_cardinality = summary;
    different_cardinality.source_members = 32;
    different_cardinality.fields.phase = .ready;
    different_cardinality.fields.counts = @splat(32);
    different_cardinality.fields.cursor_digest = hash("");
    try different_cardinality.validate();
    different_cardinality.fields.counts[2] -= 1;
    try std.testing.expectError(error.InvalidRelationalIndexValidation, different_cardinality.validate());
    bytes[bytes.len - 33] ^= 1;
    try std.testing.expectError(error.InvalidRelationalIndexValidation, decodeRecord(alloc, bytes));
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    try std.testing.expectError(error.InvalidRelationalIndexValidation, decodeRecord(alloc, bytes));
    bytes[bytes.len - 33] ^= 1;
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    for (0..8) |failure| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = failure });
        if (decodeRecord(failing.allocator(), bytes)) |value| {
            var owned = value;
            owned.deinit();
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
    }
}
