//! Durable registration intents are owned by the node that issues a receipt.
//! Sequence allocation and retirement share the session namespace write lock.
const std = @import("std");
const wire = @import("../pgwire/completion.zig");
const state_key = "\x00\x00__sql_completion_producer__";
const prefix = "\x00\x00__sql_completion_outbox__:";
pub const max_window = 65536;
pub const Producer = struct { id: [8]u8, next: u64 = 1, retired_through: u64 = 0, active: u64 = 0 };
pub const Record = struct {
    outcome: wire.Outcome,
    phase: enum { registering, registered, acknowledging, discarding } = .registering,
    prepared: bool = false,
    execution_admitted: bool = false,
    joined: bool = false,
};
pub const RecoveryPage = struct { references: [][16]u8, after: u64 };
// Node-local stores retain their original namespace (issuer zero). Shared
// transaction stores must bind a separate facade to each immutable issuer.
pub const Namespace = struct {
    issuer: u64 = 0,
    fn stateKey(self: Namespace, buf: []u8) ![]const u8 {
        if (self.issuer == 0) return state_key;
        return std.fmt.bufPrint(buf, "{s}:{x:0>16}", .{ state_key, self.issuer });
    }
    fn entryPrefix(self: Namespace, buf: []u8) ![]const u8 {
        if (self.issuer == 0) return prefix;
        return std.fmt.bufPrint(buf, "\x00\x00__sql_completion_issuer__:{x:0>16}:", .{self.issuer});
    }
    pub fn recoveryPage(self: Namespace, txn: anytype, alloc: std.mem.Allocator, before: u64, after: u64, limit: usize) !RecoveryPage {
        var prefix_buf: [128]u8 = undefined;
        const entry_prefix = try self.entryPrefix(&prefix_buf);
        if (limit == 0) return .{ .references = try alloc.alloc([16]u8, 0), .after = after };
        const state = try self.producer(txn, alloc, @splat(0));
        var references: std.ArrayList([16]u8) = .empty;
        errdefer references.deinit(alloc);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var buf: [128]u8 = undefined;
        const lower = try std.fmt.bufPrint(&buf, "{s}{x:0>16}", .{ entry_prefix, after +| 1 });
        var row = try cursor.seekAtOrAfter(lower);
        var next: u64 = 0;
        while (row) |entry| : (row = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, entry_prefix)) break;
            const seq = try std.fmt.parseInt(u64, entry.key[entry_prefix.len..], 16);
            if (seq >= before) break;
            if (references.items.len == limit) {
                next = @import("completion_retirement.zig").sequence(references.items[references.items.len - 1]);
                break;
            }
            var reference: [16]u8 = undefined;
            reference[0..8].* = state.id;
            std.mem.writeInt(u64, reference[8..16], seq, .big);
            try references.append(alloc, reference);
        }
        return .{ .references = try references.toOwnedSlice(alloc), .after = next };
    }
    pub fn key(self: Namespace, buf: []u8, reference: [16]u8) ![]const u8 {
        var prefix_buf: [128]u8 = undefined;
        const entry_prefix = try self.entryPrefix(&prefix_buf);
        return std.fmt.bufPrint(buf, "{s}{x:0>16}", .{ entry_prefix, @import("completion_retirement.zig").sequence(reference) });
    }
    pub fn producer(self: Namespace, txn: anytype, alloc: std.mem.Allocator, seed: [8]u8) !Producer {
        var state_buf: [128]u8 = undefined;
        const value = txn.get(try self.stateKey(&state_buf)) catch |err| switch (err) {
            error.NotFound => return .{ .id = seed },
            else => return err,
        };
        var parsed = try std.json.parseFromSlice(Producer, alloc, value, .{});
        defer parsed.deinit();
        return parsed.value;
    }
    fn putProducer(self: Namespace, txn: anytype, alloc: std.mem.Allocator, value: Producer) !void {
        const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
        defer alloc.free(bytes);
        var state_buf: [128]u8 = undefined;
        try txn.put(try self.stateKey(&state_buf), bytes);
    }
    // Compute capacity with bounded point reads, without creating WAL traffic for
    // read-only admission. Sparse retired holes are reclaimed by the next mutation.
    pub fn available(self: Namespace, txn: anytype, alloc: std.mem.Allocator) !usize {
        var state = try self.producer(txn, alloc, @splat(0));
        try self.advanceFloor(txn, &state);
        if (state.next == std.math.maxInt(u64)) return 0;
        const occupied = state.next - state.retired_through - 1;
        return @intCast(@min(max_window - @min(max_window, occupied), std.math.maxInt(u64) - state.next));
    }
    pub fn create(self: Namespace, txn: anytype, alloc: std.mem.Allocator, seed: [8]u8, outcome: wire.Outcome, prepared: bool) ![16]u8 {
        var state = try self.producer(txn, alloc, seed);
        try self.advanceFloor(txn, &state);
        if (state.next == std.math.maxInt(u64) or state.next - state.retired_through > max_window) return error.SessionLimitExceeded;
        var reference: [16]u8 = undefined;
        reference[0..8].* = state.id;
        std.mem.writeInt(u64, reference[8..16], state.next, .big);
        state.next += 1;
        state.active += 1;
        try self.putProducer(txn, alloc, state);
        try self.put(txn, alloc, reference, .{ .outcome = outcome, .prepared = prepared });
        return reference;
    }
    pub fn put(self: Namespace, txn: anytype, alloc: std.mem.Allocator, reference: [16]u8, record: Record) !void {
        var buf: [128]u8 = undefined;
        const value = try std.json.Stringify.valueAlloc(alloc, record, .{});
        defer alloc.free(value);
        try txn.put(try self.key(&buf, reference), value);
    }
    pub fn load(self: Namespace, txn: anytype, alloc: std.mem.Allocator, reference: [16]u8) !?std.json.Parsed(Record) {
        const state = try self.producer(txn, alloc, @splat(0));
        // Missing entries are idempotently acknowledged only by the producer that
        // actually allocated them. A replacement/foreign store is not retirement.
        const seq = @import("completion_retirement.zig").sequence(reference);
        if (!std.mem.eql(u8, &state.id, reference[0..8]) or seq == 0 or seq >= state.next) return error.CompletionWrongOwner;
        var buf: [128]u8 = undefined;
        const value = txn.get(try self.key(&buf, reference)) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try std.json.parseFromSlice(Record, alloc, value, .{ .allocate = .alloc_always });
    }
    pub fn remove(self: Namespace, txn: anytype, alloc: std.mem.Allocator, reference: [16]u8) !void {
        var state = try self.producer(txn, alloc, @splat(0));
        if (!std.mem.eql(u8, &state.id, reference[0..8])) return error.InvalidArgument;
        var buf: [128]u8 = undefined;
        const target = try self.key(&buf, reference);
        _ = txn.get(target) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        if (state.active == 0) return error.InvalidArgument;
        try txn.delete(target);
        state.active -= 1;
        try self.advanceFloor(txn, &state);
        try self.putProducer(txn, alloc, state);
    }
    // Point lookups avoid materializing a write-cursor snapshot of the whole session
    // namespace. Bound sparse-hole collection; later mutations continue the prefix.
    fn advanceFloor(self: Namespace, txn: anytype, state: *Producer) !void {
        var prefix_buf: [128]u8 = undefined;
        const entry_prefix = try self.entryPrefix(&prefix_buf);
        if (state.active == 0) {
            state.retired_through = state.next - 1;
            return;
        }
        for (0..128) |_| {
            const candidate = state.retired_through + 1;
            if (candidate >= state.next) return;
            var buf: [128]u8 = undefined;
            const target = try std.fmt.bufPrint(&buf, "{s}{x:0>16}", .{ entry_prefix, candidate });
            _ = txn.get(target) catch |err| switch (err) {
                error.NotFound => {
                    state.retired_through = candidate;
                    continue;
                },
                else => return err,
            };
            return;
        }
    }
};
