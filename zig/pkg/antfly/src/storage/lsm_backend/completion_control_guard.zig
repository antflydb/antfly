// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Retained control-owner sidecar. The original accepted BEGIN envelope stays
//! available after its document cell drains and is reused. Local installation
//! identity and durable Raft reconciliation remain separate required proofs.
const std = @import("std");
const record_codec = @import("completion_control_record.zig");
const entry_codec = @import("completion_entry.zig");
const begin = @import("completion_control_begin.zig");
const protocol = @import("../../common/completion_entry_protocol.zig");

pub const max_owners = record_codec.max_owners;
pub const max_bytes = record_codec.encoded_bytes + protocol.max_wire_bytes;
pub const filenames = [_][]const u8{
    "completion-control-0.guard", "completion-control-1.guard",
    "completion-control-2.guard", "completion-control-3.guard",
};
pub const pending_filenames = [_][]const u8{
    "completion-control-pending-0.guard", "completion-control-pending-1.guard",
    "completion-control-pending-2.guard", "completion-control-pending-3.guard",
};

/// Any published or interrupted control sidecar is a restoration obligation.
/// A missing installation must not replay its WAL through ordinary admission,
/// and a pending publication must not be interpreted as an empty owner slot.
pub fn hasAny(storage: anytype, alloc: std.mem.Allocator, root: []const u8) !bool {
    inline for (.{ filenames, pending_filenames }) |names| {
        for (names) |filename| {
            const path = try std.fs.path.join(alloc, &.{ root, filename });
            defer alloc.free(path);
            if (storage.fileSize(path)) |_| return true else |err| {
                if (err != error.FileNotFound) return err;
            }
        }
    }
    return false;
}

pub const Guard = struct {
    record: record_codec.Record,
    envelope: []const u8,

    fn validate(self: Guard) !void {
        if (self.envelope.len == 0 or self.envelope.len > protocol.max_wire_bytes)
            return error.CompletionSlotTooLarge;
        if (!protocol.looksLike(self.envelope) or self.envelope.len < protocol.magic.len)
            return error.InvalidCompletionSlot;
        if (!std.mem.eql(u8, &protocol.payloadDigest(self.envelope), &self.record.begin.digest))
            return error.CompletionSlotChecksumMismatch;
    }

    pub fn encode(self: Guard, alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        const record = try self.record.encode();
        const bytes = try alloc.alloc(u8, record.len + self.envelope.len);
        @memcpy(bytes[0..record.len], &record);
        @memcpy(bytes[record.len..], self.envelope);
        return bytes;
    }

    /// The returned envelope borrows the complete sidecar. Its canonical entry
    /// and BEGIN semantics must also be checked before installing obligations.
    pub fn decode(bytes: []const u8) !Guard {
        if (bytes.len > max_bytes) return error.CompletionSlotTooLarge;
        if (bytes.len <= record_codec.encoded_bytes) return error.InvalidCompletionSlot;
        const self: Guard = .{
            .record = try record_codec.Record.decode(bytes[0..record_codec.encoded_bytes]),
            .envelope = bytes[record_codec.encoded_bytes..],
        };
        try self.validate();
        return self;
    }

    /// A checksum only binds opaque bytes. Before restoring an obligation,
    /// prove that those bytes are the canonical fresh BEGIN described by the
    /// immutable owner, and that its installation identity still matches.
    pub fn inspectBegin(self: Guard, alloc: std.mem.Allocator) !begin.Declaration {
        try self.record.verifyOwner(self.record.authority, self.record.txn_id);
        try self.validate();
        var decoded = try entry_codec.decode(alloc, self.envelope);
        defer decoded.deinit();
        const entry = decoded.entry;
        if (entry.kind != .mutation or entry.group_id != self.record.authority.group_id or
            !std.mem.eql(u8, &entry.group_incarnation, &self.record.authority.incarnation) or
            !std.mem.eql(u8, &entry.policy_digest, &self.record.authority.policy_digest) or
            !std.mem.eql(u8, &entry.schema_catalog_digest, &self.record.authority.schema_catalog_digest) or
            entry.previous_index != self.record.begin.index - 1 or
            (entry.previous_index != 0 and entry.previous_term == 0) or
            entry.previous_term > self.record.begin.term)
            return error.InvalidCompletionSlot;
        // The envelope's txn_id is a derived physical mutation ID. The
        // logical transaction ID is carried by the BEGIN record operation.
        const declaration = try begin.inspect(entry.prepare_operations);
        if (!std.mem.eql(u8, &declaration.txn_id, &self.record.txn_id) or
            !std.meta.eql(declaration.participants, self.record.participants))
            return error.InvalidCompletionSlot;
        return declaration;
    }

    pub fn validateBegin(self: Guard, alloc: std.mem.Allocator) !void {
        _ = try self.inspectBegin(alloc);
    }
};

pub const Owned = struct {
    alloc: std.mem.Allocator,
    bytes: []u8,
    guard: Guard,
    declaration: begin.Declaration,

    pub fn deinit(self: *Owned) void {
        self.alloc.free(self.bytes);
        self.* = undefined;
    }
};

/// The caller constructs this before seeking Raft acceptance and keeps it
/// through publication. `stage` makes an immutable pending obligation durable;
/// `publish` only renames and syncs preowned paths. A failed stage or publish
/// never deletes a possibly durable owner without an authoritative log proof.
pub const Publication = struct {
    alloc: std.mem.Allocator,
    pending_path: []u8,
    final_path: []u8,
    bytes: []u8,
    staged: bool = false,

    pub fn prepare(alloc: std.mem.Allocator, root: []const u8, guard: Guard) !Publication {
        if (guard.record.slot_index >= max_owners) return error.InvalidCompletionSlot;
        try guard.validateBegin(alloc);
        const pending_path = try std.fs.path.join(alloc, &.{ root, pending_filenames[guard.record.slot_index] });
        errdefer alloc.free(pending_path);
        const final_path = try std.fs.path.join(alloc, &.{ root, filenames[guard.record.slot_index] });
        errdefer alloc.free(final_path);
        const bytes = try guard.encode(alloc);
        return .{ .alloc = alloc, .pending_path = pending_path, .final_path = final_path, .bytes = bytes };
    }

    pub fn deinit(self: *Publication) void {
        self.alloc.free(self.bytes);
        self.alloc.free(self.final_path);
        self.alloc.free(self.pending_path);
        self.* = undefined;
    }

    fn requireAbsent(storage: anytype, path: []const u8) !void {
        if (storage.fileSize(path)) |_| return error.CompletionReservationBusy else |err| {
            if (err != error.FileNotFound) return err;
        }
    }

    pub fn stage(self: *Publication, storage: anytype) !void {
        if (self.staged) return error.CompletionReservationBusy;
        try requireAbsent(storage, self.pending_path);
        try requireAbsent(storage, self.final_path);
        var writer = try storage.beginAtomicWrite(self.alloc, self.pending_path);
        var live = true;
        errdefer if (live) writer.abort();
        try writer.appendSlice(self.bytes);
        live = false;
        try writer.finish();
        self.staged = true;
    }

    pub fn publish(self: *Publication, storage: anytype) !void {
        if (!self.staged) return error.InvalidCompletionSlot;
        try requireAbsent(storage, self.final_path);
        try storage.renameAbsolute(self.pending_path, self.final_path);
        // A rename without this sync is not yet a durable publication. On any
        // failure the caller remains uncertain and startup fences both paths.
        try storage.syncParentAbsolute(self.final_path);
        self.staged = false;
    }
};

/// Startup allocation, before advertising backing. Absence is not truncation
/// authority. Corruption or a different installation never becomes an empty
/// slot and must leave the database unavailable until reconciled.
pub fn load(alloc: std.mem.Allocator, storage: anytype, root: []const u8, index: usize, authority: record_codec.Authority) !?Owned {
    if (index >= max_owners) return error.InvalidCompletionSlot;
    const pending_path = try std.fs.path.join(alloc, &.{ root, pending_filenames[index] });
    defer alloc.free(pending_path);
    if (storage.fileSize(pending_path)) |_| return error.CompletionRecoveryCapacityRequired else |err| {
        if (err != error.FileNotFound) return err;
    }
    const path = try std.fs.path.join(alloc, &.{ root, filenames[index] });
    defer alloc.free(path);
    const size = storage.fileSize(path) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    if (size > max_bytes) return error.CompletionSlotTooLarge;
    const bytes = try alloc.alloc(u8, @intCast(size));
    errdefer alloc.free(bytes);
    try storage.readFileRangeInto(alloc, path, 0, bytes);
    const guard = try Guard.decode(bytes);
    if (guard.record.slot_index != index) return error.InvalidCompletionSlot;
    try guard.record.verifyOwner(authority, guard.record.txn_id);
    const declaration = try guard.inspectBegin(alloc);
    return .{ .alloc = alloc, .bytes = bytes, .guard = guard, .declaration = declaration };
}

/// Restores every locally published control obligation from trusted
/// installation identity alone. Cross-slot aliases are corruption, not extra
/// capacity. The caller still needs durable Raft reconciliation before using
/// these owners or admitting new work.
pub const Set = struct {
    owners: [max_owners]?Owned = @splat(null),
    count: usize = 0,

    pub fn deinit(self: *Set) void {
        for (&self.owners) |*slot| if (slot.*) |*owned| owned.deinit();
        self.* = .{};
    }
};

pub fn loadAll(alloc: std.mem.Allocator, storage: anytype, root: []const u8, authority: record_codec.Authority) !Set {
    var result: Set = .{};
    errdefer result.deinit();
    for (0..max_owners) |index| {
        const owner = try load(alloc, storage, root, index, authority) orelse continue;
        for (result.owners[0..index]) |previous| if (previous) |held| {
            if (std.mem.eql(u8, &held.guard.record.txn_id, &owner.guard.record.txn_id) or
                held.guard.record.output_run_id == owner.guard.record.output_run_id)
            {
                var duplicate = owner;
                duplicate.deinit();
                return error.InvalidCompletionSlot;
            }
        };
        result.owners[index] = owner;
        result.count += 1;
    }
    return result;
}

test "workload admission completion compiler control guard retains BEGIN and immutable output ownership" {
    const alloc = std.testing.allocator;
    const envelope = protocol.magic ++ "canonical-BEGIN-fixture";
    const record: record_codec.Record = .{
        .authority = .{ .group_id = 7, .incarnation = @splat(9), .policy_digest = @splat(11), .schema_catalog_digest = @splat(13), .generation = 17 },
        .txn_id = @splat(19),
        .begin = .{ .term = 23, .index = 29, .digest = protocol.payloadDigest(envelope) },
        .participants = try record_codec.Participants.measure(&.{ "coordinator", "participant" }),
        .slot_index = 2,
        .output_run_id = 101,
    };
    const bytes = try (Guard{ .record = record, .envelope = envelope }).encode(alloc);
    defer alloc.free(bytes);
    const decoded = try Guard.decode(bytes);
    try std.testing.expectEqualDeep(record, decoded.record);
    try std.testing.expectEqualStrings(envelope, decoded.envelope);
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, Guard.decode(bytes));
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidCompletionSlot, Guard.decode(bytes[0..record_codec.encoded_bytes]));
    var wrong = record;
    wrong.begin.digest[0] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, (Guard{ .record = wrong, .envelope = envelope }).encode(std.testing.failing_allocator));
}
