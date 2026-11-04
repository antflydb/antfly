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

//! Outcome receipts for exported writes that end with ANTFLY_OUTCOME_UNKNOWN.
//!
//! Plain autocommit SQL and the batch exports have no transaction identity a
//! caller could look up after an uncertain commit. Each public handle owns a
//! writer: a random 16-byte id and a call sequence. Every tracked write commits
//! the marker `(writer -> sequence)` atomically with its primary write (see
//! `storage/db/apply_receipts.zig`). When a call's outcome is unknown, the
//! caller gets the receipt `(writer, sequence)` and the handle retires that
//! writer, so its marker never moves again. After the store has reconciled the
//! uncertain commit, the marker answers exactly: equal means applied, lower or
//! absent means not applied.
//!
//! Reconciliation:
//! - Lite connections keep an uncertain owner fenced. A fresh database handle
//!   reconstructs durable state; a status query on the old owner refuses.
//! - Directory (LSM) handles have no such flag: a WAL record may be durable
//!   but unpublished until the handle is reopened. Their receipts stay pending
//!   in a process-wide registry until the issuing handle closes.

const std = @import("std");
const apply_receipts = @import("../storage/db/apply_receipts.zig");

pub const Marker = apply_receipts.OutcomeMarker;
pub const writer_len = apply_receipts.outcome_writer_len;

/// Text form: `or1:<32 hex writer>:<decimal sequence>`, followed by
/// `:<hex table name>` when the marker lives in a named SQL table's database
/// rather than the handle's root (`default`) database.
pub const text_prefix = "or1:";
/// Matches the SQL table name limit (`tables.create`).
pub const max_table_len = 1024;
pub const max_text_len = text_prefix.len + 2 * writer_len + 1 + 20 + 1 + 2 * max_table_len;

/// A receipt: the marker plus the database (table) that holds it.
pub const Receipt = struct {
    marker: Marker,
    table_buf: [max_table_len]u8 = undefined,
    table_len: ?usize = null,

    pub fn init(marker: Marker, table_name: ?[]const u8) Receipt {
        var receipt: Receipt = .{ .marker = marker };
        if (table_name) |name| {
            std.debug.assert(name.len <= max_table_len);
            @memcpy(receipt.table_buf[0..name.len], name);
            receipt.table_len = name.len;
        }
        return receipt;
    }

    /// The named table holding the marker; null for the root database.
    pub fn table(self: *const Receipt) ?[]const u8 {
        return if (self.table_len) |len| self.table_buf[0..len] else null;
    }
};

/// Named tables are tracked for close-time cleanup with this allocator.
const tables_allocator = std.heap.page_allocator;

pub const Writer = struct {
    mutex: std.atomic.Mutex = .unlocked,
    id: ?[writer_len]u8 = null,
    sequence: u64 = 0,
    /// The current writer's marker may exist in the root database.
    marker_written: bool = false,
    /// Named tables whose databases may hold the current writer's marker.
    /// Best effort: a name that cannot be recorded only leaves a marker behind.
    written_tables: std.ArrayListUnmanaged([]u8) = .empty,
    /// Directory handles: unknown receipts stay pending until close.
    pending_until_close: bool = false,
    /// Receipts of this writer that could not be recorded as pending. While
    /// any exist, every receipt in the process reads as pending.
    unregistered: usize = 0,

    fn lock(self: *Writer) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// The marker for the next tracked call into `table_name` (null: the
    /// root database).
    pub fn next(self: *Writer, table_name: ?[]const u8) Marker {
        self.lock();
        defer self.mutex.unlock();
        const id = self.id orelse blk: {
            var fresh: [writer_len]u8 = undefined;
            // The process-wide threaded Io, as for the handle locks.
            std.Io.random(std.Options.debug_io, &fresh);
            self.id = fresh;
            self.sequence = 0;
            break :blk fresh;
        };
        self.sequence += 1;
        if (table_name) |name| {
            for (self.written_tables.items) |known| {
                if (std.mem.eql(u8, known, name)) break;
            } else if (tables_allocator.dupe(u8, name)) |owned| {
                self.written_tables.append(tables_allocator, owned) catch tables_allocator.free(owned);
            } else |_| {}
        } else self.marker_written = true;
        return .{ .writer = id, .sequence = self.sequence };
    }

    /// The call holding `marker` (committed to `table_name`) ended with an
    /// unknown outcome: publish its receipt and retire the writer so its
    /// marker never moves again.
    pub fn retire(self: *Writer, marker: Marker, table_name: ?[]const u8) void {
        self.lock();
        const pending = self.pending_until_close;
        if (self.id) |id| if (std.mem.eql(u8, &id, &marker.writer)) {
            self.id = null;
            // The retired writer's markers stay: the receipt needs one, and
            // the rest are a few bytes each.
            self.marker_written = false;
            self.clearTables();
        };
        self.mutex.unlock();
        last_receipt = Receipt.init(marker, table_name);
        if (pending) registerPending(marker.writer, self);
    }

    fn clearTables(self: *Writer) void {
        for (self.written_tables.items) |name| tables_allocator.free(name);
        self.written_tables.clearAndFree(tables_allocator);
    }

    pub const Cleanup = struct {
        writer: [writer_len]u8,
        root: bool,
        /// Owned; release with `deinit`.
        tables: [][]u8,

        pub fn deinit(self: *Cleanup) void {
            for (self.tables) |name| tables_allocator.free(name);
            tables_allocator.free(self.tables);
        }
    };

    /// The current writer's markers to remove at a clean close, if any.
    pub fn takeCloseCleanup(self: *Writer) ?Cleanup {
        self.lock();
        defer self.mutex.unlock();
        const id = self.id orelse return null;
        if (!self.marker_written and self.written_tables.items.len == 0) return null;
        const tables = self.written_tables.toOwnedSlice(tables_allocator) catch blk: {
            self.clearTables();
            // Zero-length: freeing it in `Cleanup.deinit` is a no-op.
            break :blk @constCast(@as([]const []u8, &.{}));
        };
        const cleanup: Cleanup = .{ .writer = id, .root = self.marker_written, .tables = tables };
        self.marker_written = false;
        return cleanup;
    }

    pub fn hasCloseCleanup(self: *Writer) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.id != null and (self.marker_written or self.written_tables.items.len != 0);
    }

    /// The issuing handle is closing: its uncertain commits are settled by
    /// the next open, so its receipts stop being pending.
    pub fn deinit(self: *Writer) void {
        releasePending(self);
        self.lock();
        defer self.mutex.unlock();
        self.clearTables();
    }
};

/// The writer of the exported call running on this thread, if tracked.
threadlocal var active: ?*Writer = null;
/// Receipt of this thread's most recent unknown outcome, if it had one.
threadlocal var last_receipt: ?Receipt = null;

pub const Scope = struct {
    previous: ?*Writer,
    pub fn end(self: Scope) void {
        active = self.previous;
    }
};

/// Track exported writes on this thread for the duration of one C call.
pub fn begin(writer: *Writer) Scope {
    const scope: Scope = .{ .previous = active };
    active = writer;
    return scope;
}

pub fn activeWriter() ?*Writer {
    return active;
}

pub fn clearLast() void {
    last_receipt = null;
}

pub fn lastReceipt() ?*const Receipt {
    return if (last_receipt) |*receipt| receipt else null;
}

pub fn format(receipt: *const Receipt, buf: *[max_text_len]u8) []const u8 {
    const marker = receipt.marker;
    if (receipt.table()) |name|
        return std.fmt.bufPrint(buf, "{s}{x}:{d}:{x}", .{ text_prefix, &marker.writer, marker.sequence, name }) catch unreachable;
    return std.fmt.bufPrint(buf, "{s}{x}:{d}", .{ text_prefix, &marker.writer, marker.sequence }) catch unreachable;
}

pub fn parse(text: []const u8) !Receipt {
    if (!std.mem.startsWith(u8, text, text_prefix)) return error.InvalidOutcomeReceipt;
    const rest = text[text_prefix.len..];
    if (rest.len < 2 * writer_len + 2 or rest[2 * writer_len] != ':') return error.InvalidOutcomeReceipt;
    var receipt: Receipt = .{ .marker = .{ .writer = undefined, .sequence = 0 } };
    _ = std.fmt.hexToBytes(&receipt.marker.writer, rest[0 .. 2 * writer_len]) catch return error.InvalidOutcomeReceipt;
    const tail = rest[2 * writer_len + 1 ..];
    const sequence_text, const table_hex = if (std.mem.indexOfScalar(u8, tail, ':')) |colon|
        .{ tail[0..colon], @as(?[]const u8, tail[colon + 1 ..]) }
    else
        .{ tail, @as(?[]const u8, null) };
    receipt.marker.sequence = std.fmt.parseInt(u64, sequence_text, 10) catch return error.InvalidOutcomeReceipt;
    if (receipt.marker.sequence == 0) return error.InvalidOutcomeReceipt;
    if (table_hex) |hex| {
        if (hex.len == 0 or hex.len % 2 != 0 or hex.len / 2 > max_table_len) return error.InvalidOutcomeReceipt;
        const name = std.fmt.hexToBytes(receipt.table_buf[0 .. hex.len / 2], hex) catch return error.InvalidOutcomeReceipt;
        receipt.table_len = name.len;
    }
    return receipt;
}

// ------------------------------------------------ pending registry

const Pending = struct { writer: [writer_len]u8, owner: *Writer };

var registry_mutex: std.atomic.Mutex = .unlocked;
var registry: std.ArrayListUnmanaged(Pending) = .empty;
var registry_allocator: std.mem.Allocator = std.heap.page_allocator;
/// Pending receipts that could not be recorded (allocation failure). A
/// receipt that is not provably settled must never read as settled, so while
/// this is nonzero every receipt reads as pending.
var unregistered_pending: usize = 0;

fn lockRegistry() void {
    while (!registry_mutex.tryLock()) std.atomic.spinLoopHint();
}

fn registerPending(writer: [writer_len]u8, owner: *Writer) void {
    lockRegistry();
    defer registry_mutex.unlock();
    registry.append(registry_allocator, .{ .writer = writer, .owner = owner }) catch {
        // Fall back conservatively without allocating: until this handle
        // closes, no receipt in the process can be reported as settled.
        owner.unregistered += 1;
        unregistered_pending += 1;
    };
}

fn releasePending(owner: *Writer) void {
    lockRegistry();
    defer registry_mutex.unlock();
    unregistered_pending -= owner.unregistered;
    owner.unregistered = 0;
    var i: usize = 0;
    while (i < registry.items.len) {
        if (registry.items[i].owner == owner) {
            _ = registry.swapRemove(i);
        } else i += 1;
    }
    if (registry.items.len == 0) {
        registry.deinit(registry_allocator);
        registry = .empty;
    }
}

/// True while the handle that issued this receipt is still open with a
/// possibly unpublished durable record (directory handles).
pub fn isPending(writer: [writer_len]u8) bool {
    lockRegistry();
    defer registry_mutex.unlock();
    if (unregistered_pending != 0) return true;
    for (registry.items) |entry| if (std.mem.eql(u8, &entry.writer, &writer)) return true;
    return false;
}

test "capi SQL outcome receipt text round-trips and rejects malformed input" {
    var buf: [max_text_len]u8 = undefined;
    const marker: Marker = .{ .writer = @splat(0x5a), .sequence = 42 };
    const root = Receipt.init(marker, null);
    const text = format(&root, &buf);
    try std.testing.expectEqualStrings("or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:42", text);
    const parsed = try parse(text);
    try std.testing.expectEqualSlices(u8, &marker.writer, &parsed.marker.writer);
    try std.testing.expectEqual(marker.sequence, parsed.marker.sequence);
    try std.testing.expectEqual(@as(?[]const u8, null), parsed.table());
    // A named table's receipt names the database that holds its marker.
    const named = Receipt.init(marker, "orders:2026");
    var named_buf: [max_text_len]u8 = undefined;
    const named_text = format(&named, &named_buf);
    try std.testing.expectEqualStrings("or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:42:6f72646572733a32303236", named_text);
    const named_parsed = try parse(named_text);
    try std.testing.expectEqualStrings("orders:2026", named_parsed.table().?);
    try std.testing.expectEqual(marker.sequence, named_parsed.marker.sequence);
    for ([_][]const u8{ "", "or1:", "or2:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1", "or1:5a5a:1", "or1:zz5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:0", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:x", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1:", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1:abc", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1:zz" }) |bad| {
        try std.testing.expectError(error.InvalidOutcomeReceipt, parse(bad));
    }
}

test "capi SQL outcome receipt writer retires after an unknown outcome and tracks pending" {
    var writer: Writer = .{ .pending_until_close = true };
    defer writer.deinit();
    const first = writer.next(null);
    const second = writer.next("orders");
    try std.testing.expectEqualSlices(u8, &first.writer, &second.writer);
    try std.testing.expectEqual(first.sequence + 1, second.sequence);
    try std.testing.expect(writer.hasCloseCleanup());
    writer.retire(second, "orders");
    try std.testing.expectEqual(second.sequence, lastReceipt().?.marker.sequence);
    try std.testing.expectEqualStrings("orders", lastReceipt().?.table().?);
    try std.testing.expect(isPending(second.writer));
    // A retired writer is never reused, and is not cleaned up at close.
    try std.testing.expect(!writer.hasCloseCleanup());
    const third = writer.next(null);
    try std.testing.expect(!std.mem.eql(u8, &third.writer, &second.writer));
    try std.testing.expectEqual(@as(u64, 1), third.sequence);
    writer.deinit();
    try std.testing.expect(!isPending(second.writer));
    clearLast();
}

test "capi SQL outcome receipt that cannot be recorded as pending never reads as settled" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const previous = registry_allocator;
    registry_allocator = failing.allocator();
    defer registry_allocator = previous;

    var writer: Writer = .{ .pending_until_close = true };
    const marker = writer.next(null);
    writer.retire(marker, null);
    // Not recorded, yet still pending: and conservatively, so is any other.
    try std.testing.expect(isPending(marker.writer));
    try std.testing.expect(isPending(@splat(0x11)));
    // Closing the issuing handle settles its commits.
    writer.deinit();
    try std.testing.expect(!isPending(marker.writer));
    try std.testing.expect(!isPending(@splat(0x11)));
    clearLast();
}
