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
//! - Lite connections mark an uncertain runtime and reconcile it by reopening
//!   on their next call; a status query enters the handle the same way and
//!   refuses while the runtime still needs reconciliation.
//! - Directory (LSM) handles have no such flag: a WAL record may be durable
//!   but unpublished until the handle is reopened. Their receipts stay pending
//!   in a process-wide registry until the issuing handle closes.

const std = @import("std");
const apply_receipts = @import("../storage/db/apply_receipts.zig");

pub const Marker = apply_receipts.OutcomeMarker;
pub const writer_len = apply_receipts.outcome_writer_len;

/// Text form: `or1:<32 hex writer>:<decimal sequence>`.
pub const text_prefix = "or1:";
pub const max_text_len = text_prefix.len + 2 * writer_len + 1 + 20;

pub const Writer = struct {
    mutex: std.atomic.Mutex = .unlocked,
    id: ?[writer_len]u8 = null,
    sequence: u64 = 0,
    /// The current writer's marker may exist on disk (a tracked write ran).
    marker_written: bool = false,
    /// Directory handles: unknown receipts stay pending until close.
    pending_until_close: bool = false,

    fn lock(self: *Writer) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// The marker for the next tracked call.
    pub fn next(self: *Writer) Marker {
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
        self.marker_written = true;
        return .{ .writer = id, .sequence = self.sequence };
    }

    /// The call holding `marker` ended with an unknown outcome: publish its
    /// receipt and retire the writer so its marker never moves again.
    pub fn retire(self: *Writer, marker: Marker) void {
        self.lock();
        const pending = self.pending_until_close;
        if (self.id) |id| if (std.mem.eql(u8, &id, &marker.writer)) {
            self.id = null;
            self.marker_written = false;
        };
        self.mutex.unlock();
        last_receipt = marker;
        if (pending) registerPending(marker.writer, self);
    }

    /// The writer whose marker should be removed at a clean close, if any.
    pub fn takeCloseCleanup(self: *Writer) ?[writer_len]u8 {
        self.lock();
        defer self.mutex.unlock();
        if (!self.marker_written) return null;
        self.marker_written = false;
        return self.id;
    }

    /// The issuing handle is closing: its uncertain commits are settled by
    /// the next open, so its receipts stop being pending.
    pub fn deinit(self: *Writer) void {
        releasePending(self);
    }
};

/// The writer of the exported call running on this thread, if tracked.
threadlocal var active: ?*Writer = null;
/// Receipt of this thread's most recent unknown outcome, if it had one.
threadlocal var last_receipt: ?Marker = null;

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

pub fn lastReceipt() ?Marker {
    return last_receipt;
}

pub fn format(marker: Marker, buf: *[max_text_len]u8) []const u8 {
    return std.fmt.bufPrint(buf, "{s}{x}:{d}", .{ text_prefix, &marker.writer, marker.sequence }) catch unreachable;
}

pub fn parse(text: []const u8) !Marker {
    if (!std.mem.startsWith(u8, text, text_prefix)) return error.InvalidOutcomeReceipt;
    const rest = text[text_prefix.len..];
    if (rest.len < 2 * writer_len + 2 or rest[2 * writer_len] != ':') return error.InvalidOutcomeReceipt;
    var marker: Marker = .{ .writer = undefined, .sequence = 0 };
    _ = std.fmt.hexToBytes(&marker.writer, rest[0 .. 2 * writer_len]) catch return error.InvalidOutcomeReceipt;
    marker.sequence = std.fmt.parseInt(u64, rest[2 * writer_len + 1 ..], 10) catch return error.InvalidOutcomeReceipt;
    if (marker.sequence == 0) return error.InvalidOutcomeReceipt;
    return marker;
}

// ------------------------------------------------ pending registry

const Pending = struct { writer: [writer_len]u8, owner: *Writer };

var registry_mutex: std.atomic.Mutex = .unlocked;
var registry: std.ArrayListUnmanaged(Pending) = .empty;

fn lockRegistry() void {
    while (!registry_mutex.tryLock()) std.atomic.spinLoopHint();
}

fn registerPending(writer: [writer_len]u8, owner: *Writer) void {
    lockRegistry();
    defer registry_mutex.unlock();
    // Best effort: without memory the receipt reads settled one call early,
    // which the caller sees as a not-yet-reopened handle's current view.
    registry.append(std.heap.page_allocator, .{ .writer = writer, .owner = owner }) catch {};
}

fn releasePending(owner: *Writer) void {
    lockRegistry();
    defer registry_mutex.unlock();
    var i: usize = 0;
    while (i < registry.items.len) {
        if (registry.items[i].owner == owner) {
            _ = registry.swapRemove(i);
        } else i += 1;
    }
    if (registry.items.len == 0) {
        registry.deinit(std.heap.page_allocator);
        registry = .empty;
    }
}

/// True while the handle that issued this receipt is still open with a
/// possibly unpublished durable record (directory handles).
pub fn isPending(writer: [writer_len]u8) bool {
    lockRegistry();
    defer registry_mutex.unlock();
    for (registry.items) |entry| if (std.mem.eql(u8, &entry.writer, &writer)) return true;
    return false;
}

test "capi SQL outcome receipt text round-trips and rejects malformed input" {
    var buf: [max_text_len]u8 = undefined;
    const marker: Marker = .{ .writer = @splat(0x5a), .sequence = 42 };
    const text = format(marker, &buf);
    try std.testing.expectEqualStrings("or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:42", text);
    const parsed = try parse(text);
    try std.testing.expectEqualSlices(u8, &marker.writer, &parsed.writer);
    try std.testing.expectEqual(marker.sequence, parsed.sequence);
    for ([_][]const u8{ "", "or1:", "or2:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1", "or1:5a5a:1", "or1:zz5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:1", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:0", "or1:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a:x" }) |bad| {
        try std.testing.expectError(error.InvalidOutcomeReceipt, parse(bad));
    }
}

test "capi SQL outcome receipt writer retires after an unknown outcome and tracks pending" {
    var writer: Writer = .{ .pending_until_close = true };
    defer writer.deinit();
    const first = writer.next();
    const second = writer.next();
    try std.testing.expectEqualSlices(u8, &first.writer, &second.writer);
    try std.testing.expectEqual(first.sequence + 1, second.sequence);
    writer.retire(second);
    try std.testing.expectEqual(second.sequence, lastReceipt().?.sequence);
    try std.testing.expect(isPending(second.writer));
    // A retired writer is never reused.
    const third = writer.next();
    try std.testing.expect(!std.mem.eql(u8, &third.writer, &second.writer));
    try std.testing.expectEqual(@as(u64, 1), third.sequence);
    writer.deinit();
    try std.testing.expect(!isPending(second.writer));
    clearLast();
}
