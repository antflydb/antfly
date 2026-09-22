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

//! Resident scheduling state only; durable index progress remains authoritative.
//! One catalog-bound sweep can span arbitrary time/entry-limited slices. Wake
//! tickets prevent a concurrent repair notification from being cleared by an
//! older clean sweep. Restore workers use their own cursor, never this proof.
const std = @import("std");
const Head = @import("relational_index_catalog.zig").Head;

pub const Sweep = struct {
    running: std.atomic.Value(bool) = .init(false),
    requested: std.atomic.Value(u64) = .init(0),
    observed: std.atomic.Value(u64) = .init(0),
    pending: std.atomic.Value(bool) = .init(false),
    head: ?Head = null,
    namespace: u64 = 0,
    ticket: u64 = 0,
    slice_ticket: u64 = 0,
    cursor: usize = 0,
    count: usize = 0,
    remaining: usize = 0,
    did_work: bool = false,

    pub fn request(self: *Sweep) void {
        _ = self.requested.fetchAdd(1, .release);
    }

    pub fn isPending(self: *const Sweep) bool {
        return self.pending.load(.acquire) or self.requested.load(.acquire) != self.observed.load(.acquire);
    }

    pub fn enter(self: *Sweep) bool {
        if (self.running.swap(true, .acquire)) return false;
        self.slice_ticket = self.requested.load(.acquire);
        return true;
    }

    pub fn leave(self: *Sweep) void {
        self.running.store(false, .release);
    }

    pub fn begin(self: *Sweep, head: Head, namespace: u64, count: usize) void {
        const changed = self.head == null or !self.head.?.eql(head) or self.namespace != namespace or self.count != count;
        if (changed or self.ticket != self.slice_ticket or self.remaining == 0) {
            self.head = head;
            self.namespace = namespace;
            self.ticket = self.slice_ticket;
            // Repeated wakeups must not starve later indexes. Only a changed
            // catalog/namespace invalidates the ordinal; a new proof can start
            // anywhere in the same round-robin ring.
            if (changed) self.cursor = 0;
            self.count = count;
            self.remaining = count;
            self.did_work = false;
        }
    }

    pub fn next(self: *Sweep) ?usize {
        if (self.remaining == 0) return null;
        const index = self.cursor;
        self.cursor = (self.cursor + 1) % self.count;
        self.remaining -= 1;
        return index;
    }

    pub fn finish(self: *Sweep, work: bool) void {
        self.did_work = self.did_work or work;
        self.pending.store(self.remaining != 0 or self.did_work, .release);
        self.observed.store(self.slice_ticket, .release);
    }

    pub fn empty(self: *Sweep, work: bool) void {
        self.head = null;
        self.remaining = 0;
        self.did_work = false;
        self.finish(work);
    }

    pub fn failed(self: *Sweep) void {
        // Restart the proof after backoff; a failed probe is never clean work.
        self.remaining = 0;
        self.pending.store(true, .release);
    }
};

test "relational index system maintenance sweep spans slices and retains concurrent wakeups" {
    var sweep: Sweep = .{};
    var head: Head = .{ .revision = 1, .schema_version = 1, .blob_digest = @splat(1) };
    for (0..33) |i| {
        try std.testing.expect(sweep.enter());
        try std.testing.expect(!sweep.enter());
        sweep.begin(head, 1, 33);
        try std.testing.expectEqual(i, sweep.next().?);
        sweep.finish(false);
        sweep.leave();
        try std.testing.expectEqual(i != 32, sweep.isPending());
    }
    try std.testing.expect(sweep.enter());
    sweep.begin(head, 1, 1);
    _ = sweep.next();
    sweep.request(); // Arrives after the last READY observation.
    sweep.finish(false);
    sweep.leave();
    try std.testing.expect(sweep.isPending());
    try std.testing.expect(sweep.enter());
    sweep.begin(head, 1, 1);
    try std.testing.expectEqual(@as(usize, 0), sweep.next().?);
    sweep.finish(true);
    sweep.leave();
    try std.testing.expect(sweep.isPending());
    try std.testing.expect(sweep.enter());
    sweep.begin(head, 1, 33);
    _ = sweep.next();
    head.revision += 1;
    sweep.begin(head, 1, 33);
    try std.testing.expectEqual(@as(usize, 0), sweep.next().?);
    sweep.begin(head, 2, 33);
    try std.testing.expectEqual(@as(usize, 0), sweep.next().?);
    sweep.failed();
    sweep.begin(head, 2, 33);
    try std.testing.expectEqual(@as(usize, 1), sweep.next().?);
    sweep.request();
    sweep.finish(false);
    sweep.leave();
    try std.testing.expect(sweep.enter());
    sweep.begin(head, 2, 33);
    try std.testing.expectEqual(@as(usize, 2), sweep.next().?);
    sweep.empty(false);
    sweep.leave();
    try std.testing.expect(!sweep.isPending());
}
