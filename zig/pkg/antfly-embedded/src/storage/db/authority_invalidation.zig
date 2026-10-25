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

//! Publication-driven cohort invalidation. Sources and intrusive watches live
//! in their owners, so shared storage archives observe the same state without
//! relying on duplicate module globals. Publication is rare; reads are O(1).
const std = @import("std");
pub const Cohort = struct {
    revision: @import("antfly_platform").atomic.Value(u64) = .init(0),
    writers: std.atomic.Value(usize) = .init(0),
    pub fn stable(self: *const Cohort) ?u64 {
        const before = self.revision.load(.acquire);
        if (self.writers.load(.acquire) != 0) return null;
        return if (self.revision.load(.acquire) == before) before else null;
    }
};
pub const Watch = struct {
    source: ?*Source = null,
    cohort: *Cohort = undefined,
    previous: ?*Watch = null,
    next: ?*Watch = null,
    /// Watch and cohort addresses must stay stable until detach completes.
    pub fn attach(self: *Watch, source: *Source, cohort: *Cohort) void {
        source.lock();
        defer source.mutex.unlock();
        std.debug.assert(self.source == null);
        self.* = .{ .source = source, .cohort = cohort, .next = source.head };
        if (source.head) |head| head.previous = self;
        source.head = self;
    }
    pub fn detach(self: *Watch) void {
        const source = self.source orelse return;
        source.lock();
        defer source.mutex.unlock();
        if (self.previous) |prior| prior.next = self.next else source.head = self.next;
        if (self.next) |next| next.previous = self.previous;
        self.* = .{};
    }
};
pub const Source = struct {
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Watch = null,
    fn lock(self: *Source) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub const Publication = struct {
        source: *Source,
        pub fn finish(self: Publication) void {
            var current = self.source.head;
            while (current) |watch| : (current = watch.next) {
                _ = watch.cohort.revision.fetchAdd(1, .acq_rel);
                _ = watch.cohort.writers.fetchSub(1, .acq_rel);
            }
            self.source.mutex.unlock();
        }
    };
    /// Hold through the state change. Registration and removal cannot race
    /// either notification, and an in-flight publication never licenses reuse.
    pub fn begin(self: *Source) Publication {
        self.lock();
        var current = self.head;
        while (current) |watch| : (current = watch.next) {
            _ = watch.cohort.writers.fetchAdd(1, .acq_rel);
            _ = watch.cohort.revision.fetchAdd(1, .acq_rel);
        }
        return .{ .source = self };
    }
};

test "authority cohorts invalidate overlapping publications and detached watches" {
    var first: Source = .{};
    var second: Source = .{};
    var cohort: Cohort = .{};
    var other: Cohort = .{};
    var a: Watch = .{};
    var b: Watch = .{};
    var c: Watch = .{};
    a.attach(&first, &cohort);
    defer a.detach();
    b.attach(&second, &cohort);
    defer b.detach();
    c.attach(&first, &other);
    const prior = cohort.stable().?;
    const write_a = first.begin();
    const write_b = second.begin();
    try std.testing.expect(cohort.stable() == null);
    try std.testing.expect(other.stable() == null);
    write_a.finish();
    try std.testing.expect(cohort.stable() == null);
    try std.testing.expect(other.stable() != null);
    c.detach();
    const detached = other.stable();
    write_b.finish();
    try std.testing.expect(cohort.stable().? != prior);
    const next = first.begin();
    next.finish();
    try std.testing.expectEqual(detached, other.stable());
}

test "authority cohorts balance registration racing publication" {
    var source: Source = .{};
    const Writer = struct {
        fn run(target: *Source) void {
            for (0..2000) |_| {
                const publication = target.begin();
                publication.finish();
            }
        }
    };
    var writer = try std.testing.io.concurrent(Writer.run, .{&source});
    defer writer.await(std.testing.io);
    for (0..2000) |_| {
        var cohort: Cohort = .{};
        var watch: Watch = .{};
        watch.attach(&source, &cohort);
        watch.detach();
        // detach waits for both notifications of any publication it observed.
        try std.testing.expectEqual(@as(usize, 0), cohort.writers.load(.acquire));
        try std.testing.expect(cohort.stable() != null);
    }
}
