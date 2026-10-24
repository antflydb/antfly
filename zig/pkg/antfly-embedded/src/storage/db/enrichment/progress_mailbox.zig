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

//! One copied advisory sample per runtime. Publishers never wait for the
//! coordinator or call an observer. Contention drops a report; the next report
//! replaces the pending sample. Lifecycle invalidation never waits for a writer.
const std = @import("std");

pub fn Mailbox(comptime Phase: type) type {
    return struct {
        const Self = @This();
        pub const Input = struct {
            phase: Phase,
            completed: u64 = 0,
            total: u64 = 0,
            model: []const u8 = "",
            backend: []const u8 = "",
            deadline_ns: ?u64 = null,
            now_ns: u64,
            now_ms: u64,
        };
        pub const Sample = struct {
            generation: u64,
            phase: Phase,
            completed: u64,
            total: u64,
            deadline_ns: ?u64,
            now_ns: u64,
            now_ms: u64,
            model: [256]u8 = undefined,
            model_len: usize = 0,
            backend: [32]u8 = undefined,
            backend_len: usize = 0,
        };
        mutex: std.atomic.Mutex = .unlocked,
        generation: std.atomic.Value(u64) = .init(0),
        pending: ?Sample = null,

        pub fn captureGeneration(self: *Self) u64 {
            return self.generation.load(.acquire);
        }

        pub fn publish(self: *Self, input: Input) bool {
            return self.publishInGeneration(self.captureGeneration(), input);
        }

        pub fn publishInGeneration(self: *Self, generation: u64, input: Input) bool {
            if (!self.mutex.tryLock()) return false;
            defer self.mutex.unlock();
            if (generation != self.generation.load(.acquire)) return false;
            var sample: Sample = .{ .generation = generation, .phase = input.phase, .completed = input.completed, .total = input.total, .deadline_ns = input.deadline_ns, .now_ns = input.now_ns, .now_ms = input.now_ms };
            if (self.pending) |previous| if (previous.generation == generation) {
                // Empty detail fields do not erase previously reported labels
                // or a deadline when reports coalesce before a status read.
                sample.model = previous.model;
                sample.model_len = previous.model_len;
                sample.backend = previous.backend;
                sample.backend_len = previous.backend_len;
                if (sample.deadline_ns == null) sample.deadline_ns = previous.deadline_ns;
            };
            copyLabel(&sample.model, &sample.model_len, input.model);
            copyLabel(&sample.backend, &sample.backend_len, input.backend);
            self.pending = sample;
            return true;
        }

        pub fn take(self: *Self) ?Sample {
            if (!self.mutex.tryLock()) return null;
            defer self.mutex.unlock();
            const sample = self.pending orelse return null;
            self.pending = null;
            return if (sample.generation == self.generation.load(.acquire)) sample else null;
        }

        /// Called by the owning lifecycle after old provider callbacks join.
        /// A writer already in flight can publish only an obsolete generation.
        pub fn invalidate(self: *Self) void {
            _ = self.generation.fetchAdd(1, .acq_rel);
        }
    };
}

fn copyLabel(storage: []u8, len: *usize, value: []const u8) void {
    if (value.len == 0) return;
    var end = @min(value.len, storage.len);
    if (end < value.len) while (end > 0 and value[end] & 0xc0 == 0x80) {
        end -= 1;
    };
    if (!std.unicode.utf8ValidateSlice(value[0..end])) return;
    @memcpy(storage[0..end], value[0..end]);
    len.* = end;
}

const TestMailbox = Mailbox(enum { loading, executing, publishing });

test "enrichment progress mailbox coalesces copied details without retaining borrowed labels" {
    try std.testing.expect(@sizeOf(TestMailbox) <= 512);
    var mailbox: TestMailbox = .{};
    var model: [1024]u8 = @splat('m');
    var backend: [1024]u8 = @splat('b');
    try std.testing.expect(mailbox.publish(.{ .phase = .loading, .model = &model, .backend = &backend, .deadline_ns = 100, .now_ns = 1, .now_ms = 2 }));
    @memset(&model, 'x');
    @memset(&backend, 'x');
    for (0..1000) |completed| try std.testing.expect(mailbox.publish(.{ .phase = .executing, .completed = completed, .total = 1000, .now_ns = 3, .now_ms = 4 }));
    const sample = mailbox.take().?;
    try std.testing.expectEqual(@as(u64, 999), sample.completed);
    try std.testing.expectEqual(@as(?u64, 100), sample.deadline_ns);
    try std.testing.expectEqual(@as(usize, 256), sample.model_len);
    try std.testing.expectEqual(@as(usize, 32), sample.backend_len);
    try std.testing.expect(std.mem.allEqual(u8, sample.model[0..sample.model_len], 'm'));
    try std.testing.expect(std.mem.allEqual(u8, sample.backend[0..sample.backend_len], 'b'));
    try std.testing.expect(mailbox.take() == null);
}

test "enrichment progress mailbox never waits on contention or lifecycle invalidation" {
    var mailbox: TestMailbox = .{};
    try std.testing.expect(mailbox.publish(.{ .phase = .executing, .now_ns = 1, .now_ms = 1 }));
    try std.testing.expect(mailbox.mutex.tryLock());
    try std.testing.expect(!mailbox.publish(.{ .phase = .loading, .now_ns = 2, .now_ms = 2 }));
    try std.testing.expect(mailbox.take() == null);
    mailbox.invalidate();
    mailbox.mutex.unlock();
    try std.testing.expect(mailbox.take() == null);
    try std.testing.expect(mailbox.publish(.{ .phase = .publishing, .now_ns = 3, .now_ms = 3 }));
    try std.testing.expectEqual(.publishing, mailbox.take().?.phase);
}

test "enrichment progress mailbox preserves UTF8 and clears detail across lifecycle generations" {
    var mailbox: TestMailbox = .{};
    var model: [258]u8 = @splat('a');
    @memcpy(model[255..], "\xe2\x82\xac");
    try std.testing.expect(mailbox.publish(.{ .phase = .executing, .model = &model, .backend = "\xff", .deadline_ns = 10, .now_ns = 1, .now_ms = 1 }));
    const sample = mailbox.take().?;
    try std.testing.expectEqual(@as(usize, 255), sample.model_len);
    try std.testing.expectEqual(@as(usize, 0), sample.backend_len);
    try std.testing.expect(mailbox.publish(.{ .phase = .executing, .model = "old", .deadline_ns = 10, .now_ns = 2, .now_ms = 2 }));
    const old_generation = mailbox.captureGeneration();
    mailbox.invalidate();
    try std.testing.expect(mailbox.publish(.{ .phase = .loading, .now_ns = 3, .now_ms = 3 }));
    try std.testing.expect(!mailbox.publishInGeneration(old_generation, .{ .phase = .executing, .model = "late-old", .now_ns = 2, .now_ms = 2 }));
    const fresh = mailbox.take().?;
    try std.testing.expectEqual(@as(usize, 0), fresh.model_len);
    try std.testing.expect(fresh.deadline_ns == null);
}
