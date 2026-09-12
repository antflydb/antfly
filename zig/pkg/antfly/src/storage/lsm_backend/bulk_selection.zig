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

//! Allocation-free, resumable selection over immutable logical generations.
//! Even a publication containing millions of files occupies one index entry.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const time = @import("antfly_platform").time;
pub const Policy = struct {
    mode: enum { tier, delta, window } = .tier,
    fan_in: usize = 4,
    max_bytes: u64 = 0,
    sequence: u64 = 0,
    pub fn eql(a: Policy, b: Policy) bool {
        return std.meta.eql(a, b);
    }
};
pub const Range = struct { start: usize, len: usize };
pub const Job = struct {
    directory: *const Directory,
    policy: Policy,
    candidate: usize = 0,
    end: usize = 0,
    bytes: u64 = 0,
    largest: u64 = 0,
    visits: usize = 0,
    done: bool = false,
    result: ?Range = null,

    pub fn init(directory: *const Directory, policy: Policy) Job {
        return .{ .directory = directory, .policy = policy };
    }
    pub fn step(self: *Job, credits_arg: usize, deadline: u64) bool {
        var credits = credits_arg;
        const count = self.directory.generationCount();
        while (!self.done and credits != 0 and time.monotonicNs() < deadline) {
            credits -= 1;
            self.visits += 1;
            if (self.policy.mode != .tier) {
                self.selectPrefix();
                self.done = true;
                break;
            }
            if (self.policy.fan_in < 2 or count < self.policy.fan_in or self.candidate >= count) {
                self.done = true;
                break;
            }
            const tree = self.directory.generations.root.?;
            if (self.policy.sequence != 0 and tree.at(self.candidate).sequence >= self.policy.sequence) {
                // Skip an entire fenced prefix, not every file or generation.
                self.candidate = tree.lowerBound(.{ .sequence = self.policy.sequence });
                if (self.candidate < count and tree.at(self.candidate).sequence == self.policy.sequence) self.candidate += 1;
                self.end = self.candidate;
                continue;
            }
            const generation = tree.at(self.end);
            self.end += 1;
            self.bytes +|= generation.bytes;
            self.largest = @max(self.largest, generation.bytes);
            const geometric = self.end - self.candidate >= self.policy.fan_in and
                self.bytes >= self.largest *| @as(u64, @intCast(self.policy.fan_in));
            const within = self.policy.max_bytes == 0 or self.bytes <= self.policy.max_bytes;
            if (geometric and self.largest != 0 and within) {
                const start = self.directory.generationPrefix(self.candidate).files;
                self.result = .{ .start = start, .len = self.directory.generationPrefix(self.end).files - start };
            }
            if (geometric or !within or self.end == count) {
                self.candidate += 1;
                self.end = self.candidate;
                self.bytes = 0;
                self.largest = 0;
            }
        }
        return self.done;
    }
    fn selectPrefix(self: *Job) void {
        const tree = self.directory.generations.root orelse return;
        var count: usize = 0;
        if (self.policy.mode == .window) {
            if (self.policy.sequence == 0) return;
            count = tree.lowerBound(.{ .sequence = self.policy.sequence });
            if (count < tree.count and tree.at(count).sequence == self.policy.sequence) count += 1;
        } else {
            if (self.policy.fan_in < 2) return;
            count = tree.lowerBound(.{ .sequence = tree.summary.largest_sequence });
        }
        if (count < 2) return;
        const prefix = self.directory.generationPrefix(count);
        if (self.policy.max_bytes != 0 and prefix.bytes > self.policy.max_bytes) return;
        if (self.policy.mode == .delta and prefix.bytes < prefix.largest_bytes *| @as(u64, @intCast(self.policy.fan_in))) return;
        self.result = .{ .start = 0, .len = prefix.files };
    }
};
