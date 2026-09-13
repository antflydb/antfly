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

//! Bounded, aging frequency sketch for scan-resistant schema-cache admission.
//! One-pass or cyclic scans must not evict every reusable compiled epoch. Ties
//! favor a resident entry; aging lets a changed working set displace old plans.
const std = @import("std");

pub const Admission = struct {
    counters: [4][256]u8 = .{.{0} ** 256} ** 4,
    samples: u16 = 0,

    fn hash(version: u32, lane: usize) usize {
        var value: u64 = @as(u64, version) +% (@as(u64, lane) *% 0x9e3779b97f4a7c15);
        value = (value ^ (value >> 30)) *% 0xbf58476d1ce4e5b9;
        value = (value ^ (value >> 27)) *% 0x94d049bb133111eb;
        return @intCast((value ^ (value >> 31)) & 255);
    }

    pub fn record(self: *Admission, version: u32) void {
        self.samples += 1;
        if (self.samples == 4096) {
            for (&self.counters) |*lane| for (lane) |*counter| {
                counter.* >>= 1;
            };
            self.samples = 0;
        }
        for (&self.counters, 0..) |*lane, index| lane[hash(version, index)] +|= 1;
    }

    pub fn frequency(self: *const Admission, version: u32) u8 {
        var result: u8 = 255;
        for (self.counters, 0..) |lane, index| result = @min(result, lane[hash(version, index)]);
        return result;
    }

    pub fn admits(self: *const Admission, incoming: u32, resident: u32) bool {
        return self.frequency(incoming) > self.frequency(resident);
    }
};

test "schema admission resists cyclic scans and adapts to a new working set" {
    var admission: Admission = .{};
    var resident: [32]u32 = undefined;
    for (&resident, 0..) |*version, index| version.* = @intCast(index);
    var misses: usize = 0;
    for (0..3300) |index| {
        const version: u32 = @intCast(index % 33);
        admission.record(version);
        if (std.mem.indexOfScalar(u32, &resident, version) != null) continue;
        misses += 1;
        var victim: usize = 0;
        for (resident, 0..) |candidate, i| if (admission.frequency(candidate) < admission.frequency(resident[victim])) {
            victim = i;
        };
        if (admission.admits(version, resident[victim])) resident[victim] = version;
    }
    try std.testing.expect(misses < 200);
    for (0..8192) |_| admission.record(1000);
    try std.testing.expect(admission.admits(1000, resident[0]));
}
