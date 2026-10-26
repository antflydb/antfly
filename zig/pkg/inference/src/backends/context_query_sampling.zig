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

//! Stable, disjoint, uniformly sampled replay query populations.
pub fn partition(position: u32) struct { held: bool, ordinal: u32 } {
    const group = position / 4;
    // Exactly one held-out position per quartet, with its phase permuted by
    // a stable hash. A fixed position % 4 split aliases periodic prompts and
    // can exclude entire token/query classes from either sample population.
    var hash = group;
    hash = (hash ^ (hash >> 16)) *% 0x7feb352d;
    hash = (hash ^ (hash >> 15)) *% 0x846ca68b;
    hash ^= hash >> 16;
    const held_phase = hash % 4;
    const phase = position % 4;
    const held = phase == held_phase;
    return .{ .held = held, .ordinal = if (held) group else group * 3 + phase - @as(u32, @intFromBool(phase > held_phase)) };
}

pub const Bounds = struct {
    first_position: u32,
    last_position: u32,
    first_ordinal: u32,
    last_ordinal: u32,

    pub fn init(start: u32, end: u32, held: bool) !Bounds {
        if (start >= end) return error.InsufficientContextQueries;
        var first = start;
        while (first < end and partition(first).held != held) : (first += 1) {}
        if (first == end) return error.InsufficientContextQueries;
        var last = end - 1;
        while (partition(last).held != held) : (last -= 1) {}
        return .{ .first_position = first, .last_position = last, .first_ordinal = partition(first).ordinal, .last_ordinal = partition(last).ordinal };
    }

    /// Select an exact, deterministic number of ordinals across the entire
    /// partition, including both endpoints. Never use the last-query ring for
    /// a reference replay: that discards most of a long selected payload.
    pub fn slot(self: Bounds, ordinal: u32, count: u32) !?u32 {
        const population = self.last_ordinal - self.first_ordinal + 1;
        if (count == 0 or count > population) return error.InsufficientContextQueries;
        if (ordinal < self.first_ordinal or ordinal > self.last_ordinal) return null;
        const relative = ordinal - self.first_ordinal;
        if (count == 1) return if (relative == (population - 1) / 2) 0 else null;
        const index = (@as(u64, relative) * (count - 1) + population - 2) / (population - 1);
        const selected = index * (population - 1) / (count - 1);
        return if (selected == relative) @intCast(index) else null;
    }
};
