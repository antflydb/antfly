// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

/// Allocation-free global/category warning budget. Repeated failed requests or
/// durable recovery slices emit at most one diagnostic per thirty seconds.
pub const Gate = struct {
    next_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),

    pub fn admit(self: *@This(), now_ns: u64) bool {
        const next = self.next_ns.load(.monotonic);
        if (now_ns < next) return false;
        return self.next_ns.cmpxchgStrong(next, now_ns +| 30 * std.time.ns_per_s, .monotonic, .monotonic) == null;
    }
};
