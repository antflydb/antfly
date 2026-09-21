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

const std = @import("std");

/// Allocation-free global/category warning budget. Repeated failed requests or
/// durable recovery slices emit at most one diagnostic per thirty seconds.
pub const Gate = struct {
    next_ns: std.atomic.Value(u64) = .init(0),

    pub fn admit(self: *@This(), now_ns: u64) bool {
        const next = self.next_ns.load(.monotonic);
        if (now_ns < next) return false;
        return self.next_ns.cmpxchgStrong(next, now_ns +| 30 * std.time.ns_per_s, .monotonic, .monotonic) == null;
    }
};
