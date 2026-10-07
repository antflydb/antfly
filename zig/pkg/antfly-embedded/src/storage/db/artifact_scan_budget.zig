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

//! Shared budget across logical-head, legacy-row and output-tail cursors.
const std = @import("std");
pub const Budget = struct {
    max_visits: usize = 128,
    max_bytes: usize = 64 * 1024,
    deadline_ns: ?u64 = null,
    visits: usize = 0,
    bytes: usize = 0,
    pub fn exhausted(self: Budget) bool {
        // Permit one forward step even with an expired deadline or a row
        // larger than the page byte budget. Never starve oversized members.
        if (self.visits == 0) return false;
        return self.visits >= self.max_visits or self.bytes >= self.max_bytes or
            (if (self.deadline_ns) |deadline| @import("antfly_platform").time.monotonicNs() >= deadline else false);
    }
    pub fn visit(self: *Budget, bytes: usize) void {
        self.visits +|= 1;
        self.bytes +|= bytes;
    }
};
