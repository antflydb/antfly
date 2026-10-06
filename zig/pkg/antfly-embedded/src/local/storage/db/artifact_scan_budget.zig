// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
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
