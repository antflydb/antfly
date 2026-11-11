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

const search_types = @import("antfly_vectorindex").search_types;

/// Borrowed by each loader call; owned by one search, never shared across queries.
pub const Session = struct {
    cancellation: ?search_types.CancellationToken = null,
    deadline_ns: ?u64 = null,
    budget_ns: u64 = 0,
    elapsed_ns: u64 = 0,
    state: ?*anyopaque = null,
    release: ?*const fn (*anyopaque) void = null,

    pub fn deinit(self: *Session) void {
        if (self.state) |state| self.release.?(state);
        self.state = null;
    }

    pub fn checkDispatch(self: *const Session, elapsed_ns: u64) !void {
        try self.check();
        if (self.budget_ns != 0 and elapsed_ns >= self.budget_ns) return error.ReembedBudgetExceeded;
    }

    pub fn beginDispatch(self: *const Session) !u64 {
        try self.checkDispatch(self.elapsed_ns);
        return @import("antfly_platform").time.monotonicNs();
    }

    pub fn finishDispatch(self: *Session, started: u64) void {
        self.elapsed_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
    }

    pub fn check(self: *const Session) !void {
        if (self.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
    }
};
