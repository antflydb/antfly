// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Borrowed synchronous lifetime authority. The callback must not suspend,
/// acquire CPU, or outlive the caller. Scoring may invoke it under index locks.
/// Its owner retains the original clock, cancellation and shutdown state.
pub const Authority = struct {
    context: *const anyopaque,
    check_fn: *const fn (*const anyopaque) anyerror!void,

    pub fn check(self: Authority) !void {
        try self.check_fn(self.context);
    }
};
