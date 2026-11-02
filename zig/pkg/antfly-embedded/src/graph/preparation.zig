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

//! Coordinator-only graph preparation. Tokens from this borrow must stay in
//! synchronous CPU work; providers and parallel workers retain passive tokens.
const std = @import("std");
const Cancellation = @import("antfly_cancellation").CancellationToken;

pub const Checkpoint = struct {
    ptr: *anyopaque,
    check: *const fn (*anyopaque) anyerror!void,
};
pub const Control = struct {
    cancellation: Cancellation = .none,
    checkpoint: ?Checkpoint = null,
    pub fn check(self: *const Control) !void {
        try self.cancellation.check();
        if (self.checkpoint) |point| point.check(point.ptr) catch |err| {
            try self.cancellation.check();
            return err;
        };
        try self.cancellation.check();
    }
    pub fn token(self: *const Control) Cancellation {
        return .{ .ptr = self, .check_fn = checkToken };
    }
    fn checkToken(raw: *const anyopaque) !void {
        try @as(*const Control, @ptrCast(@alignCast(raw))).check();
    }
};

/// In-place heapsort keeps the existing preparation memory envelope and checks
/// authority between bounded groups of comparisons. A refused sort is owned by
/// its caller and must be discarded, never published as a partial ordering.
pub fn sort(comptime T: type, values: []T, context: anytype, comptime less: fn (@TypeOf(context), T, T) bool, cancellation: Cancellation) !void {
    try cancellation.check();
    if (values.len < 2) return;
    var checks: usize = 0;
    var start = values.len / 2;
    while (start != 0) {
        start -= 1;
        try sift(T, values, start, context, less, cancellation, &checks);
    }
    var end = values.len;
    while (end > 1) {
        end -= 1;
        std.mem.swap(T, &values[0], &values[end]);
        try sift(T, values[0..end], 0, context, less, cancellation, &checks);
    }
    try cancellation.check();
}
fn sift(comptime T: type, values: []T, initial: usize, context: anytype, comptime less: fn (@TypeOf(context), T, T) bool, cancellation: Cancellation, checks: *usize) !void {
    var root = initial;
    while (root < values.len / 2) {
        if (checks.* % 1024 == 0) try cancellation.check();
        checks.* +%= 1;
        var child = root * 2 + 1;
        if (child + 1 < values.len and less(context, values[child], values[child + 1])) child += 1;
        if (!less(context, values[root], values[child])) return;
        std.mem.swap(T, &values[root], &values[child]);
        root = child;
    }
}
