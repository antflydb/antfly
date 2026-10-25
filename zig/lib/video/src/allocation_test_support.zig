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

//! Checked allocation-failure campaigns with optional successful-allocation traces.
const std = @import("std");

pub fn checkAllAllocationFailures(comptime run: anytype, args: anytype) !void {
    // Keep exhaustive failure injection and SafeAllocator's leak, mismatch,
    // double-free and write-after-free checks. Capturing successful allocation
    // stacks in every replay dominates these repeated campaigns on Darwin.
    // FailingAllocator still retains the stack of the injected failure.
    var safe: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0, .check_write_after_free = true });
    defer std.debug.assert(safe.deinit() == 0);
    const traces = try std.testing.environ.containsUnempty(std.testing.allocator, "ANTFLY_TEST_ALLOCATOR_TRACES");
    try std.testing.checkAllAllocationFailures(if (traces) std.testing.allocator else safe.allocator(), run, args);
}
