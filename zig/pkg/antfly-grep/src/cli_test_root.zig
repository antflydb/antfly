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

//! Exercise the ordinary CLI with Debug checks and explicit leak detection,
//! without unwinding every successful allocation in large workspace fixtures.
const std = @import("std");

pub fn main(init: std.process.Init) u8 {
    var safe: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{
        .stack_trace_frames = 0,
        .check_write_after_free = true,
    });
    defer if (safe.deinit() != 0) @panic("CLI test allocator leaked");
    var actual = init;
    const traces = init.environ_map.get("ANTFLY_TEST_ALLOCATOR_TRACES") orelse "";
    const capture_traces = std.mem.eql(u8, traces, "1") or std.ascii.eqlIgnoreCase(traces, "true") or
        std.ascii.eqlIgnoreCase(traces, "yes") or std.ascii.eqlIgnoreCase(traces, "on");
    if (!capture_traces) actual.gpa = safe.allocator();
    return @import("main.zig").runMain(actual);
}
