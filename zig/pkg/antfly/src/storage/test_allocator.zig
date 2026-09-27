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

//! Leak-checked fixture allocations with opt-in ownership backtraces.
const std = @import("std");
const platform = @import("antfly_platform");

/// Keep leak and ownership checks in large fixtures without collecting a stack
/// trace on every allocation/free. Opt in to traces when diagnosing a failure.
pub const TestAllocator = struct {
    state: std.heap.DebugAllocator(.{ .stack_trace_frames = 0, .resize_stack_traces = false }) = .init,

    pub fn allocator(self: *TestAllocator) std.mem.Allocator {
        return if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else self.state.allocator();
    }

    pub fn deinit(self: *TestAllocator) void {
        std.debug.assert(self.state.deinit() == .ok);
    }
};
