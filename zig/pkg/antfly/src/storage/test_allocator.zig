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
