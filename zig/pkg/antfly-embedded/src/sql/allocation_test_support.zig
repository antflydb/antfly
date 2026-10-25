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
pub const CheckedAllocator = @import("antfly_platform").allocator.CheckedTestAllocator;

pub fn checkAllAllocationFailures(comptime run: anytype, args: anytype) !void {
    // Keep stable failure enumeration, every failure position, and memory
    // safety checks. Only successful allocation/free backtraces are opt-in.
    var checked = try CheckedAllocator.init();
    defer checked.deinit();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(checked.allocator(), run, args);
}
