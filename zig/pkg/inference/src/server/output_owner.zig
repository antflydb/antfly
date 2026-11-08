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

//! Reference-counted admission for outputs shared by independent consumers.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");

/// Allocate with `std.heap.smp_allocator` and count every producer or consumer
/// reference. The final release retires the lease and destroys the owner.
pub const OutputOwner = struct {
    lease: memory.AdmissionLease,
    remaining: std.atomic.Value(usize),
    pub fn release(self: *@This()) void {
        if (self.remaining.fetchSub(1, .acq_rel) == 1) {
            self.lease.release();
            std.heap.smp_allocator.destroy(self);
        }
    }
};
