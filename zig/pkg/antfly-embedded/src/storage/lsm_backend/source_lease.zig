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

const std = @import("std");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const SegmentSource = @import("../../segment_source.zig").Source;

pub const Lease = struct {
    allocator: Allocator,
    refs: platform.atomic.Value(usize) = .init(1),
    source: SegmentSource,
    pub fn retainOwner(self: *@This()) *@This() {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    /// Convert a held metadata reference to an active provider use outside
    /// cache locks. Failure leaves the held reference for releaseOwner.
    pub fn activateHeld(self: *@This()) bool {
        return self.source.acquireUse();
    }
    pub fn retain(self: *@This()) ?*@This() {
        if (!self.source.acquireUse()) return null;
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *@This()) void {
        self.source.releaseUse();
        self.releaseOwner();
    }
    /// Cache ownership does not count as active provider use.
    pub fn releaseOwner(self: *@This()) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            self.source.close();
            self.allocator.destroy(self);
        }
    }
};
