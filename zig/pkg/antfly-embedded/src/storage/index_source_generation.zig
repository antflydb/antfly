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

//! Logical source ownership, independent of encoded posting/column storage.
//! Snapshot copies use one atomic retain; a generation owns one actual source
//! registration. Sharing physical bytes therefore never pins prior logical roots.
const std = @import("std");
const publication = @import("index_publication.zig");
const A = std.mem.Allocator;
pub const Generation = struct {
    allocator: A,
    references: std.atomic.Value(usize) = .init(1),
    registration: publication.SharedReaders.Registration,
    /// Consumes registration on every path, including allocation failure.
    pub fn create(a: A, value: publication.SharedReaders.Registration) !*Generation {
        var registration = value;
        errdefer registration.deinit();
        if (!registration.present()) return error.InvalidIndexPublication;
        const self = try a.create(Generation);
        self.* = .{ .allocator = a, .registration = registration };
        return self;
    }
    pub fn retain(self: *Generation) void {
        const before = self.references.fetchAdd(1, .monotonic);
        if (before == 0 or before == std.math.maxInt(usize)) @panic("source generation reference overflow");
    }
    pub fn release(self: *Generation) void {
        const before = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
        self.registration.deinit();
        self.allocator.destroy(self);
    }
    pub fn lease(self: *const Generation) !publication.SharedReaders.Lease {
        return (try self.registration.lease()) orelse error.IndexSourceLeaseClosed;
    }
};

test "index source logical generations retain independently through retirement" {
    const a = std.testing.allocator;
    var owner = try publication.SharedReaders.open(a, .{ .owner = @splat(87), .publication = @splat(88) });
    defer owner.deinit();
    const generation = try Generation.create(a, try owner.register());
    generation.retain();
    owner.retire();
    var query = try generation.lease();
    generation.release();
    try std.testing.expect(owner.drained() == null);
    generation.release();
    try std.testing.expect(owner.drained() == null);
    query.deinit();
    try std.testing.expect(owner.drained() != null);
}

test "index source logical generation allocation failure consumes its registration" {
    const a = std.testing.allocator;
    var owner = try publication.SharedReaders.open(a, .{ .owner = @splat(89), .publication = @splat(90) });
    defer owner.deinit();
    var bytes: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&bytes);
    try std.testing.expectError(error.OutOfMemory, Generation.create(fixed.allocator(), try owner.register()));
    owner.retire();
    try std.testing.expect(owner.drained() != null);
}
