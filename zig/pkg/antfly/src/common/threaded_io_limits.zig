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

//! Process-lifetime `std.Io.Threaded` concurrency ceilings.
//!
//! Threaded executors retain workers until deinitialization. Long-lived owners
//! must therefore use a finite limit even when their normal workload has tighter
//! admission control. These values are backstops, not scheduling targets.

const std = @import("std");

/// General ceiling for process- or database-lifetime executors. Production
/// request, query, Raft, and index-worker admission stays below this value.
pub const service: u32 = 256;

/// Nested inference I/O and model-runtime fan-out. Request admission defaults
/// below this value; the ceiling leaves bounded headroom for model warmup and
/// multi-part operations without sharing API listener workers.
pub const inference: u32 = 64;

/// The serverless object-store pool is shared by five storage lanes behind a
/// listener admitting at most 64 connection handlers by default. Fourfold
/// headroom covers background lanes and larger configured listeners without
/// allowing unbounded retention.
pub const serverless_object_store: u32 = service;

pub fn initService(alloc: std.mem.Allocator) std.Io.Threaded {
    return std.Io.Threaded.init(alloc, .{
        .concurrent_limit = .limited(service),
    });
}

pub fn initServerlessObjectStore(alloc: std.mem.Allocator) std.Io.Threaded {
    return std.Io.Threaded.init(alloc, .{
        .concurrent_limit = .limited(serverless_object_store),
    });
}

test "threaded io production limits are finite" {
    try std.testing.expect(service > 0);
    try std.testing.expect(inference > 0);
    try std.testing.expect(inference <= service);
    try std.testing.expect(serverless_object_store > 0);
    try std.testing.expect(serverless_object_store <= service);

    var service_io = initService(std.testing.allocator);
    defer service_io.deinit();
    try std.testing.expectEqual(std.Io.Limit.limited(service), service_io.concurrent_limit);

    var object_store_io = initServerlessObjectStore(std.testing.allocator);
    defer object_store_io.deinit();
    try std.testing.expectEqual(
        std.Io.Limit.limited(serverless_object_store),
        object_store_io.concurrent_limit,
    );
}
