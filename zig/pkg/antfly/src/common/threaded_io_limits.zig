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

/// BackendRuntime owns isolated executors because API, Raft, durable work, and
/// inference may synchronously submit nested work. Isolation avoids pool
/// dependency deadlocks, but each pool needs its own small ceiling so lazy
/// activation cannot ratchet the process to several multiples of `service`.
/// The defaults, including dedicated service workers, sum to 252, leaving four
/// threads of headroom under the explicit aggregate ceiling. These are a
/// value-semantic default profile, not individual maxima: callers may
/// redistribute capacity while keeping fixed lanes nonzero and the total
/// bounded; dedicated workers may be disabled with a zero capacity.
pub const backend_runtime_aggregate: u32 = service;
pub const backend_runtime_durable_background: u32 = 48;
pub const backend_runtime_api: u32 = 48;
pub const backend_runtime_raft_inbound: u32 = 32;
pub const backend_runtime_raft_outbound: u32 = 32;
pub const backend_runtime_inference: u32 = 48;
pub const backend_runtime_control: u32 = 8;
pub const backend_runtime_workers: u32 = 32;
/// Compatibility name for non-BackendRuntime inference executors.
pub const inference: u32 = backend_runtime_inference;

/// CPU-bound PDF raster work needs physical worker affinity for reusable
/// scratch. These are dedicated threads rather than `std.Io` workers, so keep
/// the process-lifetime ceiling intentionally small; page-level admission may
/// choose less concurrency per invocation.
pub const pdf_render: u32 = 4;
pub const pdf_render_queue: u32 = pdf_render * 2;
pub const pdf_render_window_scratch_bytes: usize = 512 * 1024 * 1024;
pub const pdf_render_retained_scratch_bytes_per_worker: usize = 2 * 1024 * 1024;
pub const pdf_render_runtime_overhead_bytes: usize = pdf_render_retained_scratch_bytes_per_worker * pdf_render;
pub const pdf_render_max_scratch_bytes: usize = pdf_render_window_scratch_bytes + pdf_render_runtime_overhead_bytes;

pub const BackendRuntimeLaneLimits = struct {
    durable_background: u32 = backend_runtime_durable_background,
    api: u32 = backend_runtime_api,
    raft_inbound: u32 = backend_runtime_raft_inbound,
    raft_outbound: u32 = backend_runtime_raft_outbound,
    inference: u32 = backend_runtime_inference,
    control: u32 = backend_runtime_control,
    pdf_render: u32 = pdf_render,
    /// Dedicated long-lived service workers (Raft senders, listeners, etc.).
    /// Reserve their full ceiling even before lazy executor activation so
    /// other lanes cannot steal capacity required by their dependencies.
    /// Zero explicitly disables dedicated worker admission.
    worker_capacity: u32 = backend_runtime_workers,

    pub fn total(self: @This()) u64 {
        return @as(u64, self.durable_background) +
            @as(u64, self.api) +
            @as(u64, self.raft_inbound) +
            @as(u64, self.raft_outbound) +
            @as(u64, self.inference) +
            @as(u64, self.control) +
            @as(u64, self.pdf_render) +
            @as(u64, self.worker_capacity);
    }

    pub fn validate(self: @This()) !void {
        if (self.durable_background == 0 or self.durable_background > backend_runtime_aggregate or
            self.api == 0 or self.api > backend_runtime_aggregate or
            self.raft_inbound == 0 or self.raft_inbound > backend_runtime_aggregate or
            self.raft_outbound == 0 or self.raft_outbound > backend_runtime_aggregate or
            self.inference == 0 or self.inference > backend_runtime_aggregate or
            self.control == 0 or self.control > backend_runtime_aggregate or
            self.pdf_render == 0 or self.pdf_render > backend_runtime_aggregate or
            self.worker_capacity > backend_runtime_aggregate or
            self.total() > backend_runtime_aggregate)
            return error.InvalidBackendRuntimeLaneLimits;
    }
};

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

test "threaded io worker reservations share the aggregate lane budget" {
    var limits = BackendRuntimeLaneLimits{};
    const fixed_lanes = limits.total() - limits.worker_capacity;
    limits.worker_capacity = @intCast(backend_runtime_aggregate - fixed_lanes);
    try limits.validate();
    try std.testing.expectEqual(@as(u64, backend_runtime_aggregate), limits.total());
    limits.worker_capacity += 1;
    try std.testing.expectError(error.InvalidBackendRuntimeLaneLimits, limits.validate());
    // Neither an oversized individual pool nor wide addition may wrap the
    // aggregate check. Zero remains an explicit no-dedicated-workers profile.
    limits.worker_capacity = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidBackendRuntimeLaneLimits, limits.validate());
    limits.worker_capacity = 0;
    try limits.validate();
    try std.testing.expectEqual(fixed_lanes, limits.total());
}

test "threaded io production limits are finite" {
    try std.testing.expect(service > 0);
    try std.testing.expect(inference > 0);
    try std.testing.expect(inference <= service);
    try std.testing.expect(pdf_render > 0);
    try std.testing.expect(pdf_render <= inference);
    try std.testing.expect(pdf_render_queue >= pdf_render);
    try std.testing.expect(
        pdf_render_runtime_overhead_bytes <= pdf_render_max_scratch_bytes,
    );
    try std.testing.expect(serverless_object_store > 0);
    try std.testing.expect(serverless_object_store <= service);

    const runtime_limits = BackendRuntimeLaneLimits{};
    try runtime_limits.validate();
    const expected_runtime_total = @as(u64, backend_runtime_durable_background) +
        backend_runtime_api + backend_runtime_raft_inbound +
        backend_runtime_raft_outbound + backend_runtime_inference +
        backend_runtime_control + pdf_render + backend_runtime_workers;
    try std.testing.expectEqual(@as(u64, 252), expected_runtime_total);
    try std.testing.expectEqual(expected_runtime_total, runtime_limits.total());
    try std.testing.expect(runtime_limits.total() <= backend_runtime_aggregate);
    try std.testing.expect(backend_runtime_control < backend_runtime_api);
    try std.testing.expect(backend_runtime_control < backend_runtime_durable_background);
    try (BackendRuntimeLaneLimits{
        .durable_background = 16,
        .api = 128,
        .raft_inbound = 16,
        .raft_outbound = 16,
        .worker_capacity = 16,
    }).validate();
    try std.testing.expectError(
        error.InvalidBackendRuntimeLaneLimits,
        (BackendRuntimeLaneLimits{ .api = backend_runtime_aggregate + 1 }).validate(),
    );

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
