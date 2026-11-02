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

//! Optional backend measurements. Counters are context cumulative; memory is
//! a gauge with explicit ownership scope. Null means unmeasured, never zero.
const std = @import("std");
pub const Counters = struct {
    kernel_dispatches: ?u64 = null,
    graph_submissions: ?u64 = null,
    frame_submissions: ?u64 = null,
    /// Explicit successful project-owned host synchronization API calls.
    /// GPU dependencies are separate; implicit vendor/free waits stay opaque.
    synchronizations: ?u64 = null,
    physical_allocations: ?u64 = null,
    physical_frees: ?u64 = null,

    /// A reset/unsupported counter is unknown for this interval. Concurrent
    /// submissions to the same context remain part of the context interval.
    pub fn since(self: Counters, before: Counters) Counters {
        var result: Counters = .{};
        inline for (@typeInfo(Counters).@"struct".field_names) |field| {
            if (@field(self, field)) |after| {
                if (@field(before, field)) |start| {
                    if (after >= start) @field(result, field) = after - start;
                }
            }
        }
        return result;
    }
};
pub const Memory = struct {
    scope: enum { unavailable, backend_device_buffers, backend_runtime_buffers } = .unavailable,
    live_bytes: ?u64 = null,
    /// Lifetime high-water gauge; not an invocation's peak allocation.
    lifetime_peak_bytes: ?u64 = null,
};
/// Worker-process observations include other contexts and threads. These are
/// distinct from the invocation's submitting-thread counters and program leases.
pub const ProcessCounters = struct {
    upload_calls: u64 = 0,
    upload_bytes: u64 = 0,
    download_calls: u64 = 0,
    download_bytes: u64 = 0,
    copy_calls: u64 = 0,
    copy_bytes: u64 = 0,
    completed_commands: u64 = 0,
    failed_commands: u64 = 0,
    physical_allocations: u64 = 0,
    physical_frees: u64 = 0,
    allocated_bytes: u64 = 0,
    freed_bytes: u64 = 0,
    initialized_bytes: u64 = 0,
    initialization_calls: u64 = 0,

    pub fn since(self: ProcessCounters, before: ProcessCounters) ProcessCounters {
        var result: ProcessCounters = .{};
        inline for (@typeInfo(ProcessCounters).@"struct".field_names) |field|
            @field(result, field) = @field(self, field) -| @field(before, field);
        return result;
    }
};
pub const Process = struct {
    /// Tracked MTLBuffer allocatedSize, including command-retained temporaries.
    /// Alias bytes use logical mapped length and are never added to owned bytes.
    memory_scope: enum { tracked_metal_buffers_worker_process } = .tracked_metal_buffers_worker_process,
    counters: ProcessCounters = .{},
    owned_live_bytes: u64 = 0,
    owned_lifetime_peak_bytes: u64 = 0,
    private_live_bytes: u64 = 0,
    private_lifetime_peak_bytes: u64 = 0,
    host_visible_live_bytes: u64 = 0,
    host_visible_lifetime_peak_bytes: u64 = 0,
    alias_live_bytes: u64 = 0,
    alias_lifetime_peak_bytes: u64 = 0,

    pub fn since(self: Process, before: Process) Process {
        var result = self;
        result.counters = self.counters.since(before.counters);
        return result;
    }
};
/// Actual requested allocation spans through an explicitly observed owner
/// allocator. Allocator padding, mapped files and external library heaps are
/// separate scopes. Matching/ancestor identities must never be added together.
pub const ObservedAllocator = struct {
    counter_scope: enum { owner_lifetime, owner_interval } = .owner_lifetime,
    owner_identity: usize,
    parent_identity: ?usize,
    coverage: @import("allocation_observer.zig").Coverage,
    usage: @import("allocation_observer.zig").Snapshot,
    pub fn init(owner: *@import("allocation_observer.zig").State) ObservedAllocator {
        return .{ .owner_identity = @intFromPtr(owner), .parent_identity = if (owner.parent()) |p| @intFromPtr(p) else null, .coverage = owner.coverage, .usage = owner.snapshot() };
    }
    fn since(self: ObservedAllocator, before: ObservedAllocator) ObservedAllocator {
        var result = self;
        if (self.owner_identity == before.owner_identity and self.counter_scope == .owner_lifetime and before.counter_scope == .owner_lifetime) {
            result.usage = self.usage.since(before.usage);
            result.counter_scope = .owner_interval;
        }
        return result;
    }
};
pub const NativeAllocations = struct {
    compute: ?ObservedAllocator = null,
    model: ?ObservedAllocator = null,
    late_enable_failed: bool = false,
    fn since(self: NativeAllocations, before: NativeAllocations) NativeAllocations {
        var result = self;
        if (self.compute) |after| if (before.compute) |start| {
            result.compute = after.since(start);
        };
        if (self.model) |after| if (before.model) |start| {
            result.model = after.since(start);
        };
        return result;
    }
};
/// Explicit CUDA API/topology scopes. No field claims total completed kernels
/// or kernels hidden in an eager vendor operation/device-dynamic launch.
pub const CudaCounters = struct {
    direct_kernel_submissions: u64 = 0,
    foreign_stream_kernel_calls: u64 = 0,
    capture_recording_kernel_calls: u64 = 0,
    capture_recording_graph_calls: u64 = 0,
    known_graph_submissions: u64 = 0,
    declared_graph_kernel_nodes_submitted: u64 = 0,
    declared_graph_nodes_submitted: u64 = 0,
    opaque_graph_submissions: u64 = 0,
    /// Explicit project-owned successful host wait APIs. These are not CPU
    /// blocking durations or implicit vendor/free synchronizations.
    host_stream_synchronizations: u64 = 0,
    host_event_synchronizations: u64 = 0,
    failed_host_synchronizations: u64 = 0,
    /// Accepted GPU dependency API calls, distinct from host synchronization.
    gpu_stream_dependencies: u64 = 0,
    capture_recording_stream_wait_events: u64 = 0,
    foreign_stream_wait_event_calls: u64 = 0,
    failed_gpu_stream_dependencies: u64 = 0,
    metadata_failures: u64 = 0,
    pub fn since(self: CudaCounters, before: CudaCounters) CudaCounters {
        var result: CudaCounters = .{};
        inline for (@typeInfo(CudaCounters).@"struct".field_names) |field|
            @field(result, field) = @field(self, field) -| @field(before, field);
        return result;
    }
};
pub const Cuda = struct {
    state: enum { enabled, allocation_failed } = .enabled,
    owner_identity: ?usize = null,
    counter_scope: enum { observer_lifetime, context_interval } = .observer_lifetime,
    coverage: enum { since_enable } = .since_enable,
    counters: ?CudaCounters = null,
    registered_graphs: ?usize = null,
    fn since(self: Cuda, before: Cuda) Cuda {
        var result = self;
        if (self.owner_identity != null and self.owner_identity == before.owner_identity) {
            if (self.counters) |after| if (before.counters) |start| {
                result.counters = after.since(start);
                result.counter_scope = .context_interval;
            };
        }
        return result;
    }
};
pub const Snapshot = struct {
    counters: Counters = .{},
    native_allocations: ?NativeAllocations = null,
    /// Immutable deduplicated owner intervals for observed numerical dispatch.
    allocation_owners: ?@import("allocation_registry.zig").Summary = null,
    cuda: ?Cuda = null,
    memory: Memory = .{},
    /// Invocation interval on its submitting thread, including CPU fallbacks.
    submission: ?@import("runtime_observation.zig").Counters = null,
    /// Process-wide interval/gauges, never per-request ownership or residency.
    process: ?Process = null,
    pub fn since(self: Snapshot, before: Snapshot) Snapshot {
        return .{
            .counters = self.counters.since(before.counters),
            .memory = self.memory,
            .cuda = if (self.cuda) |after| if (before.cuda) |start| after.since(start) else after else null,
            .native_allocations = if (self.native_allocations) |after| if (before.native_allocations) |start| after.since(start) else after else null,
            .process = if (self.process) |after| if (before.process) |start| after.since(start) else after else null,
        };
    }
};

test "runtime telemetry distinguishes unavailable reset and measured zero counters" {
    const before = Snapshot{ .counters = .{ .kernel_dispatches = 10, .graph_submissions = 4, .synchronizations = 3 }, .memory = .{ .scope = .backend_device_buffers, .live_bytes = 128, .lifetime_peak_bytes = 1024 } };
    const after = Snapshot{ .counters = .{ .kernel_dispatches = 12, .graph_submissions = 4, .synchronizations = 1 }, .memory = .{ .scope = .backend_device_buffers, .live_bytes = 64, .lifetime_peak_bytes = 1024 } };
    const delta = after.since(before);
    try std.testing.expectEqual(@as(?u64, 2), delta.counters.kernel_dispatches);
    try std.testing.expectEqual(@as(?u64, 0), delta.counters.graph_submissions);
    try std.testing.expectEqual(@as(?u64, null), delta.counters.synchronizations);
    try std.testing.expectEqual(@as(?u64, null), delta.counters.physical_allocations);
    try std.testing.expectEqual(@as(?u64, 64), delta.memory.live_bytes);
    try std.testing.expectEqual(@as(?u64, 1024), delta.memory.lifetime_peak_bytes);
}
