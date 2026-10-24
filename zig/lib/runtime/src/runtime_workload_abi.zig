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

//! Layout-only workload telemetry shared by independent execution owners.
//! No scheduler pointers, allocator ownership or transport state cross here.
/// Active execution-lease observations, with fixed server-owned class order:
/// query general, write, inference, background, query bounded, query analytical.
/// This is wall slot-time, not thread CPU utilization. Absence is explicit.
pub const RunnableServiceStats = extern struct {
    pub const abi_version: u32 = 1;
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    measurement_model: u32 = 1, // acknowledged active CPU lease slot-time
    available: bool = false,
    quantum_ns: u64 = 0,
    estimated_slot_ns: [6]u64 = @splat(0),
    observed_slot_ns: [6]u64 = @splat(0),
    unobserved_estimate_ns: [6]u64 = @splat(0),
    refunded_slot_ns: [6]u64 = @splat(0),
    debt_paid_slot_ns: [6]u64 = @splat(0),
    clipped_adjustments: [6]u64 = @splat(0),
    debt_slot_ns: [6]u64 = @splat(0),
    rebate_slot_ns: [6]u64 = @splat(0),

    pub fn validate(self: *const @This()) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(@This()) or self.measurement_model != 1) return error.UnsupportedVersion;
        if (!self.available) return;
        if (self.quantum_ns == 0 or self.quantum_ns > 100_000_000) return error.InvalidArgument;
        for (self.debt_slot_ns, self.rebate_slot_ns) |debt, rebate| {
            if ((debt != 0 and rebate != 0) or debt > 64 * self.quantum_ns or rebate > 64 * self.quantum_ns) return error.InvalidArgument;
        }
    }
};

pub const RunnableStats = extern struct {
    enabled: bool = false,
    capacity_slots: u64 = 0,
    control_capacity_slots: u64 = 0,
    operation_capacity: u64 = 0,
    metadata_capacity_bytes: u64 = 0,
    control_metadata_capacity_bytes: u64 = 0,
    max_wait_ms: u64 = 0,
    quantum_ms: u64 = 0,
    checkpoint_budget: u64 = 0,
    operations: u64 = 0,
    retained_bytes: u64 = 0,
    control_operations: u64 = 0,
    control_retained_bytes: u64 = 0,
    queued: u64 = 0,
    runnable_slots: u64 = 0,
    control_slots: u64 = 0,
    resource_blocked: u64 = 0,
    output_blocked: u64 = 0,
    /// Query, write, inference, background, control; bounded server classes.
    grants: [5]u64 = @splat(0),
    yields: u64 = 0,
    backfills: u64 = 0,
    rejected: u64 = 0,
    wait_ns: u64 = 0,
    read_isolation_enabled: u64 = 0,
    general_capacity_slots: u64 = 0,
    bounded_read_capacity_slots: u64 = 0,
    bounded_read_operation_capacity: u64 = 0,
    bounded_read_metadata_capacity_bytes: u64 = 0,
    bounded_read_operations: u64 = 0,
    bounded_read_bytes: u64 = 0,
    bounded_read_slots: u64 = 0,
    /// General, bounded, analytical; trusted execution policy, not wire hints.
    read_lane_grants: [3]u64 = @splat(0),
    read_transition_reserved_operations: u64 = 0,
    read_transition_reserved_bytes: u64 = 0,
    read_transition_operations: u64 = 0,
    read_transition_bytes: u64 = 0,
    read_demotions: u64 = 0,
    read_transition_expired: u64 = 0,
    read_service_debt_paid: u64 = 0,
    /// CPU only; operation/heap charges remain in the general partition.
    cpu_borrowed_slots: u64 = 0,
    cpu_borrow_grants: u64 = 0,
    cpu_borrow_returns: u64 = 0,
    service: RunnableServiceStats = .{},
};

/// Physical read allocations. Lane order is general, bounded, transition.
/// Transition credits reserve capacity and must not be added to live usage.
pub const ReadMemoryStats = extern struct {
    enabled: bool = false,
    reserved_bytes: u64 = 0,
    capacity_bytes: [3]u64 = @splat(0),
    group_capacity: [3]u64 = @splat(0),
    used_bytes: [3]u64 = @splat(0),
    groups: [3]u64 = @splat(0),
    transition_reserved_bytes: u64 = 0,
    transition_reserved_groups: u64 = 0,
    rejected: u64 = 0,
};
