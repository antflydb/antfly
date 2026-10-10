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
};
