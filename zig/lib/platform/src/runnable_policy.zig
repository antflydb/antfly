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

pub const CapacityMode = enum(u8) { fixed, automatic };
pub const Config = struct {
    /// Automatic mode treats slots, max_bytes and control_bytes as ceilings.
    /// Fixed policies preserve their exact declared control reservation.
    capacity_mode: CapacityMode = .fixed,
    slots: usize = 8,
    control_slots: usize = 1,
    /// Dedicated cleanup CPU drawn from slots, separate from health controls.
    /// Zero retains the legacy shared-control policy until callers opt in.
    cleanup_slots: usize = 0,
    /// Independent, nonborrowable return CPU for a single guarded publisher.
    /// The original operation/class/lifetime remains authoritative. Fixed
    /// policies opt in explicitly; automatic startup funds this inside slots.
    publication_slots: usize = 0,
    /// Reserve bounded_read_slots ordinary CPUs and one quarter of ordinary
    /// registry count/bytes for verified bounded reads.
    read_isolation: ?bool = null,
    /// Total bounded CPU reserve inside slots. One slot is a nonborrowable
    /// floor; only excess capacity may lend one CPU to a verified stage.
    bounded_read_slots: usize = 1,
    weights: [4]u16 = .{ 4, 2, 1, 1 },
    max_operations: usize = 128,
    max_bytes: u64 = 8 << 20,
    control_bytes: u64 = 64 << 10,
    max_wait_ms: u32 = 250,
    max_backfills: u16 = 8,
    quantum_ms: u32 = 2,
    checkpoint_budget: u32 = 64,

    /// Resolve once before publication. Protected lanes remain inside the
    /// declared slot ceiling, with a minimum ordinary lane even below that CPU
    /// count. Slots bound runnable ownership, not guaranteed dedicated cores.
    /// Metadata receives at most 1/32 of the visible memory envelope, subject to
    /// the configured ceiling. This is not a cap on storage/model/process heap.
    pub fn resolveCapacity(self: Config, cpu: @import("process_memory.zig").CpuCapacity, memory: @import("process_memory.zig").Envelope) !Config {
        try self.validate();
        if (self.capacity_mode == .fixed) return self;
        var resolved = self;
        resolved.capacity_mode = .fixed;
        resolved.cleanup_slots = 1;
        resolved.publication_slots = 1;
        const minimum = self.control_slots + 3;
        if (minimum > self.slots) return error.InvalidRunnablePolicy;
        resolved.slots = @max(minimum, @min(self.slots, cpu.parallelism()));
        resolved.read_isolation = self.read_isolation orelse (resolved.ordinarySlots() > self.bounded_read_slots and self.max_operations >= 8);
        if (memory.limit_bytes != 0) resolved.max_bytes = @min(self.max_bytes, memory.limit_bytes / 32);
        // Large nodes can hold the complete protocol baseline without taking
        // foreground headroom. Smaller envelopes retain half for ordinary work;
        // never shrink below the generic protected-control minimum (or a
        // stricter caller-selected smaller ceiling).
        resolved.control_bytes = @min(self.control_bytes, resolved.max_bytes / 2);
        if (resolved.control_bytes < @min(self.control_bytes, @as(u64, 64 << 10))) return error.InvalidRunnablePolicy;
        try resolved.validateManagedCleanupCapacity();
        return resolved;
    }

    /// Honor a stricter configured process budget before publishing one
    /// effective policy to API/storage consumers. A fixed policy never probes.
    pub fn resolveForStartup(self: Config, process_limit_bytes: u64) !Config {
        if (self.capacity_mode == .fixed) {
            try self.validate();
            return self;
        }
        return self.resolveCapacity(@import("process_memory.zig").cpuCapacity(), startupMemoryEnvelope(process_limit_bytes));
    }

    pub fn startupMemoryEnvelope(process_limit_bytes: u64) @import("process_memory.zig").Envelope {
        var memory = @import("process_memory.zig").systemEnvelope();
        if (process_limit_bytes != 0) memory.limit_bytes = if (memory.limit_bytes == 0)
            process_limit_bytes
        else
            @min(memory.limit_bytes, process_limit_bytes);
        return memory;
    }

    pub fn ordinarySlots(self: Config) usize {
        return self.slots - self.control_slots - self.cleanup_slots - self.publication_slots;
    }

    pub fn readIsolation(self: Config) bool {
        return self.read_isolation orelse false;
    }

    pub fn generalSlots(self: Config) usize {
        return self.ordinarySlots() - self.boundedReadSlots();
    }

    pub fn boundedReadSlots(self: Config) usize {
        return if (self.readIsolation()) self.bounded_read_slots else 0;
    }

    pub fn boundedReadFloorSlots(self: Config) usize {
        return @intFromBool(self.readIsolation());
    }

    pub fn cpuBorrowSlots(self: Config) usize {
        // Keep at most one borrower until broader lending is qualified.
        return @min(@as(usize, 1), self.boundedReadSlots() - self.boundedReadFloorSlots());
    }

    pub fn boundedReadMetadataBytes(self: Config) u64 {
        return if (self.readIsolation()) (self.max_bytes - self.control_bytes) / 4 else 0;
    }

    pub fn generalMetadataBytes(self: Config) u64 {
        return self.max_bytes - self.control_bytes - self.boundedReadMetadataBytes();
    }

    /// Managed startup needs independent ordinary, health and cleanup capacity.
    /// Keep generic scheduler validation separate for standalone consumers.
    pub noinline fn validateManagedCleanupCapacity(self: Config) !void {
        try self.validate();
        if (self.max_operations < 4 or (self.cleanup_slots == 0 and self.ordinarySlots() < (if (self.readIsolation()) self.bounded_read_slots + 2 else 2)))
            return error.ManagerCleanupCapacityUnavailable;
    }

    /// Protect sparse arrivals as well as a rapid bypass stream. Derive an
    /// internal bound from the existing wait policy, without another clock or
    /// admission deadline: at most 25 ms and at most one quarter of queue wait.
    pub fn backfillAgeNs(self: Config) u64 {
        return @min(25 * std.time.ns_per_ms, @as(u64, self.max_wait_ms) * std.time.ns_per_ms / 4);
    }

    pub fn validate(self: Config) !void {
        if (self.slots < 2 or self.slots > 65535 or self.control_slots == 0 or self.control_slots >= self.slots or
            self.cleanup_slots > 1 or self.publication_slots > 1 or self.cleanup_slots + self.publication_slots >= self.slots - self.control_slots or
            self.bounded_read_slots == 0 or self.bounded_read_slots > 65533 or
            (self.readIsolation() and (self.ordinarySlots() <= self.bounded_read_slots or self.max_operations < 8 or self.max_bytes -| self.control_bytes < 4)) or
            self.max_operations < 2 or self.max_bytes == 0 or self.control_bytes == 0 or self.control_bytes >= self.max_bytes or self.max_wait_ms == 0 or self.max_wait_ms > 60000 or
            self.quantum_ms == 0 or self.quantum_ms > 100 or self.checkpoint_budget == 0 or self.checkpoint_budget > 1048576)
            return error.InvalidRunnablePolicy;
        for (self.weights) |weight| if (weight == 0) return error.InvalidRunnablePolicy;
    }
};
