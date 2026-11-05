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

//! Feedback controller for detached replay. A turn retains one primary window;
//! sizing targets a short elapsed quantum while hard limits bound payload memory.
//! Per-kind lanes survive runtime refresh through the namespace scheduling owner.
const std = @import("std");
pub const Budget = struct { records: usize = 64, bytes: u64 = 1024 * 1024 };
pub const Snapshot = struct {
    records: [5]usize = @splat(64),
    bytes: [5]u64 = @splat(1024 * 1024),
    observations: u64 = 0,
    overruns: u64 = 0,
    retention_pressure: bool = false,
    target_quantum_ns: u64 = 25 * std.time.ns_per_ms,
    effective_quantum_ns: [5]u64 = @splat(25 * std.time.ns_per_ms),
};
pub const ProducerSnapshot = struct {
    max_document_groups: usize = 8,
    max_input_bytes: u64 = 256 * 1024,
    observations: u64 = 0,
    overruns: u64 = 0,
    retention_pressure: bool = false,
    target_quantum_ns: u64 = 25 * std.time.ns_per_ms,
};
pub const Controller = struct {
    mutex: std.atomic.Mutex = .unlocked,
    lanes: [5]Budget = @splat(.{}),
    publication_cost_ns: [5]u64 = @splat(0),
    pressure: bool = false,
    blocked_appends: u64 = 0,
    observations: u64 = 0,
    overruns: u64 = 0,
    max_records: usize = 2048,
    max_bytes: u64 = 16 * 1024 * 1024,
    pub const quantum_ns = 25 * std.time.ns_per_ms;
    pub const max_quantum_ns = 200 * std.time.ns_per_ms;
    fn effectiveQuantum(self: *const Controller, lane: usize) u64 {
        if (self.pressure) return quantum_ns;
        return @min(max_quantum_ns, @max(quantum_ns, self.publication_cost_ns[lane] *| 2));
    }
    fn lock(self: *Controller) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn updatePressure(self: *Controller, pending: u64, reusable: u64, blocked: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.pressure = pending > 128 and reusable < 128 and blocked > self.blocked_appends;
        self.blocked_appends = blocked;
    }
    pub fn snapshot(self: *Controller) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var result = Snapshot{ .observations = self.observations, .overruns = self.overruns, .retention_pressure = self.pressure };
        for (self.lanes, 0..) |lane, i| {
            result.effective_quantum_ns[i] = self.effectiveQuantum(i);
            result.records[i] = if (self.pressure) @min(lane.records, 32) else lane.records;
            result.bytes[i] = if (self.pressure) @min(lane.bytes, 512 * 1024) else lane.bytes;
        }
        return result;
    }
    pub fn producerSnapshot(self: *Controller) ProducerSnapshot {
        const current = self.snapshot();
        return .{ .max_document_groups = current.records[0], .max_input_bytes = current.bytes[0], .observations = current.observations, .overruns = current.overruns, .retention_pressure = current.retention_pressure, .target_quantum_ns = current.effective_quantum_ns[0] };
    }
    pub fn budget(self: *Controller, lane: usize) Budget {
        self.lock();
        defer self.mutex.unlock();
        var result = self.lanes[lane];
        if (self.pressure) {
            result.records = @min(result.records, 32);
            result.bytes = @min(result.bytes, 512 * 1024);
        }
        return result;
    }
    pub fn observe(self: *Controller, lane: usize, used: Budget, elapsed_ns: u64, scanned: usize, failed: bool) void {
        self.lock();
        defer self.mutex.unlock();
        self.observeLocked(lane, used, elapsed_ns, scanned >= used.records, failed);
    }

    /// Amortize measured fixed publication cost without charging queue waits
    /// as preparation. Bounded memory/record caps remain independent, and
    /// retention pressure immediately restores the short base quantum.
    pub fn observePreparedWindow(self: *Controller, lane: usize, used: Budget, preparation_ns: u64, publication_ns: u64, filled: bool, failed: bool) void {
        self.lock();
        defer self.mutex.unlock();
        const sample = @min(publication_ns, max_quantum_ns / 2);
        const previous = self.publication_cost_ns[lane];
        self.publication_cost_ns[lane] = if (previous == 0) sample else (previous * 3 + sample) / 4;
        self.observeLocked(lane, used, preparation_ns, filled, failed);
    }

    fn observeLocked(self: *Controller, lane: usize, used: Budget, elapsed_ns: u64, filled: bool, failed: bool) void {
        self.observations +|= 1;
        const target_ns = self.effectiveQuantum(lane);
        if (failed or elapsed_ns > target_ns) {
            self.overruns +|= @intFromBool(!failed);
            self.lanes[lane] = .{ .records = @max(8, used.records / 2), .bytes = @max(128 * 1024, used.bytes / 2) };
        } else if (!self.pressure and filled) {
            if (elapsed_ns < target_ns / 2) {
                self.lanes[lane] = .{ .records = @min(self.max_records, used.records * 2), .bytes = @min(self.max_bytes, used.bytes * 2) };
            } else if (elapsed_ns < target_ns * 3 / 4) {
                // Recover gradually after a cold overrun. Requiring enough
                // slack to double can otherwise strand a healthy small window
                // forever, paying extra fixed publication costs on every turn.
                self.lanes[lane] = .{ .records = @min(self.max_records, used.records + @max(1, used.records / 4)), .bytes = @min(self.max_bytes, used.bytes + @max(1, used.bytes / 4)) };
            }
        }
    }

    /// Admission and durable publication are fixed costs to amortize. They
    /// still count toward the call's yield deadline, but shrinking preparation
    /// windows because another writer held the gate amplifies contention.
    pub fn observePreparation(self: *Controller, lane: usize, used: Budget, elapsed_ns: u64, publication_ns: u64, scanned: usize, failed: bool) void {
        self.observePreparedWindow(lane, used, elapsed_ns -| publication_ns, publication_ns, scanned >= used.records, failed);
    }

    /// A stale publication or cooperative cancellation says nothing about
    /// preparation capacity. Keep measured slow work, but treat only memory
    /// admission failures as evidence that the next window must be smaller.
    pub fn observeFailure(self: *Controller, lane: usize, used: Budget, preparation_ns: u64, failure: ?anyerror) void {
        const memory_pressure = if (failure) |err| err == error.ResourceBudgetExceeded or err == error.OutOfMemory else false;
        self.observe(lane, used, preparation_ns, 0, memory_pressure);
    }
};

test "replay budget preserves capacity across cooperative aborts and recovers after memory pressure" {
    var state: Controller = .{};
    const initial = state.budget(0);
    for (0..32) |_| {
        state.observeFailure(0, state.budget(0), 1, error.EnrichmentWaitCanceled);
        state.observeFailure(0, state.budget(0), 1, error.EnrichmentSourceChanged);
    }
    try std.testing.expectEqualDeep(initial, state.budget(0));
    state.observeFailure(0, initial, Controller.quantum_ns + 1, error.EnrichmentWaitCanceled);
    try std.testing.expectEqual(@as(usize, 32), state.budget(0).records);
    state.observeFailure(0, state.budget(0), 1, error.ResourceBudgetExceeded);
    try std.testing.expectEqual(@as(usize, 16), state.budget(0).records);
    state.observeFailure(0, state.budget(0), 1, error.OutOfMemory);
    try std.testing.expectEqual(@as(usize, 8), state.budget(0).records);
    const reduced = state.budget(0);
    state.observe(0, reduced, 1, reduced.records, false);
    try std.testing.expectEqual(@as(usize, 16), state.budget(0).records);
    try std.testing.expectEqualDeep(initial, state.budget(1));
}

test "replay budget excludes publication contention without hiding slow preparation or failure" {
    var state: Controller = .{};
    const initial = state.budget(0);
    state.observePreparation(0, initial, 10 * std.time.ns_per_s, 10 * std.time.ns_per_s - 1, initial.records, false);
    try std.testing.expectEqual(@as(usize, 128), state.budget(0).records);
    const grown = state.budget(0);
    state.observePreparation(0, grown, 10 * std.time.ns_per_s, 9 * std.time.ns_per_s, grown.records, false);
    try std.testing.expectEqualDeep(initial, state.budget(0));
    state.observePreparation(0, initial, 1, 2, 0, true);
    try std.testing.expectEqual(@as(usize, 32), state.budget(0).records);
}

test "replay budget grows only on full cheap windows and shrinks expensive work" {
    var state: Controller = .{};
    const initial = state.budget(0);
    state.observe(0, initial, 1, initial.records - 1, false);
    try std.testing.expectEqualDeep(initial, state.budget(0));
    state.observe(0, initial, 1, initial.records, false);
    try std.testing.expectEqual(@as(usize, 128), state.budget(0).records);
    const grown = state.budget(0);
    state.observe(0, grown, Controller.quantum_ns + 1, grown.records, false);
    try std.testing.expectEqualDeep(initial, state.budget(0));
    try std.testing.expectEqualDeep(initial, state.budget(1));
}

test "replay budget bounds memory and retains a minimum under pressure and failures" {
    var state: Controller = .{};
    for (0..20) |_| {
        const used = state.budget(0);
        state.observe(0, used, 1, used.records, false);
    }
    try std.testing.expectEqual(@as(usize, 2048), state.budget(0).records);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), state.budget(0).bytes);
    state.updatePressure(1000, 0, 1);
    try std.testing.expectEqual(@as(usize, 32), state.budget(0).records);
    for (0..20) |_| {
        const used = state.budget(0);
        state.observe(0, used, 1, 0, true);
    }
    try std.testing.expectEqual(@as(usize, 8), state.budget(0).records);
    try std.testing.expectEqual(@as(u64, 128 * 1024), state.budget(0).bytes);
    state.updatePressure(0, 1000, 1);
    try std.testing.expect(!state.pressure);
}

test "replay budget respects a producer specific discovery ceiling" {
    var producer: Controller = .{ .lanes = @splat(.{ .records = 8, .bytes = 256 * 1024 }), .max_records = 128, .max_bytes = 4 * 1024 * 1024 };
    for (0..20) |_| {
        const used = producer.budget(0);
        producer.observe(0, used, 1, used.records, false);
    }
    try std.testing.expectEqual(@as(usize, 128), producer.budget(0).records);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), producer.budget(0).bytes);
    const full = producer.budget(0);
    producer.observe(0, full, Controller.quantum_ns + 1, full.records, false);
    try std.testing.expectEqual(@as(usize, 64), producer.budget(0).records);
}

test "replay budget recovers gradual capacity after a cold overrun without exceeding its quantum" {
    var state: Controller = .{};
    state.observe(0, state.budget(0), Controller.quantum_ns + 1, 64, false);
    const reduced = state.budget(0);
    try std.testing.expectEqual(@as(usize, 32), reduced.records);
    state.observe(0, reduced, Controller.quantum_ns * 3 / 5, reduced.records, false);
    const recovered = state.budget(0);
    try std.testing.expectEqual(@as(usize, 40), recovered.records);
    try std.testing.expectEqual(reduced.bytes + reduced.bytes / 4, recovered.bytes);
    state.observe(0, recovered, Controller.quantum_ns * 4 / 5, recovered.records, false);
    try std.testing.expectEqualDeep(recovered, state.budget(0));
    state.observe(0, recovered, 1, recovered.records - 1, false);
    try std.testing.expectEqualDeep(recovered, state.budget(0));
    state.updatePressure(256, 0, 1);
    const pressured = state.budget(0);
    state.observe(0, pressured, 1, pressured.records, false);
    try std.testing.expectEqualDeep(pressured, state.budget(0));
}

test "replay budget amortizes publication cost with a hard ceiling and immediate retention override" {
    var state: Controller = .{};
    const initial = state.budget(0);
    state.observePreparedWindow(0, initial, 40 * std.time.ns_per_ms, 80 * std.time.ns_per_ms, true, false);
    try std.testing.expectEqual(@as(usize, 128), state.budget(0).records);
    try std.testing.expectEqual(@as(u64, 160 * std.time.ns_per_ms), state.snapshot().effective_quantum_ns[0]);
    const grown = state.budget(0);
    state.observePreparedWindow(0, grown, 40 * std.time.ns_per_ms, std.time.ns_per_s, true, false);
    try std.testing.expect(state.snapshot().effective_quantum_ns[0] <= Controller.max_quantum_ns);
    state.updatePressure(256, 0, 1);
    const pressured = state.budget(0);
    try std.testing.expectEqual(Controller.quantum_ns, state.snapshot().effective_quantum_ns[0]);
    state.observePreparedWindow(0, pressured, 40 * std.time.ns_per_ms, std.time.ns_per_s, true, false);
    try std.testing.expectEqual(@max(@as(usize, 8), pressured.records / 2), state.budget(0).records);
    try std.testing.expectEqual(Controller.quantum_ns, state.snapshot().effective_quantum_ns[1]);
}
