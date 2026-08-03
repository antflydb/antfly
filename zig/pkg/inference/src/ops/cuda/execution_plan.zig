// Copyright 2026 Antfly, Inc.
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

/// Admission result for the current one-buffer-per-allocation arena layout.
/// Every learned allocation ordinal owns a distinct `cuMemAlloc` address, so
/// the sum of its byte lengths is the arena's simultaneous physical high-water
/// request. This is deliberately separate from allocation liveness: when the
/// allocator gains liveness coloring, this accounting must move to the
/// resulting physical slots rather than the logical trace.
pub const TempArenaAdmission = struct {
    physical_high_water_bytes: usize,
    largest_slot_bytes: usize,
    budget_bytes: usize,
    overflowed: bool,
    admitted: bool,
};

pub fn tempArenaAdmission(lengths: []const usize, budget_bytes: usize) TempArenaAdmission {
    var physical_high_water_bytes: usize = 0;
    var largest_slot_bytes: usize = 0;
    var overflowed = false;
    for (lengths) |byte_len| {
        largest_slot_bytes = @max(largest_slot_bytes, byte_len);
        physical_high_water_bytes = std.math.add(usize, physical_high_water_bytes, byte_len) catch blk: {
            overflowed = true;
            break :blk std.math.maxInt(usize);
        };
    }
    return .{
        .physical_high_water_bytes = physical_high_water_bytes,
        .largest_slot_bytes = largest_slot_bytes,
        .budget_bytes = budget_bytes,
        .overflowed = overflowed,
        .admitted = !overflowed and lengths.len != 0 and physical_high_water_bytes <= budget_bytes,
    };
}

/// A host-side description of the temporary-buffer ABI for one decode
/// iteration. CUDA graphs retain device addresses, so capture is only safe
/// after both the allocation count and every requested byte length have been
/// observed to repeat.
///
/// The planner deliberately learns at decode-iteration boundaries rather than
/// relying on a model-specific allocation-count constant. It is therefore
/// valid across model geometries, CUDA architectures, kernel schedules, and
/// optional fusions. Any topology change invalidates readiness and requires a
/// fresh matching observation before capture can resume.
pub const TempArenaPlanner = struct {
    const required_matching_iterations: u8 = 2;

    /// Capture-warmup budget for graph-replay-required callers, measured in
    /// decode-iteration boundaries. A cold trace reaches `ready_for_capture`
    /// in four boundaries (two matching observations, activation, one pinned
    /// validation iteration) and each topology drift costs one invalidation
    /// boundary plus a three-boundary relearn. 64 boundaries therefore covers
    /// more than a dozen legitimate drift cycles (distinct decode phases,
    /// schedule-threshold crossings) while still guaranteeing that a trace
    /// which never stabilizes fails closed instead of running eager forever.
    pub const max_capture_warmup_boundaries: usize = 64;

    candidate_lengths: std.ArrayListUnmanaged(usize) = .empty,
    current_lengths: std.ArrayListUnmanaged(usize) = .empty,
    observing: bool = false,
    active: bool = false,
    ready_for_capture: bool = false,
    matching_iterations: u8 = 0,
    allocation_seq_base: usize = 0,
    capture_warmup_boundaries: usize = 0,
    observations: usize = 0,
    activations: usize = 0,
    invalidations: usize = 0,
    planned_physical_high_water_bytes: usize = 0,
    peak_planned_physical_high_water_bytes: usize = 0,
    admission_budget_bytes: usize = 0,
    admission_denials: usize = 0,
    disabled: bool = false,
    pinned_validation_failed: bool = false,
    replay_validation_failed: bool = false,

    pub const BoundaryResult = enum {
        observing,
        unchanged,
        activated,
        invalidated,
    };

    pub fn deinit(self: *TempArenaPlanner, allocator: std.mem.Allocator) void {
        self.candidate_lengths.deinit(allocator);
        self.current_lengths.deinit(allocator);
        self.* = .{};
    }

    pub fn period(self: *const TempArenaPlanner) usize {
        return if (self.active) self.candidate_lengths.items.len else 0;
    }

    /// Account and admit the learned physical layout before any pinned slot is
    /// installed. A denial permanently disables this planner instance: simply
    /// observing the same oversized trace again cannot make it safe, and CUDA
    /// graph-required callers must continue through their existing fail-closed
    /// path instead of capturing unbudgeted addresses.
    pub fn admitCandidate(self: *TempArenaPlanner, budget_bytes: usize) TempArenaAdmission {
        const admission = tempArenaAdmission(self.candidate_lengths.items, budget_bytes);
        self.planned_physical_high_water_bytes = admission.physical_high_water_bytes;
        self.peak_planned_physical_high_water_bytes = @max(
            self.peak_planned_physical_high_water_bytes,
            admission.physical_high_water_bytes,
        );
        self.admission_budget_bytes = budget_bytes;
        if (!admission.admitted) {
            self.admission_denials += 1;
            self.disabled = true;
            self.active = false;
            self.ready_for_capture = false;
        }
        return admission;
    }

    pub fn noteAllocation(
        self: *TempArenaPlanner,
        allocator: std.mem.Allocator,
        byte_len: usize,
        max_slots: usize,
    ) !void {
        if (self.disabled or !self.observing) return;
        if (byte_len == 0 or self.current_lengths.items.len >= max_slots) {
            self.disabled = true;
            self.active = false;
            self.ready_for_capture = false;
            return;
        }
        try self.current_lengths.append(allocator, byte_len);
    }

    pub fn markPinnedValidationFailure(self: *TempArenaPlanner) void {
        if (self.active) self.pinned_validation_failed = true;
    }

    /// Whether graph-replay-required callers must stop treating the missing
    /// capture readiness as warmup. A disabled planner is excluded: that state
    /// already fails closed through the required-mode capture path.
    pub fn captureWarmupExhausted(self: *const TempArenaPlanner) bool {
        return !self.disabled and !self.ready_for_capture and
            self.capture_warmup_boundaries >= max_capture_warmup_boundaries;
    }

    /// Return whether a graph that skips `allocation_count` host allocations
    /// starts at the expected point in the learned per-iteration ABI.
    pub fn canGraphReplay(
        self: *const TempArenaPlanner,
        allocation_seq: usize,
        allocation_count: usize,
    ) bool {
        if (!self.active or !self.ready_for_capture or self.pinned_validation_failed or
            self.replay_validation_failed)
        {
            return false;
        }
        const candidate = self.candidate_lengths.items;
        if (candidate.len == 0 or allocation_seq < self.allocation_seq_base) return false;
        const phase = (allocation_seq - self.allocation_seq_base) % candidate.len;
        if (phase != self.current_lengths.items.len) return false;
        if (allocation_count > candidate.len - phase) return false;
        if (self.current_lengths.capacity < phase + allocation_count) return false;
        return std.mem.eql(usize, candidate[0..phase], self.current_lengths.items);
    }

    /// CUDA graph replay skips the captured host allocation calls. Rebuild
    /// that exact interval from the learned ABI so the next decode boundary
    /// validates the complete iteration, including any uncaptured prefix or
    /// tail. Merely marking the interval as replayed is unsafe: a missing or
    /// extra tail allocation can phase-shift cyclic pinned slots, and repeated
    /// byte lengths can otherwise hide the drift.
    pub fn markGraphReplay(
        self: *TempArenaPlanner,
        allocation_seq: usize,
        allocation_count: usize,
    ) bool {
        if (!self.canGraphReplay(allocation_seq, allocation_count)) {
            if (self.active) self.replay_validation_failed = true;
            return false;
        }
        const phase = self.current_lengths.items.len;
        const end = phase + allocation_count;
        for (self.candidate_lengths.items[phase..end]) |byte_len| {
            self.current_lengths.appendAssumeCapacity(byte_len);
        }
        return true;
    }

    /// Mark the beginning of the next decode iteration. The lengths collected
    /// since the preceding boundary describe the just-finished iteration.
    pub fn beginDecodeIteration(
        self: *TempArenaPlanner,
        allocator: std.mem.Allocator,
        allocation_seq: usize,
    ) !BoundaryResult {
        if (self.disabled) return .unchanged;
        self.capture_warmup_boundaries +|= 1;
        if (!self.observing) {
            self.observing = true;
            self.current_lengths.clearRetainingCapacity();
            return .observing;
        }

        self.observations += 1;

        // A replay-validation or pinned-slot failure means the reconstructed
        // host trace is not trustworthy. Retain the old candidate only as a
        // seed for the next complete eager observation.
        if (self.active and (self.replay_validation_failed or self.pinned_validation_failed)) {
            self.active = false;
            self.ready_for_capture = false;
            self.replay_validation_failed = false;
            self.pinned_validation_failed = false;
            self.invalidations += 1;
            self.matching_iterations = if (self.candidate_lengths.items.len == 0) 0 else 1;
            self.current_lengths.clearRetainingCapacity();
            return .invalidated;
        }

        const matches = std.mem.eql(
            usize,
            self.candidate_lengths.items,
            self.current_lengths.items,
        );
        if (self.active and (!matches or self.pinned_validation_failed)) {
            self.active = false;
            self.ready_for_capture = false;
            self.replay_validation_failed = false;
            self.pinned_validation_failed = false;
            self.invalidations += 1;
            try self.replaceCandidate(allocator);
            self.matching_iterations = if (self.candidate_lengths.items.len == 0) 0 else 1;
            self.current_lengths.clearRetainingCapacity();
            return .invalidated;
        }

        if (self.candidate_lengths.items.len == 0) {
            try self.replaceCandidate(allocator);
            self.matching_iterations = if (self.candidate_lengths.items.len == 0) 0 else 1;
        } else if (matches) {
            if (self.matching_iterations < std.math.maxInt(u8)) self.matching_iterations += 1;
        } else {
            self.invalidations += 1;
            try self.replaceCandidate(allocator);
            self.matching_iterations = if (self.candidate_lengths.items.len == 0) 0 else 1;
        }

        self.current_lengths.clearRetainingCapacity();
        if (self.active) {
            // The first complete iteration using pinned slots matched the
            // learned ABI. Capture may begin at the next boundary, and any
            // subsequent invalidation starts a fresh bounded warmup.
            self.ready_for_capture = true;
            self.capture_warmup_boundaries = 0;
            return .unchanged;
        }
        if (self.matching_iterations >= required_matching_iterations and self.candidate_lengths.items.len != 0) {
            self.active = true;
            self.ready_for_capture = false;
            self.allocation_seq_base = allocation_seq;
            self.activations += 1;
            return .activated;
        }
        return .observing;
    }

    /// Preserve the last validated shape as a seed across request boundaries,
    /// but require the new request to prove it once before pinning addresses.
    pub fn resetForRequest(self: *TempArenaPlanner) void {
        self.current_lengths.clearRetainingCapacity();
        self.observing = false;
        self.active = false;
        self.ready_for_capture = false;
        self.replay_validation_failed = false;
        self.pinned_validation_failed = false;
        self.allocation_seq_base = 0;
        self.capture_warmup_boundaries = 0;
        self.matching_iterations = if (self.candidate_lengths.items.len == 0) 0 else 1;
    }

    fn replaceCandidate(self: *TempArenaPlanner, allocator: std.mem.Allocator) !void {
        self.candidate_lengths.clearRetainingCapacity();
        try self.candidate_lengths.appendSlice(allocator, self.current_lengths.items);
    }
};

test "CUDA temp arena planner requires repeatability and a pinned validation iteration" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);

    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.observing, try planner.beginDecodeIteration(std.testing.allocator, 100));
    try planner.noteAllocation(std.testing.allocator, 64, 16);
    try planner.noteAllocation(std.testing.allocator, 128, 16);
    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.observing, try planner.beginDecodeIteration(std.testing.allocator, 102));
    try std.testing.expect(!planner.active);

    try planner.noteAllocation(std.testing.allocator, 64, 16);
    try planner.noteAllocation(std.testing.allocator, 128, 16);
    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.activated, try planner.beginDecodeIteration(std.testing.allocator, 104));
    try std.testing.expect(planner.active);
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expectEqual(@as(usize, 2), planner.period());
    try std.testing.expectEqual(@as(usize, 104), planner.allocation_seq_base);

    try planner.noteAllocation(std.testing.allocator, 64, 16);
    try planner.noteAllocation(std.testing.allocator, 128, 16);
    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.unchanged, try planner.beginDecodeIteration(std.testing.allocator, 106));
    try std.testing.expect(planner.ready_for_capture);

    // Replayed graphs do not execute host allocation calls.
    try std.testing.expect(planner.markGraphReplay(106, 2));
    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.unchanged, try planner.beginDecodeIteration(std.testing.allocator, 108));
    try std.testing.expect(planner.ready_for_capture);
}

test "CUDA temp arena planner invalidates on shape drift and relearns" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);

    _ = try planner.beginDecodeIteration(std.testing.allocator, 0);
    inline for (0..2) |_| {
        try planner.noteAllocation(std.testing.allocator, 64, 8);
        try planner.noteAllocation(std.testing.allocator, 128, 8);
        _ = try planner.beginDecodeIteration(std.testing.allocator, 2);
    }
    try planner.noteAllocation(std.testing.allocator, 64, 8);
    try planner.noteAllocation(std.testing.allocator, 128, 8);
    _ = try planner.beginDecodeIteration(std.testing.allocator, 4);
    try std.testing.expect(planner.ready_for_capture);

    try planner.noteAllocation(std.testing.allocator, 64, 8);
    try planner.noteAllocation(std.testing.allocator, 256, 8);
    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.invalidated, try planner.beginDecodeIteration(std.testing.allocator, 6));
    try std.testing.expect(!planner.active);
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expectEqualSlices(usize, &.{ 64, 256 }, planner.candidate_lengths.items);
    try std.testing.expectEqual(@as(usize, 1), planner.invalidations);
}

test "CUDA temp arena planner keeps a validated request seed but requires reproving it" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 32, 96 });
    planner.active = true;
    planner.ready_for_capture = true;
    planner.matching_iterations = 3;

    planner.resetForRequest();
    try std.testing.expect(!planner.active);
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expectEqual(@as(u8, 1), planner.matching_iterations);
    _ = try planner.beginDecodeIteration(std.testing.allocator, 200);
    try planner.noteAllocation(std.testing.allocator, 32, 8);
    try planner.noteAllocation(std.testing.allocator, 96, 8);
    try std.testing.expectEqual(TempArenaPlanner.BoundaryResult.activated, try planner.beginDecodeIteration(std.testing.allocator, 202));
}

test "CUDA temp arena planner rejects a repeated shape when a pinned slot misses" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 128 });
    planner.observing = true;
    planner.active = true;
    planner.ready_for_capture = true;
    planner.matching_iterations = 3;

    try planner.noteAllocation(std.testing.allocator, 64, 8);
    planner.markPinnedValidationFailure();
    try planner.noteAllocation(std.testing.allocator, 128, 8);
    try std.testing.expectEqual(
        TempArenaPlanner.BoundaryResult.invalidated,
        try planner.beginDecodeIteration(std.testing.allocator, 2),
    );
    try std.testing.expect(!planner.active);
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expectEqualSlices(usize, &.{ 64, 128 }, planner.candidate_lengths.items);
}

test "CUDA temp arena planner accepts a marked graph replay with uncaptured tail allocations" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 128, 256 });
    try planner.current_lengths.ensureTotalCapacity(std.testing.allocator, 3);
    planner.observing = true;
    planner.active = true;
    planner.ready_for_capture = true;
    planner.matching_iterations = 3;

    // Only the uncaptured tail allocation is visible to the host on replay.
    try std.testing.expect(planner.markGraphReplay(0, 2));
    try planner.noteAllocation(std.testing.allocator, 256, 8);
    try std.testing.expectEqual(
        TempArenaPlanner.BoundaryResult.unchanged,
        try planner.beginDecodeIteration(std.testing.allocator, 3),
    );
    try std.testing.expect(planner.active);
    try std.testing.expect(planner.ready_for_capture);
    try std.testing.expectEqual(@as(usize, 0), planner.current_lengths.items.len);
}

test "CUDA temp arena planner invalidates a replay interval after pinned drift" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 128 });
    try planner.current_lengths.ensureTotalCapacity(std.testing.allocator, 2);
    planner.observing = true;
    planner.active = true;
    planner.ready_for_capture = true;
    planner.matching_iterations = 3;

    try std.testing.expect(planner.markGraphReplay(0, 2));
    planner.markPinnedValidationFailure();
    try planner.noteAllocation(std.testing.allocator, 256, 8);
    try std.testing.expectEqual(
        TempArenaPlanner.BoundaryResult.invalidated,
        try planner.beginDecodeIteration(std.testing.allocator, 2),
    );
    try std.testing.expect(!planner.active);
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expectEqualSlices(usize, &.{ 64, 128 }, planner.candidate_lengths.items);
}

test "CUDA temp arena planner rejects a replay count that crosses the iteration boundary" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 64, 64 });
    try planner.current_lengths.ensureTotalCapacity(std.testing.allocator, 3);
    planner.observing = true;
    planner.active = true;
    planner.ready_for_capture = true;
    planner.matching_iterations = 3;
    planner.allocation_seq_base = 100;

    try planner.noteAllocation(std.testing.allocator, 64, 8);
    try std.testing.expect(!planner.markGraphReplay(101, 3));
    try std.testing.expectEqual(
        TempArenaPlanner.BoundaryResult.invalidated,
        try planner.beginDecodeIteration(std.testing.allocator, 104),
    );
    try std.testing.expect(!planner.active);
}

test "CUDA temp arena planner reconstructs replay and detects a missing tail" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 64, 64 });
    try planner.current_lengths.ensureTotalCapacity(std.testing.allocator, 3);
    planner.observing = true;
    planner.active = true;
    planner.ready_for_capture = true;
    planner.matching_iterations = 3;
    planner.allocation_seq_base = 100;

    try std.testing.expect(planner.markGraphReplay(100, 2));
    try std.testing.expectEqual(
        TempArenaPlanner.BoundaryResult.invalidated,
        try planner.beginDecodeIteration(std.testing.allocator, 102),
    );
    try std.testing.expect(!planner.active);
}

test "CUDA temp arena admission accounts the simultaneous physical high water" {
    const admitted = tempArenaAdmission(&.{ 64, 128, 256 }, 448);
    try std.testing.expectEqual(@as(usize, 448), admitted.physical_high_water_bytes);
    try std.testing.expectEqual(@as(usize, 256), admitted.largest_slot_bytes);
    try std.testing.expectEqual(@as(usize, 448), admitted.budget_bytes);
    try std.testing.expect(!admitted.overflowed);
    try std.testing.expect(admitted.admitted);

    const denied = tempArenaAdmission(&.{ 64, 128, 256 }, 447);
    try std.testing.expectEqual(@as(usize, 448), denied.physical_high_water_bytes);
    try std.testing.expect(!denied.admitted);
}

test "CUDA temp arena admission fails closed on overflow and empty plans" {
    const overflowed = tempArenaAdmission(&.{ std.math.maxInt(usize), 1 }, std.math.maxInt(usize));
    try std.testing.expectEqual(std.math.maxInt(usize), overflowed.physical_high_water_bytes);
    try std.testing.expect(overflowed.overflowed);
    try std.testing.expect(!overflowed.admitted);

    const empty = tempArenaAdmission(&.{}, std.math.maxInt(usize));
    try std.testing.expectEqual(@as(usize, 0), empty.physical_high_water_bytes);
    try std.testing.expect(!empty.overflowed);
    try std.testing.expect(!empty.admitted);
}

test "CUDA temp arena planner admission denial disables unsafe relearning" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 128, 256 });
    planner.active = true;
    planner.ready_for_capture = true;

    const denied = planner.admitCandidate(447);
    try std.testing.expect(!denied.admitted);
    try std.testing.expect(planner.disabled);
    try std.testing.expect(!planner.active);
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expectEqual(@as(usize, 448), planner.planned_physical_high_water_bytes);
    try std.testing.expectEqual(@as(usize, 448), planner.peak_planned_physical_high_water_bytes);
    try std.testing.expectEqual(@as(usize, 447), planner.admission_budget_bytes);
    try std.testing.expectEqual(@as(usize, 1), planner.admission_denials);

    planner.resetForRequest();
    try std.testing.expect(planner.disabled);
    try std.testing.expectEqual(
        TempArenaPlanner.BoundaryResult.unchanged,
        try planner.beginDecodeIteration(std.testing.allocator, 0),
    );
}

test "CUDA temp arena planner records admitted footprint and lifetime peak" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 64, 128 });
    planner.active = true;

    const first = planner.admitCandidate(1024);
    try std.testing.expect(first.admitted);
    try std.testing.expect(!planner.disabled);
    try std.testing.expectEqual(@as(usize, 192), planner.planned_physical_high_water_bytes);
    try std.testing.expectEqual(@as(usize, 192), planner.peak_planned_physical_high_water_bytes);

    planner.candidate_lengths.clearRetainingCapacity();
    try planner.candidate_lengths.appendSlice(std.testing.allocator, &.{ 32, 64 });
    planner.active = true;
    const second = planner.admitCandidate(1024);
    try std.testing.expect(second.admitted);
    try std.testing.expectEqual(@as(usize, 96), planner.planned_physical_high_water_bytes);
    try std.testing.expectEqual(@as(usize, 192), planner.peak_planned_physical_high_water_bytes);
}

test "CUDA temp arena planner exhausts the capture warmup budget on an unstable trace" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);

    // A trace whose byte lengths never repeat keeps the planner learning at
    // every boundary. Required-mode callers must observe budget exhaustion
    // rather than warming up forever.
    var seq: usize = 0;
    var byte_len: usize = 64;
    for (0..TempArenaPlanner.max_capture_warmup_boundaries) |_| {
        try std.testing.expect(!planner.captureWarmupExhausted());
        _ = try planner.beginDecodeIteration(std.testing.allocator, seq);
        try planner.noteAllocation(std.testing.allocator, byte_len, 16);
        byte_len += 64;
        seq += 1;
    }
    try std.testing.expect(planner.captureWarmupExhausted());
    try std.testing.expect(!planner.ready_for_capture);
    try std.testing.expect(!planner.disabled);

    // A fresh request restarts the bounded warmup from a full budget.
    planner.resetForRequest();
    try std.testing.expect(!planner.captureWarmupExhausted());
    try std.testing.expectEqual(@as(usize, 0), planner.capture_warmup_boundaries);
}

test "CUDA temp arena planner keeps capture reachable within the warmup budget" {
    var planner: TempArenaPlanner = .{};
    defer planner.deinit(std.testing.allocator);

    // The clean flow (two matching observations, activation, one pinned
    // validation iteration) stabilizes in four boundaries: far inside the
    // budget, and readiness returns the budget to a fresh warmup episode.
    _ = try planner.beginDecodeIteration(std.testing.allocator, 0);
    inline for (0..3) |iteration| {
        try planner.noteAllocation(std.testing.allocator, 64, 16);
        try planner.noteAllocation(std.testing.allocator, 128, 16);
        _ = try planner.beginDecodeIteration(std.testing.allocator, (iteration + 1) * 2);
        try std.testing.expect(!planner.captureWarmupExhausted());
    }
    try std.testing.expect(planner.ready_for_capture);
    try std.testing.expectEqual(@as(usize, 0), planner.capture_warmup_boundaries);
    try std.testing.expect(!planner.captureWarmupExhausted());
}
