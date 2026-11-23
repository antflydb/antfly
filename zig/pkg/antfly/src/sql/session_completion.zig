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

//! Data-only terminal SQL session completion rules.
//! Kept independent of API coordination and storage-owner clients.
const registry = @import("../system_catalog/prepared_transactions.zig");

/// A ready local delivery receipt is not the physical participant ACK.
/// The existing canonical handoff deletes the retained operation only AFTER
/// native peer/coordinator ACKs. Keep the indexing-only early scope release.
pub fn sessionRequiresPhysicalRecovery(progress: registry.CommitProgress, record: registry.Record) bool {
    if (record.phase == .retired) return false;
    return progress != .indexing or record.phase != .resolved or
        record.native_lock_releases != record.nativeLockMask();
}

/// This path already owns an immutable purpose and terminal decision. An
/// uncertain phase-two ACK or bounded recovery yield remains pending under
/// that SAME identity; it never authorizes another SQL submission.
pub fn sessionPhysicalRecoveryFailure(err: anyerror) anyerror {
    return switch (err) {
        error.CatalogGenerationChanged, error.CommitDecisionUnknown => error.MetadataMutationOutcomeUnknown,
        else => err,
    };
}

/// The driver supplies authenticated retained-state observation and one existing
/// recovery quantum. There is deliberately no SQL execution or plan-refresh
/// callback. Pending replies may yield; every other failure retains its class.
pub fn drainSealedSession(driver: anytype) !void {
    var quanta: u8 = 0;
    while (true) {
        try driver.check();
        const complete = driver.step() catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => false,
            else => return err,
        };
        if (complete) return;
        quanta += 1;
        if (quanta == 8) {
            try driver.pause();
            quanta = 0;
        }
    }
}

test "SQL implicit abort rejoins exact owned terminal abort after sealed rejection and multiple release quanta" {
    const std = @import("std");
    const Driver = struct {
        expected: [16]u8 = @splat(7),
        retained: [16]u8 = @splat(7),
        calls: usize = 0,
        pauses: usize = 0,
        checks: usize = 0,
        release_quanta: usize = 0,
        canceled: bool = false,
        unknown: bool = false,
        committed: bool = false,
        acknowledged: bool = false,
        lose_reply: bool = true,
        pub fn check(self: *@This()) !void {
            self.checks += 1;
            if (self.canceled or self.checks > 40) return error.Canceled;
        }
        pub fn step(self: *@This()) !bool {
            self.calls += 1;
            if (!std.mem.eql(u8, &self.expected, &self.retained)) return error.ExternalTransactionDecisionChanged;
            if (self.acknowledged) return true;
            if (self.unknown) return error.MetadataMutationOutcomeUnknown;
            if (!self.committed) self.release_quanta += 1;
            if (self.lose_reply and self.release_quanta == 5) {
                self.lose_reply = false;
                return error.MetadataMutationOutcomeUnknown;
            }
            if (self.committed or self.release_quanta == 19) self.acknowledged = true;
            return false;
        }
        pub fn pause(self: *@This()) !void {
            self.pauses += 1;
        }
    };
    // A definite rejected admission still has more than one full work quantum
    // of retained native locks. Lost replies never mean completion by absence.
    var rejected: Driver = .{};
    try drainSealedSession(&rejected);
    try std.testing.expectEqual(@as(usize, 19), rejected.release_quanta);
    try std.testing.expectEqual(@as(usize, 20), rejected.calls);
    try std.testing.expectEqual(@as(usize, 2), rejected.pauses);
    try std.testing.expect(rejected.acknowledged);
    var unknown: Driver = .{ .unknown = true };
    try std.testing.expectError(error.Canceled, drainSealedSession(&unknown));
    try std.testing.expect(!unknown.acknowledged and unknown.release_quanta == 0);
    var committed: Driver = .{ .committed = true };
    try drainSealedSession(&committed);
    try std.testing.expect(committed.committed and committed.acknowledged);
    try std.testing.expectEqual(@as(usize, 0), committed.release_quanta);
    var foreign: Driver = .{ .retained = @splat(8) };
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, drainSealedSession(&foreign));
    try std.testing.expectEqual(@as(usize, 0), foreign.release_quanta);
    var canceled: Driver = .{ .canceled = true };
    try std.testing.expectError(error.Canceled, drainSealedSession(&canceled));
    try std.testing.expectEqual(@as(usize, 0), canceled.calls);
}

/// Completion is observed from the same durable Session cut as its retained
/// immutable seal, including after acknowledgement. Admission getters reject
/// those terminal records and therefore cannot establish this proof.
pub const SealedOutcome = enum { unknown, aborted, committed, committed_pending, committed_repair_required };
pub fn sealedTerminalOutcome(state: anytype, expected_seal: []const u8) SealedOutcome {
    const std = @import("std");
    const seal = state.external_preparation orelse return .unknown;
    if (!std.mem.eql(u8, seal, expected_seal)) return .unknown;
    if (state.terminal_abort) return if (state.terminal_commit == null) .aborted else .unknown;
    const terminal = state.terminal_commit orelse return .unknown;
    return switch (terminal.status) {
        .committed => if (terminal.repair_required) .committed_repair_required else .committed,
        .committed_recovery_pending, .committed_visibility_pending => .committed_pending,
    };
}
