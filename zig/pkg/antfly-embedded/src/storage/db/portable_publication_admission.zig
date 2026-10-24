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

//! Portable publication waits without retaining dependencies its readers need.
const std = @import("std");
const admission = @import("mutation_admission.zig");
const Controls = @import("apply_rw_lock.zig").ApplyRwLock.Acquisition;
const PolicyLease = @import("row_policy_gate.zig").Gate.Lease;

const Destination = struct {
    namespace: @import("doc_identity_namespace.zig").Namespace,
    range: @import("../byte_range.zig").ByteRange,

    fn copy(a: std.mem.Allocator, bytes: []const u8, lifetime: anytype) ![]const u8 {
        const owned = try a.alloc(u8, bytes.len);
        errdefer a.free(owned);
        var offset: usize = 0;
        while (offset < bytes.len) {
            try lifetime.check();
            const end = offset + @min(bytes.len - offset, 64 * 1024);
            @memcpy(owned[offset..end], bytes[offset..end]);
            offset = end;
        }
        try lifetime.check();
        return owned;
    }
    fn capture(owner: anytype, a: std.mem.Allocator, lifetime: anytype) !Destination {
        const range = owner.core.byteRange();
        const start = try copy(a, range.start, lifetime);
        errdefer if (start.len != 0) a.free(start);
        const end = try copy(a, range.end, lifetime);
        return .{ .namespace = owner.core.identity_namespace, .range = .{ .start = start, .end = end } };
    }
    fn same(left: []const u8, right: []const u8, lifetime: anytype) !bool {
        if (left.len != right.len) return false;
        var offset: usize = 0;
        while (offset < left.len) {
            try lifetime.check();
            const end = offset + @min(left.len - offset, 64 * 1024);
            if (!std.mem.eql(u8, left[offset..end], right[offset..end])) return false;
            offset = end;
        }
        return true;
    }
    fn matches(self: Destination, owner: anytype, lifetime: anytype) !bool {
        const range = owner.core.byteRange();
        return self.namespace.eql(owner.core.identity_namespace) and
            try same(self.range.start, range.start, lifetime) and
            try same(self.range.end, range.end, lifetime);
    }
};

/// Success transfers apply-exclusive and drained publication exclusion to the
/// caller; its prepared raw-policy lease is live again. All failure paths retire
/// this helper's service ownership. The original scope/CPU boundary stays borrowed.
pub fn acquire(owner: anytype, alloc: std.mem.Allocator, policy: *PolicyLease, controls: ?Controls, comptime pristine: anytype) !void {
    const store = owner.core.store;
    const io = owner.backend_runtime.io() orelse owner.backend_runtime.filesystemIo() orelse std.Options.debug_io;
    const original = controls orelse {
        // A legacy caller may own the sole ordinary CPU without a wait port.
        // Publication is still pre-decision: reject contention so its readers
        // can resume, rather than waiting while retaining that capacity.
        if (!owner.core.apply_mutex.tryLockExclusiveForAdmission()) return error.Backpressured;
        errdefer owner.core.unlockApply();
        if (!try pristine(owner, alloc, null)) return error.LiteImportTargetNotEmpty;
        try store.beginPortableImportPublication(io);
        return;
    };
    try original.lifetime.check();
    std.debug.assert(policy.gate == &owner.local_execution.row_policy_gate);
    // Preparation pins raw policy. Retire it before any apply wait, then check
    // current policy afresh under apply before trusting the prepared generation.
    policy.release();
    errdefer policy.release();
    var destination: ?Destination = null;
    defer if (destination) |*saved| saved.range.deinit(alloc);
    while (true) {
        try original.lifetime.check();
        try admission.lockApplyPreDecision(owner, original);
        var apply_owned = true;
        defer if (apply_owned) owner.core.unlockApply();
        policy.* = try owner.local_execution.row_policy_gate.enterRaw();
        try original.lifetime.check();
        if (destination) |saved| if (!try saved.matches(owner, original.lifetime)) return error.CatalogGenerationChanged;
        if (!try pristine(owner, alloc, admission.retainedLeaseControls(original))) return error.LiteImportTargetNotEmpty;
        if (destination == null) destination = try Destination.capture(owner, alloc, original.lifetime);
        try store.claimPortableImportPublicationWithAdmission(original);
        var publication_owned = true;
        defer if (publication_owned) store.cancelPortableImportPublication();
        if (store.portableImportHasReaders()) {
            policy.release();
            owner.core.unlockApply();
            apply_owned = false;
            try original.lifetime.check();
            if (original.execution_wait == null) return error.Backpressured;
            // Keep new readers excluded while old ones resume/retire. Neither
            // apply nor raw-policy ownership can consume their resume capacity.
            try store.awaitPortableImportPublicationReadersWithAdmission(io, original);
            store.cancelPortableImportPublication();
            publication_owned = false;
            // Writers/policy may have advanced while apply was free. Reacquire
            // and recheck the full pristine-target predicate, never reuse a
            // stale emptiness result or add privileged bypass reads.
            continue;
        }
        // Exclusion prevents new readers, so this readiness check cannot
        // wait. Preserve the final recovery/lifetime check even when the
        // first reader-count observation was already zero.
        try store.awaitPortableImportPublicationReadersWithAdmission(io, admission.retainedLeaseControls(original));
        apply_owned = false;
        publication_owned = false;
        return;
    }
}
