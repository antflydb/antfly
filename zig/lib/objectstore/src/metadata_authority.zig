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

//! Shared transactional-root authority. Journal namespaces preserve their exact
//! protocol; new conditional namespaces replace one permanent root atomically.
const std = @import("std");
const journal = @import("immutable_log.zig");
const conditional = @import("conditional_root.zig");
const A = std.mem.Allocator;
pub const Backend = enum { journal, conditional, bounded_journal };
pub const Authority = struct {
    log: journal.Log,
    root: ?conditional.Context = null,
    cursor: ?conditional.Cursor = null,
    journal_auth: ?journal.ProofAuthority = null,
    journal_proof: ?journal.WriteProof = null,
    fn acceptJournal(self: *Authority, result: journal.CertifiedRecovery) journal.Recovery {
        self.journal_proof = result.proof;
        return result.recovery;
    }
    fn certify(self: *Authority, result: journal.Recovery, anchor: journal.ProofTime) !journal.Recovery {
        const auth = self.journal_auth orelse return result;
        self.journal_proof = try auth.issue(self.log, result.state, anchor);
        return result;
    }
    fn observedJournal(self: *Authority, a: A, state: ?journal.State, takeover: bool, follow_owners: bool, read_only: bool) !journal.Recovery {
        const auth = self.journal_auth orelse return if (follow_owners) self.log.observeCommitted(a, state) else if (read_only) self.log.observe(a, state.?) else self.log.recover(a, state, takeover);
        const anchor = auth.now();
        if (!takeover and state != null and (follow_owners or read_only or state.?.next < journal.Log.epoch_commits)) {
            if (self.journal_proof) |proof| {
                if (self.log.observeCertified(a, state.?, auth, proof, follow_owners)) |value| return self.acceptJournal(value) else |err| switch (err) {
                    error.ImmutableLogProofExpired => {
                        if (self.log.refreshCertificate(a, state.?, auth, proof)) |refreshed| return self.acceptJournal(try self.log.observeCertified(a, state.?, auth, refreshed, follow_owners)) else |failure| switch (failure) {
                            error.ImmutableLogProofRequiresRecovery => {},
                            else => return failure,
                        }
                    },
                    error.InvalidImmutableLogProof => {},
                    else => return err,
                }
            }
        }
        const result = if (follow_owners) try self.log.observeCommitted(a, state) else if (read_only) try self.log.observe(a, state.?) else try self.log.recover(a, state, takeover);
        return self.certify(result, anchor);
    }

    fn accept(self: *Authority, value: conditional.Snapshot) journal.Recovery {
        self.cursor = value.cursor();
        return .{ .state = value.state };
    }
    pub fn recover(self: *Authority, a: A, state: ?journal.State, takeover: bool) !journal.Recovery {
        const root = self.root orelse return self.observedJournal(a, state, takeover, false, false);
        if (takeover) return self.accept(try root.begin(a));
        const current = (try root.observe(a)) orelse return error.ConditionalRootMissing;
        if (current.retired) return error.ConditionalRootRetired;
        if (!std.mem.eql(u8, &current.state.owner, &root.owner)) return error.ImmutableLogFenced;
        return self.accept(current);
    }
    pub fn observe(self: *Authority, a: A, state: journal.State) !journal.Recovery {
        const root = self.root orelse {
            if (self.journal_auth != null) return self.observedJournal(a, state, false, false, true);
            return self.log.observe(a, state);
        };
        const current = (try root.observe(a)) orelse return error.ConditionalRootMissing;
        if (current.retired) return error.ConditionalRootRetired;
        if (!std.mem.eql(u8, &current.state.owner, &root.owner)) return error.ImmutableLogFenced;
        return self.accept(current);
    }
    pub fn observeCommitted(self: *Authority, a: A, state: ?journal.State) !journal.Recovery {
        const root = self.root orelse return self.observedJournal(a, state, false, true, true);
        const current = (try root.observe(a)) orelse return error.ConditionalRootMissing;
        if (current.retired) return error.ConditionalRootRetired;
        return self.accept(current);
    }
    fn snapshot(self: Authority, state: journal.State) !conditional.Snapshot {
        const c = self.cursor orelse return error.ConditionalRootRevisionRequired;
        return .{ .state = state, .revision = c.revision, .nonce = c.nonce, .retired = c.retired, .auth = c.auth };
    }
    pub fn append(self: *Authority, a: A, state: journal.State, bytes: []const u8) !journal.State {
        const root = self.root orelse {
            const auth = self.journal_auth orelse return self.log.append(a, state, bytes);
            if (self.journal_proof) |proof| return self.acceptJournal(try self.log.appendCertified(a, state, bytes, auth, proof)).state;
            const anchor = auth.now();
            return (try self.certify(.{ .state = try self.log.append(a, state, bytes) }, anchor)).state;
        };
        return self.accept(try root.append(a, try self.snapshot(state), bytes)).state;
    }
    pub fn writeCheckpoint(self: *Authority, a: A, state: journal.State, force: bool) !bool {
        var root = self.root orelse return self.log.writeCheckpoint(a, state, force);
        // A GC barrier must still authenticate current ownership. ROOT already
        // is the durable recovery floor; it never needs a second checkpoint PUT.
        root.proofs = null;
        root.proof_key = null;
        root.proof_scope = @splat(0);
        try root.verifySource(a, try self.snapshot(state));
        return true;
    }
    pub fn retirable(self: *Authority, a: A, keys: []const []const u8, before: u64) ![]const []const u8 {
        if (self.root != null) return &.{};
        return self.log.retirable(a, keys, before);
    }
};
