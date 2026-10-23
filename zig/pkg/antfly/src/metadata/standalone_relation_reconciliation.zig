// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded relation catalog progression for the single durable metadata owner.
//! Publication uses the same source CAS, local proof and capability activation
//! as replicated metadata, without manufacturing Raft terms or apply receipts.
const std = @import("std");
const worker = @import("relation_reconciliation_worker.zig");
const control = @import("relation_reconciliation_command.zig");
const protocol = @import("topology_protocol.zig");
const r = @import("antfly_local_sources").system_catalog_relation_reconciliation;
const incarnation = @import("antfly_local_sources").metadata_incarnation;

pub const Coordinator = struct {
    next_round_at_ns: u64 = 0,
    prefer_gc: bool = false,

    /// Caller serializes this lane with catalog mutations and excludes HA
    /// owners. One durable intent at most per round; scans are bounded by the
    /// storage owner and every apply rechecks the observed source/root cut.
    /// true means exactly one control intent was successfully applied.
    pub fn step(self: *Coordinator, store: anytype, alloc: std.mem.Allocator, group: u64, identity: incarnation.MetadataClusterIncarnation, node_id: u64, now_ns: u64) !bool {
        if (now_ns < self.next_round_at_ns) return false;
        self.next_round_at_ns = now_ns +| worker.round_interval_ns;
        try store.expireRelationPublicationProofs(now_ns);
        const activation: protocol.Activation = .{
            // Standalone has one binary and one metadata/data membership.
            // Advertise its current decoder cohort, not the original relation
            // mutation floor: later view, role-default and sequence records
            // must not rely on a synthetic test-only activation. Feature
            // readiness, publication/owner ACKs and SQL ingress capabilities
            // remain separate gates. HA is excluded by the caller.
            .version = protocol.current_version,
            .incarnation = identity,
            .member_count = 1,
            .membership_fingerprint = @import("reallocation_request.zig").membershipFingerprint(&.{node_id}),
        };
        const admitted = try store.topologyActivation(group);
        if (admitted == null or !admitted.?.satisfies(activation)) {
            const bytes = try std.json.Stringify.valueAlloc(alloc, activation, .{});
            defer alloc.free(bytes);
            try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = bytes });
            return true;
        }
        const work = try store.relationReconciliationWork(group);
        const command: control.Command = blk: {
            // Share the replicated worker's generation-building migration.
            // A relation-only seal is not proof of constraint-name coverage.
            var occupancy = activation;
            occupancy.version = protocol.constraint_occupancy_version;
            if (try worker.coverageMigration(work, group, occupancy)) |migration| {
                try store.cancelRelationPublicationProof(group);
                break :blk migration;
            }
            if (!(self.prefer_gc and work.garbage != null)) {
                if (work.current) |state| {
                    if (state.epoch.eql(work.epoch.?) and state.phase == .ready and state.failure == .none and
                        (work.root == null or !work.root.?.eql(r.Generation.of(&state))))
                    {
                        const proof = (try store.stepRelationPublicationProof(state, now_ns)) orelse return false;
                        var publication_activation = activation;
                        publication_activation.version = control.publicationVersion(proof.state.coverage);
                        break :blk .{ .publish = .{ .state = proof.state, .prior = proof.root, .activation = publication_activation } };
                    }
                }
            }
            try store.cancelRelationPublicationProof(group);
            break :blk (try worker.nextIntent(work, group, self.prefer_gc)) orelse return false;
        };
        const bytes = try command.encodeAlloc(alloc);
        defer alloc.free(bytes);
        try store.applyStandaloneCommand(group, .{ .apply_relation_reconciliation = bytes });
        self.prefer_gc = command != .garbage;
        return true;
    }
};
