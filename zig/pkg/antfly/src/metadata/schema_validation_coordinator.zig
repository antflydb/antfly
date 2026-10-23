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

//! All-owner metadata decisions share the sparse owner work feed. Physical
//! execution stays local to each data owner; no coordinator-side owner fanout,
//! retained read transaction, timer thread or second restore system is needed.
const std = @import("std");
const work = @import("schema_validation_work.zig");
const ledger = @import("schema_validation_ledger.zig");
const receipts = @import("schema_validation_receipt.zig");
const operation = @import("antfly_local_sources").api_operation;

pub fn isDeferred(err: anyerror) bool {
    return @import("authority.zig").isRetryableError(err) or switch (err) {
        error.Canceled,
        error.Cancelled,
        error.DeadlineExceeded,
        error.Timeout,
        error.MetadataUnavailable,
        error.MetadataMutationOutcomeUnknown,
        error.MetadataReplicationPending,
        error.GenerationPublicationChanged,
        error.CatalogGenerationChanged,
        error.CatalogPublicationProofPending,
        error.CatalogOccupancyNotReady,
        error.IndexOwnerReceiptChanged,
        error.StoreRootEnrollmentChanged,
        error.ResourceBudgetExceeded,
        error.OutOfMemory,
        error.ResourceTemporarilyUnavailable,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.RemoteUnavailable,
        error.TableTopologyProtocolUpgradeRequired,
        => true,
        else => false,
    };
}

pub const Coordinator = struct {
    running: std.atomic.Value(bool) = .init(false),
    incarnation: [32]u8 = @splat(0),
    cursor: work.Cursor = .{ .scan = 0 },
    idle_until_ns: u64 = 0,

    /// Host supplies CURRENT metadata leadership/native writer authority.
    /// Both receiving metadata reads and accepting writes independently rejoin
    /// that authority; this scheduling identity is never an execution grant.
    pub fn step(self: *@This(), a: std.mem.Allocator, group: u64, host: anytype, now_ns: u64) !bool {
        if (self.running.swap(true, .acq_rel)) return false;
        defer self.running.store(false, .release);
        const expected = (try host.currentAuthority()) orelse {
            self.cursor = .{ .scan = 0 };
            self.idle_until_ns = 0;
            return false;
        };
        if (!std.mem.eql(u8, &self.incarnation, &expected.metadata_incarnation)) {
            self.incarnation = expected.metadata_incarnation;
            self.cursor = .{ .scan = 0 };
            self.idle_until_ns = 0;
        }
        if (now_ns < self.idle_until_ns) return false;
        const context: operation.RequestContext = .{ .deadline_ns = now_ns +| std.time.ns_per_s, .schema_validation_authority = true };
        const query: work.Query = .{ .group = group, .metadata_incarnation = self.incarnation, .cursor = self.cursor };
        const bytes = host.systemCatalog(a, context, .{ .schema_validation_work = query }) catch |err| {
            if (err == error.GenerationPublicationChanged) self.cursor = .{ .scan = 0 };
            self.idle_until_ns = now_ns +| std.time.ns_per_s;
            return err;
        };
        defer a.free(bytes);
        if (bytes.len == 0 or bytes.len > work.max_wire_bytes) return error.InvalidCatalogRecord;
        var page = try std.json.parseFromSlice(work.Page, a, bytes, .{ .ignore_unknown_fields = false });
        defer page.deinit();
        try page.value.validate(query);
        const actual = (try host.currentAuthority()) orelse return error.NotLeader;
        if (!std.meta.eql(actual, expected)) return error.NotLeader;
        // Cursors are hints. A restart/lost reply re-reads the current durable
        // head; it never resumes a caller-chosen physical action or old plan.
        self.cursor = page.value.next orelse .{ .scan = 0 };
        if (page.value.next == null) self.idle_until_ns = now_ns +| std.time.ns_per_s;
        const head = page.value.head orelse return page.value.next != null;
        const decision: ledger.Decision = .{ .lookup = .fromHead(head), .revision = head.revision };
        const command: ledger.Command = switch (head.phase) {
            .validated => .{ .install = decision },
            .installed => .{ .publish = decision },
            else => return page.value.next != null,
        };
        const next = if (command == .install) try head.decideInstall(head.revision) else try head.publish(head.revision);
        const expected_receipt = try ledger.commandReceipt(command, next);
        const encoded = try std.json.Stringify.valueAlloc(a, command, .{});
        defer a.free(encoded);
        // Exactly one attempt in this quantum. Acceptance is an immutable CAS
        // decision, not a physical effect. Unknown delivery re-enters discovery;
        // current phase suppresses resubmission after durable acceptance.
        const raw = try host.systemCatalog(a, context, .{ .schema_validation_submit = encoded });
        defer a.free(raw);
        if (raw.len == 0 or raw.len > 32 * 1024) return error.MetadataMutationOutcomeUnknown;
        var observed = try std.json.parseFromSlice(receipts.Observation, a, raw, .{ .ignore_unknown_fields = false });
        defer observed.deinit();
        if (!try observed.value.isAcknowledged(expected_receipt)) return error.MetadataMutationOutcomeUnknown;
        return true;
    }
};
