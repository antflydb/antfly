// Copyright 2026 Antfly, Inc.
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

//! The existing cluster-backup attempt's metadata/native-pin adapter. Uploads
//! remain owned by backups.zig; this adapter never holds a write fence across
//! an upload and never substitutes a fresh snapshot for a missing durable pin.
const std = @import("std");
const cohort = @import("../metadata/backup_cohort.zig");
const metadata = @import("../metadata/table_manager.zig");
const metadata_api = @import("../metadata/api.zig");
const operation = @import("operation.zig");
const reads = if (@import("builtin").is_test) @import("table_reads.zig") else @import("table_read_source.zig");
const writes = if (@import("builtin").is_test) @import("table_writes.zig") else @import("table_write_source.zig");
const router_api = @import("table_router.zig");
const catalog_api = @import("table_catalog.zig");
const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
const identity = @import("../storage/db/doc_identity.zig");
const seal = @import("../storage/db/native_backup_seal.zig");

pub fn idForAttempt(attempt: []const u8) u64 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly backup cohort attempt v1");
    hash.update(attempt);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.mem.readInt(u64, digest[0..8], .little) | 1;
}

pub fn Session(comptime Source: type) type {
    return struct {
        const Self = @This();
        alloc: std.mem.Allocator,
        source: Source,
        read: reads.TableReadSource,
        write: writes.TableWriteSource,
        request: operation.RequestContext,
        parsed: std.json.Parsed(cohort.Job),
        seal_capacity: []cohort.SealReceipt,
        plan_digest: [32]u8,

        pub fn deinit(self: *Self) void {
            self.parsed.deinit();
        }

        pub fn admit(alloc: std.mem.Allocator, source: Source, read: reads.TableReadSource, write: writes.TableWriteSource, request: operation.RequestContext, router: router_api.HostedGroupRouter, catalog: catalog_api.CatalogSource, snapshot: *const metadata_api.AdminSnapshot, marker: anytype, location: []const u8, connection: []const u8) !Self {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const selected = try a.dupe(@TypeOf(marker.tables[0]), marker.tables);
            std.mem.sort(@TypeOf(marker.tables[0]), selected, {}, struct {
                fn less(_: void, left: @TypeOf(marker.tables[0]), right: @TypeOf(marker.tables[0])) bool {
                    return std.mem.order(u8, left.name, right.name) == .lt;
                }
            }.less);
            var proofs = std.ArrayListUnmanaged(cohort.TableProof).empty;
            var owners = std.ArrayListUnmanaged(cohort.Owner).empty;
            const id = idForAttempt(marker.attempt_id);
            for (selected) |selection| {
                const table = for (snapshot.tables) |value| {
                    if (std.mem.eql(u8, value.name, selection.name)) break value;
                } else return error.TableNotFound;
                try proofs.append(a, .{ .table_id = table.table_id, .name = table.name, .definition = metadata.tableDefinitionFingerprint(table), .manifest_definition = cohort.manifestDefinition(table.name, table.description, table.schema_json, table.read_schema_json, table.indexes_json, table.replication_sources_json) });
                var ranges = std.ArrayListUnmanaged(metadata.RangeRecord).empty;
                for (snapshot.ranges) |range| if (range.table_id == table.table_id) try ranges.append(a, range);
                metadata.sortKeyspaceRanges(metadata.RangeRecord, ranges.items);
                for (ranges.items) |range| {
                    try request.ensureActive();
                    var response = (try read.topologyStatus(alloc, table.name, range.start_key, "{\"mode\":\"identity\"}")) orelse return error.TableNotFound;
                    defer response.deinit(alloc);
                    const Identity = struct { namespace: identity.Namespace, catalog_digest: [32]u8, next_epoch: u64, backup_seal_supported: bool = false };
                    const native = try std.json.parseFromSlice(Identity, a, response.json, .{ .allocate = .alloc_always });
                    if (!native.value.backup_seal_supported) return error.BackupSealBackendUnsupported;
                    if (native.value.namespace.table_id != table.table_id) return error.CatalogChanged;
                    var route = (try router_api.resolveGroupRoute(alloc, catalog, router, range.group_id, .prefer_leader)) orelse return error.GroupLeaderUnavailable;
                    defer route.deinit(alloc);
                    const capture_node = switch (route) {
                        .local => router.localNodeId(),
                        .remote => |remote| remote.node_id,
                    };
                    if (capture_node == 0) return error.BackupPinSourceUnavailable;
                    try owners.append(a, .{ .table_name = table.name, .range_start = range.start_key, .range_end = range.end_key orelse "", .artifact_id = selection.artifact_backup_id, .capture_node_id = capture_node, .fence = .{ .transition_id = id, .attempt = 1, .admission_epoch = native.value.next_epoch, .owner_group_id = range.group_id, .peer_group_id = range.group_id, .role = .backup_snapshot, .namespace = native.value.namespace, .catalog_digest = native.value.catalog_digest } });
                }
            }
            const proof_json = try std.json.Stringify.valueAlloc(a, .{ .tables = proofs.items, .owners = owners.items }, .{});
            var digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(proof_json, &digest, .{});
            const job: cohort.Job = .{ .id = id, .revision = 1, .backup_id = marker.cluster_backup_id, .attempt_id = marker.attempt_id, .artifact_format = switch (marker.format) {
                .native => .native,
                .portable => .portable,
            }, .location = location, .connection = connection, .tables = proofs.items, .state = .{ .metadata_digest = digest, .owners = owners.items } };
            try job.validate();
            const encoded = try std.json.Stringify.valueAlloc(a, job, .{});
            try source.compareAndSetBackupCohort(alloc, .{ .job_id = id, .expected_revision = 0, .value = encoded }, request);
            return (try load(alloc, source, read, write, request, id)) orelse return error.BackupCohortChanged;
        }

        pub fn load(alloc: std.mem.Allocator, source: Source, read: reads.TableReadSource, write: writes.TableWriteSource, request: operation.RequestContext, id: u64) !?Self {
            const encoded = (try source.getBackupCohort(alloc, id, request)) orelse return null;
            defer alloc.free(encoded);
            var parsed = try std.json.parseFromSlice(cohort.Job, alloc, encoded, .{ .allocate = .alloc_always });
            errdefer parsed.deinit();
            try parsed.value.validate();
            const seal_capacity = try parsed.arena.allocator().alloc(cohort.SealReceipt, parsed.value.state.owners.len);
            if (parsed.value.seals.len > seal_capacity.len) return error.InvalidBackupCohort;
            @memcpy(seal_capacity[0..parsed.value.seals.len], parsed.value.seals);
            parsed.value.seals = seal_capacity[0..parsed.value.seals.len];
            return .{ .alloc = alloc, .source = source, .read = read, .write = write, .request = request, .parsed = parsed, .seal_capacity = seal_capacity, .plan_digest = try parsed.value.planDigest(alloc) };
        }

        fn checkpoint(self: *Self, phase: cohort.Phase, cursor: usize, receipt: ?cohort.SealReceipt, digest: ?[32]u8) !void {
            try self.request.ensureActive();
            var replacement: cohort.Progress = .{ .revision = self.parsed.value.revision + 1, .phase = phase, .cursor = cursor, .owner_count = @intCast(self.parsed.value.state.owners.len), .plan_digest = self.plan_digest, .manifest_sha256 = self.parsed.value.manifest_sha256 };
            if (digest) |value| replacement.manifest_sha256 = value;
            const json = try std.json.Stringify.valueAlloc(self.alloc, replacement, .{});
            defer self.alloc.free(json);
            try self.source.compareAndSetBackupCohort(self.alloc, .{ .job_id = self.parsed.value.id, .expected_revision = self.parsed.value.revision, .value = json, .seal = receipt }, self.request);
            try replacement.applyTo(&self.parsed.value);
            if (receipt) |value| {
                if (self.parsed.value.seals.len == self.seal_capacity.len) return error.InvalidBackupCohort;
                self.seal_capacity[self.parsed.value.seals.len] = value;
                self.parsed.value.seals = self.seal_capacity[0 .. self.parsed.value.seals.len + 1];
            }
        }

        fn next(self: *Self, phase: cohort.Phase, receipt: ?cohort.SealReceipt) !void {
            const state = self.parsed.value.state;
            if (state.cursor + 1 == state.owners.len) try self.checkpoint(phase, 0, receipt, null) else try self.checkpoint(state.phase, state.cursor + 1, receipt, null);
        }

        fn control(self: *Self, owner: cohort.Owner, action: @FieldType(topology.Command, "action")) !void {
            try self.request.ensureActive();
            _ = (try self.write.batch(self.alloc, owner.table_name, .{ .relational_topology = .{ .fence = owner.fence, .action = action } })) orelse return error.TableNotFound;
        }

        fn pin(self: *Self, owner: cohort.Owner, command: seal.Request) ![]u8 {
            try self.request.ensureActive();
            return (try self.write.backupPinControl(self.alloc, owner.table_name, owner.fence.owner_group_id, command, .{ .deadline_ns = self.request.deadline_ns orelse return error.InvalidArgument, .cancellation = self.request.cancellation, .capture_node_id = owner.capture_node_id })) orelse return error.BackupPinSourceUnavailable;
        }

        pub const Preparation = enum { advanced, waiting, ready };

        /// One owner action per slice; callers delay only while waiting. No table-size
        /// work occurs before this reaches exporting.
        pub fn prepareSlice(self: *Self) !Preparation {
            try self.request.ensureActive();
            const state = self.parsed.value.state;
            const owner = state.owners[state.cursor];
            switch (state.phase) {
                .freezing => {
                    try self.control(owner, .begin);
                    try self.next(.draining, null);
                },
                .draining => {
                    var response = (try self.read.topologyStatus(self.alloc, owner.table_name, owner.range_start, "{\"mode\":\"status\"}")) orelse return error.TableNotFound;
                    defer response.deinit(self.alloc);
                    const status = try std.json.parseFromSlice(cohort.Observation, self.alloc, response.json, .{});
                    defer status.deinit();
                    if (status.value.fence == null or !status.value.fence.?.eql(owner.fence)) return error.BackupCohortFenceLost;
                    if (!status.value.drained) return .waiting;
                    try self.next(.capturing, null);
                },
                .capturing => {
                    const body = try self.pin(owner, .{ .seal = .{ .id = owner.artifact_id, .fence = owner.fence } });
                    defer self.alloc.free(body);
                    const parsed = try std.json.parseFromSlice(seal.Handle, self.alloc, body, .{});
                    defer parsed.deinit();
                    if (!parsed.value.fence.eql(owner.fence)) return error.BackupCohortFenceLost;
                    try self.next(.releasing, .{ .handle = parsed.value, .source_node_id = owner.capture_node_id });
                },
                .releasing => {
                    try self.control(owner, .release);
                    try self.next(.exporting, null);
                },
                .exporting, .publishing, .reclaiming, .completed => return .ready,
                .cancelling, .cancel_reclaiming, .cancelled => return error.Canceled,
            }
            return if (self.parsed.value.state.phase == .exporting) .ready else .advanced;
        }

        /// Called only after ALL selected immutable table manifests have been
        /// verified/published by the existing artifact publisher.
        pub fn exported(self: *Self) !void {
            while (self.parsed.value.state.phase == .exporting) try self.next(.publishing, null);
            if (self.parsed.value.state.phase != .publishing) return error.BackupCohortChanged;
        }

        pub fn committed(self: *Self, digest: [32]u8) !void {
            if (self.parsed.value.state.phase == .publishing) try self.checkpoint(.reclaiming, 0, null, digest);
            if (self.parsed.value.manifest_sha256 == null or !std.mem.eql(u8, &self.parsed.value.manifest_sha256.?, &digest)) return error.BackupCohortChanged;
            while (self.parsed.value.state.phase == .reclaiming) {
                const owner = self.parsed.value.state.owners[self.parsed.value.state.cursor];
                // Receipts are appended in the immutable owner's ordinal
                // order and reconstructed in that order by the durable getter.
                // Reclaim must stay O(owners), not search every seal per owner.
                const ordinal = self.parsed.value.state.cursor;
                if (ordinal >= self.parsed.value.seals.len) return error.BackupCohortChanged;
                const receipt = self.parsed.value.seals[ordinal];
                if (!receipt.handle.fence.eql(owner.fence) or receipt.source_node_id != owner.capture_node_id) return error.BackupCohortChanged;
                const body = try self.pin(owner, .{ .release = receipt.handle });
                self.alloc.free(body);
                try self.next(.completed, null);
            }
        }

        /// Caller has already fenced repository publication using the existing
        /// attempt writer/cleanup protocol, not a racy manifest absence check.
        pub fn canceledAfterPublicationFence(self: *Self) !void {
            if (self.parsed.value.state.phase == .completed or self.parsed.value.state.phase == .reclaiming) return error.BackupCohortAlreadyCommitted;
            if (self.parsed.value.state.phase != .cancelling and self.parsed.value.state.phase != .cancel_reclaiming and self.parsed.value.state.phase != .cancelled) try self.checkpoint(.cancelling, 0, null, null);
            while (self.parsed.value.state.phase == .cancelling) {
                const owner = self.parsed.value.state.owners[self.parsed.value.state.cursor];
                try self.control(owner, .cancel);
                try self.next(.cancel_reclaiming, null);
            }
            while (self.parsed.value.state.phase == .cancel_reclaiming) {
                const owner = self.parsed.value.state.owners[self.parsed.value.state.cursor];
                const body = try self.pin(owner, .{ .cancel = owner.fence });
                self.alloc.free(body);
                try self.next(.cancelled, null);
            }
        }
    };
}
