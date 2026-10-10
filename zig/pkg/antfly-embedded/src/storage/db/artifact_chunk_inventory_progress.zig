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

//! Receiver-verified inventory reconstruction. These ordered controls establish
//! the old set a producer must replace; they never certify accepted output.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const reconstruction = @import("artifact_chunk_reconstruction.zig");
const keys = @import("../internal_keys.zig");
const Plan = @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot;
const Request = @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest;
const Claim = @import("artifact_stream_census.zig").Claim;
const prefix = "\x00\x00__artifact_publication__:chunk-inventory:";
const Key = [prefix.len + 80]u8;
const Encoded = [361]u8;
const Record = struct { root: u128, checkpoint: reconstruction.Checkpoint, claim: Claim };

fn key(root: u128, checkpoint: reconstruction.Checkpoint) Key {
    var raw: Key = undefined;
    @memcpy(raw[0..prefix.len], prefix);
    @memcpy(raw[prefix.len..][0..24], &checkpoint.authority.namespace);
    std.mem.writeInt(u64, raw[prefix.len + 24 ..][0..8], checkpoint.authority.epoch, .big);
    std.mem.writeInt(u128, raw[prefix.len + 32 ..][0..16], root, .big);
    @memcpy(raw[prefix.len + 48 ..], &checkpoint.scope_digest);
    return raw;
}

fn checksum(selected: *const Key, raw: []const u8) publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:chunk-inventory-progress:v1:");
    hash.update(selected);
    hash.update(raw);
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

fn fingerprint(checkpoint: reconstruction.Checkpoint, complete: bool) !publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:chunk-inventory-page:v1:");
    hash.update(&try checkpoint.encode());
    hash.update(&.{@intFromBool(complete)});
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

fn encode(record: Record) !Encoded {
    var raw: Encoded = @splat(0);
    @memcpy(raw[0..4], "ACI1");
    std.mem.writeInt(u128, raw[4..20], record.root, .big);
    @memcpy(raw[20..265], &try record.checkpoint.encode());
    @memcpy(raw[265..297], &record.claim.before);
    @memcpy(raw[297..329], &record.claim.after);
    @memcpy(raw[329..361], &checksum(&key(record.root, record.checkpoint), raw[0..329]));
    return raw;
}

const Loaded = struct { record: Record, digest: publication.Digest };
fn load(txn: anytype, selected: *const Key) !?Loaded {
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "ACI1") or
        !std.mem.eql(u8, raw[329..361], &checksum(selected, raw[0..329]))) return error.ArtifactCatalogCorrupt;
    const record: Record = .{ .root = std.mem.readInt(u128, raw[4..20], .big), .checkpoint = try reconstruction.Checkpoint.decode(raw[20..265]), .claim = .{ .before = raw[265..297].*, .after = raw[297..329].* } };
    if (record.root == 0 or record.checkpoint.manifest.count == 0 or
        !std.mem.eql(u8, selected, &key(record.root, record.checkpoint)) or
        !std.mem.eql(u8, &record.claim.after, &try fingerprint(record.checkpoint, false)) or
        std.mem.eql(u8, &record.claim.before, &record.claim.after)) return error.ArtifactCatalogCorrupt;
    return .{ .record = record, .digest = raw[329..361].* };
}

fn stamp(txn: anytype) !?[40]u8 {
    const raw = txn.get(@import("artifact_inventory.zig").local_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 40) return error.ArtifactCatalogCorrupt;
    return raw[0..40].*;
}

pub const Prepared = struct {
    alloc: std.mem.Allocator,
    document: []u8,
    manifest: []u8,
    selected: Key,
    expected: ?publication.Digest,
    catalog_stamp: ?[40]u8,
    previous: reconstruction.Checkpoint,
    record: Record,
    complete: bool,
    duplicate: bool,
    unit: bool,
    visits: u32,
    bytes: u32,

    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.document);
        self.alloc.free(self.manifest);
        self.* = undefined;
    }

    pub fn command(self: *const Prepared, producer: []const u8) publication.Command {
        const authority = self.record.checkpoint.authority;
        var result: publication.Command = .{ .mode = .census, .producer_kind = .enrichment, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = producer, .producer_generation = authority.epoch, .producer_artifact_name = producer, .producer_scope_key = if (self.unit) self.manifest else "", .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .census = .{ .document_key = self.document, .chunk_name = producer, .visits = self.visits, .bytes = self.bytes, .before = self.record.claim.before, .after = self.record.claim.after } };
        result.publication_digest = result.digest();
        return result;
    }

    /// Shares the owner's final writer, applied marker and standby outbox.
    /// Only fixed metadata and predecessor/revision CAS run under that writer.
    pub fn stage(self: *const Prepared, txn: anytype, root: u128) !bool {
        if (root == 0 or root != self.record.root) return error.DurableRootIncarnationUnavailable;
        if (!std.meta.eql(self.catalog_stamp, try stamp(txn))) return error.ArtifactCatalogDrift;
        const current = try load(txn, &self.selected);
        const actual: ?publication.Digest = if (current) |value| value.digest else null;
        if (!std.meta.eql(self.expected, actual)) return error.EnrichmentSourceChanged;
        const state = (try @import("artifact_producer_obligations.zig").load(txn)) orelse return error.ArtifactCatalogDrift;
        try state.requireAuthority(self.record.checkpoint.authority);
        if (state.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
        if (self.duplicate) {
            try self.record.checkpoint.requireCurrent(txn, self.manifest);
            return false;
        }
        try self.previous.requireCurrent(txn, self.manifest);
        const present = txn.get(self.manifest) catch |err| if (err == error.NotFound) null else return err;
        if (present != null) return error.EnrichmentSourceChanged;
        if (self.complete) {
            try txn.put(self.manifest, &self.record.checkpoint.manifest.encode());
            try txn.delete(&self.selected);
        } else try txn.put(&self.selected, &try encode(self.record));
        return self.complete;
    }
};

pub fn prepare(alloc: std.mem.Allocator, txn: anytype, root: u128, request: Request, plan: *const Plan, unit: ?[]const u8, limits: reconstruction.Limits, claim: ?Claim) !?Prepared {
    if (limits.rows == 0 or limits.rows > 128 or limits.bytes == 0 or limits.bytes > 64 * 1024) return error.InvalidBatchRequest;
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (request.kind != .chunk_text or (unit != null) != (request.upstream_artifact_name.len != 0)) return error.OnlineMergeArtifactTailsUnsupported;
    const producer = if (request.artifact_name.len != 0) request.artifact_name else request.index_name;
    const authority = if (unit != null) blk: {
        // Unit chunks are children of the extraction worker, not separate
        // generated templates. Bind its exact immutable parent and catalog
        // child, using the same authorization as producer input capture.
        var parent = for (plan.generated_templates) |candidate| {
            if (candidate.kind == .asset and std.mem.eql(u8, candidate.artifact_name, request.upstream_artifact_name)) break candidate;
        } else return error.ArtifactCatalogDrift;
        parent.doc_key = request.doc_key;
        break :blk (try @import("artifact_chunk_publication.zig").authorizeUnitChild(alloc, txn, parent, producer, plan)) orelse return error.ArtifactCatalogDrift;
    } else ((try @import("artifact_producer_input.zig").authorizeTemplate(alloc, txn, request, plan)) orelse return error.ArtifactCatalogDrift).authority;
    const manifest = try chunks.scopedKeyAlloc(alloc, request.doc_key, producer, unit);
    var transferred = false;
    defer if (!transferred) alloc.free(manifest);
    if (manifest.len > 1024 * 1024) return error.ResourceBudgetExceeded;
    var scope_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(manifest, &scope_digest, .{});
    const initial: reconstruction.Checkpoint = .{ .authority = authority, .scope_digest = scope_digest, .stream_revision = try publication.artifactRevision(txn, authority.namespace, manifest), .manifest = chunks.Builder.init().finish() };
    const selected = key(root, initial);
    const old = try load(txn, &selected);
    var previous = initial;
    if (old) |stored| {
        const current = blk: {
            stored.record.checkpoint.requireCurrent(txn, manifest) catch |err| switch (err) {
                error.EnrichmentSourceChanged => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        if (current) {
            if (claim) |expected| if (std.meta.eql(expected, stored.record.claim)) {
                const catalog_stamp = try stamp(txn);
                const document = try alloc.dupe(u8, request.doc_key);
                transferred = true;
                return .{ .alloc = alloc, .document = document, .manifest = manifest, .selected = selected, .expected = stored.digest, .catalog_stamp = catalog_stamp, .previous = stored.record.checkpoint, .record = stored.record, .complete = false, .duplicate = true, .unit = unit != null, .visits = @intCast(limits.rows), .bytes = @intCast(limits.bytes) };
            };
            previous = stored.record.checkpoint;
        }
    }
    var page = (try reconstruction.scan(alloc, txn, .{ .document = request.doc_key, .producer = producer, .unit = unit }, previous, limits)) orelse {
        if (claim != null) return error.EnrichmentSourceChanged;
        return null;
    };
    defer page.deinit();
    const actual_claim: Claim = .{ .before = try fingerprint(page.previous, false), .after = try fingerprint(page.next, page.at_end) };
    if (claim) |expected| if (!std.meta.eql(expected, actual_claim)) return error.EnrichmentSourceChanged;
    const catalog_stamp = try stamp(txn);
    const document = try alloc.dupe(u8, request.doc_key);
    transferred = true;
    return .{ .alloc = alloc, .document = document, .manifest = manifest, .selected = selected, .expected = if (old) |value| value.digest else null, .catalog_stamp = catalog_stamp, .previous = page.previous, .record = .{ .root = root, .checkpoint = page.next, .claim = actual_claim }, .complete = page.at_end, .duplicate = false, .unit = unit != null, .visits = @max(1, page.next.manifest.count - page.previous.manifest.count), .bytes = @intCast(limits.bytes) };
}

pub fn prepareCommand(alloc: std.mem.Allocator, txn: anytype, root: u128, command: publication.Command, plan: *const Plan) !Prepared {
    try command.validate(alloc);
    if (command.mode != .census or command.producer_kind != .enrichment) return error.InvalidBatchRequest;
    const page = command.census.?;
    var selected: ?Request = null;
    for (plan.generated_templates) |candidate| {
        if (candidate.kind != .chunk_text or !std.mem.eql(u8, candidate.artifact_name, command.producer_name)) continue;
        if (selected != null) return error.ArtifactCatalogDrift;
        selected = candidate;
    }
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    if (selected == null and command.producer_scope_key.len != 0) {
        const catalogs = plan.artifact_catalogs orelse return error.ArtifactCatalogDrift;
        const configs = try @import("catalog/enrichment_catalog.zig").deserializeCatalog(scratch.allocator(), catalogs.enrichments);
        const child = for (configs) |config| {
            if (config.kind == .chunk and std.mem.eql(u8, config.name, command.producer_name)) break config;
        } else return error.ArtifactCatalogDrift;
        if (child.source_artifact_name.len == 0) return error.InvalidBatchRequest;
        // These fields identify only the physical inventory scope. prepare()
        // authenticates the actual parent/child catalog relationship.
        selected = .{ .kind = .chunk_text, .index_name = "", .artifact_name = child.name, .doc_key = page.document_key, .source_field = child.source_field, .upstream_artifact_name = child.source_artifact_name };
    }
    var request = selected orelse return error.ArtifactCatalogDrift;
    request.doc_key = page.document_key;
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (active.epoch != command.authority_epoch or !std.mem.eql(u8, &active.namespace, &command.namespace) or !std.mem.eql(u8, &active.catalog_digest, &command.catalog_digest)) return error.ArtifactCatalogDrift;
    const root_key = try chunks.keyAlloc(alloc, page.document_key, command.producer_name);
    defer alloc.free(root_key);
    var unit: ?[]u8 = null;
    defer if (unit) |value| alloc.free(value);
    if (command.producer_scope_key.len != 0) {
        const scope = command.producer_scope_key;
        if (!chunks.isKey(scope) or !std.mem.startsWith(u8, scope, root_key) or scope.len <= root_key.len + 3 or scope[root_key.len] != keys.document_unit_record_kind) return error.InvalidBatchRequest;
        unit = try keys.decodeBodyAlloc(alloc, scope[root_key.len + 1 .. scope.len - 2]);
    }
    return (try prepare(alloc, txn, root, request, plan, unit, .{ .rows = page.visits, .bytes = page.bytes, .time_budget_ns = null }, .{ .before = page.before, .after = page.after })) orelse error.EnrichmentSourceChanged;
}

pub fn collectObsoletePage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    if (root == 0) return true;
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store, prefix, 48, 48, &identity);
}

test "ordered artifact inventory reconstruction orders bounded pages and resumes after restart" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/ordered-inventory", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(alloc, path, options);
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "chunks", .kind = .chunk, .field = "body", .chunk_size = 4 });
    try db.addEnrichment(.{ .name = "units", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"document_extraction\"}" });
    try db.addEnrichment(.{ .name = "unit-chunks", .kind = .chunk, .field = "body", .source_artifact_name = "units", .chunk_size = 4 });
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{\"sources\":[{\"artifact\":\"chunks\"}]}" });
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"text\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 1 });
    var expected = chunks.Builder.init();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        for (0..130) |ordinal| {
            const member = try keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", @intCast(ordinal));
            defer alloc.free(member);
            const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"chunk text\",\"ordinal\":{d}}}", .{ordinal});
            defer alloc.free(value);
            try txn.put(member, value);
            try expected.append(@intCast(ordinal), value);
        }
        for (0..5) |ordinal| {
            const member = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "unit-chunks", "unit\x00\xff", @intCast(ordinal));
            defer alloc.free(member);
            try txn.put(member, "unit chunk");
        }
        try txn.commit();
    }
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 2 });
    var activation: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_publication = activation }, .{ .term = 1, .index = 3 });
    var index: u64 = 4;
    var pages: usize = 0;
    var raced = false;
    while (true) {
        const terminal = blk: {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            var request = for (plan.plan().generated_templates) |candidate| {
                if (candidate.kind == .chunk_text) break candidate;
            } else return error.TestUnexpectedResult;
            request.doc_key = "doc";
            var page = read: {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                break :read (try prepare(alloc, &txn, db.root_incarnation, request, plan.plan(), null, .{ .rows = 17 }, null)).?;
            };
            defer page.deinit();
            if (pages == 1 and !raced) {
                raced = true;
                // Same bytes written behind the scan still revoke its causal
                // fence. Neither the old command nor its prepared writer may
                // publish a certificate assembled across that mutation.
                const member = try keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 0);
                defer alloc.free(member);
                var marker: [16]u8 = undefined;
                std.mem.writeInt(u64, marker[0..8], 1, .little);
                std.mem.writeInt(u64, marker[8..16], index, .little);
                try db.core.store.putBatch(&.{ .{ .key = member, .value = "{\"body\":\"chunk text\",\"ordinal\":0}" }, .{ .key = &keys.ordered_document_applied_entry_key, .value = &marker } }, &.{});
                index += 1;
                {
                    var txn = try db.core.store.beginWriteTxn();
                    defer txn.abort();
                    try std.testing.expectError(error.EnrichmentSourceChanged, page.stage(&txn, db.root_incarnation));
                }
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                try std.testing.expectError(error.EnrichmentSourceChanged, prepareCommand(alloc, &txn, db.root_incarnation, page.command("chunks"), plan.plan()));
                var restarted = (try prepare(alloc, &txn, db.root_incarnation, request, plan.plan(), null, .{ .rows = 17 }, null)).?;
                errdefer restarted.deinit();
                try std.testing.expectEqual(@as(u32, 0), restarted.previous.manifest.count);
                page.deinit();
                page = restarted;
            }
            const command = page.command("chunks");
            try command.validate(alloc);
            if (pages == 0) {
                const AllocationHarness = struct {
                    fn run(a: std.mem.Allocator, store: *@import("../docstore.zig").DocStore, root: u128, selected: Request, pinned: *const Plan) !void {
                        var txn = try store.beginReadTxn();
                        defer txn.abort();
                        var prepared = (try prepare(a, &txn, root, selected, pinned, null, .{ .rows = 1 }, null)).?;
                        defer prepared.deinit();
                        var received = try prepareCommand(a, &txn, root, prepared.command("chunks"), pinned);
                        defer received.deinit();
                    }
                };
                try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, AllocationHarness.run, .{ db.core.store, db.root_incarnation, request, plan.plan() });
            }
            // Receiver verification uses the sender's observed count, not its
            // deadline: different machine speeds cannot select different cuts.
            {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                var received = try prepareCommand(alloc, &txn, db.root_incarnation, command, plan.plan());
                defer received.deinit();
                try std.testing.expectEqualDeep(page.record, received.record);
                try std.testing.expectEqual(page.complete, received.complete);
                var forged = command;
                forged.census.?.after[0] ^= 1;
                forged.publication_digest = forged.digest();
                try std.testing.expectError(error.EnrichmentSourceChanged, prepareCommand(alloc, &txn, db.root_incarnation, forged, plan.plan()));
            }
            // A failed writer cannot leave either a checkpoint or manifest.
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                try std.testing.expectError(error.DurableRootIncarnationUnavailable, page.stage(&txn, db.root_incarnation + 1));
                _ = try page.stage(&txn, db.root_incarnation);
            }
            try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = index });
            index += 1;
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                try std.testing.expectError(error.EnrichmentSourceChanged, page.stage(&txn, db.root_incarnation));
            }
            if (!page.complete) {
                // Simulate a lost delivery reply: replay at a new log index is
                // idempotent and cannot advance the accepted prefix twice.
                try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = index });
                index += 1;
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                const stored = (try load(&txn, &page.selected)).?;
                try std.testing.expectEqualDeep(page.record, stored.record);
                try std.testing.expectError(error.NotFound, txn.get(page.manifest));
            } else {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                try std.testing.expectEqualDeep(expected.finish(), try chunks.Manifest.decode(try txn.get(page.manifest)));
                try std.testing.expectEqual(null, try load(&txn, &page.selected));
                // Manifest-only reconstruction cannot fabricate acceptance.
                try std.testing.expectError(error.ArtifactPublicationPending, @import("artifact_stream_progress.zig").prepareEnrichmentClosure(alloc, &txn, db.root_incarnation, request, plan.plan()));
                try std.testing.expectEqual(null, try prepare(alloc, &txn, db.root_incarnation, request, plan.plan(), null, .{}, null));
            }
            break :blk page.complete;
        };
        pages += 1;
        if (terminal) break;
        try std.testing.expect(pages <= 130);
        if (pages == 1) {
            db.close();
            db = try db_mod.DB.open(alloc, path, options);
        }
    }
    try std.testing.expect(pages > 1);
    // Extraction-owned children have no standalone worker template. Their
    // ordered inventory still supports bounded reconstruction and cold resume.
    var unit_expected = chunks.Builder.init();
    for (0..5) |ordinal| try unit_expected.append(@intCast(ordinal), "unit chunk");
    var unit_pages: usize = 0;
    while (true) {
        const terminal = blk: {
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            const request: Request = .{ .kind = .chunk_text, .index_name = "", .artifact_name = "unit-chunks", .doc_key = "doc", .source_field = "body", .upstream_artifact_name = "units" };
            var page = read: {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                break :read (try prepare(alloc, &txn, db.root_incarnation, request, plan.plan(), "unit\x00\xff", .{ .rows = 2 }, null)).?;
            };
            defer page.deinit();
            const command = page.command("unit-chunks");
            {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                var received = try prepareCommand(alloc, &txn, db.root_incarnation, command, plan.plan());
                defer received.deinit();
                try std.testing.expectEqualDeep(page.record, received.record);
                const AllocationCheck = struct {
                    fn run(a: std.mem.Allocator, snapshot: @TypeOf(&txn), root: u128, control: publication.Command, pinned: *const Plan) !void {
                        var result = try prepareCommand(a, snapshot, root, control, pinned);
                        defer result.deinit();
                    }
                };
                if (unit_pages == 0) try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ &txn, db.root_incarnation, command, plan.plan() });
            }
            try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_publication = command }, .{ .term = 1, .index = index });
            index += 1;
            if (page.complete) {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                try std.testing.expectEqualDeep(unit_expected.finish(), try chunks.Manifest.decode(try txn.get(page.manifest)));
                const root_manifest = try chunks.keyAlloc(alloc, "doc", "unit-chunks");
                defer alloc.free(root_manifest);
                try std.testing.expectError(error.NotFound, txn.get(root_manifest));
                const unit_key = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "units", "unit\x00\xff");
                defer alloc.free(unit_key);
                try std.testing.expectError(error.ArtifactPublicationPending, @import("artifact_chunk_publication.zig").readAcceptedUnit(alloc, &txn, "doc", "unit-chunks", unit_key));
            }
            break :blk page.complete;
        };
        unit_pages += 1;
        if (terminal) break;
        try std.testing.expect(unit_pages <= 5);
        if (unit_pages == 1) {
            db.close();
            db = try db_mod.DB.open(alloc, path, options);
        }
    }
    try std.testing.expect(unit_pages > 1);
}

test "ordered artifact inventory reconstruction checkpoint rejects corruption and root rebinding" {
    const Fake = struct {
        raw: Encoded,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return &self.raw;
        }
    };
    var builder = chunks.Builder.init();
    try builder.append(0, "value");
    const checkpoint: reconstruction.Checkpoint = .{ .authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) }, .scope_digest = @splat(4), .stream_revision = null, .manifest = builder.finish() };
    const record: Record = .{ .root = 5, .checkpoint = checkpoint, .claim = .{ .before = @splat(0), .after = try fingerprint(checkpoint, false) } };
    const raw = try encode(record);
    var fake: Fake = .{ .raw = raw };
    const selected = key(5, checkpoint);
    try std.testing.expectEqualDeep(record, (try load(&fake, &selected)).?.record);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&fake, &key(6, checkpoint)));
    for (0..raw.len) |offset| {
        fake.raw = raw;
        fake.raw[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&fake, &selected));
    }
}
