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

//! Native publication continuations. Intermediate roots belong to a guarded,
//! collector-visible job, never public HEAD. Each new owner uses a fresh upload
//! scope and validates the original generation before advancing durable work.
const std = @import("std");
const objects = @import("objectstore");
const A = std.mem.Allocator;
const manifest = @import("manifest/mod.zig");
const artifacts = @import("artifacts/store.zig");
const facts = @import("build/document_facts.zig");
const fact_builder = @import("build/document_facts_builder.zig");
const builder = @import("build/builder.zig");
const postings = @import("build/publication_postings.zig");
const materializer = @import("query/materializer.zig");
const tree = @import("graph_segment/page_tree.zig");
const PageStore = @import("graph_segment/page_store.zig").PageStore;
const lease = @import("build/work_lease.zig");
const sources = @import("search_sources.zig");
const quantum = objects.work_quantum;
const Policy = @import("catalog/types.zig").NamespacePolicy;
const max_state_bytes = 1024 * 1024;
const ttl = 10 * std.time.ns_per_min;
pub const Action = enum { step, status, cancel };
pub const Receipt = struct { format: u8 = 1, id: []const u8, status: enum { completed, superseded, cancelled }, published: bool, version: u64, wal_end_lsn: u64, consumed_records: usize = 0, artifact_count: usize = 0 };
pub const State = struct {
    format: u8 = 1,
    id: []const u8,
    source_version: u64,
    source_lsn: u64,
    lsn: u64,
    timestamp: u64,
    payload_checksum: [32]u8,
    document_id: []const u8,
    before_body: ?facts.BodyRef = null,
    body: ?facts.BodyRef = null,
    stored_body: ?facts.BodyRef = null,
    source_input_bytes: ?u64 = null,
    staged: manifest.Manifest,
    phase: enum { text, sparse, vector, facts, finalize } = .text,
    index: usize = 0,
    postings: ?postings.Job = null,
    candidate: ?u64 = null,
    candidate_fence: u64 = 0,
    ack_cursor: ?u64 = null,
    maintenance_pending: bool = false,
    posting_quantum: ?u16 = null,
};
pub const Pointer = struct { format: u8 = 1, expires: u64, artifact: manifest.ArtifactRef };
pub const Saved = struct { etag: ?[]const u8 = null, pointer: ?Pointer = null, state: ?State = null };
pub const Registry = struct {
    client: objects.Client,
    bucket: []const u8,
    prefix: []const u8,
    namespace: []const u8,
    store: *artifacts.ArtifactStore,
    pub fn key(self: Registry, a: A) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/progress/{s}/PUBLICATION_PLAN", .{ self.prefix, self.namespace });
    }
    fn resultKey(self: Registry, a: A, id: []const u8) ![]u8 {
        if (id.len != 32) return error.InvalidPublicationJobId;
        for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidPublicationJobId;
        return std.fmt.allocPrint(a, "{s}/progress/{s}/PUBLICATION_RESULTS/{s}", .{ self.prefix, self.namespace, id });
    }
    pub fn load(self: Registry, a: A, now: u64) !Saved {
        var client = self.client;
        client.allocator = a;
        var value = client.getObject(self.bucket, try self.key(a), .{ .max_response_bytes = 4096 }) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer value.deinit(a);
        const etag = try a.dupe(u8, value.metadata.etag orelse return error.ConditionalReadUnsupported);
        if (value.body.len == 0) return .{ .etag = etag };
        const pointer = (try std.json.parseFromSlice(Pointer, a, value.body, .{ .allocate = .alloc_always })).value;
        if (pointer.format != 1 or pointer.artifact.byte_len > max_state_bytes) return error.InvalidPublicationCheckpoint;
        if (pointer.expires <= now) return .{ .etag = etag };
        return .{ .etag = etag, .pointer = pointer, .state = try self.readState(a, pointer.artifact) };
    }
    pub fn readState(self: Registry, a: A, ref: manifest.ArtifactRef) !State {
        if (ref.kind != .stored_fields or ref.byte_len > max_state_bytes) return error.InvalidPublicationCheckpoint;
        const bytes = try self.store.getVerifiedAllocWithCancellationUsingAllocator(a, ref.artifact_id, ref.byte_len, ref.checksum, .none);
        defer a.free(bytes);
        const state = (try std.json.parseFromSlice(State, a, bytes, .{ .allocate = .alloc_always })).value;
        if (state.format != 1 or state.id.len != 32 or state.lsn <= state.source_lsn or !std.mem.eql(u8, state.staged.namespace, self.namespace)) return error.InvalidPublicationCheckpoint;
        if (state.postings) |job| try job.validate(PageStore.namespaceDomain(self.namespace));
        if (state.posting_quantum) |units| if (units == 0 or units > 4096) return error.InvalidPublicationCheckpoint;
        return state;
    }
    fn guard(self: Registry, a: A, held: *lease.HeldLease) !struct { key: []const u8, etag: []const u8 } {
        try held.renew(held.ttl_ns);
        const head_key = try std.fmt.allocPrint(a, "{s}/progress/{s}/HEAD", .{ self.prefix, self.namespace });
        var client = self.client;
        client.allocator = a;
        var head = try client.statObject(self.bucket, head_key);
        defer head.deinit(a);
        const record = (try @import("head_coordination.zig").parseContentTypeAlloc(a, head.content_type)) orelse return error.WorkLeaseLost;
        if (record.value.released or record.value.fencing_token != held.acquisition.fencing_token or record.value.owner_id == null or !std.mem.eql(u8, record.value.owner_id.?, held.owner_id)) return error.WorkLeaseLost;
        return .{ .key = head_key, .etag = try a.dupe(u8, head.etag orelse return error.ConditionalReadUnsupported) };
    }
    pub fn save(self: Registry, a: A, previous: ?[]const u8, state: ?State, held: *lease.HeldLease) ![]const u8 {
        const condition = try self.guard(a, held);
        const body = if (state) |checkpoint| blk: {
            const bytes = try std.json.Stringify.valueAlloc(a, checkpoint, .{ .emit_null_optional_fields = false });
            if (bytes.len > max_state_bytes) return error.PublicationCheckpointTooLarge;
            var stored = try self.store.put(bytes);
            defer stored.deinit(self.store.allocator);
            break :blk try std.json.Stringify.valueAlloc(a, Pointer{ .expires = lease.nowUnixNs(held.io) +| ttl, .artifact = .{ .kind = .stored_fields, .artifact_id = stored.artifact_id, .checksum = stored.checksum, .byte_len = stored.byte_len } }, .{});
        } else "";
        var client = self.client;
        client.allocator = a;
        var result = client.putObject(self.bucket, try self.key(a), body, .{ .if_match_etag = previous, .if_none_match = previous == null, .guard = .{ .key = condition.key, .etag = condition.etag } }) catch |err| switch (err) {
            error.PreconditionFailed => return error.WorkLeaseLost,
            else => return err,
        };
        defer result.deinit(a);
        return a.dupe(u8, result.etag orelse return error.ConditionalReadUnsupported);
    }
    pub fn receipt(self: Registry, a: A, id: []const u8) !?Receipt {
        var client = self.client;
        client.allocator = a;
        var result = client.getObject(self.bucket, try self.resultKey(a, id), .{ .max_response_bytes = 4096 }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(a);
        const value = (try std.json.parseFromSlice(Receipt, a, result.body, .{ .allocate = .alloc_always })).value;
        if (value.format != 1 or !std.mem.eql(u8, value.id, id)) return error.InvalidPublicationReceipt;
        return value;
    }
    pub fn complete(self: Registry, a: A, previous: ?[]const u8, value: Receipt, held: *lease.HeldLease) !void {
        const condition = try self.guard(a, held);
        const body = try std.json.Stringify.valueAlloc(a, value, .{});
        var client = self.client;
        client.allocator = a;
        if (client.putObject(self.bucket, try self.resultKey(a, value.id), body, .{ .if_none_match = true, .guard = .{ .key = condition.key, .etag = condition.etag } })) |result_| {
            var result = result_;
            result.deinit(a);
        } else |err| switch (err) {
            error.PreconditionFailed => {
                const prior = (try self.receipt(a, value.id)) orelse return error.WorkLeaseLost;
                const encoded = try std.json.Stringify.valueAlloc(a, prior, .{});
                if (!std.mem.eql(u8, body, encoded)) return error.PublicationReceiptConflict;
            },
            else => return err,
        }
        _ = try self.save(a, previous, null, held);
    }
};

pub const Context = struct {
    alloc: A,
    registry: Registry,
    manifests: *manifest.ManifestStore,
    progress: *@import("catalog/mod.zig").ProgressStore,
    wal: *@import("wal/mod.zig").WalStore,
    io: std.Io,
    cancellation: @import("maintenance_cancellation.zig").Token,
    policy: Policy,
    force: bool = false,
    /// Borrowed prepared-slice lease; the outer transaction owns release.
    prepared_lease: ?*lease.HeldLease = null,
    adaptive_slices: bool = false,
    prepare_slices: bool = false,
    max_posting_units: usize = 4096,
    batch_probe_records: ?usize = null,

    pub fn run(self: Context, action: Action, requested_id: ?[]const u8, units: usize) !?[]u8 {
        if (units == 0 or units > 256 or self.max_posting_units == 0 or self.max_posting_units > 4096) return error.InvalidPublicationBudget;
        var metadata = std.heap.ArenaAllocator.init(self.alloc);
        defer metadata.deinit();
        const m = metadata.allocator();
        if (requested_id) |id| if (try self.registry.receipt(m, id)) |done| return try self.responseReceipt(done, false);
        var saved = try self.registry.load(m, lease.nowUnixNs(self.io));
        if (action == .status) {
            if (requested_id) |id| if (saved.state == null or !std.mem.eql(u8, saved.state.?.id, id)) return error.PublicationJobNotFound;
            return try self.responseState(saved.state);
        }
        if (requested_id != null and saved.state == null) return error.PublicationJobNotFound;
        // Avoid taking a work lease for ordinary bounded records. Their existing
        // batched builder remains the fast path; active jobs always resume.
        if (saved.state == null and action == .step and !self.force) {
            const version = self.progress.getHead(self.registry.namespace) catch |err| switch (err) {
                error.FileNotFound => 0,
                else => return err,
            };
            const cutoff = if (version == 0) 0 else blk: {
                var source = try self.manifests.getAlloc(self.registry.namespace, version);
                defer source.deinit(self.alloc);
                break :blk source.wal_end_lsn;
            };
            const latest = try self.wal.latestLsn(self.registry.namespace);
            if (latest <= cutoff) {
                if (self.batch_probe_records != null) return try std.json.Stringify.valueAlloc(self.alloc, .{ .published = false, .version = try std.fmt.allocPrint(m, "{d}", .{version}), .wal_end_lsn = try std.fmt.allocPrint(m, "{d}", .{cutoff}), .consumed_records = @as(usize, 0), .has_more = false }, .{});
                return null;
            }
            const rows = try self.wal.readRangeAlloc(self.registry.namespace, .{ .start_lsn = cutoff + 1, .end_lsn = @min(latest, cutoff +| @as(u64, @intCast(self.batch_probe_records orelse 1))), .max_records = self.batch_probe_records orelse 1, .max_bytes = 4 * 1024 * 1024 });
            defer @import("wal/mod.zig").freeRecords(self.alloc, rows);
            if (rows.len == 0) return error.PublicationWalGap;
            if (rows[0].payload.len < 16 * 1024) {
                if (self.batch_probe_records != null) {
                    var count: usize = 0;
                    for (rows) |row| {
                        if (row.payload.len >= 16 * 1024) break;
                        count += 1;
                    }
                    return try std.json.Stringify.valueAlloc(self.alloc, .{ .__publication_batch_records = count, .__publication_batch_version = try std.fmt.allocPrint(m, "{d}", .{version}) }, .{});
                }
                return null;
            }
        }
        if (self.prepare_slices and self.batch_probe_records != null and self.prepared_lease == null) return try self.alloc.dupe(u8, "{\"__publication_slice\":true}");
        var nonce: [16]u8 = undefined;
        self.io.random(&nonce);
        const owner = std.fmt.bytesToHex(nonce, .lower);
        var acquired: ?lease.HeldLease = null;
        const held = self.prepared_lease orelse blk: {
            acquired = (try lease.acquireHeld(try self.progress.workLeaseProvider(), self.io, self.registry.namespace, &owner, 30 * std.time.ns_per_s)) orelse return error.WorkLeaseLost;
            break :blk &acquired.?;
        };
        defer if (self.prepared_lease == null) {
            _ = held.release() catch false;
        };
        const cancellation = held.cancellation(self.cancellation);
        try cancellation.check();
        saved = try self.registry.load(m, lease.nowUnixNs(self.io));
        const scope = try artifacts.UploadScope.forPublication(PageStore.namespaceDomain(self.registry.namespace), held.acquisition.fencing_token, self.io);
        const prior_scope = self.registry.store.upload_scope;
        self.registry.store.upload_scope = scope;
        defer self.registry.store.upload_scope = prior_scope;
        var bridge = @import("maintenance_cancellation.zig").GraphBridge{ .maintenance = cancellation };
        var reads: u64 = 16 * 1024 * 1024;
        var writes: u64 = 16 * 1024 * 1024;
        var pages = PageStore{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = self.registry.store, .cancellation = bridge.token(), .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        if (self.adaptive_slices) pages.admitPublication();
        const version = self.progress.getHead(self.registry.namespace) catch |err| switch (err) {
            error.FileNotFound => 0,
            else => return err,
        };
        var source_store = self.manifests.*;
        source_store.allocator = m;
        const source = if (version != 0) try source_store.getAlloc(self.registry.namespace, version) else null;
        if (saved.state == null) {
            if (requested_id != null or action == .cancel) return error.PublicationJobNotFound;
            saved.state = (try self.start(m, &pages, source, &owner)) orelse return null;
            _ = try self.registry.save(m, saved.etag, saved.state, held);
            return try self.responseState(saved.state);
        }
        var state = saved.state.?;
        if (requested_id) |id| if (!std.mem.eql(u8, state.id, id)) return error.PublicationJobNotFound;
        if (try self.registry.receipt(m, state.id)) |done| {
            _ = try self.registry.save(m, saved.etag, null, held);
            return try self.responseReceipt(done, requested_id == null);
        }
        if (action == .cancel) {
            const done = Receipt{ .id = state.id, .status = .cancelled, .published = false, .version = version, .wal_end_lsn = if (source) |s| s.wal_end_lsn else 0 };
            try self.registry.complete(m, saved.etag, done, held);
            return try self.responseReceipt(done, false);
        }
        if (state.candidate) |candidate| {
            var cursor: ?u64 = state.ack_cursor orelse version;
            for (0..128) |_| {
                const current = cursor orelse break;
                if (current < candidate) break;
                const ancestor = try source_store.getAlloc(self.registry.namespace, current);
                if (current == candidate) {
                    if (ancestor.publication_fencing_token == state.candidate_fence and ancestor.wal_end_lsn == state.lsn) {
                        const done = Receipt{ .id = state.id, .status = .completed, .published = true, .version = candidate, .wal_end_lsn = state.lsn, .consumed_records = 1, .artifact_count = state.staged.artifacts.len };
                        try self.registry.complete(m, saved.etag, done, held);
                        return try self.responseReceipt(done, requested_id == null);
                    }
                    break;
                }
                cursor = if (ancestor.publication_lineage_tracked) ancestor.publication_parent_version else null;
            }
            if (cursor != null and cursor.? > candidate) {
                state.ack_cursor = cursor;
                _ = try self.registry.save(m, saved.etag, state, held);
                return try self.responseState(state);
            }
        }
        if (version != state.source_version or !std.meta.eql(self.policy, state.staged.stats.policy)) {
            const done = Receipt{ .id = state.id, .status = .superseded, .published = false, .version = version, .wal_end_lsn = if (source) |s| s.wal_end_lsn else 0 };
            try self.registry.complete(m, saved.etag, done, held);
            return try self.responseReceipt(done, requested_id == null);
        }
        if (try self.progress.getManifestGcFloor(self.registry.namespace)) |floor| if (state.source_version != 0 and state.source_version < floor) return error.PublicationSourceRetired;
        if (state.phase == .finalize) {
            // Verify the accepted record is still the exact frozen source. The
            // collector holds this WAL position while the job is live.
            const records = try self.wal.readRangeAlloc(self.registry.namespace, .{ .start_lsn = state.lsn, .end_lsn = state.lsn, .max_records = 1, .max_bytes = 4 * 1024 * 1024 });
            defer @import("wal/mod.zig").freeRecords(self.alloc, records);
            var checksum: [32]u8 = undefined;
            if (records.len != 1) return error.PublicationWalGap;
            std.crypto.hash.sha2.Sha256.hash(records[0].payload, &checksum, .{});
            if (!std.mem.eql(u8, &checksum, &state.payload_checksum)) return error.PublicationSourceChanged;
            var final = try self.finalManifest(m, &state);
            try builder.stampPublicationFence(&final, held.guard());
            const candidate = try builder.putManifestForPublication(self.manifests, &final, if (version == 0) null else version);
            state.staged = final;
            state.candidate = candidate;
            state.candidate_fence = held.acquisition.fencing_token;
            saved.etag = try self.registry.save(m, saved.etag, state, held);
            if (!try builder.compareAndSwapHeadGuarded(self.progress, self.registry.namespace, if (version == 0) null else version, candidate, held.guard())) return error.HeadChanged;
            const done = Receipt{ .id = state.id, .status = .completed, .published = true, .version = candidate, .wal_end_lsn = state.lsn, .consumed_records = 1, .artifact_count = final.artifacts.len };
            try self.registry.complete(m, saved.etag, done, held);
            return try self.responseReceipt(done, requested_id == null);
        }
        if (state.postings) |*job| {
            const maximum: usize = if (self.adaptive_slices) @min(self.max_posting_units, state.posting_quantum orelse units) else units;
            const started = std.Io.Clock.now(.awake, self.io).nanoseconds;
            const before_reads = reads;
            const before_writes = writes;
            var processed: usize = 0;
            const completed = if (self.adaptive_slices) try job.stepBudgeted(self.alloc, &pages, maximum, &processed) else try job.step(self.alloc, &pages, units);
            if (self.adaptive_slices) {
                const elapsed = std.Io.Clock.now(.awake, self.io).nanoseconds - started;
                if (elapsed < 0) return error.InvalidWorkQuantum;
                state.posting_quantum = @intCast(try (quantum.Policy{}).next(maximum, .{ .completed = processed, .elapsed_ns = std.math.cast(u64, elapsed) orelse return error.InvalidWorkQuantum, .read_bytes = before_reads - reads, .write_bytes = before_writes - writes }));
            }
            if (completed) |ref| {
                defer builder.freeArtifactRef(self.alloc, ref);
                try replaceNamed(m, &state.staged, ref);
                state.postings = null;
                state.index += 1;
            }
        } else try self.advance(m, &state, source, &pages, cancellation);
        _ = try self.registry.save(m, saved.etag, state, held);
        return try self.responseState(state);
    }

    fn start(self: Context, m: A, pages: *PageStore, source: ?manifest.Manifest, id: []const u8) !?State {
        const version = if (source) |s| s.version else 0;
        if (source) |s| {
            if (s.stats.graph_segment_count != 0 or !builder.allArtifactsPagedOrAbsent(s, .text_segment) or !builder.allArtifactsPagedOrAbsent(s, .sparse_segment) or !builder.allArtifactsPagedOrAbsent(s, .vector_segment)) return error.PublicationReindexRequired;
            if (!std.meta.eql(s.stats.policy, self.policy)) return null;
        }
        const cutoff = if (source) |s| s.wal_end_lsn else 0;
        if ((try self.wal.latestLsn(self.registry.namespace)) <= cutoff) return null;
        const records = try self.wal.readRangeAlloc(self.registry.namespace, .{ .start_lsn = cutoff + 1, .end_lsn = cutoff + 1, .max_records = 1, .max_bytes = 4 * 1024 * 1024 });
        defer @import("wal/mod.zig").freeRecords(self.alloc, records);
        if (records.len != 1) return error.PublicationWalGap;
        const applicable = try builder.applicableWalRecordsForHeadAlloc(self.alloc, records, version);
        defer self.alloc.free(applicable);
        if (applicable.len == 0) return null;
        const mutations = try builder.decodeWalMutationsAlloc(self.alloc, applicable);
        defer {
            for (mutations) |mutation| {
                self.alloc.free(mutation.doc_id);
                if (mutation.body) |body| self.alloc.free(body);
            }
            self.alloc.free(mutations);
        }
        if (mutations.len != 1) return error.InvalidPublicationDocument;
        const prior = if (source) |s| find(s, .document_facts, null) orelse return error.DocumentFactsRequired else null;
        var before_ref: ?facts.BodyRef = null;
        var before: []materializer.Document = &.{};
        if (prior) |ref| {
            const root = try facts.loadRoot(self.alloc, pages, ref);
            if (try fact_builder.needsRebuild(self.alloc, root, self.policy, source.?.stats.indexes_json)) return null;
            if (try facts.lookup(self.alloc, pages.store(), root, mutations[0].doc_id)) |fact| {
                before_ref = fact.body;
                const old_body = try facts.readBodyAlloc(self.alloc, pages, fact.body);
                errdefer self.alloc.free(old_body);
                const old_id = try self.alloc.dupe(u8, mutations[0].doc_id);
                errdefer self.alloc.free(old_id);
                before = try self.alloc.alloc(materializer.Document, 1);
                before[0] = .{ .doc_id = old_id, .body = old_body, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns, .source_input_bytes = fact.source_input_bytes };
            }
        }
        defer if (before.len != 0) materializer.freeDocuments(self.alloc, before);
        const after = try materializer.materializeWithBodiesAlloc(self.alloc, before, mutations, .{ .pages = pages, .artifacts = self.registry.store, .namespace = self.registry.namespace, .domain = pages.domain, .cancellation = pages.cancellation });
        defer materializer.freeDocuments(self.alloc, after);
        var state = State{ .id = try m.dupe(u8, id), .source_version = version, .source_lsn = cutoff, .lsn = records[0].lsn, .timestamp = records[0].timestamp_ns, .payload_checksum = undefined, .document_id = try m.dupe(u8, mutations[0].doc_id), .before_body = before_ref, .staged = if (source) |s| try manifest.cloneManifest(m, s) else .{ .namespace = try m.dupe(u8, self.registry.namespace), .version = 0, .built_at_ns = 0, .wal_start_lsn = 1, .wal_end_lsn = 0, .stats = .{ .policy = self.policy, .published_search_sources = sources.defaultPublishedSearchSources() }, .artifacts = try m.alloc(manifest.ArtifactRef, 0) } };
        std.crypto.hash.sha2.Sha256.hash(records[0].payload, &state.payload_checksum, .{});
        if (after.len != 0) {
            if (after.len != 1) return error.InvalidPublicationDocument;
            state.body = try facts.putBody(self.alloc, pages, after[0].body);
            if (after[0].stored_body) |body| state.stored_body = try facts.putBody(self.alloc, pages, body);
            state.source_input_bytes = after[0].source_input_bytes;
        }
        return state;
    }
    fn advance(self: Context, m: A, state: *State, source: ?manifest.Manifest, pages: *PageStore, cancellation: @import("maintenance_cancellation.zig").Token) !void {
        const body = if (state.body) |ref| try facts.readBodyAlloc(self.alloc, pages, ref) else null;
        defer if (body) |bytes| self.alloc.free(bytes);
        const stored = if (state.stored_body) |ref| try facts.readStoredBodyAlloc(self.alloc, pages, ref) else null;
        defer if (stored) |bytes| self.alloc.free(bytes);
        var one: [1]materializer.Document = undefined;
        const docs: []const materializer.Document = if (body) |bytes| blk: {
            one[0] = .{ .doc_id = @constCast(state.document_id), .body = bytes, .stored_body = stored, .last_lsn = state.lsn, .last_timestamp_ns = state.timestamp, .source_input_bytes = state.source_input_bytes };
            break :blk &one;
        } else &.{};
        const mutations = [_]materializer.Mutation{.{ .doc_id = state.document_id, .body = body, .kind = if (body != null) .upsert else .delete, .lsn = state.lsn, .timestamp_ns = state.timestamp }};
        switch (state.phase) {
            .text => {
                const specs = try builder.resolvePublishedTextIndexSpecsAlloc(self.alloc, .{ .schema_json = state.staged.stats.schema_json, .read_schema_json = state.staged.stats.read_schema_json, .indexes_json = state.staged.stats.indexes_json }, &.{});
                defer @import("antfly_local_sources").api_full_text_indexes.freeFullTextIndexSpecs(self.alloc, specs);
                if (state.index == specs.len) {
                    state.phase = .sparse;
                    state.index = 0;
                    return;
                }
                if (state.index > specs.len) return error.InvalidPublicationCheckpoint;
                const spec = specs[state.index];
                const normalized = if (docs.len != 0) try builder.normalizedTextForDocumentAlloc(self.alloc, docs[0], spec) else null;
                defer if (normalized) |bytes| self.alloc.free(bytes);
                state.postings = try postings.Job.prepareText(self.alloc, m, pages, find(state.staged, .text_segment, spec.name), state.document_id, normalized, spec);
            },
            .sparse => {
                const selected = try sources.listSparseSourcesAlloc(self.alloc, sources.defaultPublishedSearchSources());
                defer sources.freeSparseSourceDescriptors(self.alloc, selected);
                if (state.index == selected.len) {
                    state.phase = .vector;
                    state.index = 0;
                    return;
                }
                if (state.index > selected.len) return error.InvalidPublicationCheckpoint;
                const spec = selected[state.index];
                var delta = try builder.allocSparseSegmentAlloc(self.alloc, docs, spec, cancellation);
                defer if (delta) |*value| value.deinit(self.alloc);
                state.postings = try postings.Job.prepareSparse(self.alloc, m, pages, find(state.staged, .sparse_segment, spec.index_name), state.document_id, delta, spec.index_name, try @import("paged_projection.zig").fingerprint(self.alloc, spec));
            },
            .vector => {
                // Native vector runs already bound page/block production and
                // expose durable merge continuation. Maintain those private
                // runs before admitting another index-family update.
                const selected = try sources.listVectorSourcesAlloc(self.alloc, sources.defaultPublishedSearchSources());
                defer sources.freeVectorSourceDescriptors(self.alloc, selected);
                if (state.index == selected.len) {
                    state.phase = .facts;
                    state.index = 0;
                    return;
                }
                if (state.index > selected.len) return error.InvalidPublicationCheckpoint;
                const spec = selected[state.index];
                if (find(state.staged, .vector_segment, spec.index_name)) |prior| {
                    if (try @import("vector_segment/runs.zig").maintain(self.alloc, pages, prior)) |step| {
                        defer builder.freeArtifactRef(self.alloc, step.ref);
                        try replaceNamed(m, &state.staged, step.ref);
                        return;
                    }
                }
                const refs = try builder.buildVectorArtifactRefsWithContextAllocUntil(self.alloc, self.registry.store, state.staged, .cosine, &.{}, docs, &mutations, null, &.{}, .{ .vector = spec }, cancellation, .{ .enabled = true, .partial = true, .namespace = self.registry.namespace, .attempt = pages.attempt, .maintenance_pending = &state.maintenance_pending });
                defer builder.freeArtifactRefs(self.alloc, refs);
                for (refs) |ref| try replaceNamed(m, &state.staged, ref);
                state.index += 1;
            },
            .facts => {
                const ref = try fact_builder.publishAlloc(self.alloc, pages, if (source) |s| find(s, .document_facts, null) else null, docs, &mutations, state.staged.stats.policy, state.staged.stats.indexes_json, state.lsn);
                defer builder.freeArtifactRef(self.alloc, ref);
                const root = try facts.loadRoot(self.alloc, pages, ref);
                try replaceNamed(m, &state.staged, ref);
                state.staged.stats.document_count = root.document_count;
                state.staged.stats.derived_outputs = try builder.derivedOutputsFromFactCountsAlloc(m, root.counts);
                state.staged.wal_end_lsn = state.lsn;
                state.phase = .finalize;
            },
            .finalize => unreachable,
        }
    }
    fn finalManifest(self: Context, m: A, state: *State) !manifest.Manifest {
        const empty = try @import("document_segment/mod.zig").encodeAlloc(self.alloc, &.{});
        defer self.alloc.free(empty);
        var stored = try self.registry.store.put(empty);
        defer stored.deinit(self.registry.store.allocator);
        const document = try builder.artifactRefFromMetadataAlloc(m, .document_segment, stored);
        var text_refs = std.ArrayList(manifest.ArtifactRef).empty;
        var sparse_refs = std.ArrayList(manifest.ArtifactRef).empty;
        var vector_refs = std.ArrayList(manifest.ArtifactRef).empty;
        for (state.staged.artifacts) |ref| switch (ref.kind) {
            .text_segment => try text_refs.append(m, ref),
            .sparse_segment => try sparse_refs.append(m, ref),
            .vector_segment => try vector_refs.append(m, ref),
            else => {},
        };
        var final = try builder.buildRebasedManifestFromRefsAlloc(m, self.registry.namespace, state.source_version + 1, state.timestamp, state.lsn, @intCast(state.staged.stats.document_count), state.source_version + 1, .inline_rebase, document, text_refs.items, sparse_refs.items, vector_refs.items, &.{}, state.staged.stats.derived_outputs, state.staged.stats.policy, .{ .schema_json = state.staged.stats.schema_json, .read_schema_json = state.staged.stats.read_schema_json, .indexes_json = state.staged.stats.indexes_json, .base_source = state.staged.base_source });
        if (find(state.staged, .document_facts, null)) |ref| try replaceNamed(m, &final, ref) else return error.DocumentFactsRequired;
        return final;
    }
    fn responseState(self: Context, state: ?State) ![]u8 {
        const version = try std.fmt.allocPrint(self.alloc, "{d}", .{if (state) |value| value.source_version else 0});
        defer self.alloc.free(version);
        const cutoff = try std.fmt.allocPrint(self.alloc, "{d}", .{if (state) |value| value.source_lsn else 0});
        defer self.alloc.free(cutoff);
        var digest: [32]u8 = @splat(0);
        if (state) |value| {
            const bytes = try std.json.Stringify.valueAlloc(self.alloc, value, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(bytes);
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        }
        const checkpoint = std.fmt.bytesToHex(digest, .lower);
        return std.json.Stringify.valueAlloc(self.alloc, .{ .checkpoint_sha256 = if (state != null) @as(?[]const u8, &checkpoint) else null, .posting_quantum = if (state) |value| value.posting_quantum else null, .published = false, .version = version, .wal_end_lsn = cutoff, .consumed_records = @as(usize, 0), .has_more = state != null, .job_id = if (state) |s| s.id else null, .phase = if (state) |s| @tagName(s.phase) else "idle", .offset = if (state) |s| (if (s.postings) |job| job.offset else 0) else @as(u64, 0) }, .{});
    }
    fn responseReceipt(self: Context, done: Receipt, next: bool) ![]u8 {
        const version = try std.fmt.allocPrint(self.alloc, "{d}", .{done.version});
        defer self.alloc.free(version);
        const cutoff = try std.fmt.allocPrint(self.alloc, "{d}", .{done.wal_end_lsn});
        defer self.alloc.free(cutoff);
        const more = next and (try self.wal.latestLsn(self.registry.namespace)) > done.wal_end_lsn;
        const receipt_bytes = try std.json.Stringify.valueAlloc(self.alloc, done, .{});
        defer self.alloc.free(receipt_bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(receipt_bytes, &digest, .{});
        const identity = std.fmt.bytesToHex(digest, .lower);
        return std.json.Stringify.valueAlloc(self.alloc, .{ .receipt_sha256 = @as([]const u8, &identity), .published = done.published, .version = version, .wal_end_lsn = cutoff, .consumed_records = done.consumed_records, .artifact_count = done.artifact_count, .has_more = more, .job_id = done.id, .phase = @tagName(done.status) }, .{});
    }
};
fn find(source: manifest.Manifest, kind: manifest.ArtifactKind, name: ?[]const u8) ?manifest.ArtifactRef {
    for (source.artifacts) |ref| if (ref.kind == kind and (name == null or std.mem.eql(u8, ref.name, name.?))) return ref;
    return null;
}
fn replaceNamed(a: A, source: *manifest.Manifest, ref: manifest.ArtifactRef) !void {
    var refs = std.ArrayList(manifest.ArtifactRef).empty;
    for (source.artifacts) |prior| if (prior.kind != ref.kind or !std.mem.eql(u8, prior.name, ref.name)) {
        try refs.append(a, prior);
    };
    try refs.append(a, try builder.cloneArtifactRefAlloc(a, ref));
    source.artifacts = try refs.toOwnedSlice(a);
    switch (ref.kind) {
        .text_segment => {
            var n: u32 = 0;
            for (source.artifacts) |item| if (item.kind == .text_segment) {
                n += 1;
            };
            source.stats.text_segment_count = n;
        },
        .sparse_segment => {
            var n: u32 = 0;
            for (source.artifacts) |item| if (item.kind == .sparse_segment) {
                n += 1;
            };
            source.stats.sparse_segment_count = n;
        },
        .vector_segment => {
            var n: u32 = 0;
            for (source.artifacts) |item| if (item.kind == .vector_segment) {
                n += 1;
            };
            source.stats.vector_segment_count = n;
        },
        else => {},
    }
}

test "serverless publication continuation keeps partial roots private across ownership changes and frozen WAL prefixes" {
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    // Single-threaded contract fixture. Real cross-key atomicity is verified
    // against the coordinator's transactionSync in the workerd suite.
    var vtable = client.vtable.*;
    vtable.supports_guarded_put = true;
    vtable.put_object = struct {
        fn put(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, body: []const u8, options: objects.PutOptions) !objects.PutResult {
            const backing: *objects.MemoryClient = @ptrCast(@alignCast(raw));
            var original = backing.client();
            original.allocator = allocator;
            if (options.guard) |guard| {
                var head = try original.statObject(bucket, guard.key);
                defer head.deinit(allocator);
                if (head.etag == null or !std.mem.eql(u8, head.etag.?, guard.etag)) return error.PreconditionFailed;
            }
            var plain = options;
            plain.guard = null;
            return original.putObject(bucket, key, body, plain);
        }
    }.put;
    client.vtable = &vtable;
    try client.makeBucket("database");
    var object_artifacts = try @import("artifacts/object_store.zig").ObjectStore.initWithClient(a, client, "database", "test/artifacts");
    var store = object_artifacts.artifactStore();
    defer store.deinit();
    var object_manifests = try @import("manifest/object_store.zig").ObjectStore.initWithClient(a, client, "database", "test/manifests");
    var manifest_store = object_manifests.manifestStore();
    defer manifest_store.deinit();
    var object_progress = try @import("catalog/object_progress_store.zig").ObjectProgressStore.initWithClient(a, client, "database", "test/progress");
    var progress = object_progress.progressStore();
    defer progress.deinit();
    var object_wal = try @import("wal/indexed_object_store.zig").IndexedObjectStore.init(a, client, "database", "test");
    defer object_wal.deinit();
    var wal = object_wal.walStore();
    var build = builder.Builder.init(a, &store, &manifest_store, &progress, &wal);
    build.setIo(std.testing.io);
    build.paged_text = true;
    build.paged_projections = true;
    const initial = try @import("api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "one", .body = "{\"text\":\"old old\",\"embedding\":[1,0],\"sparse_embedding\":{\"old\":1}}" });
    defer a.free(initial);
    _ = try wal.append("docs", 1, initial);
    var baseline = try build.publishNamespaceWithMetricAndPlanWithCancellation("docs", .cosine, .{ .targets = .{ .published_search_sources = sources.defaultPublishedSearchSources(), .include_graph = false } }, .none);
    defer baseline.deinit(a);
    const body = "{\"text\":\"alpha alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu\",\"embedding\":[0,1],\"sparse_embedding\":{\"new\":2}}";
    const payload = try @import("api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "one", .body = body });
    defer a.free(payload);
    _ = try wal.append("docs", 2, payload);
    const context = Context{ .alloc = a, .registry = .{ .client = client, .bucket = "database", .prefix = "test", .namespace = "docs", .store = &store }, .manifests = &manifest_store, .progress = &progress, .wal = &wal, .io = std.testing.io, .cancellation = .{ .io = std.testing.io }, .policy = .{}, .force = true };
    var probe_context = context;
    probe_context.force = false;
    probe_context.batch_probe_records = 64;
    const probe_response = (try probe_context.run(.step, null, 3)).?;
    defer a.free(probe_response);
    const probe_result = try std.json.parseFromSlice(struct { __publication_batch_records: usize, __publication_batch_version: []const u8 }, a, probe_response, .{});
    defer probe_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe_result.value.__publication_batch_records);
    try std.testing.expectEqual(baseline.version, try std.fmt.parseInt(u64, probe_result.value.__publication_batch_version, 10));
    const started = (try context.run(.step, null, 3)).?;
    defer a.free(started);
    const first = try std.json.parseFromSlice(struct { job_id: []const u8 }, a, started, .{ .ignore_unknown_fields = true });
    defer first.deinit();
    var finished = false;
    var final_response: ?[]u8 = null;
    defer if (final_response) |value| a.free(value);
    for (0..128) |i| {
        if (i == 4) {
            const later = try @import("api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "later", .body = "{\"text\":\"later\"}" });
            defer a.free(later);
            _ = try wal.append("docs", 3, later);
            const collection = @import("hosted_collection.zig").Context{ .alloc = a, .namespace = "docs", .prefix = "test", .artifacts = &store, .manifests = &manifest_store, .progress = &progress, .wal = &object_wal, .io = std.testing.io, .publication_registry = context.registry };
            const plan = try collection.prepare();
            defer a.free(plan);
            const parsed = try std.json.parseFromSlice(struct { roots: []@import("hosted_collection.zig").Task }, a, plan, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            var protected = false;
            for (parsed.value.roots) |task| if (task.kind == .publication_state) {
                const traced = try collection.trace(task);
                defer a.free(traced);
                const closure = try std.json.parseFromSlice(struct { children: []@import("hosted_collection.zig").Task, wal_keep_from: ?[]const u8 = null }, a, traced, .{ .ignore_unknown_fields = true });
                defer closure.deinit();
                try std.testing.expect(closure.value.children.len != 0);
                try std.testing.expectEqualStrings("2", closure.value.wal_keep_from.?);
                protected = true;
            };
            try std.testing.expect(protected);
        }
        const response = (try context.run(.step, first.value.job_id, 3)).?;
        const parsed = try std.json.parseFromSlice(struct { published: bool, has_more: bool }, a, response, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.published) {
            final_response = response;
            finished = true;
            break;
        }
        a.free(response);
        try std.testing.expectEqual(baseline.version, try progress.getHead("docs"));
    }
    try std.testing.expect(finished);
    var published = try manifest_store.getAlloc("docs", try progress.getHead("docs"));
    defer published.deinit(a);
    try std.testing.expectEqual(@as(u64, 2), published.wal_end_lsn);
    var query = @import("query/runtime.zig").QueryRuntime.init(a, &store, &manifest_store, &progress);
    defer query.deinit();
    var session = try query.openHeadSession("docs");
    defer session.deinit();
    const hits = try @import("query/indexed_reader.zig").searchAlloc(a, &session, .{ .text = @constCast("alpha"), .limit = 10 });
    defer @import("query/request.zig").freeHits(a, hits);
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    var sparse_terms = [_]@import("query/request.zig").SparseTermWeight{.{ .term = @constCast("new"), .weight = 1 }};
    const sparse = try @import("query/indexed_reader.zig").searchAlloc(a, &session, .{ .text = @constCast(""), .mode = .sparse, .sparse = &sparse_terms, .limit = 10 });
    defer @import("query/request.zig").freeHits(a, sparse);
    try std.testing.expectEqual(@as(usize, 1), sparse.len);
    var later = try build.publishNamespaceWithMetricAndPlanWithCancellation("docs", .cosine, .{ .targets = .{ .published_search_sources = sources.defaultPublishedSearchSources(), .include_graph = false } }, .none);
    defer later.deinit(a);
    const status = (try context.run(.status, first.value.job_id, 3)).?;
    defer a.free(status);
    try std.testing.expectEqualStrings(final_response.?, status);
}
