// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Source input versions are captured at the physical transaction boundary,
//! including coordinated transaction resolution, rather than at one API path.
const std = @import("std");
const keys = @import("internal_keys.zig");
const publication = @import("db/artifact_publication.zig");
const authority = @import("source_authority.zig");

pub const Capture = struct {
    /// Borrowed only for this physical writer attempt. Set by trusted ingress,
    /// never inferred from stored artifact flags or imported metadata.
    participant: ?@import("commit_participant.zig").Participant = null,
    allocator: ?std.mem.Allocator = null,
    checked: bool = false,
    selected: ?publication.Authority = null,
    documents: std.StringHashMapUnmanaged(void) = .empty,
    artifacts: std.StringHashMapUnmanaged(void) = .empty,
    artifact_owners: std.StringHashMapUnmanaged(void) = .empty,
    raft_marker: ?[16]u8 = null,
    native_position: ?u64 = null,
    replay_sequence: ?u64 = null,
    replay_next: ?u64 = null,
    replay_ambiguous: bool = false,
    staging: bool = false,
    staged: bool = false,
    poisoned: bool = false,

    pub fn attach(self: *Capture, participant: @import("commit_participant.zig").Participant) !void {
        if (self.participant != null or self.allocator != null or self.checked or self.staged or self.poisoned) return error.InvalidBatch;
        participant.reset(participant.ptr);
        self.participant = participant;
    }

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        var iter = self.documents.keyIterator();
        while (iter.next()) |key| alloc.free(key.*);
        self.documents.deinit(alloc);
        var artifact_iter = self.artifacts.keyIterator();
        while (artifact_iter.next()) |key| alloc.free(key.*);
        self.artifacts.deinit(alloc);
        var owners = self.artifact_owners.keyIterator();
        while (owners.next()) |key| alloc.free(key.*);
        self.artifact_owners.deinit(alloc);
    }

    fn rememberArtifact(self: *Capture, alloc: std.mem.Allocator, key: []const u8) !void {
        if (self.artifacts.contains(key)) return;
        const owned = try alloc.dupe(u8, key);
        errdefer alloc.free(owned);
        try self.artifacts.put(alloc, owned, {});
    }

    pub fn touch(self: *Capture, alloc: std.mem.Allocator, txn: anytype, key: []const u8, value: ?[]const u8) !void {
        if (self.staging) return;
        if (self.participant) |participant| participant.observe(participant.ptr, key, value);
        self.allocator = alloc;
        errdefer self.poisoned = true;
        const replay_entry = key.len == keys.replay_key_len and key[0] == keys.replay_namespace and key[1] == keys.replay_all_kind;
        const replay_meta = std.mem.eql(u8, key, &keys.replay_meta_next_sequence_key);
        if (replay_entry or replay_meta) {
            if (!self.checked) {
                self.selected = try publication.authority(txn);
                self.checked = true;
            }
            if (self.selected == null) return;
            if (self.staged) return error.RetainedEffectsMixedControl;
            if (value != null) {
                if (replay_entry) {
                    const sequence = std.mem.readInt(u64, key[2..][0..8], .big);
                    if (sequence == 0 or (self.replay_sequence != null and self.replay_sequence.? != sequence)) self.replay_ambiguous = true;
                    self.replay_sequence = sequence;
                } else if (value.?.len == 8) {
                    const next = std.mem.readInt(u64, value.?[0..8], .little);
                    const previous = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
                    if (next <= 1 or (self.replay_next != null and self.replay_next.? != next) or
                        (if (previous) |raw| raw.len != 8 or next <= std.mem.readInt(u64, raw[0..8], .little) else false)) self.replay_ambiguous = true;
                    self.replay_next = next;
                } else self.replay_ambiguous = true;
            } else {
                // Journal retirement never gives a simultaneous data mutation
                // credit for effects that a projector may have already passed.
                self.replay_ambiguous = true;
            }
            return;
        }
        if (std.mem.eql(u8, key, &keys.raft_document_applied_entry_key)) {
            self.raft_marker = if (value) |raw| if (raw.len == 16) raw[0..16].* else null else null;
            return;
        }
        if (std.mem.eql(u8, key, authority.key)) {
            if (value) |raw| {
                const next = try authority.State.decode(raw);
                if (next.kind == .native and next.sequence != 0) {
                    if (try authority.load(txn)) |old| {
                        if (old.kind == .native and std.mem.eql(u8, &old.namespace, &next.namespace) and next.sequence == old.sequence +| 1)
                            self.native_position = next.sequence;
                    }
                }
            }
            return;
        }
        const artifact = publication.guardedArtifactKey(key);
        if (!artifact and !keys.isStoredDocumentRowKey(key) and !keys.isTtlKey(key)) return;
        if (self.staged) return error.RetainedEffectsMixedControl;
        if (!self.checked) {
            self.selected = try publication.authority(txn);
            self.checked = true;
        }
        if (self.selected == null) return;
        if (artifact) {
            if (self.artifacts.contains(key)) return;
            const document = try @import("db/artifact_publication_owner.zig").documentAlloc(alloc, key);
            var owned = true;
            defer if (owned) alloc.free(document);
            const owner = try self.artifact_owners.getOrPut(alloc, document);
            if (!owner.found_existing) owned = false;
            // Chunk reconstruction pages fence an entire root/unit stream,
            // including insertions and deletions beyond their current cursor.
            // The manifest need not exist: its revision is a prefix witness,
            // not proof that an inventory or producer output was completed.
            if (try @import("db/artifact_chunk_manifest.zig").keyForMemberAlloc(alloc, key)) |stream| {
                defer alloc.free(stream);
                try self.rememberArtifact(alloc, stream);
            }
            if (@import("db/artifact_generation_scope.zig").isHead(key)) {
                // Head publication changes logical stream membership even if
                // every legacy physical key is unchanged. Invalidate both the
                // exact head proof and the whole-stream reconstruction witness.
                const stream = try alloc.dupe(u8, key);
                defer alloc.free(stream);
                stream[keys.findComponentTerminator(stream, 1).? + 2] = try @import("db/artifact_generation_scope.zig").manifestKindForHead(key);
                try self.rememberArtifact(alloc, stream);
            }
            if (@import("db/online_graph_artifacts.zig").isKey(key)) {
                const sentinels = try @import("db/graph_mutation_scopes.zig").countSentinelsAlloc(alloc, key);
                defer {
                    for (sentinels) |sentinel| alloc.free(sentinel);
                    alloc.free(sentinels);
                }
                for (sentinels) |sentinel| try self.rememberArtifact(alloc, sentinel);
            }
            try self.rememberArtifact(alloc, key);
            return;
        }
        const document = (try keys.decodeDocumentComponentAlloc(alloc, key)) orelse return error.InvalidBatchRequest;
        errdefer alloc.free(document);
        const result = try self.documents.getOrPut(alloc, document);
        if (result.found_existing) alloc.free(document);
    }

    /// Called after retained capture; its native clock advance, if any, is
    /// reused. The caller temporarily suppresses retained metadata observation
    /// while this helper stages only the revision sidecars and native clock.
    pub fn stage(self: *Capture, txn: anytype, retained_advanced: bool) !void {
        if (self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.staged or (self.documents.count() == 0 and self.artifacts.count() == 0)) return;
        errdefer self.poisoned = true;
        const expected = self.selected orelse return error.ArtifactCatalogCorrupt;
        const current = (try publication.authority(txn)) orelse return error.RetainedEffectsFenceMismatch;
        if (!std.meta.eql(expected, current)) return error.RetainedEffectsFenceMismatch;
        const owner = (try authority.load(txn)) orelse return error.OnlineSourceScopeChanged;
        if (!std.mem.eql(u8, &owner.namespace, &expected.namespace)) return error.IdentityNamespaceMismatch;
        self.staging = true;
        defer self.staging = false;
        const position: publication.Position = switch (owner.kind) {
            .raft => blk: {
                // Presence of an older stored marker is not proof: only a
                // marker observed in this exact transaction grants authority.
                const marker = self.raft_marker orelse return error.RetainedEffectsFenceMismatch;
                const result: publication.Position = .{ .raft = .{ .term = std.mem.readInt(u64, marker[0..8], .little), .index = std.mem.readInt(u64, marker[8..16], .little) } };
                try result.validate();
                break :blk result;
            },
            .native => blk: {
                if (self.raft_marker != null) return error.OnlineSourceScopeChanged;
                const sequence = if (retained_advanced or self.native_position != null) owner.sequence else try authority.advance(txn, expected.namespace, null);
                if (self.native_position) |observed| if (observed != sequence) return error.OnlineSourceScopeChanged;
                break :blk .{ .native = .{ .namespace = publication.namespaceFromBytes(expected.namespace), .sequence = sequence } };
            },
        };
        const encoded = try position.encode();
        const replay_sequence = if (!self.replay_ambiguous and self.replay_sequence != null and self.replay_next != null and
            self.replay_sequence.? != std.math.maxInt(u64) and self.replay_next.? == self.replay_sequence.? + 1) self.replay_sequence else null;
        if (replay_sequence == null) try @import("db/artifact_source_gap.zig").record(txn);
        const materialization = try (publication.Materialization{ .position = position, .replay_sequence = replay_sequence }).encode();
        var iter = self.documents.keyIterator();
        while (iter.next()) |document| {
            try txn.put(&publication.inputRevisionKey(expected.namespace, document.*), &encoded);
            try txn.put(&publication.materializationRevisionKey(expected.namespace, document.*), &materialization);
            _ = try @import("db/artifact_producer_obligations.zig").mark(self.allocator orelse return error.ArtifactCatalogCorrupt, txn, expected, document.*, position);
        }
        var artifacts = self.artifacts.keyIterator();
        while (artifacts.next()) |artifact| try txn.put(&publication.artifactRevisionKey(expected.namespace, artifact.*), &encoded);
        if (self.participant) |participant| {
            var view_txn = txn;
            try participant.stage(participant.ptr, @import("commit_participant.zig").View.from(&view_txn), &encoded);
        }
        var owners = self.artifact_owners.keyIterator();
        while (owners.next()) |document| {
            if (!self.documents.contains(document.*)) {
                try txn.put(&publication.materializationRevisionKey(expected.namespace, document.*), &materialization);
                // A changed scope set can introduce required outputs that
                // have no old provenance reference for the validation sweep
                // to discover. Reopen completion once per affected owner,
                // preserving its PRIMARY position rather than substituting
                // this artifact publication's materialization position.
                const input_position = try publication.inputRevision(txn, expected.namespace, document.*);
                _ = try @import("db/artifact_producer_obligations.zig").mark(self.allocator orelse return error.ArtifactCatalogCorrupt, txn, expected, document.*, input_position);
            }
        }
        if (try @import("db/artifact_producer_obligations.zig").load(txn) != null)
            try @import("db/artifact_producer_validation.zig").invalidate(self.allocator orelse return error.ArtifactCatalogCorrupt, txn, expected);
        self.staged = true;
    }
};

test "ordered artifact inventory input capture requires same transaction authority and distinguishes native clocks" {
    const Fake = struct {
        values: std.StringHashMap([]u8),
        pub fn delete(self: *@This(), key: []const u8) !void {
            if (self.values.fetchRemove(key)) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
        }
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const owned = try std.testing.allocator.dupe(u8, value);
            errdefer std.testing.allocator.free(owned);
            if (self.values.getPtr(key)) |old| {
                std.testing.allocator.free(old.*);
                old.* = owned;
            } else {
                const owned_key = try std.testing.allocator.dupe(u8, key);
                errdefer std.testing.allocator.free(owned_key);
                try self.values.put(owned_key, owned);
            }
        }
        pub fn deinit(self: *@This()) void {
            var iter = self.values.iterator();
            while (iter.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.values.deinit();
        }
    };
    const alloc = std.testing.allocator;
    const namespace: [24]u8 = @splat(1);
    const activation: publication.Command = .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    const document = try keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(document);
    for ([_]authority.Kind{ .raft, .native }) |kind| {
        var txn: Fake = .{ .values = std.StringHashMap([]u8).init(alloc) };
        defer txn.deinit();
        try authority.bind(&txn, kind, namespace);
        try publication.stageAuthority(&txn, activation);
        const obligations = @import("db/artifact_producer_obligations.zig");
        const active = (try publication.authority(&txn)).?;
        try obligations.begin(alloc, &txn, active);
        try @import("db/artifact_producer_validation.zig").begin(alloc, &txn, active);
        var capture: Capture = .{};
        defer capture.deinit(alloc);
        try capture.touch(alloc, &txn, document, "row");
        try txn.put(document, "row");
        if (kind == .raft) {
            var marker: [16]u8 = undefined;
            std.mem.writeInt(u64, marker[0..8], 2, .little);
            std.mem.writeInt(u64, marker[8..16], 9, .little);
            // An old persisted watermark alone must not grant authority.
            try txn.put(&keys.raft_document_applied_entry_key, &marker);
            try std.testing.expectError(error.RetainedEffectsFenceMismatch, capture.stage(&txn, false));
            capture.poisoned = false;
            try capture.touch(alloc, &txn, &keys.raft_document_applied_entry_key, &marker);
        }
        try capture.stage(&txn, false);
        const position = (try publication.inputRevision(&txn, namespace, "doc")).?;
        try std.testing.expectEqualDeep(position, (try publication.materializationRevision(&txn, namespace, "doc")).?);
        try std.testing.expectEqual(null, (try publication.materializationState(&txn, namespace, "doc")).?.replay_sequence);
        if (kind == .raft) {
            try std.testing.expectEqualDeep(@as(publication.Position, .{ .raft = .{ .term = 2, .index = 9 } }), position);
        } else {
            try std.testing.expect(position == .native);
            try std.testing.expectEqual(@as(u64, 1), position.native.sequence);
            try std.testing.expectEqual(@as(u64, 1), (try authority.load(&txn)).?.sequence);
        }
        try capture.stage(&txn, false);
        try std.testing.expectEqualDeep(position, (try publication.inputRevision(&txn, namespace, "doc")).?);
        const original_work = (try obligations.lookupWork(alloc, &txn, active, "doc")).?;
        try obligations.complete(alloc, &txn, active, original_work);
        try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&txn)).?.pending_documents);
        const manifests = @import("db/artifact_chunk_manifest.zig");
        const root = try manifests.keyAlloc(alloc, "doc", "chunks");
        defer alloc.free(root);
        const unit = try manifests.scopedKeyAlloc(alloc, "doc", "chunks", "unit\x00");
        defer alloc.free(unit);
        const root_zero = try keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 0);
        defer alloc.free(root_zero);
        const root_one = try keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 1);
        defer alloc.free(root_one);
        const unit_zero = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", "unit\x00", 0);
        defer alloc.free(unit_zero);
        var chunk_capture: Capture = .{};
        defer chunk_capture.deinit(alloc);
        var replay_next: [8]u8 = undefined;
        std.mem.writeInt(u64, &replay_next, 42, .little);
        const replay_key = keys.replayEntryKey(keys.replay_all_kind, 41);
        try chunk_capture.touch(alloc, &txn, &keys.replay_meta_next_sequence_key, &replay_next);
        try txn.put(&keys.replay_meta_next_sequence_key, &replay_next);
        try chunk_capture.touch(alloc, &txn, &replay_key, "journal");
        try txn.put(&replay_key, "journal");
        try chunk_capture.touch(alloc, &txn, root_zero, "new");
        try chunk_capture.touch(alloc, &txn, root_one, null);
        try chunk_capture.touch(alloc, &txn, unit_zero, null);
        const head = try alloc.dupe(u8, unit);
        defer alloc.free(head);
        head[keys.findComponentTerminator(head, 1).? + 2] = keys.producer_generation_head_kind;
        try chunk_capture.touch(alloc, &txn, head, null);
        const extraction = try @import("db/artifact_generation_scope.zig").extractionKeyAlloc(alloc, "doc", "chunks");
        defer alloc.free(extraction);
        const extraction_head = try alloc.dupe(u8, extraction);
        defer alloc.free(extraction_head);
        extraction_head[keys.findComponentTerminator(extraction_head, 1).? + 2] = keys.extraction_generation_head_kind;
        try chunk_capture.touch(alloc, &txn, extraction_head, null);
        // Head and member changes deduplicate the same stream witness.
        try std.testing.expectEqual(@as(usize, 8), chunk_capture.artifacts.count());
        try std.testing.expectEqual(@as(usize, 1), chunk_capture.artifact_owners.count());
        if (kind == .raft) {
            var marker: [16]u8 = undefined;
            std.mem.writeInt(u64, marker[0..8], 2, .little);
            std.mem.writeInt(u64, marker[8..16], 11, .little);
            try chunk_capture.touch(alloc, &txn, &keys.raft_document_applied_entry_key, &marker);
        }
        try chunk_capture.stage(&txn, false);
        try std.testing.expectEqual(@as(?u64, 41), (try publication.materializationState(&txn, namespace, "doc")).?.replay_sequence);
        const reopened_work = (try obligations.lookupWork(alloc, &txn, active, "doc")).?;
        try std.testing.expectEqualDeep(original_work.position, reopened_work.position);
        try std.testing.expectEqual(original_work.revision + 1, reopened_work.revision);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&txn)).?.pending_documents);
        // Several changed artifacts for one owner reopen one obligation; a
        // repeated staging call cannot increment its revision again.
        try chunk_capture.stage(&txn, false);
        try std.testing.expectEqual(reopened_work.revision, (try obligations.lookupWork(alloc, &txn, active, "doc")).?.revision);
        // A later journal deletion must abort the transaction, not leave an
        // already-staged boundary referring to a retired replay record.
        try std.testing.expectError(error.RetainedEffectsMixedControl, chunk_capture.touch(alloc, &txn, &replay_key, null));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, chunk_capture.stage(&txn, false));
        try std.testing.expectError(error.NotFound, txn.get(root));
        try std.testing.expectError(error.NotFound, txn.get(unit));
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, root_zero), try publication.artifactRevision(&txn, namespace, root));
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, unit_zero), try publication.artifactRevision(&txn, namespace, unit));
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, head), try publication.artifactRevision(&txn, namespace, unit));
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, head), try publication.materializationRevision(&txn, namespace, "doc"));
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, extraction_head), try publication.artifactRevision(&txn, namespace, extraction));
        // A graph tombstone changes the prefix witness even when no caller
        // writes a visible-count value. The sentinel is revision-only.
        const graph_key = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc", "graph", "links", "target", "source");
        defer alloc.free(graph_key);
        const sentinel = try keys.graphEdgeContenderCountKeyAlloc(alloc, "doc", "graph");
        defer alloc.free(sentinel);
        var graph_capture: Capture = .{};
        defer graph_capture.deinit(alloc);
        // Reusing an old journal record and watermark cannot certify new
        // effects, even when both keys appear in this physical transaction.
        try graph_capture.touch(alloc, &txn, &keys.replay_meta_next_sequence_key, &replay_next);
        try graph_capture.touch(alloc, &txn, &replay_key, "journal");
        try graph_capture.touch(alloc, &txn, graph_key, null);
        if (kind == .raft) {
            var marker: [16]u8 = undefined;
            std.mem.writeInt(u64, marker[0..8], 2, .little);
            std.mem.writeInt(u64, marker[8..16], 10, .little);
            try graph_capture.touch(alloc, &txn, &keys.raft_document_applied_entry_key, &marker);
        }
        try graph_capture.stage(&txn, false);
        try std.testing.expectEqual(null, (try publication.materializationState(&txn, namespace, "doc")).?.replay_sequence);
        try std.testing.expectError(error.NotFound, txn.get(sentinel));
        try std.testing.expect((try publication.artifactRevision(&txn, namespace, sentinel)) != null);
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, graph_key), try publication.artifactRevision(&txn, namespace, sentinel));
        try std.testing.expectEqualDeep(try publication.artifactRevision(&txn, namespace, graph_key), try publication.materializationRevision(&txn, namespace, "doc"));
        try std.testing.expectEqualDeep(position, (try publication.inputRevision(&txn, namespace, "doc")).?);
        const before_other = try publication.materializationRevision(&txn, namespace, "doc");
        var other_capture: Capture = .{};
        defer other_capture.deinit(alloc);
        const other = try keys.chunkArtifactKeyAlloc(alloc, "other\x00", "chunks", 0);
        defer alloc.free(other);
        try other_capture.touch(alloc, &txn, other, "value");
        if (kind == .raft) {
            var marker: [16]u8 = undefined;
            std.mem.writeInt(u64, marker[0..8], 2, .little);
            std.mem.writeInt(u64, marker[8..16], 12, .little);
            try other_capture.touch(alloc, &txn, &keys.raft_document_applied_entry_key, &marker);
        }
        try other_capture.stage(&txn, false);
        try std.testing.expectEqualDeep(before_other, try publication.materializationRevision(&txn, namespace, "doc"));
        try std.testing.expect((try publication.materializationRevision(&txn, namespace, "other\x00")) != null);
        // Nested resolver ownership belongs to the original document, not the
        // encoded chunk key. Producer completion must watch the same owner.
        const resolution = try keys.resolutionArtifactKeyAlloc(alloc, unit_zero, "entities");
        defer alloc.free(resolution);
        var nested: Capture = .{};
        defer nested.deinit(alloc);
        try nested.touch(alloc, &txn, resolution, null);
        try std.testing.expect(nested.artifact_owners.contains("doc"));
        try std.testing.expect(!nested.artifact_owners.contains(unit_zero));
    }
}
