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

//! Authenticated, paged file identities and delete-aware publication deltas.
//! Query navigation resolves only reached files; legacy inventories remain valid.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const native = @import("lake_index_native_state.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const Inventory = local.serverless_external_source_types.Inventory;
pub const DeleteProof = struct { recipe: [32]u8, digest: [32]u8, source: [32]u8, data: [32]u8, count: u32, versions: tree.Ref };
const DeleteVersion = struct { recipe: [32]u8, etag: []const u8, version: []const u8 };
fn deleteRecipe(value: anytype) [32]u8 {
    var buffer: [4096]u8 = undefined;
    var writer: std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256) = .init(&buffer);
    std.json.Stringify.value(value, .{}, &writer.writer) catch unreachable;
    writer.writer.flush() catch unreachable;
    var digest: [32]u8 = undefined;
    writer.hasher.final(&digest);
    return digest;
}
fn exactVersion(version: []const u8) bool {
    return version.len != 0 and !std.mem.eql(u8, version, "null") and !std.mem.startsWith(u8, version, "object-stat:") and !std.mem.startsWith(u8, version, "iceberg:");
}
pub const Root = struct {
    version: u8 = 2,
    coverage: ?struct { version: u8 = 1, plan: ?[32]u8, data: [32]u8 } = null,
    domain: [32]u8,
    source: []const u8,
    schema: []const u8,
    snapshot: []const u8,
    count: u32,
    rows: ?u64 = null,
    bytes: ?u64 = null,
    files: ?tree.Ref = null,
    file_summary: ?Summary = null,
    public: ?tree.Ref = null,
    private: ?tree.Ref = null,
    delete_proof: ?DeleteProof = null,
};
const Proof = struct { id: [32]u8, digest: [32]u8, index: usize };
pub const Summary = struct {
    first: [32]u8,
    last: [32]u8,
    digest: [32]u8,
    count: u64,
    fn encode(self: @This(), a: A) ![]u8 {
        const bytes = try a.alloc(u8, 104);
        bytes[0..32].* = self.first;
        bytes[32..64].* = self.last;
        bytes[64..96].* = self.digest;
        std.mem.writeInt(u64, bytes[96..104], self.count, .big);
        return bytes;
    }
    fn decode(bytes: []const u8, count: u64) !@This() {
        if (bytes.len != 104 or std.mem.readInt(u64, bytes[96..104], .big) != count or count == 0 or std.mem.order(u8, bytes[0..32], bytes[32..64]) == .gt) return error.InvalidNativeLakeFileState;
        return .{ .first = bytes[0..32].*, .last = bytes[32..64].*, .digest = bytes[64..96].*, .count = count };
    }
};
fn proofHash(id: [32]u8, digest: [32]u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-file-set-proof-v1");
    hash.update(&id);
    hash.update(&digest);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn reduceFiles(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var result: ?Summary = null;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (entries) |entry| {
        _ = scratch.reset(.retain_capacity);
        const summary = if (height == 0) leaf: {
            const record = try std.json.parseFromSliceLeaky(Record, scratch.allocator(), entry.value, .{});
            const id = key(record.file.file_id);
            if (!std.mem.eql(u8, entry.key, &id)) return error.InvalidNativeLakeFileState;
            break :leaf Summary{ .first = id, .last = id, .count = 1, .digest = proofHash(id, record.digest) };
        } else branch: {
            if (entry.aggregate.len == 0) return try a.alloc(u8, 0);
            break :branch try Summary.decode(entry.aggregate, entry.child.?.records);
        };
        if (result) |*value| {
            if (std.mem.order(u8, &value.last, &summary.first) != .lt) return error.InvalidNativeLakeFileState;
            value.last = summary.last;
            value.count = try std.math.add(u64, value.count, summary.count);
            for (&value.digest, summary.digest) |*byte, other| byte.* ^= other;
        } else result = summary;
    }
    return if (result) |value| value.encode(a) else try a.alloc(u8, 0);
}
fn proofLower(proofs: []const Proof, id: [32]u8, inclusive: bool) usize {
    var lo: usize = 0;
    var hi = proofs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const order = std.mem.order(u8, &proofs[mid].id, &id);
        if (order == .lt or (inclusive and order == .eq)) lo = mid + 1 else hi = mid;
    }
    return lo;
}
const Record = struct { file: local.serverless_external_source_types.FileEntry, digest: [32]u8, deletes: [32]u8 };
pub const Delta = struct {
    source: []const u8,
    schema: []const u8,
    prior_snapshot: ?[]const u8,
    prior_count: u32,
    files: []native.File,
    changed: []bool,
    prior_changed: []const native.File,
    by_id: std.StringHashMapUnmanaged(usize),
};
fn qualified(provider: *Provider, file: local.serverless_external_source_types.FileEntry) local.serverless_external_source_types.FileEntry {
    var result = file;
    if (provider.source.verified_files) |files| if (files.get(file.file_id)) |verified| {
        result.etag = verified.etag;
        result.version_id = verified.version_id;
    };
    result.row_groups = &.{};
    result.lower_bounds = &.{};
    result.upper_bounds = &.{};
    return result;
}
fn key(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
pub fn load(a: A, store: stores.ArtifactStore, ref: Ref, token: Token, cached: ?artifacts.CachedRead) !Root {
    if (ref.kind != .doc_values or (ref.metadata_version != 1 and ref.metadata_version != 2) or ref.byte_len > 64 * 1024) return error.InvalidNativeLakeFileState;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFileState;
    if (root.version != ref.metadata_version or !std.mem.eql(u8, &scope.domain, &root.domain) or root.source.len == 0 or root.schema.len == 0 or root.snapshot.len == 0) return error.InvalidNativeLakeFileState;
    if (root.delete_proof) |proof| {
        try proof.versions.validate();
        if (proof.count == 0 or proof.versions.records != proof.count or root.coverage == null or !std.mem.eql(u8, &proof.data, &root.coverage.?.data)) return error.InvalidNativeLakeFileState;
    }
    if (root.coverage) |proof| if (root.version < 2 or proof.version != 1) return error.InvalidNativeLakeFileState;
    for ([_]?tree.Ref{ root.files, root.public, root.private }) |r| {
        if ((r == null) != (root.count == 0)) return error.InvalidNativeLakeFileState;
        if (r) |value| {
            try value.validate();
            if (value.records != root.count) return error.InvalidNativeLakeFileState;
        }
    }
    if (root.file_summary) |summary| if (summary.count != root.count or std.mem.order(u8, &summary.first, &summary.last) == .gt) return error.InvalidNativeLakeFileState;
    return root;
}
fn publishDeleteProof(a: A, storage_: tree.Store, provider: *Provider, data: [32]u8) !?DeleteProof {
    const source = provider.source;
    // A retained catalog snapshot plus exact immutable provider versions is
    // the proof contract. Unversioned stores keep fresh HEAD qualification.
    const plan = source.scanner.iceberg_delete_plan orelse return null;
    if (!source.immutable_objects or source.snapshot_pin == null) return null;
    if (plan.files.len == 0 or plan.files.len > std.math.maxInt(u32)) return null;
    try provider.context.ensureActive();
    const location = try local.serverless_query_lake_range_io.objectLocationForUri(plan.files[0].file_path);
    const client = if (source.context_store) |context_store| context_store.base else source.scanner.object_reader.client;
    if (!try client.supportsObjectVersions(location.bucket, provider.context.cancellation)) return null;
    const qualification = try local.serverless_query_lake_serving.DeleteQualification.create(a);
    defer qualification.destroy();
    const evidence_set = (try qualification.acquire(source.scanner.object_reader.client, plan, provider.context)) orelse return null;
    for (evidence_set.versions) |version| if (!exactVersion(version.version)) return null;
    const coverage = try @import("lake_index_coverage.zig").finishDigest(source, provider.context, data);
    if (!std.mem.eql(u8, &coverage.delete_objects, &evidence_set.digest)) return error.ExternalLakeIndexSourceChanged;
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = storage_ };
    defer builder.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (plan.files, evidence_set.versions, 0..) |file, version, index| {
        try provider.context.ensureActive();
        _ = scratch.reset(.retain_capacity);
        var key_: [32]u8 = @splat(0);
        std.mem.writeInt(u32, key_[28..32], @intCast(index), .big);
        const bytes = try std.json.Stringify.valueAlloc(scratch.allocator(), DeleteVersion{ .recipe = deleteRecipe(file), .etag = version.etag, .version = version.version }, .{});
        try builder.add(&key_, bytes);
    }
    return .{ .recipe = evidence_set.recipe, .digest = evidence_set.digest, .source = coverage.source, .data = data, .count = @intCast(plan.files.len), .versions = (try builder.finish()).? };
}
/// Allocates only one reached delete selector; no archive-wide version map.
pub fn deleteVersion(a: A, store: stores.ArtifactStore, root: Root, index: usize, file: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile, token: Token, context: local.serverless_query_lake_read_context.Context) !local.serverless_query_lake_serving.DeleteQualification.Version {
    try context.ensureActive();
    try token.check();
    const proof = root.delete_proof orelse return error.InvalidNativeLakeFileState;
    if (index >= proof.count) return error.InvalidNativeLakeFileState;
    var reads: u64 = 8 * 1024 * 1024;
    var writes: u64 = 0;
    var backend = store;
    var pages: pages_mod.PageStore = .{ .domain = root.domain, .artifacts = &backend, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var key_: [32]u8 = @splat(0);
    std.mem.writeInt(u32, key_[28..32], @intCast(index), .big);
    const bytes = (try lookup(sa, pages.store(), proof.versions, &key_)) orelse return error.InvalidNativeLakeFileState;
    return ownDeleteVersion(a, bytes, file);
}
fn ownDeleteVersion(a: A, bytes: []const u8, file: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile) !local.serverless_query_lake_serving.DeleteQualification.Version {
    const parsed = try std.json.parseFromSlice(DeleteVersion, a, bytes, .{});
    defer parsed.deinit();
    const record = parsed.value;
    if (!std.mem.eql(u8, &record.recipe, &deleteRecipe(file)) or !exactVersion(record.version)) return error.InvalidNativeLakeFileState;
    const etag = try a.dupe(u8, record.etag);
    errdefer a.free(etag);
    return .{ .etag = etag, .version = try a.dupe(u8, record.version), .sequence = file.data_sequence_number, .position = file.content == .position_deletes };
}
fn lookup(a: A, storage: tree.Store, root: ?tree.Ref, id: []const u8) !?[]u8 {
    var cursor = try tree.Cursor.init(a, storage, root, id, null);
    defer cursor.deinit();
    const found = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, found.key, id)) return null;
    return try a.dupe(u8, found.value);
}
const Batch = struct {
    arena: std.heap.ArenaAllocator,
    changes: std.ArrayList(tree.Mutation) = .empty,
    fn add(self: *@This(), id: []const u8, value: ?[]const u8) !void {
        const a = self.arena.allocator();
        try self.changes.append(a, .{ .key = try a.dupe(u8, id), .value = if (value) |v| try a.dupe(u8, v) else null });
    }
    fn flush(self: *@This(), storage: tree.Store, root: *?tree.Ref) !void {
        if (self.changes.items.len == 0) return;
        std.mem.sort(tree.Mutation, self.changes.items, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        root.* = try tree.apply(self.arena.allocator(), storage, root.*, self.changes.items);
        _ = self.arena.reset(.retain_capacity);
        self.changes = .empty;
    }
};
pub const BuildStats = struct { prior_pages: usize = 0, prior_records: usize = 0, skipped_subtrees: usize = 0 };
pub const Built = struct { ref: Ref, delta: Delta, stats: BuildStats };
pub fn build(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, prior: ?Root, token: Token) !Built {
    return buildProven(a, out, store, provider, prior, token, null);
}
pub fn buildProven(a: A, out: A, store: *stores.ArtifactStore, provider: *Provider, prior: ?Root, token: Token, data: ?[32]u8) !Built {
    const inv = provider.source.inventory;
    const compatible = if (prior) |old| std.mem.eql(u8, old.source, inv.source_id) and std.mem.eql(u8, old.schema, inv.schema_fingerprint) and std.mem.eql(u8, &old.domain, &store.upload_scope.?.domain) else false;
    var root: Root = .{ .domain = store.upload_scope.?.domain, .source = inv.source_id, .schema = inv.schema_fingerprint, .snapshot = inv.snapshot_id, .count = @intCast(inv.files.len) };
    root.rows = 0;
    root.bytes = 0;
    for (inv.files) |file| {
        root.rows.? +|= file.row_count;
        root.bytes.? +|= file.byte_len;
    }
    if (data) |digest| root.coverage = .{ .plan = if (provider.source.immutable_objects) provider.source.plan_identity else null, .data = digest };
    if (compatible) {
        root.files = prior.?.files;
        root.private = prior.?.private;
    }
    var reads: u64 = tree.max_page_bytes;
    var writes: u64 = tree.max_page_bytes;
    var pages: pages_mod.PageStore = .{ .domain = root.domain, .attempt = store.upload_scope.?.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    var file_storage = pages.store();
    file_storage.reduce = reduceFiles;
    if (data) |digest| root.delete_proof = try publishDeleteProof(a, pages.store(), provider, digest);
    var files: Batch = .{ .arena = .init(a) };
    defer files.arena.deinit();
    var private: Batch = .{ .arena = .init(a) };
    defer private.arena.deinit();
    const PublicEntry = struct { digest: [32]u8, file: []const u8 };
    var native_by_id: std.StringHashMapUnmanaged(usize) = .empty;
    const current = try out.alloc(native.File, inv.files.len);
    const changed = try out.alloc(bool, inv.files.len);
    @memset(changed, true);
    const proofs = try a.alloc(Proof, inv.files.len);
    defer a.free(proofs);
    const prefix = try a.alloc([32]u8, inv.files.len + 1);
    defer a.free(prefix);
    prefix[0] = @splat(0);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (inv.files, current, 0..) |file, *state, i| {
        try provider.context.ensureActive();
        try token.check();
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const digest = try native.identity(sa, provider, file);
        const deletes = if (provider.source.prepared_deletes) |prepared| try prepared.fileFingerprint(file.file_id) else local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint();
        state.* = .{ .id = file.file_id, .digest = digest };
        proofs[i] = .{ .id = key(file.file_id), .digest = digest, .index = i };
        try native_by_id.put(out, file.file_id, i);
        const bytes = try std.json.Stringify.valueAlloc(sa, Record{ .file = qualified(provider, file), .digest = digest, .deletes = deletes }, .{});
        // Defer stable file/private mutations until the old paged census proves
        // which records changed. Public IDs intentionally fence each snapshot.
        if (!compatible) {
            try files.add(&key(file.file_id), bytes);
            try private.add(&digest, file.file_id);
            if (files.changes.items.len >= 64) {
                try files.flush(file_storage, &root.files);
                try private.flush(pages.store(), &root.private);
            }
        }
    }
    var prior_changed: std.ArrayList(native.File) = .empty;
    var stats: BuildStats = .{};
    std.mem.sort(Proof, proofs, {}, struct {
        fn less(_: void, l: Proof, r: Proof) bool {
            return std.mem.order(u8, &l.id, &r.id) == .lt;
        }
    }.less);
    for (proofs, 0..) |proof, i| {
        prefix[i + 1] = prefix[i];
        const digest = proofHash(proof.id, proof.digest);
        for (&prefix[i + 1], digest) |*byte, other| byte.* ^= other;
    }
    root.file_summary = if (proofs.len != 0) .{ .first = proofs[0].id, .last = proofs[proofs.len - 1].id, .count = proofs.len, .digest = prefix[proofs.len] } else null;
    if (compatible) {
        const Diff = struct {
            a: A,
            out: A,
            storage: tree.Store,
            file_storage: tree.Store,
            provider: *Provider,
            proofs: []const Proof,
            prefix: []const [32]u8,
            current: []const native.File,
            changed: []bool,
            prior_changed: *std.ArrayList(native.File),
            files: *Batch,
            private: *Batch,
            root: *Root,
            token: Token,
            stats: *BuildStats,
            fn unchanged(self: *@This(), summary: Summary) bool {
                const lower = proofLower(self.proofs, summary.first, false);
                const upper = proofLower(self.proofs, summary.last, true);
                if (upper - lower != summary.count) return false;
                var digest = self.prefix[upper];
                for (&digest, self.prefix[lower]) |*byte, other| byte.* ^= other;
                if (!std.mem.eql(u8, &digest, &summary.digest)) return false;
                for (self.proofs[lower..upper]) |proof| self.changed[proof.index] = false;
                self.stats.skipped_subtrees += 1;
                return true;
            }
            pub fn childSummary(self: *@This(), ref: tree.Ref, bytes: []const u8) anyerror!void {
                try self.provider.context.ensureActive();
                try self.token.check();
                if (bytes.len != 0 and self.unchanged(try Summary.decode(bytes, ref.records))) return;
                self.stats.prior_pages += 1;
                try tree.walkPage(self.a, self.storage, ref, self);
            }
            pub fn record(self: *@This(), id: []const u8, bytes: []const u8) anyerror!void {
                self.stats.prior_records += 1;
                var arena_ = std.heap.ArenaAllocator.init(self.a);
                defer arena_.deinit();
                const sa = arena_.allocator();
                const old = try std.json.parseFromSliceLeaky(Record, sa, bytes, .{});
                const old_id = key(old.file.file_id);
                if (!std.mem.eql(u8, id, &old_id)) return error.InvalidNativeLakeFileState;
                const index = proofLower(self.proofs, old_id, false);
                if (index < self.proofs.len and std.mem.eql(u8, &self.proofs[index].id, &old_id)) {
                    const i = self.proofs[index].index;
                    if (!std.mem.eql(u8, old.file.file_id, self.current[i].id)) return error.InvalidNativeLakeFileState;
                    self.changed[i] = !std.mem.eql(u8, &old.digest, &self.current[i].digest);
                    if (!self.changed[i]) return;
                }
                try self.prior_changed.append(self.out, .{ .id = try self.out.dupe(u8, old.file.file_id), .digest = old.digest });
                try self.files.add(id, null);
                try self.private.add(&old.digest, null);
                if (self.files.changes.items.len >= 64) {
                    try self.files.flush(self.file_storage, &self.root.files);
                    try self.private.flush(self.storage, &self.root.private);
                }
            }
        };
        var diff: Diff = .{ .a = a, .out = out, .storage = pages.store(), .file_storage = file_storage, .provider = provider, .proofs = proofs, .prefix = prefix, .current = current, .changed = changed, .prior_changed = &prior_changed, .files = &files, .private = &private, .root = &root, .token = token, .stats = &stats };
        if (prior.?.files) |ref| {
            if (prior.?.file_summary == null or !diff.unchanged(prior.?.file_summary.?)) {
                stats.prior_pages += 1;
                try tree.walkPage(a, pages.store(), ref, &diff);
            }
        }
        try files.flush(file_storage, &root.files);
        try private.flush(pages.store(), &root.private);
        for (inv.files, current, changed) |file, state, is_changed| {
            if (!is_changed) continue;
            _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            const deletes = if (provider.source.prepared_deletes) |prepared| try prepared.fileFingerprint(file.file_id) else local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint();
            const bytes = try std.json.Stringify.valueAlloc(sa, Record{ .file = qualified(provider, file), .digest = state.digest, .deletes = deletes }, .{});
            try files.add(&key(file.file_id), bytes);
            try private.add(&state.digest, file.file_id);
            if (files.changes.items.len >= 64) {
                try files.flush(file_storage, &root.files);
                try private.flush(pages.store(), &root.private);
            }
        }
    }
    try files.flush(file_storage, &root.files);
    try private.flush(pages.store(), &root.private);
    const same_membership = membership: {
        if (!compatible or prior.?.count != inv.files.len or !std.mem.eql(u8, prior.?.snapshot, inv.snapshot_id)) break :membership false;
        for (prior_changed.items) |old| if (!native_by_id.contains(old.id)) break :membership false;
        break :membership true;
    };
    if (same_membership) root.public = prior.?.public else {
        const public_entries = try a.alloc(PublicEntry, inv.files.len);
        defer a.free(public_entries);
        for (inv.files, public_entries) |file, *entry| entry.* = .{ .digest = local.storage_rowsource_identity.fileDigest(inv.source_id, inv.snapshot_id, file.file_id), .file = file.file_id };
        std.mem.sort(PublicEntry, public_entries, {}, struct {
            fn less(_: void, l: PublicEntry, r: PublicEntry) bool {
                return std.mem.order(u8, &l.digest, &r.digest) == .lt;
            }
        }.less);
        const PublicSource = struct {
            entries: []const PublicEntry,
            position: usize = 0,
            pub fn next(self: *@This()) !?tree.Cursor.Record {
                if (self.position == self.entries.len) return null;
                const entry = &self.entries[self.position];
                self.position += 1;
                return .{ .key = &entry.digest, .value = entry.file };
            }
        };
        var public_source: PublicSource = .{ .entries = public_entries };
        root.public = try tree.buildSorted(a, pages.store(), &public_source);
    }
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(bytes);
    var uploaded = try store.putWithCancellation(bytes, token);
    defer uploaded.deinit(store.allocator);
    return .{ .stats = stats, .ref = .{ .kind = .doc_values, .metadata_version = 2, .artifact_id = try out.dupe(u8, uploaded.artifact_id), .checksum = try out.dupe(u8, uploaded.checksum), .byte_len = uploaded.byte_len }, .delta = .{ .source = inv.source_id, .schema = inv.schema_fingerprint, .prior_count = if (compatible) prior.?.count else 0, .prior_snapshot = if (compatible) try out.dupe(u8, prior.?.snapshot) else null, .files = current, .changed = changed, .prior_changed = prior_changed.items, .by_id = native_by_id } };
}
/// The resolver borrows fresh publication authority. Only immutable root/page
/// payloads enter shared caches; request contexts never escape this owner.
pub const Resolver = struct {
    root: Root,
    store: stores.ArtifactStore,
    cached: artifacts.CachedRead,
    token: Token,
    reads: u64 = 64 * 1024 * 1024,
    writes: u64 = 0,
    pages: pages_mod.PageStore = undefined,
    page_cache: ?tree.Cache = null,
    page_cache_limit: usize = 1024 * 1024,
    memo_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 },
    memo_arena: ?std.heap.ArenaAllocator = null,
    public_memo: std.StringHashMapUnmanaged([]const u8) = .empty,
    private_memo: std.StringHashMapUnmanaged([]const u8) = .empty,
    digest_memo: std.StringHashMapUnmanaged([]const u8) = .empty,
    memo_disabled: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *@This()) !void {
        while (!self.mutex.tryLock()) {
            try self.token.check();
            try self.cached.context.ensureActive();
            if (self.cached.context.io) |io| try io.sleep(std.Io.Duration.zero, .awake) else @import("antfly_platform").time.yieldBriefly();
        }
    }
    pub fn deinit(self: *@This()) void {
        if (self.page_cache) |*cache| cache.deinit();
        if (self.memo_arena) |*arena| arena.deinit();
    }
    fn remember(self: *@This(), map: *std.StringHashMapUnmanaged([]const u8), id: []const u8, value: []const u8) ?[]const u8 {
        if (self.memo_disabled or self.public_memo.count() + self.private_memo.count() + self.digest_memo.count() >= 512) return null;
        if (self.memo_arena == null) self.memo_arena = .init(self.memo_budget.allocator());
        const a = self.memo_arena.?.allocator();
        const owned_id = a.dupe(u8, id) catch {
            self.memo_disabled = true;
            return null;
        };
        const owned = a.dupe(u8, value) catch {
            self.memo_disabled = true;
            return null;
        };
        map.put(a, owned_id, owned) catch {
            self.memo_disabled = true;
            return null;
        };
        return owned;
    }
    fn storage(self: *@This()) tree.Store {
        self.pages = .{ .domain = self.root.domain, .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .read_cache = .{ .ptr = self, .read = readPage } };
        if (self.page_cache == null) self.page_cache = .{ .alloc = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .underlying = self.pages.store(), .max_bytes = self.page_cache_limit };
        return self.page_cache.?.store();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: Ref, offset: u64, length: usize, _: [32]u8, token: Token, budget: *u64) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        try stores.chargeReadBudget(budget, length);
        if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeFileState;
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
    fn point(self: *@This(), a: A, root: ?tree.Ref, id: []const u8) !?[]u8 {
        var ref = root orelse return null;
        var lower: ?[32]u8 = null;
        var upper: ?[32]u8 = null;
        const Loader = struct {
            owner: *Resolver,
            ref: tree.Ref,
            fn decode(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                const owned = item.arena.allocator();
                const page = try owned.create(tree.Page);
                page.* = try tree.readPage(owned, self_.owner.storage(), self_.ref);
                for (page.entries) |entry| if (entry.key.len != 32) return error.InvalidNativeLakeFileState;
                item.payload = .{ .extension = page };
            }
        };
        const Check = struct {
            owner: *Resolver,
            fn check(raw: *anyopaque) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                try self_.owner.token.check();
                try self_.owner.cached.context.ensureActive();
            }
        };
        var check: Check = .{ .owner = self };
        var context = self.cached.context;
        context.checkpoint = .{ .ptr = &check, .check = Check.check };
        while (true) {
            try context.ensureActive();
            const cache_key = @import("lake_wal_image.zig").digest(.{ .format = "decoded-file-navigation-page-v1", .scope = self.cached.scope, .domain = self.root.domain, .ref = ref });
            var loader: Loader = .{ .owner = self, .ref = ref };
            const lease = self.cached.cache.decoded.acquire(cache_key, @min(64 * 1024 * 1024, @as(usize, ref.bytes) * 16 + 65536), context, .{ .ptr = &loader, .load = Loader.decode }) catch |err| switch (err) {
                // Caching is optional under memory pressure. The ordinary
                // authenticated cursor retains the same complete point lookup.
                error.OutOfMemory, error.SqlMemoryLimitExceeded => return lookup(a, self.storage(), root, id),
                else => return err,
            };
            defer lease.release();
            try context.ensureActive();
            const page: *const tree.Page = @ptrCast(@alignCast(lease.item.payload.extension));
            if (lower) |first| if (!std.mem.eql(u8, page.entries[0].key, &first)) return error.InvalidNativeLakeFileState;
            if (upper) |last| if (std.mem.order(u8, page.entries[page.entries.len - 1].key, &last) != .lt) return error.InvalidNativeLakeFileState;
            var lo: usize = 0;
            var hi = page.entries.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.order(u8, page.entries[mid].key, id) != .gt) lo = mid + 1 else hi = mid;
            }
            if (lo == 0) return null;
            const entry = page.entries[lo - 1];
            if (ref.height == 0) return if (std.mem.eql(u8, entry.key, id)) try a.dupe(u8, entry.value) else null;
            ref = entry.child orelse return error.InvalidNativeLakeFileState;
            lower = entry.key[0..32].*;
            if (lo < page.entries.len) upper = page.entries[lo].key[0..32].*;
        }
    }
    pub fn file(self: *@This(), a: A, hex: []const u8, public: bool) !?[]const u8 {
        try self.lock();
        defer self.mutex.unlock();
        try self.cached.context.ensureActive();
        try self.token.check();
        const map = if (public) &self.public_memo else &self.private_memo;
        if (map.get(hex)) |found| return found;
        if (hex.len != 64) return error.ExternalLakeSnapshotMismatch;
        var id: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, hex) catch return error.ExternalLakeSnapshotMismatch;
        var scratch_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(scratch_budget.allocator());
        defer scratch.deinit();
        const found = (try self.point(scratch.allocator(), if (public) self.root.public else self.root.private, &id)) orelse return null;
        return self.remember(map, hex, found) orelse try a.dupe(u8, found);
    }
    /// A selected file entry owns every field independently of decoded-page
    /// residency. This permits sparse source opening after cold process restart.
    pub fn inventoryFile(self: *@This(), a: A, id: []const u8) !local.serverless_external_source_types.FileEntry {
        return self.inventoryFileBounded(a, id, false);
    }
    pub fn inventoryFileForScan(self: *@This(), a: A, id: []const u8) !local.serverless_external_source_types.FileEntry {
        return self.inventoryFileBounded(a, id, true);
    }
    fn inventoryFileBounded(self: *@This(), a: A, id: []const u8, scan: bool) !local.serverless_external_source_types.FileEntry {
        try self.lock();
        defer self.mutex.unlock();
        const previous_reads = self.reads;
        defer if (scan) {
            self.reads = previous_reads;
        };
        if (scan) self.reads = 8 * 1024 * 1024;
        try self.cached.context.ensureActive();
        try self.token.check();
        var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(memory.allocator());
        defer scratch.deinit();
        const bytes = (try self.point(scratch.allocator(), self.root.files, &key(id))) orelse return error.ExternalLakeSnapshotMismatch;
        const record = try std.json.parseFromSliceLeaky(Record, scratch.allocator(), bytes, .{});
        if (!std.mem.eql(u8, record.file.file_id, id)) return error.ExternalLakeSnapshotMismatch;
        try record.file.validate();
        // The binary codec transfers ordinary FileEntry ownership and cleans up
        // partially decoded fields on allocation failure. Leaky JSON decoding
        // into the request allocator would strand allocations under pressure.
        const codec = local.serverless_external_source_codec;
        const encoded = try codec.encodeAlloc(scratch.allocator(), .{
            .format = .iceberg,
            .source_id = @constCast(self.root.source),
            .source_uri = @constCast("published-navigation"),
            .snapshot_id = @constCast(self.root.snapshot),
            .schema_fingerprint = @constCast(self.root.schema),
            .files = @constCast(&[_]local.serverless_external_source_types.FileEntry{record.file}),
        });
        var inventory = try codec.decodeAlloc(a, encoded);
        defer inventory.deinit(a);
        const file_ = inventory.files[0];
        a.free(inventory.files);
        inventory.files = &.{};
        return file_;
    }
    /// Return only the exact selectors needed by a reached Parquet file.
    /// The caller owns the strings; all other source metadata remains in its
    /// fresh immutable manifest plan.
    pub fn deleteSelector(self: *@This(), a: A, index: usize, file_: local.serverless_query_lake_iceberg_snapshot.IcebergDeleteFile, context: local.serverless_query_lake_read_context.Context) !local.serverless_query_lake_serving.DeleteQualification.Version {
        try context.ensureActive();
        try self.lock();
        defer self.mutex.unlock();
        const proof = self.root.delete_proof orelse return error.InvalidNativeLakeFileState;
        if (index >= proof.count) return error.InvalidNativeLakeFileState;
        const previous_reads = self.reads;
        defer self.reads = previous_reads;
        self.reads = 8 * 1024 * 1024;
        var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(memory.allocator());
        defer scratch.deinit();
        var key_: [32]u8 = @splat(0);
        std.mem.writeInt(u32, key_[28..32], @intCast(index), .big);
        const bytes = (try self.point(scratch.allocator(), proof.versions, &key_)) orelse return error.InvalidNativeLakeFileState;
        try context.ensureActive();
        return ownDeleteVersion(a, bytes, file_);
    }
    pub fn selectors(self: *@This(), a: A, file_: local.serverless_external_source_types.FileEntry) !local.serverless_query_lake_serving.ServingSource.FileVersions {
        try self.lock();
        defer self.mutex.unlock();
        try self.cached.context.ensureActive();
        try self.token.check();
        var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(memory.allocator());
        defer scratch.deinit();
        const sa = scratch.allocator();
        const bytes = (try self.point(sa, self.root.files, &key(file_.file_id))) orelse return error.ExternalLakeSnapshotMismatch;
        const record = try std.json.parseFromSliceLeaky(Record, sa, bytes, .{});
        if (!std.mem.eql(u8, record.file.file_id, file_.file_id) or !std.mem.eql(u8, record.file.object_uri, file_.object_uri) or record.file.byte_len != file_.byte_len) return error.ExternalLakeSnapshotMismatch;
        const coverage = @import("lake_index_coverage.zig");
        if (!coverage.versionIsStrong(record.file.etag, record.file.version_id)) return error.InvalidExternalLakeIndexCoverage;
        if (coverage.versionIsStrong(file_.etag, file_.version_id) and (!std.mem.eql(u8, file_.etag, record.file.etag) or !std.mem.eql(u8, file_.version_id, record.file.version_id))) return error.ExternalLakeIndexSourceChanged;
        const etag = try a.dupe(u8, record.file.etag);
        errdefer a.free(etag);
        return .{ .etag = etag, .version_id = try a.dupe(u8, record.file.version_id) };
    }
    pub fn digest(self: *@This(), a: A, file_id: []const u8) ![]const u8 {
        try self.lock();
        defer self.mutex.unlock();
        try self.cached.context.ensureActive();
        try self.token.check();
        if (self.digest_memo.get(file_id)) |found| return found;
        var scratch_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 4 * 1024 * 1024 };
        var scratch = std.heap.ArenaAllocator.init(scratch_budget.allocator());
        defer scratch.deinit();
        const sa = scratch.allocator();
        const bytes = (try self.point(sa, self.root.files, &key(file_id))) orelse return error.ExternalLakeSnapshotMismatch;
        const record = try std.json.parseFromSliceLeaky(Record, sa, bytes, .{});
        if (!std.mem.eql(u8, record.file.file_id, file_id)) return error.ExternalLakeSnapshotMismatch;
        const hex = std.fmt.bytesToHex(record.digest, .lower);
        return self.remember(&self.digest_memo, file_id, &hex) orelse try a.dupe(u8, &hex);
    }
};
pub fn walk(a: A, store: *stores.ArtifactStore, ref: Ref, token: Token, visitor: anytype) !void {
    const root = try load(a, store.*, ref, token, null);
    var reads: u64 = tree.max_page_bytes;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = root.domain, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    for ([_]?tree.Ref{ root.files, root.public, root.private }) |r| if (r) |value| try tree.walkPostOrder(a, pages.store(), value, visitor, false);
}

test "external lake bounded WAL paged source navigation resolves only reached identities and reconciles per-file deletes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("paged-source-navigation");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const entries = try ca.alloc(local.serverless_external_source_types.FileEntry, 256);
    for (entries, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = try std.fmt.allocPrint(ca, "file:///source-{d}.parquet", .{i}), .etag = @constCast("exact"), .byte_len = 128, .row_count = 0, .row_groups = &.{} };
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.inventory = .{ .format = .iceberg, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("before"), .schema_fingerprint = @constCast("schema"), .files = entries };
    source.verified_files = null;
    source.prepared_deletes = null;
    source.scanner.iceberg_delete_plan = null;
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    source.immutable_objects = true;
    source.plan_identity = @splat(8);
    const first = try buildProven(a, ca, &store, &provider, null, .none, @splat(9));
    const old = try load(ca, store, first.ref, .none, null);
    try std.testing.expectEqualDeep(@as([32]u8, @splat(8)), old.coverage.?.plan.?);
    try std.testing.expectEqualDeep(@as([32]u8, @splat(9)), old.coverage.?.data);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(3), .context = .{} };
    var resolver: Resolver = .{ .root = old, .store = store, .cached = cached, .token = .none };
    defer resolver.deinit();
    {
        var cold = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cold.deinit();
        var walking: Resolver = .{ .root = old, .store = store, .cached = .{ .cache = &cold, .scope = @splat(5), .context = .{} }, .token = .none, .reads = 0 };
        defer walking.deinit();
        var entry = try walking.inventoryFileForScan(a, "file-255");
        defer entry.deinit(a);
        try std.testing.expectEqualStrings("file-255", entry.file_id);
        try std.testing.expectEqual(@as(u64, 0), walking.reads);
    }
    const Ownership = struct {
        fn check(alloc: A, reader: *Resolver) !void {
            var file_ = try reader.inventoryFile(alloc, "file-255");
            defer file_.deinit(alloc);
            try std.testing.expectEqualStrings("file-255", file_.file_id);
            // Every field is owned independently of the immutable decoded page.
            file_.file_id[0] = 'X';
            try std.testing.expectEqualStrings("exact", file_.etag);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Ownership.check, .{&resolver});
    var owned_file = try resolver.inventoryFile(a, "file-255");
    defer owned_file.deinit(a);
    try std.testing.expectEqualStrings("file-255", owned_file.file_id);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, resolver.inventoryFile(a, "absent"));
    const versions = try resolver.selectors(a, entries[255]);
    defer a.free(versions.etag);
    defer a.free(versions.version_id);
    try std.testing.expectEqualStrings("exact", versions.etag);
    var wrong_file = entries[255];
    wrong_file.byte_len += 1;
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, resolver.selectors(a, wrong_file));
    wrong_file = entries[255];
    wrong_file.etag = @constCast("changed");
    try std.testing.expectError(error.ExternalLakeIndexSourceChanged, resolver.selectors(a, wrong_file));
    const public_id = local.storage_rowsource_identity.fileDigest("source", "before", "file-255");
    const public_hex = std.fmt.bytesToHex(public_id, .lower);
    try std.testing.expectEqualStrings("file-255", (try resolver.file(ca, &public_hex, true)).?);
    const private_hex = try resolver.digest(ca, "file-255");
    try std.testing.expectEqualStrings("file-255", (try resolver.file(ca, private_hex, false)).?);
    // One query resolves one file, not the inventory. Repeated hits perform no
    // more metadata I/O; cache ownership stays independent of result arenas.
    const read_bytes = resolver.reads;
    try std.testing.expectEqualStrings("file-255", (try resolver.file(ca, &public_hex, true)).?);
    try std.testing.expectEqual(read_bytes, resolver.reads);
    try std.testing.expectEqual(@as(u32, 1), resolver.public_memo.count());
    var peer: Resolver = .{ .root = old, .store = store, .cached = cached, .token = .none };
    defer peer.deinit();
    const allowance = peer.reads;
    try std.testing.expectEqualStrings("file-255", (try peer.file(ca, &public_hex, true)).?);
    try std.testing.expectEqual(allowance, peer.reads);
    try std.testing.expect(resolver.memo_budget.peak < 64 * 1024);
    var prepared: local.serverless_query_lake_prepared_deletes.Prepared = .{ .arena = .init(a), .delete_files = &.{}, .equality = &.{}, .columns = &.{} };
    defer prepared.arena.deinit();
    for (entries, 0..) |file, i| try prepared.files.put(prepared.arena.allocator(), file.file_id, .{ .index = i, .fingerprint = if (i == 0) @splat(9) else local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint() });
    source.prepared_deletes = &prepared;
    source.inventory.snapshot_id = @constCast("after");
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 2, std.testing.io);
    const next = try build(a, ca, &store, &provider, old, .none);
    try std.testing.expectEqual(@as(usize, 1), next.delta.prior_changed.len);
    try std.testing.expect(next.stats.prior_records < entries.len / 2);
    try std.testing.expect(next.stats.skipped_subtrees > 0);
    for (next.delta.changed, 0..) |changed, i| try std.testing.expectEqual(i == 0, changed);
    const root = try load(ca, store, next.ref, .none, null);
    var newer: Resolver = .{ .root = root, .store = store, .cached = cached, .token = .none };
    defer newer.deinit();
    const old_digest = try resolver.digest(ca, "file-0");
    const new_digest = try newer.digest(ca, "file-0");
    try std.testing.expect(!std.mem.eql(u8, old_digest, new_digest));
    try std.testing.expect(try newer.file(ca, old_digest, false) == null);
    try std.testing.expectEqualStrings("file-0", (try resolver.file(ca, old_digest, false)).?);
    try std.testing.expect(try newer.file(ca, &public_hex, true) == null);
    provider.native_delta = next.delta;
    const exact = try native.identity(ca, &provider, entries[0]);
    try std.testing.expectEqualStrings(new_digest, &std.fmt.bytesToHex(exact, .lower));
    // Unchanged delete-bearing refreshes read no prior file records/pages.
    provider.native_delta = null;
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 3, std.testing.io);
    const unchanged = try build(a, ca, &store, &provider, root, .none);
    try std.testing.expectEqual(@as(usize, 0), unchanged.stats.prior_pages);
    try std.testing.expectEqual(@as(usize, 0), unchanged.stats.prior_records);
    try std.testing.expectEqual(@as(usize, 0), unchanged.delta.prior_changed.len);
    // Wrong authority cannot turn a cached page into a capability.
    newer.cached.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, newer.file(ca, new_digest, false));
    try std.testing.expectError(error.DeadlineExceeded, newer.inventoryFile(a, "file-0"));
}

test "external lake metadata navigation durable delete selectors validate only reached versions and remain GC roots" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var provider_storage = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider.init(a);
    defer provider_storage.deinit();
    var client = provider_storage.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "delete", "bytes", .{});
    put.deinit(a);
    const iceberg = local.serverless_query_lake_iceberg_snapshot;
    var deletes = [_]iceberg.IcebergDeleteFile{.{ .content = .position_deletes, .file_path = @constCast("s3://bucket/delete"), .file_format = @constCast("PARQUET"), .snapshot_id = 1, .data_sequence_number = 2, .file_sequence_number = 3, .record_count = 1, .file_size_in_bytes = 5 }};
    const inventory: Inventory = .{ .format = .iceberg, .source_id = @constCast("source"), .source_uri = @constCast("s3://bucket/table"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = &.{} };
    const Pin = struct {
        fn check(_: *anyopaque) !void {}
        fn close(_: *anyopaque) void {}
    };
    var marker: u8 = 0;
    var source: local.serverless_query_lake_serving.ServingSource = .{ .alloc = a, .store = undefined, .inventory = inventory, .scanner = local.serverless_query_lake_serving.PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client), .immutable_objects = true, .snapshot_pin = .{ .ptr = &marker, .check = Pin.check, .deinit = Pin.close } };
    source.scanner.iceberg_delete_plan = .{ .files = &deletes };
    var provider: Provider = .{ .source = &source, .context = .{} };
    var directory = try local.common_test_directory.TestDirectory.init("delete-version-proof");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@import("lake_index_publication.zig").uploadDomainWithNamespace(1, @splat(7), null), 1, std.testing.io);
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: pages_mod.PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const proof = (try publishDeleteProof(a, pages.store(), &provider, @splat(9))).?;
    const root: Root = .{ .domain = store.upload_scope.?.domain, .source = "source", .schema = "schema", .snapshot = "snapshot", .count = 0, .coverage = .{ .plan = @splat(8), .data = @splat(9) }, .delete_proof = proof };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    var resolver: Resolver = .{ .root = root, .store = store, .cached = .{ .cache = &cache, .scope = @splat(3), .context = .{} }, .token = .none };
    defer resolver.deinit();
    for (0..2) |_| {
        const selector = try resolver.deleteSelector(a, 0, deletes[0], .{});
        a.free(selector.etag);
        a.free(selector.version);
    }
    try std.testing.expect(cache.decoded.hits > 0);
    const before = provider_storage.stats;
    const version = try deleteVersion(a, store, root, 0, deletes[0], .none, .{});
    defer {
        a.free(version.etag);
        a.free(version.version);
    }
    try std.testing.expect(exactVersion(version.version));
    try std.testing.expectEqual(before, provider_storage.stats);
    var changed = deletes[0];
    changed.file_size_in_bytes += 1;
    try std.testing.expectError(error.InvalidNativeLakeFileState, deleteVersion(a, store, root, 0, changed, .none, .{}));
    try std.testing.expectError(error.DeadlineExceeded, deleteVersion(a, store, root, 0, deletes[0], .none, .{ .deadline_ns = 0 }));
    const root_bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
    var root_ref = try store.putWithCancellation(root_bytes, .none);
    defer root_ref.deinit(a);
    var inventory_ref = try store.putWithCancellation("inventory", .none);
    defer inventory_ref.deinit(a);
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 1, .authority = undefined, .store = store, .identity = @splat(7), .context = .{}, .token = @splat(1), .audit_only = true, .remaining_reads = 16 * 1024 * 1024 };
    defer {
        var keys = collector.marked.keyIterator();
        while (keys.next()) |key_| a.free(key_.*);
        collector.marked.deinit(a);
    }
    const publication: local.metadata_lake_index_catalog.Publication = .{ .reader_protocol = local.metadata_lake_index_catalog.native_reader_protocol, .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = proof.source, .credentials = @splat(2), .store = @splat(7) }, .published_at_ms = 1, .base_source = .{ .external_iceberg = .{ .format = .iceberg, .source_uri = "s3://bucket/metadata.json", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory_ref.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory_ref.artifact_id, .checksum = inventory_ref.checksum, .byte_len = inventory_ref.byte_len }, .source_navigation = .{ .kind = .doc_values, .metadata_version = 2, .artifact_id = root_ref.artifact_id, .checksum = root_ref.checksum, .byte_len = root_ref.byte_len } };
    try collector.markPublication(publication);
    try std.testing.expect(collector.marked.count() >= 3);
    source.immutable_objects = false;
    try std.testing.expect((try publishDeleteProof(a, pages.store(), &provider, @splat(9))) == null);
}
