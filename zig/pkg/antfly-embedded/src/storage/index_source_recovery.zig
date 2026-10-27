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

//! Bounded recovery for the primary/index ownership handoff. The primary
//! journal and claims commit together, then a physical barrier precedes index
//! publication. A missing index receipt is cancelled under the index owner lock
//! before any claim is released. Recovery never reads mutable primary bodies.
const std = @import("std");
const claims = @import("index_source_claim.zig");
const publication = claims.publication;
const maps = @import("index_source_map.zig");
const A = std.mem.Allocator;
const segment_source = @import("../segment_source.zig");
const Digest = publication.Digest;
pub const prefix = "\x00\x00__metadata__:index_source_job_v1:";
pub const Key = [prefix.len + 64]u8;
const record_bytes = 4 + 1 + 32 + 32 + 4 + 4 + 1 + 32 + 32;
pub const Phase = enum(u8) { prepared = 0, dormant = 1, cancelling = 2, retiring = 3, map_reclaim = 4 };
pub const Job = struct {
    binding: publication.Binding,
    root: Digest,
    segment: Digest,
    documents: u32,
    processed: u32 = 0,
    after: ?Digest = null,
    phase: Phase = .prepared,
    role: publication.Role = .segment,
    /// Acknowledgment advances the journal without changing its preparation.
    /// Retry may observe that progress, but never cancellation or retirement.
    pub fn requirePreparation(self: Job, expected: Job) !void {
        if ((self.phase != .prepared and self.phase != .dormant) or self.role != expected.role or self.documents != expected.documents or
            !std.meta.eql(self.binding, expected.binding) or !std.mem.eql(u8, &self.root, &expected.root) or !std.mem.eql(u8, &self.segment, &expected.segment)) return error.IndexPublicationConflict;
    }
};
fn key(binding: publication.Binding) Key {
    var out: Key = undefined;
    @memcpy(out[0..prefix.len], prefix);
    @memcpy(out[prefix.len..][0..32], &binding.owner);
    @memcpy(out[prefix.len + 32 ..], &binding.publication);
    return out;
}
fn checksum(binding: publication.Binding, bytes: []const u8) Digest {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update(&key(binding));
    h.update(bytes);
    return h.finalResult();
}
fn encode(job: Job) [record_bytes]u8 {
    var raw: [record_bytes]u8 = @splat(0);
    @memcpy(raw[0..4], if (job.role == .source_leaf) "ISJ2" else "ISJ1");
    raw[4] = @backingInt(job.phase);
    @memcpy(raw[5..37], &job.root);
    @memcpy(raw[37..69], &job.segment);
    std.mem.writeInt(u32, raw[69..73], job.documents, .little);
    std.mem.writeInt(u32, raw[73..77], job.processed, .little);
    if (job.after) |after| {
        raw[77] = 1;
        @memcpy(raw[78..110], &after);
    }
    @memcpy(raw[110..], &checksum(job.binding, raw[0..110]));
    return raw;
}
pub fn load(reader: anytype, binding: publication.Binding) !?Job {
    const raw = reader.getPhysical(&key(binding)) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != record_bytes or (!std.mem.eql(u8, raw[0..4], "ISJ1") and !std.mem.eql(u8, raw[0..4], "ISJ2")) or raw[4] > @backingInt(Phase.map_reclaim) or raw[77] > 1 or
        !std.mem.eql(u8, raw[110..], &checksum(binding, raw[0..110]))) return error.IndexSourceJobCorrupt;
    const result: Job = .{ .binding = binding, .phase = @fromBackingInt(@intCast(raw[4])), .root = raw[5..37].*, .segment = raw[37..69].*, .documents = std.mem.readInt(u32, raw[69..73], .little), .processed = std.mem.readInt(u32, raw[73..77], .little), .after = if (raw[77] == 0) null else raw[78..110].*, .role = if (std.mem.eql(u8, raw[0..4], "ISJ2")) .source_leaf else .segment };
    if (result.documents == 0 or result.documents > maps.maximum_documents or (result.role == .source_leaf and result.documents > maps.page_capacity) or result.processed > result.documents or
        (result.phase != .dormant and result.phase != .map_reclaim and ((result.processed == 0) != (result.after == null))) or ((result.phase == .dormant or result.phase == .map_reclaim) and (result.after != null or result.processed != result.documents))) return error.IndexSourceJobCorrupt;
    return result;
}
fn save(writer: anytype, job: Job) !void {
    try writer.put(&key(job.binding), &encode(job));
}
fn forget(writer: anytype, job: Job) !void {
    try writer.delete(&key(job.binding));
    try claims.changeAuxiliaryMetadata(writer, 0, @sizeOf(Key) + record_bytes, .{ .metadata_bytes = std.math.maxInt(u64), .value_bytes = std.math.maxInt(u64), .claims = std.math.maxInt(u64) });
}
pub const Input = struct {
    physical: []const u8 = "",
    root: Digest,
    from: union(enum) {
        current: u64,
        observed: *const claims.ObservedSource,
        retained: struct { namespace: [24]u8, sequence: u64 },
        published: struct { reference: claims.Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease },
    },
};
pub const Prepared = struct {
    job: Job,
    references: []claims.Reference,
    publisher: publication.SharedReaders.PublisherRegistration,
    source_stamp: ?segment_source.ContentStamp = null,
    pub fn deinit(self: *Prepared, a: A) void {
        self.publisher.deinit();
        a.free(self.references);
        self.* = undefined;
    }
};
/// Copy only surviving source coordinates through authenticated native reads.
/// A map window with no output rows needs no source lease check or metadata I/O.
/// The captured owner and all output input borrows remain live through prepare.
pub fn assignMappedInputs(index: anytype, captured: anytype, map: anytype, input_index: usize, outputs: []const []Input) !void {
    const base: u32 = if (@hasField(@TypeOf(captured.*), "first_document")) captured.first_document else 0;
    var first: u32 = 0;
    while (first < captured.view.documents) {
        var references: [maps.page_capacity]claims.Reference = undefined;
        var targets: [maps.page_capacity]?@import("../segment.zig").MergeSourceMap.Location = undefined;
        const count = @min(references.len, captured.view.documents - first);
        if (try map.lookupWindow(input_index, try std.math.add(u32, base, first), targets[0..count]) != 0) {
            for (targets[0..count]) |target| if (target) |position| {
                if (position.segment >= outputs.len or position.doc >= outputs[position.segment].len) return error.InvalidIndexSourceMap;
            };
            try captured.readWindow(index, first, references[0..count]);
            for (targets[0..count], references[0..count]) |target, reference| if (target) |position| {
                const slot = &outputs[position.segment][position.doc];
                if (slot.from != .current or slot.from.current != 0) return error.InvalidIndexSourceMap;
                slot.* = .{ .root = reference.root, .from = .{ .published = .{ .reference = reference, .decision = captured.decision, .lease = &captured.lease.inner } } };
            };
        }
        first += @intCast(count);
    }
}

/// Reject impossible cardinalities before hashing outputs, allocating corpus
/// coordinates or reserving a durable publication. This is only a lower bound;
/// the primary transaction still enforces current aggregate ownership budgets.
fn validateDocumentCount(count: usize, limits: claims.Limits) !void {
    if (count == 0 or count > maps.maximum_documents) return error.InvalidIndexSourceMap;
    if (count > limits.claims) return error.IndexSourceClaimsFull;
}

fn validateFreshInput(input: Input) !void {
    const keys = @import("internal_keys.zig");
    if (input.from == .published) {
        const source = input.from.published;
        if (!std.mem.eql(u8, &source.reference.root, &input.root)) return error.PreparedReadSetChanged;
        if (input.physical.len != 0) {
            if (!keys.isPrimaryDocumentKey(input.physical)) return error.Unsupported;
            var physical_digest: Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(input.physical, &physical_digest, .{});
            if (!std.mem.eql(u8, &physical_digest, &source.reference.document)) return error.PreparedReadSetChanged;
        }
    } else if (input.from == .observed) {
        const source = input.from.observed;
        if (!std.mem.eql(u8, &source.root, &input.root) or
            !std.mem.eql(u8, source.physical, input.physical)) return error.PreparedReadSetChanged;
    } else if (!keys.isPrimaryDocumentKey(input.physical)) return error.Unsupported;
}
fn prepareInput(a: A, writer: anytype, binding: publication.Binding, input: Input, limits: claims.Limits, comptime unique: bool) !claims.Reference {
    return switch (input.from) {
        .observed => |source| try claims.prepareObserved(a, writer, binding, source, limits, unique),
        .current => |revision| if (unique) try claims.prepareCurrentUnique(a, writer, binding, input.physical, revision, input.root, limits) else try claims.prepareCurrent(a, writer, binding, input.physical, revision, input.root, limits),
        .retained => |retained| if (unique) try claims.prepareRetainedUnique(a, writer, binding, retained.namespace, retained.sequence, input.physical, input.root, limits) else try claims.prepareRetained(a, writer, binding, retained.namespace, retained.sequence, input.physical, input.root, limits),
        .published => |source| if (unique) try claims.forkPublishedUnique(a, writer, source.reference, source.decision, source.lease, binding, limits) else try claims.forkPublished(a, writer, source.reference, source.decision, source.lease, binding, limits),
    };
}

/// Prepare one atomic replacement's source roots in one primary transaction
/// and one durability barrier. Each output retains its own birth, journal and
/// bounded receipt. Working state belongs to the caller's admitted allocator;
/// no fixed output-count limit or corpus-sized uniqueness table is needed.
pub fn prepareFreshSegments(a: A, primary: anytype, index: anytype, owner: Digest, inputs: anytype, segments: anytype, limits: claims.Limits, io: std.Io) ![]Prepared {
    if (inputs.len == 0 or inputs.len != segments.len) return error.InvalidIndexSourceMap;
    var total: u64 = 0;
    for (inputs) |rows| {
        try validateDocumentCount(rows.len, limits);
        total = std.math.add(u64, total, rows.len) catch return error.IndexSourceClaimsFull;
        if (total > limits.claims) return error.IndexSourceClaimsFull;
    }
    try index.reserveSourceOwner(owner);
    const prepared = try a.alloc(Prepared, segments.len);
    var initialized: usize = 0;
    errdefer {
        for (prepared[0..initialized]) |*item| item.deinit(a);
        a.free(prepared);
    }
    // Hash immutable outputs before opening the primary writer. Registrations
    // stay held through the eventual index publication or recovery cancellation.
    for (prepared, segments, inputs) |*item, segment, rows| {
        const stamp = try segment_source.ContentStamp.capture(segment.data.source());
        var fresh = try index.beginNewSourcePublication(io);
        defer fresh.publisher.deinit();
        const refs = try a.alloc(claims.Reference, rows.len);
        item.* = .{
            .job = .{ .binding = fresh.binding, .root = @splat(0), .segment = stamp.digest, .documents = @intCast(rows.len) },
            .references = refs,
            .publisher = fresh.publisher.take(),
            .source_stamp = stamp,
        };
        initialized += 1;
    }
    {
        var writer = try primary.beginWriteTxn();
        errdefer writer.abort();
        var admission = claims.Preparation(@TypeOf(&writer)).init(&writer);
        for (prepared, inputs) |*item, rows| {
            if (try load(&writer, item.job.binding) != null) return error.IndexSourcePublicationPending;
            for (rows, item.references) |input, *reference| {
                try validateFreshInput(input);
                reference.* = try prepareInput(a, &admission, item.job.binding, input, limits, true);
            }
            const plan = try maps.Plan.prepare(a, item.job.binding, item.job.segment, item.references);
            defer plan.deinit(a);
            item.job.root = plan.root();
            try claims.changeAuxiliaryMetadata(&writer, @sizeOf(Key) + record_bytes, 0, limits);
            try save(&writer, item.job);
        }
        try writer.commit();
    }
    index.noteSourceRecoveryWork();
    try primary.sync(true);
    return prepared;
}
/// A caller retains this preparation until publication or cancellation. If it
/// crashes before publication, recovery fences the abandoned publication and
/// the replay owner may rebuild under a fresh globally unique publication ID.
/// Fixed-cohort source ownership. The identity is a native family layout/range
/// identity, not an asserted SHA of a separately materialized segment. The
/// family publisher verifies ordinal membership and the held physical layout.
pub fn prepareSourceLeaves(a: A, primary: anytype, index: anytype, owner: Digest, cohorts: anytype, identities: []const Digest, limits: claims.Limits, io: std.Io) ![]Prepared {
    return prepareSourceLeavesWithMinimum(28, a, primary, index, owner, cohorts, identities, limits, io);
}

pub fn prepareFieldSourceLeaves(a: A, primary: anytype, index: anytype, owner: Digest, cohorts: anytype, identities: []const Digest, limits: claims.Limits, io: std.Io) ![]Prepared {
    return prepareSourceLeavesWithMinimum(29, a, primary, index, owner, cohorts, identities, limits, io);
}

fn prepareSourceLeavesWithMinimum(comptime minimum: u32, a: A, primary: anytype, index: anytype, owner: Digest, cohorts: anytype, identities: []const Digest, limits: claims.Limits, io: std.Io) ![]Prepared {
    if (@import("schema.zig").maximum_reader_format_version < minimum) return error.UnsupportedVersion;
    if (cohorts.len == 0 or cohorts.len != identities.len) return error.InvalidIndexSourceMap;
    var total: u64 = 0;
    for (cohorts) |rows| {
        try validateDocumentCount(rows.len, limits);
        if (rows.len > maps.page_capacity) return error.InvalidIndexSourceMap;
        total = try std.math.add(u64, total, rows.len);
        if (total > limits.claims) return error.IndexSourceClaimsFull;
    }
    try index.reserveSourceOwner(owner);
    if (minimum >= 29) try index.admitSourceFields() else try index.admitSourceFamilies();
    const result = try a.alloc(Prepared, cohorts.len);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |*item| item.deinit(a);
        a.free(result);
    }
    for (cohorts, identities, result) |rows, identity, *item| {
        var birth = try index.beginNewSourcePublication(io);
        defer birth.publisher.deinit();
        const refs = try a.alloc(claims.Reference, rows.len);
        item.* = .{ .job = .{ .binding = birth.binding, .root = @splat(0), .segment = identity, .documents = @intCast(rows.len), .role = .source_leaf }, .references = refs, .publisher = birth.publisher.take() };
        initialized += 1;
    }
    {
        var writer = try primary.beginWriteTxn();
        errdefer writer.abort();
        var admission = claims.PreparationWithMinimum(@TypeOf(&writer), minimum).init(&writer);
        try admission.ensureIndexSourceAdmission(a);
        for (cohorts, result) |rows, *item| {
            if (try load(&writer, item.job.binding) != null) return error.IndexSourcePublicationPending;
            for (rows, item.references) |input, *ref| {
                try validateFreshInput(input);
                ref.* = try prepareInput(a, &admission, item.job.binding, input, limits, true);
            }
            const plan = try maps.Plan.prepare(a, item.job.binding, item.job.segment, item.references);
            defer plan.deinit(a);
            item.job.root = plan.root();
            try claims.changeAuxiliaryMetadata(&writer, @sizeOf(Key) + record_bytes, 0, limits);
            try save(&writer, item.job);
        }
        try writer.commit();
    }
    index.noteSourceRecoveryWork();
    try primary.sync(true);
    return result;
}

/// Import a source family in bounded cohorts from actual donor read sessions.
/// The producer owns the donor cut and returns a batch whose source leases
/// remain live until batch.deinit. Sessions decode one row at a time. Each journal commits with its claims, and
/// one final physical barrier precedes returning any publishable preparation.
/// Donor reader sessions, not receiver current rows, supply immutable values.
pub fn prepareImportedSourceLeaves(a: A, primary: anytype, index: anytype, owner: Digest, producer: anytype, documents: u32, identities: []const Digest, limits: claims.Limits, io: std.Io) ![]Prepared {
    return prepareImportedSourceLeavesWithMinimum(28, a, primary, index, owner, producer, documents, identities, limits, io);
}
pub fn prepareImportedFieldSourceLeaves(a: A, primary: anytype, index: anytype, owner: Digest, producer: anytype, documents: u32, identities: []const Digest, limits: claims.Limits, io: std.Io) ![]Prepared {
    return prepareImportedSourceLeavesWithMinimum(29, a, primary, index, owner, producer, documents, identities, limits, io);
}
fn prepareImportedSourceLeavesWithMinimum(comptime minimum: u32, a: A, primary: anytype, index: anytype, owner: Digest, producer: anytype, documents: u32, identities: []const Digest, limits: claims.Limits, io: std.Io) ![]Prepared {
    if (@import("schema.zig").maximum_reader_format_version < minimum) return error.UnsupportedVersion;
    try validateDocumentCount(documents, limits);
    const count = (documents - 1) / maps.page_capacity + 1;
    if (identities.len != count) return error.InvalidIndexSourceMap;
    try index.reserveSourceOwner(owner);
    if (minimum == 29) try index.admitSourceFields() else try index.admitSourceFamilies();
    const result = try a.alloc(Prepared, count);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |*item| item.deinit(a);
        a.free(result);
    }
    for (result, identities, 0..) |*item, identity, ordinal| {
        const first: u32 = @intCast(ordinal * maps.page_capacity);
        const rows = @min(maps.page_capacity, documents - first);
        var batch = try producer.capture(a, first, rows);
        defer batch.deinit(a);
        if (batch.references.len != rows) return error.InvalidIndexSourceMap;
        var birth = try index.beginNewSourcePublication(io);
        defer birth.publisher.deinit();
        const references = try a.alloc(claims.Reference, rows);
        item.* = .{ .job = .{ .binding = birth.binding, .root = @splat(0), .segment = identity, .documents = rows, .role = .source_leaf }, .references = references, .publisher = birth.publisher.take() };
        initialized += 1;
        var writer = try primary.beginWriteTxn();
        errdefer writer.abort();
        if (try load(&writer, item.job.binding) != null) return error.IndexSourcePublicationPending;
        var admission = claims.PreparationWithMinimum(@TypeOf(&writer), minimum).init(&writer);
        for (references, 0..) |*reference, row| {
            // Decode/import one recipe at a time. A cohort of large inline
            // sources must not retain 64 decoded document bodies in scratch.
            var session = try batch.open(a, row);
            defer session.deinit();
            reference.* = try session.importClaimUnique(a, &admission, item.job.binding, limits);
        }
        const plan = try maps.Plan.prepare(a, item.job.binding, identity, references);
        defer plan.deinit(a);
        item.job.root = plan.root();
        try claims.changeAuxiliaryMetadata(&writer, @sizeOf(Key) + record_bytes, 0, limits);
        try save(&writer, item.job);
        try writer.commit();
        // Mark debt after each committed cohort, including an error later in
        // this import. Native publisher registrations fence recovery meanwhile.
        index.noteSourceRecoveryWork();
    }
    try primary.sync(true);
    return result;
}

pub fn prepare(a: A, primary: anytype, index: anytype, binding: publication.Binding, inputs: []const Input, segment: []const u8, limits: claims.Limits) !Prepared {
    try validateDocumentCount(inputs.len, limits);
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(segment, &digest, .{});
    return prepareDigest(a, primary, index, binding, inputs, digest, limits, null);
}
/// The retained immutable source remains owned by its prepared segment. Its
/// content stamp avoids hashing/materializing it again during publication.
pub fn prepareSource(a: A, primary: anytype, index: anytype, binding: publication.Binding, inputs: []const Input, source: segment_source.Source, limits: claims.Limits) !Prepared {
    try validateDocumentCount(inputs.len, limits);
    const stamp = try segment_source.ContentStamp.capture(source);
    var prepared = try prepareDigest(a, primary, index, binding, inputs, stamp.digest, limits, null);
    prepared.source_stamp = stamp;
    return prepared;
}
/// Allocate and pin a fresh native birth before primary claims. Hashing the
/// immutable output precedes that pin, so large output hashing does not hold
/// an old publication epoch open. No public caller supplies an epoch proof.
pub fn prepareFreshSource(a: A, primary: anytype, index: anytype, owner: Digest, inputs: []const Input, source: segment_source.Source, limits: claims.Limits, io: std.Io) !Prepared {
    try validateDocumentCount(inputs.len, limits);
    const stamp = try segment_source.ContentStamp.capture(source);
    try index.reserveSourceOwner(owner);
    var fresh = try index.beginNewSourcePublication(io);
    defer fresh.publisher.deinit();
    var prepared = try prepareDigest(a, primary, index, fresh.binding, inputs, stamp.digest, limits, &fresh.publisher);
    prepared.source_stamp = stamp;
    return prepared;
}
pub fn prepareFresh(a: A, primary: anytype, index: anytype, owner: Digest, inputs: []const Input, bytes: []const u8, limits: claims.Limits, io: std.Io) !Prepared {
    try validateDocumentCount(inputs.len, limits);
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    try index.reserveSourceOwner(owner);
    var fresh = try index.beginNewSourcePublication(io);
    defer fresh.publisher.deinit();
    return prepareDigest(a, primary, index, fresh.binding, inputs, digest, limits, &fresh.publisher);
}
/// Cross-primary publication consumes held native source sessions. Claims and
/// missing immutable values commit with the destination journal before the
/// destination index can expose a root. Source sessions and their read cut stay
/// live for this call; no destination current-row lookup supplies source bytes.
pub fn prepareImported(a: A, primary: anytype, index: anytype, owner: Digest, sessions: anytype, segment: []const u8, limits: claims.Limits, io: std.Io) !Prepared {
    try validateDocumentCount(sessions.len, limits);
    var segment_hash: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(segment, &segment_hash, .{});
    return prepareImportedDigest(a, primary, index, owner, sessions, segment_hash, limits, io);
}
pub fn prepareImportedSource(a: A, primary: anytype, index: anytype, owner: Digest, sessions: anytype, source: segment_source.Source, limits: claims.Limits, io: std.Io) !Prepared {
    try validateDocumentCount(sessions.len, limits);
    const stamp = try segment_source.ContentStamp.capture(source);
    var prepared = try prepareImportedDigest(a, primary, index, owner, sessions, stamp.digest, limits, io);
    prepared.source_stamp = stamp;
    return prepared;
}
fn prepareImportedDigest(a: A, primary: anytype, index: anytype, owner: Digest, sessions: anytype, segment_hash: Digest, limits: claims.Limits, io: std.Io) !Prepared {
    try validateDocumentCount(sessions.len, limits);
    try index.reserveSourceOwner(owner);
    var fresh = try index.beginNewSourcePublication(io);
    defer fresh.publisher.deinit();
    const references = try a.alloc(claims.Reference, sessions.len);
    errdefer a.free(references);
    var job: Job = undefined;
    {
        var writer = try primary.beginWriteTxn();
        errdefer writer.abort();
        if (try load(&writer, fresh.binding) != null) return error.IndexSourcePublicationPending;
        var admission = claims.Preparation(@TypeOf(&writer)).init(&writer);
        for (sessions, references) |session, *reference| {
            reference.* = try session.importClaimUnique(a, &admission, fresh.binding, limits);
        }
        const plan = try maps.Plan.prepare(a, fresh.binding, segment_hash, references);
        defer plan.deinit(a);
        job = .{ .binding = fresh.binding, .root = plan.root(), .segment = segment_hash, .documents = @intCast(sessions.len) };
        try claims.changeAuxiliaryMetadata(&writer, @sizeOf(Key) + record_bytes, 0, limits);
        try save(&writer, job);
        try writer.commit();
    }
    index.noteSourceRecoveryWork();
    try primary.sync(true);
    return .{ .job = job, .references = references, .publisher = fresh.publisher.take() };
}

fn prepareDigest(a: A, primary: anytype, index: anytype, binding: publication.Binding, inputs: []const Input, segment_hash: Digest, limits: claims.Limits, supplied: ?*publication.SharedReaders.PublisherRegistration) !Prepared {
    try validateDocumentCount(inputs.len, limits);
    if (!binding.valid()) return error.InvalidIndexPublication;
    var seen: std.AutoHashMapUnmanaged(Digest, void) = .empty;
    defer seen.deinit(a);
    if (supplied == null) for (inputs) |input| {
        var digest: Digest = undefined;
        if (input.from == .published and input.physical.len == 0) {
            digest = input.from.published.reference.document;
        } else {
            if (!@import("internal_keys.zig").isPrimaryDocumentKey(input.physical)) return error.Unsupported;
            std.crypto.hash.sha2.Sha256.hash(input.physical, &digest, .{});
        }
        if (input.from == .published) {
            const source = input.from.published;
            if (!std.mem.eql(u8, &source.reference.document, &digest) or !std.mem.eql(u8, &source.reference.root, &input.root)) return error.PreparedReadSetChanged;
        }
        if ((try seen.getOrPut(a, digest)).found_existing) return error.InvalidIndexSourceMap;
    };
    try index.reserveSourceOwner(binding.owner);
    var publisher = if (supplied) |value| value.take() else try index.beginSourcePublication(binding);
    errdefer publisher.deinit();
    try publisher.require(binding);
    const references = try a.alloc(claims.Reference, inputs.len);
    errdefer a.free(references);
    var job: Job = undefined;
    {
        var writer = try primary.beginWriteTxn();
        errdefer writer.abort();
        if (try load(&writer, binding) != null) return error.IndexSourcePublicationPending;
        var admission = claims.Preparation(@TypeOf(&writer)).init(&writer);
        for (inputs, references) |input, *ref| {
            // Validate fresh input coordinates in the same transaction that
            // creates their claims, avoiding a second corpus-sized hash table.
            if (supplied != null) try validateFreshInput(input);
            ref.* = if (supplied != null) try prepareInput(a, &admission, binding, input, limits, true) else try prepareInput(a, &admission, binding, input, limits, false);
        }
        const plan = try maps.Plan.prepare(a, binding, segment_hash, references);
        defer plan.deinit(a);
        job = .{ .binding = binding, .root = plan.root(), .segment = segment_hash, .documents = @intCast(inputs.len) };
        try claims.changeAuxiliaryMetadata(&writer, @sizeOf(Key) + record_bytes, 0, limits);
        try save(&writer, job);
        try writer.commit();
    }
    // A committed asynchronous transaction alone does not prove that an index
    // in a different store may start exposing its references.
    index.noteSourceRecoveryWork();
    try primary.sync(true);
    return .{ .job = job, .references = references, .publisher = publisher };
}
/// Verify a borrowed payload against its durable primary preparation.
fn requirePrepared(primary: anytype, prepared: *const Prepared, segment: []const u8) !void {
    var actual: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(segment, &actual, .{});
    if (!std.mem.eql(u8, &actual, &prepared.job.segment)) return error.IndexPublicationConflict;
    {
        var reader = try primary.beginReadTxn();
        defer reader.abort();
        const job = (try load(&reader, prepared.job.binding)) orelse return error.IndexSourceClaimMissing;
        if (job.phase != .prepared and job.phase != .dormant) return error.IndexPublicationCancelled;
        if (!std.mem.eql(u8, &job.root, &prepared.job.root) or job.documents != prepared.references.len) return error.IndexPublicationConflict;
    }
    try primary.sync(true);
}
/// Takes segment ownership on every path. A post-commit error leaves the
/// durable journal for recovery; callers must never unconditionally drop claims.
pub fn publishOwned(a: A, primary: anytype, index: anytype, prepared: *const Prepared, segment: []u8) !publication.Decision {
    var owned: ?[]u8 = segment;
    defer if (owned) |bytes| a.free(bytes);
    try requirePrepared(primary, prepared, segment);
    owned = null;
    const decision = try index.indexSegmentOwnedWithSourceMapFromAllocator(a, segment, prepared.job.binding, prepared.references);
    if (!std.mem.eql(u8, &decision.root, &prepared.job.root)) return error.IndexPublicationConflict;
    return decision;
}
/// Compare-and-replace consumes bytes on every path. Null is a stale structural
/// plan; its journal remains pending until its preparation lease is released,
/// when ordinary recovery fences and cancels it. Never release claims on an
/// ambiguous post-commit error. Retried committed replacements are idempotent.
pub fn replaceOwned(a: A, primary: anytype, index: anytype, prepared: *const Prepared, previous: []const u64, segment: []u8) !?publication.Decision {
    var owned: ?[]u8 = segment;
    defer if (owned) |bytes| a.free(bytes);
    try requirePrepared(primary, prepared, segment);
    owned = null;
    const decision = (try index.replaceSegmentsIfActiveOwnedWithSourceMapFromAllocator(a, previous, segment, prepared.job.binding, prepared.references)) orelse return null;
    if (!std.mem.eql(u8, &decision.root, &prepared.job.root)) return error.IndexPublicationConflict;
    return decision;
}
/// Publish multiple streamed outputs as one structural transaction. All source
/// preparations remain live through the call. Prepared segment buffers and their
/// staging array transfer to the index on every path.
pub fn replacePrepared(a: A, primary: anytype, index: anytype, prepared: []const Prepared, previous: []const u64, segments: anytype, deleted: anytype) !bool {
    var owned = true;
    defer if (owned) index.discardPreparedMergeSegments(segments);
    if (prepared.len != segments.len) return error.InvalidIndexSourceMap;
    const roots = try a.alloc(@TypeOf(index.*).SourceRoot, prepared.len);
    defer a.free(roots);
    {
        var read = try primary.beginReadTxn();
        defer read.abort();
        for (prepared, segments, roots) |*source, segment, *root| {
            const stamp = source.source_stamp orelse return error.IndexPublicationConflict;
            try stamp.require(segment.data.source());
            const job = (try load(&read, source.job.binding)) orelse return error.IndexSourceClaimMissing;
            if (job.phase != .prepared and job.phase != .dormant) return error.IndexPublicationCancelled;
            if (!std.mem.eql(u8, &job.root, &source.job.root) or !std.mem.eql(u8, &job.segment, &stamp.digest) or job.documents != source.references.len) return error.IndexPublicationConflict;
            root.* = .{ .binding = source.job.binding, .references = source.references, .stamp = stamp };
        }
    }
    try primary.sync(true);
    owned = false;
    return index.replaceSegmentsIfActiveManyPreparedWithSourceRoots(a, previous, segments, deleted, roots);
}
pub const Progress = struct { worked: bool = false, finished: bool = false, waiting_readers: bool = false, waiting_publisher: bool = false, more: bool = false };
/// One transition visits at most128 fixed claim headers. Reclamation may load
/// one bounded immutable recipe at a time, never source values. Durable cursor
/// changes commit with the corresponding acknowledgment/deletion operations.
pub fn recoverBinding(a: A, primary: anytype, index: anytype, binding: publication.Binding, limit: u32) !Progress {
    if (limit == 0 or limit > 128) return error.InvalidArgument;
    const observed = blk: {
        var reader = try primary.beginReadTxn();
        defer reader.abort();
        break :blk (try load(&reader, binding)) orelse return .{ .finished = true };
    };
    // An idle published job retains ownership and grants no new authority.
    // A current-tip state hint can therefore avoid both a primary writer and
    // an fsync on every dormant publication in a maintenance sweep.
    if (observed.phase == .dormant and try index.sourcePublicationState(binding) == .published) return .{};
    var decision = try index.loadDurableSourcePublication(binding);
    if (decision == null) decision = index.cancelSourcePublicationIfAbandoned(binding) catch |err| switch (err) {
        error.IndexPublicationInFlight => return .{ .waiting_publisher = true },
        error.IndexPublicationAlreadyCommitted => (try index.loadDurableSourcePublication(binding)) orelse return error.IndexPublicationMissing,
        else => return err,
    };
    const accepted = decision.?;
    if (accepted.state != .cancelled) try accepted.requireRole(observed.role);
    if (accepted.state != .cancelled and !std.mem.eql(u8, &accepted.root, &observed.root)) return error.IndexPublicationConflict;
    var retirement: ?@TypeOf(try index.retireSourcePublicationWithReaders(binding)) = null;
    defer if (retirement) |*value| value.deinit();
    if (accepted.state == .retired) {
        retirement = try index.retireSourcePublicationWithReaders(binding);
        if (retirement.?.drained() == null) return .{ .waiting_readers = true };
    }
    if (observed.phase == .map_reclaim) {
        if (retirement == null) return error.IndexSourceJobCorrupt;
        const progress = try index.reclaimSourceMapWindow(&retirement.?, limit);
        if (!progress.done) return .{ .worked = true, .more = true };
        // Do not forget the primary cleanup job before the other store has
        // durably removed its page directory.
        try index.sync(true);
        var writer = try primary.beginWriteTxn();
        var writer_open = true;
        defer if (writer_open) writer.abort();
        const current = (try load(&writer, binding)) orelse return .{ .finished = true };
        if (!std.meta.eql(current, observed)) return .{};
        try forget(&writer, current);
        try writer.commit();
        writer_open = false;
        return .{ .worked = true, .finished = true };
    }
    var writer = try primary.beginWriteTxn();
    var writer_open = true;
    defer if (writer_open) writer.abort();
    var job = (try load(&writer, binding)) orelse return .{ .finished = true };
    if (!std.meta.eql(job, observed)) return .{};
    const phase: Phase = switch (accepted.state) {
        .published => .prepared,
        .cancelled => .cancelling,
        .retired => .retiring,
    };
    if (job.phase == .dormant and accepted.state == .published) return .{};
    if (job.phase != phase) {
        job.phase = phase;
        job.after = null;
        job.processed = 0;
    }
    var page = try claims.addressWindow(a, &writer, binding, job.after, limit);
    defer page.deinit(a);
    var consumed: usize = 0;
    var recipe_bytes: usize = 0;
    for (page.items) |address| {
        if (accepted.state != .published) {
            const bytes = try address.metadataBytes(&writer);
            if (recipe_bytes != 0 and bytes > 16 * 1024 * 1024 - recipe_bytes) break;
            recipe_bytes = try std.math.add(usize, recipe_bytes, bytes);
        }
        switch (accepted.state) {
            .published => try claims.acknowledge(a, &writer, address.headerReference(), accepted),
            .cancelled => {
                if (!try claims.cancel(a, &writer, try address.resolve(a, &writer), accepted)) return error.IndexSourceJobCorrupt;
            },
            .retired => {
                if (!try claims.retire(a, &writer, try address.resolve(a, &writer), accepted, retirement.?.drained().?)) return error.IndexSourceJobCorrupt;
            },
        }
        job.processed = try std.math.add(u32, job.processed, 1);
        if (job.processed > job.documents) return error.IndexSourceJobCorrupt;
        job.after = address.document;
        consumed += 1;
    }
    const done = page.done and consumed == page.items.len;
    if (done) {
        if (job.processed != job.documents) return error.IndexSourceJobCorrupt;
        job.after = null;
        switch (accepted.state) {
            .published => {
                job.phase = .dormant;
                try save(&writer, job);
            },
            .cancelled => try forget(&writer, job),
            .retired => {
                job.phase = .map_reclaim;
                try save(&writer, job);
            },
        }
    } else try save(&writer, job);
    try writer.commit();
    writer_open = false;
    return .{ .worked = true, .finished = done and accepted.state == .cancelled, .more = !done or accepted.state == .retired };
}
/// A single physical-key seek, independent of the number of publications.
/// Call after owner quiescence before deleting the permanent index fence.
pub fn ownerHasJobs(primary: anytype, owner: Digest) !bool {
    var start: [prefix.len + 32]u8 = undefined;
    @memcpy(start[0..prefix.len], prefix);
    @memcpy(start[prefix.len..], &owner);
    var reader = try primary.beginReadTxn();
    defer reader.abort();
    var cursor = try reader.openPhysicalKeyCursorAdapter();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(&start)) orelse return false;
    if (!std.mem.startsWith(u8, row.key, &start)) return false;
    if (row.key.len != @sizeOf(Key)) return error.IndexSourceJobCorrupt;
    return true;
}

/// A maintenance owner keeps this small cursor across bounded turns. Reaching
/// the end closes the current sweep; a later maintenance tick starts another.
/// Dormant publications do not turn run-until-idle into an infinite loop.
pub const Sweep = struct {
    after: ?Key = null,
    pending: ?publication.Binding = null,
    pub const Result = struct { progress: Progress, scan_more: bool };
    pub fn step(self: *Sweep, a: A, primary: anytype, index: anytype, owner: Digest, limit: u32) !Result {
        if (self.pending) |binding| {
            if (!std.mem.eql(u8, &binding.owner, &owner)) {
                self.pending = null;
                self.after = null;
            } else {
                const progress = try recoverBinding(a, primary, index, binding, limit);
                if (!progress.more) self.pending = null;
                return .{ .progress = progress, .scan_more = true };
            }
        }
        var start: [prefix.len + 32]u8 = undefined;
        @memcpy(start[0..prefix.len], prefix);
        @memcpy(start[prefix.len..], &owner);
        const selected: ?publication.Binding = blk: {
            var reader = try primary.beginReadTxn();
            defer reader.abort();
            var cursor = try reader.openPhysicalKeyCursorAdapter();
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(if (self.after) |*after| after else &start);
            if (row != null and self.after != null and std.mem.eql(u8, row.?.key, &self.after.?)) row = try cursor.next();
            const current = row orelse break :blk null;
            if (!std.mem.startsWith(u8, current.key, &start)) break :blk null;
            if (current.key.len != @sizeOf(Key)) return error.IndexSourceJobCorrupt;
            self.after = current.key[0..@sizeOf(Key)].*;
            break :blk .{ .owner = owner, .publication = current.key[prefix.len + 32 ..][0..32].* };
        };
        const binding = selected orelse {
            self.after = null;
            return .{ .progress = .{}, .scan_more = false };
        };
        const progress = try recoverBinding(a, primary, index, binding, limit);
        if (progress.more) self.pending = binding;
        return .{ .progress = progress, .scan_more = true };
    }
};
