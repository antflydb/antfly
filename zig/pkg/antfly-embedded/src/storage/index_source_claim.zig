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

//! Exact source afterimages owned by index publications. Shared immutable value
//! sets keep body ownership unchanged across metadata-only revisions. Every
//! mutation runs in the primary transaction and poisons it on any failure.
//! Publication decisions belong to the index transaction, never this store.
const std = @import("std");
const claim_module = @This();
const payload = @import("document_payload.zig");
const ownership = payload.ownership;
const recipe = @import("retained_reference.zig");
const revision = @import("document_mutation_revision.zig");
const keys = @import("internal_keys.zig");
pub const publication = @import("index_publication.zig");
pub const Digest = [32]u8;
const A = std.mem.Allocator;
const claim_prefix = "\x00\x00__metadata__:index_source_claim_v1:";
const set_prefix = "\x00\x00__metadata__:index_source_values_v1:";
const snapshot_prefix = "\x00\x00__metadata__:index_source_snapshot_v1:";
const snapshot_refs_prefix = "\x00\x00__metadata__:index_source_snapshot_refs_v1:";
const budget_key = "\x00\x00__metadata__:index_source_budget_v1";
const initialized_key = "\x00\x00__metadata__:index_source_initialized_v1";
pub const ClaimKey = [claim_prefix.len + 96]u8;
const SetKey = [set_prefix.len + 64]u8;
const SnapshotKey = [snapshot_prefix.len + 64]u8;
const SnapshotRefsKey = [snapshot_refs_prefix.len + 64]u8;
pub const Limits = struct { metadata_bytes: u64 = 64 * 1024 * 1024, value_bytes: u64 = 256 * 1024 * 1024, claims: u64 = 1024 * 1024 };
pub const Revision = struct { domain: enum(u8) { mutation = 0, retained = 1 }, value: u64 };
pub const Reference = struct {
    binding: publication.Binding,
    document: Digest,
    revision: Revision,
    root: Digest,
    snapshot: Digest,
    pub fn key(self: Reference) ClaimKey {
        var out: ClaimKey = undefined;
        @memcpy(out[0..claim_prefix.len], claim_prefix);
        @memcpy(out[claim_prefix.len..][0..32], &self.binding.owner);
        @memcpy(out[claim_prefix.len + 32 ..][0..32], &self.binding.publication);
        @memcpy(out[claim_prefix.len + 64 ..], &self.document);
        return out;
    }
};
fn hash(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
    return out;
}
fn boundHash(key: []const u8, bytes: []const u8) Digest {
    var state = std.crypto.hash.sha2.Sha256.init(.{});
    state.update(key);
    state.update(bytes);
    return state.finalResult();
}
fn maybe(reader: anytype, key: []const u8) !?[]const u8 {
    return payload.rawGet(reader, key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}
pub const Budget = struct {
    metadata_bytes: u64 = 0,
    value_bytes: u64 = 0,
    claims: u64 = 0,
    pub fn load(reader: anytype) !Budget {
        const raw = (try maybe(reader, budget_key)) orelse return error.IndexSourceClaimsCorrupt;
        return decode(raw);
    }
    fn decode(raw: []const u8) !Budget {
        if (raw.len != 60 or !std.mem.eql(u8, raw[0..4], "ISB1") or !std.mem.eql(u8, raw[28..], &hash(raw[0..28]))) return error.IndexSourceClaimsCorrupt;
        return .{ .metadata_bytes = std.mem.readInt(u64, raw[4..12], .little), .value_bytes = std.mem.readInt(u64, raw[12..20], .little), .claims = std.mem.readInt(u64, raw[20..28], .little) };
    }
    fn save(self: Budget, writer: anytype, limits: Limits) !void {
        if (self.metadata_bytes > limits.metadata_bytes or self.value_bytes > limits.value_bytes or self.claims > limits.claims) return error.IndexSourceClaimsFull;
        var out: [60]u8 = undefined;
        @memcpy(out[0..4], "ISB1");
        std.mem.writeInt(u64, out[4..12], self.metadata_bytes, .little);
        std.mem.writeInt(u64, out[12..20], self.value_bytes, .little);
        std.mem.writeInt(u64, out[20..28], self.claims, .little);
        @memcpy(out[28..], &hash(out[0..28]));
        try writer.put(budget_key, &out);
    }
};
/// Borrows one still-open native write transaction. Admission is shared only
/// within that transaction; no persisted bit or caller-supplied proof skips a
/// later transaction's checks. Individual claims retain their revision, recipe,
/// uniqueness and budget checks. Never reuse after the writer commits/aborts.
pub fn Preparation(comptime Writer: type) type {
    return PreparationWithMinimum(Writer, 27);
}
/// A stronger physical consumer fence shares the same admission checks. It
/// must not hide an initialized ownership domain behind a missing older fence.
pub fn PreparationWithMinimum(comptime Writer: type, comptime minimum: u32) type {
    return struct {
        writer: Writer,
        admitted: u32 = 0,

        pub fn init(writer: Writer) @This() {
            return .{ .writer = writer };
        }
        pub fn ensureIndexSourceAdmission(self: *@This(), a: A) !void {
            return self.ensureIndexSourceMinimumAdmission(a, minimum);
        }
        pub fn ensureIndexSourceMinimumAdmission(self: *@This(), a: A, comptime requested: u32) !void {
            const required = @max(minimum, requested);
            if (self.admitted >= required) return;
            errdefer self.poison();
            try admitMinimum(a, self.writer, required);
            self.admitted = required;
        }
        pub fn getPhysical(self: *@This(), name: []const u8) anyerror![]const u8 {
            return payload.rawGet(self.writer, name);
        }
        pub fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            return self.getPhysical(name);
        }
        pub fn hasPrefix(self: *@This(), prefix: []const u8) anyerror!bool {
            return self.writer.hasPrefix(prefix);
        }
        pub fn put(self: *@This(), name: []const u8, value: []const u8) anyerror!void {
            if (std.mem.eql(u8, name, @import("schema.zig").schema_key)) self.admitted = 0;
            return self.writer.put(name, value);
        }
        pub fn delete(self: *@This(), name: []const u8) anyerror!void {
            if (std.mem.eql(u8, name, @import("schema.zig").schema_key)) self.admitted = 0;
            return self.writer.delete(name);
        }
        pub fn poison(self: *@This()) void {
            self.admitted = 0;
            self.writer.poison();
        }
    };
}

fn admit(a: A, writer: anytype) !void {
    return admitMinimum(a, writer, 27);
}
fn admitMinimum(a: A, writer: anytype, comptime minimum: u32) !void {
    const schema = @import("schema.zig");
    if (minimum < 27 or minimum > schema.maximum_reader_format_version) return error.UnsupportedVersion;
    const Writer = switch (@typeInfo(@TypeOf(writer))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(writer),
    };
    if (comptime @hasDecl(Writer, "ensureIndexSourceMinimumAdmission")) return writer.ensureIndexSourceMinimumAdmission(a, minimum);
    if (comptime @hasDecl(Writer, "ensureIndexSourceAdmission")) {
        try writer.ensureIndexSourceAdmission(a);
        if (minimum == 27) return;
    }
    const raw = try payload.rawGet(writer, schema.schema_key);
    const format = try schema.serializedReaderFormat(raw);
    // Own the AST only for promotion, before any probe can invalidate raw.
    // Already-admitted transactions allocate no schema object at all.
    const promotion = if (format < minimum) try schema.deserializeSchema(a, raw) else null;
    defer if (promotion) |decoded| schema.freeSchema(a, decoded);
    const existing = if (try maybe(writer, budget_key)) |budget| blk: {
        if (format < 27) return error.IndexSourceClaimsCorrupt;
        _ = try Budget.decode(budget);
        break :blk true;
    } else false;
    if (!existing) {
        // A shared format fence never implies initialized source ownership.
        if (try maybe(writer, initialized_key) != null) return error.IndexSourceClaimsCorrupt;
        for ([_][]const u8{ claim_prefix, set_prefix, snapshot_prefix, snapshot_refs_prefix, @import("index_source_recovery.zig").prefix }) |prefix| {
            if (try writer.hasPrefix(prefix)) return error.IndexSourceClaimsCorrupt;
        }
    }
    if (promotion) |decoded| {
        const encoded = try schema.serializeSchemaWithMinimumFormat(a, decoded, minimum);
        defer a.free(encoded);
        try writer.put(schema.schema_key, encoded);
    }
    if (!existing) {
        try (Budget{}).save(writer, .{});
        // The fixed witness prevents reset after complete reclamation and loss
        // of the initialized budget; it grants no source/read authorization.
        try writer.put(initialized_key, "1");
    }
}

const ValueSet = struct {
    count: u64,
    values: []ownership.Value,
    fn deinit(self: ValueSet, a: A) void {
        a.free(self.values);
    }
    fn load(a: A, reader: anytype, key: SetKey) !?ValueSet {
        const raw = (try maybe(reader, &key)) orelse return null;
        if (raw.len < 46 or !std.mem.eql(u8, raw[0..4], "ISV1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &boundHash(&key, raw[0 .. raw.len - 32]))) return error.IndexSourceClaimsCorrupt;
        const count = std.mem.readInt(u64, raw[4..12], .little);
        const size = std.mem.readInt(u16, raw[12..14], .little);
        if (count == 0 or size == 0 or size > 512 or raw.len != 46 + @as(usize, size) * 40) return error.IndexSourceClaimsCorrupt;
        const values = try a.alloc(ownership.Value, size);
        errdefer a.free(values);
        for (values, 0..) |*value, i| {
            const offset = 14 + i * 40;
            value.* = .{ .digest = raw[offset..][0..32].*, .length = std.math.cast(usize, std.mem.readInt(u64, raw[offset + 32 ..][0..8], .little)) orelse return error.IndexSourceClaimsCorrupt };
            if (value.length == 0 or (i != 0 and std.mem.order(u8, &values[i - 1].digest, &value.digest) != .lt)) return error.IndexSourceClaimsCorrupt;
        }
        if (!std.mem.eql(u8, &key, &setKey(key[set_prefix.len..][0..32].*, values))) return error.IndexSourceClaimsCorrupt;
        return .{ .count = count, .values = values };
    }
    fn save(self: ValueSet, a: A, writer: anytype, key: SetKey) !void {
        const raw = try a.alloc(u8, 46 + self.values.len * 40);
        defer a.free(raw);
        @memcpy(raw[0..4], "ISV1");
        std.mem.writeInt(u64, raw[4..12], self.count, .little);
        std.mem.writeInt(u16, raw[12..14], @intCast(self.values.len), .little);
        for (self.values, 0..) |value, i| {
            const offset = 14 + i * 40;
            @memcpy(raw[offset..][0..32], &value.digest);
            std.mem.writeInt(u64, raw[offset + 32 ..][0..8], value.length, .little);
        }
        @memcpy(raw[raw.len - 32 ..], &boundHash(&key, raw[0 .. raw.len - 32]));
        try writer.put(&key, raw);
    }
};
fn setKey(document: Digest, values: []const ownership.Value) SetKey {
    var state = std.crypto.hash.sha2.Sha256.init(.{});
    state.update("antfly.index.source.values/1\x00");
    for (values) |value| {
        state.update(&value.digest);
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, value.length, .little);
        state.update(&length);
    }
    var out: SetKey = undefined;
    @memcpy(out[0..set_prefix.len], set_prefix);
    @memcpy(out[set_prefix.len..][0..32], &document);
    @memcpy(out[set_prefix.len + 32 ..], &state.finalResult());
    return out;
}
fn effectValues(a: A, effect: recipe.Effect) ![]ownership.Value {
    if (effect.manifest) |manifest| {
        const all = try payload.immutableValues(a, manifest);
        defer a.free(all);
        std.mem.sort(ownership.Value, all, {}, struct {
            fn less(_: void, l: ownership.Value, r: ownership.Value) bool {
                return std.mem.order(u8, &l.digest, &r.digest) == .lt;
            }
        }.less);
        var size: usize = 0;
        for (all) |item| {
            if (size != 0 and std.mem.eql(u8, &all[size - 1].digest, &item.digest)) {
                if (all[size - 1].length != item.length) return error.IndexSourceClaimsCorrupt;
                continue;
            }
            all[size] = item;
            size += 1;
        }
        return a.dupe(ownership.Value, all[0..size]);
    }
    return a.alloc(ownership.Value, 0);
}
fn sourceRoot(effect: recipe.Effect) Digest {
    return if (effect.manifest) |manifest| manifest.logical_digest else hash(effect.value.?);
}
fn snapshotKey(ref: Reference) SnapshotKey {
    var out: SnapshotKey = undefined;
    @memcpy(out[0..snapshot_prefix.len], snapshot_prefix);
    @memcpy(out[snapshot_prefix.len..][0..32], &ref.document);
    @memcpy(out[snapshot_prefix.len + 32 ..], &ref.snapshot);
    return out;
}
fn snapshotRefsKey(ref: Reference) SnapshotRefsKey {
    var out: SnapshotRefsKey = undefined;
    @memcpy(out[0..snapshot_refs_prefix.len], snapshot_refs_prefix);
    @memcpy(out[snapshot_refs_prefix.len..][0..32], &ref.document);
    @memcpy(out[snapshot_refs_prefix.len + 32 ..], &ref.snapshot);
    return out;
}
fn snapshotId(source_revision: Revision, effect: recipe.Effect) Digest {
    var state = std.crypto.hash.sha2.Sha256.init(.{});
    state.update("antfly.index.source.snapshot/1\x00");
    state.update(&.{@backingInt(source_revision.domain)});
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, source_revision.value, .little);
    state.update(&encoded);
    std.mem.writeInt(u64, &encoded, effect.timestamp, .little);
    state.update(&encoded);
    state.update(&sourceRoot(effect));
    if (effect.manifest) |manifest| state.update(&manifest.physical_commitment);
    return state.finalResult();
}
fn loadCount(reader: anytype, ref: Reference) !?u64 {
    const raw = (try maybe(reader, &snapshotRefsKey(ref))) orelse return null;
    if (raw.len != 44 or !std.mem.eql(u8, raw[0..4], "ISR1") or !std.mem.eql(u8, raw[12..], &boundHash(&snapshotRefsKey(ref), raw[0..12]))) return error.IndexSourceClaimsCorrupt;
    const count = std.mem.readInt(u64, raw[4..12], .little);
    if (count == 0) return error.IndexSourceClaimsCorrupt;
    return count;
}
fn saveCount(writer: anytype, ref: Reference, count: u64) !void {
    var raw: [44]u8 = undefined;
    @memcpy(raw[0..4], "ISR1");
    std.mem.writeInt(u64, raw[4..12], count, .little);
    @memcpy(raw[12..], &boundHash(&snapshotRefsKey(ref), raw[0..12]));
    try writer.put(&snapshotRefsKey(ref), &raw);
}
const Header = struct {
    raw: [101]u8,
    fn load(reader: anytype, ref: Reference) !?Header {
        const raw = (try maybe(reader, &ref.key())) orelse return null;
        if (raw.len != 101 or !std.mem.eql(u8, raw[0..4], "ISC2") or raw[4] > 1 or
            !std.mem.eql(u8, raw[69..], &boundHash(&ref.key(), raw[0..69]))) return error.IndexSourceClaimsCorrupt;
        if (!std.mem.eql(u8, raw[5..37], &ref.snapshot)) return error.IndexSourceClaimConflict;
        return .{ .raw = raw[0..101].* };
    }
    fn published(self: Header) bool {
        return self.raw[4] != 0;
    }
    fn root(self: Header) Digest {
        return self.raw[37..69].*;
    }
    fn save(self: *Header, writer: anytype, ref: Reference) !void {
        @memcpy(self.raw[69..], &boundHash(&ref.key(), self.raw[0..69]));
        try writer.put(&ref.key(), &self.raw);
    }
};
const Record = struct {
    raw: []u8,
    plan: recipe.Plan,
    header: Header,
    fn deinit(self: Record, a: A) void {
        self.plan.deinit(a);
        a.free(self.raw);
    }
    fn load(a: A, reader: anytype, ref: Reference) !?Record {
        const header = (try Header.load(reader, ref)) orelse return null;
        const borrowed = (try maybe(reader, &snapshotKey(ref))) orelse return error.IndexSourceClaimsCorrupt;
        if (borrowed.len < 86 or borrowed.len > 16 * 1024 * 1024 or !std.mem.eql(u8, borrowed[0..4], "ISS1") or borrowed[4] != @backingInt(ref.revision.domain) or
            !std.mem.eql(u8, borrowed[borrowed.len - 32 ..], &boundHash(&snapshotKey(ref), borrowed[0 .. borrowed.len - 32]))) return error.IndexSourceClaimsCorrupt;
        const raw = try a.dupe(u8, borrowed);
        errdefer a.free(raw);
        const plan = try recipe.Plan.decode(a, raw[5 .. raw.len - 32], ref.revision.value);
        errdefer plan.deinit(a);
        if (plan.effects.len != 1 or plan.effects[0].value == null or !std.mem.eql(u8, &hash(plan.effects[0].key), &ref.document) or
            !std.mem.eql(u8, &sourceRoot(plan.effects[0]), &ref.root) or !std.mem.eql(u8, &snapshotId(ref.revision, plan.effects[0]), &ref.snapshot)) return error.IndexSourceClaimsCorrupt;
        return .{ .raw = raw, .plan = plan, .header = header };
    }
};
fn link(writer: anytype, ref: Reference, budget: *Budget, limits: Limits) !Reference {
    if (try Header.load(writer, ref)) |_| return ref;
    return linkAbsent(writer, ref, budget, limits);
}
fn linkAbsent(writer: anytype, ref: Reference, budget: *Budget, limits: Limits) !Reference {
    const count = (try loadCount(writer, ref)) orelse return error.IndexSourceClaimMissing;
    try saveCount(writer, ref, try std.math.add(u64, count, 1));
    var header: Header = .{ .raw = @splat(0) };
    @memcpy(header.raw[0..4], "ISC2");
    @memcpy(header.raw[5..37], &ref.snapshot);
    try header.save(writer, ref);
    budget.metadata_bytes = try std.math.add(u64, budget.metadata_bytes, 101 + @sizeOf(ClaimKey));
    budget.claims = try std.math.add(u64, budget.claims, 1);
    try budget.save(writer, limits);
    return ref;
}
fn stage(a: A, writer: anytype, binding: publication.Binding, source_revision: Revision, effect: recipe.Effect, limits: Limits, imported_source: anytype, comptime unique: bool) !Reference {
    if (!binding.valid()) return error.InvalidIndexPublication;
    const ref: Reference = .{ .binding = binding, .revision = source_revision, .document = hash(effect.key), .root = sourceRoot(effect), .snapshot = snapshotId(source_revision, effect) };
    if (unique) {
        if (try maybe(writer, &ref.key()) != null) return error.InvalidIndexSourceMap;
    } else if (try Header.load(writer, ref)) |_| return ref;
    var budget = try Budget.load(writer);
    if (try loadCount(writer, ref) != null) return linkAbsent(writer, ref, &budget, limits);
    const stored_recipe = try recipe.encode(a, source_revision.value, false, false, (&effect)[0..1]);
    defer a.free(stored_recipe);
    if (stored_recipe.len > 16 * 1024 * 1024 - 37) return error.IndexSourceClaimsFull;
    const raw = try a.alloc(u8, stored_recipe.len + 37);
    defer a.free(raw);
    @memcpy(raw[0..4], "ISS1");
    raw[4] = @backingInt(source_revision.domain);
    @memcpy(raw[5 .. raw.len - 32], stored_recipe);
    @memcpy(raw[raw.len - 32 ..], &boundHash(&snapshotKey(ref), raw[0 .. raw.len - 32]));
    const immutable = try effectValues(a, effect);
    defer a.free(immutable);
    if (immutable.len != 0) {
        const set_key = setKey(ref.document, immutable);
        if (try ValueSet.load(a, writer, set_key)) |existing| {
            defer existing.deinit(a);
            const next: ValueSet = .{ .count = try std.math.add(u64, existing.count, 1), .values = existing.values };
            try next.save(a, writer, set_key);
        } else {
            budget.value_bytes = try std.math.add(u64, budget.value_bytes, if (comptime @TypeOf(imported_source) == @TypeOf(null)) try ownership.acquireIndexedPin(a, writer, effect.key, immutable) else try ownership.importIndexedPin(a, writer, imported_source, effect.key, immutable));
            try (ValueSet{ .count = 1, .values = immutable }).save(a, writer, set_key);
            budget.metadata_bytes = try std.math.add(u64, budget.metadata_bytes, 46 + immutable.len * 40 + @sizeOf(SetKey));
        }
    }
    try writer.put(&snapshotKey(ref), raw);
    // Link performs the common refcount increment; an immutable snapshot is
    // written once, never copied to update its counter or fork another index.
    try saveCount(writer, ref, 1);
    budget.metadata_bytes = try std.math.add(u64, budget.metadata_bytes, raw.len + @sizeOf(SnapshotKey) + 44 + @sizeOf(SnapshotRefsKey));
    var header: Header = .{ .raw = @splat(0) };
    @memcpy(header.raw[0..4], "ISC2");
    @memcpy(header.raw[5..37], &ref.snapshot);
    try header.save(writer, ref);
    budget.metadata_bytes = try std.math.add(u64, budget.metadata_bytes, 101 + @sizeOf(ClaimKey));
    budget.claims = try std.math.add(u64, budget.claims, 1);
    try budget.save(writer, limits);
    return ref;
}

/// Native observation of the exact bytes fed to an index producer. The caller
/// owns a stable read transaction until claims are prepared; a current probe,
/// serialized revision, or content digest cannot substitute for that read cut.
pub const ObservedSource = struct {
    reader: *@import("docstore.zig").DocStore.Txn,
    physical: []u8,
    mutation: u64,
    root: Digest,
    witness: *const fn () void,
    produced: []const u8 = &.{},
    owned_produced: ?[]u8 = null,

    fn sealed() void {}
    pub fn deinit(self: *ObservedSource, a: A) void {
        a.free(self.physical);
        if (self.owned_produced) |bytes| a.free(bytes);
        self.* = undefined;
    }
    fn require(self: *const ObservedSource) !void {
        if (self.witness != &sealed or self.reader.read == null or self.reader.write != null or
            self.reader.probe != null or self.reader.current_scan != null) return error.IndexSourceReadSnapshotRequired;
    }
    pub fn requireProduced(self: *const ObservedSource, produced: []const u8) !void {
        try self.require();
        if (self.produced.ptr != produced.ptr or self.produced.len != produced.len) return error.PreparedReadSetChanged;
    }
    pub fn jsonStringify(_: ObservedSource, _: anytype) !void {
        return error.NativeIndexSourceCannotSerialize;
    }
    pub fn jsonParse(_: A, _: anytype, _: std.json.ParseOptions) !ObservedSource {
        return error.NativeIndexSourceCannotSerialize;
    }

    pub fn capture(a: A, reader: *@import("docstore.zig").DocStore.Txn, physical: []const u8, produced: []const u8) !ObservedSource {
        var result: ObservedSource = .{ .reader = reader, .physical = undefined, .mutation = 0, .root = undefined, .witness = &sealed };
        try result.require();
        if (!keys.isPrimaryDocumentKey(physical)) return error.Unsupported;
        result.mutation = try revision.load(reader, revision.keyForPhysical(physical).?);
        const borrowed = try payload.rawGet(reader, physical);
        if (borrowed.len > 16 * 1024 * 1024) return error.IndexSourceClaimsFull;
        if (payload.isManifest(borrowed)) {
            // Metadata must outlive subsequent blob reads. Compare exact source
            // bytes in bounded scratch, without a second whole-document copy.
            const raw = try a.dupe(u8, borrowed);
            defer a.free(raw);
            const manifest = try payload.Manifest.decode(a, raw);
            defer a.free(manifest.parts);
            if (manifest.logical_bytes != produced.len) return error.PreparedReadSetChanged;
            var scratch: [64 * 1024]u8 = undefined;
            var offset: usize = 0;
            while (offset < produced.len) {
                const part = scratch[0..@min(scratch.len, produced.len - offset)];
                try manifest.readInto(reader, physical, offset, part);
                if (!std.mem.eql(u8, part, produced[offset..][0..part.len])) return error.PreparedReadSetChanged;
                offset += part.len;
            }
            result.root = manifest.logical_digest;
        } else {
            if (!std.mem.eql(u8, borrowed, produced)) return error.PreparedReadSetChanged;
            result.root = hash(borrowed);
        }
        result.physical = try a.dupe(u8, physical);
        result.produced = produced;
        return result;
    }

    /// Own materialization and its proof together. The bytes come directly
    /// from this held read snapshot, so no caller-provided afterimage must be
    /// reread and compared. Missing rows leave the existing replay path intact.
    pub fn readSortedOwned(a: A, reader: *@import("docstore.zig").DocStore.Txn, physical: []const []const u8) !?[]ObservedSource {
        const authority: ObservedSource = .{ .reader = reader, .physical = undefined, .mutation = 0, .root = undefined, .witness = &sealed };
        try authority.require();
        for (physical) |key| if (!keys.isPrimaryDocumentKey(key)) return error.Unsupported;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const values = try scratch.alloc(?[]const u8, physical.len);
        try reader.getManySortedPhysical(physical, values);
        for (values) |value| if (value == null) return null;
        const result = try a.alloc(ObservedSource, physical.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |*source| source.deinit(a);
            a.free(result);
        }
        // Close every borrowed row before any subsequent LSM read. Inline rows
        // go directly to their final owner; only small manifest metadata uses
        // temporary copies, avoiding a whole-batch duplicate of inline bodies.
        for (physical, values, result) |key, *raw, *source| {
            if (raw.*.?.len > 16 * 1024 * 1024) return error.IndexSourceClaimsFull;
            const manifest = payload.isManifest(raw.*.?);
            const produced = if (manifest) null else try a.dupe(u8, raw.*.?);
            errdefer if (produced) |bytes| a.free(bytes);
            if (manifest) raw.* = try scratch.dupe(u8, raw.*.?);
            source.* = .{ .reader = reader, .physical = try a.dupe(u8, key), .mutation = 0, .root = if (produced) |bytes| hash(bytes) else undefined, .witness = &sealed, .produced = produced orelse &.{}, .owned_produced = produced };
            initialized += 1;
        }
        for (physical, values, result) |key, raw, *source| {
            source.mutation = try revision.load(reader, revision.keyForPhysical(key).?);
            if (source.owned_produced == null) {
                const manifest = try payload.Manifest.decode(a, raw.?);
                defer a.free(manifest.parts);
                const produced = try manifest.readOwned(a, reader, key);
                source.root = manifest.logical_digest;
                source.produced = produced;
                source.owned_produced = produced;
            }
        }
        return result;
    }
};

/// Import ownership from the producer's still-held MVCC cut, even after the
/// current primary row has changed or its old immutable values were deleted.
pub fn prepareObserved(a: A, writer: anytype, binding: publication.Binding, observed: *const ObservedSource, limits: Limits, comptime unique: bool) !Reference {
    errdefer writer.poison();
    try observed.require();
    const actual = try revision.load(observed.reader, revision.keyForPhysical(observed.physical).?);
    if (actual != observed.mutation) return error.PreparedReadSetChanged;
    const borrowed = try payload.rawGet(observed.reader, observed.physical);
    if (borrowed.len > 16 * 1024 * 1024) return error.IndexSourceClaimsFull;
    const raw = try a.dupe(u8, borrowed);
    defer a.free(raw);
    const manifest = if (payload.isManifest(raw)) try payload.Manifest.decode(a, raw) else null;
    defer if (manifest) |decoded| a.free(decoded.parts);
    const effect: recipe.Effect = .{ .key = observed.physical, .value = raw, .timestamp = 0, .manifest = manifest };
    if (!std.mem.eql(u8, &sourceRoot(effect), &observed.root)) return error.PreparedReadSetChanged;
    try admit(a, writer);
    return stage(a, writer, binding, .{ .domain = .mutation, .value = actual }, effect, limits, observed.reader, unique);
}

/// A primary afterimage is pinned before its index publication can become
/// visible. All input and ownership checks use this same write transaction.
pub fn prepareCurrent(a: A, writer: anytype, binding: publication.Binding, physical: []const u8, expected_revision: u64, expected_root: Digest, limits: Limits) !Reference {
    return prepareCurrentImpl(a, writer, binding, physical, expected_revision, expected_root, limits, false);
}
/// Strict creation for one fresh publication transaction. A repeated document
/// poisons that transaction instead of being interpreted as an idempotent retry.
pub fn prepareCurrentUnique(a: A, writer: anytype, binding: publication.Binding, physical: []const u8, expected_revision: u64, expected_root: Digest, limits: Limits) !Reference {
    return prepareCurrentImpl(a, writer, binding, physical, expected_revision, expected_root, limits, true);
}
fn prepareCurrentImpl(a: A, writer: anytype, binding: publication.Binding, physical: []const u8, expected_revision: u64, expected_root: Digest, limits: Limits, comptime unique: bool) !Reference {
    errdefer writer.poison();
    if (!keys.isStoredDocumentRowKey(physical)) return error.Unsupported;
    const borrowed = try payload.rawGet(writer, physical);
    if (borrowed.len > 16 * 1024 * 1024) return error.IndexSourceClaimsFull;
    const raw = try a.dupe(u8, borrowed);
    defer a.free(raw);
    const actual_revision = try revision.load(writer, revision.keyForPhysical(physical).?);
    if (actual_revision != expected_revision) return error.PreparedReadSetChanged;
    const manifest = if (payload.isManifest(raw)) try payload.Manifest.decode(a, raw) else null;
    defer if (manifest) |decoded| a.free(decoded.parts);
    const effect: recipe.Effect = .{ .key = physical, .value = raw, .timestamp = 0, .manifest = manifest };
    if (!std.mem.eql(u8, &sourceRoot(effect), &expected_root)) return error.PreparedReadSetChanged;
    try admit(a, writer);
    return stage(a, writer, binding, .{ .domain = .mutation, .value = actual_revision }, effect, limits, null, unique);
}
/// Handoff from a still-live retained afterimage. Bare value counters or
/// disposable content caches cannot authorize a historical source revision.
pub fn prepareRetained(a: A, writer: anytype, binding: publication.Binding, namespace: [24]u8, sequence: u64, physical: []const u8, expected_root: Digest, limits: Limits) !Reference {
    return prepareRetainedImpl(a, writer, binding, namespace, sequence, physical, expected_root, limits, false);
}
/// Strict creation for one fresh publication transaction. A repeated document
/// poisons that transaction instead of being interpreted as an idempotent retry.
pub fn prepareRetainedUnique(a: A, writer: anytype, binding: publication.Binding, namespace: [24]u8, sequence: u64, physical: []const u8, expected_root: Digest, limits: Limits) !Reference {
    return prepareRetainedImpl(a, writer, binding, namespace, sequence, physical, expected_root, limits, true);
}
fn prepareRetainedImpl(a: A, writer: anytype, binding: publication.Binding, namespace: [24]u8, sequence: u64, physical: []const u8, expected_root: Digest, limits: Limits, comptime unique: bool) !Reference {
    errdefer writer.poison();
    const retention = @import("retained_effects.zig");
    const state = (try retention.load(writer)) orelse return error.IndexSourceHistoryExpired;
    if (!std.mem.eql(u8, &state.namespace, &namespace) or sequence <= state.reclaimed or sequence > state.latest) return error.IndexSourceHistoryExpired;
    const borrowed = (try maybe(writer, &retention.recordKey(sequence))) orelse return error.IndexSourceHistoryExpired;
    if (!recipe.isRecord(borrowed)) return error.Unsupported;
    if (borrowed.len > 16 * 1024 * 1024) return error.IndexSourceClaimsFull;
    const raw = try a.dupe(u8, borrowed);
    defer a.free(raw);
    const plan = try recipe.Plan.decode(a, raw, sequence);
    defer plan.deinit(a);
    for (plan.effects) |effect| if (std.mem.eql(u8, effect.key, physical)) {
        if (effect.value == null or !std.mem.eql(u8, &sourceRoot(effect), &expected_root)) return error.PreparedReadSetChanged;
        try admit(a, writer);
        return stage(a, writer, binding, .{ .domain = .retained, .value = sequence }, effect, limits, null, unique);
    };
    return error.IndexSourceHistoryExpired;
}
pub fn fork(a: A, writer: anytype, original: Reference, binding: publication.Binding, limits: Limits) !Reference {
    _ = a;
    errdefer writer.poison();
    if (!binding.valid()) return error.InvalidIndexPublication;
    _ = (try Header.load(writer, original)) orelse return error.IndexSourceClaimMissing;
    var next = original;
    next.binding = binding;
    var budget = try Budget.load(writer);
    return link(writer, next, &budget, limits);
}
/// Structural handoff from a publication's held source cut. Resolve and check
/// the immutable recipe before linking it; current primary bytes are irrelevant.
pub fn forkPublished(a: A, writer: anytype, original: Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease, binding: publication.Binding, limits: Limits) !Reference {
    return forkPublishedImpl(a, writer, original, decision, lease, binding, limits, false);
}
/// Strict creation for one fresh publication transaction. A repeated document
/// poisons that transaction instead of being interpreted as an idempotent retry.
pub fn forkPublishedUnique(a: A, writer: anytype, original: Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease, binding: publication.Binding, limits: Limits) !Reference {
    return forkPublishedImpl(a, writer, original, decision, lease, binding, limits, true);
}
fn forkPublishedImpl(a: A, writer: anytype, original: Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease, binding: publication.Binding, limits: Limits, comptime unique: bool) !Reference {
    errdefer writer.poison();
    try decision.requireSourceRole();
    try decision.require(original.binding, .published);
    try lease.require(original.binding);
    const record = (try Record.load(a, writer, original)) orelse return error.IndexSourceClaimMissing;
    defer record.deinit(a);
    if (record.header.published() and !std.mem.eql(u8, &record.header.root(), &decision.root)) return error.IndexSourceClaimConflict;
    if (!unique) return fork(a, writer, original, binding, limits);
    if (!binding.valid()) return error.InvalidIndexPublication;
    var next = original;
    next.binding = binding;
    if (try maybe(writer, &next.key()) != null) return error.InvalidIndexSourceMap;
    var budget = try Budget.load(writer);
    return linkAbsent(writer, next, &budget, limits);
}
pub fn acknowledge(a: A, writer: anytype, ref: Reference, decision: publication.Decision) !void {
    _ = a;
    errdefer writer.poison();
    try decision.requireSourceRole();
    try decision.require(ref.binding, .published);
    var header = (try Header.load(writer, ref)) orelse return error.IndexSourceClaimMissing;
    if (header.published()) {
        if (!std.mem.eql(u8, &header.root(), &decision.root)) return error.IndexSourceClaimConflict;
        return;
    }
    header.raw[4] = 1;
    @memcpy(header.raw[37..69], &decision.root);
    try header.save(writer, ref);
}
/// Read immutable claim descendants through the actual publication lease.
/// A current probe is sufficient here because those records/values cannot be
/// reclaimed or replaced while the native lease lives. It is never a source
/// observation for a producer, nor authority to read current primary rows.
pub fn LeasedSourceReader(comptime Reader: type) type {
    return struct {
        reader: Reader,
        binding: publication.Binding,
        lease: *const publication.ReaderPins.Lease,

        pub fn getPhysical(self: @This(), key: []const u8) ![]const u8 {
            try self.lease.require(self.binding);
            if (keys.isStoredSourceRowKey(key)) return error.CurrentPrimarySourceReadForbidden;
            const Base = switch (@typeInfo(Reader)) {
                .pointer => |pointer| pointer.child,
                else => Reader,
            };
            if (comptime @hasDecl(Base, "getLeasedPhysical")) return self.reader.getLeasedPhysical(key);
            return payload.rawGet(self.reader, key);
        }
        pub fn get(self: @This(), key: []const u8) ![]const u8 {
            return self.getPhysical(key);
        }
        pub fn jsonStringify(_: @This(), _: anytype) !void {
            return error.NativeIndexSourceCannotSerialize;
        }
        pub fn jsonParse(_: A, _: anytype, _: std.json.ParseOptions) !@This() {
            return error.NativeIndexSourceCannotSerialize;
        }
    };
}

pub const CapturedSource = struct {
    reference: Reference,
    decision: publication.Decision,
    lease: publication.SharedReaders.Lease,
    pub fn deinit(self: *CapturedSource) void {
        self.lease.deinit();
    }
    pub fn immutableReader(self: *const CapturedSource, primary: anytype) !LeasedSourceReader(@TypeOf(primary)) {
        try self.decision.requireSourceRole();
        try self.decision.require(self.reference.binding, .published);
        try self.lease.inner.require(self.reference.binding);
        return .{ .reader = primary, .binding = self.reference.binding, .lease = &self.lease.inner };
    }

    pub fn openRead(self: *const CapturedSource, a: A, primary: anytype) !ReadSession(@TypeOf(primary)) {
        return claim_module.openRead(a, primary, self.reference, self.decision, &self.lease.inner);
    }
    pub fn read(self: *const CapturedSource, a: A, primary: anytype) !Source {
        return claim_module.read(a, primary, self.reference, self.decision, &self.lease.inner);
    }
    pub fn readProjected(self: *const CapturedSource, a: A, primary: anytype, paths: []const []const []const u8, limits: ProjectionLimits) !?ProjectedSource {
        return claim_module.readProjected(a, primary, self.reference, self.decision, &self.lease.inner, paths, limits);
    }
};

pub const CapturedRoot = struct {
    decision: publication.Decision,
    lease: publication.SharedReaders.Lease,
    pub fn deinit(self: *@This()) void {
        self.lease.deinit();
    }
};
/// Compact row evidence when a publication root is shared by selected rows.
pub const SnapshotEntry = struct {
    document: Digest,
    revision: Revision,
    root: Digest,
    snapshot: Digest,
    pub fn fromReference(value: Reference) @This() {
        return .{ .document = value.document, .revision = value.revision, .root = value.root, .snapshot = value.snapshot };
    }
    pub fn reference(self: @This(), binding: publication.Binding) Reference {
        return .{ .binding = binding, .document = self.document, .revision = self.revision, .root = self.root, .snapshot = self.snapshot };
    }
};

pub const PredicateSource = struct { bytes: []u8, original_bytes: u64, projected: bool };

pub const Source = struct {
    physical: []u8,
    bytes: []u8,
    pub fn deinit(self: Source, a: A) void {
        a.free(self.physical);
        a.free(self.bytes);
    }
};
/// The index root is acquired before the primary read view. A pending claim is
/// readable after index publication, even if a crash prevented acknowledgment.
/// The latest primary row is never used to hydrate this source revision.
/// Owns one decoded immutable recipe, borrowing the caller's primary read
/// view and actual reader lease. Both must remain live until deinit. Keeping
/// their native identities here prevents fallback from switching read views.
pub fn ReadSession(comptime Reader: type) type {
    return struct {
        allocator: A,
        reader: Reader,
        record: Record,
        binding: publication.Binding,
        revision: Revision,
        lease: *const publication.ReaderPins.Lease,
        pub fn deinit(self: *@This()) void {
            self.record.deinit(self.allocator);
            self.* = undefined;
        }
        pub fn originalBytes(self: *const @This()) !usize {
            try self.lease.require(self.binding);
            return self.record.plan.effects[0].valueLength();
        }
        pub fn read(self: *const @This(), a: A) !Source {
            try self.lease.require(self.binding);
            const effect = self.record.plan.effects[0];
            const physical = try a.dupe(u8, effect.key);
            errdefer a.free(physical);
            const bytes = if (effect.manifest) |manifest| try manifest.readOwned(a, self.reader, physical) else try a.dupe(u8, effect.value.?);
            return .{ .physical = physical, .bytes = bytes };
        }
        /// Shape-preserving input for closed logical predicates. Opaque
        /// container extents stay intact: response projection may compact
        /// arrays, which would change numeric paths during verification.
        pub fn readLogicalPaths(self: *const @This(), output: A, paths: []const []const []const u8, limits: ProjectionLimits) !PredicateSource {
            const original = try self.originalBytes();
            var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(output));
            defer arena.deinit();
            const a = arena.allocator();
            const selected = if (paths.len == 0) null else self.readProjected(a, paths, limits) catch |err| switch (err) {
                error.IndexSourceProjectionFull => null,
                else => return err,
            };
            if (selected) |projected| {
                defer projected.deinit(a);
                if (projected.values.len != paths.len) return error.IndexSourceClaimsCorrupt;
                const order = try a.alloc(usize, paths.len);
                for (order, 0..) |*entry, i| entry.* = i;
                std.mem.sort(usize, order, projected.values, struct {
                    fn less(values: []const ProjectedValue, left: usize, right: usize) bool {
                        return values[left].covered < values[right].covered;
                    }
                }.less);
                var root: std.json.Value = .{ .object = .empty };
                const accepted = try a.alloc(usize, paths.len);
                var accepted_count: usize = 0;
                for (order) |index| {
                    const value = projected.values[index];
                    if (value.covered > paths[index].len or (value.json != null and value.covered == 0)) return error.IndexSourceClaimsCorrupt;
                    const prefix = paths[index][0..value.covered];
                    const covered = for (accepted[0..accepted_count]) |parent| {
                        const parent_path = paths[parent][0..projected.values[parent].covered];
                        if (parent_path.len > prefix.len) continue;
                        var equal = true;
                        for (parent_path, prefix[0..parent_path.len]) |left, right| if (!std.mem.eql(u8, left, right)) {
                            equal = false;
                            break;
                        };
                        if (equal) break true;
                    } else false;
                    if (covered) continue;
                    var cursor = &root;
                    for (prefix, 0..) |part, depth| {
                        if (cursor.* != .object) return error.IndexSourceClaimsCorrupt;
                        const entry = try cursor.object.getOrPut(a, part);
                        if (!entry.found_existing) {
                            entry.key_ptr.* = try a.dupe(u8, part);
                            entry.value_ptr.* = .{ .object = .empty };
                        }
                        if (depth + 1 == prefix.len and value.json != null) {
                            entry.value_ptr.* = try std.json.parseFromSliceLeaky(std.json.Value, a, value.json.?, .{ .allocate = .alloc_always });
                        }
                        cursor = entry.value_ptr;
                    }
                    if (value.json != null) {
                        accepted[accepted_count] = index;
                        accepted_count += 1;
                    }
                }
                return .{ .bytes = try std.json.Stringify.valueAlloc(output, root, .{}), .original_bytes = original, .projected = true };
            }
            const full = try self.read(output);
            output.free(full.physical);
            return .{ .bytes = full.bytes, .original_bytes = original, .projected = false };
        }
        /// Native cross-primary handoff. Retains the indexed revision and
        /// recipe even when either primary's mutable row has changed or gone.
        pub fn importClaim(self: *const @This(), a: A, writer: anytype, binding: publication.Binding, limits: Limits) !Reference {
            return self.importClaimImpl(a, writer, binding, limits, false);
        }
        pub fn importClaimUnique(self: *const @This(), a: A, writer: anytype, binding: publication.Binding, limits: Limits) !Reference {
            return self.importClaimImpl(a, writer, binding, limits, true);
        }
        fn importClaimImpl(self: *const @This(), a: A, writer: anytype, binding: publication.Binding, limits: Limits, comptime unique: bool) !Reference {
            errdefer writer.poison();
            try self.lease.require(self.binding);
            const effect = self.record.plan.effects[0];
            // Relational recipes require their schema/column generation as
            // well; a byte copy cannot substitute for that native lifetime.
            if (!keys.isPrimaryDocumentKey(effect.key)) return error.Unsupported;
            try admit(a, writer);
            return stage(a, writer, binding, self.revision, effect, limits, self.reader, unique);
        }
        pub fn readProjected(self: *const @This(), a: A, paths: []const []const []const u8, limits: ProjectionLimits) !?ProjectedSource {
            try self.lease.require(self.binding);
            return projectRecord(a, self.reader, &self.record, paths, limits);
        }
    };
}
pub fn openRead(a: A, reader: anytype, ref: Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease) !ReadSession(@TypeOf(reader)) {
    try decision.requireSourceRole();
    try decision.require(ref.binding, .published);
    try lease.require(ref.binding);
    const record = (try Record.load(a, reader, ref)) orelse return error.IndexSourceClaimMissing;
    errdefer record.deinit(a);
    if (record.header.published() and !std.mem.eql(u8, &record.header.root(), &decision.root)) return error.IndexSourceClaimConflict;
    return .{ .allocator = a, .reader = reader, .record = record, .binding = ref.binding, .revision = ref.revision, .lease = lease };
}
pub fn read(a: A, reader: anytype, ref: Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease) !Source {
    var session = try openRead(a, reader, ref, decision, lease);
    defer session.deinit();
    return session.read(a);
}
pub const ProjectedValue = struct {
    /// Null is proven absent. Opaque containers cover a path prefix and retain
    /// their JSON shape for the ordinary projection interpreter to finish.
    /// For absent values, covered counts proven existing object ancestors.
    json: ?[]const u8,
    covered: usize,
    canonical: ?Digest,
};
pub const ProjectedSource = struct {
    physical: []u8,
    values: []ProjectedValue,
    data: []u8,
    pub fn deinit(self: ProjectedSource, a: A) void {
        a.free(self.physical);
        a.free(self.values);
        a.free(self.data);
    }
};
pub const ProjectionLimits = struct { paths: usize = 128, bytes: usize = 16 * 1024 * 1024 };
/// Project from the publication's exact immutable manifest. No lookup of the
/// current row, mutable directory, scalar cache or latest revision is permitted.
/// Null means this older/inline recipe needs the ordinary full-source path.
pub fn readProjected(a: A, reader: anytype, ref: Reference, decision: publication.Decision, lease: *const publication.ReaderPins.Lease, paths: []const []const []const u8, limits: ProjectionLimits) !?ProjectedSource {
    // Reject invalid selection budgets before reading the recipe.
    try validateProjection(paths, limits);
    var session = try openRead(a, reader, ref, decision, lease);
    defer session.deinit();
    return session.readProjected(a, paths, limits);
}
fn validateProjection(paths: []const []const []const u8, limits: ProjectionLimits) !void {
    if (paths.len > @min(limits.paths, 128)) return error.IndexSourceProjectionFull;
    for (paths) |path| if (path.len == 0 or path.len > 128) return error.InvalidArgument;
}
fn projectRecord(a: A, reader: anytype, record: *const Record, paths: []const []const []const u8, limits: ProjectionLimits) !?ProjectedSource {
    try validateProjection(paths, limits);
    const effect = record.plan.effects[0];
    const manifest = effect.manifest orelse return null;
    const logical = @import("document_commitment.zig");
    if (manifest.logical_record.len == 0) return null;
    const tree = (try logical.openTree(a, manifest.logical_record, manifest.logical_digest)) orelse return null;
    defer tree.deinit(a);
    if (std.mem.readInt(u64, tree.raw[4..12], .little) != manifest.logical_bytes or tree.raw[44] != effect.key[effect.key.len - 1]) return error.IndexSourceClaimsCorrupt;
    const projection = @import("document_projection.zig");
    var selected: [128]?projection.Directory.Selection = undefined;
    var missing_objects: [128]usize = undefined;
    var requested: [projection.maximum_fields]bool = @splat(false);
    for (paths, selected[0..paths.len], missing_objects[0..paths.len]) |path, *selection, *objects| {
        const location = tree.directory.locate(path);
        selection.* = location.value;
        objects.* = location.objects;
        if (selection.*) |value| requested[value.field.index] = true;
    }
    var covers: [projection.maximum_fields]?u16 = @splat(null);
    var offsets: [projection.maximum_fields]?usize = @splat(null);
    var length: usize = 0;
    for (tree.directory.fields) |field| if (requested[field.index]) {
        var cover = field.index;
        var parent = field.parent;
        while (parent != projection.no_parent) {
            if (requested[parent]) cover = parent;
            parent = tree.directory.fields[parent].parent;
        }
        covers[field.index] = cover;
        if (offsets[cover] == null) {
            offsets[cover] = length;
            length = try std.math.add(usize, length, @intCast(tree.directory.fields[cover].length));
            if (length > limits.bytes) return error.IndexSourceProjectionFull;
        }
        offsets[field.index] = offsets[cover].? + @as(usize, @intCast(field.offset - tree.directory.fields[cover].offset));
    };
    const physical = try a.dupe(u8, effect.key);
    errdefer a.free(physical);
    const values = try a.alloc(ProjectedValue, paths.len);
    errdefer a.free(values);
    const data = try a.alloc(u8, length);
    errdefer a.free(data);
    // A disposable physical scope keeps decoder buffers out of a long-lived
    // query transaction. Results own their final bytes before that scope closes.
    const T = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(reader),
    };
    var filled = false;
    if (comptime @hasDecl(T, "openReadScope")) scoped: {
        var scope = reader.openReadScope(@import("antfly_platform").allocator.reclaimingScratch(a)) catch |err| {
            if (err == error.Unsupported) break :scoped;
            return err;
        };
        defer scope.close();
        if (!scope.supportsDirectInto()) break :scoped;
        fillProjection(manifest, &scope, physical, tree.directory.fields, &covers, &offsets, data) catch |err| {
            if (err == error.Unsupported) break :scoped;
            return err;
        };
        filled = true;
    }
    if (!filled) try fillProjection(manifest, reader, physical, tree.directory.fields, &covers, &offsets, data);
    for (selected[0..paths.len], missing_objects[0..paths.len], values) |selection, objects, *value| {
        value.* = if (selection) |found| .{
            .json = data[offsets[found.field.index].?..][0..@intCast(found.field.length)],
            .covered = found.covered,
            .canonical = tree.canonical[found.field.index],
        } else .{ .json = null, .covered = objects, .canonical = null };
    }
    return .{ .physical = physical, .values = values, .data = data };
}

fn fillProjection(manifest: payload.Manifest, reader: anytype, physical: []const u8, fields: []const @import("document_projection.zig").Field, covers: []const ?u16, offsets: []const ?usize, data: []u8) !void {
    for (fields) |field| if (covers[field.index] == field.index) {
        const offset = offsets[field.index].?;
        const target = data[offset..][0..@intCast(field.length)];
        try manifest.readInto(reader, physical, @intCast(field.offset), target);
        if (!std.mem.eql(u8, &hash(target), &field.digest)) return error.IndexSourceClaimsCorrupt;
    };
}

fn release(a: A, writer: anytype, ref: Reference) !bool {
    _ = (try Header.load(writer, ref)) orelse return false;
    var budget = try Budget.load(writer);
    const count = (try loadCount(writer, ref)) orelse return error.IndexSourceClaimsCorrupt;
    if (count == 1) {
        const record = (try Record.load(a, writer, ref)) orelse return error.IndexSourceClaimMissing;
        defer record.deinit(a);
        const effect = record.plan.effects[0];
        const immutable = try effectValues(a, effect);
        defer a.free(immutable);
        if (immutable.len != 0) {
            const set_key = setKey(ref.document, immutable);
            var retained = (try ValueSet.load(a, writer, set_key)) orelse return error.IndexSourceClaimsCorrupt;
            defer retained.deinit(a);
            retained.count -= 1;
            if (retained.count == 0) {
                budget.value_bytes = try std.math.sub(u64, budget.value_bytes, try ownership.releaseIndexedPin(a, writer, effect.key, retained.values));
                budget.metadata_bytes = try std.math.sub(u64, budget.metadata_bytes, 46 + retained.values.len * 40 + @sizeOf(SetKey));
                try writer.delete(&set_key);
            } else try retained.save(a, writer, set_key);
        }
        budget.metadata_bytes = try std.math.sub(u64, budget.metadata_bytes, record.raw.len + @sizeOf(SnapshotKey) + 44 + @sizeOf(SnapshotRefsKey));
        try writer.delete(&snapshotKey(ref));
        try writer.delete(&snapshotRefsKey(ref));
    } else try saveCount(writer, ref, count - 1);
    budget.metadata_bytes = try std.math.sub(u64, budget.metadata_bytes, 101 + @sizeOf(ClaimKey));
    budget.claims = try std.math.sub(u64, budget.claims, 1);
    try writer.delete(&ref.key());
    try budget.save(writer, .{ .metadata_bytes = std.math.maxInt(u64), .value_bytes = std.math.maxInt(u64), .claims = std.math.maxInt(u64) });
    return true;
}
pub fn cancel(a: A, writer: anytype, ref: Reference, decision: publication.Decision) !bool {
    errdefer writer.poison();
    try decision.require(ref.binding, .cancelled);
    return release(a, writer, ref);
}
pub fn retire(a: A, writer: anytype, ref: Reference, decision: publication.Decision, drained: publication.ReaderPins.Drained) !bool {
    errdefer writer.poison();
    try decision.requireSourceRole();
    try decision.require(ref.binding, .retired);
    try publication.ReaderPins.requireDrained(drained, ref.binding);
    return release(a, writer, ref);
}
/// Charge the publication recovery journal to the same bounded metadata pool.
/// It is staged in the same primary transaction as its source claims.
pub fn changeAuxiliaryMetadata(writer: anytype, added: u64, removed: u64, limits: Limits) !void {
    errdefer writer.poison();
    var budget = try Budget.load(writer);
    budget.metadata_bytes = try std.math.sub(u64, try std.math.add(u64, budget.metadata_bytes, added), removed);
    try budget.save(writer, limits);
}
pub const Address = struct {
    binding: publication.Binding,
    document: Digest,
    snapshot: Digest,
    /// Only acknowledgment uses this header address. Materialization and
    /// release must resolve the authenticated immutable source recipe first.
    pub fn headerReference(self: Address) Reference {
        return .{ .binding = self.binding, .document = self.document, .snapshot = self.snapshot, .revision = .{ .domain = .mutation, .value = 0 }, .root = @splat(0) };
    }
    pub fn metadataBytes(self: Address, reader: anytype) !usize {
        const raw = (try maybe(reader, &snapshotKey(self.headerReference()))) orelse return error.IndexSourceClaimsCorrupt;
        if (raw.len < 86 or raw.len > 16 * 1024 * 1024) return error.IndexSourceClaimsCorrupt;
        return raw.len;
    }
    pub fn resolve(self: Address, a: A, reader: anytype) !Reference {
        const address = self.headerReference();
        _ = (try Header.load(reader, address)) orelse return error.IndexSourceClaimMissing;
        const borrowed = (try maybe(reader, &snapshotKey(address))) orelse return error.IndexSourceClaimsCorrupt;
        if (borrowed.len < 86 or borrowed.len > 16 * 1024 * 1024 or !std.mem.eql(u8, borrowed[0..4], "ISS1") or borrowed[4] > 1 or
            !std.mem.eql(u8, borrowed[borrowed.len - 32 ..], &boundHash(&snapshotKey(address), borrowed[0 .. borrowed.len - 32]))) return error.IndexSourceClaimsCorrupt;
        const raw = try a.dupe(u8, borrowed);
        defer a.free(raw);
        const source_revision: Revision = .{ .domain = @fromBackingInt(@intCast(raw[4])), .value = std.mem.readInt(u64, raw[9..17], .little) };
        const plan = try recipe.Plan.decode(a, raw[5 .. raw.len - 32], source_revision.value);
        defer plan.deinit(a);
        if (plan.effects.len != 1 or plan.effects[0].value == null or !std.mem.eql(u8, &hash(plan.effects[0].key), &self.document) or
            !std.mem.eql(u8, &snapshotId(source_revision, plan.effects[0]), &self.snapshot)) return error.IndexSourceClaimsCorrupt;
        return .{ .binding = self.binding, .document = self.document, .snapshot = self.snapshot, .revision = source_revision, .root = sourceRoot(plan.effects[0]) };
    }
};
pub const AddressPage = struct {
    items: []Address,
    done: bool,
    pub fn deinit(self: *AddressPage, a: A) void {
        a.free(self.items);
        self.* = undefined;
    }
};
/// Fixed-key existence probe for receipt GC. A corrupt key fails closed;
/// no source recipe, value, or header body is loaded.
pub fn bindingHasClaims(reader: anytype, binding: publication.Binding) !bool {
    if (!binding.valid()) return error.InvalidArgument;
    var start: [claim_prefix.len + 64]u8 = undefined;
    @memcpy(start[0..claim_prefix.len], claim_prefix);
    @memcpy(start[claim_prefix.len..][0..32], &binding.owner);
    @memcpy(start[claim_prefix.len + 32 ..], &binding.publication);
    var cursor = try reader.openPhysicalKeyCursorAdapter();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(&start)) orelse return false;
    if (!std.mem.startsWith(u8, row.key, &start)) return false;
    if (row.key.len != @sizeOf(ClaimKey)) return error.IndexSourceClaimsCorrupt;
    return true;
}
/// A physical cursor is required: logical document scans hide this namespace.
/// Each address owns fixed-size identifiers before another read can invalidate
/// the cursor's borrowed row. No source recipe or value is read on this path.
pub fn addressWindow(a: A, reader: anytype, binding: publication.Binding, after: ?Digest, limit: u32) !AddressPage {
    if (limit == 0 or limit > 128 or !binding.valid()) return error.InvalidArgument;
    var start: [claim_prefix.len + 64]u8 = undefined;
    @memcpy(start[0..claim_prefix.len], claim_prefix);
    @memcpy(start[claim_prefix.len..][0..32], &binding.owner);
    @memcpy(start[claim_prefix.len + 32 ..], &binding.publication);
    var seek: ClaimKey = undefined;
    @memcpy(seek[0..start.len], &start);
    @memcpy(seek[start.len..], if (after) |*digest| digest else &@as(Digest, @splat(0)));
    var cursor = try reader.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(if (after == null) &start else &seek);
    if (after != null and row != null and std.mem.eql(u8, row.?.key, &seek)) row = try cursor.next();
    var addresses: std.ArrayList(Address) = .empty;
    errdefer addresses.deinit(a);
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, &start)) break;
        if (addresses.items.len == limit) return .{ .items = try addresses.toOwnedSlice(a), .done = false };
        if (entry.key.len != @sizeOf(ClaimKey) or entry.value.len != 101 or !std.mem.eql(u8, entry.value[0..4], "ISC2") or entry.value[4] > 1 or
            !std.mem.eql(u8, entry.value[69..], &boundHash(entry.key, entry.value[0..69]))) return error.IndexSourceClaimsCorrupt;
        try addresses.append(a, .{ .binding = binding, .document = entry.key[start.len..][0..32].*, .snapshot = entry.value[5..37].* });
        row = try cursor.next();
    }
    return .{ .items = try addresses.toOwnedSlice(a), .done = true };
}
pub fn privatePrefix(candidate: []const u8) ?[]const u8 {
    for ([_][]const u8{ claim_prefix, set_prefix, snapshot_prefix, snapshot_refs_prefix, budget_key, initialized_key, @import("index_source_recovery.zig").prefix }) |prefix| if (std.mem.startsWith(u8, candidate, prefix)) return prefix;
    return null;
}
