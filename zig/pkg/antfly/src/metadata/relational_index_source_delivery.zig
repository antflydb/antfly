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

//! Compact complete-delivery provenance. Only the source receiver may derive
//! this statement from an actual pinned batch and metadata-issued routes.
//! Possession alone does not authorize consumption or schema publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const native = local.storage_db_relational_index_owner_control;
const jobs = local.storage_db_relational_index_owner_job;
const delivery = jobs.private_claims.delivery;
const Source = jobs.private_claims.SourceStream;
const publication = @import("relational_index_publication.zig");
const Digest = [32]u8;
pub const max_parts = jobs.private_claims.max_claims;
// The complete 128-destination typed statement fits even at maximum integer
// and byte-array widths. The wire-footprint test guards this transport bound.
pub const max_wire_bytes = 512 * 1024;

pub const Delivered = struct { index: u16, signed: publication.SignedClaimDelivery };

pub const Targets = struct {
    items: []const Delivered,
    pub fn jsonStringify(self: Targets, jw: anytype) !void {
        try jw.write(self.items);
    }
    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !Targets {
        if (try source.next() != .array_begin) return error.UnexpectedToken;
        var items: std.ArrayList(Delivered) = .empty;
        var limited = options;
        limited.max_value_len = @min(options.max_value_len orelse std.math.maxInt(usize), 128);
        while (try source.peekNextTokenType() != .array_end) {
            if (items.items.len >= max_parts) return error.LengthMismatch;
            try items.append(alloc, try std.json.innerParse(Delivered, alloc, source, limited));
        }
        _ = try source.next();
        return .{ .items = try items.toOwnedSlice(alloc) };
    }
};

pub const Request = struct {
    ordinal: u64,
    batch_digest: Digest,
    targets: Targets,
    pub fn validate(self: Request) !void {
        if (self.ordinal == 0 or std.mem.allEqual(u8, &self.batch_digest, 0) or self.targets.items.len > max_parts) return error.InvalidPrivateIndexClaimReceipt;
        var seen: [max_parts]bool = @splat(false);
        for (self.targets.items) |target| {
            if (target.index >= self.targets.items.len or seen[target.index]) return error.InvalidPrivateIndexClaimReceipt;
            seen[target.index] = true;
            try target.signed.verifyRecorded(max_parts);
        }
    }
};

pub const Part = struct {
    index: u16,
    row_mask: [16]u8,
    rows: u16,
    inventory_xor: Digest,
    target: publication.SignedClaimDelivery,
};

pub const Parts = struct {
    items: []const Part,
    pub fn jsonStringify(self: Parts, jw: anytype) !void {
        try jw.write(self.items);
    }
    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !Parts {
        if (try source.next() != .array_begin) return error.UnexpectedToken;
        var items: std.ArrayList(Part) = .empty;
        var limited = options;
        limited.max_value_len = @min(options.max_value_len orelse std.math.maxInt(usize), 128);
        while (try source.peekNextTokenType() != .array_end) {
            if (items.items.len >= max_parts) return error.LengthMismatch;
            try items.append(alloc, try std.json.innerParse(Part, alloc, source, limited));
        }
        _ = try source.next();
        return .{ .items = try items.toOwnedSlice(alloc) };
    }
};

pub const Data = struct {
    source: jobs.Binding,
    header: Source.Header,
    routes_digest: Digest,
    parts: Parts,

    /// Stable logical acceptance identity, independent of physical re-attestation
    /// and target creation cuts. Current authority still requires digest()/proof
    /// verification; this hash alone never authenticates delivery.
    pub fn logicalDigest(self: Data) !Digest {
        _ = try self.digest();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-logical-delivery/v1");
        state.update(&try self.source.encode());
        state.update(&self.header.digest());
        state.update(&self.routes_digest);
        for (self.parts.items) |part| {
            var index: [2]u8 = undefined;
            std.mem.writeInt(u16, &index, part.index, .little);
            state.update(&index);
            state.update(&part.row_mask);
            state.update(&part.inventory_xor);
            state.update(&try part.target.outcome.binding.encode());
            state.update(&part.target.outcome.claims_digest);
        }
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

    /// Header and complete row-mask checks are independent of response order.
    /// The producer canonicalizes parts by index before hashing/signing.
    pub fn digest(self: Data) !Digest {
        _ = try Source.Header.decode(&self.header.bytes, self.source, std.mem.readInt(u64, self.header.bytes[104..112], .little));
        if (self.source.authority != .native or self.source.phase != .reserved or self.header.bytes[184] == @backingInt(Source.State.failed) or
            std.mem.allEqual(u8, &self.routes_digest, 0) or self.parts.items.len > max_parts) return error.InvalidPrivateIndexClaimReceipt;
        const rows = std.mem.readInt(u32, self.header.bytes[96..100], .little);
        var mask: u128 = 0;
        var destinations: [local.storage_db_relational_index_publication_progress.max_owners]bool = @splat(false);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-source-delivery/v1");
        state.update(&try self.source.encode());
        state.update(&self.header.digest());
        state.update(&self.routes_digest);
        for (self.parts.items, 0..) |part, i| {
            const target = part.target.outcome.binding;
            const bits = std.mem.readInt(u128, &part.row_mask, .little);
            if (part.index != i or part.rows == 0 or part.rows != @popCount(bits) or mask & bits != 0 or
                target.authority != .native or target.phase != .reserved or !std.mem.eql(u8, &target.id, &self.source.id) or
                !std.mem.eql(u8, &target.plan_digest, &self.source.plan_digest) or target.fence.namespace.table_id != self.source.fence.namespace.table_id or
                !std.mem.eql(u8, &target.inputs_digest, &self.source.inputs_digest) or !std.mem.eql(u8, &target.schema_before, &self.source.schema_before) or
                !std.mem.eql(u8, &target.integrity_candidate, &self.source.integrity_candidate) or !std.mem.eql(u8, &target.integrity_reserved, &self.source.integrity_reserved) or
                target.physical_candidate.schema_version != self.source.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimReceipt;
            mask |= bits;
            try part.target.validateRecordedShape(part.rows);
            if (destinations[target.ordinal]) return error.InvalidPrivateIndexClaimReceipt;
            destinations[target.ordinal] = true;
            var counts: [4]u8 = undefined;
            std.mem.writeInt(u16, counts[0..2], part.index, .little);
            std.mem.writeInt(u16, counts[2..4], part.rows, .little);
            state.update(&counts);
            state.update(&part.row_mask);
            state.update(&part.inventory_xor);
            state.update(&try part.target.commitment());
        }
        const expected: u128 = if (rows == 128) std.math.maxInt(u128) else (@as(u128, 1) << @as(u7, @intCast(rows))) - 1;
        if (mask != expected) return error.InvalidPrivateIndexClaimReceipt;
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
};

pub const Owned = struct {
    alloc: std.mem.Allocator,
    data: Data,
    pub fn deinit(self: *Owned) void {
        self.alloc.free(self.data.parts.items);
        self.* = undefined;
    }
};

pub fn prepare(alloc: std.mem.Allocator, read: *const native.SourceRead, routes: *const delivery.Subset, request: Request, context: local.api_operation.RequestContext) !Owned {
    try context.ensureActive();
    try request.validate();
    const batch = if (read.batch) |*value| value else return error.InvalidPrivateIndexClaimReceipt;
    if (read.replication_pending or read.error_name != null or request.ordinal != std.mem.readInt(u64, batch.header.bytes[104..112], .little) or
        !std.mem.eql(u8, &request.batch_digest, &batch.header.digest())) return error.InvalidPrivateIndexClaimReceipt;
    const plan = try delivery.Plan.prepareControlled(routes, read.binding, delivery.View.fromBatch(batch), context);
    var indices: [max_parts]u16 = undefined;
    for (request.targets.items, indices[0..request.targets.items.len]) |target, *index| index.* = target.index;
    _ = try plan.coverage(indices[0..request.targets.items.len]);
    const parts = try alloc.alloc(Part, plan.group_count);
    errdefer alloc.free(parts);
    var used: [max_parts]bool = @splat(false);
    for (request.targets.items) |target| {
        try context.ensureActive();
        try target.signed.verifyPartition(&plan, routes, target.index);
        const group = plan.groups[target.index];
        used[group.target_index] = true;
        var inventory: Digest = @splat(0);
        for (try plan.groupClaims(target.index)) |claim| {
            const member = native.ClaimCommand.memberDigest(claim);
            for (&inventory, member) |*byte, contribution| byte.* ^= contribution;
        }
        parts[target.index] = .{ .index = target.index, .row_mask = group.row_mask, .rows = group.count, .inventory_xor = inventory, .target = target.signed };
    }
    for (used[0..routes.targets.len]) |present| if (!present) return error.InvalidPrivateIndexClaimReceipt;
    const data: Data = .{ .source = read.binding, .header = batch.header, .routes_digest = routes.digest, .parts = .{ .items = parts } };
    _ = try data.digest();
    return .{ .alloc = alloc, .data = data };
}

pub const Signed = struct {
    data: Data,
    proof: publication.RootProof,
    pub fn verify(self: Signed) !void {
        if (self.proof.authority != .native) return error.InvalidPrivateIndexClaimReceipt;
        (publication.NativeSigningView{ .identity = self.proof.identity, .reporter_incarnation = self.proof.reporter_incarnation, .binding_digest = self.proof.authority.native }).validate(self.proof.identity.metadata_incarnation) catch return error.InvalidPrivateIndexClaimReceipt;
        self.proof.verify(try self.data.digest()) catch return error.InvalidPrivateIndexClaimReceipt;
    }
};
