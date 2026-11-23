// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data-only identities for native frozen-generation images. Control owners
//! may bind these without importing the filesystem or physical snapshot engine.
const std = @import("std");
const staging = @import("../../restore_staging_contract.zig");
const Namespace = @import("../../doc_identity_namespace.zig").Namespace;
pub fn importPathAlloc(a: std.mem.Allocator, current_path: []const u8, scope: [32]u8) ![]u8 {
    if (std.mem.allEqual(u8, &scope, 0)) return error.InvalidRelationPopulation;
    return std.fmt.allocPrint(a, "{s}.frozen-import-{s}", .{ current_path, std.fmt.bytesToHex(scope, .lower) });
}

/// A transferred archive lives inside the unpublished primary root so it
/// becomes visible and retires with the same physical generation.
pub fn installedPathAlloc(a: std.mem.Allocator, primary: []const u8, scope: [32]u8) ![]u8 {
    if (std.mem.allEqual(u8, &scope, 0)) return error.InvalidRelationPopulation;
    return std.fmt.allocPrint(a, "{s}/frozen-archive-{s}", .{ primary, std.fmt.bytesToHex(scope, .lower) });
}

pub const Binding = struct {
    scope: [32]u8,
    population: [32]u8,
    content: [32]u8,
    rows: u64,
    namespace: Namespace,

    /// Derive the logical receipt identity from the sealed plan's owner scope.
    /// This projection carries no proof that the writer completed its import.
    pub fn fromScope(scope: staging.Scope) !Binding {
        try scope.validate();
        const source = scope.population orelse return error.InvalidRelationPopulation;
        const rows = source.transaction_rows orelse return error.InvalidRelationPopulation;
        if (rows.image != .frozen) return error.InvalidRelationPopulation;
        return .{ .scope = scope.digest(), .population = try source.digest(), .content = rows.content, .rows = rows.rows, .namespace = scope.target_namespace };
    }

    /// The accepting native writer supplies its actual persisted progress.
    /// Incomplete, ordinary restore, canceled and published owners cannot mint
    /// a new initial image from their later mutable primary state.
    pub fn fromProgress(progress: staging.Progress) !Binding {
        const expected = try fromScope(progress.scope);
        const source = progress.scope.population.?;
        const rows = source.transaction_rows.?;
        // Reservation legitimately has no row cursor yet. Refuse every
        // incomplete phase before interpreting the sealed source receipt.
        // Validation may commit before the capture receipt is recovered.
        // Both phases retain the sealed source and prohibit ordinary writes.
        if ((progress.phase != .imported and progress.phase != .validated) or
            !progress.rows_complete or !progress.artifacts_complete or
            !progress.source_generation_proofs_complete) return error.RelationPopulationChanged;
        const cursor = progress.population orelse return error.InvalidRelationPopulation;
        try cursor.validate();
        if (!cursor.sealed or cursor.rows != rows.rows or
            !std.meta.eql(cursor.transaction_rows, source.transaction_rows) or
            progress.rows != cursor.rows or !std.mem.eql(u8, &cursor.binding, &expected.population) or
            !std.mem.eql(u8, &cursor.chain, &progress.logical_digest) or cursor.content == null or
            !std.mem.eql(u8, &cursor.content.?, &rows.content)) return error.RelationPopulationChanged;
        return expected;
    }
    pub fn validate(self: Binding) !void {
        if (self.namespace.table_id == 0 or std.mem.allEqual(u8, &self.scope, 0) or
            std.mem.allEqual(u8, &self.population, 0) or std.mem.allEqual(u8, &self.content, 0)) return error.InvalidRelationPopulation;
    }
    pub fn digest(self: Binding) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-frozen-image/v1\x00");
        hash.update(&self.scope);
        hash.update(&self.population);
        hash.update(&self.content);
        var number: [8]u8 = undefined;
        for ([_]u64{ self.rows, self.namespace.table_id, self.namespace.shard_id, self.namespace.range_id }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn id(self: Binding) ![64]u8 {
        return std.fmt.bytesToHex(try self.digest(), .lower);
    }
    pub fn requireProgress(self: Binding, progress: staging.Progress) !void {
        if (!std.meta.eql(self, try fromProgress(progress))) return error.RelationPopulationChanged;
    }
};
pub const Handle = struct {
    binding: Binding,
    physical: [32]u8,

    pub fn validate(self: Handle) !void {
        try self.binding.validate();
        if (std.mem.allEqual(u8, &self.physical, 0)) return error.InvalidRelationPopulation;
    }

    pub fn requireScope(self: Handle, scope: staging.Scope) !void {
        try self.validate();
        if (!std.meta.eql(self.binding, try Binding.fromScope(scope))) return error.RelationPopulationChanged;
    }
};
