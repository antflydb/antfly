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

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const integrity = @import("relational_integrity_contract.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");

pub const key = "\x00\x00__metadata__:relational_integrity_activation";

pub const header_len = 88;

pub const max_cursor_bytes = 1024 * 1024;

pub fn digest(bytes: []const u8) integrity.Digest {
    var result: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn generationSet(catalog: catalog_mod.Catalog) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly active constraint coverage v1");
    state.update(&catalog.incarnation);
    state.update(&catalog.checks_digest);
    for (catalog.bindings) |binding| if (!binding.retired) {
        state.update(&binding.generation);
        state.update(&binding.definition.fingerprint);
    };
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub fn hasActive(catalog: catalog_mod.Catalog) bool {
    if (hasChecks(catalog)) return true;
    for (catalog.bindings) |binding| if (!binding.retired) return true;
    return false;
}

pub fn hasChecks(catalog: catalog_mod.Catalog) bool {
    return !std.mem.allEqual(u8, &catalog.checks_digest, 0);
}

pub fn firstPhase(catalog: catalog_mod.Catalog) Phase {
    return if (hasKind(catalog, .unique)) .unique else if (hasKind(catalog, .foreign_key)) .foreign_key else .check;
}

pub fn nextPhase(catalog: catalog_mod.Catalog, phase: Phase) ?Phase {
    if (phase == .unique and hasKind(catalog, .foreign_key)) return .foreign_key;
    if (phase != .check and hasChecks(catalog)) return .check;
    return null;
}

pub fn hasKind(catalog: catalog_mod.Catalog, kind: catalog_mod.Kind) bool {
    for (catalog.bindings) |binding| if (!binding.retired and binding.definition.kind == kind) return true;
    return false;
}

pub const State = enum(u8) { validating = 0, enforced = 1, invalid = 2 };

pub const Phase = enum(u8) { unique = 0, foreign_key = 1, check = 2 };

pub const Progress = struct {
    generation_set: integrity.Digest,
    owner: integrity.Digest,
    schema_version: u32,
    state: State = .validating,
    phase: Phase = .unique,
    rows_scanned: u64 = 0,
    /// Physical primary-namespace continuation includes skipped auxiliary
    /// records, preventing empty projected pages from repeatedly rescanning.
    cursor: []const u8 = "",
    failure: []const u8 = "",

    pub fn readyForReferences(self: Progress) bool {
        return self.state == .enforced or self.phase != .unique;
    }

    pub fn retry(self: Progress, catalog: catalog_mod.Catalog) !Progress {
        if (self.state != .invalid) return error.InvalidConstraintActivation;
        return .{ .generation_set = generationSet(catalog), .owner = self.owner, .schema_version = catalog.schema_version, .phase = firstPhase(catalog) };
    }

    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (self.cursor.len > max_cursor_bytes or self.failure.len > 4096 or
            (self.state == .enforced and self.cursor.len != 0) or (self.state != .invalid and self.failure.len != 0)) return error.InvalidConstraintActivation;
        const out = try alloc.alloc(u8, header_len + self.cursor.len + self.failure.len + 32);
        @memcpy(out[0..4], "AIA1");
        @memcpy(out[4..36], &self.generation_set);
        @memcpy(out[36..68], &self.owner);
        std.mem.writeInt(u32, out[68..72], self.schema_version, .little);
        out[72] = @intFromEnum(self.state);
        out[73] = @intFromEnum(self.phase);
        @memset(out[74..76], 0);
        std.mem.writeInt(u64, out[76..84], self.rows_scanned, .little);
        std.mem.writeInt(u32, out[84..88], @intCast(self.cursor.len), .little);
        // The bounded footer remainder is the diagnostic; no duplicate size.
        @memcpy(out[header_len..][0..self.cursor.len], self.cursor);
        @memcpy(out[header_len + self.cursor.len ..][0..self.failure.len], self.failure);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }

    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len < header_len + 32 or bytes.len > header_len + max_cursor_bytes + 4096 + 32 or
            !std.mem.eql(u8, bytes[0..4], "AIA1") or !std.mem.allEqual(u8, bytes[74..76], 0) or bytes[72] > 2 or bytes[73] > 2 or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidConstraintActivation;
        const cursor_len = std.mem.readInt(u32, bytes[84..88], .little);
        const payload = bytes[header_len .. bytes.len - 32];
        if (cursor_len > max_cursor_bytes or cursor_len > payload.len or payload.len - cursor_len > 4096) return error.InvalidConstraintActivation;
        const result: Progress = .{
            .generation_set = bytes[4..36].*,
            .owner = bytes[36..68].*,
            .schema_version = std.mem.readInt(u32, bytes[68..72], .little),
            .state = switch (bytes[72]) {
                0 => .validating,
                1 => .enforced,
                2 => .invalid,
                else => unreachable,
            },
            .phase = @enumFromInt(bytes[73]),
            .rows_scanned = std.mem.readInt(u64, bytes[76..84], .little),
            .cursor = payload[0..cursor_len],
            .failure = payload[cursor_len..],
        };
        if ((result.state == .enforced and result.cursor.len != 0) or (result.state != .invalid and result.failure.len != 0)) return error.InvalidConstraintActivation;
        return result;
    }

    pub fn matches(self: Progress, catalog: catalog_mod.Catalog, owner: integrity.Digest) bool {
        return std.mem.eql(u8, &self.generation_set, &generationSet(catalog)) and std.mem.eql(u8, &self.owner, &owner);
    }
};

pub const Command = struct {
    routing_key: []const u8,
    expected: ?[]const u8,
    next: []const u8,
    retry: bool = false,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
