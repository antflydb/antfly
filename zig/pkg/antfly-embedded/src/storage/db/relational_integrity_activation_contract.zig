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

/// Selected root datums and null provenance from one immutable primary-row
/// observation. Coordinators authenticate markers against their pinned schema
/// before evaluation; JSON alone cannot carry the original SQL null bits.
pub const Row = struct {
    key: []const u8,
    json: []const u8,
    version: u64,
    expected_content_digest: ?[32]u8 = null,
    json_null_fields: []const []const u8 = &.{},

    pub fn nativeJsonSkipField(row: @This(), comptime name: []const u8) bool {
        return std.mem.eql(u8, name, "json_null_fields") and row.json_null_fields.len == 0;
    }
};

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
            (self.state == .enforced and (self.cursor.len != 0 or self.failure.len != 0))) return error.InvalidConstraintActivation;
        const out = try alloc.alloc(u8, header_len + self.cursor.len + self.failure.len + 32);
        @memcpy(out[0..4], "AIA1");
        @memcpy(out[4..36], &self.generation_set);
        @memcpy(out[36..68], &self.owner);
        std.mem.writeInt(u32, out[68..72], self.schema_version, .little);
        out[72] = @backingInt(self.state);
        out[73] = @backingInt(self.phase);
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
            .phase = @fromBackingInt(bytes[73]),
            .rows_scanned = std.mem.readInt(u64, bytes[76..84], .little),
            .cursor = payload[0..cursor_len],
            .failure = payload[cursor_len..],
        };
        if (result.state == .enforced and (result.cursor.len != 0 or result.failure.len != 0)) return error.InvalidConstraintActivation;
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
    /// Guarded, retryable diagnostic; never advances source coverage.
    diagnostic: bool = false,
    validation: ?ForeignKeyValidationScope = null,
    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "validation") and self.validation == null;
    }
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};

/// Private semantic validation shares activation's 2PC transport and progress
/// shape, but never resets the table's already-enforced physical coverage.
pub const validation_key = "\x00\x00__metadata__:schema_validation_job:foreign_key_progress";
pub const validation_inputs_key = validation_key ++ ":inputs";
const owner_contract = @import("relational_schema_owner_contract.zig");
/// Stream base64 at admission rather than expanding schema-sized inputs into
/// JSON integer arrays. Parsing owns decoded bytes in the request's JSON arena.
pub const ValidationInputs = struct {
    bytes: []const u8 = "",

    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginWriteRaw();
        try stream.writer.writeByte('"');
        var encoded: [4096]u8 = undefined;
        var offset: usize = 0;
        while (offset < self.bytes.len) {
            const size = @min(3072, self.bytes.len - offset);
            try stream.writer.writeAll(std.base64.standard.Encoder.encode(&encoded, self.bytes[offset..][0..size]));
            offset += size;
        }
        try stream.writer.writeByte('"');
        stream.endWriteRaw();
    }
    pub fn nativeJsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        return self.jsonStringify(stream);
    }
    fn decode(alloc: Allocator, text: []const u8) !@This() {
        if (text.len % 4 != 0 or text.len > std.base64.standard.Encoder.calcSize(owner_contract.max_input_bytes)) return error.InvalidSchemaValidationJob;
        const size = std.base64.standard.Decoder.calcSizeForSlice(text) catch return error.InvalidSchemaValidationJob;
        if (size > owner_contract.max_input_bytes) return error.InvalidSchemaValidationJob;
        const bytes = try alloc.alloc(u8, size);
        errdefer alloc.free(bytes);
        std.base64.standard.Decoder.decode(bytes, text) catch return error.InvalidSchemaValidationJob;
        if (size % 3 != 0) {
            var tail: [4]u8 = undefined;
            const canonical = std.base64.standard.Encoder.encode(&tail, bytes[size - size % 3 ..]);
            if (!std.mem.eql(u8, canonical, text[text.len - 4 ..])) return error.InvalidSchemaValidationJob;
        }
        return .{ .bytes = bytes };
    }
    pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        return decode(alloc, try std.json.innerParse([]const u8, alloc, source, options)) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidCharacter,
        };
    }
    pub fn jsonParseFromValue(alloc: Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return decode(alloc, try std.json.innerParseFromValue([]const u8, alloc, value, options)) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidCharacter,
        };
    }
};

pub const ForeignKeyValidationScope = struct {
    binding: owner_contract.Binding,
    physical_root: u128,
    action: enum { begin, page, cancel },
    inputs: ValidationInputs = .{},

    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        if (self.binding.purpose != .foreign_key_source or ((self.binding.authority == .native) != (self.physical_root != 0)) or
            ((self.action == .begin) != (self.inputs.bytes.len != 0)) or self.inputs.bytes.len > owner_contract.max_input_bytes) return error.InvalidSchemaValidationJob;
        if (self.action == .begin and !std.mem.eql(u8, &digest(self.inputs.bytes), &self.binding.inputs)) return error.SchemaValidationJobChanged;
    }
    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "inputs") and self.inputs.bytes.len == 0;
    }
};

/// One bounded slot per owner. Immutable inputs live beside it; every cursor
/// CAS is an ordinary transaction intent, joined to the actual parent effects.
/// Native progress binds its physical root. Raft progress uses root zero so
/// replicas apply identical bytes; current-root readiness is qualified by the
/// receiving read-index/receipt path. Neither grants snapshot import authority.
pub const ForeignKeyValidationRecord = struct {
    binding: owner_contract.Binding,
    physical_root: u128,
    revision: u64,
    progress: Progress,
    pub const max_wire_bytes = 72 + owner_contract.max_header + header_len + max_cursor_bytes + 4096 + 32;

    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        return @import("relational_integrity_json.zig").write(self, stream);
    }

    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        if (self.binding.purpose != .foreign_key_source or ((self.binding.authority == .native) != (self.physical_root != 0)) or self.revision == 0 or
            self.progress.schema_version != self.binding.before_version or self.progress.phase != .foreign_key or
            !std.mem.eql(u8, &self.progress.owner, &self.binding.owner) or std.mem.allEqual(u8, &self.progress.generation_set, 0) or
            self.progress.cursor.len > max_cursor_bytes or self.progress.failure.len > 4096 or
            (self.progress.state == .invalid and self.progress.failure.len == 0) or
            (self.progress.state != .invalid and self.progress.failure.len != 0) or
            (self.progress.state == .enforced and self.progress.cursor.len != 0)) return error.InvalidSchemaValidationJob;
    }
    pub fn encode(self: @This(), alloc: Allocator) ![]u8 {
        try self.validate();
        var bound_bytes: [owner_contract.max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&bound_bytes);
        try std.json.Stringify.value(self.binding, .{}, &writer);
        const bound = writer.buffered();
        const progress = try self.progress.encode(alloc);
        defer alloc.free(progress);
        const bytes = try alloc.alloc(u8, 72 + bound.len + progress.len);
        @memcpy(bytes[0..8], "AFKV\x01\x00\x00\x00");
        std.mem.writeInt(u128, bytes[8..24], self.physical_root, .little);
        std.mem.writeInt(u64, bytes[24..32], self.revision, .little);
        std.mem.writeInt(u32, bytes[32..36], @intCast(bound.len), .little);
        std.mem.writeInt(u32, bytes[36..40], @intCast(progress.len), .little);
        @memcpy(bytes[40..][0..bound.len], bound);
        @memcpy(bytes[40 + bound.len ..][0..progress.len], progress);
        @memcpy(bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]));
        return bytes;
    }
    /// Only progress cursor/diagnostic slices borrow the checked source frame.
    /// Binding and root identities are fixed owned values after stack parsing.
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len < 72 or bytes.len > max_wire_bytes or !std.mem.eql(u8, bytes[0..8], "AFKV\x01\x00\x00\x00") or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidSchemaValidationJob;
        const bound_len: usize = std.mem.readInt(u32, bytes[32..36], .little);
        const progress_len: usize = std.mem.readInt(u32, bytes[36..40], .little);
        if (bound_len == 0 or bound_len > owner_contract.max_header or bound_len > bytes.len - 72 or
            progress_len < header_len + 32 or progress_len != bytes.len - 72 - bound_len) return error.InvalidSchemaValidationJob;
        var scratch: [64 * 1024]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&scratch);
        const binding = try std.json.parseFromSliceLeaky(owner_contract.Binding, fixed.allocator(), bytes[40..][0..bound_len], .{ .ignore_unknown_fields = false });
        var canonical: [owner_contract.max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(binding, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[40..][0..bound_len])) return error.InvalidSchemaValidationJob;
        const result: @This() = .{ .binding = binding, .physical_root = std.mem.readInt(u128, bytes[8..24], .little), .revision = std.mem.readInt(u64, bytes[24..32], .little), .progress = try Progress.decode(bytes[40 + bound_len ..][0..progress_len]) };
        try result.validate();
        return result;
    }
};

test "foreign key validation input transport streams compact canonical base64 across block boundaries" {
    const a = std.testing.allocator;
    for ([_]usize{ 0, 1, 2, 3, 3072, 3073, 6145 }) |size| {
        const raw = try a.alloc(u8, size);
        defer a.free(raw);
        for (raw, 0..) |*byte, i| byte.* = @truncate(i);
        const encoded = try std.json.Stringify.valueAlloc(a, ValidationInputs{ .bytes = raw }, .{});
        defer a.free(encoded);
        try std.testing.expectEqual(2 + std.base64.standard.Encoder.calcSize(size), encoded.len);
        var parsed = try std.json.parseFromSlice(ValidationInputs, a, encoded, .{});
        defer parsed.deinit();
        try std.testing.expectEqualSlices(u8, raw, parsed.value.bytes);
        var tree = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
        defer tree.deinit();
        var decoded = try std.json.parseFromValue(ValidationInputs, a, tree.value, .{});
        defer decoded.deinit();
        try std.testing.expectEqualSlices(u8, raw, decoded.value.bytes);
        const Native = struct {
            value: ValidationInputs,
            pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
                return @import("relational_integrity_json.zig").write(self.value, stream);
            }
        };
        const native = try std.json.Stringify.valueAlloc(a, Native{ .value = .{ .bytes = raw } }, .{});
        defer a.free(native);
        try std.testing.expectEqualSlices(u8, encoded, native);
    }
    for ([_][]const u8{ "\"AB==\"", "\"AAB=\"", "\"AQ\"", "\"!!!!\"" }) |bad| {
        try std.testing.expectError(error.InvalidCharacter, std.json.parseFromSlice(ValidationInputs, a, bad, .{}));
    }
}

pub fn commandProgress(command: Command, bytes: []const u8) !Progress {
    return if (command.validation != null) (try ForeignKeyValidationRecord.decode(bytes)).progress else Progress.decode(bytes);
}

/// A failed page retains its predecessor's coverage. Only the enclosing 2PC
/// row/claim guards can publish the proposed diagnostic and next revision.
pub fn encodeCommandProgress(alloc: Allocator, command: Command, progress: Progress) ![]u8 {
    if (command.validation != null) {
        var record = try ForeignKeyValidationRecord.decode(command.expected orelse return error.ConstraintActivationChanged);
        record.revision = try std.math.add(u64, record.revision, 1);
        record.progress = progress;
        return record.encode(alloc);
    }
    return progress.encode(alloc);
}
