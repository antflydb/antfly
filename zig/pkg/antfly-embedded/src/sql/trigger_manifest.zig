// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Statement-pinned executable data for table triggers. This format contains no
//! callbacks, credentials or ambient lookup capability. Its canonical digest
//! identifies data; the existing private mutation receiver must separately
//! authenticate the statement authority and match its real immutable schema.
const std = @import("std");
const attachments = @import("../system_catalog/triggers.zig");
const routines = @import("../system_catalog/routines.zig");
const A = std.mem.Allocator;
pub const max_bytes = 4 * 1024 * 1024;
pub const max_routines = 256;
pub const max_lookups = 1024;
pub const Digest = [32]u8;

pub const Table = struct {
    id: u64,
    schema_version: u32,
    schema_digest: Digest,
    attachment_generation: u64,
    attachment_digest: Digest,
    pub fn validate(self: Table) !void {
        if (self.id == 0 or std.mem.allEqual(u8, &self.schema_digest, 0) or std.mem.allEqual(u8, &self.attachment_digest, 0)) return error.InvalidSqlTriggerManifest;
    }
    pub fn fingerprint(self: Table) !Digest {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-trigger-preparation/table/v1\x00");
        var integers: [20]u8 = undefined;
        std.mem.writeInt(u64, integers[0..8], self.id, .little);
        std.mem.writeInt(u32, integers[8..12], self.schema_version, .little);
        std.mem.writeInt(u64, integers[12..20], self.attachment_generation, .little);
        hash.update(&integers);
        hash.update(&self.schema_digest);
        hash.update(&self.attachment_digest);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
    pub fn eql(self: Table, other: Table) bool {
        return std.meta.eql(self, other);
    }
};
pub const Routine = struct {
    /// Authoritative current labels accompany immutable IDs, including after
    /// function/namespace rename. Attachment labels never retarget an ID.
    reference: routines.RoutineReference,
    record: routines.Record,
    pub fn validate(self: Routine) !void {
        try self.reference.validate();
        try self.record.validate();
        if (self.reference.id != self.record.id or (self.record.kind != .function and self.record.kind != .operator)) return error.InvalidSqlTriggerManifest;
    }
};
pub const Lookup = struct {
    /// Compiler-qualified spelling with internal NUL separators. This is a
    /// captured resolver result, not a search_path to execute at the receiver.
    name: []const u8,
    routine_id: u64,
    /// Current namespace from the captured metadata lookup proof, not the
    /// attachment's creation-time display labels.
    namespace_id: u64,
};
pub const Clock = struct {
    transaction_micros: i64,
    statement_micros: i64,
    pub fn validate(self: Clock) !void {
        const Timestamp = @import("temporal_value.zig").Timestamp;
        const transaction = try Timestamp.init(self.transaction_micros);
        const statement = try Timestamp.init(self.statement_micros);
        if (transaction.infinity() != 0 or statement.infinity() != 0) return error.InvalidTransactionClock;
    }
};
/// A positive same-cut witness that the table has no trigger state. This is
/// not an invented empty attachment generation or an executable program.
pub const Absence = struct {
    metadata_group_id: u64,
    incarnation: [16]u8,
    source_revision: u64,
    table_id: u64,
    schema_version: u32,
    schema_digest: Digest,
    /// False attests actual native schema absence, not a synthetic version0 view.
    schema_present: bool = true,
    pub fn fromAdmission(admission: @import("../storage/db/prepared_graph_effects.zig").Admission, schema_version: u32) !Absence {
        try admission.validate();
        return .{ .metadata_group_id = admission.metadata_group_id, .incarnation = admission.incarnation, .source_revision = admission.source_revision, .table_id = admission.table_id, .schema_version = schema_version, .schema_digest = admission.schema_digest };
    }
    pub fn validate(self: Absence) !void {
        if (self.metadata_group_id == 0 or self.table_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.schema_digest, 0)) return error.InvalidSqlTriggerManifest;
        if (!self.schema_present) {
            const empty = @import("../storage/db/relational_schema_owner_contract.zig").hash("");
            if (self.schema_version != 0 or !std.mem.eql(u8, &self.schema_digest, &empty)) return error.InvalidSqlTriggerManifest;
        }
    }
    pub fn table(self: Absence) !Table {
        try self.validate();
        return .{ .id = self.table_id, .schema_version = self.schema_version, .schema_digest = self.schema_digest, .attachment_generation = 0, .attachment_digest = try canonicalDigest("antfly/sql-trigger-absence/v1\x00", self) };
    }
    pub fn requireAdmission(self: Absence, admission: @import("../storage/db/prepared_graph_effects.zig").Admission) !void {
        var expected = try fromAdmission(admission, self.schema_version);
        expected.schema_present = self.schema_present;
        if (!self.schema_present) try self.requireNoSchema();
        if (!std.meta.eql(self, expected)) return error.CatalogGenerationChanged;
    }
    pub fn jsonStringify(self: Absence, writer: anytype) !void {
        if (!self.schema_present) return writer.write(.{ .metadata_group_id = self.metadata_group_id, .incarnation = self.incarnation, .source_revision = self.source_revision, .table_id = self.table_id, .schema_version = self.schema_version, .schema_digest = self.schema_digest, .schema_present = false });
        try writer.write(.{ .metadata_group_id = self.metadata_group_id, .incarnation = self.incarnation, .source_revision = self.source_revision, .table_id = self.table_id, .schema_version = self.schema_version, .schema_digest = self.schema_digest });
    }
    pub fn requireNoSchema(self: Absence) !void {
        try self.validate();
        const empty = @import("../storage/db/relational_schema_owner_contract.zig").hash("");
        if (self.schema_present or self.schema_version != 0 or !std.mem.eql(u8, &self.schema_digest, &empty)) return error.CatalogGenerationChanged;
    }
    pub fn requireView(self: Absence, view: anytype) !void {
        try self.validate();
        if (!self.schema_present) return error.CatalogGenerationChanged;
        if (view.triggerState() != null or view.version() != self.schema_version) return error.CatalogGenerationChanged;
        const validator = view.validator() orelse return error.CatalogGenerationChanged;
        const digest = validator.source_digest orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &self.schema_digest, &digest)) return error.CatalogGenerationChanged;
    }
};

pub const Manifest = struct {
    format: u8 = 1,
    metadata_incarnation: [16]u8,
    catalog_revision: u64,
    table: Table,
    attachments: ?attachments.State = null,
    absence: ?Absence = null,
    /// Sorted by immutable routine ID. Replacement changes record.generation
    /// and the digest, while existing statement owners keep their pinned body.
    routines: []const Routine,
    /// Sorted by (qualified spelling, routine ID), retaining all captured
    /// overloads without granting access to ambient catalog/name resolution.
    lookups: []const Lookup = &.{},
    clock: ?Clock = null,

    pub fn validate(self: Manifest, a: A) !void {
        if ((self.format != 1 and self.format != 2) or self.catalog_revision == 0 or std.mem.allEqual(u8, &self.metadata_incarnation, 0) or self.routines.len > max_routines or self.lookups.len > max_lookups) return error.InvalidSqlTriggerManifest;
        try self.table.validate();
        if (self.absence) |absent| {
            try absent.validate();
            if (self.format != 2 or self.attachments != null or self.routines.len != 0 or self.lookups.len != 0 or !std.mem.eql(u8, &self.metadata_incarnation, &absent.incarnation) or !self.table.eql(try absent.table())) return error.InvalidSqlTriggerManifest;
        } else {
            const state = self.attachments orelse return error.InvalidSqlTriggerManifest;
            if (self.format != 1) return error.InvalidSqlTriggerManifest;
            try state.validate();
            if (!std.mem.eql(u8, &self.metadata_incarnation, &state.metadata_incarnation) or self.table.id != state.table_id or self.table.attachment_generation != state.generation or !std.mem.eql(u8, &self.table.attachment_digest, &try attachmentDigest(state))) return error.InvalidSqlTriggerManifest;
        }
        if (self.clock) |clock| try clock.validate();
        for (self.routines, 0..) |routine, i| {
            try routine.validate();
            if (i != 0 and self.routines[i - 1].record.id >= routine.record.id) return error.InvalidSqlTriggerManifest;
        }
        // Binding is by stable routine ID. A function SET SCHEMA must not
        // require rewriting every attachment's creation-time namespace label.
        for (self.attachmentEntries()) |entry| {
            const routine = self.findRoutine(entry.function.id) orelse return error.InvalidSqlTriggerManifest;
            if (routine.record.kind != .function or !routine.record.definition.returns_trigger) return error.InvalidSqlTriggerManifest;
        }
        for (self.lookups, 0..) |lookup, i| {
            const captured = self.findRoutine(lookup.routine_id) orelse return error.InvalidSqlTriggerManifest;
            if (lookup.name.len == 0 or lookup.name.len > 1024 or !std.unicode.utf8ValidateSlice(lookup.name) or lookup.namespace_id != captured.reference.namespace_id) return error.InvalidSqlTriggerManifest;
            if (i != 0) {
                const previous = self.lookups[i - 1];
                const order = std.mem.order(u8, previous.name, lookup.name);
                if (order == .gt or (order == .eq and previous.routine_id >= lookup.routine_id)) return error.InvalidSqlTriggerManifest;
            }
        }
        // References embedded in operator/function definitions are part of
        // this same closure. Missing or substituted helper functions fail
        // before the first row effect, rather than invoking ambient lookup.
        for (self.routines) |routine| {
            const uses = try routine.record.functionUsesAlloc(a);
            defer routines.Record.freeFunctionUses(a, uses);
            for (uses) |use| {
                const helper = self.findRoutine(use.reference.id) orelse return error.InvalidSqlTriggerManifest;
                if (helper.record.kind != .function or helper.record.definition.returns_trigger or !std.meta.eql(helper.record.definition.result, use.result) or helper.record.definition.arguments.len != use.arguments.len) return error.InvalidSqlTriggerManifest;
                for (helper.record.definition.arguments, use.arguments) |argument, expected| if (!std.meta.eql(argument.type, expected)) return error.InvalidSqlTriggerManifest;
            }
        }
        try requireEncodedBound(self);
    }
    pub fn findRoutine(self: Manifest, id: u64) ?Routine {
        var low: usize = 0;
        var high = self.routines.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const actual = self.routines[mid].record.id;
            if (actual == id) return self.routines[mid];
            if (actual < id) low = mid + 1 else high = mid;
        }
        return null;
    }
    pub fn attachmentEntries(self: Manifest) []const attachments.Entry {
        return if (self.attachments) |state| state.entries else &.{};
    }
    /// Existing present-state programs retain their exact canonical JSON.
    pub fn jsonStringify(self: Manifest, writer: anytype) !void {
        if (self.absence) |absent| return writer.write(.{ .format = self.format, .metadata_incarnation = self.metadata_incarnation, .catalog_revision = self.catalog_revision, .table = self.table, .absence = absent, .routines = self.routines, .lookups = self.lookups, .clock = self.clock });
        try writer.write(.{ .format = self.format, .metadata_incarnation = self.metadata_incarnation, .catalog_revision = self.catalog_revision, .table = self.table, .attachments = self.attachments, .routines = self.routines, .lookups = self.lookups, .clock = self.clock });
    }
    pub fn requireFor(self: Manifest, expected: Table, incarnation: [16]u8) !void {
        if (!self.table.eql(expected) or !std.mem.eql(u8, &self.metadata_incarnation, &incarnation)) return error.CatalogGenerationChanged;
    }
    pub fn digest(self: Manifest) !Digest {
        return canonicalDigest("antfly/sql-trigger-statement/v1\x00", self);
    }
    pub fn encode(self: Manifest, a: A) ![]u8 {
        try self.validate(a);
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(Manifest) {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidSqlTriggerManifest;
        var parsed = std.json.parseFromSlice(Manifest, a, bytes, .{ .allocate = .alloc_always }) catch |failure| return if (failure == error.OutOfMemory) failure else error.InvalidSqlTriggerManifest;
        errdefer parsed.deinit();
        try parsed.value.validate(a);
        const canonical = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(canonical);
        if (!std.mem.eql(u8, canonical, bytes)) return error.InvalidSqlTriggerManifest;
        return parsed;
    }
};
pub fn attachmentDigest(state: attachments.State) !Digest {
    try state.validate();
    return canonicalDigest("antfly/sql-table-triggers/v1\x00", state);
}
fn requireEncodedBound(value: anytype) !void {
    var count = std.Io.Writer.Discarding.init(&.{});
    try std.json.Stringify.value(value, .{}, &count.writer);
    if (count.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
}
fn canonicalDigest(domain: []const u8, value: anytype) !Digest {
    var state = std.crypto.hash.sha2.Sha256.init(.{});
    state.update(domain);
    var count = std.Io.Writer.Discarding.init(&.{});
    var buffer: [4096]u8 = undefined;
    var hashing = std.Io.Writer.Hashed(std.crypto.hash.sha2.Sha256).initHasher(&count.writer, state, &buffer);
    try std.json.Stringify.value(value, .{}, &hashing.writer);
    try hashing.writer.flush();
    if (count.fullCount() > max_bytes) return error.SqlProgramLimitExceeded;
    return hashing.hasher.finalResult();
}

test "graph preparation absence witness is explicit mutually exclusive bound and canonical without invented attachment generation" {
    const a = std.testing.allocator;
    const admission: @import("../storage/db/prepared_graph_effects.zig").Admission = .{ .metadata_group_id = 11, .incarnation = @splat(1), .source_revision = 9, .table_id = 7, .schema_digest = @splat(2), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} };
    const absent = try Absence.fromAdmission(admission, 3);
    const NativeView = struct {
        exists: bool = false,
        digest: Digest = @splat(2),
        pub fn triggerState(self: @This()) ?*const attachments.State {
            return if (self.exists) &state else null;
        }
        pub fn version(_: @This()) u32 {
            return 3;
        }
        pub fn validator(self: @This()) ?struct { source_digest: ?Digest } {
            return .{ .source_digest = self.digest };
        }
        const state: attachments.State = .{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 1, .next_id = 1, .entries = &.{} };
    };
    try absent.requireView(NativeView{});
    try std.testing.expectError(error.CatalogGenerationChanged, absent.requireView(NativeView{ .exists = true }));
    try std.testing.expectError(error.CatalogGenerationChanged, absent.requireView(NativeView{ .digest = @splat(9) }));
    var changed = admission;
    changed.source_revision += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, absent.requireAdmission(changed));
    const program: Manifest = .{ .format = 2, .metadata_incarnation = admission.incarnation, .catalog_revision = 8, .table = try absent.table(), .absence = absent, .routines = &.{} };
    try std.testing.expectEqual(@as(u64, 0), program.table.attachment_generation);
    const bytes = try program.encode(a);
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "\"attachments\"") == null);
    var decoded = try Manifest.decode(a, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(program, decoded.value);
    var mixed = program;
    mixed.attachments = NativeView.state;
    try std.testing.expectError(error.InvalidSqlTriggerManifest, mixed.validate(a));
    var forged = program;
    forged.table.attachment_generation = 1;
    try std.testing.expectError(error.InvalidSqlTriggerManifest, forged.validate(a));
    const present: Manifest = .{ .metadata_incarnation = @splat(1), .catalog_revision = 8, .table = .{ .id = 7, .schema_version = 3, .schema_digest = @splat(2), .attachment_generation = 1, .attachment_digest = try attachmentDigest(NativeView.state) }, .attachments = NativeView.state, .routines = &.{} };
    const actual = try present.encode(a);
    defer a.free(actual);
    const legacy = try std.json.Stringify.valueAlloc(a, .{ .format = present.format, .metadata_incarnation = present.metadata_incarnation, .catalog_revision = present.catalog_revision, .table = present.table, .attachments = NativeView.state, .routines = present.routines, .lookups = present.lookups, .clock = present.clock }, .{});
    defer a.free(legacy);
    try std.testing.expectEqualStrings(legacy, actual);
}
