// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Self-contained immutable collation witnesses in authoritative column layouts.
//! Data owners verify ICU versions before opening a table plan. Display names
//! never enter the schema or require a metadata round-trip on a row path.
const std = @import("std");
const records = @import("../system_catalog/collations.zig");
const native = @import("../sql/native_collation.zig");
const bindings = @import("../sql/collation_binding.zig");
const A = std.mem.Allocator;
pub const property_name = "x-antfly-sql-collation";
pub const max_collations: usize = 4096;
pub const max_native_collations: usize = 128;
pub const Mode = enum { structural, native };
pub const Spec = struct {
    bound: records.Bound,
    locale: []const u8,

    pub fn validate(self: Spec) !void {
        try self.bound.validate();
        if (!self.bound.deterministic) return error.UnsupportedSqlCollation;
        if (self.locale.len == 0 or self.locale.len > 1024 or !std.unicode.utf8ValidateSlice(self.locale) or std.mem.indexOfScalar(u8, self.locale, 0) != null) return error.InvalidSqlCollationRecord;
    }
    pub fn clone(self: Spec, a: A) !Spec {
        try self.validate();
        var result = self;
        result.locale = try a.dupe(u8, self.locale);
        return result;
    }
    pub fn decode(a: A, bytes: []const u8) !Spec {
        if (bytes.len > 16 * 1024) return error.InvalidSqlCollationRecord;
        var parsed = try std.json.parseFromSlice(Spec, a, bytes, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        return parsed.value.clone(a);
    }
    pub fn fingerprint(self: Spec) ![32]u8 {
        try self.validate();
        var buffer: [8192]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buffer);
        try std.json.Stringify.value(self, .{}, &writer);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:icu-collation-domain:v1\x00");
        hash.update(writer.buffered());
        return hash.finalResult();
    }
    pub fn requireRecord(self: Spec, record: records.Record) !void {
        try self.validate();
        try self.bound.requireRecord(record);
        if (!std.mem.eql(u8, self.locale, record.definition.locale)) return error.SqlStaleCollation;
    }
    pub fn eql(self: Spec, other: Spec) bool {
        return self.bound.eql(other.bound) and std.mem.eql(u8, self.locale, other.locale);
    }
    pub fn fromRecord(record: records.Record) !Spec {
        try record.validate();
        const result: Spec = .{ .bound = record.bound(), .locale = record.definition.locale };
        try result.validate();
        return result;
    }
    pub fn prepare(self: Spec, a: A, provider: *const native.Provider) !bindings.View {
        try self.validate();
        const record: records.Record = .{ .reference = self.bound.reference, .definition = .{
            .provider = self.bound.provider,
            .locale = self.locale,
            .deterministic = self.bound.deterministic,
            .version = self.bound.version,
            .owner = .auth_disabled,
        } };
        return bindings.View.init(provider, a, record);
    }
};

pub fn property(a: A, value: std.json.Value) !?Spec {
    if (value != .object) return error.InvalidSqlCollationRecord;
    const encoded = value.object.get(property_name) orelse return null;
    const result = std.json.parseFromValueLeaky(Spec, a, encoded, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSqlCollationRecord;
    try result.validate();
    return result;
}

/// Only actual declared properties are interpreted. A JSON literal containing
/// a matching key is user data, never a dependency or native execution witness.
pub fn collectColumnsAlloc(a: A, schema: std.json.Value) ![]Spec {
    if (schema != .object) return error.InvalidSqlCollationRecord;
    const documents = schema.object.get("document_schemas") orelse return a.alloc(Spec, 0);
    if (documents != .object) return error.InvalidSqlCollationRecord;
    var by_id: std.AutoHashMap(u64, Spec) = .init(a);
    defer by_id.deinit();
    var layouts = documents.object.iterator();
    while (layouts.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidSqlCollationRecord;
        const root = entry.value_ptr.object.get("schema") orelse continue;
        if (root != .object) return error.InvalidSqlCollationRecord;
        const properties = root.object.get("properties") orelse continue;
        if (properties != .object) return error.InvalidSqlCollationRecord;
        var columns = properties.object.iterator();
        while (columns.next()) |column| if (try property(a, column.value_ptr.*)) |spec| {
            const found = try by_id.getOrPut(spec.bound.reference.id);
            if (found.found_existing and !found.value_ptr.eql(spec)) return error.InvalidSqlCollationRecord;
            found.value_ptr.* = spec;
            if (by_id.count() > max_collations) return error.SqlProgramLimitExceeded;
        };
    }
    const result = try a.alloc(Spec, by_id.count());
    errdefer a.free(result);
    var entries = by_id.valueIterator();
    var i: usize = 0;
    while (entries.next()) |spec| : (i += 1) result[i] = spec.*;
    std.mem.sort(Spec, result, {}, struct {
        fn less(_: void, left: Spec, right: Spec) bool {
            return left.bound.reference.id < right.bound.reference.id;
        }
    }.less);
    return result;
}

pub const Registry = struct {
    mode: Mode = .native,
    context: native.Context = .{},
    arena: *std.heap.ArenaAllocator,
    provider: ?*native.Provider = null,
    views: std.AutoHashMapUnmanaged(u64, *bindings.View) = .empty,
    specs: std.AutoHashMapUnmanaged(u64, Spec) = .empty,

    pub fn init(backing: A, specs: []const Spec) !Registry {
        return initMode(backing, specs, .native);
    }
    pub fn initMode(backing: A, specs: []const Spec, mode: Mode) !Registry {
        return initConfigured(backing, specs, mode, .{});
    }
    pub fn initConfigured(backing: A, specs: []const Spec, mode: Mode, context: native.Context) !Registry {
        if (specs.len > max_collations) return error.SqlProgramLimitExceeded;
        const arena = try backing.create(std.heap.ArenaAllocator);
        arena.* = .init(backing);
        var self: Registry = .{ .arena = arena, .mode = mode, .context = context };
        errdefer self.deinit();
        for (specs) |spec| _ = try self.add(spec);
        return self;
    }
    pub fn add(self: *Registry, spec: Spec) !*const bindings.View {
        try spec.validate();
        if (self.specs.get(spec.bound.reference.id)) |installed| {
            if (!installed.eql(spec)) return error.InvalidSqlCollationRecord;
            return self.views.get(spec.bound.reference.id) orelse return error.SqlUnboundCollation;
        }
        if (self.specs.count() == (if (self.mode == .native) max_native_collations else max_collations)) return error.SqlProgramLimitExceeded;
        const a = self.arena.allocator();
        if (self.mode == .native and self.provider == null) {
            const provider = try a.create(native.Provider);
            provider.* = if (self.context.factory) |factory| configured: {
                var receiver = try factory.receive();
                defer receiver.deinit();
                break :configured native.Provider.fromReceiver(receiver);
            } else if (self.context.configured) return error.SqlIcuProviderUnavailable else try native.Provider.load(null);
            self.provider = provider;
        }
        const view = try a.create(bindings.View);
        view.* = if (self.mode == .native) try spec.prepare(a, self.provider.?) else .{ .witness = .{ .named = spec.bound }, .locale = try a.dupe(u8, spec.locale), .allocator = a };
        errdefer view.deinit();
        var owned_spec = spec;
        owned_spec.locale = try a.dupe(u8, spec.locale);
        try self.specs.put(a, spec.bound.reference.id, owned_spec);
        errdefer _ = self.specs.remove(spec.bound.reference.id);
        try self.views.put(a, spec.bound.reference.id, view);
        return view;
    }
    pub fn find(self: *const Registry, spec: Spec) !*const bindings.View {
        try spec.validate();
        const installed = self.specs.get(spec.bound.reference.id) orelse return error.SqlUnboundCollation;
        if (!installed.eql(spec)) return error.SqlStaleCollation;
        const view = self.views.get(spec.bound.reference.id) orelse return error.SqlUnboundCollation;
        if (view.witness != .named or !view.witness.named.eql(spec.bound)) return error.SqlStaleCollation;
        return view;
    }
    pub fn deinit(self: *Registry) void {
        const backing = self.arena.child_allocator;
        var views = self.views.valueIterator();
        while (views.next()) |view| view.*.deinit();
        if (self.provider) |provider| provider.deinit();
        self.arena.deinit();
        backing.destroy(self.arena);
        self.* = undefined;
    }
};

pub fn freeSpecs(a: A, specs: []const Spec) void {
    for (specs) |spec| a.free(spec.locale);
    a.free(specs);
}

/// Capture both accepting and historical layouts before publishing a table
/// successor. A historical layout remains a dependency until its real owner
/// retires it; dropping a collation cannot strand cold rows or index readers.
pub fn guardsForDefinitionsAlloc(a: A, write: []const u8, read: []const u8) ![]Spec {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var by_id: std.AutoHashMap(u64, Spec) = .init(scratch);
    for ([_][]const u8{ write, read }) |bytes| {
        // Escape sequences may spell the property name without its literal bytes.
        if (std.mem.indexOf(u8, bytes, property_name) == null and std.mem.indexOfScalar(u8, bytes, '\\') == null) continue;
        const document = try std.json.parseFromSliceLeaky(std.json.Value, scratch, bytes, .{});
        for (try collectColumnsAlloc(scratch, document)) |spec| {
            const entry = try by_id.getOrPut(spec.bound.reference.id);
            if (entry.found_existing and !entry.value_ptr.eql(spec)) return error.InvalidSqlCollationRecord;
            entry.value_ptr.* = spec;
        }
    }
    const result = try a.alloc(Spec, by_id.count());
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |spec| a.free(spec.locale);
        a.free(result);
    }
    var specs = by_id.valueIterator();
    while (specs.next()) |spec| {
        result[initialized] = try spec.clone(a);
        initialized += 1;
    }
    std.mem.sort(Spec, result, {}, struct {
        fn less(_: void, left: Spec, right: Spec) bool {
            return left.bound.reference.id < right.bound.reference.id;
        }
    }.less);
    return result;
}

/// Bounded allocation-free identity of the actual comparison domains prepared
/// by one physical schema owner. Names/order/duplicate column uses do not alter
/// the identity; immutable catalog versions and native versions do.
pub const DomainFingerprintSet = struct {
    const Entry = struct { id: u64, fingerprint: [32]u8 };
    entries: [max_native_collations]Entry = undefined,
    len: usize = 0,
    pub fn include(self: *@This(), spec: Spec) !void {
        const fingerprint = try spec.fingerprint();
        for (self.entries[0..self.len]) |entry| if (entry.id == spec.bound.reference.id) {
            if (!std.mem.eql(u8, &entry.fingerprint, &fingerprint)) return error.InvalidSqlCollationRecord;
            return;
        };
        if (self.len == self.entries.len) return error.SqlProgramLimitExceeded;
        self.entries[self.len] = .{ .id = spec.bound.reference.id, .fingerprint = fingerprint };
        self.len += 1;
    }
    pub fn finish(self: *@This()) [32]u8 {
        std.mem.sort(Entry, self.entries[0..self.len], {}, struct {
            fn less(_: void, lhs: Entry, rhs: Entry) bool {
                return lhs.id < rhs.id;
            }
        }.less);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-prepared-collation-domains/v1\x00");
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, self.len, .little);
        hash.update(&encoded);
        for (self.entries[0..self.len]) |entry| {
            std.mem.writeInt(u64, &encoded, entry.id, .little);
            hash.update(&encoded);
            hash.update(&entry.fingerprint);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};
pub fn domainsFingerprint(specs: []const Spec) ![32]u8 {
    var set: DomainFingerprintSet = .{};
    for (specs) |spec| try set.include(spec);
    return set.finish();
}
pub fn columnsDomainsFingerprint(columns: anytype) ![32]u8 {
    var set: DomainFingerprintSet = .{};
    for (columns) |column| if (column.collation_spec) |spec| try set.include(spec);
    return set.finish();
}

test "SQL ICU collation guards retain historical layouts and reject conflicting witnesses" {
    const a = std.testing.allocator;
    const record: records.Record = .{
        .reference = .{ .database_id = 1, .namespace_id = 2, .id = 19, .origin_id = @splat(5), .generation = 1 },
        .definition = .{ .locale = "und-u-ks-level2", .version = .{ .collation = .{ 153, 120, 0, 0 }, .uca = .{ 15, 1, 0, 0 } }, .owner = .auth_disabled },
    };
    const spec = try Spec.fromRecord(record);
    const Schema = struct {
        fn encode(allocator: A, witness: Spec) ![]u8 {
            return std.json.Stringify.valueAlloc(allocator, .{
                .document_schemas = .{ .row = .{ .schema = .{ .properties = .{
                    .label = .{ .@"x-antfly-sql-collation" = witness },
                } } } },
            }, .{});
        }
    };
    const historic = try Schema.encode(a, spec);
    defer a.free(historic);
    const refs = try guardsForDefinitionsAlloc(a, "{}", historic);
    defer freeSpecs(a, refs);
    try std.testing.expectEqual(@as(usize, 1), refs.len);
    try std.testing.expect(spec.eql(refs[0]));
    const escaped = try std.mem.replaceOwned(u8, a, historic, property_name, "\\u0078-antfly-sql-collation");
    defer a.free(escaped);
    const escaped_refs = try guardsForDefinitionsAlloc(a, "{}", escaped);
    defer freeSpecs(a, escaped_refs);
    try std.testing.expectEqual(@as(usize, 1), escaped_refs.len);
    try std.testing.expect(spec.eql(escaped_refs[0]));
    var changed = spec;
    changed.bound.version.uca[0] += 1;
    const current = try Schema.encode(a, changed);
    defer a.free(current);
    try std.testing.expectError(error.InvalidSqlCollationRecord, guardsForDefinitionsAlloc(a, current, historic));
    var registry = try Registry.initMode(a, refs, .structural);
    defer registry.deinit();
    try std.testing.expectEqual(try registry.find(spec), try registry.add(spec));
    try std.testing.expectError(error.SqlStaleCollation, registry.find(changed));
}
