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

//! Shared FK target-definition leases, held with the source job's metadata
//! admission and released with its terminal decision. Parent-indexed records
//! make DDL checks proportional to live dependents; source-indexed digests make
//! checkpoint validation bidirectional without reparsing a plan for every FK.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_schema_owner_contract;
const A = std.mem.Allocator;
pub const all_prefix = "\x00\x00__metadata__:schema_validation_dependency:";
const header_len = 148;
pub const max_record_bytes = header_len + 4096 + 32;

pub const Key = struct {
    group: u64,
    child: u64,
    id: [16]u8,
    ordinal: u16,
    parent: ?u64 = null,

    pub fn encode(self: @This(), buf: []u8) ![]const u8 {
        if (self.group == 0 or self.child == 0 or self.ordinal >= 256 or std.mem.allEqual(u8, &self.id, 0) or (self.parent != null and self.parent.? == 0)) return error.InvalidGenerationPublication;
        const id = std.fmt.bytesToHex(self.id, .lower);
        return if (self.parent) |parent|
            std.fmt.bufPrint(buf, "{s}{d}:p:{d}:{d}:{s}:{d}", .{ all_prefix, self.group, parent, self.child, id, self.ordinal })
        else
            std.fmt.bufPrint(buf, "{s}{d}:s:{d}:{s}:{d}", .{ all_prefix, self.group, self.child, id, self.ordinal });
    }
    pub fn parse(bytes: []const u8) !?@This() {
        if (!std.mem.startsWith(u8, bytes, all_prefix)) return null;
        var parts = std.mem.splitScalar(u8, bytes[all_prefix.len..], ':');
        var result: @This() = .{ .group = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidGenerationPublication, 10) catch return error.InvalidGenerationPublication, .child = 0, .id = undefined, .ordinal = 0 };
        const kind = parts.next() orelse return error.InvalidGenerationPublication;
        if (std.mem.eql(u8, kind, "p")) {
            result.parent = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidGenerationPublication, 10) catch return error.InvalidGenerationPublication;
        } else if (!std.mem.eql(u8, kind, "s")) return error.InvalidGenerationPublication;
        result.child = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidGenerationPublication, 10) catch return error.InvalidGenerationPublication;
        const id = parts.next() orelse return error.InvalidGenerationPublication;
        if (id.len != 32) return error.InvalidGenerationPublication;
        _ = std.fmt.hexToBytes(&result.id, id) catch return error.InvalidGenerationPublication;
        result.ordinal = std.fmt.parseInt(u16, parts.next() orelse return error.InvalidGenerationPublication, 10) catch return error.InvalidGenerationPublication;
        if (parts.next() != null) return error.InvalidGenerationPublication;
        var canonical: [256]u8 = undefined;
        if (!std.mem.eql(u8, bytes, try result.encode(&canonical))) return error.InvalidGenerationPublication;
        return result;
    }
};

pub fn prefixForGroup(buf: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidGenerationPublication;
    return std.fmt.bufPrint(buf, "{s}{d}:", .{ all_prefix, group });
}

pub fn parentPrefix(buf: []u8, group: u64, parent: u64) ![]const u8 {
    if (group == 0 or parent == 0) return error.InvalidGenerationPublication;
    return std.fmt.bufPrint(buf, "{s}{d}:p:{d}:", .{ all_prefix, group, parent });
}

pub const Record = struct {
    key: Key,
    metadata_incarnation: [32]u8,
    plan_digest: [32]u8,
    unique_name: []const u8,
    definition: [32]u8,

    pub fn validate(self: @This()) !void {
        var buf: [256]u8 = undefined;
        _ = try self.key.encode(&buf);
        if (self.key.parent == null or !local.metadata_incarnation.isValid(self.metadata_incarnation) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or std.mem.allEqual(u8, &self.definition, 0) or
            self.unique_name.len == 0 or self.unique_name.len > 4096 or std.mem.indexOfScalar(u8, self.unique_name, 0) != null) return error.InvalidGenerationPublication;
    }
    pub fn encode(self: @This(), a: A) ![]u8 {
        try self.validate();
        const bytes = try a.alloc(u8, header_len + self.unique_name.len + 32);
        @memcpy(bytes[0..8], "ASVD\x01\x00\x00\x00");
        std.mem.writeInt(u64, bytes[8..16], self.key.group, .little);
        std.mem.writeInt(u64, bytes[16..24], self.key.parent.?, .little);
        std.mem.writeInt(u64, bytes[24..32], self.key.child, .little);
        @memcpy(bytes[32..48], &self.key.id);
        @memcpy(bytes[48..80], &self.metadata_incarnation);
        @memcpy(bytes[80..112], &self.plan_digest);
        std.mem.writeInt(u16, bytes[112..114], self.key.ordinal, .little);
        std.mem.writeInt(u16, bytes[114..116], @intCast(self.unique_name.len), .little);
        @memcpy(bytes[116..148], &self.definition);
        @memcpy(bytes[header_len..][0..self.unique_name.len], self.unique_name);
        @memcpy(bytes[bytes.len - 32 ..], &contract.hash(bytes[0 .. bytes.len - 32]));
        return bytes;
    }
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len < header_len + 33 or bytes.len > max_record_bytes or !std.mem.eql(u8, bytes[0..8], "ASVD\x01\x00\x00\x00") or
            std.mem.readInt(u16, bytes[114..116], .little) != bytes.len - header_len - 32 or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &contract.hash(bytes[0 .. bytes.len - 32]))) return error.InvalidGenerationPublication;
        const result: @This() = .{ .key = .{ .group = std.mem.readInt(u64, bytes[8..16], .little), .parent = std.mem.readInt(u64, bytes[16..24], .little), .child = std.mem.readInt(u64, bytes[24..32], .little), .id = bytes[32..48].*, .ordinal = std.mem.readInt(u16, bytes[112..114], .little) }, .metadata_incarnation = bytes[48..80].*, .plan_digest = bytes[80..112].*, .definition = bytes[116..148].*, .unique_name = bytes[header_len .. bytes.len - 32] };
        try result.validate();
        return result;
    }
    pub fn fromSource(head: anytype, source: contract.ForeignKeySource, ordinal: usize) !@This() {
        const parent = source.parents[ordinal];
        return .{ .key = .{ .group = head.metadata_group_id, .parent = parent.parent_table_id, .child = head.table_id, .id = head.id, .ordinal = @intCast(ordinal) }, .metadata_incarnation = head.metadata_incarnation, .plan_digest = head.plan_digest, .unique_name = parent.parent_unique, .definition = parent.parent_definition orelse return error.InvalidGenerationPublication };
    }
};

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn acquire(a: A, txn: anytype, head: anytype, source: contract.ForeignKeySource) !void {
    try source.validate();
    for (source.parents, 0..) |parent, ordinal| {
        if (try @import("table_lifecycle_lock.zig").read(txn, head.metadata_group_id, parent.parent_table_id)) |lock| {
            if (lock.kind == .generation) return error.GenerationPublicationChanged;
        }
        const record = try Record.fromSource(head, source, ordinal);
        const bytes = try record.encode(a);
        defer a.free(bytes);
        var key_buf: [256]u8 = undefined;
        const parent_key = try record.key.encode(&key_buf);
        if (try optional(txn, parent_key) != null) return error.GenerationPublicationChanged;
        try txn.put(parent_key, bytes);
        var source_key = record.key;
        source_key.parent = null;
        const index_key = try source_key.encode(&key_buf);
        if (try optional(txn, index_key) != null) return error.GenerationPublicationChanged;
        var index: [40]u8 = undefined;
        std.mem.writeInt(u64, index[0..8], record.key.parent.?, .little);
        @memcpy(index[8..40], &contract.hash(bytes));
        try txn.put(index_key, &index);
    }
}

/// Check every expected pair from one immutable source configuration. The
/// checkpoint's reverse scan independently rejects orphan or extra keys.
pub fn requireSource(a: A, txn: anytype, head: anytype, source: contract.ForeignKeySource, terminal: bool) !void {
    for (source.parents, 0..) |_, ordinal| {
        const record = try Record.fromSource(head, source, ordinal);
        var key_buf: [256]u8 = undefined;
        const parent = try optional(txn, try record.key.encode(&key_buf));
        var source_key = record.key;
        source_key.parent = null;
        if (terminal) {
            if (parent != null or try optional(txn, try source_key.encode(&key_buf)) != null) return error.InvalidGenerationPublication;
            continue;
        }
        const bytes = try record.encode(a);
        defer a.free(bytes);
        if (!std.mem.eql(u8, parent orelse return error.InvalidGenerationPublication, bytes)) return error.InvalidGenerationPublication;
        const indexed = (try optional(txn, try source_key.encode(&key_buf))) orelse return error.InvalidGenerationPublication;
        if (indexed.len != 40 or std.mem.readInt(u64, indexed[0..8], .little) != record.key.parent.? or !std.mem.eql(u8, indexed[8..40], &contract.hash(bytes))) return error.InvalidGenerationPublication;
    }
}

/// Last-owner ACK cleanup uses a bounded index walk, with no JSON parsing or
/// heap allocation. Close the cursor before deleting its rows; store cursor
/// invalidation rules therefore cannot skip a lease during atomic release.
pub fn releaseJob(txn: anytype, head: anytype, constraints: u16) !void {
    if (constraints == 0 or constraints > 256) return error.InvalidGenerationPublication;
    var entries: [256]Key = undefined;
    var seen: [256]bool = @splat(false);
    var count: usize = 0;
    var prefix_buf: [256]u8 = undefined;
    const id = std.fmt.bytesToHex(head.id, .lower);
    const prefix = try std.fmt.bufPrint(&prefix_buf, "{s}{d}:s:{d}:{s}:", .{ all_prefix, head.metadata_group_id, head.table_id, id });
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var row = try cursor.seekAtOrAfter(prefix);
        while (row) |item| : (row = try cursor.next()) {
            if (!std.mem.startsWith(u8, item.key, prefix)) break;
            if (count == constraints or item.value.len != 40) return error.InvalidGenerationPublication;
            var key = (try Key.parse(item.key)) orelse return error.InvalidGenerationPublication;
            if (key.parent != null or key.ordinal >= constraints or seen[key.ordinal]) return error.InvalidGenerationPublication;
            key.parent = std.mem.readInt(u64, item.value[0..8], .little);
            const expected = item.value[8..40].*;
            var key_buf: [256]u8 = undefined;
            const bytes = try txn.get(try key.encode(&key_buf));
            if (!std.mem.eql(u8, &contract.hash(bytes), &expected)) return error.InvalidGenerationPublication;
            const record = try Record.decode(bytes);
            if (!std.meta.eql(record.key, key) or !std.mem.eql(u8, &record.metadata_incarnation, &head.metadata_incarnation) or
                !std.mem.eql(u8, &record.plan_digest, &head.plan_digest)) return error.InvalidGenerationPublication;
            seen[key.ordinal] = true;
            entries[count] = key;
            count += 1;
        }
    }
    if (count != constraints) return error.InvalidGenerationPublication;
    for (entries[0..count]) |entry| {
        var buf: [256]u8 = undefined;
        try txn.delete(try entry.encode(&buf));
        var index = entry;
        index.parent = null;
        try txn.delete(try index.encode(&buf));
    }
}

pub fn requireUnleased(txn: anytype, group: u64, parent: u64) !void {
    var buf: [256]u8 = undefined;
    const prefix = try parentPrefix(&buf, group, parent);
    var cursor = try txn.openCursor();
    defer cursor.close();
    if (try cursor.seekAtOrAfter(prefix)) |row| if (std.mem.startsWith(u8, row.key, prefix)) return error.GenerationPublicationChanged;
}

/// Common parent schema writer. The no-lease path allocates nothing; active
/// leases permit unrelated columns/indexes and reject only changed targets.
pub fn requireDefinitionsRetained(a: A, txn: anytype, group: u64, parent: u64, schema_json: []const u8) !void {
    var buf: [256]u8 = undefined;
    const prefix = try parentPrefix(&buf, group, parent);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    if (row == null or !std.mem.startsWith(u8, row.?.key, prefix)) return;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const owned = scratch.allocator();
    var public = try local.schema_mod.parseValidatedTableSchema(owned, schema_json);
    defer public.deinit(owned);
    const runtime = try local.schema_mod.deriveRelationalCheckLayout(owned, public);
    defer local.storage_schema.freeSchema(owned, runtime);
    const definitions = try local.schema_relational_declarations.definitionFingerprints(owned, public, runtime);
    defer local.schema_relational_declarations.freeDefinitions(owned, definitions);
    var targets: std.StringHashMapUnmanaged([32]u8) = .empty;
    defer targets.deinit(owned);
    for (definitions) |definition| {
        if (definition.kind == .unique) try targets.put(owned, definition.name, definition.fingerprint);
    }
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        const record = try Record.decode(item.value);
        var key_buf: [256]u8 = undefined;
        if (!std.mem.eql(u8, item.key, try record.key.encode(&key_buf))) return error.InvalidGenerationPublication;
        const retained = targets.get(record.unique_name) orelse return error.GenerationPublicationChanged;
        if (!std.mem.eql(u8, &retained, &record.definition)) return error.GenerationPublicationChanged;
    }
}

const TestFixture = struct {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const Head = struct { metadata_group_id: u64 = 21, metadata_incarnation: [32]u8 = "0123456789abcdef0123456789abcdef".*, table_id: u64, id: [16]u8, plan_digest: [32]u8 };
    fn head(child: u64, id: u8) Head {
        return .{ .table_id = child, .id = @splat(id), .plan_digest = @splat(id + 1) };
    }
    fn scope(target_definition: [32]u8, name: []const u8) contract.ForeignKeyParent {
        return .{ .constraint_name = name, .declared_parent = "parents", .parent_table = "parents", .parent_table_id = 7, .parent_schema_version = 1, .parent_schema = @splat(1), .parent_public = contract.hash(schema_json), .parent_catalog = @splat(2), .parent_unique = "pk", .parent_generation = @splat(3), .parent_definition = target_definition };
    }
    fn fingerprint(a: A) ![32]u8 {
        var public = try local.schema_mod.parseValidatedTableSchema(a, schema_json);
        defer public.deinit(a);
        const runtime = try local.schema_mod.deriveRelationalCheckLayout(a, public);
        defer local.storage_schema.freeSchema(a, runtime);
        const definitions = try local.schema_relational_declarations.definitionFingerprints(a, public, runtime);
        defer local.schema_relational_declarations.freeDefinitions(a, definitions);
        return definitions[0].fingerprint;
    }
};

test "schema validation publication dependency codecs reject corruption ambiguous keys and allocation loss" {
    const a = std.testing.allocator;
    const head = TestFixture.head(300, 1);
    const source: contract.ForeignKeySource = .{ .table_name = "child", .parents = &.{TestFixture.scope(@splat(4), "fk")} };
    const record = try Record.fromSource(head, source, 0);
    var buf: [256]u8 = undefined;
    const key = try record.key.encode(&buf);
    try std.testing.expectEqualDeep(record.key, (try Key.parse(key)).?);
    for ([_][]const u8{
        all_prefix ++ "021:p:7:300:01010101010101010101010101010101:0",
        all_prefix ++ "21:p:0:300:01010101010101010101010101010101:0",
        all_prefix ++ "21:p:7:300:01010101010101010101010101010101:256",
        all_prefix ++ "21:s:300:01010101010101010101010101010101:00",
    }) |bad| try std.testing.expectError(error.InvalidGenerationPublication, Key.parse(bad));
    const bytes = try record.encode(a);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(record, try Record.decode(bytes));
    for (0..bytes.len) |i| {
        bytes[i] ^= 1;
        try std.testing.expectError(error.InvalidGenerationPublication, Record.decode(bytes));
        bytes[i] ^= 1;
    }
    const Allocation = struct {
        fn run(allocator: A, value: Record) !void {
            const raw = try value.encode(allocator);
            defer allocator.free(raw);
            try std.testing.expectEqualDeep(value, try Record.decode(raw));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Allocation.run, .{record});
}

test "schema validation publication dependency leases share targets preserve unrelated DDL and recover atomic release" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("fk-definition-leases");
    defer directory.cleanup();
    const path = try a.dupeSentinel(u8, directory.path(), 0);
    defer a.free(path);
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    var live = true;
    defer if (live) store.close();
    const fingerprint = try TestFixture.fingerprint(a);
    const source: contract.ForeignKeySource = .{ .table_name = "children", .parents = &.{TestFixture.scope(fingerprint, "fk")} };
    const first = TestFixture.head(300, 1);
    const second = TestFixture.head(301, 3);
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        try acquire(a, &write, first, source);
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try requireUnleased(&read, 21, 7);
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try acquire(a, &write, first, source);
        try acquire(a, &write, second, source);
        try write.commit();
    }
    const unrelated = try std.mem.replaceOwned(u8, a, TestFixture.schema_json, "\"properties\":{", "\"properties\":{\"payload\":{\"type\":\"string\"},");
    defer a.free(unrelated);
    const changed_target = try std.mem.replaceOwned(u8, a, TestFixture.schema_json, "\"name\":\"pk\"", "\"name\":\"replacement\"");
    defer a.free(changed_target);
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try requireSource(a, &read, first, source, false);
        try requireSource(a, &read, second, source, false);
        try std.testing.expectError(error.GenerationPublicationChanged, requireUnleased(&read, 21, 7));
        try requireDefinitionsRetained(a, &read, 21, 7, unrelated);
        try std.testing.expectError(error.GenerationPublicationChanged, requireDefinitionsRetained(a, &read, 21, 7, changed_target));
    }
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        try std.testing.expectError(error.GenerationPublicationChanged, @import("table_lifecycle_lock.zig").acquire(&write, 21, 7, .{ .kind = .generation, .id = @splat(5) }));
        try releaseJob(&write, first, 1);
    }
    store.close();
    live = false;
    store = try local.storage_docstore.DocStore.open(a, path, .{});
    live = true;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try requireSource(a, &read, first, source, false);
        try requireSource(a, &read, second, source, false);
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try releaseJob(&write, first, 1);
        try write.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try requireSource(a, &read, first, source, true);
        try requireSource(a, &read, second, source, false);
        try std.testing.expectError(error.GenerationPublicationChanged, requireDefinitionsRetained(a, &read, 21, 7, changed_target));
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try releaseJob(&write, second, 1);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    try requireUnleased(&read, 21, 7);
    try requireDefinitionsRetained(a, &read, 21, 7, changed_target);
}

test "schema validation publication dependency final ACK bounds 256 leases and rejects torn source indexes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("fk-definition-lease-bound");
    defer directory.cleanup();
    const path = try a.dupeSentinel(u8, directory.path(), 0);
    defer a.free(path);
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    var names: [256][16]u8 = undefined;
    var parents: [256]contract.ForeignKeyParent = undefined;
    for (&names, &parents, 0..) |*buf, *parent, i| parent.* = TestFixture.scope(@splat(4), try std.fmt.bufPrint(buf, "fk_{d}", .{i}));
    const source: contract.ForeignKeySource = .{ .table_name = "child", .parents = &parents };
    const head = TestFixture.head(300, 1);
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try acquire(a, &write, head, source);
        try write.commit();
    }
    const selected = try Record.fromSource(head, source, 9);
    var index = selected.key;
    index.parent = null;
    var buf: [256]u8 = undefined;
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        try write.delete(try index.encode(&buf));
        try std.testing.expectError(error.InvalidGenerationPublication, releaseJob(&write, head, 256));
    }
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try releaseJob(&write, head, 256);
        try write.commit();
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    try requireSource(a, &read, head, source, true);
    try requireUnleased(&read, 21, 7);
}
