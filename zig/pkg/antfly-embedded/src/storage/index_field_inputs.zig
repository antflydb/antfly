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

//! Closed input identities for actual native projected text documents. Hashes
//! preserve contribution order, analyzer pipelines, typed discriminants and
//! document coordinates. Inferred typing uses the native compiler's borrowed
//! projection visitor, so unrelated fields remain independent without copying
//! large scalar values or reproducing the compiler's inference rules.
const std = @import("std");
const intro = @import("../introducer.zig");
const A = std.mem.Allocator;
const H = std.crypto.hash.sha2.Sha256;
const Digest = [32]u8;

pub const Name = struct { name: []const u8 };
// Pointer/length lookup preserves native scalar provenance without hashing
// the body or rescanning every projected text field for each typed value.
const NativeTextIdentities = struct {
    const Key = struct { ptr: usize, len: usize };
    const Item = struct { key: Key, digest: Digest };
    small: [8]Item = undefined,
    count: usize = 0,
    overflow: std.AutoHashMapUnmanaged(Key, Digest) = .empty,
    fn put(self: *@This(), a: A, text: []const u8, digest: Digest) !void {
        const key: Key = .{ .ptr = @intFromPtr(text.ptr), .len = text.len };
        if (self.overflow.count() != 0) return self.overflow.put(a, key, digest);
        for (self.small[0..self.count]) |*item| if (std.meta.eql(item.key, key)) {
            item.digest = digest;
            return;
        };
        if (self.count < self.small.len) {
            self.small[self.count] = .{ .key = key, .digest = digest };
            self.count += 1;
            return;
        }
        try self.overflow.ensureTotalCapacity(a, 16);
        for (self.small[0..self.count]) |item| self.overflow.putAssumeCapacity(item.key, item.digest);
        try self.overflow.put(a, key, digest);
    }
    fn get(self: *const @This(), text: []const u8) ?Digest {
        const key: Key = .{ .ptr = @intFromPtr(text.ptr), .len = text.len };
        if (self.overflow.count() != 0) return self.overflow.get(key);
        for (self.small[0..self.count]) |item| if (std.meta.eql(item.key, key)) return item.digest;
        return null;
    }
};

pub const Inputs = struct {
    const LedgerState = struct { context: Digest, rows: []?Digest, current: H = .init(.{}), raw_bytes: usize };
    const Document = struct { id: []u8, ordinal: ?u32 };
    ledgers: std.StringHashMapUnmanaged(LedgerState) = .empty,
    documents: []Document = &.{},
    allocator: A,
    states: std.StringHashMapUnmanaged(H) = .empty,
    common: Digest,
    pub fn deinit(self: *Inputs) void {
        var keys = self.states.keyIterator();
        while (keys.next()) |key| self.allocator.free(key.*);
        self.states.deinit(self.allocator);
        var ledgers = self.ledgers.valueIterator();
        while (ledgers.next()) |value| self.allocator.free(value.rows);
        self.ledgers.deinit(self.allocator);
        for (self.documents) |doc| self.allocator.free(doc.id);
        self.allocator.free(self.documents);
        self.* = undefined;
    }
    fn state(self: *Inputs, name: []const u8) !*H {
        const entry = try self.states.getOrPut(self.allocator, name);
        if (!entry.found_existing) {
            errdefer _ = self.states.remove(name);
            entry.key_ptr.* = try self.allocator.dupe(u8, name);
            entry.value_ptr.* = .init(.{});
            entry.value_ptr.update("antfly/compiled-field-contributions/v2\x00");
            bytes(entry.value_ptr, name);
        }
        return if (self.ledgers.getPtr(name)) |ledger| &ledger.current else entry.value_ptr;
    }
    pub fn hasLedger(self: *const Inputs, name: []const u8) bool {
        return self.ledgers.contains(name);
    }
    pub fn persistLedger(_: *const Inputs, unchanged: bool) bool {
        // Demand admission: changing and write-once fields do not populate a
        // cache for work that has never actually been reusable.
        return unchanged;
    }
    pub fn ledgerForField(self: *const Inputs, a: A, name: []const u8) !?[]u8 {
        const value = self.ledgers.get(name) orelse return null;
        if (40 + value.rows.len * 33 > @import("index_field_ledger.zig").entry_bytes) return null;
        return try @import("index_field_ledger.zig").encode(a, value.context, value.rows);
    }
    pub fn ledgerRow(self: *const Inputs, name: []const u8, ordinal: usize) !?Digest {
        const value = self.ledgers.get(name) orelse return null;
        if (ordinal >= value.rows.len) return error.InvalidIndexFieldGeneration;
        return value.rows[ordinal];
    }
    pub fn fieldInputBytes(self: *const Inputs, name: []const u8) usize {
        return if (self.ledgers.get(name)) |value| value.raw_bytes else 0;
    }
    pub fn documentId(self: *const Inputs, ordinal: usize) ![]const u8 {
        if (ordinal >= self.documents.len) return error.InvalidIndexFieldGeneration;
        return self.documents[ordinal].id;
    }
    pub fn hasField(self: *const Inputs, name: []const u8) bool {
        return self.states.contains(name);
    }
    pub fn fieldNames(self: *const Inputs, a: A) ![]Name {
        const names = try a.alloc(Name, self.states.count());
        var keys = self.states.keyIterator();
        var i: usize = 0;
        while (keys.next()) |key| : (i += 1) names[i] = .{ .name = key.* };
        return names;
    }
    pub fn inputForField(self: *const Inputs, name: []const u8) !Digest {
        if (self.ledgers.get(name)) |value| return @import("index_field_ledger.zig").digest(value.context, value.rows);
        var out: H = .init(.{});
        out.update("antfly/compiled-field-input/v2\x00");
        out.update(&self.common);
        bytes(&out, name);
        if (self.states.get(name)) |state_value| {
            var state_copy = state_value;
            out.update(&state_copy.finalResult());
        } else out.update(&.{0});
        return out.finalResult();
    }
};

pub fn prepare(a: A, docs: []const intro.TextDocument, analysis: intro.TextAnalysisConfig, configuration: Digest) !Inputs {
    if (docs.len == 0 or std.mem.allEqual(u8, &configuration, 0)) return error.InvalidIndexFieldGeneration;
    var result: Inputs = .{ .allocator = a, .common = undefined };
    errdefer result.deinit();
    var common: H = .init(.{});
    common.update("antfly/compiled-field-context/v1\x00");
    common.update(&configuration);
    number(&common, docs.len);
    try structured(&common, analysis);
    // Persist evidence only for expensive text and at most 1/64 of its input
    // bytes. Small/write-once scalar fields retain the original hashing path.
    var sizes: std.StringHashMapUnmanaged(usize) = .empty;
    defer sizes.deinit(a);
    var text_bytes: usize = 0;
    var identity_bytes: usize = 0;
    for (docs) |doc| {
        identity_bytes +|= doc.id.len;
        for (doc.text_fields) |field| text_bytes +|= field.text.len;
    }
    const consider_ledgers = text_bytes >= 65536 and identity_bytes <= 64 * 1024 and docs.len <= @import("index_field_ledger.zig").maximum_documents;
    if (consider_ledgers) for (docs) |doc| for (doc.text_fields) |field| {
        const item = try sizes.getOrPut(a, field.field_name);
        if (!item.found_existing) item.value_ptr.* = 0;
        item.value_ptr.* +|= field.text.len;
    };
    if (consider_ledgers) {
        var context_hash: H = .init(.{});
        context_hash.update("antfly/field-ledger-context/v2\x00");
        context_hash.update(&configuration);
        try structured(&context_hash, analysis);
        const context_digest = context_hash.finalResult();
        const ledger_bytes = 40 +| docs.len *| 33;
        var eligible = sizes.iterator();
        while (eligible.next()) |item| {
            if (item.value_ptr.* < 65536 or ledger_bytes > item.value_ptr.* / 64) continue;
            _ = try result.state(item.key_ptr.*);
            var field_context: H = .init(.{});
            field_context.update(&context_digest);
            bytes(&field_context, item.key_ptr.*);
            const rows = try a.alloc(?Digest, docs.len);
            errdefer a.free(rows);
            @memset(rows, null);
            const name = result.states.getKey(item.key_ptr.*).?;
            try result.ledgers.put(a, name, .{ .context = field_context.finalResult(), .rows = rows, .raw_bytes = item.value_ptr.* });
        }
    }
    if (result.ledgers.count() != 0) {
        const identities = try a.alloc(Inputs.Document, docs.len);
        var initialized: usize = 0;
        errdefer {
            for (identities[0..initialized]) |identity| a.free(identity.id);
            a.free(identities);
        }
        for (docs, identities) |doc, *identity| {
            identity.* = .{ .id = try a.dupe(u8, doc.id), .ordinal = doc.doc_ordinal };
            initialized += 1;
        }
        result.documents = identities;
    }
    for (docs, 0..) |doc, ordinal| {
        if (doc.id.len == 0) return error.InvalidIndexFieldGeneration;
        var ledgers = result.ledgers.valueIterator();
        while (ledgers.next()) |ledger| {
            ledger.current = H.init(.{});
            ledger.current.update(&ledger.context);
            bytes(&ledger.current, doc.id);
            ledger.current.update(&.{@intFromBool(doc.doc_ordinal != null)});
            if (doc.doc_ordinal) |value| number(&ledger.current, value);
        }
        bytes(&common, doc.id);
        common.update(&.{@intFromBool(doc.doc_ordinal != null)});
        if (doc.doc_ordinal) |id| number(&common, id);
        for (doc.text_fields) |field| {
            const state = try result.state(field.field_name);
            state.update(&.{1});
            if (!result.hasLedger(field.field_name)) number(state, ordinal);
            state.update(&.{@intFromBool(field.source_field != null)});
            if (field.source_field) |name| bytes(state, name);
            const identity = if (field.source_identity) |native| native.digest(field.text) else null;
            state.update(&.{@intFromBool(identity != null)});
            if (identity) |digest| state.update(&digest) else bytes(state, field.text);
            state.update(&.{@intFromBool(field.analyzer != null)});
            if (field.analyzer) |analyzer| try structured(state, analyzer.*);
        }
        var native_text: NativeTextIdentities = .{};
        defer native_text.overflow.deinit(a);
        for (doc.text_fields) |field| if (field.source_identity) |native| {
            if (native.digest(field.text)) |digest| try native_text.put(a, field.text, digest);
        };
        const TypedVisitor = struct {
            result: *Inputs,
            ordinal: usize,
            native_text: *const NativeTextIdentities,
            pub fn field(self: *@This(), projected: intro.TypedFieldValue) !void {
                const state = try self.result.state(projected.field_name);
                state.update(&.{2});
                if (!self.result.hasLedger(projected.field_name)) number(state, self.ordinal);
                state.update(&.{ @backingInt(projected.value_type), @intFromBool(projected.conflicted) });
                switch (projected.value) {
                    inline else => |value, tag| {
                        bytes(state, @tagName(tag));
                        if (comptime tag == .bytes_val) {
                            const identity = self.native_text.get(value);
                            state.update(&.{@intFromBool(identity != null)});
                            if (identity) |digest| state.update(&digest) else try exact(state, value);
                        } else try exact(state, value);
                    },
                }
            }
        };
        var visitor: TypedVisitor = .{ .result = &result, .ordinal = ordinal, .native_text = &native_text };
        if (doc.typed_fields) |typed| {
            for (typed) |field| try visitor.field(field);
        } else {
            var parsed: ?std.json.Parsed(std.json.Value) = null;
            defer if (parsed) |*value| value.deinit();
            const source = doc.typed_source orelse blk: {
                parsed = try std.json.parseFromSlice(std.json.Value, a, doc.stored_data, .{});
                break :blk parsed.?.value;
            };
            try intro.visitTypedFieldProjection(a, source, analysis, .{
                .recursive_typed_fields = doc.recursive_typed_fields,
                .infer_type_dynamic_paths = doc.infer_type_dynamic_paths,
                .unindexed_paths = doc.unindexed_paths,
            }, &visitor);
        }
        var finished = result.ledgers.valueIterator();
        while (finished.next()) |ledger| ledger.rows[ordinal] = ledger.current.finalResult();
    }
    result.common = common.finalResult();
    return result;
}
fn number(hash: *H, value: u64) void {
    var out: [8]u8 = undefined;
    std.mem.writeInt(u64, &out, value, .little);
    hash.update(&out);
}
fn bytes(hash: *H, value: []const u8) void {
    number(hash, value.len);
    hash.update(value);
}
fn structured(hash: *H, value: anytype) !void {
    // A nested digest frames self-delimiting structured configuration without
    // materializing an encoded copy or hashing pointer addresses/padding.
    var writer = std.Io.Writer.Hashing(H).init(&.{});
    try std.json.Stringify.value(value, .{ .emit_null_optional_fields = true }, &writer.writer);
    try writer.writer.flush();
    hash.update(&writer.hasher.finalResult());
}
fn exact(hash: *H, value: anytype) !void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .int => |info| {
            const U = @Int(.unsigned, info.bits);
            var raw: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(U, &raw, @bitCast(value), .little);
            hash.update(&raw);
        },
        .float => {
            const U = @Int(.unsigned, @bitSizeOf(T));
            try exact(hash, @as(U, @bitCast(value)));
        },
        .bool => hash.update(&.{@intFromBool(value)}),
        .pointer => bytes(hash, value),
        .@"struct" => inline for (@typeInfo(T).@"struct".field_names) |name| try exact(hash, @field(value, name)),
        .@"union" => switch (value) {
            inline else => |part, tag| {
                bytes(hash, @tagName(tag));
                try exact(hash, part);
            },
        },
        else => @compileError("unhandled compiled field input type"),
    }
}

test "index source recovery compiled field inputs preserve contributions and exact typed values" {
    const a = std.testing.allocator;
    const text = [_]intro.TextField{
        .{ .field_name = "body", .source_field = "body", .text = "alpha" },
        .{ .field_name = "_all", .source_field = "body", .text = "alpha" },
        .{ .field_name = "_all", .source_field = "title", .text = "beta" },
    };
    var docs = [_]intro.TextDocument{.{ .id = "one", .doc_ordinal = 7, .stored_data = "{}", .text_fields = &text, .typed_fields = &.{} }};
    var original = try prepare(a, &docs, .{}, @splat(11));
    defer original.deinit();
    var changed = text;
    changed[2].text = "gamma";
    docs[0].text_fields = &changed;
    var edited = try prepare(a, &docs, .{}, @splat(11));
    defer edited.deinit();
    try std.testing.expectEqualDeep(try original.inputForField("body"), try edited.inputForField("body"));
    try std.testing.expect(!std.mem.eql(u8, &try original.inputForField("_all"), &try edited.inputForField("_all")));
    const before = try original.inputForField("body");
    docs[0].typed_fields = &.{.{ .field_name = "body", .value_type = .i64_val, .value = .{ .i64_val = 1 } }};
    var typed = try prepare(a, &docs, .{}, @splat(11));
    defer typed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &before, &try typed.inputForField("body")));
    docs[0].typed_fields = &.{.{ .field_name = "body", .value_type = .f64_val, .value = .{ .f64_val = 1 } }};
    var floated = try prepare(a, &docs, .{}, @splat(11));
    defer floated.deinit();
    try std.testing.expect(!std.mem.eql(u8, &try typed.inputForField("body"), &try floated.inputForField("body")));
}

test "persistent source publication field ledger is composable and allocation safe" {
    const Exercise = struct {
        fn run(a: A) !void {
            const body = try a.alloc(u8, 65536);
            defer a.free(body);
            @memset(body, 'x');
            const text = [_]intro.TextField{.{ .field_name = "body", .text = body }};
            const docs = [_]intro.TextDocument{
                .{ .id = "a", .doc_ordinal = 11, .stored_data = "{}", .text_fields = &text, .typed_fields = &.{} },
                .{ .id = "b", .doc_ordinal = 22, .stored_data = "{}", .text_fields = &text, .typed_fields = &.{} },
            };
            var both = try prepare(a, &docs, .{}, @splat(8));
            defer both.deinit();
            var reversed = try prepare(a, &.{ docs[1], docs[0] }, .{}, @splat(8));
            defer reversed.deinit();
            var one = try prepare(a, docs[1..], .{}, @splat(8));
            defer one.deinit();
            try std.testing.expect(both.hasLedger("body"));
            try std.testing.expectEqualDeep(try both.ledgerRow("body", 1), try reversed.ledgerRow("body", 0));
            try std.testing.expectEqualDeep(try both.ledgerRow("body", 1), try one.ledgerRow("body", 0));
            const raw = (try one.ledgerForField(a, "body")).?;
            defer a.free(raw);
            _ = try @import("index_field_ledger.zig").open(raw, try one.inputForField("body"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}
