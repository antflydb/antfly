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

//! Immutable named-type identities and enum symbol layouts. Catalog names,
//! physical value identities and ordering keys are deliberately independent.
const std = @import("std");
const order = @import("../common/sql_enum_order.zig");
const A = std.mem.Allocator;
pub const max_labels = 4096;
pub const max_label_bytes = 63;
pub const max_order_bytes = 8192;
pub const max_encoded_bytes = 1024 * 1024;

pub const Identity = struct {
    id: u64,
    namespace_id: u64,
    origin_id: [16]u8,
    generation: u32,
    oid: u32,
    array_oid: u32,

    pub fn init(id: u64, namespace_id: u64, generation: u32, origin_id: [16]u8) !Identity {
        // Reserve a disjoint pair above PostgreSQL's builtin OID space. The
        // catalog allocator never reuses IDs, including after retirement.
        if (id == 0 or namespace_id == 0 or generation == 0 or std.mem.allEqual(u8, &origin_id, 0) or id > (std.math.maxInt(u32) - 16385) / 2) return error.InvalidSqlNamedType;
        return .{ .id = id, .namespace_id = namespace_id, .origin_id = origin_id, .generation = generation, .oid = @intCast(16384 + id * 2), .array_oid = @intCast(16385 + id * 2) };
    }
    /// A restore may remap local catalog routing and wire OIDs. The logical
    /// origin remains unchanged, so physical migration cannot change values.
    pub fn remap(self: Identity, id: u64, namespace_id: u64) !Identity {
        try self.validate();
        return init(id, namespace_id, self.generation, self.origin_id);
    }
    pub fn validate(self: Identity) !void {
        if (!std.meta.eql(self, try init(self.id, self.namespace_id, self.generation, self.origin_id))) return error.InvalidSqlNamedType;
    }
};

pub const Label = struct {
    id: u32,
    text: []const u8,
    order_key: []const u8,

    pub fn jsonStringify(self: Label, writer: anytype) !void {
        const Bytes = struct {
            value: []const u8,
            pub fn jsonStringify(bytes: @This(), output: anytype) !void {
                try output.beginArray();
                for (bytes.value) |byte| try output.write(byte);
                try output.endArray();
            }
        };
        // Rank bytes are not text. Always emit an array, even when a rank
        // happens to be valid UTF-8, so the durable contract has one shape.
        try writer.write(.{ .id = self.id, .text = self.text, .order_key = Bytes{ .value = self.order_key } });
    }
};
pub fn validateLabel(text: []const u8) !void {
    if (text.len > max_label_bytes) return error.SqlInvalidEnumLabel;
    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidSqlNamedType;
}
pub const Placement = union(enum) { end, before: []const u8, after: []const u8 };
pub const Definition = union(enum) {
    /// Append-only symbol-ID directory. BEFORE/AFTER never reorders this array.
    labels: []const Label,
    domain: @import("domain_types.zig").Definition,

    pub fn byId(self: Definition, id: u32) !Label {
        if (self != .labels) return error.InvalidSqlEnumSymbol;
        if (id == 0 or id > self.labels.len or self.labels[id - 1].id != id) return error.InvalidSqlEnumSymbol;
        return self.labels[id - 1];
    }
    pub fn validate(self: Definition, a: A) !void {
        if (self == .domain) return self.domain.validate(a);
        var view = try View.init(a, self);
        defer view.deinit(a);
    }
};

/// Prepared once per immutable definition. Symbol lookup is O(1), label input
/// is O(log n), and comparisons use stable rank bytes without catalog callbacks.
pub const View = struct {
    definition: Definition,
    names: []u32,
    ordered: []u32,

    pub fn init(a: A, definition: Definition) !View {
        if (definition != .labels) return error.SqlTypeMismatch;
        if (definition.labels.len > max_labels) return error.SqlProgramLimitExceeded;
        var bytes: usize = 0;
        for (definition.labels, 0..) |label, i| {
            if (label.id != i + 1 or label.text.len > max_label_bytes or !std.unicode.utf8ValidateSlice(label.text) or std.mem.indexOfScalar(u8, label.text, 0) != null) return error.InvalidSqlNamedType;
            try order.validate(label.order_key, max_order_bytes);
            bytes = std.math.add(usize, bytes, label.text.len + label.order_key.len) catch return error.SqlProgramLimitExceeded;
            if (bytes > max_encoded_bytes) return error.SqlProgramLimitExceeded;
        }
        const names = try a.alloc(u32, definition.labels.len);
        errdefer a.free(names);
        const ordered = try a.alloc(u32, definition.labels.len);
        errdefer a.free(ordered);
        for (names, ordered, 0..) |*name, *rank, i| {
            name.* = @intCast(i);
            rank.* = @intCast(i);
        }
        const Sort = struct {
            fn name(def: Definition, left: u32, right: u32) bool {
                return std.mem.order(u8, def.labels[left].text, def.labels[right].text) == .lt;
            }
            fn rank(def: Definition, left: u32, right: u32) bool {
                return std.mem.order(u8, def.labels[left].order_key, def.labels[right].order_key) == .lt;
            }
        };
        std.mem.sort(u32, names, definition, Sort.name);
        std.mem.sort(u32, ordered, definition, Sort.rank);
        if (names.len > 1) for (1..names.len) |i| {
            if (std.mem.eql(u8, definition.labels[names[i - 1]].text, definition.labels[names[i]].text)) return error.SqlDuplicateEnumLabel;
            if (std.mem.eql(u8, definition.labels[ordered[i - 1]].order_key, definition.labels[ordered[i]].order_key)) return error.InvalidSqlEnumOrder;
        };
        return .{ .definition = definition, .names = names, .ordered = ordered };
    }
    pub fn deinit(self: *View, a: A) void {
        a.free(self.names);
        a.free(self.ordered);
        self.* = undefined;
    }
    pub fn find(self: View, text: []const u8) ?Label {
        var begin: usize = 0;
        var end = self.names.len;
        while (begin < end) {
            const middle = begin + (end - begin) / 2;
            const label = self.definition.labels[self.names[middle]];
            switch (std.mem.order(u8, text, label.text)) {
                .eq => return label,
                .lt => end = middle,
                .gt => begin = middle + 1,
            }
        }
        return null;
    }
    pub fn compare(self: View, left: u32, right: u32) !std.math.Order {
        return std.mem.order(u8, (try self.definition.byId(left)).order_key, (try self.definition.byId(right)).order_key);
    }
};

pub const Owned = struct {
    definition: Definition,
    alloc: A,

    pub fn deinit(self: *Owned) void {
        for (self.definition.labels) |label| {
            self.alloc.free(label.text);
            self.alloc.free(label.order_key);
        }
        self.alloc.free(self.definition.labels);
        self.* = undefined;
    }
    pub fn create(a: A, texts: []const []const u8) !Owned {
        if (texts.len > max_labels) return error.SqlProgramLimitExceeded;
        for (texts) |text| try validateLabel(text);
        const labels = try a.alloc(Label, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (labels[0..initialized]) |label| {
                a.free(label.text);
                a.free(label.order_key);
            }
            a.free(labels);
        }
        for (texts, 0..) |text, i| {
            const copied = try a.dupe(u8, text);
            errdefer a.free(copied);
            const key = try order.between(a, if (i == 0) null else labels[i - 1].order_key, null, max_order_bytes);
            labels[i] = .{ .id = @intCast(i + 1), .text = copied, .order_key = key };
            initialized += 1;
        }
        const definition: Definition = .{ .labels = labels };
        try definition.validate(a);
        return .{ .definition = definition, .alloc = a };
    }
    /// Build an owned immutable successor. The predecessor stays readable even
    /// if a later allocation or validation fails. Publication owns the CAS.
    pub fn add(a: A, previous: Definition, text: []const u8, placement: Placement) !Owned {
        try validateLabel(text);
        var view = try View.init(a, previous);
        defer view.deinit(a);
        if (view.find(text) != null) return error.SqlDuplicateEnumLabel;
        if (previous.labels.len >= max_labels) return error.SqlProgramLimitExceeded;
        const adjacent: ?Label = switch (placement) {
            .end => null,
            .before, .after => |neighbor| view.find(neighbor) orelse return error.SqlEnumLabelNotFound,
        };
        var index = view.ordered.len;
        if (adjacent) |neighbor| {
            for (view.ordered, 0..) |symbol, i| if (previous.labels[symbol].id == neighbor.id) {
                index = i + @as(usize, if (placement == .after) 1 else 0);
                break;
            };
        }
        const key = try order.between(a, if (index == 0) null else previous.labels[view.ordered[index - 1]].order_key, if (index == view.ordered.len) null else previous.labels[view.ordered[index]].order_key, max_order_bytes);
        defer a.free(key);
        const labels = try a.alloc(Label, previous.labels.len + 1);
        var initialized: usize = 0;
        errdefer {
            for (labels[0..initialized]) |label| {
                a.free(label.text);
                a.free(label.order_key);
            }
            a.free(labels);
        }
        for (labels, 0..) |*label, i| {
            const source: Label = if (i < previous.labels.len) previous.labels[i] else .{ .id = @intCast(i + 1), .text = text, .order_key = key };
            const copied = try a.dupe(u8, source.text);
            errdefer a.free(copied);
            const rank = try a.dupe(u8, source.order_key);
            label.* = .{ .id = source.id, .text = copied, .order_key = rank };
            initialized += 1;
        }
        const definition: Definition = .{ .labels = labels };
        try definition.validate(a);
        return .{ .definition = definition, .alloc = a };
    }
};

pub const Record = struct {
    version: u16 = 1,
    identity: Identity,
    definition: Definition,
    owner: @import("owners.zig").Owner,

    /// Equality and ordered-key compatibility across independently pinned
    /// epochs. ADD VALUE may append symbols; the shared history must preserve
    /// every label and rank. Local IDs and durable origins both define type
    /// identity, independently of column names and catalog ownership.
    pub fn sameValueDomain(self: Record, other: Record) bool {
        if (self.definition != .labels or other.definition != .labels) return false;
        if (self.identity.id != other.identity.id or !std.mem.eql(u8, &self.identity.origin_id, &other.identity.origin_id)) return false;
        const common = @min(self.definition.labels.len, other.definition.labels.len);
        for (self.definition.labels[0..common], other.definition.labels[0..common]) |left, right| {
            if (left.id != right.id or !std.mem.eql(u8, left.text, right.text) or !std.mem.eql(u8, left.order_key, right.order_key)) return false;
        }
        return true;
    }

    /// Compare validated immutable records before compatible publication.
    /// Older row layouts and ordered keys remain meaningful only if every
    /// existing symbol, label and rank is an exact prefix of the successor.
    /// This requires no allocator and never interprets catalog spelling.
    pub fn requireAdditiveSuccessor(self: Record, next: Record) !void {
        if (self.definition != .labels or next.definition != .labels) return error.InvalidSqlNamedType;
        try self.validateHeader();
        try next.validateHeader();
        var expected = self.identity;
        expected.generation = std.math.add(u32, expected.generation, 1) catch return error.InvalidSqlNamedType;
        if (!std.meta.eql(expected, next.identity) or next.definition.labels.len <= self.definition.labels.len or
            next.definition.labels.len > max_labels or !self.sameValueDomain(next)) return error.InvalidSqlNamedType;
    }

    /// SET NOT NULL changes one logical rule, never the physical base,
    /// defaults, checks, durable identity or owner. Historical rows require
    /// a separate, fenced validation proof before this epoch is published.
    pub fn requireDomainNotNullSuccessor(self: Record, next: Record) !void {
        try self.validateHeader();
        try next.validateHeader();
        if (self.definition != .domain or next.definition != .domain) return error.InvalidSqlNamedType;
        var identity = self.identity;
        identity.generation = std.math.add(u32, identity.generation, 1) catch return error.InvalidSqlNamedType;
        if (!std.meta.eql(identity, next.identity)) return error.InvalidSqlNamedType;
        const old = self.definition.domain;
        const new = next.definition.domain;
        if (old.not_null or !new.not_null or !std.meta.eql(old.base, new.base) or
            !std.meta.eql(old.numeric_modifier, new.numeric_modifier) or old.checks.len != new.checks.len)
            return error.InvalidSqlNamedType;
        if (old.default_expression) |expression| {
            if (!std.mem.eql(u8, expression, new.default_expression orelse return error.InvalidSqlNamedType)) return error.InvalidSqlNamedType;
        } else if (new.default_expression != null) return error.InvalidSqlNamedType;
        for (old.checks, new.checks) |left, right| {
            if (left.validated != right.validated or !std.mem.eql(u8, left.name, right.name) or
                !std.mem.eql(u8, left.expression, right.expression)) return error.InvalidSqlNamedType;
        }
    }

    pub fn requireEpochSuccessor(self: Record, next: Record) !void {
        if (self.definition == .domain) return self.requireDomainNotNullSuccessor(next);
        return self.requireAdditiveSuccessor(next);
    }

    /// Deep ownership for native schema epochs. No JSON arena or catalog
    /// cursor is retained by an installed schema.
    pub fn cloneOwned(self: Record, a: A) !Record {
        if (self.definition == .domain) {
            var definition = try self.definition.domain.cloneOwned(a);
            errdefer definition.deinitOwned(a);
            var result = self;
            result.definition = .{ .domain = definition };
            result.owner = switch (self.owner) {
                .auth_disabled => .auth_disabled,
                .user => |principal| .{ .user = .{ .name = try a.dupe(u8, principal.name), .instance_id = principal.instance_id } },
                .role => |principal| .{ .role = .{ .name = try a.dupe(u8, principal.name), .instance_id = principal.instance_id } },
            };
            return result;
        }
        const labels = try a.alloc(Label, self.definition.labels.len);
        var initialized: usize = 0;
        errdefer {
            for (labels[0..initialized]) |label| {
                a.free(label.text);
                a.free(label.order_key);
            }
            a.free(labels);
        }
        for (self.definition.labels, labels) |source, *label| {
            const text = try a.dupe(u8, source.text);
            errdefer a.free(text);
            label.* = .{ .id = source.id, .text = text, .order_key = try a.dupe(u8, source.order_key) };
            initialized += 1;
        }
        const owner: @FieldType(Record, "owner") = switch (self.owner) {
            .auth_disabled => .auth_disabled,
            .user => |principal| .{ .user = .{ .name = try a.dupe(u8, principal.name), .instance_id = principal.instance_id } },
            .role => |principal| .{ .role = .{ .name = try a.dupe(u8, principal.name), .instance_id = principal.instance_id } },
        };
        return .{ .version = self.version, .identity = self.identity, .definition = .{ .labels = labels }, .owner = owner };
    }
    pub fn deinitOwned(self: *Record, a: A) void {
        if (self.definition == .domain) {
            self.definition.domain.deinitOwned(a);
        } else {
            for (self.definition.labels) |label| {
                a.free(label.text);
                a.free(label.order_key);
            }
            a.free(self.definition.labels);
        }
        switch (self.owner) {
            .auth_disabled => {},
            .user, .role => |principal| a.free(principal.name),
        }
        self.* = undefined;
    }
    pub fn validateHeader(self: Record) !void {
        if (self.version != 1) return error.InvalidSqlNamedType;
        try self.identity.validate();
        switch (self.owner) {
            .auth_disabled => {},
            .user, .role => |principal| {
                if (principal.name.len == 0 or principal.name.len > 255 or !std.unicode.utf8ValidateSlice(principal.name) or std.mem.indexOfScalar(u8, principal.name, 0) != null or std.mem.allEqual(u8, &principal.instance_id, 0)) return error.InvalidSqlNamedType;
            },
        }
    }
    pub fn validate(self: Record, a: A) !void {
        try self.validateHeader();
        try self.definition.validate(a);
    }
    pub fn encode(self: Record, a: A) ![]u8 {
        try self.validate(a);
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_encoded_bytes) return error.SqlProgramLimitExceeded;
        return bytes;
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(Record) {
        if (bytes.len > max_encoded_bytes) return error.InvalidSqlNamedType;
        var parsed = std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSqlNamedType,
        };
        errdefer parsed.deinit();
        try parsed.value.validate(a);
        return parsed;
    }
};

test "SQL named enum views retain symbol identity through ordered insertion and cold reconstruction" {
    const a = std.testing.allocator;
    var original = try Owned.create(a, &.{ "queued", "processing", "done" });
    defer original.deinit();
    var next = try Owned.add(a, original.definition, "archived", .{ .after = "done" });
    defer next.deinit();
    var middle = try Owned.add(a, next.definition, "started", .{ .before = "processing" });
    defer middle.deinit();
    const record: Record = .{ .identity = try Identity.init(3, 2, 3, @splat(1)), .definition = middle.definition, .owner = .auth_disabled };
    const bytes = try record.encode(a);
    defer a.free(bytes);
    var restored = try Record.decode(a, bytes);
    defer restored.deinit();
    var view = try View.init(a, restored.value.definition);
    defer view.deinit(a);
    try std.testing.expectEqualStrings("started", view.find("started").?.text);
    try std.testing.expectEqual(@as(u32, 5), view.find("started").?.id);
    try std.testing.expect(try view.compare(1, 5) == .lt);
    try std.testing.expect(try view.compare(5, 2) == .lt);
    try std.testing.expect(try view.compare(3, 4) == .lt);
    for (original.definition.labels, middle.definition.labels[0..3]) |before, after| {
        try std.testing.expectEqual(before.id, after.id);
        try std.testing.expectEqualSlices(u8, before.order_key, after.order_key);
    }
    try std.testing.expect(view.find("Queued") == null);
    try std.testing.expectError(error.SqlDuplicateEnumLabel, Owned.add(a, original.definition, "done", .end));
    try std.testing.expectError(error.SqlEnumLabelNotFound, Owned.add(a, original.definition, "missing", .{ .after = "absent" }));
    try std.testing.expectError(error.InvalidSqlNamedType, Owned.create(a, &.{"bad\x00label"}));
    try std.testing.expectError(error.SqlDuplicateEnumLabel, Owned.create(a, &.{ "same", "same" }));
}

test "SQL named enum immutable successors unwind every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var original = try Owned.create(a, &.{ "queued", "processing", "done" });
            defer original.deinit();
            var next = try Owned.add(a, original.definition, "started", .{ .before = "processing" });
            defer next.deinit();
            const record: Record = .{ .identity = try Identity.init(3, 2, 2, @splat(1)), .definition = next.definition, .owner = .auth_disabled };
            const encoded = try record.encode(a);
            defer a.free(encoded);
            var decoded = try Record.decode(a, encoded);
            defer decoded.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL named enum additive publication rejects retired symbols rebound origins and changed ranks" {
    const a = std.testing.allocator;
    var first = try Owned.create(a, &.{ "z", "a" });
    defer first.deinit();
    const before: Record = .{ .identity = try Identity.init(17, 2, 1, @splat(8)), .definition = first.definition, .owner = .auth_disabled };
    var added = try Owned.add(a, first.definition, "middle", .{ .before = "a" });
    defer added.deinit();
    var after = before;
    after.identity.generation += 1;
    after.definition = added.definition;
    try before.requireAdditiveSuccessor(after);
    var forged = after;
    forged.identity.origin_id = @splat(9);
    try std.testing.expectError(error.InvalidSqlNamedType, before.requireAdditiveSuccessor(forged));
    forged = after;
    forged.identity.namespace_id += 1;
    try std.testing.expectError(error.InvalidSqlNamedType, before.requireAdditiveSuccessor(forged));
    forged = after;
    forged.identity.generation += 1;
    try std.testing.expectError(error.InvalidSqlNamedType, before.requireAdditiveSuccessor(forged));
    forged = after;
    forged.definition = first.definition;
    try std.testing.expectError(error.InvalidSqlNamedType, before.requireAdditiveSuccessor(forged));
    const labels = try a.dupe(Label, added.definition.labels);
    defer a.free(labels);
    labels[0].order_key = labels[1].order_key;
    forged = after;
    forged.definition.labels = labels;
    try std.testing.expectError(error.InvalidSqlNamedType, before.requireAdditiveSuccessor(forged));
}

test "SQL domain named records retain identity and own cold contracts independently of enum symbols" {
    const a = std.testing.allocator;
    const record: Record = .{
        .identity = try Identity.init(23, 2, 1, @splat(9)),
        .owner = .auth_disabled,
        .definition = .{ .domain = .{ .base = .{ .builtin = .text }, .not_null = true, .default_expression =
        \\{"op":"literal","type":"string","value":"queued"}
        } },
    };
    const bytes = try record.encode(a);
    defer a.free(bytes);
    var cold = try Record.decode(a, bytes);
    defer cold.deinit();
    var owned = try cold.value.cloneOwned(a);
    defer owned.deinitOwned(a);
    try std.testing.expectEqualDeep(record, owned);
    try std.testing.expectEqualDeep(record.identity, cold.value.identity);
    try std.testing.expectError(error.SqlTypeMismatch, View.init(a, owned.definition));
    try std.testing.expectError(error.InvalidSqlEnumSymbol, owned.definition.byId(1));
    try std.testing.expectError(error.InvalidSqlNamedType, owned.requireAdditiveSuccessor(owned));
    try std.testing.expect(!owned.sameValueDomain(owned));
}
