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

//! Inspection status contains no original or matched values. Storage callers
//! bind it to accepted producer provenance in the same transaction as source.
const std = @import("std");
const contract = @import("contract.zig");
const A = std.mem.Allocator;

pub const CategoryCount = struct { category: []const u8, count: u64 };
pub const FieldCount = struct { field: []const u8, findings: u64 };
pub const Wire = struct {
    policy_version: []const u8,
    current: bool,
    failed: bool,
    error_code: ?[]const u8 = null,
    protected_ready: bool,
    categories: []const CategoryCount,
    fields: []const FieldCount = &.{},
};

pub fn encode(a: A, version: [64]u8, observation: Observation) ![]u8 {
    const categories = try a.alloc(CategoryCount, observation.categories.count());
    defer a.free(categories);
    var it = observation.categories.iterator();
    var i: usize = 0;
    while (it.next()) |entry| : (i += 1) categories[i] = .{ .category = entry.key_ptr.*, .count = entry.value_ptr.* };
    const fields = try a.alloc(FieldCount, observation.fields.count());
    defer a.free(fields);
    var names = observation.fields.iterator();
    i = 0;
    while (names.next()) |entry| : (i += 1) fields[i] = .{ .field = entry.key_ptr.*, .findings = entry.value_ptr.* };
    return std.json.Stringify.valueAlloc(a, Wire{
        .policy_version = &version,
        .current = observation.current,
        .failed = observation.failed,
        .error_code = observation.error_code,
        .protected_ready = observation.protected_ready,
        .categories = categories,
        .fields = fields,
    }, .{});
}

pub const Totals = struct {
    total: u64 = 0,
    current: u64 = 0,
    failed: u64 = 0,
    with_findings: u64 = 0,
    protected_ready: u64 = 0,
    categories: std.StringHashMapUnmanaged(u64) = .empty,
    fields: std.StringHashMapUnmanaged(u64) = .empty,

    pub fn deinit(self: *Totals, a: A) void {
        var names = self.categories.keyIterator();
        while (names.next()) |name| a.free(name.*);
        self.categories.deinit(a);
        var field_names = self.fields.keyIterator();
        while (field_names.next()) |name| a.free(name.*);
        self.fields.deinit(a);
    }

    pub fn add(self: *Totals, a: A, version: [64]u8, row: Wire) !void {
        if (!std.mem.eql(u8, &version, row.policy_version)) return error.PrivacyStale;
        if ((row.failed and row.current) or (row.protected_ready and !row.current) or (!row.current and (row.categories.len != 0 or row.fields.len != 0))) return error.InvalidPrivacyReceipt;
        self.total = try std.math.add(u64, self.total, 1);
        self.current = try std.math.add(u64, self.current, @intFromBool(row.current));
        self.failed = try std.math.add(u64, self.failed, @intFromBool(row.failed));
        self.protected_ready = try std.math.add(u64, self.protected_ready, @intFromBool(row.protected_ready));
        self.with_findings = try std.math.add(u64, self.with_findings, @intFromBool(row.categories.len != 0));
        for (row.fields) |field| {
            if (field.field.len == 0 or field.field.len > 512 or field.field[0] == '_' or !std.unicode.utf8ValidateSlice(field.field)) return error.InvalidPrivacyReceipt;
            if (self.fields.getPtr(field.field)) |count| count.* = try std.math.add(u64, count.*, field.findings) else {
                const owned = try a.dupe(u8, field.field);
                errdefer a.free(owned);
                try self.fields.put(a, owned, field.findings);
            }
        }
        for (row.categories) |category| {
            if (!contract.validCategory(category.category) or category.count == 0) return error.InvalidPrivacyReceipt;
            if (self.categories.getPtr(category.category)) |count| {
                count.* = try std.math.add(u64, count.*, category.count);
            } else {
                const name = try a.dupe(u8, category.category);
                errdefer a.free(name);
                try self.categories.put(a, name, category.count);
            }
        }
    }
};

pub const Observation = struct {
    error_code: ?[]const u8 = null,
    current: bool = false,
    failed: bool = false,
    protected_ready: bool = false,
    categories: std.StringHashMapUnmanaged(u64) = .empty,
    fields: std.StringHashMapUnmanaged(u64) = .empty,

    pub fn deinit(self: *Observation, a: A) void {
        if (self.error_code) |value| a.free(value);
        var names = self.categories.keyIterator();
        while (names.next()) |name| a.free(name.*);
        self.categories.deinit(a);
        var field_names = self.fields.keyIterator();
        while (field_names.next()) |name| a.free(name.*);
        self.fields.deinit(a);
    }
};

pub fn inspect(a: A, policy: contract.Policy, source: []const u8, artifact: []const u8) !Observation {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const original = (try std.json.parseFromSlice(std.json.Value, scratch, source, .{ .parse_numbers = false })).value;
    const projected = (try std.json.parseFromSlice(std.json.Value, scratch, artifact, .{ .parse_numbers = false })).value;
    if (original != .object or projected != .object) return error.InvalidPrivacyReceipt;
    const receipt = projected.object.get("_privacy") orelse return error.InvalidPrivacyReceipt;
    if (receipt != .object) return error.InvalidPrivacyReceipt;
    const version = try policy.identity(scratch);
    const recorded = receipt.object.get("policy_version") orelse return error.InvalidPrivacyReceipt;
    const fingerprint = receipt.object.get("source_fingerprint") orelse return error.InvalidPrivacyReceipt;
    const canonical = try std.json.Stringify.valueAlloc(scratch, original, .{});
    if (recorded != .string or fingerprint != .string) return error.InvalidPrivacyReceipt;
    if (!std.mem.eql(u8, recorded.string, &version) or !std.mem.eql(u8, fingerprint.string, &contract.fingerprint(canonical))) return .{};
    const fields = receipt.object.get("fields") orelse return error.InvalidPrivacyReceipt;
    if (fields != .array or fields.array.items.len != policy.fields.len) return error.InvalidPrivacyReceipt;
    const labels = try policy.labels(scratch);
    var observation: Observation = .{};
    errdefer observation.deinit(a);
    for (policy.fields) |name| {
        var matches: usize = 0;
        for (fields.array.items) |field| {
            if (field != .object) return error.InvalidPrivacyReceipt;
            const actual_name = field.object.get("field") orelse return error.InvalidPrivacyReceipt;
            if (actual_name != .string) return error.InvalidPrivacyReceipt;
            if (!std.mem.eql(u8, actual_name.string, name)) continue;
            matches += 1;
            const value = original.object.get(name) orelse .null;
            if (value != .string and value != .null) return error.InvalidPrivacyFieldType;
            const text = if (value == .string) value.string else "";
            const proof = try std.json.parseFromValue(contract.Receipt, scratch, field.object.get("receipt") orelse return error.InvalidPrivacyReceipt, .{});
            if (!proof.value.current(version, text, try policy.revision())) {
                observation.deinit(a);
                return .{};
            }
            const findings = field.object.get("findings") orelse return error.InvalidPrivacyReceipt;
            if (findings != .array) return error.InvalidPrivacyReceipt;
            const field_name = try a.dupe(u8, name);
            observation.fields.put(a, field_name, findings.array.items.len) catch |err| {
                a.free(field_name);
                return err;
            };
            for (findings.array.items) |finding| {
                const parsed = try std.json.parseFromValue(contract.Finding, scratch, finding, .{});
                try parsed.value.validate(text, null);
                if (!std.mem.eql(u8, parsed.value.field, name)) return error.InvalidPrivacyReceipt;
                for (labels) |label| {
                    if (std.mem.eql(u8, label.name, parsed.value.category)) break;
                } else return error.InvalidPrivacyCategory;
                if (observation.categories.getPtr(parsed.value.category)) |count| {
                    count.* = try std.math.add(u64, count.*, 1);
                } else {
                    const key = try a.dupe(u8, parsed.value.category);
                    errdefer a.free(key);
                    try observation.categories.put(a, key, 1);
                }
            }
        }
        if (matches != 1) return error.InvalidPrivacyReceipt;
    }
    observation.current = true;
    return observation;
}

test "inspection status invalidates all counts when any selected or retained source changes" {
    const a = std.testing.allocator;
    const document = @import("document.zig");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const policy = contract.Policy{ .mode = .monitor, .fields = &.{ "body", "subject" } };
    const producer = try document.resolve(arena.allocator(), policy, true);
    const config = try std.json.Stringify.valueAlloc(arena.allocator(), producer.config, .{});
    const Detector = struct {
        fn call(_: *anyopaque, out: A, _: contract.Producer, _: []const u8) ![]u8 {
            return out.dupe(u8, "{\"findings\":[],\"long_document\":{\"window_count\":1}}");
        }
        fn check(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    const original = "{\"body\":\"hello\",\"subject\":\"world\",\"number\":9007199254740993}";
    const artifact = try document.produce(a, config, original, .{ .ptr = &context, .produce = Detector.call, .check = Detector.check });
    defer a.free(artifact);
    var current = try inspect(a, policy, original, artifact);
    defer current.deinit(a);
    try std.testing.expect(current.current);
    try std.testing.expectEqual(@as(u32, 2), current.fields.count());
    try std.testing.expectEqual(@as(u64, 0), current.fields.get("body").?);
    const encoded = try encode(a, try policy.identity(a), current);
    defer a.free(encoded);
    var wire = try std.json.parseFromSlice(Wire, a, encoded, .{});
    defer wire.deinit();
    var totals: Totals = .{};
    defer totals.deinit(a);
    try totals.add(a, try policy.identity(a), wire.value);
    try std.testing.expectEqual(@as(u64, 0), totals.fields.get("subject").?);
    var changed = try inspect(a, policy, "{\"body\":\"hello\",\"subject\":\"changed\",\"number\":9007199254740993}", artifact);
    defer changed.deinit(a);
    try std.testing.expect(!changed.current);
    try std.testing.expectEqual(@as(u32, 0), changed.categories.count());
    try std.testing.expectEqual(@as(u32, 0), changed.fields.count());
}
