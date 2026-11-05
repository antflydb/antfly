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

//! Privacy's durable contract. Coordinates are half-open UTF-8 byte offsets
//! into an immutable source. Matched values never belong in durable findings.
const std = @import("std");
const A = std.mem.Allocator;
pub const default_model = "fastino/gliner2-privacy-filter-PII-multi";
pub const default_revision = "1cb4166094dc58fa8d836429f060d6c95f62b495";
pub const contract_version = 1;
pub const max_categories = 128;
pub const Mode = enum { monitor, redact_for_search_and_ai, redact_before_storage };
pub const Sensitivity = enum { conservative, balanced, thorough };
pub const Replacement = enum { category, mask, remove };
pub const Group = enum { contact_details, personal_identifiers, financial, credentials };

pub const Category = struct {
    name: []const u8,
    description: []const u8 = "",
    threshold: ?f32 = null,
};
pub const Producer = struct {
    type: enum { extractor, generator } = .extractor,
    config: std.json.Value = .null,
    /// Immutable deployment/model revision. Mandatory for custom producers;
    /// mutable model aliases alone cannot certify a completed inspection.
    revision: []const u8 = "",
};
pub const Execution = struct {
    batch_items: u8 = 8,
    max_attempts: u8 = 3,
};
pub const Policy = struct {
    mode: Mode,
    fields: []const []const u8,
    category_groups: []const Group = &.{ .contact_details, .personal_identifiers, .financial, .credentials },
    categories: []const Category = &.{},
    sensitivity: Sensitivity = .balanced,
    replacement: Replacement = .category,
    producer: ?Producer = null,
    execution: Execution = .{},

    pub fn revision(self: Policy) ![]const u8 {
        const producer = self.producer orelse return default_revision;
        const config = producer.config;
        if (config == .object and producer.type == .extractor) {
            const model = config.object.get("model") orelse .null;
            const provider = config.object.get("provider") orelse .null;
            if (model == .string and provider == .string and std.mem.eql(u8, model.string, default_model) and std.mem.eql(u8, provider.string, "antfly")) {
                if (producer.revision.len != 0 and !std.mem.eql(u8, producer.revision, default_revision)) return error.PrivacyProducerRevisionMismatch;
                return default_revision;
            }
        }
        if (producer.revision.len == 0) return error.PrivacyProducerRevisionRequired;
        return producer.revision;
    }

    pub fn threshold(self: Policy) f32 {
        return switch (self.sensitivity) {
            .conservative => 0.75,
            .balanced => 0.5,
            .thorough => 0.3,
        };
    }

    pub fn validate(self: Policy) !void {
        if (self.fields.len == 0 or self.fields.len > 64) return error.InvalidPrivacyFields;
        for (self.fields, 0..) |field, i| {
            if (field.len == 0 or field.len > 512 or field[0] == '_' or !std.unicode.utf8ValidateSlice(field)) return error.InvalidPrivacyFields;
            for (self.fields[0..i]) |prior| if (std.mem.eql(u8, prior, field)) return error.InvalidPrivacyFields;
        }
        if (self.category_groups.len == 0 and self.categories.len == 0) return error.InvalidPrivacyCategories;
        if (self.category_groups.len > 4 or self.categories.len > max_categories) return error.InvalidPrivacyCategories;
        for (self.category_groups, 0..) |group, i| for (self.category_groups[0..i]) |prior| {
            if (group == prior) return error.InvalidPrivacyCategories;
        };
        for (self.categories, 0..) |category, i| {
            if (!validCategory(category.name)) return error.InvalidPrivacyCategories;
            if (category.threshold) |value| if (!std.math.isFinite(value) or value < 0 or value > 1) return error.InvalidPrivacyThreshold;
            for (self.categories[0..i]) |prior| if (std.mem.eql(u8, prior.name, category.name)) return error.InvalidPrivacyCategories;
        }
        // Canonical policies expand groups into explicit categories. Apply the
        // same bound to both shapes before preview or producer admission.
        var resolved_count = self.categories.len;
        for (self.category_groups, 0..) |group, i| for (groupLabels(group)) |name| {
            const overridden = for (self.categories) |category| {
                if (std.mem.eql(u8, name, category.name)) break true;
            } else false;
            if (overridden) continue;
            const prior_group = for (self.category_groups[0..i]) |prior| {
                const included = for (groupLabels(prior)) |prior_name| {
                    if (std.mem.eql(u8, name, prior_name)) break true;
                } else false;
                if (included) break true;
            } else false;
            if (!prior_group) resolved_count += 1;
        };
        if (resolved_count > max_categories) return error.InvalidPrivacyCategories;
        if (self.execution.batch_items == 0 or self.execution.batch_items > 8 or self.execution.max_attempts == 0 or
            self.execution.max_attempts > 5) return error.InvalidPrivacyExecution;
        if (self.producer) |producer| {
            if (producer.config != .object) return error.InvalidPrivacyProducer;
            try validateSecrets(producer.config);
            _ = try self.revision();
            if (producer.type == .extractor) {
                const config = producer.config.object;
                if (config.get("method")) |method| {
                    if (method != .string or !std.mem.eql(u8, method.string, "generated") or config.get("generator") == null) return error.InvalidPrivacyProducer;
                } else if (config.get("provider") == null) return error.InvalidPrivacyProducer;
                if (config.get("options")) |options| {
                    if (options != .object) return error.InvalidPrivacyProducer;
                    inline for (.{"include_spans"}) |name| if (options.object.get(name)) |value| {
                        if (value != .bool or !value.bool) return error.ConflictingPrivacyProducerOptions;
                    };
                    if (options.object.get("offset_unit")) |unit| {
                        if (unit != .string or !std.mem.eql(u8, unit.string, "utf8_bytes")) return error.ConflictingPrivacyProducerOptions;
                    }
                }
            } else if (producer.config.object.get("generator") == null or producer.config.object.get("output_schema") == null) return error.InvalidPrivacyProducer;
        }
    }

    pub fn labels(self: Policy, a: A) ![]Category {
        var result: std.ArrayList(Category) = .empty;
        errdefer result.deinit(a);
        for (self.category_groups) |group| for (groupLabels(group)) |name| {
            try appendCategory(a, &result, .{ .name = name, .threshold = self.threshold() });
        };
        for (self.categories) |category| try appendCategory(a, &result, .{
            .name = category.name,
            .description = category.description,
            .threshold = category.threshold orelse self.threshold(),
        });
        std.mem.sort(Category, result.items, {}, struct {
            fn less(_: void, l: Category, r: Category) bool {
                return std.mem.lessThan(u8, l.name, r.name);
            }
        }.less);
        return result.toOwnedSlice(a);
    }

    /// Arena-owned executable policy. Authored ordering and sensitivity aliases
    /// must not change producer/catalog identity for the same semantic policy.
    pub fn executable(self: Policy, a: A) !Policy {
        try self.validate();
        var result = self;
        const fields = try a.dupe([]const u8, self.fields);
        std.mem.sort([]const u8, fields, {}, struct {
            fn less(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.lessThan(u8, l, r);
            }
        }.less);
        result.fields = fields;
        result.categories = try self.labels(a);
        result.category_groups = &.{};
        result.sensitivity = .balanced;
        result.execution = .{};
        if (self.producer) |producer| {
            const raw = try std.json.Stringify.valueAlloc(a, producer.config, .{});
            var tree = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
            try canonicalize(a, &tree.value);
            result.producer = producer;
            result.producer.?.config = tree.value;
        }
        return result;
    }

    /// Canonical semantic identity excludes scheduling/retry controls. Recursively
    /// sorted JSON keys keep equivalent producer configs stable across clients.
    pub fn identity(self: Policy, a: A) ![64]u8 {
        try self.validate();
        const resolved = try self.labels(a);
        defer a.free(resolved);
        const fields = try a.dupe([]const u8, self.fields);
        defer a.free(fields);
        std.mem.sort([]const u8, fields, {}, struct {
            fn less(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.lessThan(u8, l, r);
            }
        }.less);
        const raw = try std.json.Stringify.valueAlloc(a, .{
            .contract_version = contract_version,
            .mode = self.mode,
            .fields = fields,
            .categories = resolved,
            .replacement = self.replacement,
            .producer = self.producer,
            .default_model = if (self.producer == null) default_model else "",
            .revision = try self.revision(),
        }, .{});
        defer a.free(raw);
        var tree = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer tree.deinit();
        try canonicalize(tree.arena.allocator(), &tree.value);
        const canonical = try std.json.Stringify.valueAlloc(a, tree.value, .{});
        defer a.free(canonical);
        return fingerprint(canonical);
    }
};

/// Immutable request/owner-snapshot binding. Validation, canonical identity and
/// resolved labels belong to this cut, never to each document in a scan.
/// Strings borrow the policy; the caller owns the preparation allocator.
pub const PreparedPolicy = struct {
    policy: Policy,
    version: [64]u8,
    categories: []const Category,
    revision: []const u8,

    pub fn init(a: A, policy: Policy) !PreparedPolicy {
        return .{ .policy = policy, .version = try policy.identity(a), .categories = try policy.labels(a), .revision = try policy.revision() };
    }
};

fn appendCategory(a: A, list: *std.ArrayList(Category), category: Category) !void {
    for (list.items) |*existing| if (std.mem.eql(u8, existing.name, category.name)) {
        existing.* = category;
        return;
    };
    try list.append(a, category);
}
pub fn groupLabels(group: Group) []const []const u8 {
    return switch (group) {
        .contact_details => &.{ "email", "phone_number", "address", "street_address", "city", "state_or_region", "postal_code", "country", "ip_address" },
        .personal_identifiers => &.{ "person", "full_name", "first_name", "middle_name", "last_name", "date_of_birth", "government_id", "national_id_number", "passport_number", "drivers_license_number", "license_number", "tax_id", "tax_number", "username", "account_id", "sensitive_account_id", "sensitive_date", "document_date", "expiration_date", "transaction_date" },
        .financial => &.{ "bank_account", "account_number", "routing_number", "iban", "payment_card", "card_number", "card_expiry", "card_cvv" },
        .credentials => &.{ "password", "secret", "api_key", "access_token", "recovery_code" },
    };
}
pub fn validCategory(name: []const u8) bool {
    if (name.len == 0 or name.len > 128) return false;
    for (name) |c| if (!std.ascii.isAlphanumeric(c) and c != '_') return false;
    return true;
}
fn validateSecrets(value: std.json.Value) error{PrivacyLiteralCredential}!void {
    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                const name = entry.key_ptr.*;
                if (std.mem.eql(u8, name, "api_key") or std.mem.eql(u8, name, "bearer_token") or std.mem.eql(u8, name, "password") or std.mem.eql(u8, name, "access_key") or std.mem.eql(u8, name, "secret_key")) {
                    const secret = entry.value_ptr.*;
                    if (secret != .string or (secret.string.len > 0 and !(std.mem.startsWith(u8, secret.string, "${") and std.mem.endsWith(u8, secret.string, "}")))) return error.PrivacyLiteralCredential;
                }
                if (!std.mem.eql(u8, name, "schema") and !std.mem.eql(u8, name, "output_schema") and !std.mem.eql(u8, name, "entity_definitions")) try validateSecrets(entry.value_ptr.*);
            }
        },
        .array => |array| for (array.items) |item| try validateSecrets(item),
        else => {},
    }
}
fn canonicalize(a: A, value: *std.json.Value) !void {
    switch (value.*) {
        .object => |*object| {
            const names = try a.dupe([]const u8, object.keys());
            defer a.free(names);
            std.mem.sort([]const u8, names, {}, struct {
                fn less(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.less);
            var sorted: std.json.ObjectMap = .empty;
            errdefer sorted.deinit(a);
            for (names) |name| {
                var child = object.get(name).?;
                try canonicalize(a, &child);
                try sorted.put(a, name, child);
            }
            object.deinit(a);
            object.* = sorted;
        },
        .array => |*array| for (array.items) |*child| try canonicalize(a, child),
        else => {},
    }
}
pub fn fingerprint(bytes: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

pub const Finding = struct {
    field: []const u8,
    category: []const u8,
    start: usize,
    end: usize,
    confidence: ?f32 = null,

    pub fn jsonStringify(self: Finding, writer: anytype) !void {
        try writer.beginObject();
        inline for (.{ "field", "category", "start", "end" }) |name| {
            try writer.objectField(name);
            try writer.write(@field(self, name));
        }
        if (self.confidence) |score| {
            try writer.objectField("confidence");
            try writer.write(score);
        }
        try writer.endObject();
    }

    pub fn validate(self: Finding, source: []const u8, evidence: ?[]const u8) !void {
        if (!validCategory(self.category) or self.start >= self.end or self.end > source.len or
            !boundary(source, self.start) or !boundary(source, self.end)) return error.InvalidPrivacySpan;
        if (self.confidence) |score| if (!std.math.isFinite(score) or score < 0 or score > 1) return error.InvalidPrivacyConfidence;
        if (evidence) |text| if (!std.mem.eql(u8, text, source[self.start..self.end])) return error.PrivacyEvidenceMismatch;
    }
};
pub fn boundary(source: []const u8, offset: usize) bool {
    return offset <= source.len and (offset == source.len or source[offset] & 0xc0 != 0x80);
}
pub const Receipt = struct {
    contract_version: u32 = contract_version,
    policy_version: [64]u8,
    source_fingerprint: [64]u8,
    producer_revision: []const u8,
    inspected_windows: u32,
    required_windows: u32,
    state: enum { inspected, published, failed },

    pub fn jsonStringify(self: Receipt, writer: anytype) !void {
        try writer.beginObject();
        inline for (comptime std.meta.fieldNames(Receipt)) |name| {
            try writer.objectField(name);
            if (comptime std.mem.eql(u8, name, "policy_version") or std.mem.eql(u8, name, "source_fingerprint")) {
                try writer.write(@field(self, name)[0..]);
            } else try writer.write(@field(self, name));
        }
        try writer.endObject();
    }

    pub fn current(self: Receipt, policy_version: [64]u8, source: []const u8, revision: []const u8) bool {
        return self.contract_version == contract_version and self.state != .failed and self.inspected_windows == self.required_windows and
            self.required_windows != 0 and std.mem.eql(u8, &self.policy_version, &policy_version) and
            std.mem.eql(u8, &self.source_fingerprint, &fingerprint(source)) and std.mem.eql(u8, self.producer_revision, revision);
    }
    pub fn protectedReady(self: Receipt, policy_version: [64]u8, source: []const u8, revision: []const u8) bool {
        return self.state == .published and self.current(policy_version, source, revision);
    }
};

/// Merge the union, never discard a lower-priority overlap: doing so could
/// expose the tail of a detected value. Keep original findings separately.
pub fn redact(a: A, source: []const u8, findings: []const Finding, replacement: Replacement) ![]u8 {
    if (!std.unicode.utf8ValidateSlice(source)) return error.InvalidPrivacySource;
    const ordered = try a.dupe(Finding, findings);
    defer a.free(ordered);
    for (ordered) |finding| try finding.validate(source, null);
    std.mem.sort(Finding, ordered, {}, struct {
        fn less(_: void, l: Finding, r: Finding) bool {
            if (l.start != r.start) return l.start < r.start;
            if (l.end != r.end) return l.end > r.end;
            return std.mem.lessThan(u8, l.category, r.category);
        }
    }.less);
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(a);
    var cursor: usize = 0;
    var i: usize = 0;
    while (i < ordered.len) {
        const first = ordered[i];
        var end = first.end;
        i += 1;
        while (i < ordered.len and ordered[i].start < end) : (i += 1) end = @max(end, ordered[i].end);
        try result.appendSlice(a, source[cursor..first.start]);
        switch (replacement) {
            .category => {
                try result.append(a, '[');
                for (first.category) |c| try result.append(a, std.ascii.toUpper(c));
                try result.append(a, ']');
            },
            .mask => try result.appendSlice(a, "[REDACTED]"),
            .remove => {},
        }
        cursor = end;
    }
    try result.appendSlice(a, source[cursor..]);
    return result.toOwnedSlice(a);
}

test "privacy overlap union, repeated mentions and Unicode boundaries" {
    const a = std.testing.allocator;
    const source = "é abcdef abcdef";
    const result = try redact(a, source, &.{
        .{ .field = "body", .category = "person", .start = 3, .end = 6 },
        .{ .field = "body", .category = "secret", .start = 5, .end = 9 },
        .{ .field = "body", .category = "secret", .start = 10, .end = 16 },
    }, .category);
    defer a.free(result);
    try std.testing.expectEqualStrings("é [PERSON] [SECRET]", result);
    try std.testing.expectError(error.InvalidPrivacySpan, (Finding{ .field = "body", .category = "person", .start = 1, .end = 2 }).validate(source, null));
    try std.testing.expectError(error.PrivacyEvidenceMismatch, (Finding{ .field = "body", .category = "person", .start = 3, .end = 6 }).validate(source, "def"));
}

test "privacy receipt invalidates immediately on source or policy change and inspection is not publication" {
    const version = fingerprint("policy");
    var receipt = Receipt{ .policy_version = version, .source_fingerprint = fingerprint("source"), .producer_revision = "v1", .inspected_windows = 2, .required_windows = 2, .state = .inspected };
    try std.testing.expect(receipt.current(version, "source", "v1"));
    try std.testing.expect(!receipt.protectedReady(version, "source", "v1"));
    receipt.state = .published;
    try std.testing.expect(receipt.protectedReady(version, "source", "v1"));
    try std.testing.expect(!receipt.current(version, "edited", "v1"));
    try std.testing.expect(!receipt.current(fingerprint("new"), "source", "v1"));
    try std.testing.expect(!receipt.current(version, "source", "v2"));
    receipt.inspected_windows = 1;
    try std.testing.expect(!receipt.current(version, "source", "v1"));
}

test "privacy semantic policy identity normalizes ordering and excludes execution tuning" {
    const a = std.testing.allocator;
    var first = try std.json.parseFromSlice(Policy, a, "{\"mode\":\"monitor\",\"fields\":[\"body\",\"subject\"],\"producer\":{\"revision\":\"v1\",\"config\":{\"provider\":\"mock\",\"model\":\"a\"}}}", .{});
    defer first.deinit();
    var second = try std.json.parseFromSlice(Policy, a, "{\"mode\":\"monitor\",\"fields\":[\"subject\",\"body\"],\"producer\":{\"revision\":\"v1\",\"config\":{\"model\":\"a\",\"provider\":\"mock\"}},\"execution\":{\"batch_items\":1}}", .{});
    defer second.deinit();
    try std.testing.expectEqual(try first.value.identity(a), try second.value.identity(a));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    try std.testing.expectEqualStrings(
        try std.json.Stringify.valueAlloc(scratch, try first.value.executable(scratch), .{}),
        try std.json.Stringify.valueAlloc(scratch, try second.value.executable(scratch), .{}),
    );
    second.value.mode = .redact_for_search_and_ai;
    try std.testing.expect(!std.mem.eql(u8, &(try first.value.identity(a)), &(try second.value.identity(a))));
}
