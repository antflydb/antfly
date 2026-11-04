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

//! A policy-managed document projection. This wraps ordinary artifact producers
//! and keeps downstream chunking/embedding downstream of deterministic redaction.
const std = @import("std");
pub const contract = @import("contract.zig");
const inspection = @import("inspection.zig");
const materialize = @import("materialize.zig");
const A = std.mem.Allocator;
pub const artifact_name = "_privacy_document";
pub const protected_artifact_prefix = "_privacy_document_";
pub fn protectedArtifactName(a: A, version: [64]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ protected_artifact_prefix, version });
}
pub fn isProtectedArtifactName(name: []const u8) bool {
    if (name.len != protected_artifact_prefix.len + 64 or !std.mem.startsWith(u8, name, protected_artifact_prefix)) return false;
    for (name[protected_artifact_prefix.len..]) |c| if (!std.ascii.isHex(c)) return false;
    return true;
}
pub fn isArtifactName(name: []const u8) bool {
    return std.mem.eql(u8, name, artifact_name) or isProtectedArtifactName(name);
}
pub fn consumerPolicyVersion(name: []const u8) ?[64]u8 {
    const prefix = "_privacy_";
    if (name.len <= prefix.len + 64 or !std.mem.startsWith(u8, name, prefix) or name[prefix.len + 64] != '_') return null;
    const version = name[prefix.len..][0..64];
    for (version) |c| if (!std.ascii.isHex(c)) return null;
    return version.*;
}
pub const source_field = "__antfly_privacy_source";
// A stable, input-free catalog guard survives replacement of versioned
// document projections. It never invokes a detector or produces an artifact.
pub const state_artifact_name = "_privacy_state";
pub const state_field = "__antfly_privacy_state";
pub fn isManagedArtifactName(name: []const u8) bool {
    return isArtifactName(name) or std.mem.eql(u8, name, state_artifact_name);
}
pub const config_key = "_antfly_privacy";
pub const Invoke = struct {
    ptr: *anyopaque,
    produce: *const fn (*anyopaque, A, contract.Producer, []const u8) anyerror![]u8,
    check: *const fn (*anyopaque) anyerror!void,
    /// Advance an invocation-owned field cohort only after evidence validates.
    accepted: ?*const fn (*anyopaque) void = null,
};

/// Borrowing JSON values is deliberate: callers allocate these resolved
/// configurations in an arena that lives through admission/execution.
pub fn resolve(a: A, policy: contract.Policy, managed: bool) !contract.Producer {
    try policy.validate();
    const categories = try policy.labels(a);
    var schema: std.json.Value = .{ .object = .empty };
    var names: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
    var definitions: std.json.Value = .{ .object = .empty };
    for (categories) |category| {
        try names.array.append(.{ .string = category.name });
        var definition: std.json.Value = .{ .object = .empty };
        try definition.object.put(a, "threshold", .{ .float = category.threshold.? });
        if (category.description.len != 0) try definition.object.put(a, "description", .{ .string = category.description });
        try definitions.object.put(a, category.name, definition);
    }
    try schema.object.put(a, "entities", names);
    try schema.object.put(a, "entity_definitions", definitions);
    var producer = policy.producer orelse contract.Producer{ .revision = contract.default_revision };
    producer.revision = try policy.revision();
    var config = if (producer.config == .null) (try std.json.parseFromSlice(std.json.Value, a, "{\"provider\":\"antfly\",\"model\":\"fastino/gliner2-privacy-filter-PII-multi\",\"schema_version\":2}", .{ .allocate = .alloc_always })).value else (try std.json.parseFromSlice(std.json.Value, a, try std.json.Stringify.valueAlloc(a, producer.config, .{}), .{ .allocate = .alloc_always })).value;
    if (config.object.contains(config_key)) return error.InvalidPrivacyProducer;
    if (producer.type == .extractor) {
        if (config.object.get("schema")) |authored| {
            if (!try equivalent(a, authored, schema)) return error.ConflictingPrivacyProducerSchema;
        }
        try config.object.put(a, "schema", schema);
        var options = config.object.get("options") orelse std.json.Value{ .object = .empty };
        if (options != .object) return error.InvalidPrivacyProducer;
        try options.object.put(a, "include_spans", .{ .bool = true });
        try options.object.put(a, "offset_unit", .{ .string = "utf8_bytes" });
        const model = config.object.get("model") orelse .null;
        const provider = config.object.get("provider") orelse .null;
        if (model == .string and provider == .string and std.mem.eql(u8, model.string, contract.default_model) and std.mem.eql(u8, provider.string, "antfly")) {
            try options.object.put(a, "include_confidence", .{ .bool = true });
            try options.object.put(a, "overlap", .{ .string = "allow" });
            var long: std.json.Value = .{ .object = .empty };
            try long.object.put(a, "mode", .{ .string = "window" });
            try long.object.put(a, "window_words", .{ .integer = 384 });
            try long.object.put(a, "overlap_words", .{ .integer = 32 });
            try long.object.put(a, "max_windows", .{ .integer = 64 });
            try options.object.put(a, "long_document", long);
        }
        try config.object.put(a, "options", options);
    }
    if (managed) {
        const raw = try std.json.Stringify.valueAlloc(a, policy, .{});
        const value = try std.json.parseFromSlice(std.json.Value, a, raw, .{ .allocate = .alloc_always });
        try config.object.put(a, config_key, value.value);
    }
    producer.config = config;
    return producer;
}

fn equivalent(a: A, l: std.json.Value, r: std.json.Value) error{OutOfMemory}!bool {
    if (std.meta.activeTag(l) != std.meta.activeTag(r)) return false;
    return switch (l) {
        .object => |object| same: {
            if (object.count() != r.object.count()) break :same false;
            var it = object.iterator();
            while (it.next()) |entry| if (!try equivalent(a, entry.value_ptr.*, r.object.get(entry.key_ptr.*) orelse break :same false)) break :same false;
            break :same true;
        },
        .array => |array| same: {
            if (array.items.len != r.array.items.len) break :same false;
            for (array.items, r.array.items) |lv, rv| if (!try equivalent(a, lv, rv)) break :same false;
            break :same true;
        },
        .string => |s| std.mem.eql(u8, s, r.string),
        .bool => |v| v == r.bool,
        .integer => |v| v == r.integer,
        .float => |v| v == r.float,
        .null => true,
        .number_string => |v| std.mem.eql(u8, v, r.number_string),
    };
}

pub fn isManaged(a: A, config_json: []const u8) !bool {
    if (std.mem.indexOf(u8, config_json, config_key) == null) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, config_json, .{});
    defer parsed.deinit();
    return parsed.value == .object and parsed.value.object.contains(config_key);
}

pub fn produce(a: A, config_json: []const u8, source: []const u8, invoke: Invoke) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const tree = try std.json.parseFromSlice(std.json.Value, scratch, config_json, .{});
    if (tree.value != .object) return error.InvalidPrivacyProducer;
    const policy = try std.json.parseFromValue(contract.Policy, scratch, tree.value.object.get(config_key) orelse return error.InvalidPrivacyProducer, .{});
    try policy.value.validate();
    const version = try policy.value.identity(scratch);
    const producer = try resolve(scratch, policy.value, false);
    const document = try std.json.parseFromSlice(std.json.Value, scratch, source, .{ .parse_numbers = false });
    if (document.value != .object or document.value.object.contains("_privacy")) return error.InvalidPrivacyDocument;
    var projected = document.value;
    var fields: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(scratch) };
    for (policy.value.fields) |field| {
        try invoke.check(invoke.ptr);
        const value = projected.object.get(field) orelse .null;
        const text = switch (value) {
            .null => "",
            .string => |v| v,
            else => return error.InvalidPrivacyFieldType,
        };
        const marker = materialize.Marker{ .policy_version = version, .field = field, .categories = try policy.value.labels(scratch), .replacement = policy.value.replacement, .producer_revision = producer.revision, .require_window_receipt = text.len != 0 and isNativeDefault(producer) };
        const validation = try std.json.Stringify.valueAlloc(scratch, .{ .privacy = marker }, .{});
        var attempt: u8 = 0;
        const result = retry: while (true) {
            try invoke.check(invoke.ptr);
            attempt += 1;
            // Transport/admission failures retain the artifact runtime's
            // backoff and retry ownership. Only invalid privacy evidence is
            // retried here; do not hammer a rate-limited provider or repeat an
            // invocation whose memory admission was denied.
            const produced = if (text.len == 0) "{\"findings\":[]}" else try invoke.produce(invoke.ptr, scratch, producer, text);
            const output = materialize.output(scratch, text, validation, produced) catch |err| {
                if (isControl(err) or err == error.OutOfMemory or attempt >= policy.value.execution.max_attempts) return err;
                continue;
            };
            break :retry (try std.json.parseFromSlice(std.json.Value, scratch, output.?, .{})).value;
        };
        if (value == .string) try projected.object.put(scratch, field, result.object.get("text").?);
        try fields.array.append(result);
        if (invoke.accepted) |accepted| accepted(invoke.ptr);
    }
    var receipt: std.json.Value = .{ .object = .empty };
    try receipt.object.put(scratch, "policy_version", .{ .string = try scratch.dupe(u8, &version) });
    try receipt.object.put(scratch, "source_fingerprint", .{ .string = try scratch.dupe(u8, &contract.fingerprint(source)) });
    try receipt.object.put(scratch, "fields", fields);
    try projected.object.put(scratch, "_privacy", receipt);
    return std.json.Stringify.valueAlloc(a, projected, .{});
}
fn isControl(err: anyerror) bool {
    return err == error.Canceled or err == error.Cancelled or err == error.DeadlineExceeded or err == error.Timeout;
}

fn isNativeDefault(producer: contract.Producer) bool {
    if (producer.type != .extractor or producer.config != .object) return false;
    const provider = producer.config.object.get("provider") orelse return false;
    const model = producer.config.object.get("model") orelse return false;
    return provider == .string and model == .string and std.mem.eql(u8, provider.string, "antfly") and std.mem.eql(u8, model.string, contract.default_model);
}

test "privacy document projection uses one producer contract and preserves nonselected fields" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const policy = contract.Policy{ .mode = .redact_for_search_and_ai, .fields = &.{"body"}, .category_groups = &.{.contact_details} };
    const resolved = try resolve(arena.allocator(), policy, true);
    const config = try std.json.Stringify.valueAlloc(a, resolved.config, .{});
    defer a.free(config);
    const Fake = struct {
        fn check(_: *anyopaque) !void {}
        fn call(_: *anyopaque, out: A, _: contract.Producer, source: []const u8) ![]u8 {
            try std.testing.expectEqualStrings("a@b.co", source);
            return out.dupe(u8, "{\"entities\":{\"email\":[{\"start\":0,\"end\":6,\"text\":\"a@b.co\",\"confidence\":0.9}]},\"long_document\":{\"window_count\":1}}");
        }
    };
    var fake: u8 = 0;
    const projected = try produce(a, config, "{\"body\":\"a@b.co\",\"count\":7}", .{ .ptr = &fake, .produce = Fake.call, .check = Fake.check });
    defer a.free(projected);
    try std.testing.expect(std.mem.indexOf(u8, projected, "a@b.co") == null);
    try std.testing.expect(std.mem.indexOf(u8, projected, "\"body\":\"[EMAIL]\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, projected, "\"count\":7") != null);
}

/// Scheduling hints are owner-local metadata, never publication authority.
/// Keeping them outside producer JSON preserves accepted receipts on tuning.
pub const execution_key = "\x00\x00__privacy_execution";
pub const ExecutionOverride = struct { policy_version: [64]u8, execution: contract.Execution };
pub fn withExecutionOverride(a: A, config_json: []const u8, override_json: []const u8) !?[]u8 {
    if (!try isManaged(a, config_json)) return null;
    var tree = try std.json.parseFromSlice(std.json.Value, a, config_json, .{});
    defer tree.deinit();
    var policy = try std.json.parseFromValue(contract.Policy, a, tree.value.object.get(config_key).?, .{});
    defer policy.deinit();
    var tuning = try std.json.parseFromSlice(ExecutionOverride, a, override_json, .{});
    defer tuning.deinit();
    if (!std.mem.eql(u8, &(try policy.value.identity(a)), &tuning.value.policy_version)) return null;
    policy.value.execution = tuning.value.execution;
    try policy.value.validate();
    const raw = try std.json.Stringify.valueAlloc(a, policy.value, .{});
    defer a.free(raw);
    var replacement = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer replacement.deinit();
    try tree.value.object.put(tree.arena.allocator(), config_key, replacement.value);
    return try std.json.Stringify.valueAlloc(a, tree.value, .{});
}

test "privacy execution tuning is policy bound and leaves semantic identity unchanged" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"} };
    const resolved = try resolve(scratch, policy, true);
    const config = try std.json.Stringify.valueAlloc(scratch, resolved.config, .{});
    const version = try policy.identity(scratch);
    const override = try std.json.Stringify.valueAlloc(scratch, ExecutionOverride{ .policy_version = version, .execution = .{ .batch_items = 1, .max_attempts = 5 } }, .{});
    const updated = (try withExecutionOverride(a, config, override)).?;
    defer a.free(updated);
    const tree = try std.json.parseFromSlice(std.json.Value, scratch, updated, .{});
    const tuned = try std.json.parseFromValue(contract.Policy, scratch, tree.value.object.get(config_key).?, .{});
    try std.testing.expectEqual(@as(u8, 1), tuned.value.execution.batch_items);
    try std.testing.expectEqual(@as(u8, 5), tuned.value.execution.max_attempts);
    try std.testing.expectEqual(version, try tuned.value.identity(scratch));
    const stale = try std.json.Stringify.valueAlloc(scratch, ExecutionOverride{ .policy_version = contract.fingerprint("old"), .execution = .{ .batch_items = 1 } }, .{});
    try std.testing.expect(try withExecutionOverride(a, config, stale) == null);
}
