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

//! Bounded native intake screening. This detects recognizable credentials and
//! credential-bearing context, not arbitrary confidential prose. It makes no
//! provider calls and never puts a matched value in errors or reports.
const std = @import("std");
const A = std.mem.Allocator;
const credential_safety = @import("../common/credential_safety.zig");
pub const Mode = enum { reject, redact };
pub const Alphabet = enum { alphanumeric, base64url, hex, non_separator };
pub const Rule = struct { id: []const u8, prefix: []const u8, min_length: u16, max_length: u16 = 4096, alphabet: Alphabet = .base64url };
pub const Config = struct {
    version: u16 = 1,
    mode: Mode = .reject,
    custom_rules: []const Rule = &.{},
    /// Administrative exceptions name a whole detected value by SHA-256.
    /// Neither configuration nor diagnostics contain the exception plaintext.
    allow_fingerprints: []const []const u8 = &.{},
    pub fn validate(self: Config) !void {
        if (self.version != 1 or self.custom_rules.len > 32 or self.allow_fingerprints.len > 128) return error.InvalidMemoryConfig;
        for (self.custom_rules) |rule| {
            if (rule.id.len == 0 or rule.id.len > 64 or rule.prefix.len < 3 or rule.prefix.len > 64 or rule.min_length < rule.prefix.len + 4 or rule.max_length < rule.min_length or rule.max_length > 4096) return error.InvalidMemoryConfig;
            for (rule.prefix) |byte| if (byte < 0x21 or byte > 0x7e) return error.InvalidMemoryConfig;
        }
        for (self.allow_fingerprints) |fingerprint| {
            if (fingerprint.len != 64) return error.InvalidMemoryConfig;
            for (fingerprint) |byte| if (!std.ascii.isHex(byte) or std.ascii.isUpper(byte)) return error.InvalidMemoryConfig;
        }
    }
};
pub const Kind = enum { known_token, private_key, credential_assignment, credential_url, sensitive_field, custom };
pub const Report = struct {
    policy_version: u16 = 1,
    redactions: u32 = 0,
    known_tokens: u32 = 0,
    private_keys: u32 = 0,
    credential_assignments: u32 = 0,
    credential_urls: u32 = 0,
    sensitive_fields: u32 = 0,
    custom: u32 = 0,
    pub fn add(self: *Report, other: Report) void {
        inline for (.{ "redactions", "known_tokens", "private_keys", "credential_assignments", "credential_urls", "sensitive_fields", "custom" }) |field| @field(self, field) += @field(other, field);
    }
    fn record(self: *Report, kind: Kind) void {
        self.redactions += 1;
        switch (kind) {
            .known_token => self.known_tokens += 1,
            .private_key => self.private_keys += 1,
            .credential_assignment => self.credential_assignments += 1,
            .credential_url => self.credential_urls += 1,
            .sensitive_field => self.sensitive_fields += 1,
            .custom => self.custom += 1,
        }
    }
};
pub const Result = struct { value: []const u8, report: Report = .{} };
const marker = "[REDACTED]";
const Span = struct { start: usize, end: usize, kind: Kind };
const builtin_rules = [_]Rule{
    .{ .id = "github-classic", .prefix = "ghp_", .min_length = 40 },
    .{ .id = "github-oauth", .prefix = "gho_", .min_length = 40 },
    .{ .id = "github-user", .prefix = "ghu_", .min_length = 40 },
    .{ .id = "github-server", .prefix = "ghs_", .min_length = 40 },
    .{ .id = "github-refresh", .prefix = "ghr_", .min_length = 40 },
    .{ .id = "github-fine-grained", .prefix = "github_pat_", .min_length = 80 },
    .{ .id = "provider-api-key", .prefix = "sk-", .min_length = 24 },
    .{ .id = "slack-bot", .prefix = "xoxb-", .min_length = 24 },
    .{ .id = "slack-user", .prefix = "xoxp-", .min_length = 24 },
    .{ .id = "slack-session", .prefix = "xoxs-", .min_length = 24 },
    .{ .id = "slack-app", .prefix = "xapp-", .min_length = 24 },
    .{ .id = "huggingface", .prefix = "hf_", .min_length = 32 },
    .{ .id = "google-api-key", .prefix = "AIza", .min_length = 39 },
};
fn character(alphabet: Alphabet, byte: u8) bool {
    return switch (alphabet) {
        .alphanumeric => std.ascii.isAlphanumeric(byte),
        .base64url => std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_',
        .hex => std.ascii.isHex(byte),
        .non_separator => byte > 0x20 and std.mem.indexOfScalar(u8, "\"'<>;,)}]", byte) == null,
    };
}
fn allowed(config: Config, value: []const u8) bool {
    if (config.allow_fingerprints.len == 0) return false;
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
    const hex = std.fmt.bytesToHex(hash, .lower);
    for (config.allow_fingerprints) |fingerprint| if (std.mem.eql(u8, &hex, fingerprint)) return true;
    return false;
}
fn placeholder(value: []const u8) bool {
    return value.len == 0 or std.mem.eql(u8, value, marker) or std.mem.startsWith(u8, value, "${") or std.mem.startsWith(u8, value, "{{") or std.mem.startsWith(u8, value, "$env:");
}
fn sensitiveField(name: []const u8) bool {
    var buffer: [128]u8 = undefined;
    if (name.len > buffer.len) return false;
    var len: usize = 0;
    for (name) |byte| if (std.ascii.isAlphanumeric(byte)) {
        buffer[len] = std.ascii.toLower(byte);
        len += 1;
    };
    const normalized = buffer[0..len];
    inline for (.{ "password", "passwd", "apikey", "accesstoken", "refreshtoken", "idtoken", "authtoken", "clientsecret", "secretaccesskey", "awssecretaccesskey", "awsaccesskeyid", "authorization", "proxyauthorization", "privatekey" }) |field| if (std.mem.eql(u8, normalized, field)) return true;
    return false;
}
fn identityField(name: []const u8) bool {
    return std.mem.eql(u8, name, "ranking") or std.mem.eql(u8, name, "revision") or std.mem.eql(u8, name, "producer") or std.mem.eql(u8, name, "branch") or std.mem.eql(u8, name, "checkpoint") or std.mem.eql(u8, name, "space") or std.mem.eql(u8, name, "dataset") or std.mem.eql(u8, name, "seed") or std.mem.eql(u8, name, "target") or std.mem.eql(u8, name, "actor") or std.mem.eql(u8, name, "owner") or std.mem.eql(u8, name, "principal") or std.mem.eql(u8, name, "model") or std.mem.eql(u8, name, "provider") or std.mem.eql(u8, name, "supersedes") or std.mem.eql(u8, name, "depends_on") or std.mem.eql(u8, name, "motivates") or std.mem.eql(u8, name, "rejected_in_favor_of") or std.mem.eql(u8, name, "id") or std.mem.eql(u8, name, "key") or std.mem.eql(u8, name, "subject") or std.mem.eql(u8, name, "predicate") or std.mem.eql(u8, name, "scope") or std.mem.eql(u8, name, "from") or std.mem.eql(u8, name, "to") or std.mem.eql(u8, name, "into") or std.mem.eql(u8, name, "source_version") or std.mem.endsWith(u8, name, "_id") or std.mem.endsWith(u8, name, "_ids");
}
fn prefixSpan(value: []const u8, start: usize, rule: Rule, kind: Kind) ?Span {
    if (!std.mem.startsWith(u8, value[start..], rule.prefix)) return null;
    var end = start + rule.prefix.len;
    while (end < value.len and character(rule.alphabet, value[end])) : (end += 1) {}
    if (end - start < rule.min_length) return null;
    // Oversized credentials are still credentials; never leave an unredacted tail.
    return .{ .start = start, .end = end, .kind = kind };
}
fn at(value: []const u8, start: usize, config: Config) ?Span {
    const tail = value[start..];
    if (std.mem.startsWith(u8, tail, "-----BEGIN ")) {
        inline for (.{ "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY", "OPENSSH PRIVATE KEY", "ENCRYPTED PRIVATE KEY" }) |label| {
            const begin = "-----BEGIN " ++ label ++ "-----";
            const end = "-----END " ++ label ++ "-----";
            if (std.mem.startsWith(u8, tail, begin)) return .{ .start = start, .end = if (std.mem.indexOf(u8, tail[begin.len..], end)) |offset| start + begin.len + offset + end.len else value.len, .kind = .private_key };
        }
    }
    if (start == 0 or !character(.base64url, value[start - 1])) {
        for (builtin_rules) |rule| if (rule.prefix[0] == value[start]) if (prefixSpan(value, start, rule, .known_token)) |span| return span;
        for (config.custom_rules) |rule| if (rule.prefix[0] == value[start]) if (prefixSpan(value, start, rule, .custom)) |span| return span;
        if ((std.mem.startsWith(u8, tail, "AKIA") or std.mem.startsWith(u8, tail, "ASIA")) and tail.len >= 20) {
            var valid = true;
            for (tail[4..20]) |byte| valid = valid and (std.ascii.isUpper(byte) or std.ascii.isDigit(byte));
            if (valid and (tail.len == 20 or !character(.base64url, tail[20]))) return .{ .start = start, .end = start + 20, .kind = .known_token };
        }
    }
    var scheme_end = start;
    while (scheme_end < value.len and scheme_end - start <= 32 and (std.ascii.isAlphanumeric(value[scheme_end]) or std.mem.indexOfScalar(u8, "+.-", value[scheme_end]) != null)) : (scheme_end += 1) {}
    if (scheme_end > start and std.mem.startsWith(u8, value[scheme_end..], "://")) {
        var end = scheme_end + 3;
        while (end < value.len and !std.ascii.isWhitespace(value[end]) and std.mem.indexOfScalar(u8, "\"'<>)]}", value[end]) == null) : (end += 1) {}
        if (credential_safety.urlContainsCredentials(value[start..end])) return .{ .start = start, .end = end, .kind = .credential_url };
    }
    if ((start == 0 or !std.ascii.isAlphanumeric(value[start - 1])) and (std.ascii.isAlphabetic(value[start]) or value[start] == '_')) {
        var end = start;
        while (end < value.len and (std.ascii.isAlphanumeric(value[end]) or value[end] == '_' or value[end] == '-')) : (end += 1) {}
        if (sensitiveField(value[start..end])) {
            while (end < value.len and (std.ascii.isWhitespace(value[end]) or value[end] == '\'' or value[end] == '"')) : (end += 1) {}
            if (end < value.len and (value[end] == '=' or value[end] == ':')) {
                end += 1;
                while (end < value.len and std.ascii.isWhitespace(value[end])) : (end += 1) {}
                const quote: ?u8 = if (end < value.len and (value[end] == '\'' or value[end] == '"')) value[end] else null;
                if (quote != null) end += 1;
                const begin = end;
                var escaped = false;
                // An unquoted Authorization value includes its scheme and token.
                if (quote == null and value.len - end >= 7 and std.ascii.eqlIgnoreCase(value[end..][0..7], "Bearer ")) end += 7;
                if (quote == null and value.len - end >= 6 and std.ascii.eqlIgnoreCase(value[end..][0..6], "Basic ")) end += 6;
                while (end < value.len) : (end += 1) {
                    if (quote) |q| {
                        if (value[end] == q and !escaped) break;
                        escaped = value[end] == '\\' and !escaped;
                    } else if (std.ascii.isWhitespace(value[end]) or std.mem.indexOfScalar(u8, ",;}\"", value[end]) != null) break;
                }
                if (!placeholder(value[begin..end])) return .{ .start = begin, .end = end, .kind = .credential_assignment };
            }
        }
    }
    return null;
}
pub fn text(a: A, value: []const u8, config: Config, protect_identity: bool) anyerror!Result {
    // Tool output commonly arrives as JSON inside an evidence string. Decode
    // that structure too, so escaped keys/prefixes do not bypass the boundary.
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len > 0 and (trimmed[0] == '{' or trimmed[0] == '[')) {
        var nested_config = config;
        if (protect_identity) nested_config.mode = .reject;
        const nested: ?Result = json(a, value, nested_config) catch |err| switch (err) {
            error.OutOfMemory, error.MemorySecretDetected, error.MemoryWorkLimit => return err,
            else => null,
        };
        if (nested) |result| return result;
    }
    var report: Report = .{};
    var output: std.Io.Writer.Allocating = .init(a);
    var copied: usize = 0;
    var offset: usize = 0;
    while (offset < value.len) {
        const span = at(value, offset, config) orelse {
            offset += 1;
            continue;
        };
        if (allowed(config, value[span.start..span.end])) {
            offset = span.end;
            continue;
        }
        if (config.mode == .reject or protect_identity) return error.MemorySecretDetected;
        try output.writer.writeAll(value[copied..span.start]);
        try output.writer.writeAll(marker);
        report.record(span.kind);
        copied = span.end;
        offset = span.end;
    }
    if (report.redactions == 0) return .{ .value = value };
    try output.writer.writeAll(value[copied..]);
    return .{ .value = try output.toOwnedSlice(), .report = report };
}
/// JSON strings are decoded before matching. Object keys and identity fields
/// cannot be rewritten: redacting an identifier would silently change references.
pub fn json(a: A, input: []const u8, config: Config) anyerror!Result {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, input, .{ .allocate = .alloc_always });
    var mutable = root;
    const Work = struct { value: *std.json.Value, protected: bool = false, sensitive: bool = false, depth: usize = 0 };
    var work: std.ArrayList(Work) = .empty;
    try work.append(a, .{ .value = &mutable });
    var report: Report = .{};
    var nodes: usize = 0;
    while (work.pop()) |item| {
        nodes += 1;
        if (nodes > 65536 or item.depth > 64) return error.MemoryWorkLimit;
        switch (item.value.*) {
            .string => |value| {
                if (item.sensitive and !placeholder(value) and !allowed(config, value)) {
                    if (config.mode == .reject or item.protected) return error.MemorySecretDetected;
                    item.value.* = .{ .string = marker };
                    report.record(.sensitive_field);
                } else {
                    const result = try text(a, value, config, item.protected);
                    item.value.* = .{ .string = result.value };
                    report.add(result.report);
                }
            },
            .integer, .float, .number_string => if (item.sensitive) {
                if (config.mode == .reject or item.protected) return error.MemorySecretDetected;
                item.value.* = .{ .string = marker };
                report.record(.sensitive_field);
            },
            .object => |*object| for (object.keys(), object.values()) |name, *value| {
                _ = try text(a, name, config, true);
                try work.append(a, .{ .value = value, .protected = item.protected or identityField(name), .sensitive = item.sensitive or sensitiveField(name), .depth = item.depth + 1 });
            },
            .array => |*array| for (array.items) |*value| try work.append(a, .{ .value = value, .protected = item.protected, .sensitive = item.sensitive, .depth = item.depth + 1 }),
            else => {},
        }
    }
    return .{ .value = if (report.redactions == 0) input else try std.json.Stringify.valueAlloc(a, mutable, .{}), .report = report };
}

pub fn Typed(comptime T: type) type {
    return struct { value: T, report: Report };
}
/// Native typed values already have decoded strings. Avoid serializing and
/// reparsing each model result or evidence record just to inspect its contents.
pub fn typed(comptime T: type, a: A, value: T, config: Config) !Typed(T) {
    var report: Report = .{};
    var work: usize = 0;
    return .{ .value = try cleanValue(T, a, value, config, false, false, 0, &work, &report), .report = report };
}
fn cleanValue(comptime T: type, a: A, value: T, config: Config, protected: bool, sensitive: bool, depth: usize, work: *usize, report: *Report) anyerror!T {
    if (T == std.json.Value) {
        if (!sensitive) switch (value) {
            .integer, .float, .number_string, .bool, .null => return value,
            else => {},
        };
    }
    work.* += 1;
    if (work.* > 65536 or depth > 64) return error.MemoryWorkLimit;
    if (T == std.json.Value) {
        return switch (value) {
            .string => |string| .{ .string = try cleanValue([]const u8, a, string, config, protected, sensitive, depth + 1, work, report) },
            .object => |object| blk: {
                if (config.mode == .reject) {
                    for (object.keys(), object.values()) |key, child| {
                        _ = try text(a, key, config, true);
                        _ = try cleanValue(std.json.Value, a, child, config, protected or identityField(key), sensitive or sensitiveField(key), depth + 1, work, report);
                    }
                    break :blk value;
                }
                var result: std.json.ObjectMap = .empty;
                for (object.keys(), object.values()) |key, child| {
                    _ = try text(a, key, config, true);
                    try result.put(a, key, try cleanValue(std.json.Value, a, child, config, protected or identityField(key), sensitive or sensitiveField(key), depth + 1, work, report));
                }
                break :blk .{ .object = result };
            },
            .array => |array| blk: {
                if (config.mode == .reject) {
                    for (array.items) |child| _ = try cleanValue(std.json.Value, a, child, config, protected, sensitive, depth + 1, work, report);
                    break :blk value;
                }
                var result = std.json.Array.init(a);
                for (array.items) |child| try result.append(try cleanValue(std.json.Value, a, child, config, protected, sensitive, depth + 1, work, report));
                break :blk .{ .array = result };
            },
            .integer, .float, .number_string => if (sensitive) blk: {
                if (config.mode == .reject or protected) return error.MemorySecretDetected;
                report.record(.sensitive_field);
                break :blk .{ .string = marker };
            } else value,
            else => value,
        };
    }
    return switch (@typeInfo(T)) {
        .pointer => |info| blk: {
            if (info.size != .slice) @compileError("intake values must be owned native values or slices");
            if (info.child == u8) {
                if (sensitive and !placeholder(value) and !allowed(config, value)) {
                    if (config.mode == .reject or protected) return error.MemorySecretDetected;
                    report.record(.sensitive_field);
                    break :blk marker;
                }
                const result = try text(a, value, config, protected);
                report.add(result.report);
                break :blk result.value;
            }
            if (!sensitive) switch (@typeInfo(info.child)) {
                .int, .float, .bool, .@"enum" => break :blk value,
                else => {},
            };
            if (config.mode == .reject) {
                for (value) |child| _ = try cleanValue(info.child, a, child, config, protected, sensitive, depth + 1, work, report);
                break :blk value;
            }
            const result = try a.alloc(info.child, value.len);
            for (value, result) |child, *out| out.* = try cleanValue(info.child, a, child, config, protected, sensitive, depth + 1, work, report);
            break :blk result;
        },
        .@"struct" => |info| blk: {
            var result = value;
            inline for (info.field_names, info.field_types) |name, Field| @field(result, name) = try cleanValue(Field, a, @field(value, name), config, protected or identityField(name), sensitive or sensitiveField(name), depth + 1, work, report);
            break :blk result;
        },
        .optional => |info| if (value) |child| try cleanValue(info.child, a, child, config, protected, sensitive, depth + 1, work, report) else null,
        .array => value, // Fixed byte arrays are digests and native identifiers.
        .int, .float => if (sensitive) error.MemorySecretDetected else value,
        .bool, .@"enum", .void => value,
        else => @compileError("unsupported native intake value: " ++ @typeName(T)),
    };
}

test "native secret screening rejects credentials and redacts decoded JSON without changing identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const credential = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
    try std.testing.expectError(error.MemorySecretDetected, text(a, credential, .{}, false));
    const redacted = try text(a, "before " ++ credential ++ " after", .{ .mode = .redact }, false);
    try std.testing.expectEqualStrings("before [REDACTED] after", redacted.value);
    try std.testing.expectEqual(@as(u32, 1), redacted.report.redactions);
    const encoded = "{\"text\":\"\\u0067hp_abcdefghijklmnopqrstuvwxyz0123456789\",\"metadata\":{\"api_key\":\"unprefixed-secret\"}}";
    try std.testing.expectError(error.MemorySecretDetected, json(a, encoded, .{}));
    const clean = try json(a, encoded, .{ .mode = .redact });
    try std.testing.expect(std.mem.indexOf(u8, clean.value, "unprefixed-secret") == null);
    try std.testing.expectEqual(@as(u32, 2), clean.report.redactions);
    try std.testing.expectError(error.MemorySecretDetected, json(a, "{\"source_id\":\"" ++ credential ++ "\"}", .{ .mode = .redact }));
    try std.testing.expectEqualStrings("aabbcc0123456789", (try text(a, "aabbcc0123456789", .{}, false)).value);
    try std.testing.expectEqualStrings("password=${PASSWORD}", (try text(a, "password=${PASSWORD}", .{}, false)).value);
    try std.testing.expectError(error.MemorySecretDetected, text(a, "password=unprefixed-credential", .{}, false));
    const pem = "-----BEGIN PRIVATE KEY-----\nsynthetic-test-fixture\n-----END PRIVATE KEY-----";
    try std.testing.expectEqualStrings(marker, (try text(a, pem, .{ .mode = .redact }, false)).value);
    try std.testing.expectEqualStrings(marker, (try text(a, "https://user:password@example.test/path", .{ .mode = .redact }, false)).value);
}

test "native secret redaction is idempotent for assignments, authorization, and credential URLs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "password=unprefixed-credential", "Authorization: Bearer synthetic-fixture-value", "postgresql://user:password@example.test/db", "https://example.test?%74oken=synthetic-fixture-value" }) |input| {
        const redacted = try text(a, input, .{ .mode = .redact }, false);
        try std.testing.expect(redacted.report.redactions > 0);
        const clean = try text(a, redacted.value, .{}, false);
        try std.testing.expectEqual(@as(u32, 0), clean.report.redactions);
    }
}

test "native typed secret screening preserves references and redacts free metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Record = struct { id: []const u8, text: []const u8, metadata: ?std.json.Value = null };
    const input: Record = .{ .id = "safe-id", .text = "password=synthetic-fixture", .metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"api_key\":\"opaque-fixture\"}", .{}) };
    const result = try typed(Record, a, input, .{ .mode = .redact });
    try std.testing.expectEqualStrings(input.id, result.value.id);
    try std.testing.expectEqualStrings("password=[REDACTED]", result.value.text);
    try std.testing.expectEqualStrings(marker, result.value.metadata.?.object.get("api_key").?.string);
    try std.testing.expectEqual(@as(u32, 2), result.report.redactions);
    try std.testing.expectEqualStrings("opaque-fixture", input.metadata.?.object.get("api_key").?.string);
}

test "native intake decodes escaped credentials inside structured tool-output text" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input = "{\"\\u0061pi_key\":\"unprefixed-fixture\",\"nested\":\"\\u0067hp_abcdefghijklmnopqrstuvwxyz0123456789\"}";
    try std.testing.expectError(error.MemorySecretDetected, text(a, input, .{}, false));
    const result = try text(a, input, .{ .mode = .redact }, false);
    try std.testing.expect(std.mem.indexOf(u8, result.value, "unprefixed-fixture") == null);
    try std.testing.expectEqual(@as(u32, 2), result.report.redactions);
    const ordinary = " { \"message\": \"ordinary tool output\" } ";
    try std.testing.expectEqualStrings(ordinary, (try text(a, ordinary, .{}, false)).value);
}
