// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Allocation-free, cardinality-bounded diagnostics for private errors that
//! must be normalized while crossing a runtime partition boundary.

const std = @import("std");

pub const slots_count = 256;

pub const Diagnostic = struct {
    fingerprint: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

fn skipWhitespace(bytes: []const u8, cursor: *usize) void {
    while (cursor.* < bytes.len and std.ascii.isWhitespace(bytes[cursor.*])) : (cursor.* += 1) {}
}

fn jsonStringValue(bytes: []const u8, cursor: *usize) ?[]const u8 {
    if (cursor.* == bytes.len or bytes[cursor.*] != '"') return null;
    cursor.* += 1;
    const value_start = cursor.*;
    var escaped = false;
    while (cursor.* < bytes.len) : (cursor.* += 1) {
        const byte = bytes[cursor.*];
        if (escaped) {
            escaped = false;
            continue;
        }
        if (byte == '\\') {
            escaped = true;
            continue;
        }
        if (byte == '"') {
            const value = bytes[value_start..cursor.*];
            cursor.* += 1;
            return value;
        }
    }
    return null;
}

fn skipJsonValue(bytes: []const u8, cursor: *usize) bool {
    skipWhitespace(bytes, cursor);
    if (cursor.* == bytes.len) return false;
    if (bytes[cursor.*] == '"') return jsonStringValue(bytes, cursor) != null;
    if (bytes[cursor.*] == '{' or bytes[cursor.*] == '[') {
        var depth: usize = 0;
        var in_string = false;
        var escaped = false;
        while (cursor.* < bytes.len) : (cursor.* += 1) {
            const byte = bytes[cursor.*];
            if (in_string) {
                if (escaped) {
                    escaped = false;
                } else if (byte == '\\') {
                    escaped = true;
                } else if (byte == '"') {
                    in_string = false;
                }
                continue;
            }
            switch (byte) {
                '"' => in_string = true,
                '{', '[' => depth += 1,
                '}', ']' => {
                    if (depth == 0) return false;
                    depth -= 1;
                    if (depth == 0) {
                        cursor.* += 1;
                        return true;
                    }
                },
                else => {},
            }
        }
        return false;
    }
    const value_start = cursor.*;
    while (cursor.* < bytes.len and
        bytes[cursor.*] != ',' and
        bytes[cursor.*] != '}' and
        !std.ascii.isWhitespace(bytes[cursor.*])) : (cursor.* += 1)
    {}
    return cursor.* != value_start;
}

fn jsonModelValue(request_json: []const u8) ?[]const u8 {
    // Scan top-level object fields without parsing or allocating. The returned
    // bytes remain JSON-escaped, which is sufficient for a stable, non-secret
    // diagnostic fingerprint.
    var cursor: usize = 0;
    skipWhitespace(request_json, &cursor);
    if (cursor == request_json.len or request_json[cursor] != '{') return null;
    cursor += 1;
    while (true) {
        skipWhitespace(request_json, &cursor);
        if (cursor == request_json.len or request_json[cursor] == '}') return null;
        const key = jsonStringValue(request_json, &cursor) orelse return null;
        skipWhitespace(request_json, &cursor);
        if (cursor == request_json.len or request_json[cursor] != ':') return null;
        cursor += 1;
        skipWhitespace(request_json, &cursor);
        if (std.mem.eql(u8, key, "model"))
            return jsonStringValue(request_json, &cursor);
        if (!skipJsonValue(request_json, &cursor)) return null;
        skipWhitespace(request_json, &cursor);
        if (cursor == request_json.len or request_json[cursor] == '}') return null;
        if (request_json[cursor] != ',') return null;
        cursor += 1;
    }
}

pub fn fingerprint(operation: c_int, err: anyerror, request_json: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(0x616e_7466_6c79_6966);
    var operation_bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, &operation_bytes, @intCast(operation), .little);
    hasher.update(&operation_bytes);
    const error_name = @errorName(err);
    var length_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &length_bytes, @intCast(error_name.len), .little);
    hasher.update(&length_bytes);
    hasher.update(error_name);
    if (jsonModelValue(request_json)) |model| {
        hasher.update("\x01");
        std.mem.writeInt(u64, &length_bytes, @intCast(model.len), .little);
        hasher.update(&length_bytes);
        hasher.update(model);
    } else {
        hasher.update("\x00");
    }
    // Zero is the empty-slot sentinel.
    const result = hasher.final();
    return if (result == 0) 1 else result;
}

pub fn note(diagnostics: []Diagnostic, diagnostic_fingerprint: u64) ?u64 {
    std.debug.assert(diagnostic_fingerprint != 0);
    if (diagnostics.len == 0) return null;
    const start: usize = @intCast(diagnostic_fingerprint % diagnostics.len);
    for (0..diagnostics.len) |offset| {
        const slot = &diagnostics[(start + offset) % diagnostics.len];
        const observed = slot.fingerprint.load(.acquire);
        if (observed == diagnostic_fingerprint)
            return slot.count.fetchAdd(1, .monotonic) +% 1;
        if (observed != 0) continue;
        if (slot.fingerprint.cmpxchgStrong(0, diagnostic_fingerprint, .acq_rel, .acquire)) |raced| {
            if (raced == diagnostic_fingerprint)
                return slot.count.fetchAdd(1, .monotonic) +% 1;
            continue;
        }
        return slot.count.fetchAdd(1, .monotonic) +% 1;
    }
    return null;
}

test "private inference failure diagnostics are bounded and isolated by fingerprint" {
    var diagnostics = [_]Diagnostic{ .{}, .{} };
    const first = fingerprint(1, error.UnexpectedToken, "{\"model\":\"first\",\"texts\":[]}");
    const second = fingerprint(1, error.UnexpectedToken, "{\"model\":\"second\",\"texts\":[]}");
    const other_error = fingerprint(1, error.InvalidCharacter, "{\"model\":\"first\",\"texts\":[]}");
    const empty_model = fingerprint(1, error.UnexpectedToken, "{\"model\":\"\",\"texts\":[]}");
    const absent_model = fingerprint(1, error.UnexpectedToken, "{\"texts\":[]}");
    try std.testing.expect(first != second);
    try std.testing.expect(first != other_error);
    try std.testing.expect(empty_model != absent_model);
    try std.testing.expectEqual(@as(?u64, 1), note(&diagnostics, first));
    try std.testing.expectEqual(@as(?u64, 2), note(&diagnostics, first));
    try std.testing.expectEqual(@as(?u64, 1), note(&diagnostics, second));
    try std.testing.expectEqual(@as(?u64, null), note(&diagnostics, other_error));
}

test "private inference model fingerprint extraction is allocation free and escape aware" {
    try std.testing.expectEqualStrings("model\\\"variant", jsonModelValue(
        " \n { \"model\" : \"model\\\"variant\", \"texts\": [] }",
    ).?);
    try std.testing.expectEqualStrings("late", jsonModelValue(
        "{\"request\":{\"description\":\"\\\"model\\\":\\\"decoy\\\"\"},\"model\":\"late\"}",
    ).?);
    try std.testing.expectEqual(@as(?[]const u8, null), jsonModelValue("{}"));
}
