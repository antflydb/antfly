// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

/// Returns true for field names that commonly carry credentials. The check is
/// deliberately conservative because callers use it to keep public and
/// durable semantic configuration credential-free.
pub fn fieldNameIsSensitive(field: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(field, "authorization") or
        std.ascii.eqlIgnoreCase(field, "proxy-authorization") or
        std.ascii.eqlIgnoreCase(field, "cookie") or
        std.ascii.eqlIgnoreCase(field, "set-cookie") or
        std.ascii.eqlIgnoreCase(field, "credentials_path") or
        std.ascii.eqlIgnoreCase(field, "private_key") or
        std.ascii.eqlIgnoreCase(field, "secret")) return true;

    var normalized_buffer: [128]u8 = undefined;
    if (field.len > normalized_buffer.len) return true;
    var normalized_len: usize = 0;
    for (field) |byte| {
        if (!std.ascii.isAlphanumeric(byte)) continue;
        normalized_buffer[normalized_len] = std.ascii.toLower(byte);
        normalized_len += 1;
    }
    const normalized = normalized_buffer[0..normalized_len];
    if (normalized.len == 0) return true;
    const exact_sensitive = [_][]const u8{
        "auth", "code", "cookie", "credentials", "key", "password", "secret", "sig", "token", "xauth",
    };
    for (exact_sensitive) |candidate| {
        if (std.mem.eql(u8, normalized, candidate)) return true;
    }
    const sensitive_components = [_][]const u8{
        "apikey", "accesskey", "secretkey", "privatekey", "subscriptionkey", "authtoken", "authkey",
    };
    for (sensitive_components) |component| {
        if (std.mem.indexOf(u8, normalized, component) != null) return true;
    }
    const sensitive_suffixes = [_][]const u8{
        "password", "passwd", "secret", "token", "credential", "signature", "authorization",
    };
    for (sensitive_suffixes) |suffix| {
        if (std.mem.endsWith(u8, normalized, suffix)) return true;
    }
    return false;
}

pub fn containsSecretReference(value: []const u8) bool {
    return std.mem.indexOf(u8, value, "${secret:") != null;
}

/// Detect credentials embedded in URL authority, query, or fragment fields.
/// Query keys are percent-decoded before classification so alternate spelling
/// cannot bypass the durable credential-free configuration boundary.
pub fn urlContainsCredentials(url: []const u8) bool {
    const authority_start = if (std.mem.indexOf(u8, url, "://")) |scheme_end|
        scheme_end + 3
    else if (std.mem.startsWith(u8, url, "//"))
        @as(usize, 2)
    else
        null;
    if (authority_start) |start| {
        var authority_end = url.len;
        for (url[start..], start..) |byte, index| {
            if (byte == '/' or byte == '?' or byte == '#') {
                authority_end = index;
                break;
            }
        }
        if (std.mem.indexOfScalar(u8, url[start..authority_end], '@') != null) return true;
    }

    const fragment_start = std.mem.indexOfScalar(u8, url, '#');
    if (std.mem.indexOfScalar(u8, url, '?')) |query_marker| {
        if (fragment_start == null or query_marker < fragment_start.?) {
            const query_end = fragment_start orelse url.len;
            if (parameterListContainsCredentials(url[query_marker + 1 .. query_end])) return true;
        }
    }
    if (fragment_start) |marker| {
        return parameterListContainsCredentials(url[marker + 1 ..]);
    }
    return false;
}

fn parameterListContainsCredentials(encoded_parameters: []const u8) bool {
    var parameters = std.mem.tokenizeAny(u8, encoded_parameters, "&;");
    while (parameters.next()) |parameter| {
        const key = parameter[0 .. std.mem.indexOfScalar(u8, parameter, '=') orelse parameter.len];
        if (encodedParameterKeyIsSensitive(key)) return true;
    }
    return false;
}

fn encodedParameterKeyIsSensitive(encoded: []const u8) bool {
    var decoded_buffer: [128]u8 = undefined;
    if (encoded.len > decoded_buffer.len) return true;
    var decoded_len: usize = 0;
    var index: usize = 0;
    while (index < encoded.len) {
        if (encoded[index] == '%') {
            if (index + 2 >= encoded.len) return true;
            const high = std.fmt.charToDigit(encoded[index + 1], 16) catch return true;
            const low = std.fmt.charToDigit(encoded[index + 2], 16) catch return true;
            decoded_buffer[decoded_len] = @intCast((high << 4) | low);
            decoded_len += 1;
            index += 3;
            continue;
        }
        decoded_buffer[decoded_len] = if (encoded[index] == '+') ' ' else encoded[index];
        decoded_len += 1;
        index += 1;
    }
    const key = std.mem.trim(u8, decoded_buffer[0..decoded_len], &std.ascii.whitespace);
    return fieldNameIsSensitive(key);
}

test "credential URL detection covers authority query fragment and secret references" {
    try std.testing.expect(urlContainsCredentials("https://alice:password@example.com/v1"));
    try std.testing.expect(urlContainsCredentials("https://example.com/v1?api_key=secret"));
    try std.testing.expect(urlContainsCredentials("https://example.com/v1?access%5Fkey%5Fid=secret"));
    try std.testing.expect(urlContainsCredentials("https://example.com/v1#access_token=secret"));
    try std.testing.expect(!urlContainsCredentials("https://example.com/v1?timeout=10#section"));
    try std.testing.expect(containsSecretReference("${secret:inference_url}"));
}
