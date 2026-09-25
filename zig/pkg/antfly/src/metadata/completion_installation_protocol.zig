// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Authenticated point query for a durable pending/active installation. This
//! authenticates metadata intent; it is not native backing or voting authority.
const std = @import("std");
const activation = @import("completion_activation.zig");
const catalog = @import("../common/completion_catalog_digest.zig");
const incarnation = @import("incarnation.zig");
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;
pub const path = "/internal/v1/metadata/completion/installation";
pub const max_request_bytes = 2048;
pub const max_catalog_bytes = 128 * 1024;
pub const max_response_bytes = 4 * 1024 * 1024;
pub const Keys = @import("../api/completion_attestation_protocol.zig").Keys;
pub const Request = struct {
    version: u32 = 1,
    requester: u64,
    group_id: u64,
    cluster_incarnation: incarnation.MetadataClusterIncarnation,
    nonce: u128,

    pub fn validate(self: @This()) !void {
        if (self.version != 1 or self.requester == 0 or self.group_id == 0 or self.nonce == 0 or
            !incarnation.isValid(self.cluster_incarnation)) return error.InvalidCompletionInstallation;
    }
};
pub const Response = struct {
    version: u32 = 1,
    request: Request,
    installation: ?activation.Record,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,

    pub fn validate(self: @This(), alloc: std.mem.Allocator) !void {
        try self.request.validate();
        if (self.version != 1) return error.InvalidCompletionInstallation;
        const installed = self.installation orelse {
            if (self.schema_json.len != 0 or self.read_schema_json.len != 0 or self.indexes_json.len != 0) return error.InvalidCompletionInstallation;
            return;
        };
        try installed.validate();
        if (self.version != 1 or !std.mem.eql(u8, &self.request.cluster_incarnation, &installed.cluster_incarnation))
            return error.InvalidCompletionInstallation;
        var found = false;
        for (installed.groups) |group| found = found or group.group_id == self.request.group_id;
        if (!found or self.schema_json.len > max_catalog_bytes or
            self.read_schema_json.len > max_catalog_bytes - self.schema_json.len or
            self.indexes_json.len > max_catalog_bytes - self.schema_json.len - self.read_schema_json.len)
            return error.InvalidCompletionInstallation;
        if (!std.mem.eql(u8, &(try catalog.digest(alloc, self.schema_json, self.read_schema_json, self.indexes_json)), &installed.schema_catalog_digest))
            return error.InvalidCompletionInstallation;
    }
};

fn mac(keys: Keys, key: []const u8, domain: []const u8, payload: []const u8) [32]u8 {
    var hash = Hmac.init(key);
    for ([_][]const u8{ domain, keys.issuer, payload }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hash.update(&length);
        hash.update(part);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn sign(alloc: std.mem.Allocator, keys: Keys, domain: []const u8, value: anytype, limit: usize) ![]u8 {
    try @import("../api/completion_attestation_protocol.zig").validateKeys(keys);
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(json);
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const payload_len = encoder.calcSize(json.len);
    const length = std.math.add(usize, payload_len, 44) catch return error.InvalidCompletionInstallation;
    if (length > limit) return error.InvalidCompletionInstallation;
    const frame = try alloc.alloc(u8, length);
    _ = encoder.encode(frame[0..payload_len], json);
    frame[payload_len] = '.';
    const signature = mac(keys, keys.primary, domain, frame[0..payload_len]);
    _ = encoder.encode(frame[payload_len + 1 ..], &signature);
    return frame;
}
fn verify(comptime T: type, alloc: std.mem.Allocator, keys: Keys, domain: []const u8, frame: []const u8, limit: usize) !std.json.Parsed(T) {
    try @import("../api/completion_attestation_protocol.zig").validateKeys(keys);
    if (frame.len > limit) return error.InvalidCompletionInstallation;
    const split = std.mem.indexOfScalar(u8, frame, '.') orelse return error.InvalidCompletionInstallation;
    if (frame.len - split != 44) return error.InvalidCompletionInstallation;
    var supplied: [32]u8 = undefined;
    std.base64.url_safe_no_pad.Decoder.decode(&supplied, frame[split + 1 ..]) catch return error.InvalidCompletionInstallation;
    const primary = mac(keys, keys.primary, domain, frame[0..split]);
    const alternate = mac(keys, keys.verification orelse keys.primary, domain, frame[0..split]);
    const primary_ok = std.crypto.timing_safe.eql([32]u8, supplied, primary);
    const alternate_ok = std.crypto.timing_safe.eql([32]u8, supplied, alternate);
    if (!primary_ok and !alternate_ok) return error.InvalidCompletionInstallationSignature;
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const json = try alloc.alloc(u8, decoder.calcSizeForSlice(frame[0..split]) catch return error.InvalidCompletionInstallation);
    defer alloc.free(json);
    decoder.decode(json, frame[0..split]) catch return error.InvalidCompletionInstallation;
    return std.json.parseFromSlice(T, alloc, json, .{ .allocate = .alloc_always }) catch |err| return switch (err) {
        error.OutOfMemory => err,
        else => error.InvalidCompletionInstallation,
    };
}
pub fn signRequest(alloc: std.mem.Allocator, keys: Keys, request: Request) ![]u8 {
    try request.validate();
    return sign(alloc, keys, "antfly-completion-installation-request-v1", request, max_request_bytes);
}
pub fn verifyRequest(alloc: std.mem.Allocator, keys: Keys, frame: []const u8) !Request {
    var parsed = try verify(Request, alloc, keys, "antfly-completion-installation-request-v1", frame, max_request_bytes);
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn signResponse(alloc: std.mem.Allocator, keys: Keys, response: Response) ![]u8 {
    try response.validate(alloc);
    return sign(alloc, keys, "antfly-completion-installation-response-v1", response, max_response_bytes);
}
pub fn verifyResponse(alloc: std.mem.Allocator, keys: Keys, frame: []const u8, expected: Request) !std.json.Parsed(Response) {
    try expected.validate();
    var parsed = try verify(Response, alloc, keys, "antfly-completion-installation-response-v1", frame, max_response_bytes);
    errdefer parsed.deinit();
    try parsed.value.validate(alloc);
    if (!std.meta.eql(parsed.value.request, expected)) return error.InvalidCompletionInstallation;
    return parsed;
}

test "workload admission completion installation authenticates pending intent catalog and exact challenge" {
    const alloc = std.testing.allocator;
    const keys: Keys = .{ .primary = "k" ** 32, .issuer = "cluster" };
    const request: Request = .{ .requester = 4, .group_id = 7, .cluster_incarnation = "11111111111111111111111111111111".*, .nonce = 19 };
    const signed = try signRequest(alloc, keys, request);
    defer alloc.free(signed);
    try std.testing.expectEqualDeep(request, try verifyRequest(alloc, keys, signed));
    var response: Response = .{
        .request = request,
        .installation = .{
            .cluster_incarnation = request.cluster_incarnation,
            .table_id = 1,
            .expected_definition = @splat(1),
            .schema_catalog_digest = try catalog.digest(alloc, "{}", "", "{}"),
            .expected_transition_generation = 0,
            .generation = 1,
            .policy = .{ .protocol_version = 1, .max_count = 4, .max_bytes = 65536, .max_transaction_bytes = 16384, .completion_protocol_version = 1, .profile_version = 1 },
            .groups = &.{.{ .group_id = 7, .range_id = 1, .split_attempt_epoch = 0, .range_digest = @splat(2) }},
        },
        .schema_json = "{}",
        .read_schema_json = "",
        .indexes_json = "{}",
    };
    const reply = try signResponse(alloc, keys, response);
    defer alloc.free(reply);
    var verified = try verifyResponse(alloc, keys, reply, request);
    defer verified.deinit();
    try std.testing.expectEqual(activation.Phase.pending, verified.value.installation.?.phase);
    var other = request;
    other.nonce += 1;
    try std.testing.expectError(error.InvalidCompletionInstallation, verifyResponse(alloc, keys, reply, other));
    try std.testing.expectError(error.InvalidCompletionInstallationSignature, verifyResponse(alloc, keys, signed, request));
    response.schema_json = "null";
    try std.testing.expectError(error.InvalidCompletionInstallation, signResponse(alloc, keys, response));
    reply[0] = if (reply[0] == 'A') 'B' else 'A';
    try std.testing.expectError(error.InvalidCompletionInstallationSignature, verifyResponse(alloc, keys, reply, request));
}

test "workload admission completion installation absence is authenticated and cannot carry catalog authority" {
    const alloc = std.testing.allocator;
    const keys: Keys = .{ .primary = "k" ** 32, .issuer = "cluster" };
    const request: Request = .{ .requester = 4, .group_id = 7, .cluster_incarnation = "11111111111111111111111111111111".*, .nonce = 19 };
    var response: Response = .{ .request = request, .installation = null, .schema_json = "", .read_schema_json = "", .indexes_json = "" };
    const frame = try signResponse(alloc, keys, response);
    defer alloc.free(frame);
    var verified = try verifyResponse(alloc, keys, frame, request);
    defer verified.deinit();
    try std.testing.expect(verified.value.installation == null);
    var other = request;
    other.requester += 1;
    try std.testing.expectError(error.InvalidCompletionInstallation, verifyResponse(alloc, keys, frame, other));
    response.schema_json = "{}";
    try std.testing.expectError(error.InvalidCompletionInstallation, signResponse(alloc, keys, response));
}
