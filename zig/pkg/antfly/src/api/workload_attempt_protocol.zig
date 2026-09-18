// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Authentication prerequisite for remote ownership. Authenticity is not
//! deduplication or quiescence: workers must durably admit an attempt before
//! execution and issue evidence only after the represented work has stopped.
const std = @import("std");
const attempts = @import("../common/workload_attempts.zig");
pub const AttemptId = attempts.AttemptId;
pub const request_header = "X-Antfly-Workload-Attempt";
pub const evidence_header = "X-Antfly-Workload-Evidence";
pub const max_frame_bytes = 4096;
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;

pub fn requestTarget(uri: []const u8) ![]const u8 {
    if (uri.len == 0 or std.mem.indexOfScalar(u8, uri, '#') != null) return error.InvalidAttemptTarget;
    if (uri[0] == '/') return uri;
    const scheme_end = std.mem.indexOf(u8, uri, "://") orelse return error.InvalidAttemptTarget;
    const path_start = std.mem.indexOfScalarPos(u8, uri, scheme_end + 3, '/') orelse return error.InvalidAttemptTarget;
    return uri[path_start..];
}

pub const Keys = struct {
    primary: []const u8,
    verification: ?[]const u8 = null,
    issuer: []const u8,
};

pub const Request = struct {
    version: u16,
    attempt: AttemptId,
    remaining_ns: u64,
    request_digest: [32]u8,
};

pub const Terminal = struct {
    version: u16,
    attempt: AttemptId,
    status: u16,
    response_digest: [32]u8,
};

pub const Fence = struct {
    version: u16,
    coordinator: u64,
    destination: u64,
    worker_incarnation: u64,
    fenced_through: u64,
    quiesced_through: u64,
};

pub fn nodeSubject(buffer: []u8, node_id: u64) ![]const u8 {
    if (node_id == 0) return error.InvalidNodeIdentity;
    return std.fmt.bufPrint(buffer, "node:{d}", .{node_id});
}

pub fn nodeId(subject: []const u8) !u64 {
    if (!std.mem.startsWith(u8, subject, "node:")) return error.InvalidNodeIdentity;
    const number = subject[5..];
    if (number.len == 0 or number[0] == '0') return error.InvalidNodeIdentity;
    for (number) |byte| if (byte < '0' or byte > '9') return error.InvalidNodeIdentity;
    return std.fmt.parseUnsigned(u64, number, 10) catch error.InvalidNodeIdentity;
}

fn validAttempt(id: AttemptId) bool {
    return id.coordinator != 0 and id.generation != 0 and id.sequence != 0 and
        id.operation != 0 and id.destination != 0 and id.worker_incarnation != 0;
}

fn hashPart(hash: anytype, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, @intCast(bytes.len), .big);
    hash.update(&length);
    hash.update(bytes);
}

pub fn requestDigest(method: []const u8, target: []const u8, body: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hashPart(&hash, method);
    hashPart(&hash, target);
    hashPart(&hash, body);
    return hash.finalResult();
}

fn responseDigest(body: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &result, .{});
    return result;
}

pub fn signRequest(alloc: std.mem.Allocator, keys: Keys, request: Request) ![]u8 {
    if (request.version != 1 or !validAttempt(request.attempt) or request.remaining_ns == 0)
        return error.InvalidAttemptFrame;
    return sign(alloc, keys, "antfly-workload-request-v1", request);
}

/// subject must come from verified internal-service authentication, never
/// from a request field. A valid frame is still a retransmission until deduped.
pub fn verifyRequest(alloc: std.mem.Allocator, keys: Keys, frame: []const u8, subject: []const u8, method: []const u8, target: []const u8, body: []const u8) !Request {
    const request = try verify(Request, alloc, keys, "antfly-workload-request-v1", frame);
    if (request.version != 1) return error.UnsupportedAttemptProtocol;
    if (!validAttempt(request.attempt) or request.remaining_ns == 0) return error.InvalidAttemptFrame;
    if (request.attempt.coordinator != try nodeId(subject)) return error.AttemptIdentityMismatch;
    if (!std.mem.eql(u8, &request.request_digest, &requestDigest(method, target, body))) return error.AttemptRequestMismatch;
    return request;
}

/// The caller must already have verified execution quiescence. This function
/// supplies authentication, not evidence that an HTTP error stopped work.
pub fn signTerminalAfterQuiescence(alloc: std.mem.Allocator, keys: Keys, id: AttemptId, status: u16, body: []const u8) ![]u8 {
    if (!validAttempt(id) or status < 100 or status > 599) return error.InvalidAttemptFrame;
    return sign(alloc, keys, "antfly-workload-terminal-v1", Terminal{
        .version = 1,
        .attempt = id,
        .status = status,
        .response_digest = responseDigest(body),
    });
}

pub fn verifyTerminal(alloc: std.mem.Allocator, keys: Keys, frame: []const u8, expected: AttemptId, status: u16, body: []const u8) !Terminal {
    const terminal = try verify(Terminal, alloc, keys, "antfly-workload-terminal-v1", frame);
    if (terminal.version != 1) return error.UnsupportedAttemptProtocol;
    if (!validAttempt(expected) or !std.meta.eql(terminal.attempt, expected)) return error.AttemptIdentityMismatch;
    if (terminal.status != status or !std.mem.eql(u8, &terminal.response_digest, &responseDigest(body))) return error.AttemptResponseMismatch;
    return terminal;
}

pub fn signFenceAfterQuiescence(alloc: std.mem.Allocator, keys: Keys, fence: Fence) ![]u8 {
    if (fence.version != 1 or fence.coordinator == 0 or fence.destination == 0 or fence.worker_incarnation == 0 or
        fence.fenced_through == 0 or fence.quiesced_through > fence.fenced_through) return error.InvalidAttemptFrame;
    return sign(alloc, keys, "antfly-workload-fence-v1", fence);
}

pub fn verifyFence(alloc: std.mem.Allocator, keys: Keys, frame: []const u8, expected: AttemptId) !attempts.FenceEvidence {
    const fence = try verify(Fence, alloc, keys, "antfly-workload-fence-v1", frame);
    if (fence.version != 1) return error.UnsupportedAttemptProtocol;
    if (fence.coordinator != expected.coordinator or fence.destination != expected.destination or
        fence.worker_incarnation != expected.worker_incarnation) return error.AttemptIdentityMismatch;
    if (expected.generation == 0 or fence.fenced_through < expected.generation or fence.quiesced_through < expected.generation or
        fence.quiesced_through > fence.fenced_through) return error.FencingRequired;
    return .{ .destination = fence.destination, .worker_incarnation = fence.worker_incarnation, .fenced_through = fence.fenced_through, .quiesced_through = fence.quiesced_through };
}

fn signature(keys: Keys, secret: []const u8, domain: []const u8, payload: []const u8) [32]u8 {
    var mac = Hmac.init(secret);
    hashPart(&mac, domain);
    hashPart(&mac, keys.issuer);
    hashPart(&mac, payload);
    var result: [32]u8 = undefined;
    mac.final(&result);
    return result;
}

fn validateKeys(keys: Keys) !void {
    if (keys.primary.len < 32 or keys.issuer.len == 0 or keys.issuer.len > 256) return error.AttemptAuthenticationUnavailable;
    if (keys.verification) |key| if (key.len < 32) return error.AttemptAuthenticationUnavailable;
}

fn sign(alloc: std.mem.Allocator, keys: Keys, domain: []const u8, value: anytype) ![]u8 {
    try validateKeys(keys);
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(json);
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const payload_len = encoder.calcSize(json.len);
    const length = payload_len + 1 + encoder.calcSize(32);
    if (length > max_frame_bytes) return error.InvalidAttemptFrame;
    const frame = try alloc.alloc(u8, length);
    _ = encoder.encode(frame[0..payload_len], json);
    frame[payload_len] = '.';
    const mac = signature(keys, keys.primary, domain, frame[0..payload_len]);
    _ = encoder.encode(frame[payload_len + 1 ..], &mac);
    return frame;
}

fn verify(comptime T: type, alloc: std.mem.Allocator, keys: Keys, domain: []const u8, frame: []const u8) !T {
    try validateKeys(keys);
    if (frame.len > max_frame_bytes) return error.InvalidAttemptFrame;
    const separator = std.mem.indexOfScalar(u8, frame, '.') orelse return error.InvalidAttemptFrame;
    const payload = frame[0..separator];
    const encoded_mac = frame[separator + 1 ..];
    const decoder = std.base64.url_safe_no_pad.Decoder;
    if ((decoder.calcSizeForSlice(encoded_mac) catch return error.InvalidAttemptFrame) != 32) return error.InvalidAttemptFrame;
    var supplied: [32]u8 = undefined;
    decoder.decode(&supplied, encoded_mac) catch return error.InvalidAttemptFrame;
    const primary = signature(keys, keys.primary, domain, payload);
    const additional = signature(keys, keys.verification orelse keys.primary, domain, payload);
    const primary_ok = std.crypto.timing_safe.eql([32]u8, supplied, primary);
    const additional_ok = std.crypto.timing_safe.eql([32]u8, supplied, additional);
    if (!primary_ok and !additional_ok) return error.InvalidAttemptSignature;
    const decoded_len = decoder.calcSizeForSlice(payload) catch return error.InvalidAttemptFrame;
    const decoded = try alloc.alloc(u8, decoded_len);
    defer alloc.free(decoded);
    decoder.decode(decoded, payload) catch return error.InvalidAttemptFrame;
    var parsed = std.json.parseFromSlice(T, alloc, decoded, .{}) catch return error.InvalidAttemptFrame;
    defer parsed.deinit();
    return parsed.value; // All frames have fixed-size fields and no borrowed strings.
}

test "workload admission remote signatures bind node identity payload and rotation" {
    const alloc = std.testing.allocator;
    const old: Keys = .{ .primary = "a" ** 32, .issuer = "cluster" };
    const rotated: Keys = .{ .primary = "b" ** 32, .verification = old.primary, .issuer = "cluster" };
    const id: AttemptId = .{ .coordinator = 7, .generation = 2, .sequence = 3, .operation = 4, .destination = 8, .worker_incarnation = 9 };
    const request: Request = .{ .version = 1, .attempt = id, .remaining_ns = 100, .request_digest = requestDigest("POST", "/join", "body") };
    const frame = try signRequest(alloc, old, request);
    defer alloc.free(frame);
    _ = try verifyRequest(alloc, rotated, frame, "node:7", "POST", "/join", "body");
    try std.testing.expectError(error.AttemptIdentityMismatch, verifyRequest(alloc, rotated, frame, "node:8", "POST", "/join", "body"));
    try std.testing.expectError(error.AttemptRequestMismatch, verifyRequest(alloc, rotated, frame, "node:7", "POST", "/other", "body"));
    try std.testing.expectError(error.AttemptRequestMismatch, verifyRequest(alloc, rotated, frame, "node:7", "POST", "/join", "changed"));
    try std.testing.expectError(error.InvalidNodeIdentity, verifyRequest(alloc, rotated, frame, "antfly-node", "POST", "/join", "body"));
    try std.testing.expectError(error.InvalidNodeIdentity, nodeId("node:007"));
    var wrong_cluster = rotated;
    wrong_cluster.issuer = "other";
    try std.testing.expectError(error.InvalidAttemptSignature, verifyRequest(alloc, wrong_cluster, frame, "node:7", "POST", "/join", "body"));
    frame[0] = if (frame[0] == 'A') 'B' else 'A';
    try std.testing.expectError(error.InvalidAttemptSignature, verifyRequest(alloc, rotated, frame, "node:7", "POST", "/join", "body"));
}

test "workload admission remote terminal and fence evidence cannot cross attempts or incarnations" {
    const alloc = std.testing.allocator;
    const keys: Keys = .{ .primary = "a" ** 32, .issuer = "cluster" };
    const id: AttemptId = .{ .coordinator = 7, .generation = 2, .sequence = 3, .operation = 4, .destination = 8, .worker_incarnation = 9 };
    const terminal = try signTerminalAfterQuiescence(alloc, keys, id, 200, "result");
    defer alloc.free(terminal);
    _ = try verifyTerminal(alloc, keys, terminal, id, 200, "result");
    try std.testing.expectError(error.AttemptResponseMismatch, verifyTerminal(alloc, keys, terminal, id, 503, "result"));
    try std.testing.expectError(error.AttemptResponseMismatch, verifyTerminal(alloc, keys, terminal, id, 200, "changed"));
    inline for (.{ "coordinator", "generation", "sequence", "operation", "destination", "worker_incarnation" }) |field| {
        var replay = id;
        @field(replay, field) += 1;
        try std.testing.expectError(error.AttemptIdentityMismatch, verifyTerminal(alloc, keys, terminal, replay, 200, "result"));
    }
    const fence = try signFenceAfterQuiescence(alloc, keys, .{ .version = 1, .coordinator = 7, .destination = 8, .worker_incarnation = 9, .fenced_through = 2, .quiesced_through = 2 });
    defer alloc.free(fence);
    _ = try verifyFence(alloc, keys, fence, id);
    try std.testing.expectError(error.InvalidAttemptSignature, verifyTerminal(alloc, keys, fence, id, 200, "result"));
    var newer = id;
    newer.generation = 3;
    try std.testing.expectError(error.FencingRequired, verifyFence(alloc, keys, fence, newer));
    newer = id;
    newer.worker_incarnation = 10;
    try std.testing.expectError(error.AttemptIdentityMismatch, verifyFence(alloc, keys, fence, newer));
    try std.testing.expectError(error.InvalidAttemptFrame, verifyFence(alloc, keys, "x" ** (max_frame_bytes + 1), id));
}
