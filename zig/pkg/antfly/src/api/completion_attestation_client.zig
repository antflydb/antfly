// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One bounded attestation exchange. No retry, authority cache, or metadata
//! mutation occurs here; activation must validate all returned peer proofs.
const std = @import("std");
const http = @import("../common/http/http_common.zig");
const auth = @import("internal_service_auth.zig");
const protocol = @import("completion_attestation_protocol.zig");
const platform_time = @import("antfly_platform").time;
const workspace_bytes = 192 * 1024;

pub const Options = struct {
    /// Original absolute native-monotonic deadline, including any prior queue.
    deadline_ns: u64,
    cancellation: ?*const http.RequestCancellation = null,
};

fn check(cancellation: *const http.RequestCancellation) !u32 {
    try cancellation.token().check();
    const deadline = cancellation.query_deadline_ns orelse return error.DeadlineExceeded;
    const now = platform_time.monotonicNs();
    if (now >= deadline) return error.DeadlineExceeded;
    return @intCast(@min(std.math.maxInt(u32), (deadline - now - 1) / std.time.ns_per_ms + 1));
}

pub fn fetch(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, keys: protocol.Keys, request: protocol.Request, options: Options) !protocol.Proof {
    try protocol.validateRequest(request);
    try protocol.validateKeys(keys);
    const uri = std.Uri.parse(base_uri) catch return error.InvalidCompletionAttestation;
    const path = switch (uri.path) {
        .raw => |value| value,
        .percent_encoded => |value| value,
    };
    if ((!std.mem.eql(u8, uri.scheme, "http") and !std.mem.eql(u8, uri.scheme, "https")) or
        uri.host == null or uri.user != null or uri.password != null or uri.query != null or uri.fragment != null or
        (path.len != 0 and !std.mem.eql(u8, path, "/"))) return error.InvalidCompletionAttestation;
    var cancellation = if (options.cancellation) |original| http.RequestCancellation.fromToken(original.token()) else http.RequestCancellation{};
    cancellation.allocation_owner = if (options.cancellation) |original| original.allocation_owner else null;
    cancellation.query_deadline_ns = if (options.cancellation) |original| @min(options.deadline_ns, original.query_deadline_ns orelse options.deadline_ns) else options.deadline_ns;
    _ = try check(&cancellation);
    // All transport allocations requested from this caller, decoding, signing
    // and verification share one finite workspace charged to its allocator.
    const workspace = try alloc.alloc(u8, workspace_bytes);
    defer alloc.free(workspace);
    var fixed = std.heap.FixedBufferAllocator.init(workspace);
    const bounded = fixed.allocator();
    const target = try std.fmt.allocPrint(bounded, "{s}/internal/v1/groups/{d}/completion/attestation", .{ std.mem.trimEnd(u8, base_uri, "/"), request.group_id });
    const body = try std.json.Stringify.valueAlloc(bounded, request, .{});
    if (body.len > protocol.max_request_bytes) return error.InvalidCompletionAttestation;
    var response = try auth.executeRequest(bounded, executor, .{
        .method = .POST,
        .uri = target,
        .source_node_id = request.requester,
        .content_type = "application/json",
        .body = body,
        .timeout_ms = try check(&cancellation),
        .cancellation = &cancellation,
    }, .{ .secret = keys.primary, .issuer = keys.issuer, .node_id = request.requester });
    defer response.deinit(bounded);
    _ = try check(&cancellation);
    if (response.status != 200) return error.CompletionAdmissionUnavailable;
    if (response.body.len > protocol.max_frame_bytes + 64) return error.InvalidCompletionAttestation;
    const decoded = std.json.parseFromSlice(struct { evidence: []const u8 }, bounded, response.body, .{}) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidCompletionAttestation,
    };
    defer decoded.deinit();
    const proof = try protocol.verify(bounded, keys, decoded.value.evidence, request);
    _ = try check(&cancellation);
    return proof;
}

test "workload admission completion attestation client rejects capability flags stale proof and late success" {
    const alloc = std.testing.allocator;
    const keys: protocol.Keys = .{ .primary = "q" ** 32, .issuer = "cluster" };
    const challenge: protocol.Request = .{ .requester = 3, .node_id = 5, .group_id = 7, .nonce = 11, .incarnation = @splat(13), .policy_digest = @splat(17), .generation = 19 };
    const Fake = struct {
        mode: enum { valid, flags_only, stale, unavailable, oversized, cancelled } = .valid,
        calls: usize = 0,
        cancellation: *http.RequestCancellation,
        expected_deadline: u64,
        fn execute(raw: *anyopaque, a: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(http.Method.POST, req.method);
            try std.testing.expectEqualStrings("http://worker.invalid:8080/internal/v1/groups/7/completion/attestation", req.uri);
            try std.testing.expect(req.header(auth.header_name) != null);
            try std.testing.expectEqual(self.expected_deadline, req.cancellation.?.query_deadline_ns.?);
            try std.testing.expect(req.timeout_ms.? <= 10000);
            if (self.mode == .unavailable) return .{ .status = 503 };
            if (self.mode == .oversized) return .{ .status = 200, .body = try a.alloc(u8, protocol.max_frame_bytes + 65) };
            if (self.mode == .flags_only) return .{ .status = 200, .body = try a.dupe(u8, "{\"completion_protocol_version\":1}") };
            var parsed = try std.json.parseFromSlice(protocol.Request, a, req.body, .{});
            defer parsed.deinit();
            if (self.mode == .stale) parsed.value.nonce += 1;
            var proof: protocol.Proof = .{ .request = parsed.value, .capacity = 4, .accepted = 1, .prepared = 1, .term = 2, .commit_index = 3, .applied_index = 3, .last_index = 3, .leader_id = 5, .membership = .{ .voters = 1 } };
            proof.membership.nodes[0] = 5;
            const frame = try protocol.sign(a, .{ .primary = "q" ** 32, .issuer = "cluster" }, proof);
            defer a.free(frame);
            const body = try std.json.Stringify.valueAlloc(a, .{ .evidence = frame }, .{});
            if (self.mode == .cancelled) self.cancellation.cancel();
            return .{ .status = 200, .body = body };
        }
    };
    var original: http.RequestCancellation = .{ .query_deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s };
    var fake: Fake = .{ .cancellation = &original, .expected_deadline = original.query_deadline_ns.? };
    const executor: http.RequestExecutor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } };
    const options: Options = .{ .deadline_ns = original.query_deadline_ns.? + 10 * std.time.ns_per_s, .cancellation = &original };
    const proof = try fetch(alloc, executor, "http://worker.invalid:8080/", keys, challenge, options);
    try std.testing.expectEqual(@as(u32, 4), proof.capacity);
    fake.mode = .flags_only;
    try std.testing.expectError(error.InvalidCompletionAttestation, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    fake.mode = .stale;
    try std.testing.expectError(error.CompletionAttestationIdentityMismatch, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    fake.mode = .unavailable;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    fake.mode = .oversized;
    try std.testing.expectError(error.InvalidCompletionAttestation, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    fake.mode = .cancelled;
    try std.testing.expectError(error.Canceled, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    const before = fake.calls;
    try std.testing.expectError(error.Canceled, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    try std.testing.expectEqual(before, fake.calls);
    original.cancelled.store(false, .release);
    original.query_deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, fetch(alloc, executor, "http://worker.invalid:8080", keys, challenge, options));
    try std.testing.expectEqual(before, fake.calls);
    try std.testing.expectError(error.InvalidCompletionAttestation, fetch(alloc, executor, "http://worker.invalid:8080/public", keys, challenge, options));
}
