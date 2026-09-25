// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One authenticated installation read. No retry or authority cache; native
//! installation still validates the returned public catalog against its DB.
const std = @import("std");
const http = @import("../common/http/http_common.zig");
const auth = @import("../api/internal_service_auth.zig");
pub const protocol = @import("completion_installation_protocol.zig");
const platform_time = @import("antfly_platform").time;
pub const workspace_bytes = 8 * 1024 * 1024;
pub const Options = struct {
    deadline_ns: u64,
    cancellation: ?*const http.RequestCancellation = null,
};
pub const Result = struct {
    alloc: std.mem.Allocator,
    workspace: []u8,
    fixed: std.heap.FixedBufferAllocator,
    parsed: ?std.json.Parsed(protocol.Response) = null,

    pub fn value(self: *const Result) protocol.Response {
        return self.parsed.?.value;
    }
    pub fn deinit(self: *Result) void {
        const alloc = self.alloc;
        if (self.parsed) |*parsed| parsed.deinit();
        alloc.free(self.workspace);
        alloc.destroy(self);
    }
};
/// Converts an already verified response into the fixed native-open binding.
/// This is an installation intent only; the native installer must validate its
/// own catalog and reserve backing before publishing an acquisition provider.
pub fn binding(response: protocol.Response) !?@import("kernel_owner_abi").completion_pool.InstallBinding {
    const installed = response.installation orelse return null;
    const group = for (installed.groups) |candidate| {
        if (candidate.group_id == response.request.group_id) break candidate;
    } else return error.InvalidCompletionInstallation;
    return .{
        .identity = .{ .capacity = 4, .group_id = group.group_id, .node_id = response.request.requester, .incarnation = installed.groupIncarnation(group), .policy_digest = @import("completion_activation.zig").policyDigest(installed.policy), .generation = installed.generation },
        .schema_catalog_digest = installed.schema_catalog_digest,
        .table_id = installed.table_id,
        .range_id = group.range_id,
        .split_attempt_epoch = group.split_attempt_epoch,
        .expected_definition = installed.expected_definition,
    };
}

fn check(cancellation: *const http.RequestCancellation) !u32 {
    try cancellation.token().check();
    const deadline = cancellation.query_deadline_ns orelse return error.DeadlineExceeded;
    const now = platform_time.monotonicNs();
    if (now >= deadline) return error.DeadlineExceeded;
    return @intCast(@min(std.math.maxInt(u32), (deadline - now - 1) / std.time.ns_per_ms + 1));
}
pub fn fetch(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, keys: protocol.Keys, request: protocol.Request, options: Options) !*Result {
    try request.validate();
    const uri = std.Uri.parse(base_uri) catch return error.InvalidCompletionInstallation;
    const path = switch (uri.path) {
        .raw => |v| v,
        .percent_encoded => |v| v,
    };
    if ((!std.mem.eql(u8, uri.scheme, "http") and !std.mem.eql(u8, uri.scheme, "https")) or uri.host == null or
        uri.user != null or uri.password != null or uri.query != null or uri.fragment != null or
        (path.len != 0 and !std.mem.eql(u8, path, "/"))) return error.InvalidCompletionInstallation;
    var cancellation = if (options.cancellation) |original| http.RequestCancellation.fromToken(original.token()) else http.RequestCancellation{};
    cancellation.allocation_owner = if (options.cancellation) |original| original.allocation_owner else null;
    cancellation.query_deadline_ns = if (options.cancellation) |original| @min(options.deadline_ns, original.query_deadline_ns orelse options.deadline_ns) else options.deadline_ns;
    _ = try check(&cancellation);
    const result = try alloc.create(Result);
    const workspace = alloc.alloc(u8, workspace_bytes) catch |err| {
        alloc.destroy(result);
        return err;
    };
    result.* = .{ .alloc = alloc, .workspace = workspace, .fixed = std.heap.FixedBufferAllocator.init(workspace) };
    errdefer result.deinit();
    // Keep the allocator context heap-stable for the returned parsed ownership.
    const bounded = result.fixed.allocator();
    const target = try std.fmt.allocPrint(bounded, "{s}{s}", .{ std.mem.trimEnd(u8, base_uri, "/"), protocol.path });
    const signed = try protocol.signRequest(bounded, keys, request);
    var response = try auth.executeRequest(bounded, executor, .{
        .method = .POST,
        .uri = target,
        .source_node_id = request.requester,
        .content_type = "text/plain",
        .body = signed,
        .timeout_ms = try check(&cancellation),
        .cancellation = &cancellation,
    }, .{ .secret = keys.primary, .issuer = keys.issuer, .node_id = request.requester });
    defer response.deinit(bounded);
    _ = try check(&cancellation);
    if (response.status != 200) return error.CompletionAdmissionUnavailable;
    result.parsed = try protocol.verifyResponse(bounded, keys, response.body, request);
    _ = try check(&cancellation);
    return result;
}

test "workload admission completion installation client retains parsed ownership and original cancellation" {
    const alloc = std.testing.allocator;
    const keys: protocol.Keys = .{ .primary = "k" ** 32, .issuer = "cluster" };
    const request: protocol.Request = .{ .requester = 4, .group_id = 7, .cluster_incarnation = "11111111111111111111111111111111".*, .nonce = 19 };
    const Fake = struct {
        cancellation: *http.RequestCancellation,
        mode: enum { valid, stale, flags, cancelled } = .valid,
        calls: usize = 0,
        fn execute(raw: *anyopaque, a: std.mem.Allocator, input: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqualStrings("http://metadata.invalid" ++ protocol.path, input.uri);
            try std.testing.expect(input.header(auth.header_name) != null);
            try std.testing.expectEqual(self.cancellation.query_deadline_ns, input.cancellation.?.query_deadline_ns);
            if (self.mode == .flags) return .{ .status = 200, .body = try a.dupe(u8, "{\"enabled\":true}") };
            var challenge = try protocol.verifyRequest(a, keys, input.body);
            if (self.mode == .stale) challenge.nonce += 1;
            const response: protocol.Response = .{
                .request = challenge,
                .installation = .{
                    .cluster_incarnation = challenge.cluster_incarnation,
                    .table_id = 1,
                    .expected_definition = @splat(1),
                    .schema_catalog_digest = try @import("../common/completion_catalog_digest.zig").digest(a, "{}", "", "{}"),
                    .expected_transition_generation = 0,
                    .generation = 1,
                    .policy = .{ .protocol_version = 1, .max_count = 4, .max_bytes = 65536, .max_transaction_bytes = 16384, .completion_protocol_version = 1, .profile_version = 1 },
                    .groups = &.{.{ .group_id = 7, .range_id = 1, .split_attempt_epoch = 0, .range_digest = @splat(2) }},
                },
                .schema_json = "{}",
                .read_schema_json = "",
                .indexes_json = "{}",
            };
            const frame = try protocol.signResponse(a, keys, response);
            if (self.mode == .cancelled) self.cancellation.cancel();
            return .{ .status = 200, .body = frame };
        }
    };
    var cancellation: http.RequestCancellation = .{ .query_deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s };
    var fake: Fake = .{ .cancellation = &cancellation };
    const executor: http.RequestExecutor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } };
    const options: Options = .{ .deadline_ns = cancellation.query_deadline_ns.? + std.time.ns_per_s, .cancellation = &cancellation };
    const result = try fetch(alloc, executor, "http://metadata.invalid", keys, request, options);
    try std.testing.expectEqualStrings("{}", result.value().schema_json);
    try std.testing.expectEqual(@as(u64, 1), result.value().installation.?.generation);
    try std.testing.expectEqual(@as(u64, 7), (try binding(result.value())).?.identity.group_id);
    result.deinit();
    fake.mode = .stale;
    try std.testing.expectError(error.InvalidCompletionInstallation, fetch(alloc, executor, "http://metadata.invalid", keys, request, options));
    fake.mode = .flags;
    try std.testing.expectError(error.InvalidCompletionInstallation, fetch(alloc, executor, "http://metadata.invalid", keys, request, options));
    fake.mode = .cancelled;
    try std.testing.expectError(error.Canceled, fetch(alloc, executor, "http://metadata.invalid", keys, request, options));
    const calls = fake.calls;
    try std.testing.expectError(error.Canceled, fetch(alloc, executor, "http://metadata.invalid", keys, request, options));
    try std.testing.expectEqual(calls, fake.calls);
}
