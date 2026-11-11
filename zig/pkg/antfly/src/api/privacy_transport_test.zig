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

const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("../raft/transport/http_common.zig");

test "privacy HTTP reads reject peers that omit or mismatch the policy acknowledgment" {
    const a = std.testing.allocator;
    const Fake = struct {
        ack: bool = false,
        version: [64]u8,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var result = http.HttpResponse{ .status = 200, .body = try alloc.dupe(u8, "{}") };
            if (self.ack) {
                result.headers = try alloc.alloc(http.Header, 1);
                result.headers[0] = .{ .name = try alloc.dupe(u8, "X-Antfly-Privacy-Fence"), .value = try alloc.dupe(u8, &self.version) };
            }
            return result;
        }
    };
    const version = local.privacy_contract.fingerprint("policy");
    var fake = Fake{ .version = version };
    var client = @import("http_client.zig").ApiHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    const body = try std.fmt.allocPrint(a, "{{\"_privacy_policy_version\":\"{s}\"}}", .{version});
    defer a.free(body);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupQuery("http://peer", 7, "docs", body));
    fake.ack = true;
    var response = try client.fetchGroupQuery("http://peer", 7, "docs", body);
    response.deinit(a);
    fake.version[0] = if (version[0] == '0') '1' else '0';
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupQuery("http://peer", 7, "docs", body));
}

test "privacy receivers fence legacy lookups and reject superseded assertions before reading" {
    const source = local.api_table_read_source;
    const types = local.storage_db_types;
    const Consistency = local.storage_read_consistency.ReadConsistency;
    const Fake = struct {
        version: [64]u8,
        calls: usize = 0,
        last: types.LookupOptions = .{},
        captured_deadline: ?u64 = null,
        fn capture(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, deadline: ?u64) ![64]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.captured_deadline = deadline;
            if (deadline) |end| if (end <= @import("antfly_platform").time.monotonicNs()) return error.DeadlineExceeded;
            return self.version;
        }
        fn lookup(raw: *anyopaque, a: std.mem.Allocator, table: []const u8, key: []const u8, opts: types.LookupOptions, consistency: Consistency) !?source.LookupResponse {
            return lookupLocal(raw, a, 7, table, key, opts, consistency);
        }
        fn lookupLocal(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8, opts: types.LookupOptions, _: Consistency) !?source.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.last = opts;
            return null;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: Consistency) !?source.ScanResponse {
            unreachable;
        }
        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.SearchRequest, _: Consistency) !?local.api_query_response.QueryResponse {
            unreachable;
        }
    };
    const version = local.privacy_contract.fingerprint("policy");
    var fake = Fake{ .version = version };
    const reads = source.TableReadSource{ .ptr = &fake, .vtable = &.{ .privacy_policy_version = Fake.capture, .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query, .lookup_group_local = Fake.lookupLocal } };
    _ = try reads.lookupGroupLocal(std.testing.allocator, 7, "docs", "doc", .{}, .read_index);
    try std.testing.expectEqual(version, fake.last.privacy_policy_version.?);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    const superseded = local.privacy_contract.fingerprint("previous policy");
    try std.testing.expectError(error.IndexRebuilding, reads.lookupGroupLocal(std.testing.allocator, 7, "docs", "doc", .{ .privacy_policy_version = superseded }, .read_index));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    _ = try reads.lookupGroupLocal(std.testing.allocator, 7, "docs", "doc", .{ .privacy_original = true }, .read_index);
    try std.testing.expect(fake.last.privacy_original);
    try std.testing.expect(fake.last.privacy_policy_version == null);

    const Clock = struct {
        fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = @as(*const u64, @ptrCast(@alignCast(raw.?))).* };
        }
    };
    var request_now: u64 = 17;
    var vtable = std.testing.io.vtable.*;
    vtable.now = Clock.now;
    const io: std.Io = .{ .userdata = &request_now, .vtable = &vtable };
    const options: types.LookupOptions = .{
        .execution_deadline_ns = request_now + std.time.ns_per_s,
        .execution_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&io),
    };
    for ([_]bool{ false, true }) |group_local| {
        const before = @import("antfly_platform").time.monotonicNs();
        if (group_local) {
            _ = try reads.lookupGroupLocal(std.testing.allocator, 7, "docs", "doc", options, .read_index);
        } else {
            _ = try reads.lookup(std.testing.allocator, "docs", "doc", options, .read_index);
        }
        const after = @import("antfly_platform").time.monotonicNs();
        try std.testing.expect(fake.captured_deadline.? >= before + std.time.ns_per_s);
        try std.testing.expect(fake.captured_deadline.? <= after + std.time.ns_per_s);
        try std.testing.expectEqual(options.execution_deadline_ns, fake.last.execution_deadline_ns);
        try std.testing.expectEqual(options.execution_io.?.userdata, fake.last.execution_io.?.userdata);
    }
    const reads_before_expiry = fake.calls;
    request_now = options.execution_deadline_ns.?;
    try std.testing.expectError(error.DeadlineExceeded, reads.lookupGroupLocal(std.testing.allocator, 7, "docs", "doc", options, .read_index));
    try std.testing.expectEqual(reads_before_expiry, fake.calls);
}

test "privacy routed scans accept the public range response envelope and own cursors" {
    const a = std.testing.allocator;
    var result = try @import("antfly_source_root").antfly_sources.table_writes.parseStorageKernelDocumentArtifactTableReprocessResult(a, "{\"reprocess\":\"triggered\",\"reprocess_status\":\"in_progress\",\"artifact_name\":\"_privacy_document\",\"scanned\":0,\"reprocessed\":0,\"skipped\":0,\"failed\":0,\"limit\":100,\"next_key\":\"\",\"pending_shards\":1,\"failures\":[],\"shard_cursors\":[]}");
    defer result.deinit(a);
    try std.testing.expect(result.next_key != null);
    try std.testing.expectEqualStrings("", result.next_key.?);
    try std.testing.expectEqual(@as(u32, 100), result.limit);
}

test "privacy SQL statement scans carry a current policy assertion before opening owners" {
    const reads = local.api_table_read_source;
    const Consistency = local.storage_read_consistency.ReadConsistency;
    const Fake = struct {
        version: [64]u8,
        calls: usize = 0,
        fn capture(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: ?u64) ![64]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.version;
        }
        fn open(raw: *anyopaque, _: std.mem.Allocator, scans: []const reads.RelationalStatementScan, _: Consistency) !reads.RelationalStatementRead {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), scans.len);
            try std.testing.expectEqualSlices(u8, &self.version, &scans[0].opts.privacy_policy_version.?);
            self.calls += 1;
            return error.TestCaptured;
        }
    };
    var fake = Fake{ .version = local.privacy_contract.fingerprint("policy") };
    const source: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .privacy_policy_version = Fake.capture, .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fake.open } };
    const scans = [_]reads.RelationalStatementScan{.{ .table = "docs", .from = "", .to = "", .opts = .{} }};
    try std.testing.expectError(error.TestCaptured, source.openRelationalStatement(std.testing.allocator, &scans, .read_index));
    var stale = scans;
    stale[0].opts.privacy_policy_version = local.privacy_contract.fingerprint("superseded");
    try std.testing.expectError(error.IndexRebuilding, source.openRelationalStatement(std.testing.allocator, &stale, .read_index));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "privacy policy capture preserves errors through the foreign runtime dispatcher" {
    const api = local.api_table_read_source;
    const abi = @import("antfly_runtime_abi");
    const Boundary = abi.callback_abi.Boundary(api.TableReadSource.VTable);
    const Fake = struct {
        fn capture(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: ?u64) ![64]u8 {
            const err: *anyerror = @ptrCast(@alignCast(raw));
            return err.*;
        }
        fn dispatch(contract: *const abi.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) abi.error_abi.Status {
            return Boundary.local_dispatch(contract, callback, args, output);
        }
        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: local.storage_db_types.LookupOptions, _: local.storage_read_consistency.ReadConsistency) !?api.LookupResponse {
            unreachable;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: local.storage_db_types.ScanOptions, _: local.storage_read_consistency.ReadConsistency) !?api.ScanResponse {
            unreachable;
        }
        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: local.storage_db_types.SearchRequest, _: local.storage_read_consistency.ReadConsistency) !?local.api_query_response.QueryResponse {
            unreachable;
        }
    };
    var err: anyerror = error.NotLeader;
    const source = api.TableReadSource{ .ptr = &err, .boundary_dispatch = Fake.dispatch, .vtable = &.{ .privacy_policy_version = Fake.capture, .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
    try std.testing.expectError(error.NotLeader, source.privacyPolicyVersion(std.testing.allocator, "docs", null));
    err = error.DeadlineExceeded;
    try std.testing.expectError(error.DeadlineExceeded, source.privacyPolicyVersion(std.testing.allocator, "docs", null));
}

test "privacy long document keys use bounded POST routes and preserve read controls" {
    const a = std.testing.allocator;
    const key = try a.alloc(u8, 36000);
    defer a.free(key);
    for (key, 0..) |*byte, i| byte.* = switch (i % 3) {
        0 => 255,
        1 => '"',
        else => 'x',
    };
    const Fake = struct {
        key: []const u8,
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(req.method == .POST);
            try std.testing.expect(req.uri.len < 200);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7/tables/docs/document-read"));
            var input = try std.json.parseFromSlice(@import("http_routes.zig").DocumentReadRequest, alloc, req.body, .{});
            defer input.deinit();
            const decoded = try input.value.decodeKey(alloc);
            defer alloc.free(decoded);
            try std.testing.expectEqualStrings(self.key, decoded);
            if (self.calls == 0) {
                try std.testing.expect(input.value.operation == .lookup);
                try std.testing.expect(std.mem.indexOf(u8, input.value.query, "read_consistency=stale") != null);
                try std.testing.expect(std.mem.indexOf(u8, input.value.query, "_privacy_original=true") != null);
                try std.testing.expect(std.mem.indexOf(u8, input.value.query, "fields=body") != null);
                try std.testing.expectEqual(@as(?u32, 37), req.timeout_ms);
            } else if (self.calls == 1) {
                try std.testing.expect(input.value.operation == .manifest);
                try std.testing.expectEqualStrings("_privacy_document", input.value.artifact_name);
            } else try std.testing.expect(input.value.operation == .manifests);
            self.calls += 1;
            return .{ .status = 200, .body = try alloc.dupe(u8, "{}") };
        }
    };
    var fake = Fake{ .key = key };
    var client = @import("http_client.zig").ApiHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    var lookup = try client.fetchGroupLookupWithMode("http://peer", 7, "docs", key, "body", "stale", 37, null, false, false, "", "", "", "", "", null, null, false, true, null);
    defer lookup.deinit(a);
    var manifest = try client.fetchGroupDocumentArtifactManifest("http://peer", 7, "docs", key, "_privacy_document");
    defer manifest.deinit(a);
    var manifests = try client.fetchGroupDocumentArtifactManifests("http://peer", 7, "docs", key);
    defer manifests.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "privacy manifest reads propagate cancellation and remaining HTTP deadline" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/document-read"));
            try std.testing.expect(req.timeout_ms.? > 0 and req.timeout_ms.? <= 1000);
            try std.testing.expect(req.cancellation != null);
            var found = false;
            for (req.headers) |header| if (std.ascii.eqlIgnoreCase(header.name, "x-antfly-timeout-ms")) {
                try std.testing.expectEqual(req.timeout_ms.?, try std.fmt.parseUnsigned(u32, header.value, 10));
                found = true;
            };
            try std.testing.expect(found);
            return .{ .status = 504, .body = try alloc.dupe(u8, "{}") };
        }
    };
    var fake: Fake = .{};
    var client = @import("http_client.zig").ApiHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    var canceled = std.atomic.Value(bool).init(false);
    const opts: local.storage_db_types.LookupOptions = .{
        .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled),
        .execution_deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s,
    };
    try std.testing.expectError(error.Timeout, client.fetchGroupDocumentArtifactManifestControlled("http://peer", 7, "docs", "key", "_privacy_document", opts));
    canceled.store(true, .release);
    try std.testing.expectError(error.Cancelled, client.fetchGroupDocumentArtifactManifestControlled("http://peer", 7, "docs", "key", "_privacy_document", opts));
    canceled.store(false, .release);
    var expired = opts;
    expired.execution_deadline_ns = 0;
    try std.testing.expectError(error.Timeout, client.fetchGroupDocumentArtifactManifestControlled("http://peer", 7, "docs", "key", "_privacy_document", expired));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    // A source that only implements the old callback cannot silently discard
    // controls when called from findings or authenticated group ingress.
    canceled.store(true, .release);
    const source: local.api_table_read_source.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } };
    try std.testing.expectError(error.Cancelled, source.documentArtifactManifestControlled(a, "docs", "key", "_privacy_document", .read_index, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled), .execution_deadline_ns = 0 }));
}

test "privacy remote mutations preserve preconditions and never replay ambiguous scan admission" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        ambiguous: bool = false,
        unavailable: bool = false,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(req.timeout_ms.? > 0 and req.timeout_ms.? <= 1000);
            try std.testing.expect(req.cancellation != null);
            if (self.ambiguous) {
                try std.testing.expectEqual(.POST, req.method);
                try std.testing.expect(std.mem.endsWith(u8, req.uri, "/privacy/scans"));
                return error.Timeout;
            }
            try std.testing.expectEqual(.PUT, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/tables/db%2Fpublic%2Fdocs/privacy"));
            var found = false;
            for (req.headers) |header| if (std.ascii.eqlIgnoreCase(header.name, "If-Match")) {
                try std.testing.expectEqualStrings("tag", header.value);
                found = true;
            };
            try std.testing.expect(found);
            if (self.unavailable) {
                const headers = try alloc.alloc(http.Header, 1);
                headers[0] = .{ .name = try alloc.dupe(u8, "Retry-After"), .value = try alloc.dupe(u8, "1") };
                return .{ .status = 503, .body = try alloc.dupe(u8, "{\"code\":\"privacy_storage_unavailable\",\"retryable\":true}"), .headers = headers };
            }
            const headers = try alloc.alloc(http.Header, 1);
            headers[0] = .{ .name = try alloc.dupe(u8, "ETag"), .value = try alloc.dupe(u8, "new-tag") };
            return .{ .status = 202, .body = try alloc.dupe(u8, "{\"configured\":true}"), .headers = headers };
        }
    };
    var fake: Fake = .{};
    var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    const request: local.api_operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s };
    var result = try client.forwardPrivacyMutation("http://metadata", "db/public/docs", "{}", "tag", false, request);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(u16, 202), result.response.status);
    try std.testing.expectEqualStrings("new-tag", result.etag.?);
    fake.unavailable = true;
    var unavailable = try client.forwardPrivacyMutation("http://metadata", "db/public/docs", "{}", "tag", false, request);
    defer unavailable.deinit(a);
    try std.testing.expectEqual(@as(u16, 503), unavailable.response.status);
    try std.testing.expectEqual(@as(?u32, 1), unavailable.response.retry_after_seconds);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    fake.ambiguous = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, client.forwardPrivacyMutation("http://metadata", "docs", "{}", null, true, request));
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "privacy checkpoint job commands require the same coordinated decoder as catalog attachment" {
    const service = @import("../metadata/service.zig");
    const protocol = @import("../metadata/topology_protocol.zig");
    try std.testing.expectEqual(protocol.privacy_version, service.transitionRequiredCoordinatedDecoderVersion(.{ .compare_and_set_job_state = .{ .key = "privacy/m/checkpoint", .expected_hash = "", .value = "{}" } }));
    try std.testing.expectEqual(protocol.privacy_version, service.transitionRequiredCoordinatedDecoderVersion(.{ .compare_and_set_job_state = .{ .key = "privacy/c/checkpoint", .expected_hash = "", .value = "Vchunk" } }));
    try std.testing.expectEqual(protocol.job_state_version, service.transitionRequiredCoordinatedDecoderVersion(.{ .compare_and_set_job_state = .{ .key = "documents/run", .expected_hash = "", .value = "{}" } }));
}

test "privacy scratch preserves both outer and inner invocation limits" {
    const work = @import("antfly_inference_work");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var outer = work.BoundedInvocationAllocator.init(arena.allocator(), 64);
    var inner = work.BoundedInvocationAllocator.init(outer.allocator(), 1024);
    var scratch = work.ReclaimingInvocationAllocator.init(inner.allocator());
    const a = scratch.allocator();
    const bytes = try a.alloc(u8, 32);
    try std.testing.expectEqual(@as(usize, 32), outer.live_bytes);
    try std.testing.expectEqual(@as(usize, 32), inner.live_bytes);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 40));
    try std.testing.expect(outer.limit_exceeded);
    try std.testing.expect(!inner.limit_exceeded);
    a.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), outer.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), inner.live_bytes);
    outer.limit_exceeded = false;
    inner.max_live_bytes = 16;
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 17));
    try std.testing.expect(inner.limit_exceeded);
    try std.testing.expect(!outer.limit_exceeded);
}

test "privacy remote mutation replies retain upstream outcome on every status without replay" {
    const a = std.testing.allocator;
    const Fake = struct {
        status: u16 = 202,
        unknown: bool = false,
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(req.method == .PUT or req.method == .POST);
            try std.testing.expectEqualStrings("{}", req.body);
            const headers = try alloc.alloc(http.Header, if (self.unknown) 3 else 1);
            headers[0] = .{ .name = try alloc.dupe(u8, "ETag"), .value = try alloc.dupe(u8, "upstream-etag") };
            if (self.unknown) {
                headers[1] = .{ .name = try alloc.dupe(u8, "x-antfly-raft-mutation-outcome"), .value = try alloc.dupe(u8, " unknown-v1 ") };
                headers[2] = .{ .name = try alloc.dupe(u8, "Retry-After"), .value = try alloc.dupe(u8, "1") };
            }
            return .{ .status = self.status, .body = try alloc.dupe(u8, "{\"code\":\"upstream\"}"), .headers = headers };
        }
    };
    var fake: Fake = .{};
    var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    var expected_calls: usize = 0;
    for ([_]u16{ 202, 409, 503 }) |status| for ([_]bool{ false, true }) |scan| {
        fake.status = status;
        fake.unknown = status != 202;
        var result = try client.forwardPrivacyMutation("http://metadata", "docs", "{}", if (scan) null else "etag", scan, .{});
        defer result.deinit(a);
        expected_calls += 1;
        try std.testing.expectEqual(expected_calls, fake.calls);
        try std.testing.expectEqual(status, result.response.status);
        try std.testing.expectEqualStrings("{\"code\":\"upstream\"}", result.response.body);
        try std.testing.expectEqualStrings("upstream-etag", result.etag.?);
        try std.testing.expectEqual(if (fake.unknown) @as(?@import("public_table_http.zig").MetadataMutationOutcome, .unknown) else null, result.response.metadata_mutation_outcome);
        try std.testing.expectEqual(if (fake.unknown) @as(?u32, 1) else null, result.response.retry_after_seconds);
    };
}

test "privacy compact binding HTTP reads bound replies and reject malformed or unsupported peers" {
    const a = std.testing.allocator;
    const api = @import("../metadata/api.zig");
    const Fake = struct {
        status: u16 = 200,
        malformed: bool = false,
        calls: usize = 0,
        version: [64]u8 = @splat('a'),
        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(std.mem.endsWith(u8, request.uri, "/internal/v1/catalog/privacy-policy-binding"));
            try std.testing.expectEqual(@as(?usize, 4096), request.max_response_bytes);
            return .{ .status = self.status, .body = if (self.malformed) try alloc.dupe(u8, "{}") else try std.json.Stringify.valueAlloc(alloc, api.PrivacyPolicyBinding{ .metadata_group_id = 9, .metadata_incarnation = @splat('1'), .catalog_revision = 7, .policy_version = self.version }, .{}) };
        }
    };
    var fake: Fake = .{};
    var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    var result = try client.fetchPrivacyPolicyBinding("http://metadata", "docs", null);
    defer result.deinit();
    try std.testing.expectEqual(fake.version, result.value.policy_version);
    fake.version[0] = '!';
    try std.testing.expectError(error.InvalidPrivacyCatalog, client.fetchPrivacyPolicyBinding("http://metadata", "docs", null));
    fake.malformed = true;
    try std.testing.expectError(error.MissingField, client.fetchPrivacyPolicyBinding("http://metadata", "docs", null));
    fake.status = 404;
    try std.testing.expectError(error.UnsupportedOperation, client.fetchPrivacyPolicyBinding("http://metadata", "docs", null));
    const calls = fake.calls;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, client.fetchPrivacyPolicyBinding("http://metadata", "docs", .{ .deadline_ns = 1 }));
    try std.testing.expectEqual(calls, fake.calls);
}

test "privacy metadata reply ownership failures retain outcome and never replay delivery" {
    const a = std.testing.allocator;
    const Fake = struct {
        fail: *std.testing.FailingAllocator,
        fail_after: usize,
        status: u16,
        unknown: bool,
        calls: usize = 0,
        fn execute(raw: *anyopaque, _: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            // The transport owns these allocations independently of the
            // metadata client's caller arena. Arm failures only after reply.
            const parent = std.testing.allocator;
            const headers = try parent.alloc(http.Header, if (self.unknown) 2 else 1);
            headers[0] = .{ .name = try parent.dupe(u8, "ETag"), .value = try parent.dupe(u8, "accepted") };
            if (self.unknown) headers[1] = .{ .name = try parent.dupe(u8, "X-Antfly-Raft-Mutation-Outcome"), .value = try parent.dupe(u8, "unknown-v1") };
            const response = http.HttpResponse{ .owner_allocator = parent, .status = self.status, .headers = headers, .body = try parent.dupe(u8, "{}") };
            self.fail.fail_index = self.fail.alloc_index + self.fail_after;
            return response;
        }
    };
    for ([_]u16{ 202, 409, 503, 412 }) |status| for ([_]bool{ false, true }) |scan| for (0..2) |index| {
        var fail = std.testing.FailingAllocator.init(a, .{});
        var fake = Fake{ .fail = &fail, .fail_after = index, .status = status, .unknown = status == 409 or status == 503 };
        var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(fail.allocator(), .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
        try std.testing.expectError(if (status == 412) error.OutOfMemory else error.MetadataMutationOutcomeUnknown, client.forwardPrivacyMutation("http://metadata", "docs", "{}", null, scan, .{}));
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
    };
}

test "privacy reply transport failures distinguish unsent requests from possible admission" {
    const a = std.testing.allocator;
    const Fake = struct {
        state: http.RequestDeliveryTracker.State,
        failure: anyerror,
        calls: usize = 0,
        fn execute(raw: *anyopaque, _: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const delivery = req.delivery_tracker orelse return error.MissingDeliveryTracker;
            switch (self.state) {
                .not_sent => delivery.markNotSent(),
                .may_have_been_sent => delivery.markMayHaveBeenSent(),
                .unknown => {},
            }
            return self.failure;
        }
    };
    for ([_]http.RequestDeliveryTracker.State{ .not_sent, .may_have_been_sent, .unknown }) |state| for ([_]anyerror{ error.OutOfMemory, error.Canceled, error.InvalidResponse }) |failure| for ([_]bool{ false, true }) |scan| {
        var fake = Fake{ .state = state, .failure = failure };
        var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
        try std.testing.expectError(if (state == .not_sent) failure else error.MetadataMutationOutcomeUnknown, client.forwardPrivacyMutation("http://metadata", "docs", "{}", null, scan, .{}));
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
    };
}

test "privacy compact owner readiness transport bounds replies and preserves selected namespaces" {
    const api = @import("../metadata/api.zig");
    const Fake = struct {
        status: u16 = 200,
        malformed: bool = false,
        legacy: bool = false,
        disposition: @FieldType(api.OwnerReadiness, "status") = .ready,
        calls: usize = 0,
        fn execute(ptr: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(std.mem.endsWith(u8, request.uri, "/internal/v1/catalog/owner-readiness"));
            try std.testing.expectEqual(@as(?usize, 4096), request.max_response_bytes);
            var parsed = try std.json.parseFromSlice(api.OwnerReadinessRequest, a, request.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u64, 7), parsed.value.table_id);
            try std.testing.expectEqual(@as(usize, 2), parsed.value.owners.len);
            try std.testing.expectEqual(@as(u64, 71), parsed.value.owners[0].range_id);
            return .{ .status = self.status, .body = if (self.legacy) try a.dupe(u8, "{\"ready\":true}") else if (self.malformed) try a.dupe(u8, "{}") else try std.json.Stringify.valueAlloc(a, api.OwnerReadiness{ .status = self.disposition, .metadata_group_id = 9, .metadata_incarnation = @splat('1') }, .{}) };
        }
    };
    const a = std.testing.allocator;
    var fake: Fake = .{};
    var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(a, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    const request: api.OwnerReadinessRequest = .{ .table_id = 7, .owners = &.{ .{ .group_id = 1, .shard_id = 7, .range_id = 71 }, .{ .group_id = 2, .shard_id = 7, .range_id = 72 } } };
    var result = try client.fetchOwnerReadiness("http://metadata", request, null);
    defer result.deinit();
    try std.testing.expectEqual(.ready, result.value.status);
    fake.disposition = .topology_changed;
    var changed = try client.fetchOwnerReadiness("http://metadata", request, null);
    defer changed.deinit();
    try std.testing.expectEqual(.topology_changed, changed.value.status);
    fake.disposition = .identity_unavailable;
    var unhealthy = try client.fetchOwnerReadiness("http://metadata", request, null);
    defer unhealthy.deinit();
    try std.testing.expectEqual(.identity_unavailable, unhealthy.value.status);
    fake.malformed = true;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchOwnerReadiness("http://metadata", request, null));
    fake.malformed = false;
    fake.legacy = true;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchOwnerReadiness("http://metadata", request, null));
    fake.status = 404;
    try std.testing.expectError(error.UnsupportedOperation, client.fetchOwnerReadiness("http://metadata", request, null));
    const calls = fake.calls;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, client.fetchOwnerReadiness("http://metadata", request, .{ .deadline_ns = 1 }));
    try std.testing.expectEqual(calls, fake.calls);
}

test "privacy compact owner readiness transport preserves preflight and dispatch cancellation" {
    const api = @import("../metadata/api.zig");
    const Fake = struct {
        calls: usize = 0,
        fn execute(raw: *anyopaque, _: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.Cancelled;
        }
    };
    var fake: Fake = .{};
    var signal: http.RequestCancellation = .{};
    signal.cancel();
    var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(std.testing.allocator, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    const request: api.OwnerReadinessRequest = .{ .table_id = 7, .owners = &.{.{ .group_id = 1, .shard_id = 1, .range_id = 1 }} };
    try std.testing.expectError(error.Canceled, client.fetchOwnerReadiness("http://metadata", request, .{ .deadline_ns = std.math.maxInt(u64), .cancellation = &signal }));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.Canceled, client.fetchOwnerReadiness("http://metadata", request, null));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "privacy compact owner readiness transport routing tokens fail closed on peer errors" {
    const Fake = struct {
        status: u16 = 503,
        fn execute(raw: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(std.mem.endsWith(u8, request.uri, "/routing-token"));
            try std.testing.expectEqual(@as(?usize, 4096), request.max_response_bytes);
            return .{ .status = self.status, .body = try a.dupe(u8, "invalid peer JSON") };
        }
    };
    var fake: Fake = .{};
    var client = @import("../metadata/http_client.zig").MetadataHttpClient.init(std.testing.allocator, .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } });
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchRoutingToken("http://metadata", null));
    fake.status = 200;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchRoutingToken("http://metadata", null));
    fake.status = 404;
    try std.testing.expectError(error.UnsupportedOperation, client.fetchRoutingToken("http://metadata", null));
}
