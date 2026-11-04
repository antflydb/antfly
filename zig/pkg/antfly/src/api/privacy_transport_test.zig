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
        fn capture(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: ?u64) ![64]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
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
}

test "privacy routed scans accept the public range response envelope and own cursors" {
    const a = std.testing.allocator;
    var result = try @import("table_writes.zig").parseStorageKernelDocumentArtifactTableReprocessResult(a, "{\"reprocess\":\"triggered\",\"reprocess_status\":\"in_progress\",\"artifact_name\":\"_privacy_document\",\"scanned\":0,\"reprocessed\":0,\"skipped\":0,\"failed\":0,\"limit\":100,\"next_key\":\"\",\"pending_shards\":1,\"failures\":[],\"shard_cursors\":[]}");
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
    fake.ambiguous = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, client.forwardPrivacyMutation("http://metadata", "docs", "{}", null, true, request));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}

test "privacy checkpoint job commands require the same coordinated decoder as catalog attachment" {
    const service = @import("../metadata/service.zig");
    const protocol = @import("../metadata/topology_protocol.zig");
    try std.testing.expectEqual(protocol.privacy_version, service.transitionRequiredCoordinatedDecoderVersion(.{ .compare_and_set_job_state = .{ .key = "privacy/m/checkpoint", .expected_hash = "", .value = "{}" } }));
    try std.testing.expectEqual(protocol.privacy_version, service.transitionRequiredCoordinatedDecoderVersion(.{ .compare_and_set_job_state = .{ .key = "privacy/c/checkpoint", .expected_hash = "", .value = "Vchunk" } }));
    try std.testing.expectEqual(protocol.job_state_version, service.transitionRequiredCoordinatedDecoderVersion(.{ .compare_and_set_job_state = .{ .key = "documents/run", .expected_hash = "", .value = "{}" } }));
}
