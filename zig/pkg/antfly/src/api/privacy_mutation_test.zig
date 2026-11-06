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
const A = std.mem.Allocator;
const local = @import("antfly_local_sources");
const privacy = @import("privacy.zig");
const metadata = @import("../metadata/table_manager.zig");
const Api = @import("http_server.zig");
const Server = struct {
    const Source = struct {
        a: A,
        committed: bool = false,
        installed: ?metadata.TableRecord = null,
        loads: usize = 0,
        rejection: ?anyerror = null,
        forwarded: ?*Forwarded = null,
        pub fn forwardPrivacyMutation(self: *@This(), out: A, _: []const u8, _: []const u8, _: ?[]const u8, scan: bool, _: local.api_operation.RequestContext) !?privacy.Response {
            const reply = self.forwarded orelse return null;
            reply.calls += 1;
            const raw = if (scan) "{\"phase\":\"queued\"}" else "{\"external_processing\":true,\"resolved_producer\":{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\",\"model\":\"x\"},\"revision\":\"v1\"}}";
            var response = privacy.Response{ .response = .{ .status = 202, .json = true, .body = try self.a.dupe(u8, raw) } };
            defer response.deinit(self.a);
            response.etag = try self.a.dupe(u8, "accepted");
            self.committed = true;
            reply.fail.fail_index = reply.fail.alloc_index + reply.fail_after;
            return try response.cloneMutation(out);
        }
        pub fn privacyAuthorityTerm(_: *@This()) !?u64 {
            return 1;
        }
        pub fn replaceTableDefinition(self: *@This(), _: metadata.TableRecord, replacement: metadata.TableRecord) !void {
            if (self.rejection) |err| return err;
            self.installed = try metadata.cloneTable(self.a, replacement);
            self.committed = true;
        }
    };
    const Writes = struct {
        failure: ?anyerror = null,
        calls: usize = 0,
        pub fn requestTableStructuralReconcile(self: *@This(), _: A, _: []const u8) !?void {
            self.calls += 1;
            if (self.failure) |err| return err;
            return {};
        }
    };
    source: Source,
    table_reads: ?local.api_table_read_source.TableReadSource = null,
    table_writes: ?*Writes = null,
    antfly_provider: @FieldType(Api.ApiHttpServer, "antfly_provider") = null,
    embedding_provider_runtime: @FieldType(Api.ApiHttpServer, "embedding_provider_runtime") = undefined,
    cfg: Api.ApiHttpServerConfig = .{},
    pub fn inferenceIo(_: *@This()) std.Io {
        return std.testing.io;
    }
    pub fn configuredInferenceAPIURL(_: *@This()) ?[]const u8 {
        return null;
    }
    pub fn ensurePrivacyAdmission(_: *@This(), _: local.api_operation.RequestContext) !void {}
    pub fn loadOwnedTableRecord(self: *@This(), a: A, _: []const u8) !?metadata.TableRecord {
        self.source.loads += 1;
        if (self.source.committed) return error.StorageBusy;
        return try metadata.cloneTable(a, .{ .table_id = 1, .name = "docs", .schema_json = "{}", .indexes_json = "{}" });
    }
};

const body = "{\"mode\":\"monitor\",\"fields\":[\"body\"]}";

test "privacy committed policy acceptance survives unavailable rereads and reconcile hints" {
    const a = std.testing.allocator;
    for ([_]?anyerror{ null, error.StorageBusy, error.DeadlineExceeded, error.OutOfMemory }) |failure| {
        var writes = Server.Writes{ .failure = failure };
        var server = Server{ .source = .{ .a = a }, .table_writes = &writes };
        defer if (server.source.installed) |record| metadata.freeTable(a, record);
        var response = try privacy.handle(&server, a, "docs", .put, .{ .body = body }, .{});
        defer response.deinit(a);
        try std.testing.expect(server.source.committed);
        try std.testing.expectEqual(@as(usize, 1), server.source.loads);
        try std.testing.expectEqual(@as(usize, 1), writes.calls);
        try std.testing.expectEqual(@as(u16, 202), response.response.status);
        try std.testing.expect(response.etag != null);
        try std.testing.expect(response.response.retry_after_seconds == null);
        try std.testing.expect(response.response.metadata_mutation_outcome == null);
        const saved = try std.json.parseFromSlice(std.json.Value, a, response.response.body, .{});
        defer saved.deinit();
        try std.testing.expect(saved.value.object.get("configured").?.bool);
        try std.testing.expect(saved.value.object.get("scan_id").?.string.len == 64);
    }
}

test "privacy acceptance response allocation failures occur before the policy commits" {
    const Check = struct {
        fn run(out: A) !void {
            const a = std.testing.allocator;
            var server = Server{ .source = .{ .a = a } };
            defer if (server.source.installed) |record| metadata.freeTable(a, record);
            var response = privacy.handle(&server, out, "docs", .put, .{ .body = body }, .{}) catch |err| {
                try std.testing.expect(!server.source.committed);
                return err;
            };
            defer response.deinit(out);
            try std.testing.expect(server.source.committed);
            try std.testing.expectEqual(@as(u16, 202), response.response.status);
            try std.testing.expectEqual(@as(usize, 1), server.source.loads);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "privacy rejected policy admission releases its prepared acceptance" {
    const a = std.testing.allocator;
    var server = Server{ .source = .{ .a = a, .rejection = error.NotLeader } };
    var response = try privacy.handle(&server, a, "docs", .put, .{ .body = body }, .{});
    defer response.deinit(a);
    try std.testing.expect(!server.source.committed);
    try std.testing.expectEqual(@as(u16, 503), response.response.status);
    try std.testing.expectEqual(@as(?u32, 1), response.response.retry_after_seconds);
    try std.testing.expect(response.etag == null);
}

const Forwarded = struct { fail: *std.testing.FailingAllocator, fail_after: usize, calls: usize = 0 };

test "privacy forwarded policy and scan acceptance survive every reply observation allocation failure" {
    const a = std.testing.allocator;
    for ([_]privacy.Action{ .put, .start_scan }) |action| {
        var failures: usize = 0;
        for (0..256) |index| {
            var fail = std.testing.FailingAllocator.init(a, .{});
            var vtable = fail.allocator().vtable.*;
            // One failed allocation must not prevent the fallback error reply.
            // This exercises the outcome boundary, not an exhausted process.
            vtable.alloc = struct {
                fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
                    const f: *std.testing.FailingAllocator = @ptrCast(@alignCast(ctx));
                    const result = f.allocator().rawAlloc(len, alignment, ra);
                    if (result == null) f.fail_index = std.math.maxInt(usize);
                    return result;
                }
            }.alloc;
            const out = A{ .ptr = &fail, .vtable = &vtable };
            var forwarded = Forwarded{ .fail = &fail, .fail_after = index };
            var server = Server{ .source = .{ .a = a, .forwarded = &forwarded } };
            var response = try privacy.handle(&server, out, "docs", action, .{ .body = body }, .{});
            defer response.deinit(out);
            try std.testing.expect(server.source.committed);
            try std.testing.expectEqual(@as(usize, 1), forwarded.calls);
            try std.testing.expectEqual(@as(usize, 0), server.source.loads);
            if (fail.has_induced_failure) {
                failures += 1;
                try std.testing.expectEqual(@as(u16, 409), response.response.status);
                try std.testing.expect(response.response.metadata_mutation_outcome == .unknown);
                try std.testing.expect(response.etag == null);
                var value = try std.json.parseFromSlice(struct { code: []const u8 }, a, response.response.body, .{ .ignore_unknown_fields = true });
                defer value.deinit();
                try std.testing.expectEqualStrings("privacy_mutation_outcome_unknown", value.value.code);
            } else {
                try std.testing.expectEqual(@as(u16, 202), response.response.status);
                try std.testing.expectEqualStrings("accepted", response.etag.?);
                if (action == .put) {
                    var value = try std.json.parseFromSlice(struct { external_processing: bool }, a, response.response.body, .{ .ignore_unknown_fields = true });
                    defer value.deinit();
                    try std.testing.expect(!value.value.external_processing);
                }
                break;
            }
        } else return error.MissingSuccessfulReply;
        if (action == .put) try std.testing.expect(failures > 2) else try std.testing.expectEqual(@as(usize, 2), failures);
    }
}
