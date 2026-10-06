// Copyright 2026 Antfly, Inc.
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
const client = @import("client");
const httpx = @import("httpx");
const Response = client.ApiResponse(client.PatchSchemaResponse);

test "generated client keeps identical success types source compatible" {
    try std.testing.expect(!@hasDecl(client, "SameResponse"));
    const Result = @typeInfo(@TypeOf(client.Client.same)).@"fn".return_type.?;
    const TableResponse = @typeInfo(Result).error_union.payload;
    var http = try httpx.Response.init(202, "{\"name\":\"documents\"}");
    var response = try TableResponse.fromResponse(std.testing.allocator, &http);
    defer response.deinit();
    try std.testing.expectEqualStrings("documents", response.data.?.value.name);
}

test "generated client dispatches 200 and both typed 202 outcomes by status" {
    var table_http = try httpx.Response.init(200, "{\"name\":\"documents\"}");
    var table = try Response.fromResponse(std.testing.allocator, &table_http);
    defer table.deinit();
    try std.testing.expect(table_http.body == null);
    try std.testing.expectEqualStrings("documents", table.data.?.value.status_200.name);

    var job_http = try httpx.Response.init(202, "{\"job_id\":\"rewrite-1\"}");
    var job = try Response.fromResponse(std.testing.allocator, &job_http);
    defer job.deinit();
    try std.testing.expectEqualStrings("rewrite-1", job.data.?.value.status_202.restore_job.job_id);

    var commit_http = try httpx.Response.init(202, "{\"commit_id\":\"commit-1\"}");
    var commit = try Response.fromResponse(std.testing.allocator, &commit_http);
    defer commit.deinit();
    try std.testing.expectEqualStrings("commit-1", commit.data.?.value.status_202.committed_mutation_outcome.commit_id);
}

test "generated client rejects wrong-status malformed missing and undeclared success bodies" {
    const Case = struct { status: u16, body: ?[]const u8 };
    for ([_]Case{
        .{ .status = 200, .body = "{\"job_id\":\"rewrite-1\"}" },
        .{ .status = 202, .body = "{\"name\":\"documents\"}" },
        .{ .status = 202, .body = "{" },
        .{ .status = 202, .body = null },
        .{ .status = 201, .body = "{\"name\":\"documents\"}" },
    }) |case| {
        var response = try httpx.Response.init(case.status, case.body);
        try std.testing.expectError(error.InvalidApiResponse, Response.fromResponse(std.testing.allocator, &response));
        try std.testing.expect(response.body == null);
    }
}

test "generated client preserves no-content errors and exact-before-wildcard semantics" {
    var empty_http = try httpx.Response.init(204, null);
    var empty = try Response.fromResponse(std.testing.allocator, &empty_http);
    defer empty.deinit();
    try std.testing.expect(empty.data == null);
    var conflict_http = try httpx.Response.init(409, "conflict");
    var conflict = try Response.fromResponse(std.testing.allocator, &conflict_http);
    defer conflict.deinit();
    try std.testing.expectEqualStrings("conflict", conflict.err_body.?);

    var wildcard_http = try httpx.Response.init(203, "{\"job_id\":\"rewrite-2\"}");
    var wildcard = try client.ApiResponse(client.WildcardResponse).fromResponse(std.testing.allocator, &wildcard_http);
    defer wildcard.deinit();
    try std.testing.expectEqualStrings("rewrite-2", wildcard.data.?.value.status_2XX.job_id);
    var exact_http = try httpx.Response.init(200, "{\"job_id\":\"rewrite-2\"}");
    try std.testing.expectError(error.InvalidApiResponse, client.ApiResponse(client.WildcardResponse).fromResponse(std.testing.allocator, &exact_http));
}

fn allocationCase(alloc: std.mem.Allocator) !void {
    var response = try httpx.Response.init(202, "{\"job_id\":\"rewrite-1\"}");
    var parsed = try Response.fromResponse(alloc, &response);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("rewrite-1", parsed.data.?.value.status_202.restore_job.job_id);
}

test "generated client frees partial arenas and preserves allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}
