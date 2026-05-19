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

const std = @import("std");
const metadata_openapi = @import("antfly_metadata_openapi");
const serverless = @import("serverless/mod.zig");

pub fn expectSingleOpenapiTopHit(parsed: metadata_openapi.QueryResponses, doc_id: []const u8) !void {
    const responses = parsed.responses orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), responses.len);
    const hits = responses[0].hits orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 1), hits.total);
    const hit_items = hits.hits orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), hit_items.len);
    try std.testing.expectEqualStrings(doc_id, hit_items[0]._id);
}

pub fn expectSingleServerlessHit(result: serverless.QuerySearchResult, doc_id: []const u8) !void {
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings(doc_id, result.hits[0].doc_id);
}
