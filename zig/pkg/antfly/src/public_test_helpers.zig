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

/// Sign the same HS256 trusted-principal wire token used by production HTTP
/// authentication. Fixtures supply their own claims and remain explicit about
/// the permissions granted to each request.
pub fn encodeTrustedPrincipalToken(alloc: std.mem.Allocator, secret: []const u8, payload: []const u8) ![]u8 {
    const header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    const header_encoded = try alloc.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(header.len));
    defer alloc.free(header_encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(header_encoded, header);
    const payload_encoded = try alloc.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(payload.len));
    defer alloc.free(payload_encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(payload_encoded, payload);
    const signing_input = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ header_encoded, payload_encoded });
    defer alloc.free(signing_input);
    var mac: [std.crypto.auth.hmac.sha2.HmacSha256.mac_length]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(mac[0..], signing_input, secret);
    const signature_encoded = try alloc.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(mac.len));
    defer alloc.free(signature_encoded);
    _ = std.base64.url_safe_no_pad.Encoder.encode(signature_encoded, mac[0..]);
    return std.fmt.allocPrint(alloc, "{s}.{s}", .{ signing_input, signature_encoded });
}

pub fn expectSingleOpenapiTopHit(parsed: metadata_openapi.QueryResponses, doc_id: []const u8) !void {
    const responses = parsed.responses orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), responses.len);
    const hits = responses[0].hits orelse return error.TestUnexpectedResult;
    const total = hits.total orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 1), total.value);
    try std.testing.expectEqualStrings("exact", total.relation);
    const hit_items = hits.hits orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), hit_items.len);
    try std.testing.expectEqualStrings(doc_id, hit_items[0]._id);
}

pub fn expectSingleServerlessHit(result: serverless.QuerySearchResult, doc_id: []const u8) !void {
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings(doc_id, result.hits[0].doc_id);
}
