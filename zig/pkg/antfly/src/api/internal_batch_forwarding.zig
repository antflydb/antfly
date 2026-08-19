// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
// https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations.

const std = @import("std");

/// Version 1 adds the internal `_timestamp_ns` field to data-Raft batch log
/// entries. A leader activates it only after every applying replica reports
/// at least this version through the internal capability endpoint.
pub const raft_batch_protocol_version: u16 = 1;
pub const raft_batch_timestamp_protocol_version: u16 = 1;
const http_common = @import("../raft/transport/http_common.zig");

pub const remaining_ms_header = "X-Antfly-Raft-Batch-Remaining-Ms";
pub const forwards_remaining_header = "X-Antfly-Raft-Batch-Forwards-Remaining";
pub const campaign_allowed_header = "X-Antfly-Raft-Batch-Campaign-Allowed";
pub const outcome_header = "X-Antfly-Raft-Batch-Outcome";
pub const outcome_not_proposed_v1 = "not-proposed-v1";
pub const outcome_unknown_v1 = "unknown-v1";
pub const max_forwards: u8 = 2;
pub const max_remaining_ms: u32 = 5_000;

/// Routing state carried only between trusted internal group-write endpoints.
/// The remaining budget is relative because monotonic clocks are process-local.
/// Each sender reserves response time before publishing the next budget.
pub const Context = struct {
    remaining_ms: u32,
    forwards_remaining: u8,
    campaign_allowed: bool,
};

pub fn parse(req: http_common.HttpRequest) !?Context {
    return parseValues(
        req.header(remaining_ms_header),
        req.header(forwards_remaining_header),
        req.header(campaign_allowed_header),
    );
}

pub fn parseValues(
    remaining_raw: ?[]const u8,
    forwards_raw: ?[]const u8,
    campaign_raw: ?[]const u8,
) !?Context {
    if (remaining_raw == null and forwards_raw == null and campaign_raw == null) return null;
    if (remaining_raw == null or forwards_raw == null or campaign_raw == null) {
        return error.InvalidRaftBatchForwardingHeaders;
    }

    const remaining_ms = std.fmt.parseUnsigned(u32, remaining_raw.?, 10) catch
        return error.InvalidRaftBatchForwardingHeaders;
    const forwards_remaining = std.fmt.parseUnsigned(u8, forwards_raw.?, 10) catch
        return error.InvalidRaftBatchForwardingHeaders;
    const campaign_allowed = if (std.mem.eql(u8, campaign_raw.?, "true"))
        true
    else if (std.mem.eql(u8, campaign_raw.?, "false"))
        false
    else
        return error.InvalidRaftBatchForwardingHeaders;
    if (remaining_ms == 0 or remaining_ms > max_remaining_ms or forwards_remaining > max_forwards) {
        return error.InvalidRaftBatchForwardingHeaders;
    }

    return .{
        .remaining_ms = remaining_ms,
        .forwards_remaining = forwards_remaining,
        .campaign_allowed = campaign_allowed,
    };
}

test "internal batch forwarding headers are all-or-none and strictly parsed" {
    const valid_headers = [_]http_common.RequestHeader{
        .{ .name = remaining_ms_header, .value = "425" },
        .{ .name = forwards_remaining_header, .value = "1" },
        .{ .name = campaign_allowed_header, .value = "false" },
    };
    const context = (try parse(.{ .method = .POST, .uri = "/", .headers = &valid_headers })).?;
    try std.testing.expectEqual(@as(u32, 425), context.remaining_ms);
    try std.testing.expectEqual(@as(u8, 1), context.forwards_remaining);
    try std.testing.expect(!context.campaign_allowed);

    try std.testing.expect((try parse(.{ .method = .POST, .uri = "/" })) == null);
    try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
        .method = .POST,
        .uri = "/",
        .headers = valid_headers[0..2],
    }));
    const zero_budget = [_]http_common.RequestHeader{
        .{ .name = remaining_ms_header, .value = "0" },
        .{ .name = forwards_remaining_header, .value = "1" },
        .{ .name = campaign_allowed_header, .value = "true" },
    };
    try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
        .method = .POST,
        .uri = "/",
        .headers = &zero_budget,
    }));
    const excessive_budget = [_]http_common.RequestHeader{
        .{ .name = remaining_ms_header, .value = "5001" },
        .{ .name = forwards_remaining_header, .value = "1" },
        .{ .name = campaign_allowed_header, .value = "true" },
    };
    try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
        .method = .POST,
        .uri = "/",
        .headers = &excessive_budget,
    }));
    const excessive_hops = [_]http_common.RequestHeader{
        .{ .name = remaining_ms_header, .value = "425" },
        .{ .name = forwards_remaining_header, .value = "3" },
        .{ .name = campaign_allowed_header, .value = "true" },
    };
    try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
        .method = .POST,
        .uri = "/",
        .headers = &excessive_hops,
    }));
    const invalid_campaign = [_]http_common.RequestHeader{
        .{ .name = remaining_ms_header, .value = "425" },
        .{ .name = forwards_remaining_header, .value = "1" },
        .{ .name = campaign_allowed_header, .value = "1" },
    };
    try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
        .method = .POST,
        .uri = "/",
        .headers = &invalid_campaign,
    }));
}
