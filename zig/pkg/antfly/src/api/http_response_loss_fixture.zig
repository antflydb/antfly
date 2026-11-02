// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Test-only post-handler TCP response loss. The production handler executes;
//! the socket closes before httpx can write its returned response. Keep this
//! object alive until the listener joins, and restore replies before teardown.
const std = @import("std");
const httpx = @import("httpx");

pub const ResponseLoss = struct {
    pub const Phase = enum { all_replies, capture, open, next };
    phase: Phase = .all_replies,
    struck: std.atomic.Value(bool) = .init(false),
    phase_requests: std.atomic.Value(usize) = .init(0),
    phase_hits: std.atomic.Value(usize) = .init(0),
    token_replies: std.atomic.Value(usize) = .init(0),
    advanced_rows: std.atomic.Value(usize) = .init(0),
    blocked_retirement: std.atomic.Value(usize) = .init(0),
    enabled: std.atomic.Value(bool) = .init(false),
    dropped: std.atomic.Value(usize) = .init(0),
    successful_drops: std.atomic.Value(usize) = .init(0),
    missing_h1: std.atomic.Value(usize) = .init(0),
    capability_replies: std.atomic.Value(usize) = .init(0),

    pub fn middleware(self: *ResponseLoss) httpx.Middleware {
        return httpx.Middleware.bind("post-handler TCP response loss", self, afterHandler);
    }
    fn afterHandler(self: *ResponseLoss, ctx: *httpx.Context, next: *httpx.Next) !httpx.Response {
        const routes = @import("../internal/routes.zig");
        const rpc = @import("retained_read_rpc.zig");
        const eligible = self.enabled.load(.acquire) and ctx.request.method == .POST and std.mem.startsWith(u8, ctx.request.uri.path, "/internal/v1/");
        const operation: ?@FieldType(rpc.Request, "operation") = if (eligible and self.phase != .all_replies and routes.isRetainedRead(ctx.request.uri.path)) operation: {
            const body = (try ctx.body()) orelse return error.TestMissingRetainedBody;
            var envelope = try @import("owned_retained_read.zig").decode(ctx.allocator, body);
            defer envelope.deinit();
            var parsed = try std.json.parseFromSlice(rpc.Request, ctx.allocator, envelope.value.request, .{ .parse_numbers = false });
            defer parsed.deinit();
            break :operation parsed.value.operation;
        } else null;
        const selected = if (operation) |op| switch (self.phase) {
            .all_replies => false,
            .capture => op == .capture,
            .open => op == .open or op == .open_snapshot,
            .next => op == .next,
        } else false;
        if (selected) _ = self.phase_requests.fetchAdd(1, .acq_rel);
        // Only after the selected native operation has returned successfully,
        // withhold cleanup delivery as well as its response. Earlier cleanup
        // must run normally so capture release and credit recycling progress.
        // Capability discovery remains independently available throughout.
        if (eligible and self.phase != .all_replies and self.struck.load(.acquire) and
            ((routes.isRemoteAttemptControl(ctx.request.uri.path) and !std.mem.eql(u8, ctx.request.uri.path, routes.remote_attempt_capabilities)) or operation == .close or operation == .cancel))
        {
            _ = self.blocked_retirement.fetchAdd(1, .acq_rel);
            try self.closeSocket(ctx);
            return httpx.Response.init(ctx.allocator, 503);
        }
        var response = try next.call(ctx);
        errdefer response.deinit();
        if (!self.enabled.load(.acquire) or ctx.request.method != .POST or
            !std.mem.startsWith(u8, ctx.request.uri.path, "/internal/v1/")) return response;
        // This fixture loses operation/retirement replies, not the independent
        // signed capability service. Dropping discovery instead can reject the
        // next operation before it publishes any resource at all.
        if (std.mem.eql(u8, ctx.request.uri.path, @import("../internal/routes.zig").remote_attempt_capabilities)) {
            if (response.status.code == 200) _ = self.capability_replies.fetchAdd(1, .acq_rel);
            return response;
        }
        if (self.phase != .all_replies) {
            if (!selected or response.status.code != 200) return response;
            var reply = try std.json.parseFromSlice(rpc.Response, ctx.allocator, response.body orelse return error.TestMissingNativeReply, .{ .parse_numbers = false });
            defer reply.deinit();
            if (self.phase == .next) {
                if (reply.value.rows.len == 0 or reply.value.sequence != 0) return error.TestMissingAdvancedNativePage;
                _ = self.advanced_rows.fetchAdd(reply.value.rows.len, .acq_rel);
            } else {
                if (reply.value.token == null or reply.value.busy) return error.TestMissingNativeToken;
                _ = self.token_replies.fetchAdd(1, .acq_rel);
            }
            _ = self.phase_hits.fetchAdd(1, .acq_rel);
            self.struck.store(true, .release);
        }
        _ = self.dropped.fetchAdd(1, .acq_rel);
        if (response.status.code == 200) _ = self.successful_drops.fetchAdd(1, .acq_rel);
        try self.closeSocket(ctx);
        // Normal transport teardown still owns and releases the real response.
        return response;
    }
    fn closeSocket(self: *ResponseLoss, ctx: *httpx.Context) !void {
        const socket = ctx.h1_sock orelse {
            _ = self.missing_h1.fetchAdd(1, .acq_rel);
            return error.ExpectedHttp1FaultTransport;
        };
        socket.shutdown();
    }
};
