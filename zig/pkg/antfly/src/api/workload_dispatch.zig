// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Allocation-free framing facts shared by direct and compiled ingress adapters.
const httpx = @import("httpx");
const internal_service_auth = @import("internal_service_auth.zig");

pub const Lane = enum(u8) { general = 0, control = 1, recovery = 2 };

pub const Request = struct {
    method: []const u8,
    path: []const u8,
    content_length: ?[]const u8 = null,
    transfer_encoding: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    credential: ?[]const u8 = null,
    body_received_bytes: u64 = 0,
    body_complete: bool = false,

    pub fn fromTransport(view: httpx.RequestDispatchView) Request {
        return .{
            .method = view.method,
            .path = view.path(),
            .content_length = view.header("content-length"),
            .transfer_encoding = view.header("transfer-encoding"),
            .content_encoding = view.header("content-encoding"),
            .credential = view.header(internal_service_auth.header_name),
            .body_received_bytes = view.body_received_bytes,
            .body_complete = view.body_complete,
        };
    }
};

const busy_body = "{\"error\":\"AdmissionFull\",\"reason\":\"instance_busy\",\"stage\":\"admission\",\"execution_started\":false}";
pub const busy_response = "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: " ++
    @import("std").fmt.comptimePrint("{d}", .{busy_body.len}) ++
    "\r\nRetry-After: 1\r\nConnection: close\r\n\r\n" ++ busy_body;
