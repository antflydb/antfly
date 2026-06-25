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
const http_server = @import("http_server.zig");
const pgwire = @import("pgwire.zig");

pub const OptionalListenerConfig = struct {
    bind_host: ?[]const u8 = null,
    default_bind_host: []const u8,
    bind_port: ?u16 = null,
    auth_enabled: bool = false,
    auth_error_message: []const u8 = "pgwire listener does not support auth yet; disable auth or omit --pgwire-port",
    api_server: ?*http_server.ApiHttpServer = null,
};

fn validateOptionalListenerConfig(cfg: OptionalListenerConfig) !void {
    if (cfg.bind_port == null and cfg.bind_host != null) return error.InvalidArguments;
}

pub fn startOptional(alloc: std.mem.Allocator, cfg: OptionalListenerConfig) !?pgwire.Server {
    validateOptionalListenerConfig(cfg) catch |err| {
        if (err == error.InvalidArguments and cfg.bind_port == null and cfg.bind_host != null) {
            std.log.err("--pgwire-host requires --pgwire-port", .{});
        }
        return err;
    };
    const bind_port = cfg.bind_port orelse {
        return null;
    };
    if (cfg.auth_enabled) {
        std.log.err("{s}", .{cfg.auth_error_message});
        return error.InvalidArguments;
    }
    const api_server = cfg.api_server orelse {
        std.log.err("pgwire listener requires a public API server; omit --pgwire-port", .{});
        return error.InvalidArguments;
    };
    return try pgwire.start(alloc, .{
        .bind_host = cfg.bind_host orelse cfg.default_bind_host,
        .bind_port = bind_port,
        .api_server = api_server,
    });
}

test "optional pgwire listener rejects host without port" {
    try std.testing.expectError(error.InvalidArguments, validateOptionalListenerConfig(.{
        .bind_host = "127.0.0.1",
        .default_bind_host = "127.0.0.1",
    }));
}
