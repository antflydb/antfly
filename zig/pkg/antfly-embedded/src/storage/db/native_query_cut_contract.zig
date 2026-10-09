// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub const ttl_ms: u64 = 60_000;
pub const Request = struct {
    id: []const u8,
    table_id: u64,
    expires_ms: u64,
    create: bool = false,
    timeout_ms: ?u64 = null,
    pub fn forDeadline(self: Request, deadline_ns: ?u64) Request {
        var request = self;
        if (deadline_ns) |deadline| request.timeout_ms = (deadline -| @import("antfly_platform").time.monotonicNs()) / std.time.ns_per_ms;
        return request;
    }
    pub fn validate(self: Request, now: u64) !void {
        if (self.id.len != 64 or self.table_id == 0) return error.InvalidQueryRequest;
        for (self.id) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidQueryRequest;
        if (self.expires_ms <= now or self.expires_ms > now +| ttl_ms) return error.CatalogGenerationChanged;
    }
};
