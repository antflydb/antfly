// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
const std = @import("std");

/// Private authoritative owner read. Public callers only supply the name;
/// metadata pins the epoch and the complete current range set.
pub const max_response_bytes = 8 * 1024 * 1024;
pub const max_status_cells = 16_384;
/// A null name requests the complete bounded catalog in one owner snapshot.
pub const Request = struct { name: ?[]const u8 = null, schema_version: u32 };
pub const NamedStatus = struct { name: []const u8, status: Status };
pub const Batch = struct { statuses: []const NamedStatus };
pub const Status = struct {
    table_id: u64,
    schema_version: u32,
    generation: u64,
    slot: u32,
    catalog: [32]u8,
    comparison: [32]u8,
    owner: [32]u8,
    range_start: []const u8,
    range_end: []const u8,
    state: enum { building, ready, failed },
    rows_scanned: u64,
    failure: enum { none, incompatible_schema, invalid_row },
    progress_digest: [32]u8,
    maintenance_epoch: u64,
    last_maintenance_request: [32]u8,

    pub fn jsonStringify(self: Status, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Status).@"struct".fields) |field| {
            try stream.objectField(field.name);
            if (comptime std.mem.eql(u8, field.name, "range_start") or std.mem.eql(u8, field.name, "range_end"))
                try @import("relational_integrity_json.zig").write(@field(self, field.name), stream)
            else
                try stream.write(@field(self, field.name));
        }
        try stream.endObject();
    }
};
