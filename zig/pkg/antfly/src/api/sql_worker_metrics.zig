// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Node SQL executor ownership, distinct from runnable CPU admission.
const std = @import("std");
const Runtime = @import("antfly_local_sources").storage_background_runtime.BackendRuntime;

pub fn append(writer: *std.Io.Writer, runtime: *const Runtime) !void {
    const roles = [_]Runtime.SharedSqlRole{ .execution, .control, .cleanup };
    var snapshots: [roles.len]Runtime.SharedSqlStats = undefined;
    for (roles, 0..) |role, i| snapshots[i] = runtime.sharedSqlStats(role);
    inline for (.{
        .{ "capacity", "gauge", "Configured maximum SQL executor slots for this role" },
        .{ "retained_capacity", "gauge", "SQL executor slots retained through physical retirement" },
        .{ "borrowers", "gauge", "Consumers retaining the shared SQL executor" },
        .{ "peak_borrowers", "gauge", "Peak consumers retaining the shared SQL executor" },
        .{ "retiring", "gauge", "Whether the SQL executor is physically retiring" },
        .{ "acquisitions_total", "counter", "Successful shared SQL executor lease acquisitions" },
        .{ "rejections_total", "counter", "Rejected shared SQL executor lease acquisitions" },
    }) |metric| {
        const name = "antfly_executor_sql_" ++ metric[0];
        try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n", .{ name, metric[2], name, metric[1] });
        for (roles, snapshots) |role, stats| {
            const value = if (comptime std.mem.eql(u8, metric[0], "retiring")) @intFromBool(stats.retiring) else @field(stats, metric[0]);
            try writer.print("{s}{{role=\"{s}\"}} {d}\n", .{ name, @tagName(role), value });
        }
    }
}
