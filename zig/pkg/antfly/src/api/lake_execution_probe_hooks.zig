// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! No-op production hooks for qualification-only physical work attribution.
const std = @import("std");
pub inline fn start() u64 {
    if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) return @import("antfly_platform").time.monotonicNs();
    return 0;
}
pub inline fn finish(comptime name: []const u8, started: u64) void {
    if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) @import("root").lakeExecutionProbeFamily(name, start() - started);
}

/// Emit at the failing boundary, before cross-owner error transport discards
/// context. Fixed operation names and error tags never expose object URLs,
/// credentials, source values or local paths. Compiles away in production.
pub inline fn failure(comptime operation: []const u8, err: anyerror, cause: ?anyerror) void {
    if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) {
        if (cause) |value| {
            std.debug.print("PARQUET_BULK_FAILURE_OPERATION {{\"operation\":\"{s}\",\"error_name\":\"{s}\",\"cause\":\"{s}\"}}\n", .{ operation, @errorName(err), @errorName(value) });
        } else {
            std.debug.print("PARQUET_BULK_FAILURE_OPERATION {{\"operation\":\"{s}\",\"error_name\":\"{s}\",\"cause\":null}}\n", .{ operation, @errorName(err) });
        }
    }
}
