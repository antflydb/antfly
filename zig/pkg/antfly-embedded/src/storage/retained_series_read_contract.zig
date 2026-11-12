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

//! Private native retained-read admission; no physical storage dependencies.
pub const ArchiveStart = struct {
    sequence: u64,
    complete_from_ns: i128,
    closed_at_ns: i128,
    first_key: @import("../retained_dataset_types.zig").RowKey,
};

pub const Query = struct {
    fence: @import("retained_series_control.zig").Fence,
    start_ns: i128,
    end_ns: i128,
    admitted_ns: i128,
    archive: ?ArchiveStart = null,
    dimensions_json: ?[]const u8 = null,
    after: ?@import("../retained_dataset_types.zig").RowKey = null,
};
pub const Request = union(enum) {
    status,
    revocation: @import("retained_series_control.zig").Fence,
    poll: struct {
        fence: @import("retained_series_control.zig").Fence,
        root_incarnation: u128,
        cursor: @import("retained_series.zig").Cursor,
        limit: u16 = 8,
        include_latest: bool = false,
    },
};

/// Borrowed for one synchronous call only; never retained by a read session.
/// Boolean callbacks keep cancellation portable across compiled owners.
pub const Control = struct {
    deadline_ns: u64 = @import("std").math.maxInt(u64),
    cancellation_ctx: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (*const anyopaque) bool = null,

    pub fn check(self: @This()) !void {
        if (self.is_cancelled_fn) |probe| if (probe(self.cancellation_ctx orelse return error.InvalidRetainedReadQuery)) return error.Canceled;
        if (self.deadline_ns != @import("std").math.maxInt(u64) and @import("antfly_platform").time.monotonicNs() >= self.deadline_ns) return error.Timeout;
    }
};
