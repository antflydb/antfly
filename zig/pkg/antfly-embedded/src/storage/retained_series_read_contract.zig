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
};
pub const Request = union(enum) {
    status,
    revocation: @import("retained_series_control.zig").Fence,
    poll: struct {
        fence: @import("retained_series_control.zig").Fence,
        root_incarnation: u128,
        cursor: @import("retained_series.zig").Cursor,
        limit: u16 = 8,
    },
};
