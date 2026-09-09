// Copyright 2026 Antfly, Inc.
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

//! Borrowed admission/feedback hook for each transport attempt, including
//! retries and streamed responses. Successful admission has exactly one finish.
//! The observer and its context must outlive the whole request (all retries).
const std = @import("std");
const Response = @import("response.zig").Response;

pub const AttemptObserver = struct {
    ptr: *anyopaque,
    before: *const fn (*anyopaque, Context) anyerror!void,
    after: *const fn (*anyopaque, ?*const Response) void,
    output_tokens: u64 = 0,

    pub const Context = struct {
        io: std.Io,
        deadline_ms: ?i64,
        cancellation_ptr: *const anyopaque,
        is_cancelled: *const fn (*const anyopaque) bool,
        body_bytes: usize,
        output_tokens: u64,
    };
};
