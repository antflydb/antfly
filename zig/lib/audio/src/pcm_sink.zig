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

//! Synchronous borrowed PCM blocks. Consumers copy only retained windows.
const std = @import("std");
pub const Sink = struct {
    context: *anyopaque,
    write: *const fn (*anyopaque, []const f32, u32, u8) anyerror!void,
    pub fn push(self: Sink, samples: []const f32, rate: u32, channels: u8) !void {
        try @import("work_control.zig").check();
        if (rate == 0 or channels == 0 or samples.len % channels != 0) return error.UnsupportedAudioFormat;
        try self.write(self.context, samples, rate, channels);
    }
};
