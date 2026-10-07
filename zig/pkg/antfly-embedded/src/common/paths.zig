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

//! Local workspace defaults shared by Lite and server configuration.
const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
pub fn defaultLocalBaseDir(alloc: std.mem.Allocator) ![]u8 {
    const home_var = if (builtin.os.tag == .windows) "USERPROFILE" else "HOME";
    const home = platform.env.getenv(home_var) orelse return try alloc.dupe(u8, "antflydb");
    if (home.len == 0) return try alloc.dupe(u8, "antflydb");
    return try std.fs.path.join(alloc, &.{ home, ".antfly" });
}
