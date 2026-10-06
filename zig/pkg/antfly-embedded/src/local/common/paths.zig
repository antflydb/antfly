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
