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

//! Read-path defaults with independent qualification overrides. No format change.
const std = @import("std");
pub fn enabled(name: [*:0]const u8) bool {
    return enabledDefault(name, false);
}

pub fn enabledDefault(name: [*:0]const u8, default: bool) bool {
    if (!@import("builtin").link_libc) return default;
    const value = std.c.getenv(name) orelse return default;
    return std.mem.eql(u8, std.mem.span(value), "1");
}
