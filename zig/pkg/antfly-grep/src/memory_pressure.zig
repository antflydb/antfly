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

//! Explicit idle-session memory relief. This is a best-effort OS allocator
//! hint, never a search-path operation or an RSS guarantee. Source arenas and
//! parser allocation behavior remain unchanged. Other targets safely do nothing.
const builtin = @import("builtin");
pub fn trim() void {
    if (comptime builtin.link_libc and builtin.os.tag == .macos) {
        _ = Darwin.malloc_zone_pressure_relief(null, 0);
    } else if (comptime builtin.link_libc and builtin.os.tag == .linux and builtin.abi.isGnu()) {
        _ = Glibc.malloc_trim(0);
    }
}
const Darwin = struct {
    extern "c" fn malloc_zone_pressure_relief(zone: ?*anyopaque, goal: usize) usize;
};
const Glibc = struct {
    extern "c" fn malloc_trim(pad: usize) c_int;
};
