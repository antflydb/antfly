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

const web_profile = @import("profile.zig");

pub const HostLen = web_profile.HostSize;

pub inline fn toLen(value: HostLen) usize {
    return web_profile.hostToLen(value);
}

pub inline fn fromLen(value: usize) HostLen {
    return web_profile.lenToHost(value);
}

pub inline fn sliceConst(comptime T: type, ptr: [*]const T, len: HostLen) []const T {
    return ptr[0..toLen(len)];
}

pub inline fn sliceMut(comptime T: type, ptr: [*]T, len: HostLen) []T {
    return ptr[0..toLen(len)];
}
