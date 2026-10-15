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

//! Native method outcomes shared by physical and contract-only consumers.
pub const Failure = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,
    pub fn init(name: []const u8) Failure {
        return .{ .ptr = name.ptr, .len = name.len };
    }
};
pub fn Outcome(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
