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

const Context = @import("../numeric_value.zig").Context;
pub const Work = struct {
    context: ?*Context = null,
    admission: ?@import("../cooperative_json_output.zig").Control = null,
    remaining: usize = 1024,
    failure: ?anyerror = null,
    pub fn advance(self: *Work, amount: usize) void {
        if (self.failure != null) return;
        if (amount < self.remaining) {
            self.remaining -= amount;
            return;
        }
        self.remaining = 1024;
        self.checkpoint() catch |err| {
            self.failure = err;
        };
    }
    fn checkpoint(self: *Work) !void {
        if (self.context) |context| return context.checkpointNow();
        if (self.admission) |original| return original.check(original.ptr);
    }
};
pub fn failed(work: ?*Work) bool {
    return if (work) |value| value.failure != null else false;
}
