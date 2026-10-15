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

pub const ExecutionControl = struct {
    cancellation: ?@import("antfly_cancellation").CancellationToken = null,
    deadline_ns: ?u64 = null,

    pub fn check(self: @This()) !void {
        if (self.cancellation) |token| try token.check();
        if (self.deadline_ns) |deadline| {
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        }
    }
};
