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

const types = @import("types.zig");
const message = @import("message.zig");

pub const Ready = struct {
    soft_state: ?types.SoftState = null,
    hard_state: ?types.HardState = null,
    snapshot: ?types.Snapshot = null,
    entries: []const types.Entry = &.{},
    committed_entries: []const types.Entry = &.{},
    read_states: []const types.ReadState = &.{},
    messages: []const message.Message = &.{},

    pub fn isEmpty(self: Ready) bool {
        return self.soft_state == null and
            self.hard_state == null and
            self.snapshot == null and
            self.entries.len == 0 and
            self.committed_entries.len == 0 and
            self.read_states.len == 0 and
            self.messages.len == 0;
    }
};
