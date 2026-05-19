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

pub const types = @import("types.zig");
pub const message = @import("message.zig");
pub const logger = @import("logger.zig");
pub const random = @import("random.zig");
pub const storage = @import("storage.zig");
pub const log = @import("log.zig");
pub const ready = @import("ready.zig");
pub const raft = @import("raft.zig");
pub const raw_node = @import("raw_node.zig");

pub const Config = raft.Config;
pub const Message = message.Message;
pub const Entry = types.Entry;
pub const ConfChange = types.ConfChange;
pub const ConfChangeType = types.ConfChangeType;
pub const ConfChangeTransition = types.ConfChangeTransition;
pub const ConfChangeSingle = types.ConfChangeSingle;
pub const ConfChangeV2 = types.ConfChangeV2;
pub const ConfState = types.ConfState;
pub const HardState = types.HardState;
pub const SoftState = types.SoftState;
pub const Status = types.Status;
pub const ReadState = types.ReadState;
pub const ReadOnlyOption = types.ReadOnlyOption;
pub const LogLevel = logger.LogLevel;
pub const Logger = logger.Logger;
pub const TraceEventType = logger.TraceEventType;
pub const TraceEvent = logger.TraceEvent;
pub const TraceLogger = logger.TraceLogger;
pub const RandomSource = random.RandomSource;
pub const SplitMix64 = random.SplitMix64;
pub const Ready = ready.Ready;
pub const Storage = storage.Storage;
pub const MemoryStorage = storage.MemoryStorage;
pub const RawNode = raw_node.RawNode;

test "core module compiles" {
    _ = Config;
    _ = Message;
    _ = Entry;
    _ = ConfChange;
    _ = ConfChangeType;
    _ = ConfChangeTransition;
    _ = ConfChangeSingle;
    _ = ConfChangeV2;
    _ = ConfState;
    _ = HardState;
    _ = SoftState;
    _ = Status;
    _ = ReadState;
    _ = ReadOnlyOption;
    _ = LogLevel;
    _ = Logger;
    _ = TraceEventType;
    _ = TraceEvent;
    _ = TraceLogger;
    _ = RandomSource;
    _ = SplitMix64;
    _ = Ready;
    _ = Storage;
    _ = MemoryStorage;
    _ = RawNode;
}

test {
    _ = @import("raft_test.zig");
}
