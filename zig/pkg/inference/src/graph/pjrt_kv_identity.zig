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

const std = @import("std");

pub const Kind = enum { key, value };

/// Phase-specific names alias the same layered state. Without a layer, the
/// name is the only state identifier. Keep selection, capacity, and execution
/// on this single definition, including its hash/equality contract.
pub const Identity = struct {
    layer_index: ?u32,
    name: []const u8,
    kind: Kind,

    pub fn eql(self: Identity, other: Identity) bool {
        return self.layer_index == other.layer_index and self.kind == other.kind and
            (self.layer_index != null or std.mem.eql(u8, self.name, other.name));
    }

    pub fn hash(self: Identity) u64 {
        var h = std.hash.Wyhash.init(0);
        const layered: u8 = @intFromBool(self.layer_index != null);
        const kind: u8 = @backingInt(self.kind);
        h.update(std.mem.asBytes(&layered));
        if (self.layer_index) |layer| h.update(std.mem.asBytes(&layer)) else h.update(self.name);
        h.update(std.mem.asBytes(&kind));
        return h.final();
    }

    pub const Context = struct {
        pub fn hash(_: @This(), key: Identity) u64 {
            return key.hash();
        }
        pub fn eql(_: @This(), a: Identity, b: Identity) bool {
            return a.eql(b);
        }
    };
};
