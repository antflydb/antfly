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

//! Optional immutable-generation hint. The physical query-pin guard serializes
//! access; source/replay admission fences validate the current state separately.
const std = @import("std");
const Request = @import("native_query_cut_contract.zig").Request;
pub const Proof = struct { sequence: u64, digest: [32]u8, manifest_checksum: [32]u8 };
pub const State = struct {
    id: [64]u8,
    table_id: u64,
    expires_ms: u64,
    sequence: u64,
    digest: [32]u8,
    manifest_checksum: [32]u8,

    pub fn init(request: Request, committed: Proof) !State {
        try request.validate(@import("native_query_cut.zig").nowMs());
        return .{ .id = request.id[0..64].*, .table_id = request.table_id, .expires_ms = request.expires_ms, .sequence = committed.sequence, .digest = committed.digest, .manifest_checksum = committed.manifest_checksum };
    }
    pub fn proof(self: State) Proof {
        return .{ .sequence = self.sequence, .digest = self.digest, .manifest_checksum = self.manifest_checksum };
    }
    pub fn asRequest(self: *const State) Request {
        return .{ .id = &self.id, .table_id = self.table_id, .expires_ms = self.expires_ms, .create = true };
    }
    pub fn matches(self: State, digest: [32]u8, now_ms: u64) bool {
        return self.expires_ms > now_ms and std.mem.eql(u8, &self.digest, &digest);
    }
};

test "external lake native reuse rejects state and expiry changes" {
    const id: [64]u8 = @splat('a');
    const state = try State.init(.{ .id = &id, .table_id = 7, .expires_ms = @import("native_query_cut.zig").nowMs() + 300000, .create = true }, .{ .sequence = 123, .digest = @splat(42), .manifest_checksum = @splat(1) });
    try std.testing.expect(state.matches(@splat(42), state.expires_ms - 1));
    try std.testing.expect(!state.matches(@splat(43), state.expires_ms - 1));
    try std.testing.expect(!state.matches(@splat(42), state.expires_ms));
    try std.testing.expectEqualStrings(&id, state.asRequest().id);
}
