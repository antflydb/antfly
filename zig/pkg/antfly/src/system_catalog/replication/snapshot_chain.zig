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
pub const Digest = [32]u8;
pub const end: Digest = @splat(0);

pub const Page = struct {
    relation: u64,
    offset: u64,
    count: u32,
    content_digest: Digest,
    next: Digest,

    pub fn commitment(self: Page) !Digest {
        if (self.relation == 0 or self.count == 0 or self.count > 4096 or
            std.mem.eql(u8, &self.content_digest, &end)) return error.InvalidReplicationSnapshotCursor;
        _ = std.math.add(u64, self.offset, self.count) catch return error.InvalidReplicationSnapshotCursor;
        var fields: [20]u8 = undefined;
        std.mem.writeInt(u64, fields[0..8], self.relation, .little);
        std.mem.writeInt(u64, fields[8..16], self.offset, .little);
        std.mem.writeInt(u32, fields[16..20], self.count, .little);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/subscription-snapshot-page/v1\x00");
        hash.update(&fields);
        hash.update(&self.content_digest);
        hash.update(&self.next);
        var digest: Digest = undefined;
        hash.final(&digest);
        return digest;
    }
};
