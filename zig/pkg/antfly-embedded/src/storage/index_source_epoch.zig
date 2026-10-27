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

//! Native publication birth/floor fence. ISO2 fences older index readers;
//! roots already published in an older epoch remain readable and retryable.
const std = @import("std");
const publication = @import("index_publication.zig");
pub const owner_key = "index_source_owner_v1";
pub const Owner = struct {
    identity: publication.Digest,
    current: u64 = 0,
    floor: u64 = 0,
    families: bool = false,
    fields: bool = false,
    pub fn decode(raw: []const u8) !Owner {
        if (raw.len != 68 and raw.len != 84) return error.InvalidIndexPublication;
        const legacy = raw.len == 68;
        const fields = !legacy and std.mem.eql(u8, raw[0..4], "ISO4");
        const families = fields or (!legacy and std.mem.eql(u8, raw[0..4], "ISO3"));
        if ((!families and !std.mem.eql(u8, raw[0..4], if (legacy) "ISO1" else "ISO2")) or std.mem.allEqual(u8, raw[4..36], 0)) return error.InvalidIndexPublication;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
        if (!std.mem.eql(u8, raw[raw.len - 32 ..], &digest)) return error.InvalidIndexPublication;
        const result: Owner = .{
            .identity = raw[4..36].*,
            .current = if (legacy) 0 else std.mem.readInt(u64, raw[36..44], .little),
            .floor = if (legacy) 0 else std.mem.readInt(u64, raw[44..52], .little),
            .families = families,
            .fields = fields,
        };
        if ((!legacy and result.current == 0) or result.floor > result.current) return error.InvalidIndexPublication;
        return result;
    }
    pub fn encode(self: Owner) ![84]u8 {
        if (std.mem.allEqual(u8, &self.identity, 0) or self.current == 0 or self.floor > self.current or (self.fields and !self.families)) return error.InvalidIndexPublication;
        var raw: [84]u8 = undefined;
        @memcpy(raw[0..4], if (self.fields) "ISO4" else if (self.families) "ISO3" else "ISO2");
        @memcpy(raw[4..36], &self.identity);
        std.mem.writeInt(u64, raw[36..44], self.current, .little);
        std.mem.writeInt(u64, raw[44..52], self.floor, .little);
        std.crypto.hash.sha2.Sha256.hash(raw[0..52], raw[52..84], .{});
        return raw;
    }
    pub fn admitBirth(self: Owner, binding: publication.Binding) !u64 {
        if (!std.mem.eql(u8, &self.identity, &binding.owner)) return error.IndexSourceOwnerMismatch;
        if (self.current == 0) return 0;
        const epoch = tokenEpoch(binding) orelse return error.IndexPublicationEpochExpired;
        if (epoch != self.current) return error.IndexPublicationEpochExpired;
        return epoch;
    }
    pub fn admitPublication(self: Owner, binding: publication.Binding) !u64 {
        if (!std.mem.eql(u8, &self.identity, &binding.owner)) return error.IndexSourceOwnerMismatch;
        if (self.current == 0) return 0;
        const epoch = tokenEpoch(binding) orelse 0;
        if (epoch < self.floor or epoch > self.current) return error.IndexPublicationEpochExpired;
        return epoch;
    }
    pub fn newBinding(self: Owner, entropy: [16]u8) publication.Binding {
        var nonce: [32]u8 = undefined;
        @memcpy(nonce[0..8], "ISPE1\x00\x00\x00");
        std.mem.writeInt(u64, nonce[8..16], self.current, .little);
        @memcpy(nonce[16..32], &entropy);
        return .{ .owner = self.identity, .publication = nonce };
    }
};
pub fn tokenEpoch(binding: publication.Binding) ?u64 {
    if (!std.mem.eql(u8, binding.publication[0..8], "ISPE1\x00\x00\x00")) return null;
    return std.mem.readInt(u64, binding.publication[8..16], .little);
}
