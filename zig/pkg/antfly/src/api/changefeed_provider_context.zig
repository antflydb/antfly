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

//! Independent provider continuations in one durable mixed-feed receipt.
const std = @import("std");
const control = @import("../metadata/changefeed_control.zig");
const A = std.mem.Allocator;

pub const Provider = enum { native, iceberg };
pub const Context = struct {
    version: u8 = 1,
    native: []const u8 = "",
    iceberg: []const u8,

    pub fn get(self: Context, provider: Provider) []const u8 {
        return switch (provider) {
            .native => self.native,
            .iceberg => self.iceberg,
        };
    }

    pub fn replace(self: Context, provider: Provider, value: []const u8) Context {
        var next = self;
        switch (provider) {
            .native => next.native = value,
            .iceberg => next.iceberg = value,
        }
        return next;
    }

    fn validate(self: Context) !void {
        if (self.version != 1 or self.iceberg.len == 0 or !control.validProviderContext(self.native) or !control.validProviderContext(self.iceberg))
            return error.ChangefeedResynchronizationRequired;
    }

    pub fn encode(self: Context, a: A) ![]const u8 {
        try self.validate();
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(bytes);
        const encoder = std.base64.url_safe_no_pad.Encoder;
        const len = encoder.calcSize(bytes.len);
        if (len > control.max_provider_context_bytes) return error.ChangefeedBackpressure;
        return encoder.encode(try a.alloc(u8, len), bytes);
    }

    pub fn decode(a: A, encoded: []const u8) !Context {
        if (encoded.len == 0 or !control.validProviderContext(encoded)) return error.ChangefeedResynchronizationRequired;
        const decoder = std.base64.url_safe_no_pad.Decoder;
        const len = decoder.calcSizeForSlice(encoded) catch return error.ChangefeedResynchronizationRequired;
        const bytes = try a.alloc(u8, len);
        decoder.decode(bytes, encoded) catch return error.ChangefeedResynchronizationRequired;
        const context = std.json.parseFromSliceLeaky(Context, a, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.ChangefeedResynchronizationRequired,
        };
        try context.validate();
        return context;
    }
};

test "external lake filesystem WAL mixed receipt keeps the other provider position" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first: Context = .{ .iceberg = "YQ" };
    const issued = try first.replace(.native, "Yg").encode(a);
    const final = try (try Context.decode(a, issued)).replace(.iceberg, "Yw").encode(a);
    const context = try Context.decode(a, final);
    try std.testing.expectEqualStrings("Yg", context.native);
    try std.testing.expectEqualStrings("Yw", context.iceberg);
}
