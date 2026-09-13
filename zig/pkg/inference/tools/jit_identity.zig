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

//! Preserve the runtime qualification identity format while making source
//! hashing a lazy, cached host tool with explicitly declared file inputs.
const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(allocator);
    var output: std.Io.Writer.Allocating = .init(allocator);
    var index: usize = 1;
    while (index < args.len) {
        const name = args[index];
        index += 1;
        if (std.mem.eql(u8, name, "--output")) {
            if (index + 1 != args.len) return error.InvalidArguments;
            try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[index], .data = output.written() });
            return;
        }
        var digest: [Sha256.digest_length]u8 = undefined;
        if (std.mem.eql(u8, name, "baseline")) {
            if (index == args.len) return error.InvalidArguments;
            Sha256.hash(try readSource(init, args[index]), &digest, .{});
            index += 1;
        } else {
            if (!std.mem.eql(u8, name, "qualification") and !std.mem.eql(u8, name, "dispatch")) return error.InvalidArguments;
            if (index == args.len) return error.InvalidArguments;
            const count = try std.fmt.parseInt(usize, args[index], 10);
            index += 1;
            if (count > args.len - index) return error.InvalidArguments;
            var hash = Sha256.init(.{});
            hash.update("antfly-runtime-jit-source-bundle/v1");
            updateLength(&hash, count);
            for (args[index..][0..count]) |path| {
                const source = try readSource(init, path);
                updateLength(&hash, source.len);
                hash.update(source);
            }
            hash.final(&digest);
            index += count;
        }
        try output.writer.print("pub const {s} = \"{s}\";\n", .{ name, std.fmt.bytesToHex(digest, .lower) });
    }
    return error.MissingOutput;
}

fn readSource(init: std.process.Init, path: []const u8) ![]const u8 {
    return std.Io.Dir.cwd().readFileAlloc(init.io, path, init.arena.allocator(), .limited(16 * 1024 * 1024));
}

fn updateLength(hash: *Sha256, value: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @intCast(value), .little);
    hash.update(&bytes);
}
