//! Fuzz targets for HTTP parser and HPACK decoder.
//!
//! These tests feed arbitrary bytes into the parser and HPACK decoder
//! to verify they never crash, leak memory, or trigger undefined behavior.
//!
//! Run with: zig build test --fuzz
//! Or as regular tests (single deterministic pass) with: zig build test

const std = @import("std");
const parser_mod = @import("parser.zig");
const hpack = @import("hpack.zig");

const Parser = parser_mod.Parser;
const Smith = std.testing.Smith;

const fuzz_buf_size = 512;

test "fuzz HTTP request parser" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            const allocator = std.testing.allocator;
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 0));
            const input = buf[0..len];

            var parser = Parser.init(allocator);
            defer parser.deinit();
            _ = parser.feed(input) catch {};
        }
    }.f, .{});
}

test "fuzz HTTP response parser" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            const allocator = std.testing.allocator;
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 1));
            const input = buf[0..len];

            var parser = Parser.initResponse(allocator);
            defer parser.deinit();
            _ = parser.feed(input) catch {};
        }
    }.f, .{});
}

test "fuzz HTTP request parser byte-at-a-time" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            const allocator = std.testing.allocator;
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 2));
            const input = buf[0..len];

            var parser = Parser.init(allocator);
            defer parser.deinit();
            for (input) |byte| {
                _ = parser.feed(&[_]u8{byte}) catch break;
                if (parser.isComplete() or parser.isError()) break;
            }
        }
    }.f, .{});
}

test "fuzz HPACK integer decode" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 3));
            if (len < 2) return;
            const prefix: u4 = @intCast((buf[0] % 7) + 1);
            _ = hpack.decodeInteger(buf[1..len], prefix) catch {};
        }
    }.f, .{});
}

test "fuzz HPACK string decode" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            const allocator = std.testing.allocator;
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 4));
            const result = hpack.decodeString(buf[0..len], allocator) catch return;
            allocator.free(result.value);
        }
    }.f, .{});
}

test "fuzz HPACK header decode" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            const allocator = std.testing.allocator;
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 5));

            var ctx = hpack.HpackContext.init(allocator);
            defer ctx.deinit();

            const headers = hpack.decodeHeaders(&ctx, buf[0..len], allocator) catch return;
            for (headers) |h| h.deinit(allocator);
            allocator.free(headers);
        }
    }.f, .{});
}

test "fuzz Huffman decode" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *Smith) !void {
            const allocator = std.testing.allocator;
            var buf: [fuzz_buf_size]u8 = undefined;
            const len = smith.sliceWithHash(&buf, @as(u32, 6));
            const decoded = hpack.HuffmanCodec.decode(buf[0..len], allocator) catch return;
            allocator.free(decoded);
        }
    }.f, .{});
}
