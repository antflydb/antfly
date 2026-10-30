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

//! Complete-input JSON with bounded scanning, copying and original refusal.
const std = @import("std");

/// The caller supplies its existing borrowed control; no authority is retained.
pub fn Decoder(comptime Control: type) type {
    return struct {
        /// Preserve complete-input parse defaults while bounding scanner work by bytes
        /// and tokens. A giant string needs refills even if it is the only JSON value.
        /// This does not create execution authority or reserve decoded memory; callers
        /// must supply an already admitted owner and a charged allocator.
        pub fn parse(comptime T: type, allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions, control: Control) !std.json.Parsed(T) {
            var source = Source.init(allocator, input, control);
            defer source.deinit();
            var resolved = options;
            if (resolved.max_value_len == null) resolved.max_value_len = input.len;
            if (resolved.allocate == null) resolved.allocate = .alloc_if_needed;
            source.check() catch |err| return source.original(err);
            var parsed = std.json.parseFromTokenSource(T, allocator, &source, resolved) catch |err| return source.original(err);
            errdefer parsed.deinit();
            source.check() catch |err| return source.original(err);
            return parsed;
        }

        pub const Source = struct {
            scanner: std.json.Scanner,
            input: []const u8,
            offset: usize = 0,
            tokens: usize = 0,
            control: Control,
            failure: ?anyerror = null,

            pub const NextError = std.json.Scanner.Error || std.mem.Allocator.Error || error{JsonDecodeInterrupted};
            pub const PeekError = std.json.Scanner.Error || error{JsonDecodeInterrupted};
            pub const AllocError = NextError || error{ValueTooLong};
            pub const SkipError = NextError;

            pub fn init(allocator: std.mem.Allocator, input: []const u8, control: Control) Source {
                return .{ .scanner = std.json.Scanner.initStreaming(allocator), .input = input, .control = control };
            }

            pub fn deinit(self: *Source) void {
                self.scanner.deinit();
                self.* = undefined;
            }

            pub fn original(self: *const Source, err: anyerror) anyerror {
                return if (err == error.JsonDecodeInterrupted) self.failure orelse error.InvalidRunnableTransition else err;
            }

            fn check(self: *Source) error{JsonDecodeInterrupted}!void {
                self.control.check(self.control.ptr) catch |err| {
                    self.failure = err;
                    return error.JsonDecodeInterrupted;
                };
            }

            fn refill(self: *Source) error{JsonDecodeInterrupted}!void {
                try self.check();
                const end = self.offset + @min(@as(usize, 4096), self.input.len - self.offset);
                self.scanner.feedInput(self.input[self.offset..end]);
                self.offset = end;
                if (end == self.input.len) self.scanner.endInput();
            }

            pub fn next(self: *Source) NextError!std.json.Token {
                if (self.tokens % 64 == 0) try self.check();
                while (true) {
                    const token = self.scanner.next() catch |err| switch (err) {
                        error.BufferUnderrun => {
                            try self.refill();
                            continue;
                        },
                        error.SyntaxError => return error.SyntaxError,
                        error.UnexpectedEndOfInput => return error.UnexpectedEndOfInput,
                        error.OutOfMemory => return error.OutOfMemory,
                    };
                    self.tokens +%= 1;
                    return token;
                }
            }

            pub fn peekNextTokenType(self: *Source) PeekError!std.json.TokenType {
                while (true) {
                    return self.scanner.peekNextTokenType() catch |err| switch (err) {
                        error.BufferUnderrun => {
                            try self.refill();
                            continue;
                        },
                        error.SyntaxError => return error.SyntaxError,
                        error.UnexpectedEndOfInput => return error.UnexpectedEndOfInput,
                    };
                }
            }

            pub fn nextAllocMax(self: *Source, allocator: std.mem.Allocator, when: std.json.AllocWhen, max_value_len: usize) AllocError!std.json.Token {
                const kind = try self.peekNextTokenType();
                if (kind != .number and kind != .string) return self.next();
                var bytes = std.array_list.Managed(u8).init(allocator);
                errdefer bytes.deinit();
                const borrowed = try self.allocNextIntoArrayListMax(&bytes, when, max_value_len);
                if (borrowed) |text| return if (kind == .number) .{ .number = text } else .{ .string = text };
                const owned = try self.takeOwned(&bytes);
                return if (kind == .number) .{ .allocated_number = owned } else .{ .allocated_string = owned };
            }

            pub fn allocNextIntoArrayListMax(self: *Source, bytes: *std.array_list.Managed(u8), when: std.json.AllocWhen, max_value_len: usize) AllocError!?[]const u8 {
                var prefix: ?[]const u8 = null;
                while (true) {
                    const token = try self.next();
                    switch (token) {
                        .partial_number, .partial_string => |text| {
                            if (when == .alloc_if_needed and bytes.items.len == 0) {
                                if (prefix) |old| {
                                    if (try joinBorrowed(old, text)) |joined| {
                                        prefix = joined;
                                        continue;
                                    }
                                } else {
                                    prefix = text;
                                    continue;
                                }
                            }
                            try self.flushPrefix(bytes, &prefix, max_value_len);
                            try self.append(bytes, text, max_value_len);
                        },
                        inline .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4 => |text| {
                            try self.flushPrefix(bytes, &prefix, max_value_len);
                            try self.append(bytes, &text, max_value_len);
                        },
                        .number, .string => |text| {
                            if (when == .alloc_if_needed and bytes.items.len == 0) {
                                if (prefix) |old| {
                                    if (try joinBorrowed(old, text)) |joined| return joined;
                                } else return text;
                            }
                            try self.flushPrefix(bytes, &prefix, max_value_len);
                            try self.append(bytes, text, max_value_len);
                            return null;
                        },
                        else => unreachable,
                    }
                }
            }

            fn flushPrefix(self: *Source, bytes: *std.array_list.Managed(u8), prefix: *?[]const u8, limit: usize) AllocError!void {
                if (prefix.*) |text| try self.append(bytes, text, limit);
                prefix.* = null;
            }

            pub fn skipValue(self: *Source) SkipError!void {
                const height = self.scanner.stackHeight();
                const kind = try self.peekNextTokenType();
                switch (kind) {
                    .array_begin, .object_begin => {
                        _ = try self.next();
                        while (self.scanner.stackHeight() > height) _ = try self.next();
                    },
                    .number, .string => while (true) {
                        switch (try self.next()) {
                            .number, .string => break,
                            .partial_number, .partial_string, .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4 => {},
                            else => unreachable,
                        }
                    },
                    .true, .false, .null => _ = try self.next(),
                    else => unreachable,
                }
            }

            fn copy(self: *Source, destination: []u8, input: []const u8) error{JsonDecodeInterrupted}!void {
                var offset: usize = 0;
                while (offset < input.len) {
                    try self.check();
                    const end = offset + @min(@as(usize, 4096), input.len - offset);
                    @memcpy(destination[offset..end], input[offset..end]);
                    offset = end;
                }
                try self.check();
            }

            fn append(self: *Source, bytes: *std.array_list.Managed(u8), text: []const u8, limit: usize) AllocError!void {
                const end = std.math.add(usize, bytes.items.len, text.len) catch return error.ValueTooLong;
                if (end > limit) return error.ValueTooLong;
                if (end > bytes.capacity) {
                    const capacity = @min(limit, @max(end, bytes.capacity +| bytes.capacity / 2 +| 8));
                    const replacement = try bytes.allocator.alloc(u8, capacity);
                    errdefer bytes.allocator.free(replacement);
                    try self.copy(replacement[0..bytes.items.len], bytes.items);
                    bytes.allocator.free(bytes.allocatedSlice());
                    bytes.items = replacement[0..bytes.items.len];
                    bytes.capacity = capacity;
                }
                try self.copy(bytes.items.ptr[bytes.items.len..end], text);
                bytes.items.len = end;
            }

            fn takeOwned(self: *Source, bytes: *std.array_list.Managed(u8)) AllocError![]u8 {
                try self.check();
                const allocator = bytes.allocator;
                if (bytes.items.len == bytes.capacity) {
                    const result = bytes.items;
                    bytes.* = .init(allocator);
                    return result;
                }
                const result = try allocator.alloc(u8, bytes.items.len);
                errdefer allocator.free(result);
                try self.copy(result, bytes.items);
                bytes.clearAndFree();
                return result;
            }
        };

        fn joinBorrowed(first: []const u8, next: []const u8) error{ValueTooLong}!?[]const u8 {
            // Scanner chunks borrow the same immutable complete input. Escape tokens
            // are flushed separately; only adjacent raw slices may be rejoined.
            if (@intFromPtr(first.ptr) + first.len != @intFromPtr(next.ptr)) return null;
            const len = std.math.add(usize, first.len, next.len) catch return error.ValueTooLong;
            // Complete-input Scanner ignores max_value_len for borrowed values.
            // Keep that behavior even when cooperative refills split a raw token.
            // If escapes require an owned buffer, append enforces the limit.
            return first.ptr[0..len];
        }
    };
}
