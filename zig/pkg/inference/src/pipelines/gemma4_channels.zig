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

//! Gemma 4 canonical turn projection. Public text is outside the thought
//! delimiters; <channel|> closes thought content, not its header.
const std = @import("std");

pub const Projection = struct {
    stage: enum { start, header, thought, public, closed } = .start,
    inspected: usize = 0,
    thought_start: ?usize = null,
    thought_end: ?usize = null,
    public_start: ?usize = null,
    public_end: ?usize = null,
    channel_start: i32,
    channel_end: i32,
    turn_end: i32,
    thought_header: []const i32,

    pub fn init(private: bool, start: i32, end: i32, turn_end: i32, header: []const i32) Projection {
        return .{
            .stage = if (private) .thought else .start,
            .thought_start = if (private) 0 else null,
            .channel_start = start,
            .channel_end = end,
            .turn_end = turn_end,
            .thought_header = header,
        };
    }

    pub fn update(self: *Projection, tokens: []const i64) void {
        while (self.inspected < tokens.len and self.stage != .closed) {
            const i = self.inspected;
            const token = tokens[i];
            switch (self.stage) {
                .start => {
                    if (token == self.channel_start) {
                        self.stage = .header;
                    } else if (token == self.channel_end or token == self.turn_end) {
                        self.stage = .closed;
                    } else {
                        self.public_start = i;
                        self.stage = .public;
                    }
                    continue;
                },
                .header => {
                    // Incomplete and unknown channel headers stay private.
                    if (i >= self.thought_header.len or token != self.thought_header[i]) {
                        self.stage = .closed;
                    } else if (i + 1 == self.thought_header.len) {
                        self.thought_start = i + 1;
                        self.stage = .thought;
                    }
                },
                .thought => {
                    if (token == self.channel_end) {
                        self.thought_end = i;
                        self.public_start = i + 1;
                        self.stage = .public;
                    } else if (token == self.channel_start or token == self.turn_end) {
                        self.thought_end = i;
                        self.stage = .closed;
                    }
                },
                .public => {
                    if (token == self.channel_start or token == self.channel_end or token == self.turn_end) {
                        self.public_end = i;
                        self.stage = .closed;
                    }
                },
                .closed => unreachable,
            }
            self.inspected += 1;
        }
    }

    pub fn publicTokens(self: Projection, tokens: []const i64) []const i64 {
        const start = self.public_start orelse return tokens[0..0];
        return tokens[start .. self.public_end orelse tokens.len];
    }
    pub fn thoughtTokens(self: Projection, tokens: []const i64) []const i64 {
        const start = self.thought_start orelse return tokens[0..0];
        return tokens[start .. self.thought_end orelse tokens.len];
    }
};

test "canonical Gemma4 projection preserves public answers and hides thoughts across every stream split" {
    const header = [_]i32{ 100, 101, 102 };
    const cases = [_]struct { tokens: []const i64, answer: []const i64, thought: []const i64, private: bool = false }{
        .{ .tokens = &.{ 7, 8, 104 }, .answer = &.{ 7, 8 }, .thought = &.{} },
        .{ .tokens = &.{ 100, 101, 102, 9, 10, 103, 7, 8, 104 }, .answer = &.{ 7, 8 }, .thought = &.{ 9, 10 } },
        .{ .tokens = &.{ 100, 101, 102, 103, 7, 104 }, .answer = &.{7}, .thought = &.{} },
        .{ .tokens = &.{ 9, 10, 103, 7, 104 }, .answer = &.{7}, .thought = &.{ 9, 10 }, .private = true },
        .{ .tokens = &.{ 100, 101 }, .answer = &.{}, .thought = &.{} },
        .{ .tokens = &.{ 100, 199, 102, 9, 103, 7 }, .answer = &.{}, .thought = &.{} },
        .{ .tokens = &.{ 100, 101, 102, 9, 10 }, .answer = &.{}, .thought = &.{ 9, 10 } },
        .{ .tokens = &.{ 7, 100, 101, 102, 9, 103, 8 }, .answer = &.{7}, .thought = &.{} },
    };
    for (cases) |case| {
        var streaming = Projection.init(case.private, 100, 103, 104, &header);
        for (0..case.tokens.len + 1) |end| {
            streaming.update(case.tokens[0..end]);
            const answer = streaming.publicTokens(case.tokens[0..end]);
            try std.testing.expect(std.mem.startsWith(i64, case.answer, answer));
        }
        try std.testing.expectEqualSlices(i64, case.answer, streaming.publicTokens(case.tokens));
        try std.testing.expectEqualSlices(i64, case.thought, streaming.thoughtTokens(case.tokens));
        var buffered = Projection.init(case.private, 100, 103, 104, &header);
        buffered.update(case.tokens);
        try std.testing.expectEqualSlices(i64, case.answer, buffered.publicTokens(case.tokens));
    }
}
