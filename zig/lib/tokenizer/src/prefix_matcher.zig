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

// Trie-based prefix matcher for user-defined symbols.
// Ported from go-sentencepiece/internal/prefixmatcher.

const std = @import("std");

const TrieNode = struct {
    children: std.AutoHashMap(u21, *TrieNode),
    final: bool,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) !*TrieNode {
        const node = try allocator.create(TrieNode);
        node.* = .{
            .children = std.AutoHashMap(u21, *TrieNode).init(allocator),
            .final = false,
            .allocator = allocator,
        };
        return node;
    }

    fn deinit(self: *TrieNode) void {
        var it = self.children.valueIterator();
        while (it.next()) |child| {
            child.*.deinit();
        }
        self.children.deinit();
        self.allocator.destroy(self);
    }
};

pub const PrefixMatcher = struct {
    root: *TrieNode,
    allocator: std.mem.Allocator,

    pub fn initFromMap(allocator: std.mem.Allocator, vocab: *const std.StringHashMap(void)) !PrefixMatcher {
        const root = try TrieNode.init(allocator);

        var it = vocab.keyIterator();
        while (it.next()) |key| {
            try addWord(root, key.*, allocator);
        }

        return .{ .root = root, .allocator = allocator };
    }

    pub fn deinit(self: *PrefixMatcher) void {
        self.root.deinit();
    }

    /// Find the byte length of the longest matching prefix in the trie.
    /// Returns 0 if no prefix matches.
    pub fn findPrefixLen(self: *const PrefixMatcher, text: []const u8) usize {
        var node = self.root;
        var max_len: usize = 0;
        var byte_pos: usize = 0;

        while (byte_pos < text.len) {
            const rune_len = std.unicode.utf8ByteSequenceLength(text[byte_pos]) catch break;
            if (byte_pos + rune_len > text.len) break;

            const codepoint = std.unicode.utf8Decode(text[byte_pos .. byte_pos + rune_len]) catch break;

            const child = node.children.get(codepoint) orelse break;

            byte_pos += rune_len;
            if (child.final) {
                max_len = byte_pos;
            }
            node = child;
        }

        return max_len;
    }
};

fn addWord(root: *TrieNode, word: []const u8, allocator: std.mem.Allocator) !void {
    var node = root;
    var pos: usize = 0;

    while (pos < word.len) {
        const rune_len = std.unicode.utf8ByteSequenceLength(word[pos]) catch break;
        if (pos + rune_len > word.len) break;

        const codepoint = std.unicode.utf8Decode(word[pos .. pos + rune_len]) catch break;

        const entry = try node.children.getOrPut(codepoint);
        if (!entry.found_existing) {
            entry.value_ptr.* = try TrieNode.init(allocator);
        }
        node = entry.value_ptr.*;
        pos += rune_len;
    }

    node.final = true;
}

test "empty matcher" {
    const allocator = std.testing.allocator;
    var vocab = std.StringHashMap(void).init(allocator);
    defer vocab.deinit();

    var matcher = try PrefixMatcher.initFromMap(allocator, &vocab);
    defer matcher.deinit();

    try std.testing.expectEqual(@as(usize, 0), matcher.findPrefixLen("hello"));
}

test "basic match" {
    const allocator = std.testing.allocator;
    var vocab = std.StringHashMap(void).init(allocator);
    defer vocab.deinit();
    try vocab.put("hello", {});
    try vocab.put("hell", {});

    var matcher = try PrefixMatcher.initFromMap(allocator, &vocab);
    defer matcher.deinit();

    // Should match longest prefix
    try std.testing.expectEqual(@as(usize, 5), matcher.findPrefixLen("hello world"));
    try std.testing.expectEqual(@as(usize, 4), matcher.findPrefixLen("hell yeah"));
    try std.testing.expectEqual(@as(usize, 0), matcher.findPrefixLen("world"));
}
