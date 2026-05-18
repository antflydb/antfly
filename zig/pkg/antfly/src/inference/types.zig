// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Provider-neutral types and interfaces for ML inference.

const std = @import("std");

pub const Role = enum {
    system,
    user,
    assistant,

    pub fn toSlice(self: Role) []const u8 {
        return switch (self) {
            .system => "system",
            .user => "user",
            .assistant => "assistant",
        };
    }
};

pub const ChatMessage = struct {
    role: Role,
    content: []const u8,
};

pub const EmbedResult = struct {
    /// One f32 vector per input text.
    vectors: []const []const f32,
    dimension: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EmbedResult) void {
        for (self.vectors) |v| self.allocator.free(v);
        self.allocator.free(self.vectors);
        self.* = undefined;
    }
};

pub const SparseEmbedResult = struct {
    indices: []const []const i32,
    values: []const []const f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SparseEmbedResult) void {
        for (self.indices) |idx| self.allocator.free(idx);
        self.allocator.free(self.indices);
        for (self.values) |val| self.allocator.free(val);
        self.allocator.free(self.values);
        self.* = undefined;
    }
};

pub const GenerateResult = struct {
    content: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerateResult) void {
        self.allocator.free(self.content);
        self.* = undefined;
    }
};

pub const RerankResult = struct {
    scores: []const f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *RerankResult) void {
        self.allocator.free(self.scores);
        self.* = undefined;
    }
};

/// Provider-neutral dense embedding interface.
pub const Embedder = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        embed: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) anyerror!EmbedResult,
    };

    pub fn embed(self: Embedder, alloc: std.mem.Allocator, model: []const u8, inputs: []const []const u8) !EmbedResult {
        return self.vtable.embed(self.ptr, alloc, model, inputs);
    }
};

/// Provider-neutral text generation / chat interface.
pub const Generator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        generate: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) anyerror!GenerateResult,
    };

    pub fn generate(self: Generator, alloc: std.mem.Allocator, model: []const u8, messages: []const ChatMessage) !GenerateResult {
        return self.vtable.generate(self.ptr, alloc, model, messages);
    }
};

/// Provider-neutral reranking interface.
pub const Reranker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        rerank: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) anyerror!RerankResult,
    };

    pub fn rerank(self: Reranker, alloc: std.mem.Allocator, model: []const u8, query: []const u8, documents: []const []const u8) !RerankResult {
        return self.vtable.rerank(self.ptr, alloc, model, query, documents);
    }
};

test "types compile" {
    _ = Embedder;
    _ = Generator;
    _ = Reranker;
    _ = EmbedResult;
    _ = SparseEmbedResult;
    _ = GenerateResult;
    _ = RerankResult;
    _ = ChatMessage;
    _ = Role;
}
