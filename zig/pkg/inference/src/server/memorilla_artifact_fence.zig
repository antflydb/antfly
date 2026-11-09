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

//! Full-request stat-generation fence, in addition to managed receipt identity.
//! This does not replace the loaded model's mandatory cryptographic admission.
const std = @import("std");
const qualified = @import("../models/memorilla_qualification.zig");
const selection = @import("../registry/model_selection.zig");
const receipt = @import("../registry/managed_receipt.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;

pub const Snapshot = struct {
    encoder: [32]u8,
    decoder: [32]u8,

    fn matches(self: @This(), other: @This()) bool {
        return std.mem.eql(u8, &self.encoder, &other.encoder) and
            std.mem.eql(u8, &self.decoder, &other.decoder);
    }
};

fn observe(a: A, io: std.Io, encoder: selection.ResolvedModel, decoder: selection.ResolvedModel, control: Control) !Snapshot {
    try control.check();
    if (encoder.publication == null or decoder.publication == null) return error.MemorillaManagedProducerRequired;
    try encoder.verify(a, io);
    try decoder.verify(a, io);
    const result = Snapshot{
        .encoder = try qualified.artifactGeneration(a, io, encoder.path, .qwen3_encoder_f32_v1, control),
        .decoder = try qualified.artifactGeneration(a, io, decoder.path, .qwen3_decoder_f32_kv_f32_v1, control),
    };
    try encoder.verify(a, io);
    try decoder.verify(a, io);
    try control.check();
    return result;
}

/// Capture after CPU source preflight and before acquiring memory model work.
/// Double observation rejects a moving initial view. The actual model load
/// still verifies its crypto/stat proof; a later request-wide mismatch rejects
/// publication even when a new cold load independently accepted valid files.
pub fn capture(a: A, io: std.Io, encoder: selection.ResolvedModel, decoder: selection.ResolvedModel, control: Control) !Snapshot {
    const first = try observe(a, io, encoder, decoder, control);
    const second = try observe(a, io, encoder, decoder, control);
    if (!first.matches(second)) return error.ModelArtifactsChanging;
    return first;
}

/// Call after every phase has retired and immediately before response handoff.
/// Missing/alternate artifact state is publication drift once a snapshot exists.
/// Cancellation, OOM and unrelated I/O errors retain their original contracts.
pub fn verify(a: A, io: std.Io, encoder: selection.ResolvedModel, decoder: selection.ResolvedModel, expected: Snapshot, control: Control) !void {
    const actual = observe(a, io, encoder, decoder, control) catch |err| switch (err) {
        error.FileNotFound, error.NotDir, error.UnsupportedMemorillaArtifact => return error.ModelArtifactsChanging,
        else => return err,
    };
    if (!expected.matches(actual)) return error.ModelArtifactsChanging;
    try control.check();
}

const TestInventory = struct {
    a: A,
    temp: std.testing.TmpDir,
    encoder: selection.ResolvedModel,
    decoder: selection.ResolvedModel,

    fn init(a: A) !@This() {
        const io = std.testing.io;
        var temp = std.testing.tmpDir(.{});
        errdefer temp.cleanup();
        const parsed = try std.json.parseFromSlice(std.json.Value, a, qualified.lock_bytes, .{});
        defer parsed.deinit();
        for ([_][]const u8{ "encoder", "decoder" }) |role| {
            try temp.dir.createDirPath(io, role);
            for (parsed.value.object.get("models").?.object.get(role).?.object.get("files").?.array.items) |entry| {
                const path = try std.fs.path.join(a, &.{ role, entry.object.get("path").?.string });
                defer a.free(path);
                if (std.fs.path.dirname(path)) |parent| try temp.dir.createDirPath(io, parent);
                try temp.dir.writeFile(io, .{ .sub_path = path, .data = "" });
            }
            const manifest_path = try std.fs.path.join(a, &.{ role, "model_manifest.json" });
            defer a.free(manifest_path);
            try temp.dir.writeFile(io, .{ .sub_path = manifest_path, .data = "{}" });
            const receipt_path = try std.fs.path.join(a, &.{ role, receipt.complete_filename });
            defer a.free(receipt_path);
            try temp.dir.writeFile(io, .{ .sub_path = receipt_path, .data = "unchanged receipt bytes" });
        }
        const encoder = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "encoder" });
        errdefer a.free(encoder);
        const decoder = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "decoder" });
        errdefer a.free(decoder);
        return .{ .a = a, .temp = temp, .encoder = .{ .path = encoder, .publication = try receipt.publicationGeneration(a, io, encoder) }, .decoder = .{ .path = decoder, .publication = try receipt.publicationGeneration(a, io, decoder) } };
    }

    fn deinit(self: *@This()) void {
        self.a.free(self.encoder.path);
        self.a.free(self.decoder.path);
        self.temp.cleanup();
    }

    fn receiptProofsStillPass(self: *@This()) !void {
        try self.encoder.verify(self.a, std.testing.io);
        try self.decoder.verify(self.a, std.testing.io);
    }
};

test "Memorilla full request artifact fence rejects changed required encoder file despite unchanged receipt" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var inventory = try TestInventory.init(a);
    defer inventory.deinit();
    // Empty dummy artifacts deliberately exercise stat inventory, not crypto.
    const before = try capture(a, io, inventory.encoder, inventory.decoder, .{});
    try verify(a, io, inventory.encoder, inventory.decoder, before, .{});
    try inventory.temp.dir.writeFile(io, .{ .sub_path = "encoder/config.json", .data = "changed" });
    try inventory.receiptProofsStillPass();
    try std.testing.expectError(error.ModelArtifactsChanging, verify(a, io, inventory.encoder, inventory.decoder, before, .{}));
}

test "Memorilla full request artifact fence rejects added then removed sidecar with unchanged receipts" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var inventory = try TestInventory.init(a);
    defer inventory.deinit();
    const before = try capture(a, io, inventory.encoder, inventory.decoder, .{});
    try inventory.temp.dir.writeFile(io, .{ .sub_path = "encoder/added_tokens.json", .data = "{}" });
    try inventory.receiptProofsStillPass();
    try std.testing.expectError(error.ModelArtifactsChanging, verify(a, io, inventory.encoder, inventory.decoder, before, .{}));
    try inventory.temp.dir.deleteFile(io, "encoder/added_tokens.json");
    try inventory.receiptProofsStillPass();
    try std.testing.expectError(error.ModelArtifactsChanging, verify(a, io, inventory.encoder, inventory.decoder, before, .{}));
}

test "Memorilla full request artifact fence rejects missing decoder input and manifest" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var inventory = try TestInventory.init(a);
    defer inventory.deinit();
    const before = try capture(a, io, inventory.encoder, inventory.decoder, .{});
    try inventory.temp.dir.deleteFile(io, "decoder/tokenizer.json");
    try inventory.receiptProofsStillPass();
    try std.testing.expectError(error.ModelArtifactsChanging, verify(a, io, inventory.encoder, inventory.decoder, before, .{}));
    try inventory.temp.dir.writeFile(io, .{ .sub_path = "decoder/tokenizer.json", .data = "" });
    const restored = try capture(a, io, inventory.encoder, inventory.decoder, .{});
    try inventory.temp.dir.deleteFile(io, "decoder/model_manifest.json");
    try inventory.receiptProofsStillPass();
    try std.testing.expectError(error.ModelArtifactsChanging, verify(a, io, inventory.encoder, inventory.decoder, restored, .{}));
}

test "Memorilla full request artifact fence preserves cancellation and initial missing errors" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var inventory = try TestInventory.init(a);
    defer inventory.deinit();
    const before = try capture(a, io, inventory.encoder, inventory.decoder, .{});
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, verify(a, io, inventory.encoder, inventory.decoder, before, .{ .check_fn = Cancel.check }));
    try inventory.temp.dir.deleteFile(io, "encoder/config.json");
    try std.testing.expectError(error.FileNotFound, capture(a, io, inventory.encoder, inventory.decoder, .{}));
}
