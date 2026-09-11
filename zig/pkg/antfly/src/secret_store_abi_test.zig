// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const secrets = @import("common/secrets.zig");
const error_abi = @import("runtime_error_abi.zig");

extern fn secret_store_abi_create(*const std.mem.Allocator, [*]const u8, usize, *?*secrets.FileStore) callconv(.c) error_abi.Status;
extern fn secret_store_abi_destroy(*secrets.FileStore) callconv(.c) void;
extern fn secret_store_abi_create_layered(*const std.mem.Allocator, [*]const u8, usize, [*]const u8, usize, *?*secrets.FileStore) callconv(.c) error_abi.Status;
extern fn secret_store_abi_set_open_fault(u8) callconv(.c) void;

test "secret store operations retain their IO owner across runtime archives" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fs.path.join(alloc, &.{ root, "secrets.json" });
    defer alloc.free(path);
    var store: ?*secrets.FileStore = null;
    const status = secret_store_abi_create(&alloc, path.ptr, path.len, &store);
    if (!status.isOk()) return error_abi.errorFromStatus(status);
    defer secret_store_abi_destroy(store.?);

    // A missing optional secret file is normal. Its FileNotFound must be
    // interpreted in the archive that owns the borrowed std.Io vtable.
    try std.testing.expect(!(try store.?.refreshIfChanged()));
    try std.testing.expect(!(try store.?.refreshIfChangedThrottled(std.time.ns_per_s)));
    try std.testing.expect(!store.?.healthSnapshot().last_reload_failed);
    try std.testing.expectError(error.InvalidSecretKey, store.?.put(alloc, "", "invalid"));

    var written = try store.?.put(alloc, "openai.api_key", "first");
    defer written.deinit(alloc);
    const first = try store.?.getOwned(alloc, "openai.api_key");
    defer if (first) |value| alloc.free(value);
    try std.testing.expectEqualStrings("first", first.?);
    const listed = try store.?.list(alloc);
    defer secrets.freeListedSecrets(alloc, listed);
    try std.testing.expect(listed.len >= 1);
    const resolved = try store.?.getOwnedWithGeneration(alloc, "openai.api_key");
    defer alloc.free(resolved.value);
    try std.testing.expectEqualStrings("first", resolved.value);

    // Publish a replacement outside the owner, then resolve through the other
    // archive. The new value and generation must travel together, while the
    // earlier caller-owned value remains valid.
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "secrets.next.json",
        .data = "{\"secrets\":[{\"key\":\"openai.api_key\",\"value\":\"rotated\",\"created_at_ns\":1,\"updated_at_ns\":2}]}",
    });
    try tmp.dir.rename("secrets.next.json", tmp.dir, "secrets.json", std.testing.io);
    try std.testing.expect(try store.?.refreshIfChanged());
    const rotated = try store.?.resolveValueWithGenerationOwned(alloc, "${secret:openai.api_key}");
    defer alloc.free(rotated.value);
    try std.testing.expectEqualStrings("rotated", rotated.value);
    try std.testing.expect(rotated.generation != resolved.generation);
    try std.testing.expectEqualStrings("first", resolved.value);

    // Invalid and disappearing replacements must retain the last known good
    // value, generation, and failure evidence across the archive boundary.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "secrets.json", .data = "{" });
    try std.testing.expect(!(try store.?.refreshIfChanged()));
    try std.testing.expect(store.?.reloadFailed());
    const stale = try store.?.getOwnedWithGeneration(alloc, "openai.api_key");
    defer alloc.free(stale.value);
    try std.testing.expectEqualStrings("rotated", stale.value);
    try std.testing.expectEqual(rotated.generation, stale.generation);
    try tmp.dir.deleteFile(std.testing.io, "secrets.json");
    try std.testing.expect(!(try store.?.refreshIfChanged()));
    try std.testing.expect(store.?.healthSnapshot().stale_snapshot);

    try std.testing.expect(try store.?.delete("openai.api_key"));
    try std.testing.expect(!store.?.reloadFailed());
    try std.testing.expectError(error.SecretNotFound, store.?.getOwnedWithGeneration(alloc, "vopr_abi_missing_secret"));
}

test "secret store archive boundary preserves layered precedence and rotation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const primary = try std.fs.path.join(alloc, &.{ root, "primary.json" });
    defer alloc.free(primary);
    const fallback = try std.fs.path.join(alloc, &.{ root, "fallback.json" });
    defer alloc.free(fallback);
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "fallback.json",
        .data = "{\"secrets\":[{\"key\":\"vopr.review\",\"value\":\"fallback\",\"created_at_ns\":1,\"updated_at_ns\":1}]}",
    });
    var store: ?*secrets.FileStore = null;
    const status = secret_store_abi_create_layered(&alloc, primary.ptr, primary.len, fallback.ptr, fallback.len, &store);
    if (!status.isOk()) return error_abi.errorFromStatus(status);
    defer secret_store_abi_destroy(store.?);
    const original = try store.?.getOwnedWithGeneration(alloc, "vopr.review");
    defer alloc.free(original.value);
    try std.testing.expectEqualStrings("fallback", original.value);
    var written = try store.?.put(alloc, "vopr.review", "primary");
    defer written.deinit(alloc);
    const overriding = try store.?.getOwned(alloc, "vopr.review");
    defer alloc.free(overriding.?);
    try std.testing.expectEqualStrings("primary", overriding.?);
    const listed = try store.?.list(alloc);
    defer secrets.freeListedSecrets(alloc, listed);
    var matches: usize = 0;
    for (listed) |item| matches += @intFromBool(std.mem.eql(u8, item.key, "vopr.review"));
    try std.testing.expectEqual(@as(usize, 1), matches);
    try std.testing.expect(try store.?.delete("vopr.review"));
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "fallback.next.json",
        .data = "{\"secrets\":[{\"key\":\"vopr.review\",\"value\":\"rotated-fallback\",\"created_at_ns\":1,\"updated_at_ns\":2}]}",
    });
    try tmp.dir.rename("fallback.next.json", tmp.dir, "fallback.json", std.testing.io);
    try std.testing.expect(try store.?.refreshIfChanged());
    const rotated = try store.?.getOwnedWithGeneration(alloc, "vopr.review");
    defer alloc.free(rotated.value);
    try std.testing.expectEqualStrings("rotated-fallback", rotated.value);
    try std.testing.expect(rotated.generation != original.generation);
    try std.testing.expectEqualStrings("fallback", original.value);
}

test "secret store archive boundary transports injected cancellation without mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fs.path.join(alloc, &.{ root, "secrets.json" });
    defer alloc.free(path);
    var store: ?*secrets.FileStore = null;
    const status = secret_store_abi_create(&alloc, path.ptr, path.len, &store);
    if (!status.isOk()) return error_abi.errorFromStatus(status);
    defer secret_store_abi_destroy(store.?);
    var written = try store.?.put(alloc, "vopr.review", "original");
    defer written.deinit(alloc);
    const generation = store.?.generationFast();
    secret_store_abi_set_open_fault(1);
    defer secret_store_abi_set_open_fault(0);
    try std.testing.expectError(error.Canceled, store.?.refreshIfChanged());
    try std.testing.expectError(error.Canceled, store.?.refreshIfChangedThrottled(0));
    try std.testing.expectError(error.Canceled, store.?.getOwned(alloc, "vopr.review"));
    try std.testing.expectError(error.Canceled, store.?.getOwnedWithGeneration(alloc, "vopr.review"));
    try std.testing.expectError(error.Canceled, store.?.list(alloc));
    try std.testing.expectError(error.Canceled, store.?.put(alloc, "vopr.review", "uncommitted"));
    try std.testing.expectError(error.Canceled, store.?.delete("vopr.review"));
    try std.testing.expectEqual(generation, store.?.generationFast());
    secret_store_abi_set_open_fault(2);
    try std.testing.expect(!(try store.?.refreshIfChanged()));
    try std.testing.expect(store.?.reloadFailed());
    secret_store_abi_set_open_fault(0);
    const recovered = try store.?.getOwnedWithGeneration(alloc, "vopr.review");
    defer alloc.free(recovered.value);
    try std.testing.expectEqualStrings("original", recovered.value);
    try std.testing.expect(!store.?.reloadFailed());
}
