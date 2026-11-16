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

//! Consumer-side coverage of the production adapter and separately compiled
//! storage owner. Fake publishers inject acquisition errors, never replace
//! Adapter.probe / Adapter.acquireIfChanged or the owner cache implementation.
pub const antfly_sources = @import("source_owner_control.zig");
const std = @import("std");
const abi = @import("kernel_owner_abi");
const client = @import("storage/kernel_owner_client.zig");
const scraping = @import("antfly_scraping");
const config_mod = @import("antfly_local_sources").common_config;
const remote_runtime = @import("common/remote_content_runtime.zig");
const Snapshot = scraping.RemoteContentConfig.Snapshot;
extern fn remote_content_owner_test_acquire(?*anyopaque, *Snapshot) callconv(.c) abi.Status;

const original =
    \\{"remote_content":{"security":{"max_download_size_bytes":1024},"default_s3":"r2","s3":{"r2":{"endpoint":"account.r2.cloudflarestorage.com","use_ssl":true,"buckets":["media"],"access_key_id":"${secret:access}","secret_access_key":"${secret:key}"}},"http":{"github":{"base_url":"https://api.github.com/repos/acme/","headers":{"Authorization":"${secret:token}"},"security":{"allowed_hosts":["api.github.com"]}}}}}
;

fn acquire(context: *const client.Context) !Snapshot {
    var held: Snapshot = undefined;
    const status = remote_content_owner_test_acquire(context.handle, &held);
    try std.testing.expectEqual(abi.Status.ok, status);
    return held;
}

fn expectCredentials(held: Snapshot) !void {
    try std.testing.expectEqualStrings("r2", held.config.default_s3.?);
    const s3 = held.config.getS3("r2").?;
    try std.testing.expectEqualStrings("account.r2.cloudflarestorage.com", s3.endpoint.?);
    try std.testing.expectEqual(@as(?bool, true), s3.use_ssl);
    try std.testing.expectEqualStrings("media", s3.buckets.?[0]);
    try std.testing.expectEqualStrings("${secret:access}", s3.access_key_id.?);
    try std.testing.expectEqualStrings("${secret:key}", s3.secret_access_key.?);
    const http = held.config.getHttp("github").?;
    try std.testing.expectEqualStrings("https://api.github.com/repos/acme/", http.base_url.?);
    try std.testing.expectEqualStrings("${secret:token}", http.headers.get("Authorization").?);
    try std.testing.expectEqualStrings("api.github.com", http.security.?.allowed_hosts.?[0]);
}

test "remote content production adapter bridges file publications and retains leases across archives" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = original });
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/config.json", .{tmp.sub_path});
    defer alloc.free(path);
    var runtime = try remote_runtime.Runtime.initWithIo(alloc, io, path, null, null);
    defer runtime.deinit();
    var facade: scraping.RemoteContentConfig = .{};
    runtime.attach(&facade);
    var context: client.Context = .{};
    defer context.deinit();
    try context.configureRemoteContent(&facade);
    var before = try acquire(&context);
    defer before.deinit();
    try expectCredentials(before);
    for (0..1000) |_| {
        var held = try acquire(&context);
        defer held.deinit();
        try std.testing.expectEqual(before.config, held.config);
    }
    try tmp.dir.writeFile(io, .{ .sub_path = "next.json", .data = "{\"remote_content\":{\"security\":{\"max_download_size_bytes\":2048}}}" });
    try tmp.dir.rename("next.json", tmp.dir, "config.json", io);
    try std.testing.expect(runtime.refreshIfChanged());
    var after = try acquire(&context);
    defer after.deinit();
    try std.testing.expectEqual(@as(?u64, 2048), after.config.security.?.max_download_size_bytes);
    try std.testing.expect(after.config.getS3("r2") == null);
    try std.testing.expect(after.config.getHttp("github") == null);
    try std.testing.expectEqual(@as(?u64, 1024), before.config.security.?.max_download_size_bytes);
    try expectCredentials(before);
    context.deinit();
    // Parsed snapshots and their release functions belong to the provider;
    // leases remain valid after the context drops its cache reference.
    try expectCredentials(before);
    try std.testing.expectEqual(@as(?u64, 2048), after.config.security.?.max_download_size_bytes);
}

test "remote content production adapter propagates payload failures after a successful probe" {
    const Publisher = struct {
        config: *const scraping.RemoteContentConfig,
        calls: usize = 0,
        fail_at: usize = 0,
        failure: scraping.RemoteContentConfig.RemoteContentRuntime.AcquireStatus = .timeout,
        revision: u8 = 1,
        fn acquire(raw: *anyopaque, out: *Snapshot) callconv(.c) scraping.RemoteContentConfig.RemoteContentRuntime.AcquireStatus {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return self.failure;
            out.* = .{ .config = self.config, .revision = @splat(self.revision) };
            return .ok;
        }
    };
    var config = try config_mod.Config.parseFromSlice(std.testing.allocator, original);
    defer config.deinit();
    var publisher = Publisher{ .config = &config.remote_content.? };
    var facade = scraping.RemoteContentConfig{ .runtime = .{ .context = &publisher, .acquire_fn = Publisher.acquire } };
    var context: client.Context = .{};
    defer context.deinit();
    try context.configureRemoteContent(&facade);
    var before = try acquire(&context);
    defer before.deinit();
    const cases = .{
        .{ .out_of_memory, abi.Status.out_of_memory },
        .{ .timeout, abi.Status.timeout },
        .{ .canceled, abi.Status.cancelled },
        .{ .unavailable, abi.Status.resource_temporarily_unavailable },
    };
    inline for (cases) |case| {
        publisher.revision += 1;
        publisher.failure = case[0];
        publisher.fail_at = publisher.calls + 2;
        const expected_calls = publisher.fail_at;
        var failed: Snapshot = undefined;
        try std.testing.expectEqual(case[1], remote_content_owner_test_acquire(context.handle, &failed));
        try std.testing.expectEqual(expected_calls, publisher.calls);
        // The probe succeeded; the next acquisition failed inside the real
        // payload callback. A failed build must neither serve the cache nor
        // poison the revision so that a subsequent retry skips rebuilding.
        try expectCredentials(before);
        publisher.fail_at = 0;
        const calls_before_retry = publisher.calls;
        var recovered = try acquire(&context);
        defer recovered.deinit();
        try std.testing.expectEqual(calls_before_retry + 2, publisher.calls);
        try std.testing.expectEqual(@as([32]u8, @splat(publisher.revision)), recovered.revision.?);
        try expectCredentials(recovered);
        const calls = publisher.calls;
        var warm = try acquire(&context);
        defer warm.deinit();
        try std.testing.expectEqual(recovered.config, warm.config);
        try std.testing.expectEqual(calls + 1, publisher.calls);
    }
}

test "remote content production adapter detects mutable config edits without revision identities" {
    var config = try config_mod.Config.parseFromSlice(std.testing.allocator, original);
    defer config.deinit();
    try std.testing.expect(config.remote_content.?.runtime == null);
    var context: client.Context = .{};
    defer context.deinit();
    try context.configureRemoteContent(&config.remote_content.?);
    var before = try acquire(&context);
    defer before.deinit();
    var same = try acquire(&context);
    defer same.deinit();
    try std.testing.expectEqual(before.config, same.config);
    config.remote_content.?.security.?.max_download_size_bytes = 2048;
    var changed = try acquire(&context);
    defer changed.deinit();
    try std.testing.expect(before.config != changed.config);
    try std.testing.expectEqual(@as(?u64, 1024), before.config.security.?.max_download_size_bytes);
    try std.testing.expectEqual(@as(?u64, 2048), changed.config.security.?.max_download_size_bytes);
    try expectCredentials(changed);
}
