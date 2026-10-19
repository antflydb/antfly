// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

const std = @import("std");
pub const antfly_sources = @import("source_owner_embedded_db.zig");
const config = @import("antfly_local_sources").common_config;

test "config runnable scheduler preserves canonical read isolation overrides" {
    inline for (.{
        .{ "{\"admission\":{\"runnable\":{\"capacity_mode\":\"automatic\",\"read_isolation\":false}}}", @as(?bool, false) },
        .{ "{\"admission\":{\"runnable\":{\"slots\":4,\"cleanup_slots\":1,\"read_isolation\":true}}}", @as(?bool, true) },
        .{ "{\"admission\":{\"runnable\":{\"read_isolation\":null}}}", @as(?bool, null) },
    }) |case| {
        var parsed = try config.Config.parseFromSlice(std.testing.allocator, case[0]);
        defer parsed.deinit();
        try std.testing.expectEqual(case[1], parsed.admission.runnable.?.read_isolation);
        const effective = try parsed.admission.resolveForStartup(64 << 20);
        try std.testing.expectEqual(case[1] orelse false, effective.runnable.?.readIsolation());
    }
}
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const openapi = @import("antfly_common_openapi");

test "config runnable scheduler defaults share finite capacity and preserve explicit rollback" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "{}", "{\"admission\":{}}", "{\"admission\":{\"query\":{\"max_concurrent_requests\":0},\"write\":{\"max_concurrent_requests\":0},\"inference\":{\"max_concurrent_requests\":0}}}" }) |raw| {
        var parsed = try config.Config.parseFromSlice(a, raw);
        defer parsed.deinit();
        try std.testing.expectEqualDeep(config.default_runnable_policy, parsed.admission.runnable.?);
        try std.testing.expectEqualDeep(config.default_query_waiting, parsed.admission.query.waiting);
        const effective = try parsed.admission.resolveForStartup(64 << 20);
        try std.testing.expectEqual(@as(u64, 1 << 20), effective.runnable.?.control_bytes);
        try std.testing.expect(effective.runnable.?.slots >= 3 and effective.runnable.?.slots <= 8);
        try std.testing.expectEqual(@as(usize, 1), effective.runnable.?.cleanup_slots);
        try std.testing.expect(effective.max_request_memory_bytes <= 8 << 20);
        try std.testing.expect(effective.max_retained_memory_bytes <= 8 << 20);
        try std.testing.expectEqual(parsed.admission.query.max_concurrent_requests, effective.query.max_concurrent_requests);
        try std.testing.expectEqual(parsed.admission.write.max_concurrent_requests, effective.write.max_concurrent_requests);
        try std.testing.expectEqual(parsed.admission.inference.max_concurrent_requests, effective.inference.max_concurrent_requests);
        const encoded = try std.json.Stringify.valueAlloc(a, parsed.admission, .{});
        defer a.free(encoded);
        const body = try std.fmt.allocPrint(a, "{{\"admission\":{s}}}", .{encoded});
        defer a.free(body);
        var reopened = try config.Config.parseFromSlice(a, body);
        defer reopened.deinit();
        try std.testing.expectEqualDeep(parsed.admission, reopened.admission);
    }
    var disabled = try config.Config.parseFromSlice(a, "{\"admission\":{\"runnable\":null}}");
    defer disabled.deinit();
    try std.testing.expect(disabled.admission.runnable == null);
    try std.testing.expectEqual(@as(u32, 0), disabled.admission.query.waiting.max_wait_ms);
    var fail_fast = try config.Config.parseFromSlice(a, "{\"admission\":{\"query\":{\"waiting\":{}}}}");
    defer fail_fast.deinit();
    try std.testing.expect(fail_fast.admission.runnable != null);
    try std.testing.expectEqual(@as(u32, 0), fail_fast.admission.query.waiting.max_wait_ms);
    var explicit = try config.Config.parseFromSlice(a, "{\"admission\":{\"runnable\":null,\"query\":{\"waiting\":{\"max_queued_requests\":1,\"max_queued_bytes\":4096,\"max_wait_ms\":25}}}}");
    defer explicit.deinit();
    try std.testing.expect(explicit.admission.runnable == null);
    try std.testing.expectEqual(@as(u32, 25), explicit.admission.query.waiting.max_wait_ms);
    var fixed = try config.Config.parseFromSlice(a, "{\"admission\":{\"runnable\":{\"capacity_mode\":\"fixed\",\"slots\":4},\"query\":{\"max_concurrent_requests\":5,\"waiting\":{}}}}");
    defer fixed.deinit();
    const frozen = try fixed.admission.resolveForStartup(1);
    try std.testing.expectEqualDeep(fixed.admission, frozen);
    try std.testing.expectEqual(@as(u32, 5), frozen.query.max_concurrent_requests);
}

test "config runnable scheduler public DTO roundtrip preserves managed startup capacity" {
    const allocator = std.testing.allocator;
    const dto: openapi.RunnableSchedulingConfigValue = .{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 4 };
    const body = try std.json.Stringify.valueAlloc(allocator, .{ .admission = .{ .runnable = dto } }, .{});
    defer allocator.free(body);
    var parsed = try config.Config.parseFromSlice(allocator, body);
    defer parsed.deinit();
    var scheduler = try runnable.Scheduler.init(parsed.admission.runnable.?);
    defer scheduler.close();
    try scheduler.ensureCleanupCpuBeforePublication();
    const policy = scheduler.policyAndStats().config;
    try std.testing.expectEqual(@as(usize, 3), policy.slots);
    try std.testing.expectEqual(@as(usize, 1), policy.cleanup_slots);
    try std.testing.expectEqual(@as(usize, 1), policy.ordinarySlots());
    var implicit = try config.Config.parseFromSlice(allocator,
        \\{"admission":{"runnable":{"slots":3,"max_operations":4}}}
    );
    defer implicit.deinit();
    var implicit_scheduler = try runnable.Scheduler.init(implicit.admission.runnable.?);
    defer implicit_scheduler.close();
    try implicit_scheduler.ensureCleanupCpuBeforePublication();
    try std.testing.expectEqualDeep(policy, implicit_scheduler.policyAndStats().config);
    for ([_][]const u8{
        \\{"admission":{"runnable":{"slots":2}}}
        ,
        \\{"admission":{"runnable":{"slots":3,"control_slots":2}}}
        ,
        \\{"admission":{"runnable":{"cleanup_slots":2}}}
        ,
        \\{"admission":{"runnable":{"cleanup_slots":-1}}}
        ,
        \\{"admission":{"runnable":{"max_operations":2}}}
        ,
        \\{"admission":{"runnable":{"max_operations":3}}}
        ,
    }) |invalid| try std.testing.expectError(error.InvalidConfig, config.Config.parseFromSlice(allocator, invalid));
    var disabled = try config.Config.parseFromSlice(allocator, "{\"admission\":{\"runnable\":null}}");
    defer disabled.deinit();
    try std.testing.expect(disabled.admission.runnable == null);
}

test "config runnable scheduler automatic sizing resolves once with protected capacity" {
    const platform = @import("antfly_platform");
    const dto: openapi.RunnableSchedulingConfigValue = .{ .capacity_mode = "automatic", .slots = 16 };
    const dto_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .admission = .{ .runnable = dto } }, .{});
    defer std.testing.allocator.free(dto_body);
    var parsed = try config.Config.parseFromSlice(std.testing.allocator, dto_body);
    defer parsed.deinit();
    var scheduler = try runnable.Scheduler.init(parsed.admission.runnable.?);
    defer scheduler.close();
    try scheduler.ensureCleanupCpuBeforePublication();
    const expected = try parsed.admission.runnable.?.resolveCapacity(platform.process_memory.cpuCapacity(), platform.process_memory.systemEnvelope());
    try std.testing.expectEqualDeep(expected, scheduler.policyAndStats().config);
    try std.testing.expect(scheduler.config.slots >= 3 and scheduler.config.slots <= 16);
    try std.testing.expectEqual(@as(usize, 1), scheduler.config.cleanup_slots);
    const body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .admission = .{ .runnable = parsed.admission.runnable.? } }, .{});
    defer std.testing.allocator.free(body);
    var reopened = try config.Config.parseFromSlice(std.testing.allocator, body);
    defer reopened.deinit();
    try std.testing.expectEqualDeep(parsed.admission.runnable.?, reopened.admission.runnable.?);
    try std.testing.expectError(error.InvalidConfig, config.Config.parseFromSlice(std.testing.allocator,
        \\{"admission":{"runnable":{"capacity_mode":"adaptive"}}}
    ));
}

test "config runnable scheduler startup process memory clamp freezes original policy" {
    const requested: runnable.Config = .{ .capacity_mode = .automatic, .slots = 16 };
    const effective = try requested.resolveForStartup(64 << 20);
    try std.testing.expect(effective.max_bytes <= 2 << 20);
    try std.testing.expectEqual(@as(usize, 1), effective.cleanup_slots);
    try std.testing.expectEqual((runnable.Config{}).capacity_mode, effective.capacity_mode);
    // An already published policy is never re-probed or shrunk by a consumer.
    try std.testing.expectEqualDeep(effective, try effective.resolveForStartup(1));
    try std.testing.expectEqualDeep(effective, try effective.resolveCapacity(.{ .millicpus = 250 }, .{ .limit_bytes = 1 }));
    var owner = try runnable.NodeOwner.init(effective);
    defer owner.deinit();
    try std.testing.expectEqualDeep(effective, owner.scheduler.policyAndStats().config);
    try owner.scheduler.ensureCleanupCpuBeforePublication();
    try std.testing.expectEqualDeep(effective, owner.scheduler.policyAndStats().config);
    try std.testing.expectError(error.InvalidRunnablePolicy, requested.resolveForStartup(1 << 20));
    const fixed: runnable.Config = .{};
    try std.testing.expectEqualDeep(fixed, try fixed.resolveForStartup(1));
}

test "config runnable scheduler automatic admission shares process envelope across frontend pools" {
    const requested: config.Config.AdmissionConfig = .{ .runnable = .{ .capacity_mode = .automatic } };
    const effective = try requested.resolveForStartup(64 << 20);
    try std.testing.expect(effective.max_request_memory_bytes <= 8 << 20);
    try std.testing.expect(effective.max_retained_memory_bytes <= 8 << 20);
    try std.testing.expect(effective.runnable.?.max_bytes <= 2 << 20);
    try std.testing.expectEqualDeep(requested.query, effective.query);
    try std.testing.expectEqualDeep(requested.write, effective.write);
    try std.testing.expectEqualDeep(requested.inference, effective.inference);
    try std.testing.expectEqualDeep(effective, try effective.resolveForStartup(1));
    var smaller = requested;
    smaller.max_request_memory_bytes = 2 << 20;
    smaller.max_retained_memory_bytes = 1 << 20;
    const capped = try smaller.resolveForStartup(64 << 20);
    try std.testing.expectEqual(smaller.max_request_memory_bytes, capped.max_request_memory_bytes);
    try std.testing.expectEqual(smaller.max_retained_memory_bytes, capped.max_retained_memory_bytes);
    var fixed = requested;
    fixed.runnable.?.capacity_mode = .fixed;
    try std.testing.expectEqualDeep(fixed, try fixed.resolveForStartup(1));
    const disabled: config.Config.AdmissionConfig = .{ .runnable = null };
    try std.testing.expectEqualDeep(disabled, try disabled.resolveForStartup(1));
    try std.testing.expectError(error.InvalidConfig, requested.resolveForStartup(4 << 20));
}

test "config runnable scheduler automatic startup preserves service control under shared exhaustion" {
    const Manager = @import("antfly_local_sources").storage_resource_manager.ResourceManager;
    const memory = @import("common/allocation_budget.zig");
    const requested: config.Config.AdmissionConfig = .{ .runnable = .{ .capacity_mode = .automatic } };
    const effective = try requested.resolveForStartup(64 << 20);
    var manager = Manager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 << 20 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    const Callbacks = struct {
        fn reserve(raw: *anyopaque, bytes: usize) callconv(.c) bool {
            return @as(*Manager, @ptrCast(@alignCast(raw))).reserveExternal(bytes);
        }
        fn release(raw: *anyopaque, bytes: usize) callconv(.c) void {
            @as(*Manager, @ptrCast(@alignCast(raw))).releaseExternal(bytes);
        }
    };
    {
        var service = try memory.ServiceMemory.init(std.testing.allocator, .{ .ptr = &manager, .reserve_fn = Callbacks.reserve, .release_fn = Callbacks.release }, @intCast(@min(64 << 20, effective.max_retained_memory_bytes / 4)), @intCast(@min(4 << 20, effective.max_request_memory_bytes / 8)));
        defer service.deinit();
        try std.testing.expect(manager.snapshot().memory.used_bytes > 0);
        const pressure: usize = @intCast((64 << 20) - manager.snapshot().memory.used_bytes);
        try std.testing.expect(manager.reserveExternal(pressure));
        defer manager.releaseExternal(pressure);
        try std.testing.expectError(error.OutOfMemory, service.memory.allocator(false).alloc(u8, 1));
        const control = try service.memory.allocator(true).alloc(u8, 1024);
        defer service.memory.allocator(true).free(control);
        try std.testing.expectEqual(@as(u64, 64 << 20), manager.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(usize, 0), service.memory.foreground.used.load(.acquire));
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "config runnable scheduler public DTO retains elastic CPU reserve and hard floor" {
    const a = std.testing.allocator;
    const dto: openapi.RunnableSchedulingConfigValue = .{ .slots = 5, .cleanup_slots = 1, .read_isolation = .{ .value = true }, .bounded_read_slots = 2 };
    const body = try std.json.Stringify.valueAlloc(a, .{ .admission = .{ .runnable = dto } }, .{});
    defer a.free(body);
    var parsed = try config.Config.parseFromSlice(a, body);
    defer parsed.deinit();
    const policy = parsed.admission.runnable.?;
    try std.testing.expectEqual(@as(usize, 2), policy.boundedReadSlots());
    try std.testing.expectEqual(@as(usize, 1), policy.boundedReadFloorSlots());
    try std.testing.expectEqual(@as(usize, 1), policy.cpuBorrowSlots());
    try policy.validateManagedCleanupCapacity();
    const dto_body = try std.json.Stringify.valueAlloc(a, dto, .{});
    defer a.free(dto_body);
    const restored = try std.json.parseFromSlice(openapi.RunnableSchedulingConfigValue, a, dto_body, .{});
    defer restored.deinit();
    try std.testing.expectEqual(dto.bounded_read_slots, restored.value.bounded_read_slots);
}
