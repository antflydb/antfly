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

//! Bounded private legacy work discovery. A page is one physical read cut;
//! pagination and an empty result never prove global drainage.
const std = @import("std");
const scope = @import("source_scope.zig");
const policy = @import("source_producer_policy.zig");
const Owner = @import("transaction_completion.zig").RoutedOwner;
const A = std.mem.Allocator;
pub const Status = enum(u8) { pending = 0, committed = 1, aborted = 2 };
pub const max_records = 256;
pub const Item = struct {
    transaction_id: [16]u8,
    record_digest: scope.Digest,
    status: Status,
    prepared: bool,
    prepared_known: bool,
    coordinator: bool,
    coordinator_known: bool,
    intents_resolved: bool,
    intents_resolved_known: bool,
};
pub const Page = struct {
    closure: policy.Closure,
    items: []Item,
    scanned_records: usize,
    next_after: ?[16]u8,
    reached_end: bool,
    pub fn deinit(self: *Page, a: A) void {
        a.free(self.items);
        self.* = undefined;
    }
};
pub const Query = struct {
    version: u8,
    owner: Owner,
    after: ?[16]u8,
    max_records: u16,
    minimum_applied_index: u64,
    pub fn validate(self: Query) !void {
        try self.owner.validate();
        if (self.version != 1 or self.max_records == 0 or self.max_records > max_records or self.minimum_applied_index == 0) return error.InvalidSourceAdmission;
    }
};
pub const Observation = struct {
    version: u8,
    owner: Owner,
    cut: scope.Cut,
    page: Page,
    pub fn deinit(self: *Observation, a: A) void {
        self.page.deinit(a);
        self.* = undefined;
    }
    pub fn validate(self: Observation, query: Query) !void {
        try query.validate();
        const closed: policy.Observation = .{ .version = self.version, .owner = self.owner, .cut = self.cut, .closure = self.page.closure };
        try closed.validate(.{ .version = 1, .owner = query.owner, .minimum_applied_index = query.minimum_applied_index });
        const page = self.page;
        if (page.scanned_records > query.max_records or page.items.len > page.scanned_records) return error.SourceAdmissionCorrupt;
        if (page.scanned_records == 0) {
            if (!page.reached_end or !std.meta.eql(page.next_after, query.after)) return error.SourceAdmissionCorrupt;
        } else {
            const last = page.next_after orelse return error.SourceAdmissionCorrupt;
            if (query.after) |after| if (std.mem.order(u8, &last, &after) != .gt) return error.SourceAdmissionCorrupt;
        }
        var previous = query.after;
        for (page.items) |item| {
            if (std.mem.allEqual(u8, &item.record_digest, 0) or (item.status != .pending and item.intents_resolved_known and item.intents_resolved)) return error.SourceAdmissionCorrupt;
            if (previous) |id| if (std.mem.order(u8, &item.transaction_id, &id) != .gt) return error.SourceAdmissionCorrupt;
            const last = page.next_after orelse return error.SourceAdmissionCorrupt;
            if (std.mem.order(u8, &item.transaction_id, &last) == .gt) return error.SourceAdmissionCorrupt;
            previous = item.transaction_id;
        }
    }
};

pub const max_query_bytes = 4096;
pub const max_observation_bytes = 256 * 1024;
fn requireOwner(value: std.json.Value) !void {
    if (value != .object) return error.InvalidSourceAdmission;
    for ([_][]const u8{ "version", "metadata_group_id", "metadata_incarnation", "namespace", "table_binding", "root_generation", "group_id", "topology_epoch" }) |field| if (!value.object.contains(field)) return error.InvalidSourceAdmission;
}
pub fn encodeQuery(a: A, query: Query) ![]u8 {
    try query.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, query, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_query_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn decodeQuery(a: A, bytes: []const u8) !Query {
    if (bytes.len == 0 or bytes.len > max_query_bytes) return error.InvalidSourceAdmission;
    var raw = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer raw.deinit();
    if (raw.value != .object) return error.InvalidSourceAdmission;
    try requireOwner(raw.value.object.get("owner") orelse return error.InvalidSourceAdmission);
    var parsed = std.json.parseFromValue(Query, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeObservation(a: A, query: Query, observation: Observation) ![]u8 {
    try observation.validate(query);
    const bytes = try std.json.Stringify.valueAlloc(a, observation, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_observation_bytes) return error.SourceAdmissionCorrupt;
    return bytes;
}
pub fn decodeObservation(a: A, bytes: []const u8, query: Query) !Observation {
    try query.validate();
    if (bytes.len == 0 or bytes.len > max_observation_bytes) return error.SourceAdmissionCorrupt;
    var raw = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer raw.deinit();
    if (raw.value != .object) return error.SourceAdmissionCorrupt;
    requireOwner(raw.value.object.get("owner") orelse return error.SourceAdmissionCorrupt) catch return error.SourceAdmissionCorrupt;
    const page = raw.value.object.get("page") orelse return error.SourceAdmissionCorrupt;
    if (page != .object) return error.SourceAdmissionCorrupt;
    const closure = page.object.get("closure") orelse return error.SourceAdmissionCorrupt;
    if (closure != .object or !closure.object.contains("version")) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromValue(Observation, a, raw.value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(query);
    var result = parsed.value;
    result.page.items = try a.dupe(Item, parsed.value.page.items);
    return result;
}
