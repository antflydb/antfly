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

//! Private canonical-row handoffs. The internal route/session boundary grants
//! origin authority; this descriptor only fences the immutable destination.
const std = @import("std");
const rows = @import("domain_row.zig");
const A = std.mem.Allocator;
pub const Contract = rows.PreparedContract;
pub const Wire = struct { contract: Contract, row_indices: ?[]const u32 = null };

/// One stable contract address shared by every selected row in a batch. Writes
/// borrow it; moving this owning handle never moves the pointed-to contract.
pub const Owned = struct {
    alloc: A,
    contract: *const Contract,

    pub fn create(a: A, contract: Contract) !Owned {
        try contract.validate();
        const pointer = try a.create(Contract);
        pointer.* = contract;
        return .{ .alloc = a, .contract = pointer };
    }
    pub fn deinit(self: *Owned) void {
        self.alloc.destroy(self.contract);
        self.* = undefined;
    }
};

pub fn present(writes: anytype) bool {
    for (writes) |write| if (write.prepared_domain != null) return true;
    return false;
}

/// JSON object insertion must not coalesce keys after ordinal metadata is
/// emitted. Ordinary batches pay no allocation for this private invariant.
pub fn requireUniqueKeys(a: A, writes: anytype) !void {
    if (!present(writes)) return;
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(a);
    for (writes) |write| if ((try keys.getOrPut(a, write.key)).found_existing)
        return error.InvalidBatchRequest;
}

pub fn attach(a: A, writes: anytype, wire: Wire) !Owned {
    if (writes.len == 0) return error.InvalidBatchRequest;
    var owner = try Owned.create(a, wire.contract);
    errdefer owner.deinit();
    if (wire.row_indices) |indices| {
        if (indices.len == 0 or indices.len > writes.len) return error.InvalidBatchRequest;
        var previous: ?u32 = null;
        for (indices) |index| {
            if (index >= writes.len or (previous != null and index <= previous.?)) return error.InvalidBatchRequest;
            previous = index;
            try requireUserKey(writes[index].key);
        }
        for (indices) |index| writes[index].prepared_domain = owner.contract;
    } else {
        for (writes) |write| try requireUserKey(write.key);
        for (writes) |*write| write.prepared_domain = owner.contract;
    }
    return owner;
}

pub fn cloneInto(a: A, input: anytype, output: anytype) !?Owned {
    if (input.len != output.len) return error.InvalidBatchRequest;
    var selected: ?Contract = null;
    for (input) |write| if (write.prepared_domain) |contract| {
        try contract.validate();
        if (selected) |prior| {
            if (!prior.eql(contract.*)) return error.CatalogGenerationChanged;
        } else selected = contract.*;
    };
    const contract = selected orelse return null;
    const owner = try Owned.create(a, contract);
    for (input, output) |source, *destination| destination.prepared_domain = if (source.prepared_domain != null) owner.contract else null;
    return owner;
}

/// No input-size scratch or repeated contracts on the wire. Fanout uses its
/// actual write slice, so subset ordinals cannot refer to a different shard.
pub fn writeSidecar(writer: *std.Io.Writer, writes: anytype) !void {
    if (!present(writes)) return;
    try writer.writeAll(",\"_prepared_domain_rows\":");
    try writeWire(writer, writes);
}

pub fn writeWire(writer: *std.Io.Writer, writes: anytype) !void {
    var contract: ?Contract = null;
    var selected: usize = 0;
    for (writes) |write| if (write.prepared_domain) |value| {
        try value.validate();
        try requireUserKey(write.key);
        if (contract) |prior| {
            if (!prior.eql(value.*)) return error.CatalogGenerationChanged;
        } else contract = value.*;
        selected += 1;
    };
    const value = contract orelse return;
    try writer.print("{{\"contract\":{f}", .{std.json.fmt(value, .{})});
    if (selected != writes.len) {
        try writer.writeAll(",\"row_indices\":[");
        var first = true;
        for (writes, 0..) |write, index| if (write.prepared_domain != null) {
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{d}", .{index});
        };
        try writer.writeByte(']');
    }
    try writer.writeByte('}');
}

fn requireUserKey(key: []const u8) !void {
    if (key.len == 0 or key[0] == 0 or key[0] == 1) return error.InvalidBatchRequest;
}

pub fn preserveFor(validator: anytype, contract: ?*const Contract, preserve: bool) !bool {
    if (contract) |value| {
        const compiled = validator orelse return error.InvalidBatchRequest;
        try compiled.requirePreparedDomain(value.*);
    }
    return preserve or contract != null;
}
