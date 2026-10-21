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

//! Fail-closed contracts for the private native data plane between static
//! runtime archives. The public control plane remains C-layout-only. Native
//! payloads are permitted solely because every archive is hidden, statically
//! linked, and produced by one target/toolchain invocation; these descriptors
//! prevent an incompatible payload from ever reaching a cast.

const std = @import("std");
const builtin = @import("builtin");

// Cancellation and inference checkpoints carry their owning error-domain
// dispatcher. Reject older native handles before reading the added field.
// Execution leases and recurring handles carry a shared-services capability.
// Reject older native layouts before interpreting those added fields.
// Publisher bindings now carry an optional completion-poll capability.
// Reject the previous binding layout before any native payload is decoded.
// Publisher recovery vtables include a controlled WAL matching callback.
// Reject old vtables before dereferencing their changed callback layout.
// Namespace store/read vtables include original-control snapshot and lookup callbacks.
// Reject older layouts before interpreting these added capabilities.
// Durable outbox callback values carry a copied immutable recovery identity.
// Reject older native callbacks before passing the larger argument layout.
// Namespace stores now expose owned stepped lookups and detached values.
// Reject older namespace vtables before reading the added callback.
// DB recovery cursors now own a funded provider continuation and destructor.
// Reject the prior scalar-only layout before interpreting callback payloads.
// Single-namespace stores now expose caller-owned stepped point reads.
// Reject prior store vtables before interpreting the added callback.
// DB local execution now owns a separate intent capture continuation.
// Reject prior DB layouts before accessing its reader and value owners.
// Store vtables and DB state now include an owned snapshot key cursor.
// Reject prior layouts before interpreting the cursor and its ownership.
// Version 19 adds retained cold-index continuations to owned namespace reads.
// Metadata arrays and retained cursor layout must agree across native modules.
// Version 20 adds progress-aware recovery ports and capture generations.
// Reject earlier native retry/capture layouts before reading those fields.
// Provider recovery progress and retry probes now carry independent tokens.
// Reject previous cursor and progress-return layouts before interpreting them.
// Publication owners now carry a distinct finite-step recovery callback.
// Reject earlier owner/port layouts before interpreting the reserved path.
// DocStore transactions now retain precommit durable publication accounting.
// Reject earlier transaction layouts before crossing the native boundary.
// Publisher ports now return provider-owned, precommit envelope funding.
// Reject older callback and prepared-publication layouts.
// Metadata and physical-maintenance completion retain prepared publication.
// Reject previous retained-owner and publication method contracts.
// Canonical generated-effect type identities no longer depend on physical DB owners.
// Reject earlier native type contracts before interpreting prepared payloads.
// Required preparation now gates transaction commit. Reject native owners
// that do not enforce the durable requirement before resolving intents.
// Transaction resolution carries its accepted decoded-effect owner.
// Sealed frames retain their catalog definitions through resolution.
// Reject older native owners that publish through that durable lease.
// Merged inference callback dispatch and CUDA allocation ownership require
// the combined native contract, beyond either pre-merge owner layout.
// DocStore transactions and catalog publishers carry revision witnesses.
// Reject earlier layouts before reading mutation flags or publication stamps.
// Participant workers carry negotiated preparation and seal callbacks.
// Transaction records retain their original required-preparation contract.
// Replicated BEGIN carries an explicit required-preparation contract.
// Accepted effect seals can atomically publish an ordered apply receipt.
// Required transaction records and native methods retain exact seal bindings.
// CommitControl retains a lease-fenced original participant-plan store.
// Required status evidence carries the saved participant/route binding.
// Typed status callbacks distinguish successful absence from original errors.
// Tokenizer vtables include additive token-byte decoding. Reject older
// native handles before interpreting the added callback slot.
// Owners must enforce prepared replay credits through inventory mutation and HA acknowledgement.
// Backend batches can atomically reserve prepared replay credit with seals.
// Fence merged logical-query layouts and failure/guard contracts.
// Read transcripts distinguish an empty upper bound from clearing the bound.
// Reject prior native observations before interpreting their bound operations.
// Accepted seals retain and account bound observations through recovery.
// Reject owners which can silently drop that durable preparation state.
// Snapshot cursor semantics are explicit when compiling read-conflict gaps.
// Reject older capability layouts before trusting those semantics.
// Semantic range observations own bounds and membership predicates.
// Fence prior native owners before interpreting the expanded read set.
// Read-guard metadata carries an exact actor through native batch dispatch.
// Semantic predicate enforcement adds typed native batch retirement.
// Replay reservation writers retain input contracts through final commit validation.
// Batch preparation can return owned uncommitted bytes and their witnesses.
pub const abi_version: u32 = 62;

pub const zig_compiler_id: u64 = stableId(builtin.zig_version_string);

pub const TypeContract = extern struct {
    version: u32 = abi_version,
    alignment: u32,
    size: u64,
    bit_size: u64,
    type_id: u64,
    layout_id: u64,
    compiler_id: u64 = zig_compiler_id,

    pub fn of(comptime T: type) TypeContract {
        return .{
            .alignment = @alignOf(T),
            .size = @sizeOf(T),
            .bit_size = @sizeOf(T) * 8,
            .type_id = stableId(@typeName(T)),
            .layout_id = shallowLayoutId(T),
        };
    }

    pub fn matches(self: TypeContract, expected: TypeContract) bool {
        return self.version == abi_version and
            self.version == expected.version and
            self.alignment == expected.alignment and
            self.size == expected.size and
            self.bit_size == expected.bit_size and
            self.type_id == expected.type_id and
            self.layout_id == expected.layout_id and
            self.compiler_id == zig_compiler_id and
            self.compiler_id == expected.compiler_id;
    }
};

/// Versioned, error-translating borrow of a host-owned executor. Keep the
/// received adapter at a stable address while its std.Io is in use, never
/// outlive the host's declared lease, and never deinitialize the host executor.
pub const IoBorrow = @import("runtime_io_abi.zig").Borrow;

pub const CallContract = extern struct {
    version: u32 = abi_version,
    _reserved: u32 = 0,
    method_id: u64,
    function: TypeContract,
    arguments: TypeContract,
    output: TypeContract,

    pub fn of(
        comptime method_name: []const u8,
        comptime Function: type,
        comptime Arguments: type,
        comptime Output: type,
    ) CallContract {
        return .{
            .method_id = stableId(method_name),
            .function = .of(Function),
            .arguments = .of(Arguments),
            .output = .of(Output),
        };
    }

    pub fn matches(self: CallContract, expected: CallContract) bool {
        return self.version == abi_version and
            self.version == expected.version and
            self._reserved == 0 and
            self.method_id == expected.method_id and
            self.function.matches(expected.function) and
            self.arguments.matches(expected.arguments) and
            self.output.matches(expected.output);
    }
};

pub fn stableId(comptime name: []const u8) u64 {
    // FNV-1a is deliberately simple and reproducible in every compilation
    // unit. Type size/alignment/bit-size are checked independently.
    var hash: u64 = 0xcbf29ce484222325;
    for (name) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3;
    }
    return hash;
}

/// Fingerprint the immediate representation of a transported type without
/// recursively expanding its full dependency graph. The Zig build cache
/// already keys every archive on transitive source inputs; this contract adds
/// a cheap runtime guard for field order, offsets, tags, calling conventions,
/// and exact compiler identity. Keeping the fingerprint shallow is deliberate:
/// recursive reflection over the large query/storage type graph materially
/// increases analysis and code-generation time in every runtime unit.
pub fn shallowLayoutId(comptime T: type) u64 {
    var hash: u64 = stableId(@tagName(@typeInfo(T)));
    hashInteger(&hash, @sizeOf(T));
    hashInteger(&hash, @alignOf(T));
    hashInteger(&hash, @sizeOf(T) * 8);
    switch (@typeInfo(T)) {
        .@"struct" => |structure| {
            hashBytes(&hash, @tagName(structure.layout));
            inline for (structure.field_names, structure.field_types) |reflected_name, Field| {
                hashBytes(&hash, reflected_name);
                hashBytes(&hash, @typeName(Field));
                hashInteger(&hash, @offsetOf(T, reflected_name));
                hashInteger(&hash, @sizeOf(Field));
                hashInteger(&hash, @alignOf(Field));
            }
        },
        .@"union" => |value_union| {
            hashBytes(&hash, @tagName(value_union.layout));
            if (value_union.tag_type) |Tag| hashBytes(&hash, @typeName(Tag));
            inline for (value_union.field_names, value_union.field_types) |reflected_name, Field| {
                hashBytes(&hash, reflected_name);
                hashBytes(&hash, @typeName(Field));
                hashInteger(&hash, @sizeOf(Field));
                hashInteger(&hash, @alignOf(Field));
            }
        },
        .@"enum" => |value_enum| {
            hashBytes(&hash, @typeName(value_enum.tag_type));
            inline for (value_enum.field_names, value_enum.field_values) |reflected_name, field_value| {
                hashBytes(&hash, reflected_name);
                hashInteger(&hash, field_value);
            }
        },
        .array => |array| {
            hashInteger(&hash, array.len);
            hashBytes(&hash, @typeName(array.child));
        },
        .vector => |vector| {
            hashInteger(&hash, vector.len);
            hashBytes(&hash, @typeName(vector.child));
        },
        .optional => |optional| hashBytes(&hash, @typeName(optional.child)),
        .error_union => |error_union| hashBytes(&hash, @typeName(error_union.payload)),
        .pointer => |pointer| {
            hashBytes(&hash, @tagName(pointer.size));
            hashInteger(&hash, pointer.attrs.@"align" orelse if (pointer.child == anyopaque) 1 else @alignOf(pointer.child));
            hashInteger(&hash, @intFromBool(pointer.attrs.@"const"));
            hashInteger(&hash, @intFromBool(pointer.attrs.@"volatile"));
            hashBytes(&hash, @typeName(pointer.child));
        },
        .@"fn" => |function| {
            hashBytes(&hash, @tagName(function.calling_convention));
            hashInteger(&hash, @intFromBool(function.is_var_args));
            hashBytes(&hash, @typeName(T));
        },
        else => {},
    }
    return hash;
}

fn hashBytes(hash: *u64, bytes: []const u8) void {
    for (bytes) |byte| {
        hash.* ^= byte;
        hash.* *%= 0x100000001b3;
    }
    hash.* ^= 0xff;
    hash.* *%= 0x100000001b3;
}

fn hashInteger(hash: *u64, comptime value: anytype) void {
    var remaining: u64 = @intCast(value);
    inline for (0..8) |_| {
        hash.* ^= @truncate(remaining);
        hash.* *%= 0x100000001b3;
        remaining >>= 8;
    }
}

pub fn assertUniqueMethodIds(comptime VTable: type) void {
    @setEvalBranchQuota(1_000_000);
    const fields = @typeInfo(VTable).@"struct".field_names;
    // Hash once per method. Re-hashing both names for every pair makes
    // large coarse-operation contracts spend their evaluation budget on names.
    const ids = comptime blk: {
        var values: [fields.len]u64 = undefined;
        for (fields, 0..) |name, index| values[index] = stableId(name);
        break :blk values;
    };
    inline for (ids, 0..) |left, left_index| {
        inline for (ids[left_index + 1 ..], left_index + 1..) |right, right_index| {
            if (left == right)
                @compileError("native ABI method-id collision between " ++ fields[left_index] ++ " and " ++ fields[right_index]);
        }
    }
}

test "native type contracts reject layout and identity mismatches" {
    const A = extern struct { value: u64 };
    const B = extern struct { value: i64 };
    const a = TypeContract.of(A);
    try std.testing.expect(a.matches(.of(A)));
    try std.testing.expect(!a.matches(.of(B)));

    var wrong_version = a;
    wrong_version.version += 1;
    try std.testing.expect(!wrong_version.matches(.of(A)));

    var wrong_compiler = a;
    wrong_compiler.compiler_id +%= 1;
    try std.testing.expect(!wrong_compiler.matches(.of(A)));
}

test "native shallow layout fingerprints include field order" {
    const Left = extern struct { first: u32, second: u64 };
    const Right = extern struct { second: u64, first: u32 };
    try std.testing.expect(shallowLayoutId(Left) != shallowLayoutId(Right));
}

test "native opaque pointer contracts preserve explicit alignment" {
    const plain = TypeContract.of(*anyopaque);
    try std.testing.expect(plain.matches(TypeContract.of(*anyopaque)));
    try std.testing.expect(!plain.matches(TypeContract.of(*align(16) anyopaque)));
}

test "native method identifiers are deterministic" {
    try std.testing.expectEqual(stableId("lookup"), stableId("lookup"));
    try std.testing.expect(stableId("lookup") != stableId("scan"));
}

test "native executor borrows validate before reconstructing std.Io" {
    const io = std.testing.io;
    const borrow = IoBorrow.init(&io);
    _ = try borrow.receive();

    var incompatible = borrow;
    incompatible.contract.version += 1;
    try std.testing.expectError(error.InvalidArgument, incompatible.receive());

    var misaligned = borrow;
    misaligned.vtable = @ptrFromInt(1);
    try std.testing.expectError(error.InvalidArgument, misaligned.receive());
}
