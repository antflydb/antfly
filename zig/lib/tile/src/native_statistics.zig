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

//! Ordered shared-memory tree stages. Warp, SIMD and Welford schedules remain
//! separate: selecting this program explicitly retains a halving shared tree.
const std = @import("std");

pub const Tree = struct {
    pub const Operation = enum { sum, maximum };
    pub const Axis = struct { name: []const u8, extent: []const u8, unroll: bool = true };
    pub const Plane = struct {
        partial: []const u8,
        value: []const u8,
        /// Scratch offset and admission are schedule properties. A disabled
        /// plane must not read or publish its unallocated scratch.
        offset: ?[]const u8 = null,
        when: ?[]const u8 = null,
    };
    /// Coupled maximum over a value/index pair. Scan seeds, suppression and
    /// invalid-index admission belong to callers; this tree uses unordered
    /// F32 comparisons and breaks exact ties by the lowest U32 index.
    pub const Indexed = struct {
        indices: []const u8,
        rhs_value: []const u8,
        rhs_index: []const u8,
        lhs_value: ?[]const u8 = null,
        lhs_index: ?[]const u8 = null,
        guard_lane: ?[]const u8 = null,
        neighbor_lane: ?[]const u8 = null,
        initial_shift: []const u8 = "1",
        compact_update: bool = false,

        fn update(comptime self: Indexed, comptime n: Native, comptime values: []const u8) []const u8 {
            const fmt = std.fmt.comptimePrint;
            if ((self.lhs_value == null) != (self.lhs_index == null)) @compileError("indexed tree must load both lhs fields together");
            const next = fmt("{s} + stride", .{self.neighbor_lane orelse n.lane});
            const lhs_value = self.lhs_value orelse fmt("{s}[{s}]", .{ values, n.lane });
            const lhs_index = self.lhs_index orelse fmt("{s}[{s}]", .{ self.indices, n.lane });
            const loads = fmt("            float {s} = {s}[{s}];\n            {s} {s} = {s}[{s}];\n", .{ self.rhs_value, values, next, n.integer, self.rhs_index, self.indices, next }) ++
                (if (self.lhs_value) |v| fmt("            float {s} = {s}[{s}];\n            {s} {s} = {s}[{s}];\n", .{ v, values, n.lane, n.integer, self.lhs_index.?, self.indices, n.lane }) else "");
            const condition = fmt("{s} > {s} || ({s} == {s} && {s} < {s})", .{ self.rhs_value, lhs_value, self.rhs_value, lhs_value, self.rhs_index, lhs_index });
            return loads ++ if (self.compact_update)
                fmt("            if ({s}) {{ {s}[{s}] = {s}; {s}[{s}] = {s}; }}\n", .{ condition, values, n.lane, self.rhs_value, self.indices, n.lane, self.rhs_index })
            else
                fmt("            if ({s}) {{\n                {s}[{s}] = {s};\n                {s}[{s}] = {s};\n            }}\n", .{ condition, values, n.lane, self.rhs_value, self.indices, n.lane, self.rhs_index });
        }
    };
    pub const Native = struct {
        integer: []const u8,
        lane: []const u8,
        threads: []const u8,
        barrier: []const u8,
        planes: []const Plane,
        // CUDA template planes are statically unrolled by the device compiler.
        plane_count: ?[]const u8 = null,
        axes: []const Axis = &.{},
        initialize: bool = true,
        /// Some native phases publish scratch and synchronize before this stage.
        publication_barrier: bool = true,
        maximum_when: ?[]const u8 = null,
        /// Broadcast plane zero into lane-local state before reusing scratch.
        /// The post-read barrier orders every read before the next publication.
        broadcast: ?[]const u8 = null,
        operation: Operation = .sum,
        indexed: ?Indexed = null,
        maximum: []const u8 = "max",
        add: ?[]const u8 = null,
        // Preserve the historical bounded Metal layer-norm tree admission.
        capacity: ?u32 = null,
    };

    pub fn native(comptime n: Native) []const u8 {
        const fmt = std.fmt.comptimePrint;
        if (n.initialize and !n.publication_barrier) @compileError("tree scratch publication requires its barrier");
        if (n.planes.len == 0 or (n.plane_count != null and n.planes.len != 1)) @compileError("tree needs fixed planes or one indexed plane binding");
        if (n.indexed != null and (n.operation != .maximum or n.initialize or n.planes.len != 1 or
            n.plane_count != null or n.axes.len != 0 or n.planes[0].offset != null or n.planes[0].when != null or
            n.maximum_when != null or n.capacity != null or n.broadcast != null))
            @compileError("indexed maximum requires one caller-published value/index plane");
        var init: []const u8 = "";
        var update: []const u8 = "";
        for (n.planes) |p| {
            const lane = if (p.offset) |offset| fmt("{s} + {s}", .{ offset, n.lane }) else n.lane;
            const next = fmt("{s} + stride", .{lane});
            const publish = fmt("    {s}[{s}] = {s};\n", .{ p.partial, lane, p.value });
            const sum = if (n.add) |add| fmt("            {s}[{s}] = {s}({s}[{s}], {s}[{s}]);\n", .{ p.partial, lane, add, p.partial, lane, p.partial, next }) else fmt("            {s}[{s}] += {s}[{s}];\n", .{ p.partial, lane, p.partial, next });
            const maximum = if (n.indexed) |indexed| indexed.update(n, p.partial) else fmt("            {s}[{s}] = {s}({s}[{s}], {s}[{s}]);\n", .{ p.partial, lane, n.maximum, p.partial, lane, p.partial, next });
            const combine = if (n.maximum_when) |condition|
                fmt("            if ({s}) {{\n{s}            }} else {{\n{s}            }}\n", .{ condition, maximum, sum })
            else if (n.operation == .sum) sum else maximum;
            init = init ++ if (p.when) |condition| fmt("    if ({s}) {{\n{s}    }}\n", .{ condition, publish }) else publish;
            update = update ++ if (p.when) |condition| fmt("            if ({s}) {{\n{s}            }}\n", .{ condition, combine }) else combine;
        }
        if (n.plane_count) |count| {
            init = fmt("    #pragma unroll\n    for (unsigned p = 0; p < {s}; ++p) {{\n{s}    }}\n", .{ count, init });
            update = fmt("            #pragma unroll\n            for (unsigned p = 0; p < {s}; ++p) {{\n{s}            }}\n", .{ count, update });
        }
        var axis = n.axes.len;
        while (axis != 0) {
            axis -= 1;
            const a = n.axes[axis];
            init = (if (a.unroll) "    #pragma unroll\n" else "") ++ fmt("    for ({s} {s} = 0; {s} < {s}; ++{s}) {{\n{s}    }}\n", .{ n.integer, a.name, a.name, a.extent, a.name, init });
            update = (if (a.unroll) "            #pragma unroll\n" else "") ++ fmt("            for ({s} {s} = 0; {s} < {s}; ++{s}) {{\n{s}            }}\n", .{ n.integer, a.name, a.name, a.extent, a.name, update });
        }
        const guard = if (n.capacity) |capacity| fmt(" && {s} + stride < {d}u", .{ n.lane, capacity }) else "";
        const initial_shift = if (n.indexed) |indexed| indexed.initial_shift else "1";
        const step = if (n.indexed != null) "1u" else "1";
        const guard_lane = if (n.indexed) |indexed| indexed.guard_lane orelse n.lane else n.lane;
        const stages = (if (n.initialize) init else "") ++ (if (n.publication_barrier) "    " ++ n.barrier ++ "\n" else "") ++ fmt("    for ({s} stride = {s} >> {s}; stride > 0u; stride >>= {s}) {{\n        if ({s} < stride{s}) {{\n{s}        }}\n        {s}\n    }}\n", .{ n.integer, n.threads, initial_shift, step, guard_lane, guard, update, n.barrier });
        const name = n.broadcast orelse return stages;
        if (n.planes.len != 1 or n.plane_count != null or n.axes.len != 0 or n.planes[0].when != null)
            @compileError("broadcast requires one unconditional scratch plane");
        const plane = n.planes[0];
        return stages ++ broadcast(plane.partial ++ "[" ++ (plane.offset orelse "0") ++ "]", name, n.barrier);
    }

    /// A scalar published in shared scratch must be read by every lane before
    /// any lane may reuse that slot. Keep the post-read barrier in the owner.
    pub fn broadcast(comptime value: []const u8, comptime name: []const u8, comptime barrier: []const u8) []const u8 {
        return std.fmt.comptimePrint("    const float {s} = {s};\n    {s}\n", .{ name, value, barrier });
    }

    pub fn indexedBetter(value: f32, index: u32, other: f32, other_index: u32) bool {
        return value > other or (value == other and index < other_index);
    }

    /// Execute the paired tree over caller-owned partials. NaNs do not acquire
    /// an invented total ordering; exact equal values retain the lower index.
    pub fn reduceIndexed(values: []f32, indices: []u32) error{InvalidLayout}!void {
        if (values.len == 0 or !std.math.isPowerOfTwo(values.len) or values.len != indices.len) return error.InvalidLayout;
        var stride = values.len >> 1;
        while (stride != 0) : (stride >>= 1) {
            for (0..stride) |lane| {
                if (indexedBetter(values[lane + stride], indices[lane + stride], values[lane], indices[lane])) {
                    values[lane] = values[lane + stride];
                    indices[lane] = indices[lane + stride];
                }
            }
        }
    }

    /// Execute exactly the native addition tree for already accumulated lane
    /// partials. All planes share the same stages; no reassociation or FMA.
    pub fn reduce(planes: []const []f32) error{InvalidLayout}!void {
        if (planes.len == 0) return error.InvalidLayout;
        const width = planes[0].len;
        if (width == 0 or !std.math.isPowerOfTwo(width)) return error.InvalidLayout;
        for (planes) |plane| if (plane.len != width) return error.InvalidLayout;
        var stride = width >> 1;
        while (stride != 0) : (stride >>= 1) {
            for (0..stride) |lane| for (planes) |plane| {
                plane[lane] += plane[lane + stride];
            };
        }
    }
};

/// Separate entrypoints keep wide-row register allocation independent of the
/// fixed-width warp-finish program. Dispatch uses the same static geometry.
pub const Bf16ScoreSoftmax = struct {
    pub const threads = 256;
    pub const narrow_limit = 512;
    pub fn supportsNarrow(columns: usize, compute_major: i32, compute_minor: i32) bool {
        // GB10 is physically qualified. Other devices retain the original
        // shared-tree entrypoint until their occupancy/timing is qualified.
        return columns != 0 and columns <= narrow_limit and compute_major == 12 and compute_minor == 1;
    }
    pub const header = std.fmt.comptimePrint("#define ANTFLY_BF16_SCORE_SOFTMAX_THREADS {d}u\n#define ANTFLY_BF16_SCORE_SOFTMAX_NARROW_LIMIT {d}u\n", .{ threads, narrow_limit });
};

/// Scalar CUDA shuffle fold. Callers own participation, loop bounds and
/// broadcast. RN addition and generic integer maximum are explicit policies.
pub const CudaShuffle = struct {
    value: []const u8,
    offset: []const u8 = "offset",
    loop: []const u8,
    operation: enum { sum, maximum },
    add: ?[]const u8 = null,
    maximum: []const u8 = "fmaxf",
    xor: bool = true,
    pub fn native(comptime p: CudaShuffle) []const u8 {
        const other = (if (p.xor) "__shfl_xor_sync" else "__shfl_down_sync") ++ "(0xffffffffu," ++ p.value ++ "," ++ p.offset ++ ")";
        return p.loop ++ p.value ++ (switch (p.operation) {
            .sum => if (p.add) |add| "=" ++ add ++ "(" ++ p.value ++ "," ++ other ++ ")" else "+=" ++ other,
            .maximum => "=" ++ p.maximum ++ "(" ++ p.value ++ "," ++ other ++ ")",
        }) ++ ";\n";
    }
};
