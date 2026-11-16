// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
        maximum: []const u8 = "max",
        add: ?[]const u8 = null,
        // Preserve the historical bounded Metal layer-norm tree admission.
        capacity: ?u32 = null,
    };

    pub fn native(comptime n: Native) []const u8 {
        const fmt = std.fmt.comptimePrint;
        if (n.initialize and !n.publication_barrier) @compileError("tree scratch publication requires its barrier");
        if (n.planes.len == 0 or (n.plane_count != null and n.planes.len != 1)) @compileError("tree needs fixed planes or one indexed plane binding");
        var init: []const u8 = "";
        var update: []const u8 = "";
        for (n.planes) |p| {
            const lane = if (p.offset) |offset| fmt("{s} + {s}", .{ offset, n.lane }) else n.lane;
            const next = fmt("{s} + stride", .{lane});
            const publish = fmt("    {s}[{s}] = {s};\n", .{ p.partial, lane, p.value });
            const sum = if (n.add) |add| fmt("            {s}[{s}] = {s}({s}[{s}], {s}[{s}]);\n", .{ p.partial, lane, add, p.partial, lane, p.partial, next }) else fmt("            {s}[{s}] += {s}[{s}];\n", .{ p.partial, lane, p.partial, next });
            const maximum = fmt("            {s}[{s}] = {s}({s}[{s}], {s}[{s}]);\n", .{ p.partial, lane, n.maximum, p.partial, lane, p.partial, next });
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
        const stages = (if (n.initialize) init else "") ++ (if (n.publication_barrier) "    " ++ n.barrier ++ "\n" else "") ++ fmt("    for ({s} stride = {s} >> 1; stride > 0u; stride >>= 1) {{\n        if ({s} < stride{s}) {{\n{s}        }}\n        {s}\n    }}\n", .{ n.integer, n.threads, n.lane, guard, update, n.barrier });
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
