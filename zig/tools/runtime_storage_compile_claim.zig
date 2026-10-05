//! Evaluate the production reservation rather than parsing its source layout.
const std = @import("std");
const memory = @import("runtime_memory");

pub fn main() !void {
    const linux = try std.zig.system.resolveTargetQuery(std.Io.Threaded.global_single_threaded.io(), .{
        .cpu_arch = .x86_64,
        .cpu_model = .baseline,
        .os_tag = .linux,
        .abi = .gnu,
    });
    var required: usize = 0;
    // CI builds both measured CPU releases and conservative test profiles.
    for ([_]std.builtin.OptimizeMode{ .Debug, .ReleaseFast, .ReleaseSafe }) |optimize| {
        for ([_]bool{ false, true }) |cpu_inference| {
            required = @max(required, memory.runtimeCompileMaxRss(.storage_kernel, .{
                .host = linux,
                .target = linux,
                .optimize = optimize,
                .strip = true,
                .cpu_inference = cpu_inference,
            }));
        }
    }
    std.debug.print("{d}\n", .{required});
}
