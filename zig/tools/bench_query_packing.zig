//! Complete query-preparation plus leaf-scoring comparison. Synthetic inputs
//! isolate the kernel; service qualification separately uses retained real data.
const std = @import("std");
const vector = @import("antfly_vector");
const Q = vector.quantizer.RaBitQuantizer;
const Packing = vector.quantizer.QueryPacking;
fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.arena.allocator();
    var random_state = std.Random.DefaultPrng.init(0x5252_3233);
    const random = random_state.random();
    for ([_]usize{ 128, 768, 1536 }) |dims| {
        var q = try Q.init(alloc, dims, 42, .cosine);
        defer q.deinit();
        const center = try alloc.alloc(f32, dims);
        const queries = try alloc.alloc(f32, dims * 32);
        for (center) |*v| v.* = random.float(f32) * 0.1;
        for (queries) |*v| v.* = random.float(f32) * 2 - 1;
        var scratch = try Q.EstimateScratch.init(alloc, dims);
        defer scratch.deinit(alloc);
        for ([_]usize{ 1, 128, 512 }) |count| {
            const vectors = try alloc.alloc(f32, count * dims);
            for (vectors) |*v| v.* = random.float(f32) * 2 - 1;
            var set = try q.quantize(center, vectors, count);
            defer set.deinit(alloc);
            const distances = try alloc.alloc(f32, count);
            const bounds = try alloc.alloc(f32, count);
            const expected_d = try alloc.alloc(f32, count);
            const expected_b = try alloc.alloc(f32, count);
            q.query_packing = .lanes;
            try q.estimateDistancesWithScratch(&set, queries[0..dims], expected_d, expected_b, &scratch);
            for (std.enums.values(Packing)) |mode| {
                q.query_packing = mode;
                try q.estimateDistancesWithScratch(&set, queries[0..dims], distances, bounds, &scratch);
                if (!std.mem.eql(u8, std.mem.sliceAsBytes(distances), std.mem.sliceAsBytes(expected_d)) or
                    !std.mem.eql(u8, std.mem.sliceAsBytes(bounds), std.mem.sliceAsBytes(expected_b))) return error.ScoreParityFailure;
            }
            const iterations: usize = 2048;
            for (0..5) |round| {
                const order = if (round % 2 == 0) [_]Packing{ .lanes, .reduce, .mask } else [_]Packing{ .mask, .reduce, .lanes };
                for (order) |mode| {
                    q.query_packing = mode;
                    for (0..64) |i| try q.estimateDistancesWithScratch(&set, queries[(i % 32) * dims ..][0..dims], distances, bounds, &scratch);
                    const started = now(init.io);
                    for (0..iterations) |i| {
                        try q.estimateDistancesWithScratch(&set, queries[(i % 32) * dims ..][0..dims], distances, bounds, &scratch);
                        std.mem.doNotOptimizeAway(distances.ptr);
                        std.mem.doNotOptimizeAway(bounds.ptr);
                    }
                    const elapsed = now(init.io) - started;
                    std.debug.print("packing_bench {{\"dims\":{d},\"rows\":{d},\"round\":{d},\"mode\":\"{s}\",\"iterations\":{d},\"elapsed_ns\":{d}}}\n", .{ dims, count, round, @tagName(mode), iterations, elapsed });
                }
            }
        }
    }
}
