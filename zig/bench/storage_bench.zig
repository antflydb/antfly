// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.skip();
    const command = args.next() orelse {
        usage();
        return error.MissingCommand;
    };
    if (std.mem.eql(u8, command, "--help")) {
        usage();
        return;
    }
    if (std.mem.eql(u8, command, "query")) return @import("storage/docid_query_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "analytics")) return @import("storage/db_analytics_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "write")) return @import("storage/docid_write_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "doc-set")) return @import("storage/docid_doc_set_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "summary")) return @import("storage/db_query_summary.zig").run(init, &args);
    if (std.mem.eql(u8, command, "ingest")) return @import("vectors/dense_ingest_guardrail.zig").run(init, &args);
    if (std.mem.eql(u8, command, "provisioned-ingest")) return @import("vectors/provisioned_dense_ingest_guardrail.zig").run(init, &args);
    if (std.mem.eql(u8, command, "hbc-read")) return @import("vectors/hbc_read_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "hbc-write")) return @import("vectors/hbc_write_bench.zig").runBenchmark(init, &args);
    if (std.mem.eql(u8, command, "hbc-split")) return @import("vectors/hbc_split_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "hbc-search")) return @import("vectors/hbc_bench.zig").run(init, &args);
    usage();
    return error.UnknownCommand;
}

fn usage() void {
    std.debug.print("Usage: storage_bench <query|analytics|write|doc-set|summary|ingest|provisioned-ingest|hbc-read|hbc-write|hbc-split|hbc-search> [options]\n", .{});
}
