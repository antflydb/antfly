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

const std = @import("std");
const internal = @import("inference_internal");
const hf = @import("inference_hf_tokenizer");
const data = internal.finetune.embedding_gemma2_data;
const job = internal.finetune.embedding_gemma2_job;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const model_dir = args.next() orelse return usage();
    if (std.mem.eql(u8, model_dir, "--help")) return help();
    const source = args.next() orelse return usage();
    const output = args.next() orelse return usage();
    var options = data.PrepareOptions{};
    while (args.next()) |arg| {
        const value = args.next() orelse return usage();
        if (std.mem.eql(u8, arg, "--query-max-tokens")) options.query_max_tokens = try std.fmt.parseUnsigned(u32, value, 10) else if (std.mem.eql(u8, arg, "--document-max-tokens")) options.document_max_tokens = try std.fmt.parseUnsigned(u32, value, 10) else if (std.mem.eql(u8, arg, "--max-negatives")) options.max_negatives = try std.fmt.parseUnsigned(u32, value, 10) else if (std.mem.eql(u8, arg, "--max-records")) options.max_records = try std.fmt.parseUnsigned(u64, value, 10) else if (std.mem.eql(u8, arg, "--source-repo")) options.source_repo = value else if (std.mem.eql(u8, arg, "--source-revision")) options.source_revision = value else if (std.mem.eql(u8, arg, "--source-manifest")) options.source_manifest_path = value else return usage();
    }
    // The tokenizer and all parser/cache allocations share a reclaiming bound.
    var budget = internal.runtime.bounded_allocator.BoundedAllocator{ .backing = init.gpa, .limit = 8 * 1024 * 1024 * 1024 };
    const a = budget.allocator();
    try job.rejectPathOverlap(a, init.io, output, &.{model_dir});
    const dir = try std.Io.Dir.cwd().openDir(init.io, model_dir, .{});
    defer dir.close(init.io);
    const file = try dir.openFile(init.io, "tokenizer.json", .{});
    defer file.close(init.io);
    const actual = try data.hashFile(init.io, file, 64 * 1024 * 1024);
    if (!std.mem.eql(u8, &actual, &(try data.decodeDigest(job.tokenizer_sha256)))) return error.TrainingTokenizerIdentityMismatch;
    const tokenizer = try hf.HfTokenizer.loadFromDir(a, dir, init.io, "tokenizer.json");
    defer tokenizer.tokenizer().deinitTokenizer();
    try data.prepare(a, init.io, source, output, .{ .context = tokenizer, .encode = encode }, options);
    std.debug.print("Prepared authenticated EmbeddingGemma 2 retrieval shards: {s}\n", .{output});
}

fn encode(context: *anyopaque, a: std.mem.Allocator, text: []const u8, maximum: usize) !data.Encoding {
    const tokenizer: *hf.HfTokenizer = @ptrCast(@alignCast(context));
    // encode() returns unwrapped tokens. Wrap once using the fixed executable
    // EG2 processor contract; truncate content while retaining BOS and EOS.
    const raw = try tokenizer.tokenizer().encode(a, text);
    defer a.free(raw);
    if (maximum < 2) return error.InvalidPreparedTokens;
    const count = @min(raw.len, maximum - 2);
    const ids = try a.alloc(i32, count + 2);
    ids[0] = 2;
    @memcpy(ids[1..][0..count], raw[0..count]);
    ids[ids.len - 1] = 1;
    return .{ .ids = ids, .truncated = count != raw.len };
}

fn help() void {
    std.debug.print(
        \\usage: prepare-embeddinggemma2-lora-inputs <model_dir> <retrieval.jsonl> <new_output_dir> [options]
        \\Options: --query-max-tokens 128 --document-max-tokens 512 --max-negatives 4
        \\         --max-records 1000000 --source-repo ID --source-revision COMMIT
        \\         --source-manifest dataset_manifest.json
        \\JSONL: query, positive:{{id,text,title?}}, negatives:[{{id,text}}], query_id?,
        \\       known_positive_ids?, task:retrieval|code_retrieval, split:train|validation|test, group_id?
        \\
    , .{});
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}
