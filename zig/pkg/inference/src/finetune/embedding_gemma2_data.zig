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

//! Bounded, indexed retrieval data. Records are independently authenticated;
//! neither preparation nor training reads a complete dataset into memory.
const std = @import("std");
const fs_support = @import("embedding_gemma2_fs.zig");
const job = @import("embedding_gemma2_job.zig");
const unicode = @import("gliner2_unicode_tables.zig");
const casefold = @import("embedding_gemma2_casefold_data.zig");
const Sha256 = std.crypto.hash.sha2.Sha256;
const index_magic = "EG2IDX1\x00";
const index_header_bytes = 16;
const index_entry_bytes = 56;
pub const max_record_bytes = 1024 * 1024;
// Source inventories include query/content hashes and mined negative pairs.
// They can exceed the much smaller prepared shard manifest at ordinary scale.
pub const max_source_manifest_bytes = 64 * 1024 * 1024;
pub const Split = enum(u8) { train, validation, @"test" };
pub const Task = enum { retrieval, code_retrieval };

pub const Document = struct {
    id: []const u8 = "",
    text: []const u8,
    title: ?[]const u8 = null,
};
pub const RawRecord = struct {
    query_id: []const u8 = "",
    query: []const u8,
    positive: Document,
    negatives: []const Document = &.{},
    known_positive_ids: []const []const u8 = &.{},
    task: Task = .retrieval,
    split: Split = .train,
    group_id: []const u8 = "",
};
pub const EncodedDocument = struct {
    id: []const u8,
    content_sha256: []const u8,
    input_ids: []const i32,
};
pub const Record = struct {
    query_id: []const u8,
    query_content_sha256: []const u8,
    query_ids: []const i32,
    positive: EncodedDocument,
    negatives: []const EncodedDocument,
    known_positive_ids: []const []const u8,
    task: Task,
    split: Split,
    group_id: []const u8,
};
pub const Encoding = struct { ids: []const i32, truncated: bool };
pub const Encoder = struct {
    context: *anyopaque,
    encode: *const fn (*anyopaque, std.mem.Allocator, []const u8, usize) anyerror!Encoding,
};
pub const PrepareOptions = struct {
    query_max_tokens: u32 = 128,
    document_max_tokens: u32 = 512,
    max_negatives: u32 = 4,
    max_records: u64 = 1_000_000,
    shard_bytes: u64 = 64 * 1024 * 1024,
    source_repo: []const u8 = "local",
    source_revision: []const u8 = "",
    source_manifest_path: ?[]const u8 = null,
};
pub const Shard = struct { name: []const u8, bytes: u64, sha256: []const u8 };
pub const Manifest = struct {
    version: u32 = 1,
    format: []const u8 = "antfly.embeddinggemma2.retrieval-shards/v1",
    records: u64,
    train_records: u64,
    validation_records: u64,
    test_records: u64,
    truncated_sequences: u64,
    query_max_tokens: u32,
    document_max_tokens: u32,
    max_negatives: u32,
    tokenizer_sha256: []const u8 = job.tokenizer_sha256,
    source_sha256: []const u8,
    source_repo: []const u8,
    source_revision: []const u8,
    source_manifest_sha256: ?[]const u8 = null,
    preprocessing_sha256: []const u8,
    index_sha256: []const u8,
    shards: []const Shard,
};

pub fn digestHex(allocator: std.mem.Allocator, digest: [32]u8) ![]const u8 {
    return allocator.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
pub fn sha256(bytes: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    Sha256.hash(bytes, &digest, .{});
    return digest;
}
pub fn decodeDigest(value: []const u8) ![32]u8 {
    if (value.len != 64) return error.InvalidSha256;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, value) catch return error.InvalidSha256;
    return result;
}
pub fn hashFile(io: std.Io, file: std.Io.File, maximum: u64) ![32]u8 {
    const stat = try file.stat(io);
    if (stat.kind != .file or stat.size > maximum) return error.InvalidDatasetFile;
    var buffer: [64 * 1024]u8 = undefined;
    var hash = Sha256.init(.{});
    var offset: u64 = 0;
    while (offset < stat.size) {
        const n = try file.readPositional(io, &.{&buffer}, offset);
        if (n == 0) return error.UnexpectedEndOfDataset;
        hash.update(buffer[0..n]);
        offset += n;
    }
    return hash.finalResult();
}

fn readSourceManifest(a: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8) ![]u8 {
    const file = try dir.openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.InvalidSourceManifest;
    if (stat.size > max_source_manifest_bytes) return error.SourceManifestTooLarge;
    // readFileAlloc's limit is exclusive. Retain a bound if the file grows
    // between the size check and the read.
    return dir.readFileAlloc(io, path, a, .limited(max_source_manifest_bytes + 1)) catch |err| switch (err) {
        error.StreamTooLong => error.SourceManifestTooLarge,
        else => return err,
    };
}

const SourceInventory = struct {
    allocator: std.mem.Allocator,
    parsed: std.json.Parsed(std.json.Value),
    split_hashes: [3]std.AutoHashMap([32]u8, void),
    observed_hashes: [3]std.AutoHashMap([32]u8, void),
    corpus_ids: std.AutoHashMap([32]u8, void),
    observed_corpus_ids: std.AutoHashMap([32]u8, void),
    positive_pairs: std.AutoHashMap([32]u8, void),
    observed_positive_pairs: std.AutoHashMap([32]u8, void),
    negative_pairs: std.AutoHashMap([32]u8, void),
    observed_negative_pairs: std.AutoHashMap([32]u8, void),

    fn deinit(self: *SourceInventory) void {
        for (&self.split_hashes) |*map| map.deinit();
        for (&self.observed_hashes) |*map| map.deinit();
        self.corpus_ids.deinit();
        self.observed_corpus_ids.deinit();
        self.positive_pairs.deinit();
        self.observed_positive_pairs.deinit();
        self.negative_pairs.deinit();
        self.observed_negative_pairs.deinit();
        self.parsed.deinit();
        self.* = undefined;
    }

    fn observe(self: *SourceInventory, raw: RawRecord) !void {
        const split_index: usize = @backingInt(raw.split);
        const query_hash = try normalizedContentDigest(raw.query);
        const positive_hash = try normalizedContentDigest(raw.positive.text);
        for ([_][32]u8{ query_hash, positive_hash }) |digest| {
            if (!self.split_hashes[split_index].contains(digest)) return error.SourceManifestContentMismatch;
            try self.observed_hashes[split_index].put(digest, {});
        }
        if (raw.split != .train) return;
        const positive_id = sha256(raw.positive.id);
        if (!self.corpus_ids.contains(positive_id)) return error.SourceManifestCorpusMismatch;
        try self.observed_corpus_ids.put(positive_id, {});
        const positive_pair = pairDigest(raw.query_id, raw.positive.id);
        if (!self.positive_pairs.contains(positive_pair)) return error.SourceManifestPositiveMismatch;
        try self.observed_positive_pairs.put(positive_pair, {});
        for (raw.negatives) |negative| {
            const negative_id = sha256(negative.id);
            if (!self.corpus_ids.contains(negative_id)) return error.SourceManifestCorpusMismatch;
            try self.observed_corpus_ids.put(negative_id, {});
            const negative_pair = pairDigest(raw.query_id, negative.id);
            if (!self.negative_pairs.contains(negative_pair)) return error.SourceManifestNegativeMismatch;
            try self.observed_negative_pairs.put(negative_pair, {});
        }
    }

    fn validateComplete(self: *const SourceInventory) !void {
        for (self.split_hashes, self.observed_hashes) |expected, observed| {
            if (expected.count() != observed.count()) return error.SourceManifestContentMismatch;
        }
        if (self.corpus_ids.count() != self.observed_corpus_ids.count()) return error.SourceManifestCorpusMismatch;
        if (self.positive_pairs.count() != self.observed_positive_pairs.count()) return error.SourceManifestPositiveMismatch;
        if (self.negative_pairs.count() != self.observed_negative_pairs.count()) return error.SourceManifestNegativeMismatch;
    }
};

fn pairDigest(lhs: []const u8, rhs: []const u8) [32]u8 {
    var hash = Sha256.init(.{});
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, lhs.len, .little);
    hash.update(&length);
    hash.update(lhs);
    hash.update(rhs);
    return hash.finalResult();
}

fn normalizedContentDigest(text: []const u8) ![32]u8 {
    const view = std.unicode.Utf8View.init(text) catch return error.InvalidUtf8;
    var iterator = view.iterator();
    var hash = Sha256.init(.{});
    var pending_space = false;
    var emitted = false;
    while (iterator.nextCodepoint()) |cp| {
        if (unicode.isWhitespace(cp)) {
            if (emitted) pending_space = true;
            continue;
        }
        if (pending_space) {
            hash.update(" ");
            pending_space = false;
        }
        var low: usize = 0;
        var high: usize = casefold.entries.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (casefold.entries[mid].source < cp) low = mid + 1 else high = mid;
        }
        const unchanged = [_]u21{cp};
        const mapped: []const u21 = if (low < casefold.entries.len and casefold.entries[low].source == cp)
            casefold.entries[low].target[0..casefold.entries[low].len]
        else
            &unchanged;
        for (mapped) |folded| {
            var encoded: [4]u8 = undefined;
            const length = try std.unicode.utf8Encode(folded, &encoded);
            hash.update(encoded[0..length]);
        }
        emitted = true;
    }
    return hash.finalResult();
}

test "EG2 normalized content digest matches pinned Python full casefold and whitespace" {
    try std.testing.expectEqualStrings("3.11", casefold.python_version);
    try std.testing.expectEqualStrings("14.0.0", casefold.unicode_version);
    const cases = [_]struct { text: []const u8, sha256: []const u8 }{
        .{ .text = "  MICRO µ Σςßİ\u{a0}Science  ", .sha256 = "e5161bec2044fcb6501887f104e1a6297d05a9ed58001e3ca3503faa1fcc464d" },
        .{ .text = "Straße", .sha256 = "16d96952087774fee069b7585d3991b24d90c181c09b2129b4908c35baa7f0c0" },
        .{ .text = "ΜΙΚΡΌΣ µςİ", .sha256 = "a447a218ce7abb429265b194d005b5a7854a6a279976e78e1815247f38a54c8a" },
    };
    for (cases) |case| {
        const digest = try normalizedContentDigest(case.text);
        const actual = std.fmt.bytesToHex(digest, .lower);
        try std.testing.expectEqualStrings(case.sha256, &actual);
    }
}

fn requiredObject(value: std.json.Value, name: []const u8) !std.json.ObjectMap {
    if (value != .object) return error.InvalidSourceManifest;
    const child = value.object.get(name) orelse return error.InvalidSourceManifest;
    if (child != .object) return error.InvalidSourceManifest;
    return child.object;
}
fn requiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidSourceManifest;
    if (value != .string or value.string.len == 0) return error.InvalidSourceManifest;
    return value.string;
}
fn validLowerDigest(value: []const u8, lengths: []const usize) bool {
    var valid_length = false;
    for (lengths) |length| valid_length = valid_length or value.len == length;
    if (!valid_length) return false;
    for (value) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return false;
    return true;
}
fn loadStringSet(a: std.mem.Allocator, object: std.json.ObjectMap, name: []const u8, validate_digest: bool) !std.StringHashMap(void) {
    const value = object.get(name) orelse return error.InvalidSourceManifest;
    if (value != .array or value.array.items.len == 0) return error.InvalidSourceManifest;
    var result = std.StringHashMap(void).init(a);
    errdefer result.deinit();
    for (value.array.items) |item| {
        if (item != .string or item.string.len == 0 or (validate_digest and !validLowerDigest(item.string, &.{64}))) return error.InvalidSourceManifest;
        const entry = try result.getOrPut(item.string);
        if (entry.found_existing) return error.InvalidSourceManifest;
    }
    return result;
}
fn loadDigestSet(a: std.mem.Allocator, object: std.json.ObjectMap, name: []const u8) !std.AutoHashMap([32]u8, void) {
    const value = object.get(name) orelse return error.InvalidSourceManifest;
    if (value != .array or value.array.items.len == 0) return error.InvalidSourceManifest;
    var result = std.AutoHashMap([32]u8, void).init(a);
    errdefer result.deinit();
    for (value.array.items) |item| {
        if (item != .string or !validLowerDigest(item.string, &.{64})) return error.InvalidSourceManifest;
        if ((try result.getOrPut(try decodeDigest(item.string))).found_existing) return error.InvalidSourceManifest;
    }
    return result;
}
fn loadIdentitySet(a: std.mem.Allocator, object: std.json.ObjectMap, name: []const u8) !std.AutoHashMap([32]u8, void) {
    const value = object.get(name) orelse return error.InvalidSourceManifest;
    if (value != .array or value.array.items.len == 0) return error.InvalidSourceManifest;
    var result = std.AutoHashMap([32]u8, void).init(a);
    errdefer result.deinit();
    for (value.array.items) |item| {
        if (item != .string or item.string.len == 0) return error.InvalidSourceManifest;
        if ((try result.getOrPut(sha256(item.string))).found_existing) return error.InvalidSourceManifest;
    }
    return result;
}
fn loadPairSet(a: std.mem.Allocator, object: std.json.ObjectMap, name: []const u8) !std.AutoHashMap([32]u8, void) {
    const value = object.get(name) orelse return error.InvalidSourceManifest;
    if (value != .array) return error.InvalidSourceManifest;
    var result = std.AutoHashMap([32]u8, void).init(a);
    errdefer result.deinit();
    for (value.array.items) |item| {
        if (item != .array or item.array.items.len != 2 or item.array.items[0] != .string or item.array.items[1] != .string or item.array.items[0].string.len == 0 or item.array.items[1].string.len == 0) return error.InvalidSourceManifest;
        const entry = try result.getOrPut(pairDigest(item.array.items[0].string, item.array.items[1].string));
        if (entry.found_existing) return error.InvalidSourceManifest;
    }
    return result;
}

fn parseSourceInventory(a: std.mem.Allocator, bytes: []const u8) !SourceInventory {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSourceManifest;
    const root = parsed.value.object;
    if (!std.mem.eql(u8, try requiredString(root, "schema"), "antfly.embedding_gemma2.finetune_dataset.v1")) return error.InvalidSourceManifest;
    const sources = root.get("sources") orelse return error.InvalidSourceManifest;
    if (sources != .array or sources.array.items.len == 0) return error.InvalidSourceManifest;
    var identities = std.AutoHashMap([32]u8, void).init(a);
    defer identities.deinit();
    for (sources.array.items) |source| {
        if (source != .object) return error.InvalidSourceManifest;
        const dataset = try requiredString(source.object, "dataset");
        const revision = try requiredString(source.object, "revision");
        const config = try requiredString(source.object, "config");
        _ = try requiredString(source.object, "license");
        const snapshot = try requiredString(source.object, "snapshot_sha256");
        if (!validLowerDigest(revision, &.{ 40, 64 }) or !validLowerDigest(snapshot, &.{64})) return error.InvalidSourceManifest;
        var identity_hash = Sha256.init(.{});
        for ([_][]const u8{ dataset, revision, config }) |part| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, part.len, .little);
            identity_hash.update(&length);
            identity_hash.update(part);
        }
        if ((try identities.getOrPut(identity_hash.finalResult())).found_existing) return error.InvalidSourceManifest;
    }
    const splits = try requiredObject(parsed.value, "splits");
    const miner = try requiredObject(parsed.value, "miner");
    var split_hashes: [3]std.AutoHashMap([32]u8, void) = undefined;
    var split_count: usize = 0;
    errdefer for (split_hashes[0..split_count]) |*map| map.deinit();
    for ([_][]const u8{ "train_content_sha256", "validation_content_sha256", "heldout_content_sha256" }, 0..) |name, i| {
        split_hashes[i] = try loadDigestSet(a, splits, name);
        split_count += 1;
    }
    var train_hashes = split_hashes[0].keyIterator();
    while (train_hashes.next()) |digest| if (split_hashes[1].contains(digest.*) or split_hashes[2].contains(digest.*)) return error.InvalidSourceManifest;
    var validation_hashes = split_hashes[1].keyIterator();
    while (validation_hashes.next()) |digest| if (split_hashes[2].contains(digest.*)) return error.InvalidSourceManifest;
    var observed: [3]std.AutoHashMap([32]u8, void) = .{ std.AutoHashMap([32]u8, void).init(a), std.AutoHashMap([32]u8, void).init(a), std.AutoHashMap([32]u8, void).init(a) };
    errdefer for (&observed) |*map| map.deinit();
    var corpus = try loadIdentitySet(a, splits, "train_corpus_ids");
    errdefer corpus.deinit();
    var mined_corpus = try loadIdentitySet(a, miner, "corpus_ids");
    defer mined_corpus.deinit();
    var mined_ids = mined_corpus.keyIterator();
    while (mined_ids.next()) |identity| if (!corpus.contains(identity.*)) return error.InvalidSourceManifest;
    var observed_corpus = std.AutoHashMap([32]u8, void).init(a);
    errdefer observed_corpus.deinit();
    var positives = try loadPairSet(a, splits, "train_positive_pairs");
    errdefer positives.deinit();
    var observed_positives = std.AutoHashMap([32]u8, void).init(a);
    errdefer observed_positives.deinit();
    var negatives = try loadPairSet(a, miner, "negative_pairs");
    errdefer negatives.deinit();
    var positive_iterator = positives.keyIterator();
    while (positive_iterator.next()) |pair| if (negatives.contains(pair.*)) return error.InvalidSourceManifest;
    var observed_negatives = std.AutoHashMap([32]u8, void).init(a);
    errdefer observed_negatives.deinit();
    return .{ .allocator = a, .parsed = parsed, .split_hashes = split_hashes, .observed_hashes = observed, .corpus_ids = corpus, .observed_corpus_ids = observed_corpus, .positive_pairs = positives, .observed_positive_pairs = observed_positives, .negative_pairs = negatives, .observed_negative_pairs = observed_negatives };
}

pub fn queryPrefix(task: Task) []const u8 {
    return switch (task) {
        .retrieval => "task: search result | query: ",
        .code_retrieval => "task: code retrieval | query: ",
    };
}
pub fn preprocessingDigest(options: PrepareOptions) ![32]u8 {
    var buffer: [2048]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try std.json.Stringify.value(.{
        .version = @as(u32, 1),
        .tokenizer = job.tokenizer_sha256,
        .retrieval_query = queryPrefix(.retrieval),
        .code_query = queryPrefix(.code_retrieval),
        .document_template = "title: {title|none} | text: {text}",
        .query_max_tokens = options.query_max_tokens,
        .document_max_tokens = options.document_max_tokens,
        .max_negatives = options.max_negatives,
        .truncation = "retain-bos-prefix-and-trailing-eos-v1",
    }, .{}, &writer);
    return sha256(writer.buffered());
}

fn contentDigest(text: []const u8, title: ?[]const u8) [32]u8 {
    var hash = Sha256.init(.{});
    hash.update("eg2.content.v1\x00");
    var length: [8]u8 = undefined;
    const resolved_title: []const u8 = title orelse "none";
    std.mem.writeInt(u64, &length, resolved_title.len, .little);
    hash.update(&length);
    hash.update(resolved_title);
    hash.update(text);
    return hash.finalResult();
}
fn validateText(text: []const u8) !void {
    if (std.mem.trim(u8, text, " \r\n\t").len == 0 or !std.unicode.utf8ValidateSlice(text)) return error.InvalidRetrievalText;
    for ([_][]const u8{ "<|image|>", "<|audio|>", "<|video|>" }) |marker|
        if (std.mem.indexOf(u8, text, marker) != null) return error.MediaTrainingNotSupported;
}

fn encodeDocument(a: std.mem.Allocator, encoder: Encoder, doc: Document, max_tokens: usize, truncated: *u64) !EncodedDocument {
    try validateText(doc.text);
    if (doc.title) |title| try validateText(title);
    const digest = try digestHex(a, contentDigest(doc.text, doc.title));
    const rendered = try std.fmt.allocPrint(a, "title: {s} | text: {s}", .{ doc.title orelse "none", doc.text });
    const encoding = try encoder.encode(encoder.context, a, rendered, max_tokens);
    try validateIds(encoding.ids, max_tokens);
    truncated.* += @intFromBool(encoding.truncated);
    return .{ .id = if (doc.id.len == 0) digest else doc.id, .content_sha256 = digest, .input_ids = encoding.ids };
}
fn validateIds(ids: []const i32, limit: usize) !void {
    if (ids.len < 2 or ids.len > limit or ids[0] != 2 or ids[ids.len - 1] != 1) return error.InvalidPreparedTokens;
    for (ids) |id| if (id < 0 or id >= 262_144) return error.InvalidPreparedTokens;
}

fn prepareRecord(a: std.mem.Allocator, encoder: Encoder, raw: RawRecord, options: PrepareOptions, truncated: *u64) !Record {
    try validateText(raw.query);
    if (raw.negatives.len > options.max_negatives or raw.known_positive_ids.len > 1024 or
        raw.query_id.len > 1024 or raw.group_id.len > 1024) return error.InvalidRetrievalRecord;
    const query_hash = try digestHex(a, contentDigest(raw.query, null));
    const query = try std.fmt.allocPrint(a, "{s}{s}", .{ queryPrefix(raw.task), raw.query });
    const query_encoding = try encoder.encode(encoder.context, a, query, options.query_max_tokens);
    try validateIds(query_encoding.ids, options.query_max_tokens);
    truncated.* += @intFromBool(query_encoding.truncated);
    const positive = try encodeDocument(a, encoder, raw.positive, options.document_max_tokens, truncated);
    const negatives = try a.alloc(EncodedDocument, raw.negatives.len);
    for (raw.negatives, 0..) |doc, i| {
        negatives[i] = try encodeDocument(a, encoder, doc, options.document_max_tokens, truncated);
        if (std.mem.eql(u8, negatives[i].id, positive.id) or std.mem.eql(u8, negatives[i].content_sha256, positive.content_sha256)) return error.NegativeIsPositive;
        for (raw.known_positive_ids) |id| if (std.mem.eql(u8, negatives[i].id, id)) return error.NegativeIsPositive;
        for (negatives[0..i]) |prior| if (std.mem.eql(u8, prior.id, negatives[i].id) or std.mem.eql(u8, prior.content_sha256, negatives[i].content_sha256)) return error.DuplicateHardNegative;
    }
    return .{ .query_id = if (raw.query_id.len == 0) query_hash else raw.query_id, .query_content_sha256 = query_hash, .query_ids = query_encoding.ids, .positive = positive, .negatives = negatives, .known_positive_ids = raw.known_positive_ids, .task = raw.task, .split = raw.split, .group_id = raw.group_id };
}

pub fn prepare(a: std.mem.Allocator, io: std.Io, source_path: []const u8, output_dir: []const u8, encoder: Encoder, options: PrepareOptions) !void {
    if (options.query_max_tokens < 4 or options.query_max_tokens > 512 or options.document_max_tokens < 4 or options.document_max_tokens > 512 or
        options.max_negatives > 16 or options.max_records == 0 or options.max_records > 10_000_000 or options.shard_bytes < max_record_bytes or options.shard_bytes > 1024 * 1024 * 1024)
        return error.InvalidPreparationOptions;
    try job.rejectPathOverlap(a, io, output_dir, &.{source_path});
    if (options.source_manifest_path) |path| try job.rejectPathOverlap(a, io, output_dir, &.{path});
    const cwd = std.Io.Dir.cwd();
    var staging_owner = try fs_support.OwnedStagingDir.create(a, io, output_dir, "staging");
    defer staging_owner.deinit();
    var staging = try cwd.openDir(io, staging_owner.path, .{});
    defer staging.close(io);
    var source_manifest_bytes: ?[]u8 = null;
    defer if (source_manifest_bytes) |bytes| a.free(bytes);
    var source_inventory: ?SourceInventory = null;
    defer if (source_inventory) |*inventory| inventory.deinit();
    var source_manifest_sha: ?[32]u8 = null;
    if (options.source_manifest_path) |path| {
        const bytes = try readSourceManifest(a, io, cwd, path);
        source_manifest_bytes = bytes;
        source_inventory = try parseSourceInventory(a, bytes);
        source_manifest_sha = sha256(bytes);
        const copy = try staging.createFile(io, "source-manifest.json", .{ .exclusive = true, .read = true });
        defer copy.close(io);
        try copy.writePositionalAll(io, bytes, 0);
        try copy.sync(io);
        if (!std.mem.eql(u8, &source_manifest_sha.?, &(try hashFile(io, copy, max_source_manifest_bytes)))) return error.SourceManifestChangedDuringPreparation;
    }
    const source = try cwd.openFile(io, source_path, .{});
    defer source.close(io);
    const source_sha = try hashFile(io, source, 1024 * 1024 * 1024 * 1024);
    const read_buffer = try a.alloc(u8, max_record_bytes + 1);
    defer a.free(read_buffer);
    var reader = source.reader(io, read_buffer);
    const index = try staging.createFile(io, "index.bin", .{ .exclusive = true, .read = true });
    defer index.close(io);
    var header: [index_header_bytes]u8 = @splat(0);
    @memcpy(header[0..8], index_magic);
    try index.writePositionalAll(io, &header, 0);
    var count: u64 = 0;
    var counts: [3]u64 = @splat(0);
    var truncated: u64 = 0;
    var shards = std.ArrayListUnmanaged(Shard).empty;
    defer {
        for (shards.items) |shard| {
            a.free(shard.name);
            a.free(shard.sha256);
        }
        shards.deinit(a);
    }
    var shard: ?std.Io.File = null;
    defer if (shard) |file| file.close(io);
    var shard_offset: u64 = 0;
    var shard_hasher = Sha256.init(.{});
    var splits = std.AutoHashMap([32]u8, Split).init(a);
    defer splits.deinit();
    var queries = std.AutoHashMap([32]u8, void).init(a);
    defer queries.deinit();
    var documents = std.AutoHashMap([32]u8, [32]u8).init(a);
    defer documents.deinit();
    while (try reader.interface.takeDelimiter('\n')) |line| {
        if (line.len == 0 or std.mem.trim(u8, line, "\r \t").len == 0) continue;
        if (count >= options.max_records) return error.DatasetRecordLimitExceeded;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const aa = arena.allocator();
        const raw = try std.json.parseFromSliceLeaky(RawRecord, aa, line, .{});
        if (source_inventory) |*inventory| try inventory.observe(raw);
        const record = try prepareRecord(aa, encoder, raw, options, &truncated);
        // Query and project identities may not cross labeled splits. Dataset
        // converters additionally decontaminate positive/code content.
        for ([_][]const u8{ record.query_content_sha256, record.query_id, record.group_id }) |identity| {
            if (identity.len == 0) continue;
            const entry = try splits.getOrPut(sha256(identity));
            if (entry.found_existing and entry.value_ptr.* != record.split) return error.DatasetSplitLeakage;
            entry.value_ptr.* = record.split;
        }
        // Duplicate queries cannot safely become in-batch negatives. Require
        // converters to combine their relevance labels before preparation.
        for ([_][]const u8{ record.query_id, record.query_content_sha256 }, 0..) |identity, kind| {
            var h = Sha256.init(.{});
            h.update(&.{@intCast(kind)});
            h.update(identity);
            const entry = try queries.getOrPut(h.finalResult());
            if (entry.found_existing) return error.DuplicateRetrievalQuery;
        }
        try registerDocument(&documents, record.positive);
        for (record.negatives) |negative| try registerDocument(&documents, negative);
        var payload = std.Io.Writer.Allocating.init(aa);
        try std.json.Stringify.value(record, .{}, &payload.writer);
        try payload.writer.writeByte('\n');
        const bytes = payload.written();
        if (bytes.len > max_record_bytes) return error.DatasetRecordTooLarge;
        if (shard != null and shard_offset + bytes.len > options.shard_bytes) {
            try shard.?.sync(io);
            shard.?.close(io);
            shard = null;
            shards.items[shards.items.len - 1].bytes = shard_offset;
            shards.items[shards.items.len - 1].sha256 = try digestHex(a, shard_hasher.finalResult());
        }
        if (shard == null) {
            if (shards.items.len >= 65536) return error.DatasetShardLimitExceeded;
            const name = try std.fmt.allocPrint(a, "data-{d:0>6}.jsonl", .{shards.items.len});
            shards.append(a, .{ .name = name, .bytes = 0, .sha256 = &.{} }) catch |err| {
                a.free(name);
                return err;
            };
            shard = try staging.createFile(io, name, .{ .exclusive = true });
            shard_offset = 0;
            shard_hasher = Sha256.init(.{});
        }
        try shard.?.writePositionalAll(io, bytes, shard_offset);
        shard_hasher.update(bytes);
        var entry: [index_entry_bytes]u8 = @splat(0);
        std.mem.writeInt(u32, entry[0..4], @intCast(shards.items.len - 1), .little);
        std.mem.writeInt(u32, entry[4..8], @intCast(bytes.len), .little);
        std.mem.writeInt(u64, entry[8..16], shard_offset, .little);
        @memcpy(entry[16..48], &sha256(bytes));
        entry[48] = @backingInt(record.split);
        try index.writePositionalAll(io, &entry, index_header_bytes + count * index_entry_bytes);
        shard_offset += bytes.len;
        counts[@backingInt(record.split)] += 1;
        count += 1;
    }
    if (count == 0) return error.EmptyRetrievalDataset;
    if (source_inventory) |*inventory| try inventory.validateComplete();
    if (!std.mem.eql(u8, &source_sha, &(try hashFile(io, source, 1024 * 1024 * 1024 * 1024))))
        return error.DatasetSourceChangedDuringPreparation;
    if (shard) |file| {
        try file.sync(io);
        file.close(io);
        shard = null;
        shards.items[shards.items.len - 1].bytes = shard_offset;
        shards.items[shards.items.len - 1].sha256 = try digestHex(a, shard_hasher.finalResult());
    }
    std.mem.writeInt(u64, header[8..16], count, .little);
    try index.writePositionalAll(io, &header, 0);
    try index.sync(io);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa = arena.allocator();
    const manifest = Manifest{ .records = count, .train_records = counts[0], .validation_records = counts[1], .test_records = counts[2], .truncated_sequences = truncated, .query_max_tokens = options.query_max_tokens, .document_max_tokens = options.document_max_tokens, .max_negatives = options.max_negatives, .source_sha256 = try digestHex(aa, source_sha), .source_repo = options.source_repo, .source_revision = options.source_revision, .source_manifest_sha256 = if (source_manifest_sha) |digest| try digestHex(aa, digest) else null, .preprocessing_sha256 = try digestHex(aa, try preprocessingDigest(options)), .index_sha256 = try digestHex(aa, try hashFile(io, index, index_header_bytes + count * index_entry_bytes)), .shards = shards.items };
    var json = std.Io.Writer.Allocating.init(aa);
    // Omitting an absent source binding preserves legacy manifest bytes and
    // therefore existing job/checkpoint fingerprints exactly.
    try std.json.Stringify.value(manifest, .{ .whitespace = .indent_2, .emit_null_optional_fields = false }, &json.writer);
    const manifest_file = try staging.createFile(io, "manifest.json", .{ .exclusive = true });
    defer manifest_file.close(io);
    try manifest_file.writePositionalAll(io, json.written(), 0);
    try manifest_file.sync(io);
    try staging_owner.publish(output_dir);
}

fn registerDocument(documents: *std.AutoHashMap([32]u8, [32]u8), document: EncodedDocument) !void {
    const digest = try decodeDigest(document.content_sha256);
    const entry = try documents.getOrPut(sha256(document.id));
    if (entry.found_existing and !std.mem.eql(u8, entry.value_ptr, &digest)) return error.DocumentIdentityMismatch;
    entry.value_ptr.* = digest;
}

pub const Dataset = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    index: std.Io.File,
    /// Authentication uses this immutable snapshot; replacing the index after
    /// open cannot change a record's checksum or sampler split metadata.
    index_bytes: []u8,
    parsed: std.json.Parsed(Manifest),
    manifest_sha256: [32]u8,

    pub fn open(a: std.mem.Allocator, io: std.Io, directory: []const u8) !Dataset {
        const dir = try std.Io.Dir.cwd().openDir(io, directory, .{});
        errdefer dir.close(io);
        const json = try dir.readFileAlloc(io, "manifest.json", a, .limited(16 * 1024 * 1024));
        defer a.free(json);
        const parsed = try std.json.parseFromSlice(Manifest, a, json, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        const m = parsed.value;
        if (m.version != 1 or !std.mem.eql(u8, m.format, "antfly.embeddinggemma2.retrieval-shards/v1") or
            m.records == 0 or m.records > 10_000_000 or m.shards.len == 0 or m.shards.len > 65536 or
            (std.math.add(u64, std.math.add(u64, m.train_records, m.validation_records) catch return error.InvalidPreparedManifest, m.test_records) catch return error.InvalidPreparedManifest) != m.records or
            m.query_max_tokens < 4 or m.query_max_tokens > 512 or m.document_max_tokens < 4 or m.document_max_tokens > 512 or
            m.max_negatives > 16 or !std.mem.eql(u8, m.tokenizer_sha256, job.tokenizer_sha256)) return error.InvalidPreparedManifest;
        _ = try decodeDigest(m.source_sha256);
        if (m.source_manifest_sha256) |expected_hex| {
            const expected = try decodeDigest(expected_hex);
            const source_manifest = try readSourceManifest(a, io, dir, "source-manifest.json");
            defer a.free(source_manifest);
            if (!std.mem.eql(u8, &expected, &sha256(source_manifest))) return error.SourceManifestDigestMismatch;
            var inventory = try parseSourceInventory(a, source_manifest);
            inventory.deinit();
        }
        const expected_preprocessing = try preprocessingDigest(.{ .query_max_tokens = m.query_max_tokens, .document_max_tokens = m.document_max_tokens, .max_negatives = m.max_negatives });
        if (!std.mem.eql(u8, &expected_preprocessing, &(try decodeDigest(m.preprocessing_sha256)))) return error.PreparedPreprocessingMismatch;
        const index = try dir.openFile(io, "index.bin", .{});
        errdefer index.close(io);
        if (try index.length(io) != index_header_bytes + m.records * index_entry_bytes) return error.InvalidPreparedIndex;
        var header: [index_header_bytes]u8 = undefined;
        if (try index.readPositionalAll(io, &header, 0) != header.len or !std.mem.eql(u8, header[0..8], index_magic) or
            std.mem.readInt(u64, header[8..16], .little) != m.records) return error.InvalidPreparedIndex;
        const index_bytes = try a.alloc(u8, @intCast(index_header_bytes + m.records * index_entry_bytes));
        errdefer a.free(index_bytes);
        if (try index.readPositionalAll(io, index_bytes, 0) != index_bytes.len or
            !std.mem.eql(u8, &sha256(index_bytes), &(try decodeDigest(m.index_sha256)))) return error.DatasetDigestMismatch;
        for (m.shards, 0..) |s, i| {
            var name: [32]u8 = undefined;
            const expected = try std.fmt.bufPrint(&name, "data-{d:0>6}.jsonl", .{i});
            if (!std.mem.eql(u8, s.name, expected) or s.bytes == 0 or s.bytes > 1024 * 1024 * 1024) return error.InvalidPreparedShard;
            const file = try dir.openFile(io, s.name, .{});
            defer file.close(io);
            if (try file.length(io) != s.bytes or !std.mem.eql(u8, &(try hashFile(io, file, s.bytes)), &(try decodeDigest(s.sha256)))) return error.DatasetDigestMismatch;
        }
        return .{ .allocator = a, .io = io, .dir = dir, .index = index, .index_bytes = index_bytes, .parsed = parsed, .manifest_sha256 = sha256(json) };
    }
    pub fn requireBoundSourceManifest(self: *const Dataset) ![]const u8 {
        return self.parsed.value.source_manifest_sha256 orelse error.UnboundPublicSourceManifest;
    }
    pub fn deinit(self: *Dataset) void {
        self.index.close(self.io);
        self.allocator.free(self.index_bytes);
        self.dir.close(self.io);
        self.parsed.deinit();
        self.* = undefined;
    }
    fn entry(self: *const Dataset, position: u64) ![index_entry_bytes]u8 {
        if (position >= self.parsed.value.records) return error.DatasetIndexOutOfRange;
        var bytes: [index_entry_bytes]u8 = undefined;
        const offset: usize = @intCast(index_header_bytes + position * index_entry_bytes);
        @memcpy(&bytes, self.index_bytes[offset..][0..index_entry_bytes]);
        if (bytes[48] > 2) return error.InvalidPreparedIndex;
        for (bytes[49..]) |v| if (v != 0) return error.InvalidPreparedIndex;
        return bytes;
    }
    pub fn split(self: *const Dataset, position: u64) !Split {
        return @fromBackingInt(@intCast((try self.entry(position))[48]));
    }
    pub fn read(self: *const Dataset, a: std.mem.Allocator, position: u64) !std.json.Parsed(Record) {
        const bytes = try self.entry(position);
        const shard_index = std.mem.readInt(u32, bytes[0..4], .little);
        const length = std.mem.readInt(u32, bytes[4..8], .little);
        const offset = std.mem.readInt(u64, bytes[8..16], .little);
        if (shard_index >= self.parsed.value.shards.len or length == 0 or length > max_record_bytes) return error.InvalidPreparedIndex;
        const shard = self.parsed.value.shards[shard_index];
        if (offset > shard.bytes or length > shard.bytes - offset) return error.InvalidPreparedIndex;
        const file = try self.dir.openFile(self.io, shard.name, .{});
        defer file.close(self.io);
        const payload = try a.alloc(u8, length);
        defer a.free(payload);
        if (try file.readPositionalAll(self.io, payload, offset) != payload.len) return error.UnexpectedEndOfDataset;
        if (!std.mem.eql(u8, &sha256(payload), bytes[16..48])) return error.DatasetDigestMismatch;
        const record = try std.json.parseFromSlice(Record, a, payload, .{ .allocate = .alloc_always });
        errdefer record.deinit();
        const r = record.value;
        if (@backingInt(r.split) != bytes[48] or r.negatives.len > self.parsed.value.max_negatives) return error.InvalidPreparedRecord;
        try validateIds(r.query_ids, self.parsed.value.query_max_tokens);
        try validateIds(r.positive.input_ids, self.parsed.value.document_max_tokens);
        _ = try decodeDigest(r.query_content_sha256);
        _ = try decodeDigest(r.positive.content_sha256);
        for (r.negatives) |n| {
            try validateIds(n.input_ids, self.parsed.value.document_max_tokens);
            _ = try decodeDigest(n.content_sha256);
        }
        return record;
    }
};

fn fakeEncode(_: *anyopaque, a: std.mem.Allocator, text: []const u8, maximum: usize) !Encoding {
    const ids = try a.alloc(i32, @min(text.len + 2, maximum));
    @memset(ids, 7);
    ids[0] = 2;
    ids[ids.len - 1] = 1;
    return .{ .ids = ids, .truncated = text.len + 2 > maximum };
}

test "EG2 dataset shards roundtrip, bounded reads authenticate every record" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const source = try std.fs.path.join(a, &.{ directory, "input.jsonl" });
    defer a.free(source);
    const output = try std.fs.path.join(a, &.{ directory, "prepared" });
    defer a.free(output);
    const file = try tmp.dir.createFile(io, "input.jsonl", .{});
    defer file.close(io);
    try file.writePositionalAll(io, "{\"query\":\"find this\",\"positive\":{\"id\":\"p1\",\"text\":\"document\"},\"negatives\":[{\"id\":\"n1\",\"text\":\"other\"}]}\n" ++
        "{\"query\":\"other question\",\"positive\":{\"id\":\"p2\",\"text\":\"answer\"},\"split\":\"validation\"}", 0);
    var dummy: u8 = 0;
    try prepare(a, io, source, output, .{ .context = &dummy, .encode = fakeEncode }, .{});
    var dataset = try Dataset.open(a, io, output);
    defer dataset.deinit();
    try std.testing.expectEqual(@as(u64, 2), dataset.parsed.value.records);
    try std.testing.expectEqual(Split.validation, try dataset.split(1));
    var record = try dataset.read(a, 0);
    defer record.deinit();
    try std.testing.expectEqualStrings("p1", record.value.positive.id);
    try std.testing.expectEqual(@as(i32, 2), record.value.query_ids[0]);
    const changed_index = try dataset.dir.openFile(io, "index.bin", .{ .mode = .read_write });
    defer changed_index.close(io);
    try changed_index.writePositionalAll(io, &.{2}, index_header_bytes + 48);
    try std.testing.expectEqual(Split.train, try dataset.split(0));
    var authenticated = try dataset.read(a, 0);
    defer authenticated.deinit();
    try std.testing.expectEqualStrings("p1", authenticated.value.positive.id);
    const corrupted = try dataset.dir.openFile(io, "data-000000.jsonl", .{ .mode = .read_write });
    defer corrupted.close(io);
    try corrupted.writePositionalAll(io, "!", 0);
    try std.testing.expectError(error.DatasetDigestMismatch, dataset.read(a, 0));
}

test "EG2 source manifest binds exact bytes and declared record inventory" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const source = try std.fs.path.join(a, &.{ directory, "records.jsonl" });
    defer a.free(source);
    const source_manifest = try std.fs.path.join(a, &.{ directory, "dataset_manifest.json" });
    defer a.free(source_manifest);
    const output = try std.fs.path.join(a, &.{ directory, "prepared" });
    defer a.free(output);
    const records =
        "{\"query_id\":\"q1\",\"query\":\"Find One\",\"positive\":{\"id\":\"p1\",\"text\":\"Document One\"},\"negatives\":[{\"id\":\"n1\",\"text\":\"Other\"}]}\n" ++
        "{\"query_id\":\"qv\",\"query\":\"Validate\",\"positive\":{\"id\":\"pv\",\"text\":\"Validation Document\"},\"split\":\"validation\"}\n" ++
        "{\"query_id\":\"qt\",\"query\":\"Test\",\"positive\":{\"id\":\"pt\",\"text\":\"Test Document\"},\"split\":\"test\"}\n";
    const source_file = try tmp.dir.createFile(io, "records.jsonl", .{});
    defer source_file.close(io);
    try source_file.writePositionalAll(io, records, 0);
    const h = struct {
        fn hex(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
            return digestHex(allocator, try normalizedContentDigest(text));
        }
    };
    const train_hashes = [_][]const u8{ try h.hex(a, "Find One"), try h.hex(a, "Document One") };
    defer for (train_hashes) |value| a.free(value);
    const validation_hashes = [_][]const u8{ try h.hex(a, "Validate"), try h.hex(a, "Validation Document") };
    defer for (validation_hashes) |value| a.free(value);
    const test_hashes = [_][]const u8{ try h.hex(a, "Test"), try h.hex(a, "Test Document") };
    defer for (test_hashes) |value| a.free(value);
    var manifest_json = std.Io.Writer.Allocating.init(a);
    defer manifest_json.deinit();
    try std.json.Stringify.value(.{
        .schema = "antfly.embedding_gemma2.finetune_dataset.v1",
        .sources = &.{.{ .dataset = "public/example", .revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .config = "default", .license = "public", .snapshot_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }},
        .splits = .{
            .train_content_sha256 = &train_hashes,
            .validation_content_sha256 = &validation_hashes,
            .heldout_content_sha256 = &test_hashes,
            .train_corpus_ids = &.{ "p1", "n1" },
            .train_positive_pairs = &.{&.{ "q1", "p1" }},
        },
        .miner = .{ .corpus_ids = &.{ "p1", "n1" }, .negative_pairs = &.{&.{ "q1", "n1" }} },
    }, .{ .whitespace = .indent_2 }, &manifest_json.writer);
    // Exercise preparation, exact-byte copying, reopen and tamper detection
    // with an inventory larger than the former 16 MiB ceiling.
    try manifest_json.writer.splatByteAll(' ', 16 * 1024 * 1024);
    try manifest_json.writer.writeByte('\n');
    const manifest_file = try tmp.dir.createFile(io, "dataset_manifest.json", .{});
    defer manifest_file.close(io);
    try manifest_file.writePositionalAll(io, manifest_json.written(), 0);
    var dummy: u8 = 0;
    try prepare(a, io, source, output, .{ .context = &dummy, .encode = fakeEncode }, .{ .source_manifest_path = source_manifest });
    var dataset = try Dataset.open(a, io, output);
    const bound_digest = try dataset.requireBoundSourceManifest();
    try std.testing.expectEqualSlices(u8, &sha256(manifest_json.written()), &(try decodeDigest(bound_digest)));
    dataset.deinit();
    const copied_path = try std.fs.path.join(a, &.{ output, "source-manifest.json" });
    defer a.free(copied_path);
    const copied = try readSourceManifest(a, io, std.Io.Dir.cwd(), copied_path);
    defer a.free(copied);
    try std.testing.expectEqualStrings(manifest_json.written(), copied);
    const unrelated_source = try std.fs.path.join(a, &.{ directory, "unrelated.jsonl" });
    defer a.free(unrelated_source);
    const unrelated_output = try std.fs.path.join(a, &.{ directory, "unrelated-prepared" });
    defer a.free(unrelated_output);
    const unrelated_file = try tmp.dir.createFile(io, "unrelated.jsonl", .{});
    defer unrelated_file.close(io);
    const changed_records = try std.mem.replaceOwned(u8, a, records, "Document One", "Unrelated One");
    defer a.free(changed_records);
    try unrelated_file.writePositionalAll(io, changed_records, 0);
    try std.testing.expectError(error.SourceManifestContentMismatch, prepare(a, io, unrelated_source, unrelated_output, .{ .context = &dummy, .encode = fakeEncode }, .{ .source_manifest_path = source_manifest }));
    const prepared_dir = try std.Io.Dir.cwd().openDir(io, output, .{});
    defer prepared_dir.close(io);
    const corrupt = try prepared_dir.openFile(io, "source-manifest.json", .{ .mode = .read_write });
    defer corrupt.close(io);
    try corrupt.writePositionalAll(io, " ", copied.len - 1);
    try std.testing.expectError(error.SourceManifestDigestMismatch, Dataset.open(a, io, output));
}

test "EG2 source inventories enforce the inclusive bounded size limit" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(io, "inventory.json", .{});
    defer file.close(io);
    try file.setLength(io, max_source_manifest_bytes);
    const bytes = try readSourceManifest(a, io, tmp.dir, "inventory.json");
    defer a.free(bytes);
    try std.testing.expectEqual(max_source_manifest_bytes, bytes.len);
    try file.setLength(io, max_source_manifest_bytes + 1);
    try std.testing.expectError(error.SourceManifestTooLarge, readSourceManifest(a, io, tmp.dir, "inventory.json"));
}

test "EG2 data refuses false negatives and split leakage" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var dummy: u8 = 0;
    const encoder = Encoder{ .context = &dummy, .encode = fakeEncode };
    var truncated: u64 = 0;
    try std.testing.expectError(error.NegativeIsPositive, prepareRecord(arena.allocator(), encoder, .{ .query = "query", .positive = .{ .id = "p", .text = "yes" }, .negatives = &.{.{ .id = "p", .text = "no" }} }, .{}, &truncated));
    try std.testing.expectError(error.MediaTrainingNotSupported, validateText("look <|image|>"));
}

test "EG2 preparation rejects duplicate queries and globally conflicting document IDs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const source_path = try std.fs.path.join(a, &.{ directory, "input.jsonl" });
    defer a.free(source_path);
    const output_path = try std.fs.path.join(a, &.{ directory, "prepared" });
    defer a.free(output_path);
    var dummy: u8 = 0;
    const fixtures = [_]struct { text: []const u8, failure: anyerror }{
        .{ .text = "{\"query\":\"same\",\"positive\":{\"id\":\"p\",\"text\":\"doc\"}}\n" ++
            "{\"query\":\"same\",\"positive\":{\"id\":\"p\",\"text\":\"doc\"}}\n", .failure = error.DuplicateRetrievalQuery },
        .{ .text = "{\"query\":\"first\",\"positive\":{\"id\":\"p\",\"text\":\"doc\"}}\n" ++
            "{\"query\":\"second\",\"positive\":{\"id\":\"p\",\"text\":\"different\"}}\n", .failure = error.DocumentIdentityMismatch },
        .{ .text = "{\"query\":\"first\",\"positive\":{\"id\":\"p\",\"text\":\"doc\"}}\n" ++
            "{\"query\":\"first\",\"positive\":{\"id\":\"p\",\"text\":\"doc\"},\"split\":\"test\"}\n", .failure = error.DatasetSplitLeakage },
    };
    for (fixtures) |fixture| {
        const source = try tmp.dir.createFile(io, "input.jsonl", .{});
        try source.writePositionalAll(io, fixture.text, 0);
        source.close(io);
        try std.testing.expectError(fixture.failure, prepare(a, io, source_path, output_path, .{ .context = &dummy, .encode = fakeEncode }, .{}));
    }
}
