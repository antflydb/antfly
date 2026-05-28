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

// Prometheus metrics for the Termite inference server.
//
// Uses lib/prometheus for proper exposition format. Since httpx uses
// fiber-based concurrency (single OS thread), the atomic operations in
// the prometheus lib are effectively no-ops but don't hurt.

const std = @import("std");
const prometheus = @import("prometheus");

pub const Metrics = struct {
    requests_total: prometheus.Counter(u64),
    requests_active: prometheus.Gauge(i64),
    errors_total: prometheus.Counter(u64),

    embed_requests: prometheus.Counter(u64),
    rerank_requests: prometheus.Counter(u64),
    chunk_requests: prometheus.Counter(u64),
    classify_requests: prometheus.Counter(u64),
    recognize_requests: prometheus.Counter(u64),
    extract_requests: prometheus.Counter(u64),
    rewrite_requests: prometheus.Counter(u64),
    generate_requests: prometheus.Counter(u64),
    transcribe_requests: prometheus.Counter(u64),
    read_requests: prometheus.Counter(u64),

    models_loaded: prometheus.Gauge(u64),
    cache_hits: prometheus.Counter(u64),
    cache_misses: prometheus.Counter(u64),
    queue_depth: prometheus.Gauge(i64),

    pub const default: Metrics = .{
        .requests_total = prometheus.Counter(u64).init("termite_requests_total", .{ .help = "Total number of requests" }, .{}),
        .requests_active = prometheus.Gauge(i64).init("termite_requests_active", .{ .help = "Currently active requests" }, .{}),
        .errors_total = prometheus.Counter(u64).init("termite_errors_total", .{ .help = "Total number of errors" }, .{}),
        .embed_requests = prometheus.Counter(u64).init("termite_endpoint_requests_embed", .{ .help = "Embed endpoint requests" }, .{}),
        .rerank_requests = prometheus.Counter(u64).init("termite_endpoint_requests_rerank", .{ .help = "Rerank endpoint requests" }, .{}),
        .chunk_requests = prometheus.Counter(u64).init("termite_endpoint_requests_chunk", .{ .help = "Chunk endpoint requests" }, .{}),
        .classify_requests = prometheus.Counter(u64).init("termite_endpoint_requests_classify", .{ .help = "Classify endpoint requests" }, .{}),
        .recognize_requests = prometheus.Counter(u64).init("termite_endpoint_requests_recognize", .{ .help = "Recognize endpoint requests" }, .{}),
        .extract_requests = prometheus.Counter(u64).init("termite_endpoint_requests_extract", .{ .help = "Extract endpoint requests" }, .{}),
        .rewrite_requests = prometheus.Counter(u64).init("termite_endpoint_requests_rewrite", .{ .help = "Rewrite endpoint requests" }, .{}),
        .generate_requests = prometheus.Counter(u64).init("termite_endpoint_requests_generate", .{ .help = "Generate endpoint requests" }, .{}),
        .transcribe_requests = prometheus.Counter(u64).init("termite_endpoint_requests_transcribe", .{ .help = "Transcribe endpoint requests" }, .{}),
        .read_requests = prometheus.Counter(u64).init("termite_endpoint_requests_read", .{ .help = "Read endpoint requests" }, .{}),
        .models_loaded = prometheus.Gauge(u64).init("inference_models_loaded", .{ .help = "Number of loaded models" }, .{}),
        .cache_hits = prometheus.Counter(u64).init("termite_cache_hits_total", .{ .help = "Cache hits" }, .{}),
        .cache_misses = prometheus.Counter(u64).init("termite_cache_misses_total", .{ .help = "Cache misses" }, .{}),
        .queue_depth = prometheus.Gauge(i64).init("termite_request_queue_depth", .{ .help = "Current request queue depth" }, .{}),
    };

    pub fn incRequest(self: *Metrics, endpoint: []const u8) void {
        self.requests_total.incr();
        self.requests_active.incr();

        if (std.mem.eql(u8, endpoint, "embed")) {
            self.embed_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "rerank")) {
            self.rerank_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "chunk")) {
            self.chunk_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "classify")) {
            self.classify_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "recognize")) {
            self.recognize_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "extract")) {
            self.extract_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "rewrite")) {
            self.rewrite_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "generate")) {
            self.generate_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "transcribe")) {
            self.transcribe_requests.incr();
        } else if (std.mem.eql(u8, endpoint, "read")) {
            self.read_requests.incr();
        }
    }

    pub fn decActive(self: *Metrics) void {
        self.requests_active.incrBy(-1);
    }

    pub fn incError(self: *Metrics) void {
        self.errors_total.incr();
    }

    pub fn setQueueDepth(self: *Metrics, depth: usize) void {
        self.queue_depth.set(@intCast(depth));
    }

    /// Write metrics in Prometheus exposition format.
    pub fn render(self: *Metrics, writer: *std.Io.Writer) !void {
        try prometheus.write(self, writer);
    }
};

test "metrics render" {
    var m = Metrics.default;
    m.incRequest("embed");
    m.incRequest("embed");
    m.incRequest("rerank");

    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try m.render(&writer.writer);
    const output = writer.writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "termite_requests_total 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_endpoint_requests_embed 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "termite_endpoint_requests_rerank 1\n") != null);
}
