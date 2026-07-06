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

// Prometheus metrics for the Antfly inference server.
//
// Uses lib/prometheus for proper exposition format. Since httpx uses
// fiber-based concurrency (single OS thread), the atomic operations in
// the prometheus lib are effectively no-ops but don't hurt.

const std = @import("std");
const prometheus = @import("prometheus");

comptime {
    @setEvalBranchQuota(30000);
}

pub const Metrics = struct {
    requests_total: prometheus.Counter(u64),
    requests_active: prometheus.Gauge(i64),
    errors_total: prometheus.Counter(u64),

    embed_requests: prometheus.Counter(u64),
    embed_batches_total: prometheus.Counter(u64),
    embed_batch_items_total: prometheus.Counter(u64),
    embed_batch_input_bytes_total: prometheus.Counter(u64),
    embed_batch_duration_ns_total: prometheus.Counter(u64),
    embed_batch_last_items: prometheus.Gauge(u64),
    embed_batch_last_input_bytes: prometheus.Gauge(u64),
    embed_batch_last_duration_ns: prometheus.Gauge(u64),
    embed_batch_max_items: prometheus.Gauge(u64),
    embed_batch_max_input_bytes: prometheus.Gauge(u64),
    embed_batch_max_duration_ns: prometheus.Gauge(u64),
    embed_batch_max_items_value: u64 = 0,
    embed_batch_max_input_bytes_value: u64 = 0,
    embed_batch_max_duration_ns_value: u64 = 0,
    rerank_requests: prometheus.Counter(u64),
    chunk_requests: prometheus.Counter(u64),
    classify_requests: prometheus.Counter(u64),
    recognize_requests: prometheus.Counter(u64),
    extract_requests: prometheus.Counter(u64),
    rewrite_requests: prometheus.Counter(u64),
    generate_requests: prometheus.Counter(u64),
    transcribe_requests: prometheus.Counter(u64),
    read_requests: prometheus.Counter(u64),
    predict_requests: prometheus.Counter(u64),
    predict_errors: prometheus.Counter(u64),
    predictor_load: prometheus.Counter(u64),
    predictor_evict: prometheus.Counter(u64),

    models_loaded: prometheus.Gauge(u64),
    cache_hits: prometheus.Counter(u64),
    cache_misses: prometheus.Counter(u64),
    queue_depth: prometheus.Gauge(i64),
    queue_capacity: prometheus.Gauge(i64),
    queue_available: prometheus.Gauge(i64),
    queue_active_requests: prometheus.Gauge(i64),
    queue_rejections_total: prometheus.Counter(u64),
    queue_rejected_units_total: prometheus.Counter(u64),

    pub const default: Metrics = blk: {
        @setEvalBranchQuota(40_000);
        break :blk .{
            .requests_total = prometheus.Counter(u64).init("antfly_inference_requests_total", .{ .help = "Total number of requests" }, .{}),
            .requests_active = prometheus.Gauge(i64).init("antfly_inference_requests_active", .{ .help = "Currently active requests" }, .{}),
            .errors_total = prometheus.Counter(u64).init("antfly_inference_errors_total", .{ .help = "Total number of errors" }, .{}),
            .embed_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_embed", .{ .help = "Embed endpoint requests" }, .{}),
            .embed_batches_total = prometheus.Counter(u64).init("antfly_inference_embed_batches_total", .{ .help = "Completed embed inference batches" }, .{}),
            .embed_batch_items_total = prometheus.Counter(u64).init("antfly_inference_embed_batch_items_total", .{ .help = "Total items in completed embed inference batches" }, .{}),
            .embed_batch_input_bytes_total = prometheus.Counter(u64).init("antfly_inference_embed_batch_input_bytes_total", .{ .help = "Total input bytes in completed embed inference batches" }, .{}),
            .embed_batch_duration_ns_total = prometheus.Counter(u64).init("antfly_inference_embed_batch_duration_ns_total", .{ .help = "Total elapsed nanoseconds for completed embed inference batches" }, .{}),
            .embed_batch_last_items = prometheus.Gauge(u64).init("antfly_inference_embed_batch_last_items", .{ .help = "Items in the last completed embed inference batch" }, .{}),
            .embed_batch_last_input_bytes = prometheus.Gauge(u64).init("antfly_inference_embed_batch_last_input_bytes", .{ .help = "Input bytes in the last completed embed inference batch" }, .{}),
            .embed_batch_last_duration_ns = prometheus.Gauge(u64).init("antfly_inference_embed_batch_last_duration_ns", .{ .help = "Elapsed nanoseconds for the last completed embed inference batch" }, .{}),
            .embed_batch_max_items = prometheus.Gauge(u64).init("antfly_inference_embed_batch_max_items", .{ .help = "Largest completed embed inference batch by item count" }, .{}),
            .embed_batch_max_input_bytes = prometheus.Gauge(u64).init("antfly_inference_embed_batch_max_input_bytes", .{ .help = "Largest completed embed inference batch by input bytes" }, .{}),
            .embed_batch_max_duration_ns = prometheus.Gauge(u64).init("antfly_inference_embed_batch_max_duration_ns", .{ .help = "Slowest completed embed inference batch in nanoseconds" }, .{}),
            .rerank_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_rerank", .{ .help = "Rerank endpoint requests" }, .{}),
            .chunk_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_chunk", .{ .help = "Chunk endpoint requests" }, .{}),
            .classify_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_classify", .{ .help = "Classify endpoint requests" }, .{}),
            .recognize_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_recognize", .{ .help = "Recognize endpoint requests" }, .{}),
            .extract_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_extract", .{ .help = "Extract endpoint requests" }, .{}),
            .rewrite_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_rewrite", .{ .help = "Rewrite endpoint requests" }, .{}),
            .generate_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_generate", .{ .help = "Generate endpoint requests" }, .{}),
            .transcribe_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_transcribe", .{ .help = "Transcribe endpoint requests" }, .{}),
            .read_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_read", .{ .help = "Read endpoint requests" }, .{}),
            .predict_requests = prometheus.Counter(u64).init("antfly_inference_endpoint_requests_predict", .{ .help = "Predict endpoint requests" }, .{}),
            .predict_errors = prometheus.Counter(u64).init("antfly_inference_predict_errors_total", .{ .help = "Predict-handler errors" }, .{}),
            .predictor_load = prometheus.Counter(u64).init("antfly_inference_predictor_load_total", .{ .help = "Predictor lazy-loads from disk" }, .{}),
            .predictor_evict = prometheus.Counter(u64).init("antfly_inference_predictor_evict_total", .{ .help = "Predictor cache evictions" }, .{}),
            .models_loaded = prometheus.Gauge(u64).init("antfly_inference_models_loaded", .{ .help = "Number of loaded models" }, .{}),
            .cache_hits = prometheus.Counter(u64).init("antfly_inference_cache_hits_total", .{ .help = "Cache hits" }, .{}),
            .cache_misses = prometheus.Counter(u64).init("antfly_inference_cache_misses_total", .{ .help = "Cache misses" }, .{}),
            .queue_depth = prometheus.Gauge(i64).init("antfly_inference_request_queue_depth", .{ .help = "Current request queue depth" }, .{}),
            .queue_capacity = prometheus.Gauge(i64).init("antfly_inference_request_queue_capacity", .{ .help = "Configured weighted request queue capacity" }, .{}),
            .queue_available = prometheus.Gauge(i64).init("antfly_inference_request_queue_available", .{ .help = "Available weighted request queue capacity" }, .{}),
            .queue_active_requests = prometheus.Gauge(i64).init("antfly_inference_request_queue_active_requests", .{ .help = "Currently admitted requests in the request queue" }, .{}),
            .queue_rejections_total = prometheus.Counter(u64).init("antfly_inference_request_queue_rejections_total", .{ .help = "Requests rejected because the request queue was at capacity" }, .{}),
            .queue_rejected_units_total = prometheus.Counter(u64).init("antfly_inference_request_queue_rejected_units_total", .{ .help = "Weighted request queue units rejected because the request queue was at capacity" }, .{}),
        };
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
        } else if (std.mem.eql(u8, endpoint, "predict")) {
            self.predict_requests.incr();
        }
    }

    pub fn incPredictError(self: *Metrics) void {
        self.predict_errors.incr();
    }

    pub fn incPredictorLoad(self: *Metrics) void {
        self.predictor_load.incr();
    }

    pub fn incPredictorEvict(self: *Metrics) void {
        self.predictor_evict.incr();
    }

    pub fn decActive(self: *Metrics) void {
        self.requests_active.incrBy(-1);
    }

    pub fn incError(self: *Metrics) void {
        self.errors_total.incr();
    }

    pub fn recordEmbedBatch(self: *Metrics, items: usize, input_bytes: usize, duration_ns: u64) void {
        const item_count: u64 = @intCast(items);
        const byte_count: u64 = @intCast(input_bytes);
        self.embed_batches_total.incr();
        self.embed_batch_items_total.incrBy(item_count);
        self.embed_batch_input_bytes_total.incrBy(byte_count);
        self.embed_batch_duration_ns_total.incrBy(duration_ns);
        self.embed_batch_last_items.set(item_count);
        self.embed_batch_last_input_bytes.set(byte_count);
        self.embed_batch_last_duration_ns.set(duration_ns);
        if (item_count > self.embed_batch_max_items_value) {
            self.embed_batch_max_items_value = item_count;
            self.embed_batch_max_items.set(item_count);
        }
        if (byte_count > self.embed_batch_max_input_bytes_value) {
            self.embed_batch_max_input_bytes_value = byte_count;
            self.embed_batch_max_input_bytes.set(byte_count);
        }
        if (duration_ns > self.embed_batch_max_duration_ns_value) {
            self.embed_batch_max_duration_ns_value = duration_ns;
            self.embed_batch_max_duration_ns.set(duration_ns);
        }
    }

    pub fn setQueueDepth(self: *Metrics, depth: usize) void {
        self.queue_depth.set(@intCast(depth));
    }

    pub fn setQueueState(self: *Metrics, active_units: usize, capacity: usize, active_requests: usize) void {
        self.queue_depth.set(@intCast(active_units));
        self.queue_capacity.set(@intCast(capacity));
        self.queue_available.set(@intCast(capacity - @min(active_units, capacity)));
        self.queue_active_requests.set(@intCast(active_requests));
    }

    pub fn recordQueueRejection(self: *Metrics, requested_units: usize) void {
        self.queue_rejections_total.incr();
        self.queue_rejected_units_total.incrBy(@intCast(requested_units));
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

    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_requests_total 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_endpoint_requests_embed 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_endpoint_requests_rerank 1\n") != null);

    m.setQueueState(3, 6, 2);
    m.recordQueueRejection(4);
    var queue_writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer queue_writer.deinit();
    try m.render(&queue_writer.writer);
    const queue_output = queue_writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, queue_output, "antfly_inference_request_queue_depth 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, queue_output, "antfly_inference_request_queue_capacity 6\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, queue_output, "antfly_inference_request_queue_available 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, queue_output, "antfly_inference_request_queue_active_requests 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, queue_output, "antfly_inference_request_queue_rejections_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, queue_output, "antfly_inference_request_queue_rejected_units_total 4\n") != null);

    m.recordEmbedBatch(3, 12, 1000);
    m.recordEmbedBatch(2, 20, 2000);
    var embed_writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer embed_writer.deinit();
    try m.render(&embed_writer.writer);
    const embed_output = embed_writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, embed_output, "antfly_inference_embed_batches_total 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, embed_output, "antfly_inference_embed_batch_items_total 5\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, embed_output, "antfly_inference_embed_batch_input_bytes_total 32\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, embed_output, "antfly_inference_embed_batch_duration_ns_total 3000\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, embed_output, "antfly_inference_embed_batch_last_items 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, embed_output, "antfly_inference_embed_batch_max_items 3\n") != null);
}
