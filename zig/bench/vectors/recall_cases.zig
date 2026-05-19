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

const common = @import("recall_common.zig");

pub const RecallCase = struct {
    dataset: []const u8,
    randomize: bool = false,
    top_k: usize = 10,
    count: usize,
    tolerance: f64 = 0.500001,
    expected: common.MetricStats,
};

pub const quantizer_cases = [_]RecallCase{
    .{ .dataset = "images-512d-10k.gob", .count = 1000, .expected = .{ .euclidean = 70.00, .inner_product = 70.00, .cosine = 69.50 } },
    .{ .dataset = "images-512d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 85.00, .inner_product = 85.00, .cosine = 85.00 } },
    .{ .dataset = "random-20d-1k.gob", .count = 1000, .expected = .{ .euclidean = 88.00, .inner_product = 93.50, .cosine = 89.00 } },
    .{ .dataset = "random-20d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 88.50, .inner_product = 89.00, .cosine = 88.50 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .count = 1000, .expected = .{ .euclidean = 76.00, .inner_product = 75.00, .cosine = 70.50 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 87.50, .inner_product = 87.00, .cosine = 85.50 } },
    .{ .dataset = "fashionminst-784d-10k.gob", .count = 1000, .expected = .{ .euclidean = 67.50, .inner_product = 83.00, .cosine = 66.50 } },
    .{ .dataset = "fashionminst-784d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 80.50, .inner_product = 90.00, .cosine = 83.00 } },
    .{ .dataset = "laionclip-768d-1k.gob", .count = 1000, .expected = .{ .euclidean = 70.50, .inner_product = 71.50, .cosine = 70.50 } },
    .{ .dataset = "laionclip-768d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 81.50, .inner_product = 80.50, .cosine = 81.00 } },
    .{ .dataset = "laiongemini-1408d-1k.gob", .count = 1000, .tolerance = 1.50001, .expected = .{ .euclidean = 66.00, .inner_product = 66.00, .cosine = 66.00 } },
    .{ .dataset = "laiongemini-1408d-1k.gob", .randomize = true, .count = 1000, .tolerance = 1.50001, .expected = .{ .euclidean = 79.50, .inner_product = 79.00, .cosine = 79.00 } },
    .{ .dataset = "laiongemini-512d-10k.gob", .count = 1000, .tolerance = 1.50001, .expected = .{ .euclidean = 70.00, .inner_product = 70.00, .cosine = 70.00 } },
    .{ .dataset = "laiongemini-512d-10k.gob", .randomize = true, .count = 1000, .tolerance = 1.50001, .expected = .{ .euclidean = 72.50, .inner_product = 72.00, .cosine = 72.00 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .count = 1000, .expected = .{ .euclidean = 81.50, .inner_product = 81.50, .cosine = 81.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 85.00, .inner_product = 85.00, .cosine = 85.00 } },
    .{ .dataset = "random-2048d-1k.gob", .count = 1000, .tolerance = 1.0, .expected = .{ .euclidean = 49.50, .inner_product = 46.50, .cosine = 47.50 } },
    .{ .dataset = "random-2048d-1k.gob", .randomize = true, .count = 1000, .tolerance = 1.0, .expected = .{ .euclidean = 35.50, .inner_product = 30.50, .cosine = 30.50 } },
    .{ .dataset = "random-4096d-1k.gob", .count = 1000, .tolerance = 1.0, .expected = .{ .euclidean = 42.50, .inner_product = 39.00, .cosine = 38.00 } },
    .{ .dataset = "random-4096d-1k.gob", .randomize = true, .count = 1000, .tolerance = 1.0, .expected = .{ .euclidean = 37.00, .inner_product = 34.00, .cosine = 33.50 } },
};

pub const hbc_cases = [_]RecallCase{
    .{ .dataset = "images-512d-10k.gob", .count = 1000, .expected = .{ .euclidean = 96.00, .inner_product = 94.00, .cosine = 94.50 } },
    .{ .dataset = "images-512d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 100.00, .cosine = 99.50 } },
    .{ .dataset = "random-20d-1k.gob", .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 99.50, .cosine = 98.50 } },
    .{ .dataset = "random-20d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 97.50, .inner_product = 99.00, .cosine = 97.50 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .count = 1000, .expected = .{ .euclidean = 97.50, .inner_product = 70.00, .cosine = 93.50 } },
    .{ .dataset = "fashionminst-784d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 99.50, .cosine = 100.00 } },
    .{ .dataset = "fashionminst-784d-10k.gob", .count = 1000, .expected = .{ .euclidean = 95.50, .inner_product = 56.00, .cosine = 97.00 } },
    .{ .dataset = "fashionminst-784d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 100.00, .cosine = 100.00 } },
    .{ .dataset = "laionclip-768d-1k.gob", .count = 1000, .expected = .{ .euclidean = 97.00, .inner_product = 96.00, .cosine = 90.50 } },
    .{ .dataset = "laionclip-768d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.00, .inner_product = 99.50, .cosine = 97.00 } },
    .{ .dataset = "laiongemini-1408d-1k.gob", .count = 1000, .expected = .{ .euclidean = 96.50, .inner_product = 93.00, .cosine = 87.50 } },
    .{ .dataset = "laiongemini-1408d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 97.50, .cosine = 97.00 } },
    .{ .dataset = "laiongemini-512d-10k.gob", .count = 10_000, .tolerance = 1.5, .expected = .{ .euclidean = 85.10, .inner_product = 82.20, .cosine = 78.75 } },
    .{ .dataset = "laiongemini-512d-10k.gob", .randomize = true, .count = 10_000, .tolerance = 1.5, .expected = .{ .euclidean = 87.55, .inner_product = 86.70, .cosine = 88.35 } },
    .{ .dataset = "wikiarticles-768d-10k.gob", .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 98.00, .cosine = 98.50 } },
    .{ .dataset = "wikiarticles-768d-10k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 99.50, .inner_product = 99.00, .cosine = 97.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 99.50, .cosine = 98.50 } },
    .{ .dataset = "dbpedia-1536d-1k.gob", .randomize = true, .count = 1000, .expected = .{ .euclidean = 100.00, .inner_product = 100.00, .cosine = 99.50 } },
};
