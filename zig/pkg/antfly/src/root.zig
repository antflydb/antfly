// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

pub const build_options = @import("build_options");

// Encoding & data structures
pub const roaring = @import("encoding/roaring.zig");
pub const vellum = @import("antfly_vellum");
pub const snappy = @import("encoding/snappy.zig");
pub const streamvbyte = @import("encoding/streamvbyte.zig");
pub const chunked_coder = @import("encoding/chunked_coder.zig");

// Vector math & quantization
pub const vector = @import("antfly_vector").vector;
pub const rabitq = @import("antfly_vector").rabitq;
pub const quantizer = @import("antfly_vector").quantizer;
pub const proto = @import("antfly_vector").proto;
pub const vectorindex = @import("antfly_vectorindex");
pub const casbin = @import("antfly_casbin");

// Index sections
pub const inverted = @import("section/inverted.zig");
pub const vector_section = @import("section/vector_section.zig");
pub const doc_values = @import("section/doc_values.zig");
pub const typed_doc_values = @import("section/typed_doc_values.zig");
pub const nested = @import("section/nested.zig");
pub const synonyms = @import("section/synonyms.zig");

// Segment container
pub const segment = @import("segment.zig");

// Columnar stored fields
pub const columnar = @import("columnar.zig");

// Index manager
pub const index = @import("index.zig");
pub const introducer = @import("introducer.zig");
pub const merger = @import("merger.zig");

// Search & query
pub const scorer = @import("search/scorer.zig");
pub const query = @import("search/query.zig");
pub const collector = @import("search/collector.zig");
pub const aggregation = @import("search/aggregation.zig");
pub const geo = @import("search/geo.zig");
pub const analysis = @import("search/analysis.zig");
pub const stopwords = @import("search/stopwords.zig");
pub const stemmers = @import("search/stemmers.zig");
pub const stemmers_validation = @import("search/stemmers_validation_test.zig");
pub const search = @import("search/search.zig");
pub const highlight = @import("search/highlight.zig");
pub const levenshtein = @import("search/levenshtein.zig");
pub const fusion = @import("search/fusion.zig");
pub const regex = @import("search/regex.zig");
pub const query_string = @import("search/query_string.zig");

// Graph
pub const graph = @import("graph/graph.zig");
pub const traversal = @import("graph/traversal.zig");
pub const paths = @import("graph/paths.zig");
pub const graph_query = @import("graph/query.zig");
pub const graph_pattern = @import("graph/pattern.zig");

// Sparse embeddings
pub const sparse = @import("sparse/sparse.zig");

// Inference clients (Termite, OpenAI/Ollama)
pub const inference = @import("inference/mod.zig");
pub const table_schema = @import("schema/mod.zig");
pub const image = @import("antfly_image");
pub const font = @import("antfly_font");
pub const pdf = @import("antfly_pdf");

// Serverless namespace path
pub const serverless = @import("serverless/mod.zig");
pub const serverless_server = @import("serverless/server.zig");
pub const serverless_http_server = @import("serverless_http_server.zig");
pub const serverless_http_client = @import("serverless_http_client.zig");

// Tracing (TLA+ trace validation)
pub const tracing = @import("tracing/mod.zig");

// Raft integration
pub const raft = @import("raft/mod.zig");
pub const public_api = @import("api/mod.zig");
pub const metadata = @import("metadata/mod.zig");
pub const metadata_api = @import("metadata/api.zig");
pub const metadata_admin = @import("metadata/admin.zig");
pub const metadata_http_routes = @import("metadata/http_routes.zig");
pub const metadata_http_server = @import("metadata/http_server.zig");
pub const metadata_http_client = @import("metadata/http_client.zig");
pub const metadata_service = @import("metadata/service.zig");
pub const metadata_server = @import("metadata/server.zig");
pub const metadata_sim_harness = @import("metadata/sim_harness.zig");
pub const metadata_table_workflow = @import("metadata/table_workflow.zig");
pub const metadata_replication_backfill = @import("metadata/replication_backfill.zig");
pub const metadata_placement_planner = @import("metadata/placement_planner.zig");
pub const data = @import("data/mod.zig");
pub const swarm = @import("swarm/mod.zig");
pub const termite_runtime = @import("termite/runtime.zig");
pub const usermgr = @import("usermgr/mod.zig");

// Template rendering (handlebars)
pub const template = @import("template.zig");
pub const bloom = @import("bloom");
pub const jsonschema = @import("antfly_jsonschema");
pub const common = @import("common/mod.zig");
pub const foreign = @import("foreign/mod.zig");
pub const embeddings = @import("antfly_embeddings");
pub const generating = @import("antfly_generating");
pub const generating_runtime = @import("generating/mod.zig");
pub const reranking = @import("antfly_reranking");
pub const reranking_runtime = @import("reranking/mod.zig");
pub const transcribing = @import("antfly_transcribing");
pub const synthesizing = @import("antfly_synthesizing");

// Storage backends
pub const platform_clock = @import("platform/clock.zig");
pub const platform_time = @import("platform/time.zig");
pub const storage_backend = @import("storage/backend_types.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const storage_backend_scan = @import("storage/backend_scan.zig");
pub const storage_sim_runtime = @import("storage/sim_runtime.zig");
pub const object_storage = @import("storage/object_storage.zig");
pub const host_environment = @import("storage/host_environment.zig");
pub const storage_lsm = @import("storage/lsm/mod.zig");
pub const lmdb_backend = @import("storage/lmdb_backend.zig");
pub const mem_backend = @import("storage/mem_backend.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const backend_conformance_test = @import("storage/backend_conformance_test.zig");
pub const lsm_backend_sim_test = @import("storage/lsm_backend_sim_test.zig");
pub const lmdb = @import("storage/lmdb.zig");
pub const lmdb_engine = @import("lmdb_engine");
pub const hbc = @import("storage/hbc_adapter.zig");
pub const wal = @import("storage/wal.zig");
pub const persistent = @import("storage/persistent.zig");
pub const docstore = @import("storage/docstore.zig");
pub const resource_manager = @import("storage/resource_manager.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const portable_backup = @import("storage/portable_backup.zig");
pub const internal_keys = @import("storage/internal_keys.zig");
pub const shard = @import("storage/shard.zig");
pub const enrichment = @import("storage/enrichment.zig");
pub const ttl = @import("storage/ttl.zig");
pub const transactions = @import("storage/transactions.zig");
pub const schema = @import("storage/schema.zig");
pub const db = @import("storage/db/mod.zig");

test {
    if (comptime build_options.swarm_runtime_focused_test) {
        _ = swarm;
        return;
    }

    // Encoding
    _ = roaring;
    _ = vellum;
    _ = snappy;
    _ = streamvbyte;
    _ = chunked_coder;

    // Vector
    _ = vector;
    _ = rabitq;
    _ = quantizer;
    _ = proto;
    _ = vectorindex;
    _ = casbin;

    // Sections
    _ = inverted;
    _ = vector_section;
    _ = doc_values;
    _ = typed_doc_values;
    _ = nested;
    _ = synonyms;

    // Segment
    _ = segment;

    // Columnar
    _ = columnar;

    // Index
    _ = index;
    _ = introducer;
    _ = merger;

    // Search & query
    _ = scorer;
    _ = query;
    _ = collector;
    _ = aggregation;
    _ = geo;
    _ = analysis;
    _ = stopwords;
    _ = stemmers;
    _ = stemmers_validation;
    _ = search;
    _ = highlight;
    _ = levenshtein;
    _ = fusion;
    _ = regex;
    _ = query_string;
    _ = @import("hbc_recall_test.zig");

    // Graph
    _ = graph;
    _ = traversal;
    _ = paths;
    _ = graph_query;
    _ = graph_pattern;

    // Sparse
    _ = sparse;

    // Inference
    _ = inference;
    _ = pdf;

    // Serverless
    _ = serverless;
    _ = serverless_server;
    _ = serverless_http_server;
    _ = serverless_http_client;

    // Tracing
    _ = tracing;

    // Public API
    _ = public_api;
    _ = public_api.http_server;

    // Raft integration
    _ = raft;
    _ = metadata;
    _ = metadata_api;
    _ = metadata_admin;
    _ = metadata_http_routes;
    _ = metadata_http_server;
    _ = metadata_http_client;
    _ = metadata_service;
    _ = metadata_server;
    _ = metadata_sim_harness;
    _ = metadata_table_workflow;
    _ = metadata_replication_backfill;
    _ = metadata_placement_planner;
    _ = data;
    _ = swarm;
    _ = termite_runtime;

    // Template
    _ = template;
    _ = bloom;
    _ = jsonschema;
    _ = common;
    _ = foreign;
    _ = embeddings;
    _ = generating;
    _ = generating_runtime;
    _ = reranking;
    _ = reranking_runtime;
    _ = transcribing;
    _ = synthesizing;

    // Storage
    _ = lmdb;
    _ = lmdb_engine;
    _ = hbc;
    _ = wal;
    _ = persistent;
    _ = docstore;
    _ = backup_codec;
    _ = portable_backup;
    _ = internal_keys;
    _ = shard;
    _ = enrichment;
    _ = ttl;
    _ = transactions;
    _ = schema;
    _ = object_storage;
    _ = host_environment;
    _ = storage_lsm;
    _ = storage_backend_erased;
    _ = storage_backend_scan;
    _ = mem_backend;
    _ = lsm_backend;
    _ = backend_conformance_test;
    _ = lsm_backend_sim_test;
    _ = db;
}
