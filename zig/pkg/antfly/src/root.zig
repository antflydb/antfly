// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
pub const roaring = @import("antfly_local_sources").encoding_roaring;
pub const fst = @import("antfly_fst");
pub const snappy = @import("antfly_local_sources").encoding_snappy;
pub const streamvbyte = @import("antfly_local_sources").encoding_streamvbyte;
pub const simd_bitpack = @import("antfly_local_sources").encoding_simd_bitpack;
pub const chunked_coder = @import("antfly_local_sources").encoding_chunked_coder;

// Vector math & quantization
pub const vector = @import("antfly_vector").vector;
pub const rabitq = @import("antfly_vector").rabitq;
pub const quantizer = @import("antfly_vector").quantizer;
pub const proto = @import("antfly_vector").proto;
pub const vectorindex = @import("antfly_vectorindex");
pub const casbin = @import("antfly_casbin");

// Index sections
pub const inverted = @import("antfly_local_sources").section_inverted;
pub const vector_section = @import("antfly_local_sources").section_vector_section;
pub const doc_values = @import("antfly_local_sources").section_doc_values;
pub const typed_doc_values = @import("antfly_local_sources").section_typed_doc_values;
pub const nested = @import("antfly_local_sources").section_nested;
pub const synonyms = @import("antfly_local_sources").section_synonyms;

// Segment container
pub const segment = @import("antfly_local_sources").segment;

// Columnar stored fields
pub const columnar = @import("columnar.zig");

// Index manager
pub const index = @import("antfly_local_sources").index;
pub const introducer = @import("antfly_local_sources").introducer;
pub const merger = @import("antfly_local_sources").merger;

// Search & query
pub const scorer = @import("antfly_local_sources").search_scorer;
pub const query = @import("antfly_local_sources").search_query;
pub const collector = @import("antfly_local_sources").search_collector;
pub const aggregation = @import("antfly_local_sources").search_aggregation;
pub const geo = @import("antfly_local_sources").search_geo;
pub const analysis = @import("antfly_local_sources").search_analysis;
pub const stopwords = @import("antfly_local_sources").search_stopwords;
pub const stemmers = @import("antfly_local_sources").search_stemmers;
pub const stemmers_validation = @import("antfly_local_sources").search_stemmers_validation_test;
pub const search = @import("antfly_local_sources").search_search;
pub const highlight = @import("antfly_local_sources").search_highlight;
pub const levenshtein = @import("antfly_local_sources").search_levenshtein;
pub const fusion = @import("antfly_local_sources").search_fusion;
pub const regex = @import("antfly_local_sources").search_regex;
pub const query_string = @import("antfly_local_sources").search_query_string;

// Graph
pub const graph = @import("antfly_local_sources").graph_graph;
pub const traversal = @import("antfly_local_sources").graph_traversal;
pub const paths = @import("antfly_local_sources").graph_paths;
pub const graph_query = @import("antfly_local_sources").graph_query;
pub const graph_pattern = @import("antfly_local_sources").graph_pattern;

// Sparse embeddings
pub const sparse = @import("antfly_local_sources").sparse_sparse;

// Inference clients (Antfly, OpenAI/Ollama)
pub const inference = @import("antfly_local_sources").inference_mod;
pub const table_schema = @import("antfly_local_sources").schema_mod;
pub const capi_dependencies = @import("capi_dependencies.zig");
pub const image = @import("antfly_image");
pub const font = @import("antfly_font");
pub const pdf = @import("antfly_pdf");

// Serverless namespace path
pub const serverless = @import("serverless/mod.zig");
pub const serverless_server = @import("serverless/server.zig");
pub const serverless_http_server = @import("serverless_http_server.zig");
pub const serverless_http_client = @import("serverless_http_client.zig");
pub const internal = @import("internal/mod.zig");

// Tracing (TLA+ trace validation)
pub const tracing = @import("tracing/mod.zig");

// Deterministic VOPR contracts, campaign policy, and replay artifacts.
pub const vopr = @import("vopr");
pub const domain_vopr = @import("vopr/domain_vopr.zig");
pub const data_server_vopr = @import("vopr/data_server.zig");
pub const admission_vopr = @import("vopr/admission.zig");
pub const resource_pressure_vopr = @import("vopr/resource_pressure.zig");
pub const object_store_vopr = @import("vopr/object_store.zig");
pub const replication_backfill_vopr = @import("vopr/replication_backfill.zig");
pub const supervision_vopr = @import("vopr/supervision.zig");
pub const auth_lifecycle_vopr = @import("vopr/auth_lifecycle.zig");
pub const serverless_workflow_vopr = @import("vopr/serverless_workflow.zig");
pub const db_index_races_vopr = @import("vopr/db_index_races.zig");
pub const provider_boundaries_vopr = @import("vopr/provider_boundaries.zig");
pub const composed_query_vopr = @import("vopr/composed_query.zig");
pub const query_embedding_cache_vopr = @import("vopr/query_embedding_cache.zig");
pub const production_standby_vopr = @import("vopr/production_standby.zig");
pub const production_cluster_vopr = @import("vopr/production_cluster.zig");
pub const full_cluster_vopr = @import("vopr/full_cluster.zig");
pub const generation_reranking_vopr = @import("vopr/generation_reranking.zig");
pub const distributed_query_vopr = @import("vopr/distributed_query.zig");
pub const parquet_cache_vopr = @import("vopr/parquet_cache.zig");
pub const provisioning_startup_vopr = @import("vopr/provisioning_startup.zig");
pub const restore_admission_vopr = @import("vopr/restore_admission.zig");
pub const generation_lifecycle_vopr = @import("vopr/generation_lifecycle.zig");
pub const backfill_marker_discovery_vopr = @import("vopr/backfill_marker_discovery.zig");
pub const config_extension_lifecycle_vopr = @import("vopr/config_extension_lifecycle.zig");
pub const secrets_vopr = @import("vopr/secrets.zig");
pub const vopr_determinism_audit = @import("vopr/determinism_audit.zig");
pub const external_lake_vopr = @import("vopr/external_lake.zig");
pub const media_runtime_vopr = @import("vopr/media_runtime.zig");
pub const upgrade_compatibility_vopr = @import("vopr/upgrade_compatibility.zig");
pub const request_lifecycle_vopr = @import("vopr/request_lifecycle.zig");
pub const http_lifecycle_vopr = @import("vopr/http_lifecycle.zig");
pub const http_disconnect_vopr = @import("vopr/http_disconnect.zig");

// Raft integration
pub const raft = @import("raft/mod.zig");
pub const raft_vopr = @import("raft/vopr.zig");
pub const admin = @import("admin/mod.zig");
pub const extensions = @import("extensions/mod.zig");
pub const public_api = @import("api/mod.zig");
pub const metadata = @import("metadata/mod.zig");
pub const metadata_api = @import("metadata/api.zig");
pub const metadata_admin = @import("metadata/admin.zig");
pub const metadata_http_routes = @import("metadata/http_routes.zig");
pub const metadata_http_server = @import("metadata/http_server.zig");
pub const metadata_http_client = @import("metadata/http_client.zig");
pub const metadata_service = @import("metadata/service.zig");
pub const metadata_server = @import("metadata/server.zig");
pub const metadata_vopr_harness = @import("metadata/vopr_harness.zig");
pub const metadata_table_workflow = @import("metadata/table_workflow.zig");
pub const metadata_replication_backfill = @import("metadata/replication_backfill.zig");
pub const metadata_placement_planner = @import("metadata/placement_planner.zig");
pub const data = @import("data/mod.zig");
pub const vector_migration = @import("antfly_local_sources").common_vector_migration;
pub const vector_migration_offline = @import("storage/vector_migration_offline.zig");
pub const migration_files = @import("common/migration_files.zig");
pub const standalone = @import("standalone/mod.zig");
pub const inference_runtime = @import("inference_runtime/runtime.zig");
pub const usermgr = @import("usermgr/mod.zig");

// Template rendering (handlebars)
pub const template = @import("antfly_local_sources").template;
pub const bloom = @import("bloom");
pub const jsonschema = @import("antfly_jsonschema");
pub const common = @import("common/mod.zig");
pub const foreign = @import("foreign/mod.zig");
pub const embeddings = @import("antfly_embeddings");
pub const generating = @import("antfly_generating");
pub const generating_runtime = @import("antfly_local_sources").generating_mod;
pub const reranking = @import("antfly_reranking");
pub const reranking_runtime = @import("reranking/mod.zig");
pub const transcribing = @import("antfly_transcribing");
pub const readers = @import("antfly_readers");
pub const extracting = @import("antfly_extracting");
pub const synthesizing = @import("antfly_synthesizing");
pub const asset_producer_runtime = @import("antfly_local_sources").asset_producer_runtime;

// Storage backends
pub const platform_clock = @import("antfly_platform").clock;
pub const platform_sync = @import("antfly_platform").sync;
pub const platform_time = @import("antfly_platform").time;
pub const storage_backend = @import("antfly_local_sources").storage_backend_types;
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const storage_maintenance = @import("antfly_local_sources").storage_maintenance;
pub const storage_backend_scan = @import("antfly_local_sources").storage_backend_scan;
pub const storage_sim_runtime = @import("antfly_local_sources").storage_sim_runtime;
pub const object_storage = @import("antfly_local_sources").storage_object_storage;
pub const host_environment = @import("antfly_local_sources").storage_host_environment;
pub const lite = @import("antfly_local_sources").storage_lite_mod;
pub const lite_backend = lite.backend;
pub const lite_native = lite.native;
pub const storage_lsm = @import("storage/lsm/mod.zig");
pub const mem_backend = @import("antfly_local_sources").storage_mem_backend;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
pub const backend_conformance_test = @import("storage/backend_conformance_test.zig");
pub const lsm_backend_sim_test = @import("storage/lsm_backend_sim_test.zig");
pub const lsm_vopr = @import("storage/lsm_vopr.zig");
pub const hbc = @import("antfly_local_sources").storage_hbc_adapter;
pub const posting_segment_store = @import("antfly_local_sources").storage_posting_segment_store;
pub const vector_block_store = @import("antfly_local_sources").storage_vector_block_store;
pub const hot_standby = @import("storage/hot_standby/mod.zig");
pub const standby_vopr = @import("storage/hot_standby/vopr.zig");
pub const wal = @import("antfly_local_sources").storage_wal;
pub const wal_vopr = @import("storage/wal_vopr.zig");
pub const persistent = @import("antfly_local_sources").storage_persistent;
pub const persistent_vopr = @import("storage/persistent_vopr.zig");
pub const docstore = @import("antfly_local_sources").storage_docstore;
pub const resource_manager = @import("antfly_local_sources").storage_resource_manager;
pub const backup_codec = @import("antfly_local_sources").storage_backup_codec;
pub const backup_bundle = @import("antfly_local_sources").storage_backup_bundle;
pub const backup_bundle_io = @import("antfly_local_sources").storage_backup_bundle_io;
pub const backup_repository = @import("storage/backup_repository.zig");
pub const portable_backup = @import("antfly_local_sources").storage_portable_backup;
pub const internal_keys = @import("antfly_local_sources").storage_internal_keys;
pub const shard = @import("antfly_local_sources").storage_shard;
pub const enrichment = @import("storage/enrichment.zig");
pub const ttl = @import("antfly_local_sources").storage_ttl;
pub const transactions = @import("antfly_local_sources").storage_transactions;
pub const transaction_vopr = @import("storage/transaction_vopr.zig");
pub const schema = @import("antfly_local_sources").storage_schema;
pub const db = @import("antfly_source_root").antfly_sources.selected_db;
pub const index_manager_vopr = @import("storage/index_manager_vopr.zig");
pub const db_split_vopr = @import("storage/db_split_vopr.zig");

test {
    _ = @import("system_catalog/server_call.zig");
    _ = @import("vopr/index_maintenance.zig");
    _ = @import("cmd/serverless.zig");
    // Storage shard builds compile this authoritative discovery root and then
    // select disjoint test-name prefixes. Keep it unconditional in test mode:
    // an unimported test file must fail the pre-build audit, never disappear.
    _ = @import("storage/test_manifest.zig");
    _ = @import("antfly_private_error_diagnostics");

    if (comptime build_options.standalone_runtime_focused_test) {
        _ = standalone;
        return;
    }

    // Encoding
    _ = roaring;
    _ = fst;
    _ = snappy;
    _ = streamvbyte;
    _ = simd_bitpack;
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
    _ = @import("antfly_local_sources").search_pattern_filter;
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
    _ = table_schema;
    _ = @import("antfly_local_sources").chunking_mod;
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
    _ = public_api.row_policy_install;
    _ = public_api.relational_fk_generation_publication;
    _ = public_api.row_policy_publication_coordinator;
    _ = public_api.fk_generation_publication_coordinator;
    _ = public_api.http_server;
    _ = public_api.internal_query_operations;
    _ = public_api.tables;
    _ = public_api.indexes;

    // Raft integration
    _ = raft;
    _ = raft_vopr;
    _ = @import("raft/reconciler.zig");
    _ = extensions;
    _ = @import("extensions/lifecycle.zig");
    _ = metadata;
    _ = vopr;
    _ = metadata_api;
    _ = metadata_admin;
    _ = metadata_http_routes;
    _ = metadata_http_server;
    _ = metadata_http_client;
    _ = metadata_service;
    _ = metadata_server;
    _ = metadata_vopr_harness;
    _ = metadata_table_workflow;
    _ = metadata_replication_backfill;
    _ = metadata_placement_planner;
    _ = data;
    _ = standalone;
    _ = inference_runtime;

    // Template
    _ = template;
    _ = bloom;
    _ = jsonschema;
    _ = common;
    _ = foreign;
    _ = @import("foreign/postgres_libpq.zig");
    _ = embeddings;
    _ = generating;
    _ = generating_runtime;
    _ = reranking;
    _ = reranking_runtime;
    _ = transcribing;
    _ = readers;
    _ = synthesizing;
    _ = asset_producer_runtime;

    // Storage
    _ = hbc;
    _ = hot_standby;
    _ = standby_vopr;
    _ = wal;
    _ = wal_vopr;
    _ = persistent;
    _ = persistent_vopr;
    _ = docstore;
    _ = backup_codec;
    _ = backup_bundle;
    _ = backup_bundle_io;
    _ = backup_repository;
    _ = portable_backup;
    _ = internal_keys;
    _ = shard;
    _ = enrichment;
    _ = ttl;
    _ = transactions;
    _ = transaction_vopr;
    _ = domain_vopr;
    _ = data_server_vopr;
    _ = admission_vopr;
    _ = resource_pressure_vopr;
    _ = object_store_vopr;
    _ = replication_backfill_vopr;
    _ = supervision_vopr;
    _ = auth_lifecycle_vopr;
    _ = serverless_workflow_vopr;
    _ = db_index_races_vopr;
    _ = provider_boundaries_vopr;
    _ = composed_query_vopr;
    _ = query_embedding_cache_vopr;
    _ = full_cluster_vopr;
    _ = production_standby_vopr;
    _ = generation_reranking_vopr;
    _ = distributed_query_vopr;
    _ = parquet_cache_vopr;
    _ = provisioning_startup_vopr;
    _ = restore_admission_vopr;
    _ = generation_lifecycle_vopr;
    _ = backfill_marker_discovery_vopr;
    _ = config_extension_lifecycle_vopr;
    _ = secrets_vopr;
    _ = vopr_determinism_audit;
    _ = external_lake_vopr;
    _ = media_runtime_vopr;
    _ = upgrade_compatibility_vopr;
    _ = request_lifecycle_vopr;
    _ = http_lifecycle_vopr;
    _ = http_disconnect_vopr;
    _ = schema;
    _ = object_storage;
    _ = host_environment;
    _ = storage_lsm;
    _ = storage_backend_erased;
    _ = storage_backend_scan;
    _ = mem_backend;
    _ = lsm_backend;
    _ = storage_maintenance;
    _ = backend_conformance_test;
    _ = lsm_backend_sim_test;
    _ = lsm_vopr;
    _ = db;
    _ = index_manager_vopr;
    _ = db_split_vopr;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

test "online graph snapshot native receiver module" {
    _ = @import("antfly_local_sources").storage_db_online_graph_receiver_test;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
