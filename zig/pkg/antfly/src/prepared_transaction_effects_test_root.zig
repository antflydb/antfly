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

// The local source partition owns the codec tests and their allocations.
test {
    _ = @import("antfly_local_sources").storage_prepared_transaction_effects;
    _ = @import("antfly_local_sources").storage_transactions;
    _ = @import("antfly_local_sources").storage_mem_backend;
    _ = @import("antfly_local_sources").storage_transaction_batch_admission;
    _ = @import("antfly_local_sources").storage_db_artifact_authored_acceptance;
    _ = @import("antfly_local_sources").storage_db_group_source_mutations;
    _ = @import("antfly_local_sources").storage_db_relational_index_records;
    _ = @import("antfly_local_sources").storage_db_doc_identity;
    _ = @import("antfly_local_sources").storage_docstore;
    _ = @import("antfly_local_sources").storage_db_document_child_range_effects;
    _ = @import("antfly_local_sources").storage_db_document_child_range_manifest;
}
