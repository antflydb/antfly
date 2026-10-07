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

test {
    _ = @import("antfly_local_sources").storage_db_artifact_ids;
    _ = @import("antfly_local_sources").storage_db_lookup_key_scratch;
    _ = @import("antfly_local_sources").storage_db_doc_identity;
    _ = @import("antfly_local_sources").storage_db_document_read_scratch;
    _ = @import("antfly_local_sources").storage_db_derived_derived_worker;
    _ = @import("antfly_local_sources").storage_db_derived_change_journal;
}
