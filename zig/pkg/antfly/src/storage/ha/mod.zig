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

pub const replication_record = @import("replication_record.zig");
pub const replication_log = @import("replication_log.zig");
pub const slot_store = @import("slot_store.zig");
pub const standby = @import("standby.zig");
pub const primary = @import("primary.zig");
pub const session = @import("session.zig");
pub const backup_manifest = @import("backup_manifest.zig");
pub const seed_artifact = @import("seed_artifact.zig");
pub const seed_prefix_cleanup = @import("seed_prefix_cleanup.zig");
pub const seed_activation = @import("seed_activation.zig");
pub const seed_materialization = @import("seed_materialization.zig");
pub const seed_capture = @import("seed_capture.zig");
pub const lifecycle_receipt_ledger = @import("lifecycle_receipt_ledger.zig");
pub const local_generation_gc = @import("local_generation_gc.zig");
pub const mutation_barrier = @import("mutation_barrier.zig");
pub const bootstrap = @import("bootstrap.zig");
pub const status = @import("status.zig");
pub const replication_api = @import("replication_api.zig");
pub const fencing = @import("fencing.zig");
pub const rejoin = @import("rejoin.zig");
pub const commit_gate = @import("commit_gate.zig");
pub const read_gate = @import("read_gate.zig");
pub const write_gate = @import("write_gate.zig");
pub const public_gate_state = @import("public_gate_state.zig");
pub const owner_job_gate = @import("owner_job_gate.zig");
pub const admin = @import("admin.zig");
pub const admin_exec = @import("admin_exec.zig");
pub const operator = @import("operator.zig");
pub const chaos = @import("chaos.zig");
pub const metrics = @import("metrics.zig");
pub const validation = @import("validation.zig");
pub const admin_cli = @import("admin_cli.zig");
pub const compat = @import("compat.zig");
pub const effects = @import("effects.zig");
pub const http_admin = @import("http_admin.zig");
pub const http_internal = @import("http_internal.zig");
pub const http_replication_client = @import("http_replication_client.zig");
pub const http_client = @import("http_client.zig");

test {
    _ = @import("lifecycle_receipt_inventory_test.zig");
    _ = @import("seed_prefix_cleanup_test.zig");
    _ = replication_record;
    _ = replication_log;
    _ = slot_store;
    _ = standby;
    _ = primary;
    _ = session;
    _ = backup_manifest;
    _ = seed_artifact;
    _ = seed_prefix_cleanup;
    _ = seed_activation;
    _ = seed_materialization;
    _ = seed_capture;
    _ = lifecycle_receipt_ledger;
    _ = local_generation_gc;
    _ = mutation_barrier;
    _ = bootstrap;
    _ = status;
    _ = replication_api;
    _ = fencing;
    _ = rejoin;
    _ = commit_gate;
    _ = read_gate;
    _ = write_gate;
    _ = public_gate_state;
    _ = owner_job_gate;
    _ = admin;
    _ = admin_exec;
    _ = operator;
    _ = chaos;
    _ = metrics;
    _ = validation;
    _ = admin_cli;
    _ = compat;
    _ = effects;
    _ = http_admin;
    _ = http_internal;
    _ = http_replication_client;
    _ = http_client;
}
