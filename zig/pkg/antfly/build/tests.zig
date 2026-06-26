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

const std = @import("std");

pub const DBTestStep = struct {
    name: []const u8,
    description: []const u8,
    filters: []const []const u8,
    simple_runner: bool = false,
};

pub const DBRootTestStep = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
};

pub const DBRootModuleTestSteps = struct {
    result_shape: *std.Build.Step.Run,
};

pub const DBStorageTestSteps = struct {
    all: *std.Build.Step.Run,
    sim: *std.Build.Step.Run,
};

pub const db_root_step_name = "lib-db-test";
pub const db_result_shape_step_name = "lib-db-result-shape-test";
pub const db_storage_step_name = "db-test";
pub const db_sim_step_name = "db-sim-test";

pub const DBTestFilters = struct {
    // Keep these buckets at module/category granularity. New DB tests should
    // normally join an owning module or stable prefix instead of adding a
    // one-off title here or in build.zig.
    pub const root = [_][]const u8{
        "storage.db.db.test.",
    };

    pub const enrichment = [_][]const u8{
        "storage.db.enrichment.enrichment_runtime.test.db enrichment runtime ",
        "storage.db.derived_async.test.db derived async ",
        "storage.db.split_restore_test.test.db split cutover",
        "storage.db.split_restore_test.test.db merge-style cutover",
    };

    pub const enrichment_worker = [_][]const u8{
        "storage.db.enrichment.enrichment_runtime.test.db enrichment runtime ",
    };

    pub const enrichment_replay = [_][]const u8{
        "storage.db.derived_async.test.db derived async ",
    };

    pub const enrichment_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover",
        "storage.db.split_restore_test.test.db merge-style cutover",
    };

    pub const enrichment_split_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover enrichment ",
    };

    pub const enrichment_merge_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db merge-style cutover enrichment ",
    };

    pub const enrichment_split_cutover_reopen = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover enrichment resume ",
    };

    pub const enrichment_merge_cutover_reopen = [_][]const u8{
        "storage.db.split_restore_test.test.db merge-style cutover enrichment resume ",
    };

    pub const query = [_][]const u8{
        "storage.db.db.test.db full-text",
        "storage.db.db.test.db dense ",
        "storage.db.db.test.db sparse ",
        "storage.db.db.test.db search ",
        "storage.db.db.test.db document _edges",
        "storage.db.db.test.db document _embeddings",
        "storage.db.graph_runtime.test.",
        "storage.db.search_runtime.test.db search runtime indexing ",
        "storage.db.search_runtime.test.db search runtime graph composition ",
        "storage.db.search_runtime.test.db search runtime text schema ",
        "storage.db.search_runtime.test.db search runtime identity ",
    };

    pub const result_shape = [_][]const u8{
        "db query result shape ",
    };

    pub const txn = [_][]const u8{
        "storage.db.transactions.test.",
        "storage.db.relational_integrity.test.db relational integrity transaction ",
        "storage.db.relational_integrity.test.db relational integrity constraints ",
        "storage.db.write_path.test.db write path ",
        "storage.db.maintenance.ttl_runtime.test.",
    };

    pub const sim = [_][]const u8{
        "storage.db.db_sim_test.test.",
    };

    pub const split_replay_fixtures = [_][]const u8{
        "storage.db.db_sim_test.test.db split replay ",
    };

    pub const split_restore_lifecycle = [_][]const u8{
        "storage.db.split_restore_test.test.",
    };

    pub const schema = [_][]const u8{
        "storage.db.schema_runtime.test.",
        "storage.db.relational_integrity.test.",
    };

    pub const write_path = [_][]const u8{
        "storage.db.write_path.test.",
    };

    pub const foreign_key = [_][]const u8{
        "foreign key",
    };

    pub const temporal = [_][]const u8{
        "storage.db.relational_integrity.test.db relational temporal",
    };

    pub const relational_rows = [_][]const u8{
        "storage.db.relational_rows.test.",
    };

    pub const search_runtime = [_][]const u8{
        "storage.db.search_runtime.test.",
    };

    pub const graph_runtime = [_][]const u8{
        "storage.db.graph_runtime.test.",
    };

    pub const resolution_runtime = [_][]const u8{
        "storage.db.resolution_runtime.test.db resolution runtime ",
    };

    pub const derived_async = [_][]const u8{
        "storage.db.derived_async.test.",
    };

    pub const lifecycle = [_][]const u8{
        "storage.db.lifecycle.test.",
    };

    pub const reopen = [_][]const u8{
        "storage.db.search_runtime.test.db search runtime reopen ",
    };

    pub const ttl_runtime = [_][]const u8{
        "storage.db.maintenance.ttl_runtime.test.",
    };

    pub const enrichment_any = [_][]const u8{
        "enrichment",
    };
};

pub const APITestFilters = struct {
    pub const docid = [_][]const u8{
        "api table reads reject stale doc identity before multigroup fanout",
        "distributed table reads reject stale doc identity before multigroup fanout",
        "api public table query rejects only top-level internal fields",
        "single embeddings index encoder scopes isolated enrichment failure to one index",
        "api query contract rejects doc identity control fields when with relaxes schema",
        "api query contract public parser rejects internal shard doc identity controls",
        "api distributed graph hydrate carries identity generation and clears cross-range ordinals",
        "distributed graph metric status merge validates metadata compatibility",
        "distributed graph rejects doc identity rebuild before cross-range fanout",
        "distributed graph rejects unstamped result refs before cross-range fanout",
        "distributed graph edge reader carries identity generation",
        "query merge preserves common identity read generation",
        "query encoder does not expose internal doc ordinals",
        "graph edge local read rejects stale identity generation",
        "catalog doc identity readiness checks table range health",
        "catalog resolved filter validation accepts preserved split identity domains",
        "metadata merge request validation rejects incompatible doc identity namespaces",
        "metadata merge validation handles rolling mixed-version doc identity status fixtures",
        "metadata split request validation rejects stale doc identity namespace",
        "metadata reconciler does not automatically split ordinal exhausted doc identity",
        "metadata state classifies mixed-version doc identity lifecycle reports",
        "metadata state marks doc identity rebuild required on range namespace mismatch",
        "metadata http server rejects split and merge during active doc identity reassignment before source mutation",
        "metadata http server serves status and filtered admin routes",
        "metadata http server maps source split merge doc identity conflicts",
        "metadata http client preserves split merge doc identity conflicts",
        "metadata http client parses legacy range records without doc identity fields",
        "metadata http client round-trips range doc identity fields",
        "metadata http client round-trips server endpoints",
        "table workflow doc identity guards reject active transition intents",
        "table workflow doc identity lifecycle handles mixed-version transition status",
        "metadata reconciler doc identity guards block new planning during active reassignment",
        "metadata reconciler does not upsert desired split with stale doc identity namespace",
        "metadata reconciler allows explicit merge with doc identity reassignment opt-in",
        "replay batcher tuple map keys preserve embedded delimiters",
        "db chunk cache keys preserve embedded separators",
        "enrichment worker chunk cache keys preserve embedded separators",
        "search request text stats keys preserve embedded separators",
        "merge distributed background text stats keys preserve embedded separators",
        "graph edge local read rejects stale identity namespace",
        "dense metadata keys preserve embedded index separators",
        "dense metadata lookups read legacy textual rows",
        "distributed txn participant ids preserve embedded group markers",
        "distributed join unmatched worker pages group-local right hits",
        "distributed join follow-up pagination requires stamped identity request",
        "distributed join group-local hit pagination reuses structured search generation",
        "distributed right join unmatched tracking uses ordinal identity keys",
        "distributed join unmatched worker prefers local search results over query envelopes",
        "distributed join rejects doc identity rebuild before right-table fanout",
        "distributed join stateful shuffle rejects doc identity rebuild before worker dispatch",
        "internal worker doc identity exchange audit covers every boundary",
        "internal group write routes map shard doc identity mismatch to conflict",
        "internal group join routes map doc identity mismatch to conflict",
        "internal group read routes map doc identity mismatch to conflict",
        "api http client preserves group doc identity conflicts",
        "aggregation context rejects non-current identity generation",
        "aggregation full-result rerun can reuse snapped result identity generation",
        "explicit text stats requests preserve identity generation",
        "explicit text stats requests carry resolved doc filters and apply exact projection",
        "explicit text stats requests reject stale identity generation",
        "algebraic partial request fails closed when lifecycle is stale",
        "algebraic partial request accepts current identity generation and rejects stale",
        "provisioned distributed aggregations collect path terms nested cardinality",
        "algebraic distributed planner selects identity-stamped derived join tensor program",
        "algebraic derived join tensor reads subtract identity tombstones at generation",
        "planner rejects rebuild-required schema lifecycle state",
        "algebraic adaptive progress marks rebuild required on schema drift",
        "remote simple vector query uses vector worker route",
        "encode query request serializes internal resolved doc filters with wire context",
        "simple vector shard request carries serializable resolved doc filter",
        "api http server maps public query doc identity mismatch to unavailable",
        "api http server maps retrieval agent doc identity mismatch to unavailable",
        "api http server query builder maps doc identity mismatch to unavailable",
        "api http server surfaces structured doc identity conflicts for transaction commits",
        "internal group vector worker rejects unsupported identity generation",
        "internal group graph expand rejects unsupported identity generation",
        "distributed graph expand request preserves algebraic semiring planning flag",
        "batch identity metadata delete observes buffered resurrection state",
        "identity validation accepts missing canonical rows but rejects conflicts",
        "identity allocation rejects canonical row conflicts before reserving ordinal",
        "batch identity metadata fails closed at ordinal capacity",
        "identity namespace reassignment preserves snapshot generations and rejects stale writers",
        "near-u32 ordinal pressure preserves sparse high ordinal state through reassignment",
        "storage.db.lifecycle.test.db lifecycle doc identity ",
        "storage.db.write_path.test.db write path doc identity ",
        "storage.db.transactions.test.db transactions doc identity ",
        "storage.db.split_restore_test.test.db split restore doc identity ",
        "export and import preserves doc identity metadata",
        "import rejects doc identity metadata with invalid canonical ids",
        "import rejects doc identity namespace mismatch unless preserving existing namespace",
        "storage.db.search_runtime.test.db search runtime identity ",
        "doc filter wire round-trips ordinal and doc-key filters",
        "doc filter wire rejects old required-field fixtures but tolerates additive fields",
        "doc filter wire rejects invalid ordinal fixtures from mixed-version senders",
        "dense vector id ignores ordinal metadata for a different doc",
        "dense metadata prefetch includes legacy ordinal vector ids",
        "db dense artifact rebuild preserves stable vector ids distinct from ordinals",
        "db sparse index keeps physical doc nums distinct from doc identity ordinals",
        "native dense constraints fail closed without ordinal vector mapping",
        "native constraints fail closed when resolved ordinals cannot be represented",
        "native sparse constraints fail closed without ordinal doc num mapper",
        "native sparse constraints map resolved ordinals to physical doc nums",
        "match_all candidate ordinal lookup uses identity read generation",
        "match_all consumes resolved ordinal filters without doc id projection",
        "native constraints pass identity generation to doc-set id projection",
        "native constraints pass identity read generation to live doc filtering",
        "native constraints treat resolved all-doc exclusion as empty candidates",
        "native sparse constraints keep explicit doc ids when identity coverage is incomplete",
        "text resolved doc filter projection passes identity generation to live filtering",
        "text native constraints fall back for mixed ordinal sidecar coverage",
        "text native constraints fail closed when resolved ordinals cannot be projected",
        "text native constraints treat resolved all-doc exclusion as empty candidates",
        "segment doc ordinal sidecar roundtrip and merge preserve live order",
        "db text compaction preserves ordinal filters across reopen",
        "structured filter doc set cache returns owned clones",
        "structured filter doc set cache separates shared namespace generation keys",
        "cache invalidates ownership move prefix without reviving pinned generations",
        "applyGraphUnion deduplicates by ordinals when hit pages are complete",
        "applyGraphIntersection uses ordinals when hit pages are complete",
        "query merge preserves single-result doc ordinals",
        "fuseNamedSets deduplicates aliases by ordinal when complete",
        "graph result_ref fails closed when unbounded resolved doc-set cannot project",
        "graph result_ref uses complete node doc-set when hits are paged",
        "graph query result doc-set resolution receives identity generation",
        "provisioned direct read db opens reject stale identity namespace",
        "provisioned query runtime db rejects stale identity namespace",
    };

    pub const transactions_docid = [_][]const u8{
        "transaction read snapshot map keys preserve embedded delimiters",
        "transaction session commit response includes retry hints for doc identity availability conflicts",
        "transaction session registry persists SQL catalog session state and savepoints restore it",
        "catalog source resolves foreign key ref owner groups",
        "catalog source resolves unique constraint owner groups",
        "catalog source promotes unique constraint with table schema compare and swap",
        "txn prepare parser round-trips constraint participant intents",
        "foreign key ref children request and response round-trip cursors",
        "foreign key action schedule ids include the mutating action",
        "distributed txn coordinator registers foreign key parent participants",
        "distributed txn coordinator externalizes deferred foreign key parent checks exactly",
        "single-table distributed txn coordinator registers foreign key parent groups",
        "distributed txn coordinator routes foreign key child writes through ref owners when configured",
        "distributed txn coordinator fails closed for transitional foreign key ref owner ranges",
        "distributed txn coordinator routes old and new foreign key refs with versioned child rows",
        "distributed txn coordinator routes unique-touching transforms with row proofs",
        "distributed txn coordinator routes unique constraint writes through owner ranges",
        "distributed txn coordinator routes unique owner handoff with row version proofs",
        "distributed txn coordinator allows non-unique transforms on multi-range unique tables",
        "distributed txn coordinator allows single-range unique writes to use local enforcement",
        "distributed txn coordinator rejects non-primary foreign key parent writes without unique owner topology",
        "distributed txn coordinator rejects partial match full composite foreign key writes before prepare",
        "distributed txn coordinator routes foreign key checks through unique owner ranges",
        "distributed txn coordinator routes cross-table foreign key checks through parent unique owner ranges",
        "distributed txn coordinator routes unique foreign key parent updates through ref owners",
        "distributed txn coordinator schedules mutating unique foreign key parent updates through ref owners",
        "distributed txn coordinator routes cross-table composite foreign key checks through parent unique owner ranges",
        "distributed txn coordinator routes cross-table composite foreign key checks through parent primary key owner ranges",
        "distributed txn coordinator routes unique foreign key parent deletes through ref owners",
        "distributed txn coordinator routes cross-table unique foreign key parent deletes through ref owners",
        "distributed txn coordinator routes unique foreign key set-null parent deletes through ref owners",
        "distributed txn coordinator routes unique foreign key cascade parent deletes through ref owners",
        "distributed txn explain routes restrict parent deletes through ref owners",
        "distributed txn explain fails closed on incomplete routed ref owner scans",
        "distributed txn coordinator routes foreign key reference transforms with final-value planning",
        "distributed txn coordinator allows non-reference transforms on foreign key tables",
        "distributed txn coordinator fails closed without foreign key ref owner parent delete ranges",
        "distributed txn coordinator routes foreign key parent deletes through ref owners when configured",
        "distributed txn coordinator routes deferred foreign key parent deletes through ref owners when configured",
        "distributed txn coordinator fails closed for transitional foreign key ref owner parent deletes",
        "distributed txn coordinator ignores unrelated foreign key child tables for parent delete planning",
        "distributed txn coordinator routes distributed foreign key set-null actions across child ranges",
        "foreign key action page executes owner ref cleanup and child mutation through routed participants",
        "foreign key action page fails closed for transitional ref owner topology",
        "foreign key action page routes update cascade child mutations with replacement parent key",
        "foreign key action page schedules recursive cascade work for deleted children",
        "foreign key action page accepts same table runtime parent identity for durable schedules",
        "distributed txn relational identity workload mixes owner topology churn and actions",
        "distributed txn coordinator routes distributed foreign key cascade actions across child ranges",
        "distributed txn coordinator rejects distributed foreign key cascade actions without ref owner topology",
    };

    pub const table_writes_docid = [_][]const u8{
        "table write index parser keeps full text field metadata out of storage config",
        "api auto bulk ingest does not open sessions for normal online writes",
        "weak sync levels do not drain managed db after batch",
        "provisioning detects model backed graph shorthand assets inside config_json strings",
        "provisioning does not require asset producer for copy graph shorthand assets inside config_json strings",
        "provisioned table write source rejects stale doc identity namespace before write",
        "bound table write source backs up and restores a local table",
        "bound table write source backs up and restores a portable local table",
        "provisioned table write source backs up a portable local table",
        "provisioned table restore rejects mismatched doc identity namespace",
        "provisioned restore repair open rejects stale doc identity namespace",
        "write cache reserves retirement slots when pruning multiple leased generations",
        "full text memory attribution aggregation includes norm bytes",
        "table write source core forwards required batch and defaults optional capabilities",
        "primary lookup adopts seeded write cache across visible generation bump",
        "provisioned table write source coalesces same-group waiters",
        "provisioned table write coalescer isolates failed waiters",
        "unique integrity owner topology inspection reports active and transitional ranges",
        "foreign key integrity plan clips requested span to table ranges",
        "foreign key integrity worker plan includes active owner ranges",
        "foreign key integrity stable job id uses planned action and bounds",
        "foreign key integrity diagnostics deduplicates violation samples",
        "secondary index rebuild worker helper claims repairs and finishes range",
        "provisioned secondary index rebuild worker pass repairs projected catalog range",
        "provisioned schema rewrite worker pass drains projected catalog range job",
        "schema rewrite worker pass treats unclaimed terminal jobs as terminal",
        "secondary index promotion ignores stale ready rebuild generation",
        "foreign key integrity job diagnostics merge samples across passes",
        "foreign key integrity job records diagnostics across incomplete passes",
        "unique schema controller maintenance",
        "foreign key schema controller maintenance",
        "foreign key schema controller maintenance resumes durable action job",
        "provisioned foreign key action job drains owner range page",
        "provisioned same-table foreign key action job routes runtime parent through catalog owner range",
        "provisioned table write source routes same-owner identity rewrites and rejects cross-owner rewrites",
        "provisioned table write source routes cross-table rows insert source through catalog owners",
        "recursive cte joined mutation source executes through typed read materialization and write staging",
        "provisioned table write source stages relational mutation source on single owner range",
        "provisioned table write source globally plans relational mutation source across ranges",
        "hosted provisioned table write source globally plans relational mutation source across local owner ranges",
        "provisioned table write source consistent visibility hook does not block on busy apply lock",
        "provisioned table write source consistent visibility refreshes stale dense status",
    };

    pub const provisioned_query_visibility = [_][]const u8{
        "provisioned query visibility ",
    };

    pub const table_reads_docid = [_][]const u8{
        "provisioned read cache keys entries by lsm root generation",
        "provisioned read cache keys entries by identity namespace",
        "provisioned read cache invalidates repeated ownership moves with pinned leases",
        "provisioned read cache clear preserves in-flight pending opens and bumps epoch",
        "provisioned read cache invalidate removes entries without dropping pending opens",
        "provisioned read cache retires invalidated entries until the last lease is released",
        "provisioned read cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails",
        "provisioned query runtime db opens with catalog identity namespace",
        "provisioned query runtime db rejects stale identity namespace",
        "fanout planner uses io cap and request shape",
        "merge distributed text stats sums shard corpus stats by field and term",
        "merge distributed background text stats keys preserve embedded separators",
        "graph hydrate resolved doc filter applies include and exclude sets",
        "provisioned table read source executes relational row query plans across ranges",
        "routed rows query plan executes over scanned owner rows with ctes",
        "external lake rows query and aggregate plans route through lake scan hook",
        "pinned external lake rows scanner validates schema binding against inventory",
        "object storage pinned external lake source routes row plans through scanner",
        "owned object storage lake source discovers and pins parquet prefix inventory",
        "opened object storage lake source owns store and pins parquet prefix inventory",
        "external lake routing source resolves object store for external row plans",
        "configured external lake resolver opens credentialed filesystem connection",
        "lowered sql cross-table read plans execute through routed scans",
        "lowered sql set operation plans preserve overlapping union all rows",
        "lowered sql set operation materialization admission distinguishes spill from hard caps",
        "lowered sql recursive cte materialization admission uses stream spill policy",
        "lowered sql insert source plans build batches from routed scans",
        "lowered sql merge mutation plans build batches from routed scans",
        "lowered relation population plans execute routed typed read sources",
        "lowered document sql read plans execute native lookup and bounded scan",
        "document sql catalog read producers treat catalog misses as terminal",
        "lowered document sql aggregate executes native grouped avg materialization",
        "lowered document sql aggregate uses catalog target for non-default namespace materialization",
        "remote document algebraic aggregate preserves typed unavailable and not found errors",
        "internal group document algebraic aggregate route preserves typed errors",
        "document algebraic aggregate fan-in merges raw grouped averages before applying limit",
        "document algebraic aggregate fan-in preserves empty scalar aggregate semantics",
        "parseRemoteSearchResult preserves fused index scores",
        "hosted remote temporal unique owner lookup resolves point interval",
        "provisioned standby read gate permits stale reads and routes non-stale reads to primary",
    };

    pub const table_reads_graph_metric = [_][]const u8{
        "hosted cross-range graph metric fan-in merges compatible hits pair",
        "hosted cross-range graph metric fan-in rejects unpublished or incompatible shard generations",
        "hosted cross-range graph metric fan-in rejects incompatible remote hits pair",
        "hosted cross-range graph metric fan-in rejects missing remote hits status",
        "hosted cross-range graph metric fan-in merges compatible published shard generations",
        "hosted cross-range graph metric fan-in merges active stale shard for published",
        "hosted cross-range graph metric fan-in merges nonuniform promotion shard layout",
        "encode query request includes graph metric read and rerank",
        "graph metric fan-in shard request carries internal status without mutating public request",
    };

    pub const public_table_http_docid = [_][]const u8{
        "public table batch handler maps doc identity unavailable errors",
        "public table batch handler maps HA write gate errors",
        "public table query handler maps doc identity unavailable errors",
        "public table query handler maps HA read gate errors",
        "public table query view handler maps doc identity unavailable errors",
        "public table backup handler accepts portable format",
        "public table query view handler maps HA read gate errors",
        "public document artifact manifest handlers map HA read gate errors",
        "public table graph metric action handler returns status response",
        "public document artifact manifest handler returns summary and raw state",
        "public document artifact reprocess handler returns accepted",
    };

    pub const rows = [_][]const u8{
        "relational rows unique selector",
        "relational rows conflict target upsert",
        "relational rows batch returning",
        "relational rows materializes server defaults",
        "relational rows json_set",
        "relational rows window contract",
        "relational rows join contract",
        "relational rows lateral contract",
        "relational rows cte plan contract",
        "relational rows read plan output metadata",
        "relational rows cross-table join and lateral plans execute with side schemas",
        "relational rows query contract projects coalesce",
        "relational rows query contract projects generic expression",
        "relational rows query contract parses public expression operator surface",
        "relational rows query contract projects date_trunc",
        "relational rows query contract projects string_to_array",
        "relational rows query contract supports scalar or",
        "relational rows lake bridge",
        "postgres sql adapter",
        "api http server resolves relational rows by unique selector",
        "api http server executes public relational row plan endpoints",
        "api http server exposes psql-style SQL session endpoint",
        "api http server applies SQL routine catalog plans through native runtime",
        "api http server exposes SQL routine bindings to catalog read planning",
        "api http server passes SQL routine bindings to source-backed schema DDL",
        "api http server refreshes SQL routine hooks from ready extension query functions",
        "api http server persists prepared transaction SQL DDL through durable session store fallback",
        "api http server routes public external lake row queries through configured resolver",
        "api http server resolves credentialed external lake rows from node config",
        "relational rows query projects typed expression outputs",
        "provisioned relational row plans fail closed when range topology moves during collection",
        "hosted table read source executes relational row query plans across local and remote owners",
        "hosted table read source coordinates relational row plans without local owner ranges",
        "hosted relational row plans fail closed when remote range topology moves during collection",
        "hosted relational lateral plans fail closed when remote range topology moves during right collection",
        "relational unique owner lookup requires one active owner range",
        "bound table read source executes SQL system-time as-of by commit sequence",
        "relational rows mutation source updates claimed base rows transactionally",
        "relational rows mutation source plans across injected owner ranges",
        "relational joined mutation source stages target-side updates from source rows",
        "relational joined mutation source plans across injected owner ranges",
        "relational joined mutation source stages target updates with separate source schema",
        "db row claim lease expiry aborts stale owner and lets next claimer proceed",
        "db row claim lease expiry lets direct mutation reclaim stale owner",
        "catalog source resolves groups by key and span",
    };

    pub const sql_api_parity = [_][]const u8{
        "postgres sql adapter classifies application parity corpus",
        "postgres sql adapter rejects data-driven application edge cases explicitly",
        "catalog apply applies incremental ddl plans to public schema json",
        "api http server executes public relational row plan endpoints",
        "api http server routes public external lake row queries through configured resolver",
        "api http server resolves credentialed external lake rows from node config",
        "relational rows joined mutation source contract parses lockable join plans",
        "relational rows joined mutation source validates target and source schemas independently",
        "relational rows cross-table join and lateral plans execute with side schemas",
        "postgres sql adapter typed read plans execute through relational storage",
        "postgres sql adapter typed write plans execute through relational storage",
    };

    pub const sql_api_parity_fixture = [_][]const u8{
        "postgres sql adapter checks application parity fixture freshness",
    };

    pub const internal_group_write_routes = [_][]const u8{
        "internal group write routes",
    };

    pub const raft_transition_runtime_docid = [_][]const u8{
        "transition runtime fails closed when doc identity reassignment callback is missing",
    };

    pub const serverless_docid = [_][]const u8{
        "serverless query module compiles",
        "search plan rejects internal doc identity controls",
        "serverless graph plans reject internal doc identity controls",
    };

    pub const docid_lifecycle = [_][]const u8{
        "metadata reconciler does not automatically split ordinal exhausted doc identity",
        "metadata state classifies mixed-version doc identity lifecycle reports",
        "metadata state marks doc identity rebuild required on range namespace mismatch",
        "metadata merge validation handles rolling mixed-version doc identity status fixtures",
        "metadata split request validation rejects stale doc identity namespace",
        "metadata http server rejects split and merge during active doc identity reassignment before source mutation",
        "table workflow doc identity guards reject active transition intents",
        "metadata reconciler doc identity guards block new planning during active reassignment",
        "metadata reconciler does not upsert desired split with stale doc identity namespace",
        "metadata reconciler allows explicit merge with doc identity reassignment opt-in",
        "distributed join follow-up pagination requires stamped identity request",
        "distributed join group-local hit pagination reuses structured search generation",
        "distributed join rejects doc identity rebuild before right-table fanout",
        "distributed join stateful shuffle rejects doc identity rebuild before worker dispatch",
        "distributed graph rejects doc identity rebuild before cross-range fanout",
        "distributed graph rejects unstamped result refs before cross-range fanout",
        "api distributed graph hydrate carries identity generation and clears cross-range ordinals",
        "internal worker doc identity exchange audit covers every boundary",
        "aggregation context rejects non-current identity generation",
        "aggregation full-result rerun can reuse snapped result identity generation",
        "explicit text stats requests preserve identity generation",
        "explicit text stats requests reject stale identity generation",
        "structured filter doc set cache separates shared namespace generation keys",
        "cache invalidates ownership move prefix without reviving pinned generations",
        "db text compaction preserves ordinal filters across reopen",
        "storage.db.lifecycle.test.db lifecycle doc identity ",
        "storage.db.write_path.test.db write path doc identity ",
        "identity namespace reassignment preserves snapshot generations and rejects stale writers",
        "near-u32 ordinal pressure preserves sparse high ordinal state through reassignment",
        "index manager split handoff preserves interleaved write and query summaries",
        "storage.db.transactions.test.db transactions doc identity ",
        "storage.db.split_restore_test.test.db split restore doc identity ",
        "storage.db.search_runtime.test.db search runtime identity ",
        "doc filter wire rejects old required-field fixtures but tolerates additive fields",
        "doc filter wire rejects invalid ordinal fixtures from mixed-version senders",
    };
};

const db_root_module_steps = [_]DBTestStep{
    .{
        .name = "lib-db-enrichment-test",
        .description = "Run root-module DB enrichment/replay/cutover tests",
        .filters = &DBTestFilters.enrichment,
    },
    .{
        .name = "lib-db-enrichment-worker-test",
        .description = "Run root-module DB enrichment worker tests",
        .filters = &DBTestFilters.enrichment_worker,
    },
    .{
        .name = "lib-db-enrichment-replay-test",
        .description = "Run root-module DB enrichment replay tests",
        .filters = &DBTestFilters.enrichment_replay,
    },
    .{
        .name = "lib-db-enrichment-cutover-test",
        .description = "Run root-module DB enrichment cutover tests",
        .filters = &DBTestFilters.enrichment_cutover,
    },
    .{
        .name = "lib-db-enrichment-split-cutover-test",
        .description = "Run root-module DB enrichment split cutover tests",
        .filters = &DBTestFilters.enrichment_split_cutover,
    },
    .{
        .name = "lib-db-enrichment-merge-cutover-test",
        .description = "Run root-module DB enrichment merge cutover tests",
        .filters = &DBTestFilters.enrichment_merge_cutover,
    },
    .{
        .name = "lib-db-enrichment-split-cutover-reopen-test",
        .description = "Run root-module DB split cutover reopen test",
        .filters = &DBTestFilters.enrichment_split_cutover_reopen,
    },
    .{
        .name = "lib-db-enrichment-merge-cutover-reopen-test",
        .description = "Run root-module DB merge cutover reopen test",
        .filters = &DBTestFilters.enrichment_merge_cutover_reopen,
    },
    .{
        .name = "lib-db-query-test",
        .description = "Run root-module DB query/indexing tests",
        .filters = &DBTestFilters.query,
    },
    .{
        .name = db_result_shape_step_name,
        .description = "Run focused DB query doc id boundary tests",
        .filters = &DBTestFilters.result_shape,
        .simple_runner = true,
    },
    .{
        .name = "lib-db-txn-test",
        .description = "Run focused DB write/TTL/transaction tests",
        .filters = &DBTestFilters.txn,
    },
};

const db_storage_module_steps = [_]DBTestStep{
    .{
        .name = db_sim_step_name,
        .description = "Run DB simulation and replay tests",
        .filters = &DBTestFilters.sim,
    },
    .{
        .name = "db-split-replay-fixtures",
        .description = "Run DB split replay fixture tests",
        .filters = &DBTestFilters.split_replay_fixtures,
    },
    .{
        .name = "db-split-restore-lifecycle-test",
        .description = "Run DB split/restore lifecycle regression tests",
        .filters = &DBTestFilters.split_restore_lifecycle,
    },
    .{
        .name = "db-schema-test",
        .description = "Run focused storage/db schema validation tests",
        .filters = &DBTestFilters.schema,
    },
    .{
        .name = "db-write-path-test",
        .description = "Run focused storage/db write-path tests",
        .filters = &DBTestFilters.write_path,
    },
    .{
        .name = "db-foreign-key-test",
        .description = "Run focused storage/db foreign-key unit tests",
        .filters = &DBTestFilters.foreign_key,
    },
    .{
        .name = "db-temporal-test",
        .description = "Run focused storage/db application-time temporal unit tests",
        .filters = &DBTestFilters.temporal,
    },
    .{
        .name = "db-relational-rows-test",
        .description = "Run focused storage/db relational row tests",
        .filters = &DBTestFilters.relational_rows,
    },
    .{
        .name = "db-search-runtime-test",
        .description = "Run focused storage/db search runtime tests",
        .filters = &DBTestFilters.search_runtime,
    },
    .{
        .name = "db-graph-runtime-test",
        .description = "Run focused storage/db graph runtime tests",
        .filters = &DBTestFilters.graph_runtime,
    },
    .{
        .name = "db-resolution-runtime-test",
        .description = "Run focused storage/db resolution workflow tests",
        .filters = &DBTestFilters.resolution_runtime,
    },
    .{
        .name = "db-derived-async-test",
        .description = "Run focused storage/db derived async tests",
        .filters = &DBTestFilters.derived_async,
    },
    .{
        .name = "db-lifecycle-test",
        .description = "Run focused storage/db lifecycle tests",
        .filters = &DBTestFilters.lifecycle,
    },
    .{
        .name = "lib-db-reopen-test",
        .description = "Run root-module DB reopen/compaction tests",
        .filters = &DBTestFilters.reopen,
    },
    .{
        .name = "db-ttl-runtime-test",
        .description = "Run focused storage/db TTL runtime tests",
        .filters = &DBTestFilters.ttl_runtime,
    },
    .{
        .name = "db-enrichment-test",
        .description = "Run storage/db enrichment-related unit tests",
        .filters = &DBTestFilters.enrichment_any,
    },
};

fn addProgressBanner(b: *std.Build, label: []const u8) *std.Build.Step.Run {
    return b.addSystemCommand(&.{
        "sh",
        "-c",
        b.fmt("printf '\\n==== {s} ====\\n'", .{label}),
    });
}

pub fn chainLabeledRun(
    b: *std.Build,
    artifact: *std.Build.Step.Compile,
    label: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const banner = addProgressBanner(b, label);
    if (previous) |step| banner.step.dependOn(step);
    const run = b.addRunArtifact(artifact);
    run.step.dependOn(&banner.step);
    return &run.step;
}

fn singleTestFilter(b: *std.Build, filter: []const u8) []const []const u8 {
    const filters = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
    filters[0] = filter;
    return filters;
}

fn chainLabeledFilteredTest(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filter: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = singleTestFilter(b, filter),
    });
    return chainLabeledRun(b, tests, b.fmt("{s}: {s}", .{ phase, filter }), previous);
}

pub fn chainLabeledFilteredTests(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filters: []const []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    var tail = previous;
    for (filters) |filter| {
        tail = chainLabeledFilteredTest(b, root_module, phase, filter, tail);
    }
    return tail.?;
}

fn addTestArtifact(
    b: *std.Build,
    root_module: *std.Build.Module,
    default_filters: []const []const u8,
    simple_runner: bool,
) *std.Build.Step.Compile {
    return if (simple_runner) b.addTest(.{
        .root_module = root_module,
        .filters = selectTestFilters(b, default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    }) else b.addTest(.{
        .root_module = root_module,
        .filters = selectTestFilters(b, default_filters),
    });
}

pub fn addDBRootTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
) DBRootTestStep {
    const tests = addTestArtifact(b, root_module, &DBTestFilters.root, true);
    const run = b.addRunArtifact(tests);
    const step = b.step(db_root_step_name, "Run root-module DB tests only");
    step.dependOn(&run.step);
    return .{
        .tests = tests,
        .run = run,
    };
}

pub fn addDBRootModuleTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
) DBRootModuleTestSteps {
    var result_shape: ?*std.Build.Step.Run = null;
    for (db_root_module_steps) |db_step| {
        const run = addDBFilteredTestStep(b, root_module, db_step);
        if (isDBResultShapeStep(db_step)) {
            result_shape = run;
        }
    }

    return .{
        .result_shape = result_shape orelse @panic("missing DB result-shape test step"),
    };
}

fn addDBStorageTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
) *std.Build.Step.Run {
    const tests = b.addTest(.{
        .root_module = root_module,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run = b.addRunArtifact(tests);
    const step = b.step(db_storage_step_name, "Run storage/db unit tests");
    step.dependOn(&run.step);
    return run;
}

pub fn addDBStorageTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
) DBStorageTestSteps {
    const all = addDBStorageTestStep(b, root_module);

    var sim: ?*std.Build.Step.Run = null;
    for (db_storage_module_steps) |db_step| {
        const run = addDBFilteredTestStep(b, root_module, db_step);
        if (isDBSimStep(db_step)) {
            sim = run;
        }
    }

    return .{
        .all = all,
        .sim = sim orelse @panic("missing DB simulation test step"),
    };
}

pub fn addDBFilteredTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
    db_step: DBTestStep,
) *std.Build.Step.Run {
    const tests = addTestArtifact(b, root_module, db_step.filters, db_step.simple_runner);
    const run = b.addRunArtifact(tests);
    const step = b.step(db_step.name, db_step.description);
    step.dependOn(&run.step);
    return run;
}

fn isDBResultShapeStep(db_step: DBTestStep) bool {
    return std.mem.eql(u8, db_step.name, db_result_shape_step_name);
}

fn isDBSimStep(db_step: DBTestStep) bool {
    return std.mem.eql(u8, db_step.name, db_sim_step_name);
}

pub fn selectTestFilters(
    b: *std.Build,
    default_filters: []const []const u8,
) []const []const u8 {
    const args = b.args orelse return default_filters;
    if (args.len == 0) return default_filters;

    if (std.mem.eql(u8, args[0], "--test-filter")) {
        if (args.len <= 1) return default_filters;
        return args[1..];
    }
    return args;
}
