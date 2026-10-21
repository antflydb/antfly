# Offline backup inspection

`antfly backup inspect --input snapshot.afb2` emits one JSON report without
creating a table, connecting to a server, fetching media, rendering helpers, or
running producers. Input detection uses bytes, so the filename suffix is not
significant. Supported sources are closed/stable Lite databases, full portable
AFB1/AFB2 archives, and supported full native AFB2 table archives, including
multiple shards. Deltas require a base and are not admitted by this interface.

```sh
antfly backup inspect --input snapshot.aflite \
  --destination-capabilities destination.json \
  --max-input-bytes 68719476736 \
  --max-expanded-bytes 137438953472
```

Inspection options cannot be combined with restore target, connection, location,
wait, URL, mode, backup ID, or idempotency options. Successful execution means a
report was emitted; callers must read its completeness and compatibility fields.
An incomplete, blocked, or unknown report is not admission to restore.

This is source inspection, rather than a dry run of a restore request. It does
not initialize a server client or read server credentials. Restore target,
connection and catalog-scope flags are not accepted. The draft
`restore --preflight` spelling has been replaced by `backup inspect`.

## Report version 1

- `report_version` and `inspector_version` identify the contract and analysis.
- `source` contains representation, format version, bytes, and SHA-256 of the
  original input. The digest is published only after a complete stable scan.
- `completeness` distinguishes traversal, dependency analysis, and dependency
  counts. Unknown expressions make analysis and dependency counts incomplete.
  Source failures have a stable `failure_code`, not a raw exception or path.
- `inventory` counts documents, metadata records, visited artifacts and shards.
  `requirements` groups dependency occurrences by kind and SHA-256 identity.
  Requirement digests are domain-separated by inspector version and kind.
  Counts cover the complete scan, not the displayed examples. A definition and
  its selected document/artifact values are distinct occurrences; these counts
  are not counts of distinct documents. Graph-only artifacts are validated by
  the core reader and are not included in the visited-artifact counter.
- `destination.capabilities_sha256` binds the exact capability file bytes used.
  The file itself is not copied into the report.
- `compatibility.verdict` is `compatible`, `blocked`, or `unknown`.
  Each requirement has a stable reason code and remediation.

The default library limits are 64 GiB input, 128 GiB scratch/expansion admission,
4,096 selectors and requirement groups, 16 MiB per metadata key or value,
16 MiB of retained selector bytes, 64 MiB per materialized document, and 32 total
examples (at most four per group).
Metadata keys and values are admitted before catalog/schema decoding on every
validation and inspection pass. Batch traversal borrows source bytes, so a rejected
record is not cloned first; a batch of individually bounded records may exceed
the per-record limit.
The document limit bounds logical JSON before output allocation, including
streamed zstd decompression and measured relational-row reconstruction. It also
bounds retained asset/chunk values before validation or dependency callbacks.
Artifact traversal borrows source bytes; vector and resolution artifacts retain
their own format and block limits rather than the document limit.
The expansion budget includes per-document compression inside decoded portable
blocks and is checked before dependency callbacks. Block readers enforce the
remaining output budget while decompressing, rather than materializing an
over-budget block first. Exceeding a limit produces an incomplete report. Examples
identify affected fields and document IDs; unsafe/long identifiers become
`sha256:...` labels.
Source URLs, query strings, passwords, producer configuration and credentials
are never emitted. Credential inventory uses the shared credential-field
policy, including bearer and capability tokens, and requires separate destination
evidence in addition to endpoint and model evidence. Digests are identity
bindings, not encrypted secret storage. Canonical identity hashing traverses
nested JSON with heap-backed frames, preserving sorted keys and exact numeric
normalization without depending on the process call-stack depth.

The inspector uses the restore format readers and schema decoders. It validates
all portable records, primes metadata before visiting rows, and decodes
relational rows with their historical schema. Lite uses the existing read-only
snapshot/export path and enumerates the embedded table directory or the
standalone metadata owner's authoritative tables and ranges. Standalone index
and replication definitions are inspected alongside each table's physical
catalog and records, including definitions not yet reconciled into that table.
Selectors and model names are scoped to one table; the expansion budget covers
the entire file, rather than restarting for each namespace. Catalog discovery
is bounded by the definition and metadata-byte limits.

Unknown ownership profiles, unsupported mixed-engine/migrating layouts,
missing standalone catalogs, and private FK publication owners cannot produce
a complete compatible report. They return an incomplete report; unsupported
Lite layouts use `unsupported_lite_layout`. An embedded-only library reader
also returns this code for a standalone-owned file, whose catalog requires the
server inspector.

Native archives are authenticated into private scratch;
every shard is opened read-only and exported through the same bounded portable
visitor. No target table or enrichment runtime is started. Scratch uses
`TMPDIR`, then `TEMP`, then `/tmp`, and is removed on normal completion/error.
Process death can leave native/export scratch; durable orphan cleanup is a
separate responsibility. Cancellation is checked while hashing, extracting,
copying, exporting and visiting blocks/records.

The source is locked where supported, hashed before and after inspection, and
checked for descriptor/path replacement and size/mtime/ctime changes. Use a
closed database or a stable snapshot. A report is evidence about those exact
bytes, not about later modifications at the same filename.

## Semantic source inventory

Only fields selected by executable index/enrichment definitions are source
inputs. Ordinary document paths and URLs are not dependencies. Reader,
transcriber, document-extraction and PDF-page embedding inputs are media
locators; text producers inspect media directives rather than treating every
URL in prose as a fetch. Copy producers preserve literal text, including
media-looking markers. Their template helper fetches are inventoried independently of the
copied output. Configured artifact consumers inspect their stored
asset/chunk input stream. Core validation also covers artifact envelopes and
retained provenance; a historical source string is not by itself an active
fetch requirement.

Model-aware chunking uses the execution path's source-input parser. Immutable
source references contribute their URI's access requirements; inline media
remains embedded. Its model requirement binds the enclosing chunk definition,
including the source selector, chunk policy, embedding target and dimensions.
Source references use the range transport's shared offline validation for immutable
pins, version kinds, supported schemes and URI bounds. Invalid contracts remain
unresolved even when destination capabilities attest support. No transport or
credential lookup is needed for this validation. Unrecognized or incomplete
structured inputs remain unresolved.

Active resolver catalogs include frozen adjudication models and credentials.
Their model evidence binds the complete resolver policy. A resolver's
`name_embedding` reference into another table does not establish that table's
model revision or preprocessing contract and remains `model_contract_unknown`.
Retired resolver identities retain historical state without executing it and
do not contribute active dependencies.

Prior-context render templates inventory executed helper fetches. Their output
is appended as literal text, including media-looking markers. Their named
retained artifacts are inspected as single values,
arrays, or `items` envelopes using the execution path's collection selection.
Unsupported helpers remain uninspectable even when no prior artifact exists.

Grouped chunk and embedding consumers follow the execution planner's upstream
group resolution, including dependencies declared before their group. The
inspector selects synthesized unit text from retained group assets and private
unit documents. Grouped model evidence also binds the group source definition,
including its source field, filtering, ordering and window policy.

Simple field paths, simple substitutions and static `media`, `remoteMedia`,
`remoteText` and `remotePDF` URL arguments are analyzed without fetching resources.
The inspector uses the runtime's Handlebars parser and path lookup, and combines
supported literal/substitution output before scanning media directives, using
the same whitespace-control functions as execution. Numeric substitutions
preserve the runtime's exact JSON number tokens.
Document-dependent URL components stay unknown during definition inspection;
concrete documents supply their values, while independent static fetches are
still inventoried. The pure `media` formatter
is shared with runtime; remote helpers are never called. Remote helpers use
`credentials="name"` (or a supported field reference); quoting and argument order
follow the runtime parser.

When output is interpreted as media or a locator, remote helper output is
opaque: it can contain or complete media directives even when the fetch URL is
known. Its dependencies remain incomplete and cannot be certified by destination
evidence. Copy output, fixed chunker input, model chunker text, and template-free
generator or text embedding inputs do not interpret media markers. Locator
consumers still inventory source URLs, and templates that produce media parts
remain subject to directive inspection. Executed template helper fetch URLs
and credentials still require destination evidence; structured model chunker media
and source references follow the shared runtime parser. Unsupported
helper syntax, opaque output in interpreted positions, and custom loaders are
reported as `dependency_uninspectable`. This deliberately does not claim to
interpret all Handlebars programs. Unsupported schemes and structured locators
are `source_unresolved`. Local paths/gateways and credential-bearing or expiring
URLs block compatibility. Archive-member and page/fragment selection generate
separate destination retrieval requirements. URL credentials use the shared
credential policy, including API keys, bearer tokens, and fragment parameters.
Query parameters are split before
percent-decoding their names; encoded delimiters inside values do not create
credential or retrieval parameters. No resolver or URL rewrite occurs.

## Destination capability input

```json
{
  "version": 1,
  "configuration_id": "destination-configuration-revision",
  "requirements": [
    {
      "requirement_sha256": "<64 lowercase hex characters from the inventory>",
      "supported": true,
      "query_embedding": true,
      "future_ingestion": true,
      "model_revision_verified": true,
      "preprocessing_verified": true,
      "vector_space_verified": true,
      "dimensions": 768
    }
  ]
}
```

This is an evidence/attestation interface for a trusted caller. An inspector
cannot establish destination access or model availability offline. Omitted
capability information remains unknown, including when an otherwise empty
source has no dependencies. An explicit unsupported capability blocks its
requirement. Unknown versions, malformed digests and duplicate entries are
rejected. A caller must independently establish each assertion; copying report
hashes into a file with `supported: true` is not a compatibility check.

External media requirements identify the normalized origin, including port.
The caller must verify durable access to the selected sources within that
origin under its connection and egress policy. Connection requirements bind
the source connection/credential reference or object-storage scheme/bucket.
Retrieval requirements separately bind archive/page semantics. Neither the CLI
nor the library performs arbitrary-URL requests.

PostgreSQL replication definitions produce `replication_source` requirements.
Each digest binds the declarative source, including its DSN or secret/environment
reference, source table, slot, publication, routes and transforms. Runtime status
and action hints are excluded. The inspector does not resolve credentials or
open a database connection, and literal transform values are not media sources.
Unknown providers or malformed contracts produce incomplete reports.

A replication capability requires both `supported: true` and
`future_ingestion: true`. The caller must verify destination-managed credentials,
source-table access, and the slot/publication and logical replication behavior
needed for future ingestion. Empty capability files and generic connection
evidence cannot satisfy a replication requirement. DSNs and credentials are
hashed into the identity and are never emitted in the report.

Embedding requirements bind the canonical complete source definition, including
model configuration, input/preprocessing/template, dimensions and vector-space
assertions. Credential changes may conservatively change that binding. A model
name or a stored vector is insufficient. The caller must verify the actual
source and destination model revisions, preprocessing and vector space, and
support both query embedding and future ingestion with matching dimensions.
A missing dimension contract remains `model_contract_unknown`; this includes
sparse/external sources whose available metadata does not establish that
contract and stored vectors with no matching producer catalog. Non-embedding model producers require verified revision and future
execution capability. The inspector never selects another model or regenerates
artifacts.

Recheck the source digest and destination configuration/capabilities when
admitting a restore. This report does not reserve a destination table name,
capacity, credentials, or a source lease. Ordinary single-table restore has
reconciliation semantics for an existing matching restore. Strict create-only
admission must be carried through the durable job and enforced atomically at
metadata publication; a preflight-time absence check is insufficient.


## Create-only admission audit

Source checked at `89df7aff46cf6ae9fc948459a00cbeff08d81869`:

- The ordinary [table RestoreRequest](https://github.com/antflydb/antfly/blob/89df7aff46cf6ae9fc948459a00cbeff08d81869/specs/openapi/antfly/metadata.yaml#L6907)
  admits backup ID, location and connection, with no create-only mode.
- [Owned table restore](https://github.com/antflydb/antfly/blob/89df7aff46cf6ae9fc948459a00cbeff08d81869/zig/pkg/antfly/src/api/http_server.zig#L12549)
  checks existence before preparation, enters the restore lifecycle, rechecks
  existence at publication, and creates metadata for an absent target. An
  existing matching restore follows `reconcile_only`; identity mismatch returns
  `TableAlreadyExists`. These checks do not express the strict policy that any
  pre-existing target must conflict.
- [Staged cluster/cohort restore](https://github.com/antflydb/antfly/blob/89df7aff46cf6ae9fc948459a00cbeff08d81869/zig/pkg/antfly/src/api/http_server.zig#L19832)
  has a linearizable snapshot, `fail_if_exists` plan and replicated `reserve`
  command. That is a separate dispatch path from ordinary table restore.

Before integrating a caller that promises a new table, carry an explicit
create-only policy through request validation, durable job persistence, retry
and execution. Enforce name reservation/publication in the authoritative
metadata operation, retaining idempotent retries only for the same admitted
job/source. Test concurrent create versus restore, two restore jobs targeting
the same name, and recovery after reservation/publication. This preflight slice
does not change restore admission semantics.
