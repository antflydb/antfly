# Read-only backup preflight

`antfly restore --preflight --input snapshot.afb2` emits one JSON report without
creating a table, connecting to a server, fetching media, executing remote
helpers, or running producers. Input detection uses bytes, so the filename suffix is not
significant. Supported sources are closed/stable Lite databases, full portable
AFB1/AFB2 archives, and supported full native AFB2 table archives, including
multiple shards. Deltas require a base and are not admitted by this interface.

```sh
antfly restore --preflight --input snapshot.aflite \
  --destination-capabilities destination.json \
  --max-input-bytes 68719476736 \
  --max-expanded-bytes 137438953472
```

Preflight options cannot be combined with restore target, connection, location,
wait, URL, mode, backup ID, or idempotency options. Successful execution means a
report was emitted; callers must read its completeness and compatibility fields.
An incomplete, blocked, or unknown report is not admission to restore.

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
4,096 selectors and requirement groups, 16 MiB retained selector bytes, 64 MiB
per materialized document, and 32 total examples (at most four per group).
The expansion budget includes per-document compression inside decoded portable
blocks and is checked before dependency callbacks. Exceeding a limit produces an
incomplete report. Examples identify affected fields and document IDs; unsafe/long identifiers become `sha256:...` labels.
Source URLs, query strings, passwords, producer configuration and credentials
are never emitted. Digests are identity bindings, not encrypted secret storage.

The inspector uses the restore format readers and schema decoders. It validates
all portable records, primes metadata before visiting rows, and decodes
relational rows with their historical schema. Lite uses the existing read-only
snapshot/export path. Native archives are authenticated into private scratch;
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
URL in prose as a fetch. Configured artifact consumers inspect their stored
asset/chunk input stream. Core validation also covers artifact envelopes and
retained provenance; a historical source string is not by itself an active
fetch requirement.

Model-aware chunking uses the execution path's source-input parser. Immutable
source references contribute their URI's access requirements; inline media
remains embedded. Its model requirement binds the enclosing chunk definition,
including the source selector, chunk policy, embedding target and dimensions.
Unrecognized or incomplete structured inputs remain unresolved.

Active resolver catalogs include frozen adjudication models and credentials.
Their model evidence binds the complete resolver policy. A resolver's
`name_embedding` reference into another table does not establish that table's
model revision or preprocessing contract and remains `model_contract_unknown`.
Retired resolver identities retain historical state without executing it and
do not contribute active dependencies.

Prior-context render templates use the same inspection rules as source
templates. Their named retained artifacts are inspected as single values,
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
the same whitespace-control functions as execution. The pure `media` formatter
is shared with runtime; remote helpers are never called. Remote helpers use
`credentials="name"` (or a supported field reference); quoting and argument order
follow the runtime parser.

Remote helper output is opaque: it can contain or complete media directives,
even when the fetch URL is known. Its dependencies remain incomplete and cannot
be certified by destination evidence. Unsupported helper syntax, opaque helper
output, and custom loaders are reported as `dependency_uninspectable`. This
deliberately does not claim to interpret all Handlebars programs. Unsupported
schemes and structured locators are `source_unresolved`. Local paths/gateways and credential-bearing or expiring
URLs block compatibility. Archive-member and page/fragment selection generate
separate destination retrieval requirements. No resolver or URL rewrite occurs.

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

PostgreSQL replication sources require connection evidence bound to the complete
source configuration, including its DSN, source table, publication and slot.
DSNs remain opaque, including libpq keyword strings and secret references;
preflight neither resolves credentials nor probes PostgreSQL. The trusted
caller must attest durable access and the privileges required for replication.

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
