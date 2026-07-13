# Object-storage operations

This guide applies to the Zig serverless `object` engine described in
[S3 object storage](s3-storage.md).

## Production checklist

- Provision buckets before deployment and use `require_existing`.
- Enable provider-side encryption, versioning where required, access logging,
  and lifecycle rules through infrastructure code.
- Use workload identity and short-lived credentials. Scope permissions to the
  configured bucket and prefix.
- Give WAL/catalog lanes separate buckets or credentials when their durability
  or access policy differs from artifact retention.
- Keep database-writer credentials separate from remote-content readers.
- Set bounded query-cache bytes and request/thread limits for the compute role.
- Alert on startup validation failures, credential-refresh failures, object
  request latency/errors, unpublished WAL, stalled builds, and retention lag.

## Scaling

API/query roles are stateless with respect to durable state and may scale
horizontally. Builders, compactors, enrichment workers, and retention workers
coordinate through versioned manifests, WAL positions, and compare-and-swap
progress/catalog records. Do not run an unbounded number of maintenance roles;
scale them from observed backlog and object-store request limits.

Query caches are local and bounded. Reusing a connection shares its HTTP pool;
using separate credential profiles intentionally creates separate clients.
Choose lane partitioning for security and lifecycle boundaries, not as a
substitute for ordinary prefix organization.

## Backup and recovery

Portable backup and restore use the normal authenticated `/db/v1` API. Provider
bucket replication/versioning is disaster-recovery infrastructure, not a
replacement for the database's portable backup contract.

Recovery must preserve all durable lanes at mutually consistent published
versions. Restoring only artifacts or only WAL can produce an unusable
namespace. Exercise recovery into an isolated prefix and validate table reads,
searches, and catalog status before promotion.

## Credential rotation

Prefer the cloud credential chain, where Antfly refreshes expiring credentials.
For file-backed static credentials, atomically replace the protected JSON
secret-store file. Antfly retains its last-known-good snapshot if a reload is
invalid and reports the stale condition; investigate it immediately.

## Troubleshooting

Startup is intentionally fail closed. Check, in order:

1. `deployment_mode` is `serverless` and `storage.engine` is `object`.
2. every referenced connection exists, is `external_io` with protocol `s3`,
   and has capability `storage.primary`;
3. lane buckets are present in the connection allowlist and prefixes stay
   within the configured connection prefix;
4. endpoint TLS and addressing style match the provider;
5. credentials can list and mutate a test object within the exact prefix;
6. required buckets already exist when provisioning is `require_existing`.

Explicit malformed environment values are errors, not defaults. Correct the
named variable shown in the startup log rather than relying on fallback
behavior.
