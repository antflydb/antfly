# Catalog object annotations and naming-only publication

Index and constraint comments use table-scoped stable object IDs, a structural
identity directory, and separate point annotations. UNIQUE constraints and their
backing indexes are distinct objects even when their SQL names coincide. Missing
point annotations inherit declared descriptions; an explicit NULL suppresses
that fallback. Dropping and recreating a name allocates a new ID. Empty
directories retain their allocation high-water mark, preventing name-reuse ABA.

COMMENT pins the authorized logical owner and directory epoch, then writes the
bounded point annotation in the existing catalog transaction. It does not update
the table schema, rebuild indexes, or rewrite primary rows. Native snapshots
include the directory and notes and reject orphan or noncanonical records.
Existing index GET/list routes project these annotations for administrators and
readers; they use a single table-scoped native read and verify the selected
resource's structural definition before applying presentation text. The overlay
preserves unrelated numeric tokens verbatim, including u64 counters and exact
decimal metadata.

CHECK renames use the guarded schema CAS. FK renames retain the existing
all-owner FK publication protocol. UNIQUE/PRIMARY renames use the existing
schema-only validation barrier: exact before/after names are part of immutable
signed preparation, while owners retain claim generations and ordered-index
physical IDs. The barrier fences existing transaction debt and installs metadata
without scanning or rewriting rows. It uses the ordinary install decision,
current-root re-attestation, ACK ledger, and cold recovery paths.

A compact rename-source pointer reserves destination names through the same
canonical namespace projector used by index/FK publication. Admission checks
indexed occupancy before writing the job. The pointer is created with admission
and removed only after every owner acknowledges terminal release. Ordinary ACK
progress remains neutral to the relation-source clock. Cold reconciliation reads
only active rename pointers and immutable source/successor definitions; terminal
job history does not enter the name scan.

LIKE carries authorized source keys and final declaration-family ordinals, not
caller-supplied text or destination names. The accepting CREATE transaction
reads current source annotations, resolves the actual destination declarations
after generated-name collision handling, and allocates independent destination
identities. Sources are grouped once by authenticated table ID, loaded lazily and
parsed once in a per-source region. That region is released before the next
source is loaded. The working set is one source plus destination state and
bounded prepared notes; aggregate note bytes are capped at 16 MiB. Copying
includes explicit NULL overrides and effective inline index descriptions. PostgreSQL 18 copies CHECK and backing-index comments but leaves
new UNIQUE/PRIMARY constraint comments empty; the SQL compiler follows that
behavior. EXCLUDING COMMENTS strips copied presentation text. There is no
post-CREATE COMMENT replay.

Ordinary relational indexes retain their online build path; UNIQUE indexes
retain their fenced publication protocol. The common guarded metadata writer
checks every newly added index name against both active and reserved namespace
entries before publishing the schema. A linearizable preflight supplies definite
duplicate-name diagnostics without proposing a mutation, and ordered apply
repeats that check under the exact table CAS. Concurrently lost admission remains
an uncertain outcome rather than being misreported as safe to replay. Existing
names and unchanged read-only guards need no extra namespace probes. Index
admission probes the exclusive relation-name entry directly: another table's
CHECK/FK constraint membership is not an index-name collision. Pending index
reservations still occupy that relation entry.

All new intent, source-pointer, and annotation records require decoder cohort
127. The annotation capability is independent of attribute-renaming floor 126
and sequence-provenance floor 125. Receiving new record shapes alone does not
confer mutation, publication, or owner-install authority. Compiled failure ABI
82 preserves distinct missing/duplicate index and constraint diagnostics rather
than relabeling expected catalog conflicts as internal kernel failures.

The standalone Debug model includes exhaustive allocation-failure checks and a
source-retention regression: sixteen 512 KiB source schemas load once each, with
less than 128 KiB retained between source loads when no copied notes are needed.
This measures source retention and work counts, not end-to-end latency. Native
and mounted gates additionally qualify actual catalog transactions, namespace
reservation, signed owner progress, and cold publication. A naming-only owner
manifest must contain zero scanned rows and zero row records.

Native catalog checkpoints preserve annotations. Standalone per-table portable
backup annotation export remains a separate contract; these tests do not claim
it. Owning-table CASCADE with externally borrowed sequence defaults also remains
separate from LIKE declaration/comment copying.

Snapshot requirements follow the stored format. AFNOWN1 native-owner bindings
use their own decoder floor (91); their presence does not require or authorize
the stronger complete index-delivery workflow (100). Native authority fixtures
include the same canonical reverse owner record as admission, so cancellation
proofs cannot bypass namespace membership or current-owner checks.

The combined pinned-Zig Debug gate passes the mounted original COMMENT,
paused-ACK/cold rename and LIKE campaigns; the native annotation tests; and all
77 publication/snapshot tests plus the service admission test. The PostgreSQL
oracle independently checks OID retention, heap postimages, CHECK/index namespace
overlap and LIKE copying. The parity ledger qualifies originals `sql-0144` and
`sql-0145` with these executable gates.
