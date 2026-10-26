# Collaboration sources and Google Workspace

Slack, Discord, and Google Workspace use the portable sources module for acquisition
and canonical documents. Colony supplies encrypted credentials, durable jobs,
staging, permissions, and publication. Antfly stores documents and the source
catalog; antfly-grep searches the same contracts for every provider.

## Connections and source selection

Expose one **Google Workspace** connection. Drive is a resource within that
connection, alongside Gmail and Calendar. GCS remains a separate object-storage
provider. Do not expose both Google Drive and Google Workspace as connection cards.

A connection identifies the authorized Google account and granted services. A
source identifies selected content, its sharing policy, destination, and sync
schedule. One connection can supply many independently shared sources:

| Connection | Source | Selection |
| --- | --- | --- |
| Google Workspace, Alice | Engineering documents | A Drive folder |
| Google Workspace, Alice | Customer mail | Gmail label IDs |
| Google Workspace, Alice | Team calendar | Explicit calendar IDs |
| Slack, Engineering | Reliability discussions | Explicit channel IDs |
| Discord, Community | Support discussions | Explicit channels and their threads |

Default mail and calendar sources to private. Show the selected account, service,
scope, attachment choice, sharing, and estimated inventory before the first sync.
Display status separately for each source so a mail authorization failure does not
hide successfully published Drive sources on the same connection.

The hosted onboarding flow is: choose a connection, authorize selected services,
pick resources, preview text and attachments, choose sharing, and sync. Adding Gmail
to a Drive-only connection requires additional consent. Canceling additional consent
must retain the existing Drive grants. A disconnected account requires reconnecting;
selecting another account creates a distinct connection instead of retargeting old
source identities. Do not use Google login credentials as ingestion authorization.

Existing standalone profiles and the hosted credential API can supply explicit
authorized-user credential JSON. The hosted implementation adds browser OAuth enrollment, resource picker endpoints,
and dashboard and CLI flows. These require a matching deployed Colony backend.
An API-key token option can support operator-installed Slack/Discord applications;
the hosted default experience should use installation flows rather than pasted tokens.

## Hosted onboarding contract

Keep source creation on the existing source-mode import-job API. Add read-only
provider resource selection behind Colony's authenticated instance-provider routes,
with an opaque cursor, selected service, and optional parent ID. The server fixes
provider endpoints; the browser cannot supply a URL or credential. Return only
resource IDs, names, parent IDs, content-read capability, and pagination state.
Show unavailable services as **Enable access**, not as an empty resource list.

Connection flow:

1. Choose Google Workspace, Slack, or Discord and the desired account/workspace.
2. For Google, select Drive, Gmail, and/or Calendar. Request additional consent
   only when the selected services exceed the stored grants.
3. Complete the provider callback and verify account/workspace/guild identity.
4. Select folders, Gmail labels, calendars, or channels. Enable attachments explicitly.
5. Preview representative normalized text and unsupported-file diagnostics.
6. Choose destination, private readers or explicit sharing, and schedule; create
   a source-mode job and show backfill progress.

Bind installation state and PKCE material to the initiating user, organization,
instance, provider, service selection, and any existing connection being upgraded.
Use expiring, single-use state and allowlisted callback destinations. Recheck
administrator authority on completion. Store refreshed credentials encrypted;
never put provider tokens into browser-visible configuration or catalog records.
When additional consent returns without a refresh token, preserve the existing
valid refresh credential. Test denial, callback replay, account substitution,
partial grants, token rotation, and reconnect racing a running import.

Slack stores the workspace installation and its granted channel/history/file
scopes. A managed Discord installation binds a guild to the organization and
instance; the hosted application's bot secret is deployment-owned rather than
copied into every tenant connection. Discord sources still enforce that exact
guild binding on every acquisition. Operator-supplied bot tokens remain an explicit
advanced connection type. Neither installation authorizes a different tenant's
channels, even when the same managed application can access them.

Use per-source states **Needs access**, **Backfilling**, **Syncing**, **Ready**,
**Stale**, and **Failed**, with the affected service and a direct recovery action.
Show imported message/file counts, attachment failures, last successful sync, and
the selected sharing policy. Source removal and data purge are separate actions;
the existing runner's removal behavior retains indexed data and ownership state.

Colony implements installation, status polling, resource listing, preview and source
creation. The dashboard and `antfly-cloud connections` share these endpoints.
The portable connector continues to accept explicit credentials without browser OAuth.

## Google authorization

Request the services the user selects, using read-only scopes:

| Service | Scope |
| --- | --- |
| Drive, Docs, Slides, Drive attachments | `drive.readonly` |
| Gmail and MIME attachments | `gmail.readonly` |
| Calendar content and calendar selection | `calendar.readonly` |

The connection configuration records enabled `services`. The portable connector
rejects a source whose resource is not enabled. This is application admission,
not proof that a refresh token received a new grant: the authorization flow must
record granted scopes and actual API calls must verify access. Refresh credentials
cannot acquire missing grants by changing source configuration.

Gmail uses the authorized user's `me` mailbox and pins its email identity. Calendar
resolves `primary` to a concrete calendar ID and requires event-reading access.
Free/busy access is insufficient. Service accounts need explicitly administered
mailbox authorization; ordinary service-account JSON does not imply domain-wide
mailbox access. Domain-wide delegation needs a separate administrator workflow.

Before distributing the hosted integration, qualify Google's verification and
data handling requirements for the selected scopes. Gmail read-only is a restricted
scope; the deployment's storage and processing determine additional assessment
requirements. See [Gmail scopes](https://developers.google.com/workspace/gmail/api/auth/scopes).

## Canonical records and relationships

Use one document per message, event, Drive file, or attachment. Preserve the existing
`grep` document and source catalog contracts. No new Antfly database resource is
needed. Job UUIDs isolate destination ownership through existing scoped merge.

Chat and Workspace message/event identities follow:

```text
PROVIDER://job/SOURCE_UUID/RESOURCE/CONTAINER_ID/REMOTE_ID
PROVIDER://job/SOURCE_UUID/RESOURCE/CONTAINER_ID/REMOTE_ID/attachments/ATTACHMENT_ID
```

URI schemes are `slack`, `discord`, and `workspace`; the provider enum remains
`google_workspace`. Path components are escaped. Display names, channel renames, subjects, and calendar
titles do not participate in identity. Drive retains its existing file-ID identity.
Canonical metadata plus text determine message/event revisions. Attachment revisions
also include the raw file digest; equal extracted text does not conceal changed bytes.
Drive reads retain provider-version pinning and checksum verification.

Relationship metadata includes parent identities, thread IDs, RFC Message-ID,
In-Reply-To and References, calendar iCalUID, invitation UIDs, and typed Google
resource links. A Drive link records the file ID; the same ID appears on a Drive
source record even if that source has a different job namespace. Resource IDs must
be matched within the authorized connection/account context, never globally across
tenants. Do not equate Gmail API message IDs with RFC Message-ID, or Calendar event
IDs with iCalUID; these are separate identities.

Grep initially searches the selected canonical documents. Relationship metadata
supports authorized lookup of related records; automatic thread expansion and graph
traversal need a separate query feature. A relation cannot grant access to its target.
Only return target content after applying that target source's current authorization.
External links are never fetched with a provider token.

## Attachments and extraction

Attachments are opt-in on a source. Download native attachments through the provider's
authenticated API. Re-fetch current message/file metadata for retries so temporary
download URLs are never durable identities or persisted catalog/checkpoint metadata.
Only known provider download origins are accepted, with redirects disabled. Slack
tokens go only to Slack's file origin; Discord CDN requests carry no bot token.

Preserve attachment identity, parent identity, filename, MIME type, raw digest,
extracted text, and source provenance. A removed attachment retires only its owned
record after a complete authorized reconciliation. Access to the parent is checked
again before staged attachment text is reused. Calendar Drive attachments additionally
require Drive service access and an independent version-checked Drive read.

The default extractor handles UTF-8 text, HTML, DOCX, PPTX, PDF through `pdftotext`,
and images through `tesseract`. Deploy those tools before enabling their formats, or
inject an extraction service. Extraction failures, missing OCR, encrypted documents,
unsupported formats, and empty extraction of nonempty files fail explicitly. They
cannot become successful metadata-only publications. Audio/video transcription,
legacy Office formats, and complete Sheets/workbook extraction need additional
extractors; do not represent the first sheet as an entire workbook.

Limit each file and extracted item to 32 MiB and aggregate message extraction to
32 MiB. Bound archive expansion, MIME nesting, extraction output, execution time,
attachment counts, and run bytes. Private temporary files are deleted after extraction.
Production extractors should run with restricted filesystem/network access and CPU,
memory, and wall-time limits. Record an extractor version with durable staging before
allowing extractor upgrades to reuse cached results.

## Provider acquisition

Slack inventories explicitly selected channels, paginates history and replies, and
keeps channel and thread IDs. Slack logical errors in HTTP 200 responses are failures.
Retention-limited history, unfinished cursors, missing channel membership, or rejected
file downloads cannot authorize reconciliation. External Slack file links require
their own authorized source; they are not downloaded as native Slack files.

Discord verifies guild identity, channel selection, channel/role/member overwrites,
history access, and message-content capability. It inventories selected channel
history, active threads, archived public threads, and joined archived private threads.
Private threads require membership. An empty history without proven read permissions
cannot mean deletion. Forum/media parents supply thread content rather than ordinary
channel messages. Message references remain metadata.

Gmail inventories a mailbox or the intersection of explicitly selected label IDs,
using complete pagination. MIME bodies supply headers and text; file parts supply
attachment records. HTML links and calendar invitation UIDs remain metadata even
when their visible text is normalized. A mailbox identity change invalidates pinned
items instead of silently switching accounts.

Calendar preserves recurring masters, modified exceptions, recurrence rules, time
zones, all-day dates, and original occurrence times without unbounded expansion.
Canceled recurring exceptions remain explicit cancellation records; deleted ordinary
events retire after reconciliation. Missing event detail access cannot become empty
publication. Event attachment links and descriptions retain related Drive identities.

Provider contracts: [Slack history](https://docs.slack.dev/reference/methods/conversations.history/),
[Slack replies](https://docs.slack.dev/reference/methods/conversations.replies/),
[Discord messages](https://docs.discord.com/developers/resources/message),
[Discord threads](https://docs.discord.com/developers/topics/threads),
[Gmail messages](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get),
and [Calendar events](https://developers.google.com/workspace/calendar/api/v3/reference/events).

## Synchronization and access

Reuse authoritative inventories, pinned reads, the ownership journal, scoped merge,
query readiness, catalog leases, and Cloud's publication fences. Failed reads and
partial pages never delete missing rows or renew a searchable publication. Recheck
container access even for empty cached inventories. Independently confirm omitted
owned identities using a complete authorized inventory before cleanup.

Separate acquisition authority from reader authorization. An installation may read
a channel whose content should remain private to a subset of Antfly users. Initial
sharing is explicit per source; split selections by trust boundary. Automatic Slack
membership, Discord role, or Google ACL mirroring requires identity mapping,
membership refresh, and revocation events before it can be offered as a policy mode.

Collaboration inventories admit up to 100,000 items, with durable page and per-parent hydration
checkpoints. Other provider limits still apply, including the internal Drive
connector's 10,000-item inventory. Publication rechecks current container access;
failed or incomplete acquisition cannot renew the lease. The manifest and staged
text remain bounded, rather than an unlimited history stream.

Gmail history IDs and per-calendar sync tokens support incremental enumeration
against the last published manifest. Expired cursors require a complete rebuild.
Changed/deleted parent records also replace or retire their attachment records.
Unchanged records reuse immutable normalized bundles after current scope checks. Calendar
event revisions and independently authorized Drive attachment revisions are checked
before reuse. Slack
and Discord use full polling with resumable pages; Slack Events, Discord Gateway,
Drive changes cursors, and shared installation-wide rate-limit coordination remain
additional scale work. Do not advertise event-driven or unlimited backfills.

## Tests and release sequence

| Layer | Required acceptance |
| --- | --- |
| Provider fixtures | Multi-page success; repeated/truncated cursors; thread roots/replies; archived/private threads; edits; label changes; calendar recurrence/cancellations; account changes; 403/404/429/5xx |
| Attachments | Private downloads; changed bytes with equal text; replaced/deleted files; unavailable OCR; malformed/oversized MIME and ZIPs; redirects and untrusted hosts; no tokens or signed URLs in state |
| Shared runner | First import; unchanged rerun; edited text; attachment deletion; lost acknowledgement; pinned recovery; authoritative cleanup; sibling-source isolation; readiness and stale catalog |
| Colony | Provider admission and migration; encrypted credentials; staged-text access checks; retry deadlines; attempt/publication fences; disconnect/revoke during queries |
| Actual grep | Real Go-normalized provider rows through the native binary, both standalone catalog and hosted Cloud discovery; verify text, provenance, filters, and denied readers |
| Live qualification | Disposable workspace/guild, Gmail mailbox and calendar, ordinary hosted instance, shared Drive attachment; edit/delete/revoke while polling and during retained query snapshots |

Land the portable sources module first, then update Colony's module dependency and
generated provider contract. Migrate unreleased Google Drive connections/jobs to
Workspace with Drive-only services. Keep the internal Drive acquisition code; do
not retain a second public provider choice. Drain old source workers for the provider
migration. Enable extraction tools and OAuth service grants before exposing onboarding.

Browser onboarding, consent upgrades, resource picker UI, incremental scale support,
and live-provider qualification are explicit release gates. Fixture success alone
does not establish hosted deployment readiness.

## Qualification status and live walkthrough

See [release qualification](collaboration-source-qualification.md) for tested
surfaces, live blockers and the exact CLI sequence. Local fixtures establish
contracts; only an authorized live sync and search can qualify the hosted flow.

## Durable normalized acquisition

Each completed parent and its attachments is stored as one content-addressed,
versioned bundle. Standalone runs use private state files. Hosted runs require the
existing private import staging bucket; SQL stores only encrypted selection/item
metadata and content references. The bundle binds the namespace, provider,
selection, extractor version, revisions and bytes. Digests and tenant-scoped object
paths are checked on read. Storage paths never enter searchable record metadata.
One bundle is held in memory while siblings are read. Completed bundles are reused
across worker attempts; expired blobs are rehydrated only if their pinned revisions
still match. A changed extractor version invalidates incremental reuse.

Slack history and reply pages hydrate their returned bodies directly. Resume pages
retain only identities and cursors; successful hydration checkpoints prevent a
later rate limit from repeating completed downloads and extraction. Current scope
checks are memoized for at most 30 seconds within an attempt and refreshed before
publication, including observed private thread scopes. Pinned message snapshots
remain stable during an attempt; later provider edits are reconciled on the next
sync. Independently authorized Calendar events and Drive attachments also retain
revision checks. Custom extractors must supply a stable version before using a
content store.

Hosted lifetime attempt counts are monotonic. A separate durable progress counter
allows continued 429 retries when new checkpoints were saved, while consecutive
attempts without progress retain the existing three-attempt limit and Retry-After
scheduling. Previews sample one parent and up to eight attachments; each returned
text sample is capped separately by the API.

Only organization source managers can select private collaboration connections,
regardless of destination table grants. The CLI and dashboard share an Antfly-owned
installation entry. It establishes a dashboard session before issuing provider
consent, verifies the initiating user and tenant, and preserves PKCE and single-use
callback state. The CLI validates the installation origin and path before opening it.
