# Authenticated Cloudflare mail example

This extends #1278 with application login and an email-style inbox. Better Auth
owns identity and sessions, Antfly owns durable application/auth state, R2 holds
private MIME and attachment objects, and Cloudflare supplies inbound routing and
outbound email transport. Work is isolated from the performance qualification
branch in `feat/cloudflare-mail-auth`.

## Login and persistence

The current Better Auth integration uses relational SQL sessions on Node or a
remote Antfly API owner. The document-oriented Worker API does not implement
that protocol. Workers support supplies a real atomic persistence boundary;
a SQL statement parser in JavaScript or a second SQLite database is not the
intended solution.

The Workers entry supplies a structured Better Auth adapter over Antfly's existing transactional
key/value metadata engine, with indexed identity/session lookup, atomic unique
claims, guarded verification-token consumption, transactional account creation,
rollback and committed-outcome handling. Run the adapter and transaction
callbacks inside one private auth actor, keeping requests and native I/O in
that actor's lifetime. Never replay application transaction callbacks after an
uncertain write. Reject unsupported schema changes and bound scans/transactions.

Browser requests use Better Auth session cookies. The application derives all
mailbox memberships and storage routes from the verified user. Browser-supplied
owner/tenant IDs, cursors and visibility tokens do not grant access. Auth tables,
operator credentials and raw object-store bindings are private capabilities.

## Email transport

Cloudflare Email Routing invokes an Email Worker's `email` handler. Validate the
SMTP envelope recipient against provisioned aliases, retain bounded raw MIME in
private R2 storage, and persist an ingest receipt before queueing MIME parsing
and Antfly indexing. Resolve Bcc delivery through the envelope, not the To
header. Do not assume RFC Message-ID is unique or trustworthy. Use delivery
receipts scoped to recipient and raw-content digest, retaining collision and
retry behavior explicitly.

Messages, attachment metadata, drafts, thread membership, labels and send intent
belong in Antfly. Large original MIME and attachment bytes belong in R2, exposed
only through session-authorized application routes. Parse MIME with a maintained
parser, enforce body/attachment/decompression budgets, display plain text by
default, and do not execute email HTML or fetch remote images automatically.

An outbox persists intent before a Queue dispatches Cloudflare Email Service.
Preserve accepted, queued, bounced, suppressed and unknown states. An ambiguous
send cannot be blindly retried: there is no assumed provider idempotency
contract. A stable Message-ID helps threading; it does not guarantee delivery
exactly once. Better Auth verification/recovery mail uses this transport too,
with transaction commit preceding external delivery.

Cloudflare's sending product is documented as **transactional-only**. The
proposed first example is a shared support/customer-correspondence inbox, pending
confirmation of the intended product scope. A general personal-mail service
needs an outbound provider appropriate to that use case. Keep the transport
replaceable rather than claiming Gmail-equivalent sending support.

Sources checked October 29, 2026:

- [Inbound routing](https://developers.cloudflare.com/email-service/get-started/route-emails/)
- [Workers sending API](https://developers.cloudflare.com/email-service/api/send-emails/workers-api/)
- [Transactional-only restriction](https://developers.cloudflare.com/email-service/reference/faq/)
- [Sending limits](https://developers.cloudflare.com/email-service/platform/limits/)
- [Domain configuration](https://developers.cloudflare.com/email-service/configuration/domains/)

## Local implementation and live rollout

Local work can cover real auth flows, persistence/reopen, isolation, inbound
MIME fixtures, duplicate delivery, queue replay, threading, private attachment
reads, outbox transitions and ambiguous sends without emailing anyone. Use a
local capture transport for verification/recovery mail and outbound tests.

Live activation requires a selected Cloudflare-managed domain and Email Service
onboarding, sender entitlement and DNS authentication. Do not change an existing
production domain's MX records or send real test mail as a side effect of local
implementation. Select a dedicated mail domain/subdomain and review the concrete
routing/DNS changes before activation. Arbitrary-recipient sending requires the
paid sending product; verified-destination behavior is a separate restricted
mode. Cloudflare acceptance is not proof of recipient inbox placement.


## Implemented example and validation

The [mail example](../../examples/cloudflare-serverless/MAIL.md) has two production
Worker entry points: a private Better Auth/native metadata owner and a mail app
with native per-mailbox authorities. A third capture Worker is used only in the
generated loopback development configuration. It has no SMTP binding. The
browser UI supports verified login/recovery, inbox/search, attachment download,
compose/reply and outbox inspection. Outbound intent quotas are atomic per mailbox.

Current ownership is one private mailbox per user. Shared memberships, labels,
drafts, account/content deletion and delivery-event reconciliation remain future
work. Account deletion is disabled in the application until that lifecycle is
implemented. Provider acceptance remains distinct from recipient delivery.

Live SMTP events receive independent UUID receipts. Immutable content hashes
deduplicate raw bytes only; identical messages and repeated Message-IDs remain
separate deliveries. Queue replay of an existing receipt remains idempotent.
Projection metadata is small and transactional; large immutable projection bytes
are prepared in R2 and adopted before native WAL mutation. New publication hints
are protected from an earlier publisher's cleanup with a token comparison.

Local validation passes the 121-test auth suite (including all 109 upstream
adapter cases), the 388-test Node regression run, and nine mail/UI tests including
two additional DOM cases. Actual workerd checks verified signup/login for two
users, injected native transaction rollback with no verification mail release,
queue replay, repeated identical messages, envelope/Bcc routing, Antfly search,
private attachments, replies, outbox isolation, persisted reopen and logout.
Guards reject application SQL/KV storage access. The local HTTP dev command also
passes signup, session, inbox/outbox and logout using capture-only delivery.
The browser connector could not initialize, so the UI received DOM/HTTP checks
rather than a visual browser review.

The native integration reused the unchanged qualified WASM artifacts from
`9b81fa0021`; no native engine code was changed or rebuilt for this auth work.
These local checks do not qualify a live domain, SMTP delivery or the performance
gate. No existing MX/DNS/routing records were modified and no real mail was sent.
