# Collaboration source qualification

Status on 2026-10-26: local contract checks pass; live hosted qualification is
blocked by the deployed management API version and provider setup. This is not a
release approval or evidence of a completed live sync.

## Verified locally

| Surface | Result |
| --- | --- |
| Portable sources | Full `go test -race ./...` passes, including the native `antfly-grep` contract |
| Colony | Full `go test ./...` passes; successful Google/Slack/Discord OAuth callbacks and Google partial-consent preservation additionally pass |
| Cloud CLI and Go SDK | Full Go suite passes, including authorize → connected → resources → preview → create → queue for each provider |
| Dashboard | Both source setup tests pass; TypeScript check passes |
| Public API | Bundling and Redocly validation pass; source routes included in generated Go and TypeScript contracts |
| Deployment configuration | Infrastructure Go build and test suite pass; no infrastructure apply performed |

Fixtures verify attachment selection, default private sharing, tenant/guild binding,
account substitution and callback replay rejection, canonical publication, and
search contracts. They do not prove a provider has granted production scopes.

## Live observations

Using the saved Antfly account and `antfly-inc` / `grepstein`:

- `connections list --provider google_workspace` returns HTTP 400, invalid provider.
- `connections connect google-workspace --services drive,gmail,calendar --no-wait`
  returns HTTP 404 for the installation route.
- The configured Discord bot reports zero installed guilds.
- The existing gcloud user token lacks Drive/Gmail/Calendar scopes. Normal users
  should grant these through the Workspace installation flow, rather than gcloud.
- The session's browser connector fails to initialize, so no live browser consent
  was performed.

No live source tables, mailbox/calendar content, provider app registrations,
production secrets, or production infrastructure were changed during these checks.

## Fresh-review regression fixes

Slack worker refresh validates its client credentials independently of browser
origins and persists rotated tokens under the credential-row lock. Calendar
incremental sync refreshes parents with imported Drive attachments even when no
Calendar event changed. Tests cover independent Drive edits, repeated unchanged
sync, revoked Drive access and deleted parent events. Both regressions failed
before their fixes.

## Release prerequisites

Publish/pin the portable sources module and Cloud CLI/SDK revision, then deploy the
matching Colony API, worker and dashboard with migrations 125–129. Colony must pin this published module revision; final release checks must use
`GOWORK=off` with that dependency. Drain older workers for the unreleased provider migration.

Register the exact backend callback for each enabled provider:

```text
/api/v1/source-oauth/google_workspace/callback
/api/v1/source-oauth/discord/callback
/api/v1/source-oauth/slack/callback
```

Google requires an enabled OAuth app with the selected APIs/scopes and a user grant.
The existing Antfly Google app can be reused once its callback and consent setup
are configured; Google account sign-in alone does not grant ingestion scopes.
Discord needs app credentials, the managed bot token, Message Content intent, and
browser installation into the chosen test server. Slack qualification will use Antfly Inc. (`@antfly.io`), channel
`#all-antfly-inc`; installation and hosted sync/search remain unqualified. Operator deployment instructions are in Colony's
`docs/dev/connected-source-search.md`; no provider secret belongs in a source.

Slack/Discord remain bounded polling sources with resumable acquisition pages and normalized-content checkpoints.
Gmail history and Calendar sync-token enumeration are implemented; Drive changes,
chat event ingestion, unlimited backfills and shared rate-limit coordination are
not qualified by this release test record.

## User walkthrough after deployment

Choose a dedicated source destination and a test Gmail label/calendar/folder.
Run the updated CLI, using IDs returned by each preceding command:

```sh
antfly-cloud --org antfly-inc --instance grepstein connections connect google-workspace --services drive,gmail,calendar
antfly-cloud --org antfly-inc --instance grepstein connections resources CONNECTION_ID --resource gmail
antfly-cloud --org antfly-inc --instance grepstein sources preview --connection CONNECTION_ID --resource gmail --select LABEL_ID --attachments
antfly-cloud --org antfly-inc --instance grepstein sources add --connection CONNECTION_ID --resource gmail --select LABEL_ID --attachments --name workspace-qualification --table QUALIFICATION_TABLE --every 24h --permission-ttl 15m
antfly-cloud --org antfly-inc --instance grepstein sources list
```

The browser signs in as the same Antfly user, selects the Google account and grants
access; CLI status polling returns the connection UUID. Add Calendar and Drive as
separate sources using the same connection with `--resource calendar --select CALENDAR_ID`
or `--resource drive --select FOLDER_ID`. Permission defaults stay private.

For Discord, use `connections connect discord`, choose the test server in the
browser, then `connections resources CONNECTION_ID --resource discord` and select
channels for preview/add. No bot token or guild ID should be entered by the user.

Wait for an actual successful run and a searchable catalog publication. With normal
Cloud context, run `antfly-grep --cloud --files`, then search the source alias for a
known message and attachment phrase. Confirm provenance and parent links. Repeat
after a message/event edit and attachment removal; confirm old content disappears
without affecting sibling sources. Revoke provider access and verify the source
cannot renew its searchable lease. Exercise consent denial/reconnect, another
Antfly user's denied access, and retained query snapshots before recording live
qualification as passed. Do not label queue acceptance as successful ingestion.


Local regressions additionally cover cached Drive download revocation, resumable
identity-only absence checks, rate-limited permission audits, managed chat token
validation, daily content schedules with independent access renewal, discovery
clock agreement, and bounded checkpoint cleanup. These fixture results do not
establish a live provider installation or hosted qualification.
