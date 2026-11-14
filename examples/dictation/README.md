# Antfly Dictation

<img src="Assets/AppIcon.png" width="128" alt="Antfly Dictation: Antfly outline with a whisper emoji" />

A native macOS push-to-talk app demonstrating Antfly inference and application
state: hold **Fn / Globe**, speak, release, and paste into the original
text field. It includes Apple Speech and Whisper selection, cleanup presets,
per-app preset selection, a dictionary, incremental Whisper cleanup, JSON import/export,
Antfly settings snapshots, and optional searchable history.

The app uses SwiftUI, AVAudioEngine, Carbon global hotkeys, Accessibility, and
Keychain. Sparkle is its pinned third-party update dependency. Apple Speech runs through
Antfly's existing native provider; the app does not ship a second speech engine.

## Install the macOS app

Official releases are Apple Silicon ZIPs containing a signed, notarized app and
its pinned Antfly runtime.

[Download v0.1.0-beta.1 for Apple Silicon](https://releases.antfly.io/antfly-dictation/releases/0.1.0-beta.1/Antfly-Dictation-0.1.0-beta.1-arm64.zip).

After downloading:

1. Quit any development copy and unzip the download.
2. Move **Antfly Dictation.app** into `/Applications` or `~/Applications`, then open it.
3. Allow microphone access and enable the app in Accessibility for Fn/Globe and paste.
4. Keep **On this Mac** selected. Automatic presets use ready Apple speech on
   macOS 26+, otherwise select **Download Whisper Tiny…**. No server address or
   bearer token is required for bundled mode.
5. Optional: select a cleanup generator, customize presets and vocabulary, and save settings.

Installed official apps provide **Check for Updates…** and optional automatic
updates. App updates include the bundled engine and preserve settings, models,
history, and Keychain credentials. The public Dictation download channels require
the first signed app release; publishing an Antfly engine release alone does not
publish the app. See [release activation and installation](UPDATES.md).

## Voice formatting and insertion

Voice formatting runs locally and does not require a cleanup generator. In
Settings → Speech & text → Voice formatting, spoken breaks and insertion spacing default on:

| Say | Inserted text |
| --- | --- |
| new line, next line, line break, skip a line | One line break |
| new paragraph, start a new paragraph | A blank line between paragraphs |

Commands can be spoken alone. Straight, curly, nested, escaped, and unclosed
quoted phrases are preserved as literal text; apostrophes inside words are not
quotation delimiters. For fully literal dictation, choose Verbatim and turn
spoken formatting off. This is
a deterministic English command vocabulary, so unquoted occurrences are treated
as commands. It does not reproduce every Wispr Flow editing or formatting feature.

Commands and quotations are parsed from recognition before cleanup. If cleanup
is enabled, their contents are replaced by request-specific protected markers,
without issuing a request per command. Markers must survive exactly once and in order; otherwise
cleanup fails with the original transcript available for recovery. No generated
text is interpreted as a new command. Internal and edge breaks are restored from
the protected plan, rather than inferred from the model's output.

Recognition/cleanup supplies punctuation; insertion adds boundary spaces when
Accessibility exposes the surrounding text. Existing spaces, newlines, punctuation,
selected-text replacements, and CJK boundaries are preserved. It reads small
Accessibility ranges, with a bounded fallback for small text fields.
Surrounding text is never stored; unsupported fields keep the transcript unchanged.

**Allow a trailing ‘press enter’ voice command** is off by default. Enabling it
allows a terminal **press enter** in recognized speech to send Return after the
text is pasted. This can send a message or submit a prompt. Mid-sentence and quoted
occurrences stay literal; generated text cannot trigger it. A command-only
“press enter” does not send a key without a nonempty insertion. The app verifies
the original app and text field, observes the expected text and cursor after the
paste, and rechecks focus, clipboard ownership, and cancellation before sending
Return. Accessibility queries run on a serial worker with short per-object IPC
timeouts. Confirmation has an eight-second monotonic deadline, including time
spent reading the target. The clipboard lease remains owned until confirmation
finishes, so a slow paste cannot consume the user's restored clipboard. New
copies or overlapping insertions invalidate an older pending Enter action. If
verification is unavailable or times out, Enter is not sent and the transcript
remains available to copy. Copy,
history, and original-transcript recovery never replay key actions.

Stable-segment cleanup remains incremental. If a command or quoted span crosses
a cleanup-batch boundary, the complete utterance is cleaned once more with a
single protected plan before insertion. Enabling the trailing Enter command keeps
recognition streaming but defers cleanup until release, because its terminal
position cannot be established earlier. The processing note explains this choice.
Recorded-audio cleanup likewise runs after recognition so commands can be
protected first. These paths use the configured inference concurrency limit.

Named transcribers retain their text-cleanup `/dictate` path. Raw runtime models
use the portable `/generate` contract for cleanup, which preserves compatibility
with runtimes that reject `/dictate` requests containing `transcript_text`.
External raw runtimes must permit generation for the selected cleanup model.
Protected requests keep the selected Clean/Rewrite style. Full-size user rewrite
instructions are preserved; when internal marker rules would exceed `/dictate`'s
instruction limit, cleanup uses the portable generation contract instead.
Clean-mode cleanup windows are limited to 3,072 encoded UTF-8 bytes and request
up to 4,096 output tokens on both backends. Each request budgets one token per
input byte plus 1,024 tokens for cleanup changes and response framing. The byte
bound includes marker expansion and supports byte-fallback tokenizers.

Long transcripts use at most 2,048 bytes of new input plus a recent editable
output tail of up to 1,024 bytes. A correction at the next window's start can
retract words in that tail instead of leaving both the original and corrected
word. Text older than that tail is committed; corrections referring further back
need Whole utterance processing. Live recognition reuses the same context and
revises the previous input window as new finals arrive, rather than independently
appending cleaned segments or regenerating the entire prefix. An unchanged final
flush does not invoke the model again. The context is discarded after a failure
or cancellation so retries cannot reuse incomplete state.

Unicode word boundaries handle Chinese, Japanese, Thai, punctuation, and spaces.
Oversized words and URLs fall back to grapheme boundaries. Protected markers are
always atomic, and splitting/reassembly preserves every input character.

Portable cleanup also requires a fresh completion token at the end of each
response, independently of `finish_reason`. This detects silently capped output
from older Apple runtimes bundled with existing downloads. Missing, wrong,
duplicated, empty, or nonterminal frames fail with raw transcript recovery and
cannot become eligible for Enter. The token is removed before any preview or
insertion. Updated Antfly Apple providers additionally verify natural completion
on generic `/generate` calls and reject incomplete output at the provider boundary.

Rewrite and explicit Whole utterance processing preserve one generation context.
If their encoded input exceeds this capacity, the app reports the limit before
calling a model and preserves the raw transcript. Choose Clean with Automatic
processing, shorten the recording, or insert the original transcript. Model
context limits still apply; responses are bounded, and truncated or incomplete
output cannot become insertable text. Formatting markers own
surrounding line-break whitespace; quoted literal contents remain unchanged.

Enter confirmation prerequisites are checked before Paste is posted. Failed or
canceled insertion attempts retain the clipboard payload for the normal 750 ms
delivery grace period; successful confirmation can restore it immediately.
Clipboard intent is reserved before preservation or preparation begins. A newer
operation immediately invalidates older key actions without losing the original
clipboard snapshot. Provider waits are cancellable and time out after five
seconds. A nonresponding lazy clipboard provider is not allowed to spawn more
provider reads or block shutdown; at most one provider read remains in flight,
and completed snapshots are released rather than cached indefinitely.

These features implement the common spacing/break/Enter workflow in Wispr Flow;
they do not add recognition vocabulary bias or dictionary replacement rules.

## Build and run

Requires an Apple Silicon Mac with macOS 15+ and a Swift 6 toolchain. Apple transcription additionally
requires **the Antfly host** to run macOS 26+ with eligible Speech hardware,
supported locale assets, and an Apple-provider build. A Mac client connected to
a Linux server cannot use the Mac's Speech engine through that server.

```bash
cd examples/dictation
BUNDLE_RUNTIME=1 ./build-app.sh
open 'dist/Antfly Dictation.app'
```

The build keeps caches under `.build/` and ad-hoc signs a normal application
bundle with a microphone usage description. Set `SIGNING_IDENTITY` for your own
Developer ID signature; distributing outside your machine also requires Apple's
usual notarization. Replacing an ad-hoc signed app can require regranting
Accessibility access. Run the bundle, rather than a bare `swift run` executable,
so macOS can associate permissions with its stable bundle identifier.

1. In Settings, allow the microphone. Enable Accessibility to use automatic paste.
2. Use **On this Mac** for the bundled engine. Automatic local presets prefer ready
   Apple speech; use **Download Whisper Tiny…** when Apple speech is unavailable.
   For another server, select **External Antfly server** and enter its HTTP(S) origin.
3. For an external server only, enter a bearer token if required and save it.
   Tokens are stored in Keychain, scoped to the server origin; they are never in
   settings files or snapshots. Bundled mode manages its own credentials.
4. Discover models. Named transcribers include provider facts, live-preview
   support, and native Apple availability/installed locales in the catalog.
5. Import `presets/apple.json` or `presets/whisper.json`, or edit the presets.
6. Configure a cleanup generator to enable dictionary correction and rewriting.
   The Apple sample selects the `cleanup` Apple generator in `antfly.yaml`;
   generation readiness is independent of Speech. Replace it with another
   generator if needed. Without a generator, output is raw transcription.
   Verbatim always skips cleanup.
7. Preferences save automatically. To save a snapshot in Antfly, use Settings →
   Advanced to create the storage table and publish it.
8. Focus a text field in another app, hold the shortcut, speak, then release.

For Apple, build the server from the repository's `zig/` directory:

```bash
zig build antfly -Dapple-providers=true
```

Merge `antfly.yaml` into your server configuration and enable the public inference
API as appropriate for your deployment. Use `antfly standalone --help` for the
server's configuration flags. The current Apple bridge build requires a macOS 27
SDK / Swift 6 command-line toolchain; the bridge's minimum runtime OS is 26.
See [native Apple providers](../../docs/guides/providers/apple.mdx) for sidecar
installation and asset errors. `download_assets: false` never initiates a model
setup download; opt in explicitly on the server when installing missing assets.

For local Whisper, pull a model once:

```bash
antfly inference pull openai/whisper-small --tasks transcribe
```

Start your Antfly runtime/server using your normal configuration. Configure a
named transcriber (`transcribers/whisper`) and a named generator (`cleanup`) to
use the same cleanup path as Apple. A raw local Whisper model also works; its
cleanup field then uses a raw local generator model, as in the runtime's existing
`/dictate` contract. The cleanup picker offers only named generators for named transcribers; changing
to a raw runtime transcriber clears the named cleanup selection. Discovered hosted
connection transcribers advertise shared text cleanup and preserve named generators;
runtime connections retain their raw-model cleanup contract. The selected routing
is saved with the preset, so it also works after restarting offline. Named Whisper
transcribers accept named cleanup generators,
including Apple or hosted generators. The gateway preserves Whisper dictionary
hints and decoding options on the recognition stage.

## Downloaded apps and local engine

Official downloads are Apple Silicon only and include a pinned, released Antfly runtime. New installations use **On this Mac** by default. In Settings, explicitly
download Whisper Tiny, or use native Apple speech and cleanup on macOS 26+
when the required assets and Apple Intelligence are ready. Automatic local presets
prefer ready Apple speech and otherwise use Whisper; explicit transcriber choices
are preserved. The bundled v0.3.0
runtime includes the Apple bridge. The app handles startup,
authentication, model discovery, and shutdown. No separate server install is needed.
Existing configurations keep their chosen external server. See [updates and
managed runtime](UPDATES.md#managed-runtime-for-downloaded-apps) for lifecycle,
packaging, data retention, and troubleshooting.

## Presets and shortcuts

Each preset stores an output mode (Verbatim, Clean, or Rewrite), processing strategy
(Automatic, Incremental, or Whole utterance), transcriber, optional cleanup
generator, language, tone, context, instructions, local dictionary, application
bundle IDs, and an optional legacy live-preview choice. The first preset matching the frontmost app's bundle ID wins;
otherwise the selected default is used. For example, use `com.apple.mail` for
Mail. Global dictionary entries are combined with preset entries, up to 256 terms.

The default shortcut for new settings is **Fn / Globe**, matching
[Wispr Flow's Mac default](https://docs.wisprflow.ai/articles/3152211871).
Choose **Control + Option** in the shortcut presets for keyboards without Fn.
The default behavior is hold-to-talk; toggle mode starts/stops on successive presses.
The menu bar also provides record, finish, cancel, copy, and settings controls.

**Set up shortcut & paste access** registers and saves the selected shortcut in one action. If permission
is missing, it opens Accessibility settings and finishes automatically when you
return after granting access. Cancel setup keeps the previous shortcut. This action
saves only the shortcut; other unsaved settings and credentials are not changed.
**Keyboard settings** opens the Globe-key preferences without changing them.

Server edits debounce discovery and cancel obsolete requests. HTTP failures show
the bounded structured server error and retry guidance rather than a generic
readiness message. Full WAV conversion runs off the UI actor only for clip upload,
retry, or explicit audio retention; successful live sessions use just their PCM tail.
Release-to-result timing includes conversion and finalization. Live-processing
backpressure can disable live processing without discarding the complete recording.

Settings also provide clickable Fn/Control/Option/Shift/Command buttons, a live shortcut
label, **Record shortcut**, **Modifiers only**, and **Reset to Fn**. During selection,
dictation is paused and captured keystrokes are consumed locally. Release your
keys to accept the selection, or press Escape to cancel. Completed selections save automatically;
a registration failure preserves the previous working shortcut. Settings and
credentials still save when shortcut permission is missing; the shortcut status
shows the remaining setup step. Reapplying an active shortcut is idempotent. Common editing
and system commands such as Command-C/Command-V cannot replace dictation shortcuts.

Modifier-only and Fn combinations use a dedicated global keyboard event loop and
need macOS Accessibility access. Click **Set up shortcut & paste access**, grant
access in System Settings, then return or choose Set up shortcut & paste access again. If macOS also opens
its emoji picker, set **Press Globe key to → Do Nothing** under Keyboard settings.
The app never changes system keyboard preferences or grants itself permissions.
Ordinary Fn navigation/key combinations interrupt a modifier-only dictation and
pass through instead of inserting accidental speech. Other key chords retain
Carbon conflict detection. Only one app instance should register the shortcut.

Existing saved shortcuts are preserved, including older numeric Carbon settings;
click Reset to Fn if you want the new default; the change saves automatically. Exported settings retain
compatibility fields while the optional `shortcut` object stores native modifier
flags and an optional physical key code. No numeric editing is required in the UI.

Recording stops after ten minutes and audio is bounded to 120 MiB. Live preview
has a separate bounded queue. AVAudioConverter turns microphone audio into 16 kHz mono speech audio; final
clips are PCM16 WAV (about 19 MiB for ten minutes), while preview uses float PCM.
Final clips use the binary AFATT001 attachment envelope, keeping audio out of
JSON and avoiding base64 expansion and metadata allocation limits. A server can
impose a smaller audio limit; shorten the recording if that limit is reached.

**Verbatim** is the default when no text processor is configured. **Clean up**
fixes punctuation, filler words, and dictionary spellings using a text processor.
**Rewrite** applies tone and custom instructions using the complete utterance.
Selecting Clean up or Rewrite requires a processor before recording. Existing JSON
presets without an explicit mode infer Verbatim when no processor is configured;
otherwise they infer their mode from style/instructions. Explicit choices persist.

Automatic uses named Whisper transcribers advertising `stable_segments` and
`text_cleanup`: final ASR segments are cleaned during recording, release commits
the remaining audio, and the app assembles completed cleanup without another full
clip pass. Partial hypotheses (including `stable_text`) are display-only. Queued
finals are coalesced; event sequence/timing guards prevent duplication and reject
revised final segments. A per-client FIFO gate serializes inference requests so
servers with concurrency one work while microphone capture proceeds independently.
For servers with at least two inference slots, enable Allow parallel recognition
and cleanup in Connection settings to run those requests concurrently.

Whole utterance can recognize during recording but cleans once on release.
Incremental explicitly requires stable sessions and disallows Rewrite. Automatic
and Whole utterance can fall back to the recorded clip using the same provider if
session recognition fails, including finalization after release; the UI explains
this. Cleanup failures and cancellation never re-run recognition. Explicit
Incremental fails instead.

External Apple transcribers without stable-segment metadata recognize recorded
files after release. The bundled Apple engine uses native streaming on supported
Macs. Unknown server capability metadata uses recorded clips.
Legacy live preview on older Whisper servers remains provisional and requires the
final full clip pass. Sessions are deleted on completion/failure/cancel; idle expiry
bounds sessions whose creation response was interrupted. Session disposal runs separately
from completed result delivery, uses a three-second HTTP timeout, and falls back
to the server TTL when unavailable. Live chunks also use binary
attachments. Cleanup may return one complete-text delta per batch; pipeline overlap
does not imply token-streaming generation. No provider is changed automatically.

Only naturally completed, nonempty cleanup output can replace the transcript.
Truncation, filtering, tool calls, missing completion evidence, or empty cleanup
produce an error while preserving the raw transcript for recovery. Empty speech
recognition succeeds without invoking cleanup. Apple cleanup requires bridge ABI
2 and runs to natural completion under the request deadline and a byte bound of
eight times the configured token budget; macOS 27 also verifies measured output
tokens against that budget. There is no silent engine or cloud fallback.

## Insertion and recovery

The app captures the frontmost application's process and focused Accessibility
text element at recording start. It does not activate that app later or steal
focus for its nonactivating recording overlay. Paste runs only if both the application and text
field still match. A focus change, missing permissions, or an insertion error
leaves the text available to copy. The menu bar icon indicates recording and
processing; the overlay shows Listening, Transcribing, Cleaning, and completion.
The popover and Dictation view keep raw, cleanup-preview, and final text separate.
The app pastes once after successful completion, rather than revising text in another app.
Native and TypeScript clients reject duplicate transcripts/completions and any text
mutation after completion. The native app waits for complete stream validation before insertion.
Silence produces no insertion. Expand Processing details for timings and fallback explanations.

Paste temporarily places the text on the clipboard, sends Command-V, then restores
the previous clipboard if no other app changed it. Restoration runs separately after
750 ms and does not delay the next recording. Overlapping pastes preserve the original
clipboard; a user copy takes ownership immediately. Snapshotting runs off the UI
thread and rejects more than 16 items, 64 representations, or 8 MiB of retained
data, offering manual copy instead. macOS can materialize a large promised
representation before its size is known; the app never retains it beyond the limit.
Focus is checked again immediately before sending the paste. Normal Quit waits for
an owned restoration; a concurrent user copy still prevents restoration. The OS does not acknowledge
whether a recipient actually inserted the text; “paste sent” records that limit.
Secure or custom text controls may need manual copy/paste.

Cancel stops microphone capture, cancels inference, invalidates pending callbacks,
and prevents further insertion. A paste already sent cannot be undone by cancel.
Cleanup failures offer Insert original, Copy original, and Retry cleanup. The latter
sends finalized text through `/dictate` with `transcript_text` and `audio: ""`, never
repeating recognition; it produces copyable text using corrected settings. Audio
is released after recognition when retention is off. A failed recognition clip
remains in memory for retry until a new recording, cancellation, or app exit. Retry uses the current connection and the updated version of the original preset,
produces copyable text, and never reuses an old insertion target.
Completed transcripts can be edited, copied, or exported as plain text.

## State, snapshots, and history

Settings and optional history are cached under
`~/Library/Application Support/AntflyDictation/`, using owner-only files. This cache
loads at startup and works without Antfly. User data never lives in this example's
source directory. Settings import/export uses versioned JSON and validates origin,
preset IDs, table/profile names, dictionaries, style, and size limits.

**Publish settings snapshot** writes the current settings, presets, and dictionary
to the selected Antfly table under `settings-<profile>`. **Replace settings from
Antfly** explicitly replaces the local configuration from that profile while
preserving the current connection/table/profile and its credential scope. There
is no background merge: local edits win until replaced, and publishing overwrites
the chosen remote snapshot. Use separate profile names for independent devices.

History and audio retention are off by default:

- With history enabled, up to 500 transcripts are cached locally and each result
  is queued for Antfly with raw/final text, model, preset, application, time, and
  insertion status. Network uploads run independently of recording and retry with
  backoff across app restarts. The durable outbox holds at most 500 jobs / 64 MiB,
  preserving each original server and table without storing credentials. History
  sync status is separate from dictation status. A separate revision-fenced archive
  writer keeps local retention and deletion working even if the optional outbox
  cannot open. Upload local history can also
  retry writes by stable document ID without creating duplicates.
  Recording completion schedules persistence and returns to idle immediately.
  The ordered background writer coalesces archive snapshots, bounds pending upload
  work, and limits local history to 500 entries and a conservative 16 MiB JSON byte
  budget. An entry too large for that budget remains available to copy/export.
  Persistence errors appear in History sync status. Normal Quit drains local writes,
  including explicit local clears and completed audio archives, and finishes any
  owned clipboard restoration; active recording work is canceled. Remote uploads remain durable in the outbox for restart.
- The table's full-text index searches final transcript text. Search results are
  shown as the server's JSON response, including document IDs and sources.
- Clear local history removes the local cache. Delete local history and matching
  Antfly documents pauses uploads, waits for dispatched writes to finish, cancels
  all matching pending jobs, and deletes
  the cached entries at their original server/table destinations before resuming.
  A remote deletion failure keeps local entries available for retry, but canceled
  uploads cannot restart automatically. A separate bounded deletion journal stores
  document IDs, original destinations, and acknowledgments before jobs are removed.
  Final local cleanup runs through the same ordered writer as ordinary snapshots.
  The journal is removed only after an actual archive commit; a newer requested
  snapshot that fails cannot suppress a pending deletion.
  It survives restarts, local clears, and retention changes without retaining text
  or credentials. Retry pending deletions completes the remaining targets without deleting newer transcripts,
  even when the local history list is empty. Startup cancels matching queued uploads
  before dispatch; corrupt deletion metadata pauses uploads while local retention
  remains available. If the queue cannot be loaded, remote deletion
  fails closed and retains local history; it never reports success while unknown jobs
  remain. Turning retention off retires the unavailable queue so it cannot upload on
  restart. Retention changes invalidate and drain older producers, including manual
  uploads and startup migration, before purging. Manual uploads also preserve destinations.
  Legacy archives recover destinations from pending jobs; already uploaded legacy
  entries without destination metadata bind once to the configuration that loads
  them, since their earlier destination cannot be reconstructed. Turning history retention off removes
  pending uploads and the local archive. Older remote
  records beyond the local count/byte retention window need normal Antfly table management.
- Audio retention keeps only `last-recording.wav` locally, never in Antfly. Archival
  runs independently of recognition and insertion; a disk failure produces a separate
  warning. One writer stages audio, with only the newest pending clip retained while
  disk I/O is busy. Starting another recording does not invalidate an older completed
  archive; a newer finished clip can supersede it. Normal Quit drains the writer. Canceling active processing and retention changes
  still fence its pending archive writes. Cancellation permanently revokes that
  clip's publication rights, including when newer queued clips are also canceled.
  An eligible older completed clip can still finish. Saving
  settings with retention disabled deletes retained local data; remote transcripts
  require explicit deletion. Inference providers have their own retention policy.
- Missing or invalid settings preserve retained audio/history and pause uploads.
  Restore/import settings or explicitly save the desired retention preferences to
  recover. Recording and shortcut-only setup wait for recovery; a missing settings
  file cannot silently turn shortcut setup into a retention change.
- Settings are limited to 1 MiB across validation, saving, snapshots, and import.
  Oversized edits leave the prior settings and Keychain credentials intact.

See the [voice dictation guide](../../docs/guides/voice-dictation.mdx) for the
complete mode/capability matrix and the stable-session/text-cleanup API contract.

## App icon

The icon preserves the Antfly vector outline with the system's 🤫 whisper/shushing
emoji in the center. The checked-in `.icns` contains standard and Retina sizes
from 16 to 1024 pixels. Normal app builds copy it into the bundle. Regenerate it
on macOS from this example directory:

```bash
mkdir -p .build/icon-modules .build/clang
CLANG_MODULE_CACHE_PATH="$PWD/.build/clang" \
  swift -module-cache-path .build/icon-modules build-icon.swift
```

The renderer uses `Assets/AntflyOutline.svg` derived from the repository logo and
Apple Color Emoji. Intermediates stay under `.build/AppIcon.iconset`. Quit/reopen
the app after rebuilding to load the new bundle/icon. Menu bar symbols still show
recording/processing state.

## Validation

```bash
./test.sh
```

The tests cover settings validation/round trips, credential-free exports,
application preset selection, recording transitions, SSE framing and incomplete
streams, authenticated persistence, and session append/delete behavior. They do
not access a microphone or change system permissions.

Backend integration tests use local fixture providers, with no downloaded model:

```bash
# From repository root, after building zig/zig-out/bin/antfly:
ANTFLY_BIN="$PWD/zig/zig-out/bin/antfly" \
  uv run --project zig/e2e/antfly pytest -q zig/e2e/antfly/test_dictation.py
```

To include real on-device Apple Speech using the checked-in audio fixture, build
with `-Dapple-providers=true`, install the en-US assets, and prefix that pytest
command with `ANTFLY_TEST_APPLE_SPEECH=1`. This checks native capability discovery,
file recognition, shared cleanup, Verbatim, and streaming without using a microphone
or downloading model assets.

With an installed `openai/whisper-tiny` and ready native Apple generation, add
`ANTFLY_TEST_WHISPER_SESSIONS=1` to exercise real Whisper VAD finals, binary live
PCM uploads, native cleanup before release, and a final tail commit, with server
inference concurrency one. `ANTFLY_TEST_WHISPER_MODEL` can select another already
installed Whisper model. This opt-in test never downloads assets.

Manual macOS smoke test:

1. Launch the signed app bundle; verify the icon, menu bar item, and settings tabs.
   Select Fn, grant keyboard Accessibility if needed, and verify it works in another
   app. Try Fn+arrows, modifier-only Control+Option, a recorded custom chord, Escape
   during selection, and a conflicting shortcut. Confirm selection never records audio.
2. Deny microphone access and verify a useful error; grant it in Settings and retry.
3. In TextEdit, hold/release the shortcut. Verify one result is pasted and the
   prior clipboard is restored. Repeat in toggle mode.
4. Switch text fields/apps during processing; verify no paste goes to the new target.
5. Cancel recording and inference; verify no later result is inserted.
6. Test Apple with installed assets and with a missing/unsupported locale. Test
   Whisper with Automatic Clean and pauses between phrases; confirm cleanup preview
   arrives while recording and only the remaining tail is processed on release.
   Compare Rewrite/Whole utterance and Verbatim. Confirm explicit Incremental rejects
   Apple/unknown capabilities before microphone capture. The overlay must not steal focus.
   Fail cleanup and verify all three recovery actions; copy-only results remain visible.
7. Restart the app to check local settings/history; publish then replace an Antfly
   snapshot and search a saved transcript. Export JSON and confirm no credentials.
8. Stop the server, retry, deny Accessibility, or choose a conflicting shortcut;
   verify copyable recovery and the previous working shortcut remain available.

Validation can build a separate signed bundle without replacing a running app:

```bash
APP_OUTPUT_DIR="$PWD/.build/validation-app" ./build-app.sh
```

Use a stable `SIGNING_IDENTITY` for repeated local installs if available. Replacing
an ad hoc signed bundle can invalidate its existing macOS Accessibility grant.

The macOS example workflow runs only for changes to this example’s build/test
inputs or its workflow definition. Documentation-only and unrelated repository
changes skip the dictation test and signed-app build job.

## App updates

Official releases use Sparkle for signed stable/beta updates, with update controls
in Settings and the application menu. Development builds never enable updating.
Install official releases in Applications; restarts wait for dictation and local
persistence to finish. Server and model updates remain independent.

See [updates and release setup](UPDATES.md) for organization signing credentials,
notarization, protected GitHub setup, independent release tags, recovery, and the
optional managed-local-server design.

## Getting ready and everyday use

The Setup & permissions checklist separates microphone permission, Accessibility permission,
shortcut registration, and speech model discovery. Recording from the window is a
useful test without Accessibility; dictating into another app requires its text field
to be focused before using the global shortcut. Permission status refreshes when you
return from System Settings.

The window opens to **Dictation**: a compact preset and shortcut strip, a test
button, and the latest result. **Presets**, **History**, and **Settings** have their
own actions; import/export JSON and remote snapshots live in Settings → Advanced.
Completed setup collapses to a ready summary and reopens when something needs
attention. Missing text processors get a separate, actionable banner with
**Choose processor** and **Use Verbatim**; speech readiness alone does not imply
that text cleanup is configured. Raw transcripts and processing timings are under
an expandable details section in Dictation.

Settings are grouped into General (updates and retention), Speech & text (engine and
voice formatting), Shortcuts & permissions, and Advanced (server/storage details).
Ordinary preferences save after a 650 ms pause. Saving waits until recording,
shortcut capture, and other owned operations finish; normal quit flushes valid
pending edits. The footer shows pending, saving, saved, or failed state. Invalid
edits and write failures remain visible with a retry action in Settings and Presets.
No JSON serialization happens during ordinary result rendering.

Retention toggles are drafts until **Apply retention changes** is confirmed.
Canceling leaves active retention untouched. Imports, remote preference replacement,
history deletion, model downloads, and publishing remote snapshots also require explicit confirmation. Other preferences continue
autosaving while retention toggles are staged. Corrupt/missing-settings recovery
requires explicit saving and never silently resumes uploads. History shows **Off**
in its tab when disabled, a compact empty state with **Enable history**, and an
optional search of existing server history. Earlier local entries remain accessible;
empty list, upload, and deletion controls stay hidden in the off state when there is nothing to act on.
Text history and local audio retention are independent.

New installations start with Verbatim output. **Clean up** removes fillers and fixes
punctuation; **Rewrite** also applies tone and instructions. Both require a text
processor selected in the preset. Recording is blocked with an actionable
configuration message rather than silently ignoring the selected output mode.
The same validation also applies to retries; a configuration error preserves
original-text recovery. Preferred spellings are vocabulary hints, not guaranteed
recognition replacements.

For the bundled engine, **Install / check … speech** explicitly permits Apple
asset downloads, restarts the engine, and sends one second of generated silence to
install and verify the active preset’s language (the Mac’s preferred supported language when no explicit language is
selected). This does not record microphone audio, paste text, or create history. Setup displays its current stage and can be canceled;
Apple controls the download and does not expose percentage progress through this
released runtime. Installation uses a separate cancellable HTTP session and an
explicit `X-Antfly-Inference-Timeout-Ms: 1800000` budget supported by the pinned
v0.3.0 engine. The setup request allows 30 minutes, with a short client grace period
for the server's final response. Normal dictation keeps its existing shorter
request budgets; this does not change inference defaults globally. Select another
preset before setup to install/check its language separately. Regional equivalence
that the inventory cannot prove stays unverified until a successful dictation.
External servers must enable `download_assets` on the server host themselves.

Engine checks expose checking, current, and failed states. Failed checks discard
cached model readiness. Apple speech readiness uses the native speech-availability
flag and installed languages, not catalog presence alone. Missing or ambiguous
regional language information stays unverified until dictation confirms it;
external models with an unknown default language do not get a guessed green check.
Automatic selection only chooses Apple when speech assets for the preset language
are known ready and its text processor is compatible with shared cleanup.

Switching bundled speech engines checks all configured text processors before
starting setup. An incompatible raw model or missing generator blocks the switch
and offers explicit choices: use Verbatim for affected presets, or use the bundled
Apple processor if it exists. Compatible processors and rewrite instructions are
preserved. The app commits the candidate preset selection only after setup succeeds.
Apple text processing still requires Apple Intelligence and its generation assets,
independently of Apple speech. Setup success, failure, and cancellation messages
identify the engine being configured; failed/canceled setup does not trigger an
automatic transcriber switch.

The unsaved indicator is cached on configuration/credential edits. Shortcut-only
setup updates only the saved shortcut baseline, so unrelated changes remain pending
without serializing the full configuration during recording UI updates.

Choosing **Automatic** in a preset is an explicit new selection and clears the
suppression left by a completed, failed, or canceled setup. Automatic selection
keeps an incremental Whisper preset on a compatible engine. An explicit switch
to an engine without stable segments offers **Use Automatic processing for
incompatible presets** before installation starts. Text-processor and processing
resolutions compose, so resolving one conflict does not undo the other choice.

Installation and preference saving are separate steps. New settings are written
to a private staging file and atomically published before the app activates the
candidate preset selection. If writing fails, the old selection stays active and
the window offers **Retry saving settings** (without reinstalling) or **Keep
previous selection**. Resolve that choice before editing configuration; installed
speech assets remain available either way. Errors applying already-saved settings,
such as shortcut permissions, are reported as warnings rather than pretending the
settings write failed. Setup success messages retain the language actually checked,
so changing the selected preset cannot relabel an earlier language verification.

Setup retries start a stopped managed engine before waiting for readiness. External
Apple speech recovery identifies the server and directs its administrator to install
language assets or enable `download_assets`; it never offers a local-only button.

Durable settings and shortcut writes run on a serialized filesystem worker, with a
visible saving state. The final commit is drained rather than canceled halfway
through; the app applies its known disk outcome before admitting another change.
Quitting waits for these writes and their resulting retention work. Exports use a
separate owned worker, so slow export storage does not hold up preference writes or
recording admission. Import stages credentials for the imported origin and adopts them only after the
settings write succeeds; failures preserve the active credential editor and unsaved
edits. Replacing an external origin invalidates and refreshes model discovery even
when the connection settings are not visible. Changing
processing location reconciles the engine independently of the visible settings tab.

Startup recovery only removes regular, UUID-named `.dictation-settings-*.tmp` files
inside the app's support directory. A private directory lock coordinates cleanup
with active writers across app instances; recovery skips a busy writer. It does not
scan export destinations, follow symlinks, or remove unrelated files.


## Speech languages and cleanup latency

**Use Mac language** follows the Mac’s ordered preferred languages. For native
Apple speech it selects a supported equivalent locale; unsupported preferences
produce a language-selection message instead of silently falling back to English.
An explicit language is preserved across system changes. **Detect spoken language**
is offered for the bundled multilingual Whisper model or an external transcriber
that advertises `language_detection`. Apple speech requires a selected locale.
**Use server default** remains available for external servers. Whisper requests use
its primary language code (for example `fr`); Apple requests retain the locale.
The language actually selected is frozen in each recording’s configuration.

On macOS 26+, bundled Apple speech uses a native `SpeechAnalyzer` input stream in
the app. Audio stays on this Mac; external servers always retain their server path.
The app supplies preferred spellings as native recognition hints and never downloads
assets during recording. The existing recorded clip remains available if live
recognition fails. Bounded audio/result queues fail visibly rather than dropping
samples, and cancellation drains the analyzer’s owned tasks. Final phrases can feed
Automatic/Clean up processing while recording; Rewrite and Whole utterance wait for
the complete transcript. Short utterances may not produce a final phrase until
release. Spoken Enter commands continue to defer cleanup until their final intent
is known. The Advanced preset option **Favor faster Apple speech results** is off
by default: Apple documents a smaller context window and lower accuracy for that
mode. Live recognition itself does not require enabling this tradeoff.

**Prepare Apple text processor in advance**, in Settings → Speech & text, is on by
default when a local Apple text processor is explicitly configured. It sends one
small synthetic generation request per selected processor, refreshing after two
minutes of idle time when dictation starts or engine discovery is requested;
no audio, dictionary, or user transcript is sent, and external services are never
prewarmed. It is optional and has a three-second server budget. Microphone capture
continues independently; server recognition/cleanup drains preparation for the active preset first.
App-specific presets determine which processor is prepared; unrelated preparation
is canceled when recording starts, and Verbatim never waits for a warmup task. Failed warmup falls back
to normal on-demand processing.

Cleanup retains every preferred spelling, deduplicated in preference order.
Recognition errors may bear little lexical resemblance to the intended term, so
unmatched hints are never discarded. Saved dictionaries, user instructions,
completion checks, and output limits are preserved. Cleanup never requests translation.

Raw text appears before cleanup finishes. **Use original now** cancels remaining
processing and selects the raw transcript; automatic paste still targets only the
original captured text field, without executing a spoken Enter command.
Copy-only retries remain copy-only when selecting the original; recovery uses
the active submission’s settings and destination. Cleanup
previews remain non-insertable until complete and protected formatting markers are
never shown as draft prose.

Processing details report release-to-result time, first recognition/response event,
and first cleanup text when available. Queueing, model loading, and prompt processing
are not separately reported by the released runtime and are not guessed.

Optional local checks (existing assets, no microphone input or downloads):

```sh
ANTFLY_RUN_NATIVE_SPEECH=1 ./test.sh
```

The synthetic end-to-end latency probe additionally uses
`ANTFLY_RUN_DICTATION_PIPELINE=1`. Point `ANTFLY_DICTATION_PROBE_SERVER` to an isolated
local test server, or provide the existing local engine credential through
`ANTFLY_DICTATION_RUNTIME_PASSWORD`. Supply credentials only through the environment;
never put them in a command argument, settings file, or captured log. The probe
creates no history and compares paced native recognition plus released cleanup with
a recorded clip through the same server. The generation path is qualified against
the pinned v0.3.0 release; the same synthetic request crashed rc.13 during local
qualification and did not reproduce across repeated v0.3.0 runs.

Instruction-prefix prewarming inside Antfly’s Apple bridge is a separate runtime
change, opt-in because short local samples showed no consistent gain. Its bounded,
expiring, instruction-only session cache never reuses a session
containing a previous dictation. The app continues to bundle the pinned released
runtime; bridge improvements take effect after an Antfly release and pin update.

Native Apple system-language selection uses Apple’s supported-locale equivalence.
Other locale matching preserves writing systems (for example Traditional versus
Simplified Chinese), then tries the next preferred language rather than silently
switching scripts. Transcript assembly preserves explicit whitespace and uses
boundary scripts rather than the preset language when inferring spaces, so Latin
words stay separated in mixed-language dictation.
