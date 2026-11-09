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
Settings → Voice formatting, spoken breaks and insertion spacing default on:

| Say | Inserted text |
| --- | --- |
| new line, next line, line break, skip a line | One line break |
| new paragraph, start a new paragraph | A blank line between paragraphs |

Commands can be spoken alone. Quoted phrases are preserved as literal text; turn
spoken formatting off when dictating text that discusses these commands. This is
a deterministic English command vocabulary, so unquoted occurrences are treated
as commands. It does not reproduce every Wispr Flow editing or formatting feature.
Recognition/cleanup supplies punctuation; insertion adds boundary spaces when
Accessibility exposes the surrounding text. Existing spaces, newlines, punctuation,
selected-text replacements, and CJK boundaries are preserved. It reads small Accessibility ranges, with a bounded fallback for small text fields.
Surrounding text is never stored; unsupported fields keep the transcript unchanged.

**Allow a trailing ‘press enter’ voice command** is off by default. Enabling it
allows a terminal **press enter** in recognized speech to send Return after the
text is pasted. This can send a message or submit a prompt. Mid-sentence and quoted
occurrences stay literal; generated text cannot trigger it. A command-only
“press enter” does not send a key without a nonempty insertion. The app verifies
the original app and text field, observes the expected text and cursor after the
paste, and rechecks focus and cancellation before sending Return. If verification
is unavailable or times out, text remains pasted and Enter is not sent. Copy,
history, and original-transcript recovery never replay key actions.

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
7. Save settings. To save them in Antfly, create the storage table and publish a
   settings snapshot.
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
are preserved. The bundled rc.13
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
keys to accept the selection, or press Escape to cancel. Save settings to apply;
a registration failure preserves the previous working shortcut. Settings and
credentials still save when shortcut permission is missing; the shortcut status
shows the remaining setup step. Reapplying an active shortcut is idempotent. Common editing
and system commands such as Command-C/Command-V cannot replace dictation shortcuts.

Modifier-only and Fn combinations use a dedicated global keyboard event loop and
need macOS Accessibility access. Click **Set up shortcut & paste access**, grant
access in System Settings, then return or save settings again. If macOS also opens
its emoji picker, set **Press Globe key to → Do Nothing** under Keyboard settings.
The app never changes system keyboard preferences or grants itself permissions.
Ordinary Fn navigation/key combinations interrupt a modifier-only dictation and
pass through instead of inserting accidental speech. Other key chords retain
Carbon conflict detection. Only one app instance should register the shortcut.

Existing saved shortcuts are preserved, including older numeric Carbon settings;
click Reset to Fn and Save if you want the new default. Exported settings retain
compatibility fields while the optional `shortcut` object stores native modifier
flags and an optional physical key code. No numeric editing is required in the UI.

Recording stops after ten minutes and audio is bounded to 120 MiB. Live preview
has a separate bounded queue. AVAudioConverter turns microphone audio into 16 kHz mono speech audio; final
clips are PCM16 WAV (about 19 MiB for ten minutes), while preview uses float PCM.
Final clips use the binary AFATT001 attachment envelope, keeping audio out of
JSON and avoiding base64 expansion and metadata allocation limits. A server can
impose a smaller audio limit; shorten the recording if that limit is reached.

**Clean** is the default. It fixes punctuation, filler words, and dictionary
spellings when a generator is configured. **Rewrite** applies tone and custom
instructions using the complete utterance. **Verbatim** skips generation. Without
a cleanup generator, settings explain that only recognition runs. Existing JSON
presets infer their mode from their style/instructions; explicit choices persist.

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
session recognition fails; the UI explains this. Explicit Incremental fails instead.

Apple currently recognizes recorded files, so Automatic explains that transcription
and cleanup run after release. Unknown capability metadata also uses recorded clips.
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
The popover and Transcript tab keep raw, cleanup-preview, and final text separate.
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
