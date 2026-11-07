# Antfly Dictation

<img src="Assets/AppIcon.png" width="128" alt="Antfly Dictation: Antfly outline with a whisper emoji" />

A native macOS push-to-talk app demonstrating Antfly inference and application
state: hold **Fn / Globe**, speak, release, and paste into the original
text field. It includes Apple Speech and Whisper selection, cleanup presets,
per-app preset selection, a dictionary, incremental Whisper cleanup, JSON import/export,
Antfly settings snapshots, and optional searchable history.

The app uses SwiftUI, AVAudioEngine, Carbon global hotkeys, Accessibility, and
Keychain. It has no third-party Swift dependencies. Apple Speech runs through
Antfly's existing native provider; the app does not ship a second speech engine.

## Build and run

Requires macOS 14+ and a Swift 6 toolchain. Apple transcription additionally
requires **the Antfly host** to run macOS 26+ with eligible Speech hardware,
supported locale assets, and an Apple-provider build. A Mac client connected to
a Linux server cannot use the Mac's Speech engine through that server.

```bash
cd examples/dictation
./build-app.sh
open 'dist/Antfly Dictation.app'
```

The build keeps caches under `.build/` and ad-hoc signs a normal application
bundle with a microphone usage description. Set `SIGNING_IDENTITY` for your own
Developer ID signature; distributing outside your machine also requires Apple's
usual notarization. Replacing an ad-hoc signed app can require regranting
Accessibility access. Run the bundle, rather than a bare `swift run` executable,
so macOS can associate permissions with its stable bundle identifier.

1. In Settings, allow the microphone. Enable Accessibility to use automatic paste.
2. Set the server origin (default `http://localhost:8080`). Audio and optional
   cleanup text are processed on that server and its configured providers.
3. Enter a bearer token if required and save it. Tokens are stored in Keychain,
   scoped to the server origin; they are never in settings files or snapshots.
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
to a raw runtime transcriber clears the named cleanup selection. Named Whisper
transcribers accept named cleanup generators,
including Apple or hosted generators. The gateway preserves Whisper dictionary
hints and decoding options on the recognition stage.

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

Settings provide clickable Fn/Control/Option/Shift/Command buttons, a live shortcut
label, **Record shortcut**, **Modifiers only**, and **Reset to Fn**. During selection,
dictation is paused and captured keystrokes are consumed locally. Release your
keys to accept the selection, or press Escape to cancel. Save settings to apply;
a registration failure preserves the previous working shortcut. Common editing
and system commands such as Command-C/Command-V cannot replace dictation shortcuts.

Modifier-only and Fn combinations use a dedicated global keyboard event loop and
need macOS Accessibility access. Click **Enable keyboard & paste access**, grant
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
bounds sessions whose creation response was interrupted. Live chunks also use binary
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
Silence produces no insertion. Expand Processing details for timings and fallback explanations.

Paste temporarily places the text on the clipboard, sends Command-V, then restores
the previous clipboard if no other app changed it. The OS does not acknowledge
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
  sync status is separate from dictation status. Upload local history can also
  retry writes by stable document ID without creating duplicates.
- The table's full-text index searches final transcript text. Search results are
  shown as the server's JSON response, including document IDs and sources.
- Clear local history removes the local cache. Delete local history and matching
  Antfly documents pauses uploads, removes matching pending jobs, and deletes the
  cached entries' remote IDs before resuming. Turning history retention off removes
  pending uploads and the local archive. Older remote
  records beyond the local 500-entry window need normal Antfly table management.
- Audio retention keeps only `last-recording.wav` locally, never in Antfly. Saving
  settings with retention disabled deletes retained local data; remote transcripts
  require explicit deletion. Inference providers have their own retention policy.

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
