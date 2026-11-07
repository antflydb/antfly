# Antfly Dictation

A native macOS push-to-talk app demonstrating Antfly inference and application
state: hold **Control–Option–Space**, speak, release, and paste into the original
text field. It includes Apple Speech and Whisper selection, cleanup presets,
per-app preset selection, a dictionary, live Whisper preview, JSON import/export,
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

Each preset stores a transcriber, optional cleanup generator, language, style,
context, instructions, local dictionary, optional application bundle IDs, and a
live-preview choice. The first preset matching the frontmost app's bundle ID wins;
otherwise the selected default is used. For example, use `com.apple.mail` for
Mail. Global dictionary entries are combined with preset entries, up to 256 terms.

The default is hold-to-talk; toggle mode starts/stops on successive key presses.
The menu bar provides record, finish, cancel, copy, and settings controls. The
shortcut is configurable using the macOS virtual key code and Carbon modifier
mask. Space is 49; Command is 256, Shift 512, Option 2048, Control 4096. The default
Control + Option mask is 6144. Save rejects conflicts without removing the
previously registered shortcut. Only one app instance should register a shortcut.

Recording stops after ten minutes and audio is bounded to 120 MiB. Live preview
has a separate bounded queue. AVAudioConverter turns microphone audio into 16 kHz mono speech audio; final
clips are PCM16 WAV (about 19 MiB for ten minutes), while preview uses float PCM.
Final clips use the binary AFATT001 attachment envelope, keeping audio out of
JSON and avoiding base64 expansion and metadata allocation limits. A server can
impose a smaller audio limit; shorten the recording if that limit is reached.

Apple is a recorded-file backend. Enable live preview only for Whisper models
with transcription sessions. Preview chunks are appended sequentially and sessions
are deleted on stop/cancel; idle expiry bounds sessions whose creation response
was interrupted. The final full clip is submitted to `/dictate` for authoritative
transcription and cleanup, so preview deliberately adds recognition work. Provider
cleanup may deliver one entire-text delta, while the raw transcript arrives first.
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
focus for a recording overlay. Paste runs only if both the application and text
field still match. A focus change, missing permissions, or an insertion error
leaves the text available to copy. The menu bar icon indicates recording and
processing; the popover and Transcript tab show preview/final text.

Paste temporarily places the text on the clipboard, sends Command-V, then restores
the previous clipboard if no other app changed it. The OS does not acknowledge
whether a recipient actually inserted the text; “paste sent” records that limit.
Secure or custom text controls may need manual copy/paste.

Cancel stops microphone capture, cancels inference, invalidates pending callbacks,
and prevents further insertion. A paste already sent cannot be undone by cancel.
A failed clip remains in memory for retry until a new recording, cancellation, or
app exit. Retry uses the current connection and the updated version of the original preset,
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

Manual macOS smoke test:

1. Launch the signed app bundle; verify the menu bar item and settings tabs.
2. Deny microphone access and verify a useful error; grant it in Settings and retry.
3. In TextEdit, hold/release the shortcut. Verify one result is pasted and the
   prior clipboard is restored. Repeat in toggle mode.
4. Switch text fields/apps during processing; verify no paste goes to the new target.
5. Cancel recording and inference; verify no later result is inserted.
6. Test Apple with installed assets and with a missing/unsupported locale. Test
   Whisper with live preview. Verify dictionary cleanup and Verbatim behavior.
7. Restart the app to check local settings/history; publish then replace an Antfly
   snapshot and search a saved transcript. Export JSON and confirm no credentials.
8. Stop the server, retry, deny Accessibility, or choose a conflicting shortcut;
   verify copyable recovery and the previous working shortcut remain available.
