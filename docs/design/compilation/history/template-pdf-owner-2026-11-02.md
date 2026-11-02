# Template PDF owner migration

Status: the native regression passed; the user accepted the measured library
size tradeoff on 2026-11-02. Template routing now defaults to true;
`-Dshared-pdf-templates=false` retains an explicit comparison path. This is a P1b slice of [the production plan](../production-cost-plan.md),
not completion of all PDF/image migration.

The default production template backend delegates complete text extraction and
first-page PNG rendering to the existing media owner when the experiment is
selected. That owner calls the original `pdf.Backend.native()` methods, keeping
plain-text ordering, skipped per-page parse errors, 72-DPI rendering, exact
sampling, platform fallback and decode limits. Explicit backend overrides remain
available. Normal source tests and source-only builds keep their source backend.

Template error handling consumes the provider's failure envelope directly:
nonfatal error directive names remain exact even for errors absent from the
shared status registry. Cancellation, timeout and OOM still latch fatal render
outcomes. No per-pixel or per-record foreign callbacks were introduced.

The new synchronous backend allocator forwards allocation, release, resize and
remap. The older allocate/free-only adapter would have forced buffer copies
where the caller can resize. Its callback table is borrowed only during the
operation; returned buffers are caller-owned and contain no adapter reference.
A tracked resizing allocator test proves scratch is released, exactly the
14-byte text output remains charged, resizing callbacks run successfully, and
freeing the returned output brings the tracked live bytes back to zero.

`RenderConfig.pdf_backend` is now an optional explicit override, rather than a
source backend factory stored by every default config. This removes the native
production default's reference to the source parser/renderer functions. The
private storage descriptor version is 30 because native callbacks can carry
that config value. Public CAPI signatures and durable formats are unchanged.

Focused verification so far:

- Seven opaque PDF/session/backend tests and two compiled-template tests passed
  in Fast and Debug without skips/leaks.
- All 80 existing template and remote-helper tests passed with no skips/leaks
  using `-Dtemplate-test-filter=template_remote.test.` and
  `-Dtemplate-test-filter=template.test.`; their ancillary root test also passed.
- The compiled tests cover exact fixture text, PNG output, unknown provider
  diagnostic names, explicit backend overrides, fatal OOM propagation and
  allocator admission/resizing/cleanup.
- Formatting, licenses and the control source-catalog checker passed.

The user accepted the 347,008-byte library increase for the 6,719,536-byte
server reduction. Template routing is now enabled by default.
Prior native CPU artifacts at `f5c0c79fe8` were preserved and SHA256-verified under
`.benchmark-results/compile-costs/baseline-f5c0c79fe8/` before the candidate run.
The full first-phase baseline is 229,621,752 bytes for `antfly` and 80,026,400
bytes for `libantfly.dylib`; compare the earlier pre-query-owner server baseline
of 225,639,896 bytes too.

Receipts are ignored under `.benchmark-results/compile-costs/`:
`template-pdf-owner-final.log`, `template-pdf-resizing-tests.log`,
`template-pdf-final-debug.log`, and `template-pdf-legacy-tests.log`.
An incorrectly rooted initial invocation had fixture lookup failures; its log
is retained as `template-pdf-owner-test.log`, separately from the successful
`template-pdf-owner-test-corrected.log`. Builds/tests must run from `zig/`.

## Intermediate API-owner evidence

The candidate API owner finished during the native product run. Its archive is
69,847,696 bytes versus 71,874,888 for the preserved baseline. Estimated text
symbol extents fell from 48,001,140 to 46,518,180 bytes; the codec category fell
from 448,752 to 18,216 bytes. These archive figures are not final linked savings.

The API archive no longer emits `jpeg2000.codestream.parseState` or
`reader.Reader.decodeImageToRgbaGuardedAlloc`. Optional profiling-CI guards
reject either symbol under `shared_pdf_templates=true` in Fast mode. Both guards
passed the candidate and rejected the baseline, verifying that they detect the
old implementation roots. The remaining consumers still need migration.

The manual profiling workflow exposes `shared_pdf_templates` and
`compiled-template-pdf-test`. It records the flag and forwards it consistently
to the primary product build, archive attribution and separate forced compiler
timing run; artifacts distinguish the selected variant.

Receipts: `shared-template-api-comparison.json`, `shared-template-api-guards.json`
and `shared-template-api-guards-negative.json` under the ignored measurement
folder. The product run's production sources remain frozen at `e6809c355d`.


## Full native product result

The production sources at `e6809c355d` completed all 133 steps: server and CAPI,
runtime archives, 57 storage-owner tests, three opaque-query tests, CAPI smoke
and consumer linkage, the 134-function public export boundary, CLI smoke and
linked-inference ABI integration. All passed. Ten HTTP query/deadline tests
subsequently passed against the SHA-verified installed candidate in 21.94 seconds.

| Installed stripped artifact | Baseline bytes | Candidate bytes | Change |
| --- | ---: | ---: | ---: |
| `antfly` | 229,621,752 | 222,902,216 | −6,719,536 (−2.93%) |
| `libantfly.dylib` | 80,026,400 | 80,373,408 | +347,008 (+0.43%) |

The candidate server is also 2,737,680 bytes below the older same-profile
pre-query-owner baseline. These artifacts were measured with the explicit flag before its accepted
default changed. The retained-reader migration is a separate follow-up.

Candidate SHA256:

- Server: `c0dae706fdad3588f54e43cdd82db0e61de22fa056c0e7656a0a39b9f6815b33`.
- Library: `556609305ee3f480aa5b23742fa0e1b474caa452a7512d849dd30df3357b441d`.

The library's raw final-link map was copied only after its cached dylib matched
the installed SHA256. Exact live atom attribution explains the growth:

| CAPI text owner | Baseline bytes | Candidate bytes | Change |
| --- | ---: | ---: | ---: |
| Storage | 41,667,916 | 41,627,996 | −39,920 |
| Inference | 23,297,164 | 23,297,164 | 0 |
| Shared media | 1,568,996 | 1,916,092 | +347,096 |
| Entire text section | 66,600,616 | 66,907,792 | +307,176 |

Constants grow by 15,636 bytes and exception frames by 11,312 bytes; file layout
and other sections account for the remaining file delta. Complete exact-profile
rendering now remains live in the media owner, including render runs, vector
text/font outlines, patterns and shading. Other storage PDF consumers still keep
their renderer alive, so migrating template defaults alone adds retained code
to the library. JPEG/JPEG2000/PDF image-decoder counts remain three each, with
unchanged exact atom totals; nominal repeated-name extra text grows from
3,848,196 to 4,103,924 bytes. This is a partial migration, not deduplication's exit.

The profiled command took 2,816 seconds, with a sampled individual and same-poll
aggregate compiler peak of 9.013 GiB in the API owner. Zig reported 14 minutes
for the API compiler and 11 minutes for the CAPI storage engine. The earlier run
took 2,856 seconds with different cached inputs and shared-host load; neither is
a controlled cold-build comparison. No build-time or compiler-memory gain is
established, and no scheduler reservation is lowered.

Next: migrate the retained PDF analysis, batch/raster and fork-cache consumers
to remove the remaining implementation copies. The library regression is a concrete
reason to finish P1b, rather than infer success from the server-only size win.

Additional receipts: `shared-template-pdf-products.{build.log,memory.json,rss.tsv}`,
`shared-template-products.json`, `shared-template-libantfly.{map,json}`,
`shared-template-api-artifacts.json`, and `shared-template-http-query.log` in the
ignored measurement folder. The matching raw map is retained before cache GC.
