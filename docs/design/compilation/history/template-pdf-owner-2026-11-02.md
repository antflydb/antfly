# Template PDF owner migration candidate

Status: implemented behind `-Dshared-pdf-templates=true`; product-size screening
pending. This is a P1b slice of [the production plan](../production-cost-plan.md),
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

The experiment remains off by default until both products have been screened.
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
