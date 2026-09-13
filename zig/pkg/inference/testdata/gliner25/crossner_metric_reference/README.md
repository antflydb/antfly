# CrossNER metric reference

`conll2002_metrics.py` is an unchanged copy of the official CrossNER scorer at
commit `2e7ba2a7798c961e3f29fbc51252c5a8d40224bf`. `source.json` records its
origin, exact bytes and MIT license; the upstream copyright notice is retained
in `LICENSE`. This reference is used only by offline tests and evidence audits.

The audit compiles only the exact pinned source bytes it has just read. It
encodes each entity type on an independent BIO track using the union of gold
and predicted UTF-8 span boundaries. This preserves cross-type overlap and
partial-word errors without changing their counts. Explicit boundaries isolate
documents and types. The proof concerns exact typed-span metrics; it does not
reproduce the original trainer's tokenization, batching or argument order.

The native model runtime does not load this Python reference.
