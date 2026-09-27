# Documentation

Start with the [introduction](introduction.mdx), [quickstart](guides/quickstart.mdx),
and [architecture](architecture.mdx). Downloads are [here](downloads.mdx);
shared terminology is in the [glossary](glossary.mdx).

## Sections

- [Guides](guides/) — tutorials, examples, integrations, and development setup.
- [Reference](reference/) — authentication, secrets, SDKs, formats, CLI packaging, and [licensing maintenance](reference/licensing.md).
- [Operations](operations/) — object-storage operations and runtime verification history.
- [Design](design/) — current architecture and technical decisions; the [Zig roadmap](../zig/ROADMAP.md#design-documents) indexes subsystem documents kept beside their code.
- [Plans](plans/README.md) — proposed and active work.
- [UI](ui/react-antfly.mdx) — React integration.
- [Implementation history](design/history.md) — dated evidence grouped by topic under design, reference, and operations.

## Placement rules

Keep documentation about current behavior in guides, reference, operations, or
design, according to its purpose. Keep subsystem documentation beside its code
when that makes its ownership clearer; link it from the documentation index.

Put proposed and active cross-cutting work in `plans/`. When work completes,
merge its lasting decisions and instructions into the appropriate section and
remove the plan. Preserve useful investigations, dated benchmarks, review
findings, and verification evidence in a topic-specific `history/` directory.
Give historical records a clear scope or date and a link to the living document.
Do not present old checklists or test counts as current product guarantees.

The authoritative product license scope remains in [LICENSING.md](../LICENSING.md).
