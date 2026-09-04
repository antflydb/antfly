# Docs authoring standard

Every page under `docs/guides/` is one of two kinds. A **guide** takes a reader who has
finished the [Quickstart](quickstart.mdx) to one named outcome. A **concept** page explains
how one part of Antfly works so the reader can make good decisions with it. Both are
written for people steering an agent-assisted workflow: the agent gets its knowledge from
the `antfly-skills` skill, so the page's job is judgment and context, not exhaustive
reference. (This file is `.md`, not `.mdx`, so it never syncs to the docs site.)

The bar for every sentence: it earns its place or it goes. Nothing stays because it was
there before.

## Voice

Docs are a demonstrating surface. They show the result; they never make the company's
argument. The test for any sentence: if it would survive as a slide in a pitch, it does
not belong here. Mechanism is fine ("closest can still be far away, and an answer
generated from far-away context is confidently wrong"). Slogans are not ("an agent
believes what it retrieves").

- Second person, present tense, imperative for instructions. The reader is the one
  building; Antfly is the tool in their hands.
- Short sentences, one idea each. Contractions are natural.
- No em-dashes, and no double-hyphen stand-ins. Recast with a period, colon, comma, or
  parentheses.
- No hype, no pain exaggeration, no rhetorical-question hooks, no "simply" or "just".
- No copywriting rhythm. The tells: a clipped fragment landing a paragraph ("Nothing
  leaves it."), an aphorism standing in for an explanation ("Write to Postgres; query
  Antfly."), the "not X. Y." reversal, a repeated device across pages ("Three steps get
  you there."), and a cute closer on a technical point. If a sentence would read as a
  pull quote, rewrite it as a plain statement or delete it.
- "Antfly", never "AntflyDB". "Antfly Inference" for the built-in model runtime.
  "standalone mode" for `antfly standalone`.
- Roadmap never appears as shipped. No "coming soon", no "future enhancements".
- Explain the why only for non-obvious steps. Readers are here for the commands.
- Callouts carry one fact each and only when the fact would otherwise be missed. An
  `info` callout that restates the paragraph above it is deleted.

## Frontmatter

`title`, `description`, `order`, and optionally `unlisted` and `sidebar`. Nothing else.
No `difficulty`, `estimatedTime`, `prerequisites`, or `tags`: labels about the doing rot
once an agent does the doing. A real state requirement (credentials, a running Postgres)
goes in the opener as prose with a verification command.

- Guide titles are an outcome with a verb: "Build a Support Answer Agent", "Tune Hybrid
  Search". The `description` is what the reader ends with, one sentence.
- Concept pages set `sidebar: concepts` and take a noun title: "Document Engine",
  "Antfly Lite". The `description` says what the page explains.

## Guide anatomy

Target 120 to 200 lines. Every guide uses the same six h2 headings, in this order, so
the outline on the right of the page reads the same on every guide. Only the h3 steps
under Build It vary.

1. **`<Questions>`** right after frontmatter: three to five questions a person would
   actually type into a search box.

2. **`## The Result`.** One or two sentences on what the reader has when they finish,
   in their own world, then the finished artifact: the final request and a trimmed real
   response, or the finished component. This is what lets the reader decide in ten
   seconds whether the page is for them. No framing, no category, no what-this-guide-
   covers sentence; the title already said that.

3. **`## Before You Start`.** What must already be running, as a sentence fragment
   listing the state ("Antfly running in standalone mode and Ollama with a small model
   pulled:") followed by a command that proves it. A real state requirement lives here,
   never in frontmatter.

4. **`## Build It`**, with numbered h3 steps (`### 1. Create the Help Table`). Each
   step ends in something runnable with visible output. Real commands against
   `http://localhost:8080`, real JSON, no `{{PLACEHOLDER}}` blocks. Use `<Tabs>` for CLI
   and cURL where both exist. Step headings are imperative. Reference material a step
   needs (per-provider setup, for instance) goes in an unnumbered h3 after the steps.

5. **`## Tradeoffs`.** The tuning decision, tradeoff, or failure mode the reader has to
   own because the skill cannot own it for them. Open with one plain sentence that
   names the specific decision (the heading is fixed, so the first sentence carries the
   content), then the mechanism. One section, even when it holds two decisions.

6. **`## Let Your Agent Drive`**, verbatim shape:

   ```md
   ## Let Your Agent Drive

   Everything above is also encoded in the [Antfly skill](https://github.com/antflydb/antfly-skills),
   so a coding agent can execute this guide for you:

   ```bash
   npx skills add antflydb/antfly-skills
   ```

   Then prompt it with the outcome, for example "<one concrete prompt matching this
   guide>", and use this page to judge the result.
   ```

7. **`## Next Steps`**: two or three links, each with a clause saying why you would go
   there.

Not in a guide: an "Overview" section, a "What this doesn't do" section, a
"Troubleshooting" section (fold the two errors people actually hit into the step where
they happen), an "Additional Resources" list, a feature table, any other h2.

## Concept anatomy

Target 150 to 300 lines. Concept pages answer "how does this work and what should I
decide?" They are read when a guide's Tradeoffs section was not enough. The mechanism
sections vary by subject; the bookends do not.

1. **`<Questions>`** as above.
2. **The opener**, no heading: what the thing is in one or two sentences, and the one
   decision the reader most often needs it for. Then the smallest runnable example that
   proves the thing exists, where one fits in ten lines.
3. **Sections by mechanism**, each answering one question the reader has. Headings are
   nouns or short claims, not "How It Works" or "Overview". Tables for state machines
   and option matrices; prose for reasoning.
4. **`## Decisions`**: when to choose which option, stated as rules with the reason
   attached. Usually last before the agent section; a page whose main job is a
   choose-this table (model compatibility) puts it first.
5. **`## Let Your Agent Drive`** and **`## Next Steps`** as in guides, where a concrete
   prompt makes sense. A concept page whose subject an agent would never be prompted to
   "do" (model compatibility, for instance) skips the agent section.

## Facts

Every endpoint, flag, provider name, enum value, and default is verified against the
**current** Antfly source before it ships. The API surface has moved and older examples
break silently. The recurring traps:

- API bases are `/db/v1` (database), `/auth/v1` (users and keys), `/ai/v1` (inference).
  Never `/api/v1` or a bare `/v1`.
- The local inference provider is `antfly` (not `termite`); the CLI namespace is
  `antfly inference` and the local server command is `antfly standalone`.
- Fusion is configured with `merge_config: {strategy, weights, window_size,
  rank_constant}`. `strategy` is `rrf` (default) or `rsf`; `failover` parses but is
  rejected with a 422. There is no `merge_strategy` field.
- A `reranker` needs `provider`, `model`, and `field` (or `template`). Only
  `provider: antfly` executes.
- Retrieval-agent generation runs via `steps.generation` and accepts providers
  `gemini`, `vertex`, `openai`, `ollama`, `antfly` only; responses are JSON unless
  `stream: true`.
- `semantic_search` requires `indexes: [...]`; omitting it is an HTTP 422.
- `sync_level: "full_index"` for read-after-write vector queries.
- Model refs are owner-qualified (`BAAI/bge-small-en-v1.5`,
  `mixedbread-ai/mxbai-rerank-base-v1`).

When in doubt, `antfly-skills/references/` is the verified corpus (CI-checked against
`openapi.yaml`); the implementation outranks spec prose.

A guide ships only after someone, or an agent under observation, has executed it
end-to-end against a live instance.
