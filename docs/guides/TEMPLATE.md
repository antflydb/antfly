# Guide authoring standard

Guides take a reader who has finished the [Quickstart](quickstart.mdx) to one named
outcome. They are written for humans steering an agent-assisted workflow — the agent
gets its knowledge from the `antfly-skills` skill, so the guide's job is judgment and
context, not exhaustive reference. (This file is `.md`, not `.mdx`, so it never syncs
to the docs site.)

## Shape

Match the voice of `support-answer-agent.mdx`: confident, concrete, show-first.
Target 120–180 lines. Structure:

1. **Frontmatter** — `title` (imperative outcome: "Build a …", "Add … to …"),
   `description` (one sentence, what the reader ends up with), `order`, and
   optionally `unlisted`. Nothing else — no `prerequisites`/`difficulty`/
   `estimatedTime`/`tags`: labels about the doing rot once an agent does the
   doing. A genuine state requirement (credentials, a configured Postgres)
   goes in prose with a verification command, not in metadata.
2. **`<Questions>`** — 3–5 questions a person would actually type, right after
   frontmatter.
3. **The Pattern** — a few sentences of framing: what this solution is and the one
   insight that shapes how you build it. No scope disclaimers, no "what this doesn't
   do" — humans don't read like that.
4. **Numbered steps** — each ends in something runnable with visible output. Real
   commands against `http://localhost:8080`, real JSON, no `{{PLACEHOLDER}}` blocks.
   Use `<Tabs>` for CLI/cURL variants where both exist.
5. **One "why" section** — the judgment call the reader must own (tuning, tradeoff,
   failure mode). This is the part the skill can't do for them.
6. **Let your agent drive** — every guide carries this section near the end:

   ```md
   ## Let Your Agent Drive

   Everything above is also encoded in the [Antfly skill](https://github.com/antflydb/antfly-skills),
   so a coding agent can execute this guide for you:

   ```bash
   npx skills add antflydb/antfly-skills
   ```

   Then prompt it with the outcome — e.g. "<one concrete prompt matching this guide>" —
   and use this page to judge the result.
   ```

7. **Next Steps** — 2–4 links to related guides.

## Facts

Every endpoint, flag, provider name, enum value, and default must be verified against
the **current** Antfly source before it ships — the API surface has moved and older
examples break silently. The recurring traps:

- API bases are `/db/v1` (database), `/auth/v1` (users/keys), `/ai/v1` (inference) —
  never `/api/v1`.
- The local inference provider is `antfly` (not `termite`); the CLI namespace is
  `antfly inference` and the local command is `antfly standalone`.
- Retrieval-agent generation runs via `steps.generation` and accepts providers
  `gemini`, `vertex`, `openai`, `ollama`, `antfly` only; responses are JSON unless
  `stream: true`.
- `semantic_search` requires `indexes: [...]` — omitting it is an HTTP 422.
- `sync_level: "full_index"` for read-after-write vector queries.
- Model refs are owner-qualified (`BAAI/bge-small-en-v1.5`,
  `mixedbread-ai/mxbai-rerank-base-v1`).

When in doubt, `antfly-skills/references/` is the verified corpus (CI-checked against
`openapi.yaml`); the implementation outranks spec prose.

A guide ships only after someone — or an agent under observation — has executed it
end-to-end against a live instance.
