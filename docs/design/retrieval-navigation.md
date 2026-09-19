# Retrieval-step navigation

Status: implemented by PR #779.

## API boundary

Queries describe authorized data scope and deterministic operations. Navigation
policy belongs to `steps.retrieval.navigation`, which targets exactly one entry
in `queries` by its zero-based `query_index`. The other entries remain ordinary
queries. Models, conversation history, iteration limits, tool permissions,
context budgets, generation, and streaming belong to the enclosing retrieval
agent. Canonical `graph_queries` never invokes an LLM.

The public API has no query-level `tree_search`, `graph_navigation`, or
`tree_navigation` alias. Old query-level inputs are rejected. This is an
intentional API change, with no compatibility/deprecation period. Internally,
ranked tree execution reuses the existing traversal and result extraction.

## Complete agentic tree request

```json
{
  "query": "Compare deployment recovery options across the runbook",
  "stream": true,
  "generator": {"provider": "antfly", "model": "your-tool-capable-model"},
  "max_internal_iterations": 12,
  "max_context_tokens": 16000,
  "reserve_tokens": 4000,
  "queries": [{
    "table": "runbooks",
    "full_text_search": {"match": "deployment", "field": "title"},
    "filter_query": {"term": "published", "field": "status"},
    "fields": ["title", "body"]
  }],
  "steps": {
    "retrieval": {
      "navigation": {
        "query_index": 0,
        "strategy": "tree",
        "selection": "agentic",
        "index": "section_hierarchy",
        "start_key": "deployment-guide",
        "max_depth": 5,
        "beam_width": 3
      }
    },
    "generation": {"enabled": true}
  }
}
```

Omitting `start_key` seeds agentic exploration with the first query hit. An
explicit key bypasses seed scoring but retains filters and projection. Keys
are literal document IDs. Named full-text indexes apply only to seed searches.

For graph navigation replace the navigation object with:

```json
{
  "query_index": 0,
  "strategy": "graph",
  "selection": "agentic",
  "index": "workflow",
  "direction": "out",
  "edge_types": ["next"],
  "max_steps": 8,
  "neighbor_limit": 8,
  "instruction_field": "instructions"
}
```

Include `instructions` in `fields` when using this example's opt-in field.

## Strategies and selection

| Strategy | Selection | Execution |
| --- | --- | --- |
| `graph` | `agentic` | Model chooses one offered neighbor at each hop; a single path, with no revisits. |
| `tree` | `agentic` | Model chooses an offered node from a retained frontier; unvisited siblings remain available after descent. |
| `tree` | `ranked` | Existing deterministic tree traversal, branch selection, and tree hit extraction; no model needed for navigation. |

`graph` with `ranked` is rejected; deterministic graph traversal already belongs
in `graph_queries`. Navigation cannot target a query with `graph_queries`,
aggregations, or count-only execution. Required index, strategy, selection,
and query index are validated before authorization, reads, or generation.

Graph options are `direction`, `edge_types`, `max_steps` (1–20, default 8), and
`neighbor_limit` (1–256, default 8). Tree options are `max_depth` (1–20, default
5) and `beam_width` (1–20, default 3). Options from the other strategy are
rejected. Agentic tree edges are followed outward. Tree depth starts at zero;
no children are fetched at the maximum depth. Beam width limits newly offered
children per expansion, not the total retained frontier.

Ranked tree requests use `selection: "ranked"` and work with
`max_internal_iterations: 0`. They can use a literal `start_key` or `start_nodes`
(comma-separated keys, `$roots`, or a prior-result selector), never both.
`start_nodes` is exclusive to ranked trees. Root discovery requires the existing
additional authorization check. Without an explicit selector, ranked traversal
uses seed-query results or prior query hits, preserving deterministic traversal
semantics. Caller workflow instructions are rejected for ranked selection.

Internally, a ranked tree start is a tagged choice of seed results, a literal
key, or a selector. Branch expansion replaces that choice with the selected
literal key, retaining the index and beam width and setting the expansion depth.
This transition borrows the existing key and allocates nothing; an old root or
selector cannot remain active alongside the branch key.

## Agentic execution

`search(query_index)` starts navigation once. `navigate(query_index, next_key)`
selects only an offered, unvisited key. For graphs, each result replaces the
candidate list. For trees, expansion removes visited nodes, adds bounded
children with cumulative depth, and retains unvisited siblings. For example,
`root → A → A1`, then selecting `B`, explores the root's other branch without
revisiting the root. The recorded parent of B is root, not A1.

Selected nodes are fetched again through canonical document-ID queries. Every
seed, document, and neighbor read enforces the configured table, mandatory
inclusion/exclusion predicates, prefix, and authenticated row filters. Foreign
table nodes and dangling documents are not offered. Cycles and duplicate
frontier entries are suppressed. One navigation advance per assistant batch
prevents choices based on results the model has not seen yet.

Both strategies use the enclosing generator and its conversation, including
all previously visited evidence. Optional `instruction` and string-valued
`instruction_field` explicitly opt into workflow instructions; other document
content, including unvisited candidates, remains untrusted evidence. Instructions
cannot change authorized tables, filters, tools, or budgets.

Graph navigation requires `graph_search` permission; tree navigation requires
`tree_search` permission. Seed search/filter permissions still apply. The
retrieval-step tools policy can only narrow the top-level policy. An implicit
agentic seed with only a table scope performs a scan and requires `add_filter`;
an explicit `start_key` on a bare table needs only the navigation tool. Ranked
plans are normalized before permission checks so explicit keys, root discovery,
and prior-result starts retain their tree-tool requirements. The model
cannot use `build_query` to replace a navigation target's fixed scope.

## Budgets and results

Both agentic strategies share `max_internal_iterations`, the 20-tool-call cap,
and accumulated navigation context budget. Candidate reads use bounded lookahead
(up to 1,024 graph results per expansion) to fill slots after filtering invalid
candidates. Candidate documents are pruned to fit context; only serialized keys
remain selectable. New tree children precede retained siblings when pruning,
so pruning may discard a sibling. Truncation is reported. Context exhaustion
returns the normal incomplete result with evidence collected so far.

There is one retrieval result and one stream: visited `hits`, optional
`generation`, standard usage, and standard hit/step/generation/done events.
Agentic steps are named `graph_navigation` or `tree_navigation`. Details include
`query_index`, `current_key`, `from_key`, `moves`, and `tool_call_id`; tree steps
also include cumulative `depth`, and `from_key` identifies the branch parent.
Ranked trees retain their existing tree metadata and progress events.

Query-builder tree results provide `retrieval_query_request` (ordinary query)
and `retrieval_navigation` (ranked tree policy). Put the query in `queries` and
the policy in `steps.retrieval.navigation`, adjusting `query_index` when needed.

## Validation

Regression coverage exercises step targeting, a tree branch switch after
reaching the depth limit, ancestor cycles, bounded candidates, graph direction,
mandatory predicates, canonical key reads, instruction opt-in, cumulative
context/history, rejected policy combinations, disabled tools, streaming,
ranked traversal, and rejection of removed query-level fields. Generated Go,
Python, TypeScript, and Zig models expose the same request boundary.
