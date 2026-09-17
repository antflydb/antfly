import type {
  QueryRequest,
  RetrievalAgentRequest,
  RetrievalNavigationConfig,
} from "../src/index.js";

const navigation: RetrievalNavigationConfig = {
  query_index: 0,
  strategy: "graph",
  selection: "agentic",
  index: "workflow",
  start_key: "start",
  direction: "in",
  edge_types: ["next"],
  instruction_field: "instructions",
  max_steps: 4,
  neighbor_limit: 8,
};

const request: RetrievalAgentRequest = {
  query: "Find the resolution",
  max_internal_iterations: 8,
  generator: { provider: "antfly", model: "test" },
  queries: [{ table: "runbooks" }],
  steps: { retrieval: { navigation } },
};
void request;

const query: QueryRequest = {
  // @ts-expect-error Navigation is an agent strategy, not a canonical query field.
  graph_navigation: navigation,
};
void query;

const tree: RetrievalNavigationConfig = {
  query_index: 0,
  strategy: "tree",
  selection: "agentic",
  index: "sections",
  max_depth: 5,
  beam_width: 3,
};
void tree;
const retrievalQuery: RetrievalAgentRequest["queries"][number] = {
  // @ts-expect-error Agent navigation is configured on the retrieval step.
  graph_navigation: navigation,
};
void retrievalQuery;

const removedTree: RetrievalAgentRequest["queries"][number] = {
  // @ts-expect-error Tree exploration is configured on the retrieval step.
  tree_search: { index: "sections" },
};
void removedTree;
