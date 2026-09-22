import { L, manifest, snippetsFor } from "@/lib/data";
import { RuntimeClient } from "./runtime-client";

const LINK_IDS = [
  "server-embeddings",
  "server-rerank",
  "server-generate",
  "server-chat",
  "session-factory",
  "runtime-execution-control",
  "tokenizer-main",
  "tokenizer-sentencepiece",
  "tokenizer-hf",
  "node-primitive-op",
  "node-fused-op",
  "planner-frame-descriptor",
  "runtime-submit-frame",
  "ops-compute-backend",
  "multi-executor",
  "compiler-schedules",
  "kv-manager",
  "generation-kv-config",
  "generation-kv-policy",
  "kv-pool-config",
  "kv-prompt-cache",
  "generation-config",
  "native-generate-scheduler",
  "kernel-gumbel",
];

export const metadata = { title: "Runtime" };

export default function RuntimePage() {
  return (
    <RuntimeClient
      snippets={snippetsFor(LINK_IDS.map(L))}
      gitCommit={manifest.gitCommit}
      permalinkBase={manifest.permalinkBase}
    />
  );
}
