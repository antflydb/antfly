import { AntflyClient } from "@antfly/sdk";
import {
  Annotation,
  END,
  type LangGraphRunnableConfig,
  START,
  StateGraph,
} from "@langchain/langgraph";
import { AntflyStore } from "../src/index.js";

const client = new AntflyClient({ baseUrl: process.env.ANTFLY_URL ?? "http://localhost:8080" });
const table = process.env.ANTFLY_TABLE ?? "langgraph_memory";
const store = process.argv.includes("--create")
  ? await AntflyStore.create({ client, table })
  : new AntflyStore({ client, table });
const State = Annotation.Root({ remember: Annotation<string>, recall: Annotation<string> });
const graph = new StateGraph(State)
  .addNode("memory", async (state, config: LangGraphRunnableConfig) => {
    const userId = config.configurable?.userId;
    if (!userId) throw new Error("Supply trusted application user context");
    const namespace = ["users", userId];
    if (state.remember) await config.store!.put(namespace, "preference", { text: state.remember });
    return {
      recall: String((await config.store!.get(namespace, "preference"))?.value.text ?? "No memory"),
    };
  })
  .addEdge(START, "memory")
  .addEdge("memory", END)
  .compile({ store });
console.log(
  await graph.invoke(
    { remember: process.env.REMEMBER ?? "" },
    { configurable: { userId: process.env.USER_ID ?? "alice" } }
  )
);
