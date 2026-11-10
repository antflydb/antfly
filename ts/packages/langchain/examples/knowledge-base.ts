import { AntflyReranker, AntflyRetriever, AntflyVectorStore } from "@antfly/langchain";
import { ChatOpenAI } from "@langchain/openai";
import { answerChain } from "./answer-chain.js";
import { environment, fixture } from "./common.js";

const { client, inference, embeddings } = environment();
const table = process.env.ANTFLY_TABLE ?? "antfly_langchain_knowledge";
const [command = "ask", ...args] = process.argv.slice(2);
const store = new AntflyVectorStore(embeddings, { client, table, distanceMetric: "cosine" });
switch (command) {
  case "ingest":
    await AntflyVectorStore.fromDocuments(fixture, embeddings, {
      client,
      table,
      distanceMetric: "cosine",
    });
    console.log("Persisted orchard handbook with stable document and source IDs.");
    break;
  case "delete":
    await store.delete({ ids: args.length ? args : fixture.map((doc) => String(doc.id)) });
    console.log("Deleted requested records and awaited index visibility.");
    break;
  case "verify": {
    const documents = await store.getByIds(fixture.map((doc) => String(doc.id)));
    if (documents.length !== fixture.length) throw new Error("Missing persisted fixture documents");
    const found = await store.asRetriever(1).invoke("Who owns operations?");
    if (found[0]?.id !== "owner") throw new Error("Expected persisted owner evidence");
    console.log("Verified persisted documents and retrieval without re-ingestion.");
    break;
  }
  case "ask": {
    const question = args.filter((arg) => arg !== "--generate").join(" ") || "Who owns operations?";
    const reranker = process.env.ANTFLY_RERANKER_MODEL
      ? new AntflyReranker({ client: inference, model: process.env.ANTFLY_RERANKER_MODEL })
      : undefined;
    const retriever = new AntflyRetriever({ vectorStore: store, mode: "hybrid", k: 3, reranker });
    if (args.includes("--generate")) {
      if (!process.env.OPENAI_API_KEY || !process.env.OPENAI_MODEL)
        throw new Error(
          "Generation requires explicit OPENAI_API_KEY and OPENAI_MODEL; it may incur provider charges"
        );
      const model = new ChatOpenAI({
        model: process.env.OPENAI_MODEL,
        temperature: 0,
        maxRetries: 0,
      });
      console.log(JSON.stringify(await answerChain(retriever, model).invoke(question), null, 2));
    } else {
      const documents = await retriever.invoke(question);
      console.log(
        JSON.stringify(
          {
            mode: "evidence-only",
            sources: documents.map((doc) => ({
              id: doc.id,
              content: doc.pageContent,
              metadata: doc.metadata,
            })),
          },
          null,
          2
        )
      );
    }
    break;
  }
  default:
    throw new Error("Use ingest, ask [question] [--generate], verify, or delete [IDs]");
}
