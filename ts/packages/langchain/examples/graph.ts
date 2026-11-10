import { AntflyGraphStore, createAntflyGraphTools } from "@antfly/langchain";
import { AntflyClient } from "@antfly/sdk";
import { Document } from "@langchain/core/documents";

const client = new AntflyClient({ baseUrl: process.env.ANTFLY_URL ?? "http://localhost:8080" });
const table = process.env.ANTFLY_TABLE ?? "langchain_graph";
const command = process.argv[2] ?? "query";
const options = { client, table, namespace: "orchard" };
const graph =
  command === "create" ? await AntflyGraphStore.create(options) : new AntflyGraphStore(options);
if (command === "create" || command === "ingest") {
  const mira = { id: "mira", type: "Person", properties: { name: "Mira Chen" } },
    operations = {
      id: "operations",
      type: "Team",
      properties: { text: "Owns shipment escalation" },
    };
  await graph.addGraphDocuments(
    [
      {
        nodes: [mira, operations],
        relationships: [{ source: mira, target: operations, type: "OWNS" }],
        source: new Document({ id: "handbook", pageContent: "Mira Chen owns operations." }),
      },
    ],
    { includeSource: true }
  );
} else if (command === "delete") await graph.deleteSource("handbook");
else if (command === "schema") console.log(await createAntflyGraphTools(graph)[0].invoke({}));
else
  console.log(
    JSON.stringify(
      await graph.expandDocuments([
        new Document({ id: graph.nodeKey("Person", "mira"), pageContent: "Mira Chen" }),
      ]),
      null,
      2
    )
  );
