import { Document } from "@langchain/core/documents";
import { BaseRetriever } from "@langchain/core/retrievers";
import { FakeListChatModel } from "@langchain/core/utils/testing";
import { expect, it } from "vitest";
import { answerChain } from "../examples/answer-chain.js";

class Evidence extends BaseRetriever {
  lc_namespace = ["test"];
  async _getRelevantDocuments() {
    return [new Document({ id: "owner", pageContent: "Mira owns operations." })];
  }
}
it("composes retrieval, a standard chat model, and checked citations", async () => {
  const result = await answerChain(
    new Evidence(),
    new FakeListChatModel({ responses: ["Mira owns operations. [owner]"] })
  ).invoke("Who owns operations?");
  expect(result.citations).toEqual(["owner"]);
  expect(result.sources[0].id).toBe("owner");
});
it("rejects invented citation IDs", async () => {
  await expect(
    answerChain(new Evidence(), new FakeListChatModel({ responses: ["Mira. [invented]"] })).invoke(
      "Who?"
    )
  ).rejects.toThrow("outside");
});
it("rejects an uncited answer", async () => {
  await expect(
    answerChain(
      new Evidence(),
      new FakeListChatModel({ responses: ["Mira owns operations."] })
    ).invoke("Who?")
  ).rejects.toThrow("omitted");
});
it("allows an explicit abstention", async () => {
  const result = await answerChain(
    new Evidence(),
    new FakeListChatModel({ responses: ["Insufficient evidence."] })
  ).invoke("What color is the roof?");
  expect(result.answer).toBe("Insufficient evidence.");
});
