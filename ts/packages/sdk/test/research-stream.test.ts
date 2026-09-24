import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

const request = {
  query: "How do hybrid search and reranking interact?",
  queries: [{ table: "docs" }],
};
const encoder = new TextEncoder();

function sseEvent(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function streamResponse(body: string) {
  const bytes = encoder.encode(body);
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
  return new Response(stream, { headers: { "Content-Type": "text/event-stream" } });
}

afterEach(() => vi.restoreAllMocks());

describe("research agent SSE parsing", () => {
  it("dispatches every research phase to its typed callback and resolves done", async () => {
    const plan = {
      brief: "Explain the trade-offs",
      sub_questions: [{ id: "q1", question: "What is BM25?" }],
    };
    const subQuestionStarted = { sub_question_id: "q1", question: "What is BM25?", round: 1 };
    const finding = { sub_question_id: "q1", summary: "BM25 scores lexical overlap." };
    const reflection = { round: 1, done: true, gaps: [] };
    const section = { index: 0, heading: "Overview" };
    const verification = { checked_sections: 1, supported_ratio: 1 };
    const result = {
      status: "completed",
      research_state: { phase: "done" },
      report: { markdown: "# Overview\n\nHybrid search combines BM25 and vectors. [E1]" },
    };

    const body =
      sseEvent("step_started", { name: "plan" }) +
      sseEvent("step_progress", { name: "research", phase: "plan", ...plan }) +
      sseEvent("step_progress", {
        name: "research",
        phase: "sub_question_started",
        ...subQuestionStarted,
      }) +
      sseEvent("step_progress", { name: "research", phase: "finding", ...finding }) +
      sseEvent("step_progress", { name: "research", phase: "reflection", ...reflection }) +
      sseEvent("generation", "# Overview\n\n") +
      sseEvent("step_progress", { name: "research", phase: "section", ...section }) +
      sseEvent("step_progress", { name: "research", phase: "verification", ...verification }) +
      sseEvent("step_completed", { kind: "generation", name: "write", status: "success" }) +
      sseEvent("done", result);

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(streamResponse(body));

    const callbacks = {
      onStepStarted: vi.fn(),
      onPlan: vi.fn(),
      onSubQuestionStarted: vi.fn(),
      onFinding: vi.fn(),
      onReflection: vi.fn(),
      onGeneration: vi.fn(),
      onSection: vi.fn(),
      onVerification: vi.fn(),
      onStepCompleted: vi.fn(),
      onDone: vi.fn(),
      onError: vi.fn(),
    };

    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamResearchAgent(
      request,
      callbacks
    );

    await vi.waitFor(() => expect(callbacks.onDone).toHaveBeenCalledOnce());
    expect(callbacks.onStepStarted).toHaveBeenCalledWith({ name: "plan" });
    expect(callbacks.onPlan).toHaveBeenCalledWith(
      expect.objectContaining({ brief: plan.brief, sub_questions: plan.sub_questions })
    );
    expect(callbacks.onSubQuestionStarted).toHaveBeenCalledWith(
      expect.objectContaining(subQuestionStarted)
    );
    expect(callbacks.onFinding).toHaveBeenCalledWith(expect.objectContaining(finding));
    expect(callbacks.onReflection).toHaveBeenCalledWith(expect.objectContaining(reflection));
    expect(callbacks.onGeneration).toHaveBeenCalledWith("# Overview\n\n");
    expect(callbacks.onSection).toHaveBeenCalledWith(expect.objectContaining(section));
    expect(callbacks.onVerification).toHaveBeenCalledWith(expect.objectContaining(verification));
    expect(callbacks.onStepCompleted).toHaveBeenCalledWith(
      expect.objectContaining({ name: "write" })
    );
    expect(callbacks.onDone).toHaveBeenCalledWith(result);
    expect(callbacks.onError).not.toHaveBeenCalled();
  });

  it("reports a terminal stream error through onError without calling onDone", async () => {
    const body = `${sseEvent("step_progress", {
      name: "research",
      phase: "plan",
      brief: "x",
      sub_questions: [],
    })}event: done\ndata: not-json\n\n`;
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(streamResponse(body));

    const onError = vi.fn();
    const onDone = vi.fn();
    await new AntflyClient({ baseUrl: "http://localhost:8080" }).streamResearchAgent(request, {
      onPlan: vi.fn(),
      onError,
      onDone,
    });

    await vi.waitFor(() => expect(onError).toHaveBeenCalledOnce());
    expect(onDone).not.toHaveBeenCalled();
  });

  it("sends the research request to the research endpoint for a JSON response", async () => {
    const result = {
      status: "completed",
      research_state: { phase: "done" },
      report: { markdown: "done" },
    };
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response(JSON.stringify(result), { headers: { "Content-Type": "application/json" } })
      );

    const client = new AntflyClient({ baseUrl: "http://localhost:8080/db/v1" });
    const response = await client.researchAgent(request);

    expect(response).toEqual(result);
    const [url, options] = fetchSpy.mock.calls[0];
    expect(String(url)).toContain("/agents/research");
    expect(JSON.parse(String(options?.body)).stream).toBe(false);
  });
});
