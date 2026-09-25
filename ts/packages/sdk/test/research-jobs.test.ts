/**
 * Unit tests for durable research job helpers on the Antfly SDK client.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ResearchAgentRequest, ResearchJob } from "../src/types.js";

// Mock openapi-fetch at the top level, matching client.test.ts conventions.
const mockGet = vi.fn();
const mockPost = vi.fn();

vi.mock("openapi-fetch", () => ({
  default: vi.fn(() => ({
    GET: mockGet,
    POST: mockPost,
    PUT: vi.fn(),
    DELETE: vi.fn(),
    OPTIONS: vi.fn(),
    HEAD: vi.fn(),
    PATCH: vi.fn(),
    TRACE: vi.fn(),
    request: vi.fn(),
    use: vi.fn(),
    eject: vi.fn(),
  })),
}));

const { AntflyClient, ResearchJobAdvanceConflictError } = await import("../src/client.js");

const request: ResearchAgentRequest = {
  query: "How do hybrid search and reranking interact?",
  queries: [{ table: "docs" }],
};

function job(overrides: Partial<ResearchJob> = {}): ResearchJob {
  return {
    job_id: "job1",
    state: "running",
    phase: "plan",
    advances: 0,
    ...overrides,
  };
}

describe("runResearchJob", () => {
  let client: InstanceType<typeof AntflyClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    client = new AntflyClient({ baseUrl: "http://localhost:8080" });
  });

  it("starts a job and advances until it reaches a terminal state", async () => {
    mockPost
      .mockResolvedValueOnce({ data: job({ state: "queued", phase: "plan" }), error: undefined })
      .mockResolvedValueOnce({
        data: job({ state: "running", phase: "research", advances: 1 }),
        error: undefined,
      })
      .mockResolvedValueOnce({
        data: job({ state: "succeeded", phase: "done", advances: 2 }),
        error: undefined,
      });

    const onJob = vi.fn();
    const result = await client.runResearchJob(request, { onJob });

    expect(result.state).toBe("succeeded");
    expect(mockPost).toHaveBeenCalledTimes(3);
    expect(mockPost.mock.calls[0][0]).toBe("/db/v1/agents/research/jobs");
    expect(mockPost.mock.calls[0][1]).toMatchObject({ body: { request } });
    expect(mockPost.mock.calls[1][0]).toBe("/db/v1/agents/research/jobs/{jobId}/advance");
    expect(mockPost.mock.calls[1][1]).toMatchObject({ params: { path: { jobId: "job1" } } });
    // onJob is called after start and after each advance/poll.
    expect(onJob).toHaveBeenCalledTimes(3);
    expect(onJob).toHaveBeenNthCalledWith(3, expect.objectContaining({ state: "succeeded" }));
  });

  it("passes max_phases through to each advance call", async () => {
    mockPost
      .mockResolvedValueOnce({ data: job({ state: "queued" }), error: undefined })
      .mockResolvedValueOnce({
        data: job({ state: "succeeded", phase: "done" }),
        error: undefined,
      });

    await client.runResearchJob(request, { maxPhasesPerAdvance: 3 });

    expect(mockPost.mock.calls[1][1]).toMatchObject({ body: { max_phases: 3 } });
  });

  it("treats a 409 as a concurrent advance and polls instead of racing it", async () => {
    vi.useFakeTimers();
    try {
      mockPost
        .mockResolvedValueOnce({ data: job({ state: "queued" }), error: undefined })
        .mockResolvedValueOnce({
          data: undefined,
          error: { error: "conflict", message: "another advance is in flight" },
          response: { status: 409, headers: new Headers({ "Retry-After": "2" }) },
        });
      mockGet.mockResolvedValueOnce({
        data: job({ state: "succeeded", phase: "done" }),
        error: undefined,
      });

      const resultPromise = client.runResearchJob(request);
      // Let the start + failed advance settle before advancing fake timers.
      await vi.waitFor(() => expect(mockPost).toHaveBeenCalledTimes(2));
      await vi.advanceTimersByTimeAsync(2000);

      const result = await resultPromise;
      expect(result.state).toBe("succeeded");
      expect(mockGet).toHaveBeenCalledExactlyOnceWith(
        "/db/v1/agents/research/jobs/{jobId}",
        expect.objectContaining({ params: { path: { jobId: "job1" } } })
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("advanceResearchJob throws ResearchJobAdvanceConflictError with retry metadata on 409", async () => {
    mockPost.mockResolvedValueOnce({
      data: undefined,
      error: { error: "conflict", message: "another advance is in flight" },
      response: { status: 409, headers: new Headers({ "Retry-After": "5" }) },
    });

    try {
      await client.advanceResearchJob("job1");
      expect.fail("expected a conflict error");
    } catch (error) {
      expect(error).toBeInstanceOf(ResearchJobAdvanceConflictError);
      expect(error).toMatchObject({ status: 409, retryable: true, retryAfterSeconds: 5 });
    }
  });

  it("propagates non-conflict advance failures without retrying", async () => {
    mockPost
      .mockResolvedValueOnce({ data: job({ state: "queued" }), error: undefined })
      .mockResolvedValueOnce({
        data: undefined,
        error: { error: "internal", message: "boom" },
        response: { status: 500, headers: new Headers() },
      });

    await expect(client.runResearchJob(request)).rejects.toThrow(
      "Failed to advance research job: internal"
    );
    expect(mockGet).not.toHaveBeenCalled();
  });
});
