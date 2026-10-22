import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import EmbeddingPlaygroundPage from "./EmbeddingPlaygroundPage";
import KnowledgeGraphPlaygroundPage from "./KnowledgeGraphPlaygroundPage";

const state = vi.hoisted(() => ({
  apiUrl: "http://server-a/db/v1",
  connectionId: "remote",
  models: [{ name: "shared-model", inputs: ["text"], capabilities: ["relations"] }],
}));
vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => {
    const apiUrl = state.apiUrl;
    return {
      apiUrl,
      inferenceApiUrl: "",
      inferenceConnectionId: state.connectionId,
      setInferenceConnectionId: vi.fn(),
      inferenceUrl: (operation: string) =>
        state.connectionId === "local-inference"
          ? `/ai/v1/${operation}`
          : `${apiUrl}/connections/remote/inference/${operation}`,
    };
  },
}));
vi.mock("@/hooks/use-connections", () => ({
  useSelectedInferenceModels: () => ({ models: state.models, loading: false }),
}));
vi.mock("@/components/playground/BackendInfoBar", () => ({ BackendInfoBar: () => null }));
vi.mock("@/components/playground/NoModelsGuide", () => ({ NoModelsGuide: () => null }));
vi.mock("@/components/branded-empty-state", () => ({ PlaygroundEmptyState: () => null }));
beforeEach(() => {
  state.apiUrl = "http://server-a/db/v1";
  state.connectionId = "remote";
});
afterEach(() => {
  cleanup();
  localStorage.clear();
  vi.unstubAllGlobals();
});

describe("inference request context and relation schemas", () => {
  it.each([
    ["remote", true],
    ["local-inference", false],
  ])("endpoint changes cancel only requests routed through %s", async (connectionId, expectedAbort) => {
    state.connectionId = connectionId;
    state.apiUrl = "http://server-a/db/v1";
    state.models = [{ name: "shared-model", inputs: ["text"], capabilities: ["relations"] }];
    const fetchMock = vi.fn().mockReturnValue(new Promise(() => {}));
    vi.stubGlobal("fetch", fetchMock);
    const view = render(
      <MemoryRouter>
        <EmbeddingPlaygroundPage />
      </MemoryRouter>
    );
    fireEvent.change(screen.getByPlaceholderText("Enter a query to compare against documents..."), {
      target: { value: "query" },
    });
    fireEvent.change(screen.getByPlaceholderText("Enter document text..."), {
      target: { value: "document" },
    });
    fireEvent.click(screen.getByRole("button", { name: /Embed/ }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const signal = fetchMock.mock.calls[0][1].signal as AbortSignal;
    act(() => {
      state.apiUrl = "http://server-b/db/v1";
      view.rerender(
        <MemoryRouter>
          <EmbeddingPlaygroundPage />
        </MemoryRouter>
      );
    });
    expect(signal.aborted).toBe(expectedAbort);
  });

  it.each([
    ["Babelscape/rebel-large", ["relations"]],
    ["custom/renamed-relations", ["relations", "untyped_entities"]],
    ["custom/gliner-relations", ["relations"]],
  ])("submits advertised generic-entity model %s with a compatible schema", async (name, capabilities) => {
    state.models = [{ name, inputs: ["text"], capabilities }];
    localStorage.setItem(
      "antfarm-playground-knowledge-graph",
      JSON.stringify({
        entityLabels: ["person", "organization"],
        relationLabels: ["person::works_for::organization"],
      })
    );
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ model: name, data: [{ entities: [], relations: [] }] }), {
        headers: { "content-type": "application/json" },
      })
    );
    vi.stubGlobal("fetch", fetchMock);
    render(
      <MemoryRouter initialEntries={[`/?connection=remote&model=${encodeURIComponent(name)}`]}>
        <KnowledgeGraphPlaygroundPage />
      </MemoryRouter>
    );
    expect(screen.queryByText(/Not yet available:/)).toBeNull();
    fireEvent.change(screen.getByPlaceholderText(/^(Enter text|Paste or type)/), {
      target: { value: "Alice works at Acme." },
    });
    fireEvent.click(screen.getByRole("button", { name: /Build Graph/ }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const request = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(request.model).toBe(name);
    if (name === "custom/gliner-relations") {
      expect(request.schema.entities).toEqual(["person", "organization"]);
      expect(request.schema.relations).toEqual([{ type: "person::works_for::organization" }]);
    } else {
      expect(request.schema.entities).toEqual(["ENTITY"]);
      expect(request.schema.relations).toEqual([{ type: "works_for" }]);
    }
  });
});
