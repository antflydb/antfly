import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ExtractionPlaygroundPage from "./ExtractionPlaygroundPage";
import KnowledgeGraphPlaygroundPage from "./KnowledgeGraphPlaygroundPage";

const mocks = vi.hoisted(() => ({ models: ["gliner2"], request: vi.fn() }));
vi.mock("@/hooks/use-connections", () => ({
  useSelectedInferenceModelNames: () => ({ models: mocks.models, loading: false }),
}));
vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({ inferenceUrl: () => "/extract" }),
}));
vi.mock("@/components/branded-empty-state", () => ({ PlaygroundEmptyState: () => null }));
vi.mock("@/components/playground/BackendInfoBar", () => ({ BackendInfoBar: () => null }));
vi.mock("@/components/playground/NoModelsGuide", () => ({ NoModelsGuide: () => null }));
vi.mock("@/lib/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/utils")>()),
  fetchWithRetry: mocks.request,
}));

beforeEach(() => {
  vi.stubGlobal("localStorage", { getItem: () => null, setItem: vi.fn() });
  mocks.request.mockReset();
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe.each([
  ["extraction", ExtractionPlaygroundPage],
  ["graph", KnowledgeGraphPlaygroundPage],
] as const)("%s playground availability", (_, Page) => {
  it.each([
    "gliner2.5-base",
    "invoice-extractor",
  ])("blocks an unavailable URL model %s, including keyboard submission", (model) => {
    render(
      <MemoryRouter initialEntries={[`/?model=${model}`]}>
        <Page />
      </MemoryRouter>
    );
    fireEvent.change(screen.getByPlaceholderText(/^(Enter text|Paste or type)/), {
      target: { value: "Alice works at Acme." },
    });
    expect(screen.getByText(/Not yet available:/)).toBeTruthy();
    const run = screen.getByRole("button", {
      name: /Extract Entities|Build Graph/,
    }) as HTMLButtonElement;
    expect(run.disabled).toBe(true);
    fireEvent.keyDown(document, { key: "Enter", ctrlKey: true });
    expect(mocks.request).not.toHaveBeenCalled();
  });
});
