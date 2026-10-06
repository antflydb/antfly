import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { formatGeneratorSummary, GeneratorSelector } from "./GeneratorSelector";

vi.mock("@/hooks/use-connections", () => ({
  useConnectedModels: () => ({ providers: [] }),
  liveModelSuggestions: () => ({}),
}));

afterEach(cleanup);

it("shows Apple options without a model selector", () => {
  render(
    <GeneratorSelector
      value={{ provider: "apple", max_tokens: 256 }}
      defaultConfig={{ provider: "apple", max_tokens: 256 }}
      onChange={vi.fn()}
    />
  );
  expect(screen.queryByText("Model")).toBeNull();
  expect(screen.getByText("Temperature")).toBeDefined();
  expect(formatGeneratorSummary({ provider: "apple" })).toBe("Apple (On Device)");
});
