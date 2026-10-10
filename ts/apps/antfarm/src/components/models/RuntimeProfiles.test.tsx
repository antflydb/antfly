import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { InferenceModel } from "@/data/inference-models";
import { RuntimeProfiles } from "./RuntimeProfiles";

const models: InferenceModel[] = ["local", "remote"].map((connectionId) => ({
  id: connectionId,
  connectionId,
  connectionName: connectionId,
  name: "Gemma4 E2B",
  provider: "antfly",
  source: "google/gemma4-e2b-qat",
  sourceUrl: "",
  type: "generator",
  description: "",
  variants: [],
  inRegistry: true,
}));
const key = (connection: string) =>
  `antfarm-runtime-profile:${JSON.stringify(["endpoint", connection])}`;
beforeEach(() => localStorage.clear());
afterEach(cleanup);

it("accepts whole CPU-core and context counts through native form validation", () => {
  render(<RuntimeProfiles models={models} endpointKey="endpoint" onDetails={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: "Set up device" }));
  const cores = screen.getByLabelText("CPU cores") as HTMLInputElement;
  fireEvent.change(cores, { target: { value: "10" } });
  expect(cores.form?.checkValidity()).toBe(true);
  fireEvent.change(cores, { target: { value: "10.5" } });
  expect(cores.validity.stepMismatch).toBe(true);
  fireEvent.change(cores, { target: { value: "10" } });
  fireEvent.click(screen.getByRole("button", { name: "Save device" }));
  expect(JSON.parse(localStorage.getItem(key("local")) ?? "{}").device.cpuCores).toBe(10);

  fireEvent.click(screen.getByRole("button", { name: "Record measurements" }));
  const context = screen.getByLabelText("Tested context (tokens)") as HTMLInputElement;
  fireEvent.change(context, { target: { value: "4031" } });
  expect(context.form?.checkValidity()).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Save measurements" }));
  const stored = JSON.parse(localStorage.getItem(key("local")) ?? "{}");
  expect(stored.models[JSON.stringify(["generator", models[0].source])].contextTokens).toBe(4031);
});

it("keeps device and model measurements scoped to their connection", () => {
  localStorage.setItem(
    key("local"),
    JSON.stringify({
      version: 1,
      device: { name: "MacBook Air", chip: "Apple M4", memoryGB: 16 },
      models: {
        [JSON.stringify(["generator", models[0].source])]: {
          quantization: "Q4 QAT",
          notes: "Local run",
          decodeTokensPerSecond: 40,
        },
      },
    })
  );
  render(<RuntimeProfiles models={models} endpointKey="endpoint" onDetails={vi.fn()} />);
  expect(screen.getByText("MacBook Air")).toBeTruthy();
  expect(screen.getByText("Q4 QAT")).toBeTruthy();
  fireEvent.change(screen.getByLabelText("Runtime profile connection"), {
    target: { value: "remote" },
  });
  expect(screen.queryByText("MacBook Air")).toBeNull();
  expect(screen.queryByText("Q4 QAT")).toBeNull();
  fireEvent.change(screen.getByLabelText("Runtime profile connection"), {
    target: { value: "local" },
  });
  expect(screen.getByText("MacBook Air")).toBeTruthy();
});

it("invalidates recorded model measurements when the hardware changes", () => {
  localStorage.setItem(
    key("local"),
    JSON.stringify({
      version: 1,
      device: { name: "MacBook Air", chip: "Apple M4", memoryGB: 16 },
      models: {
        [JSON.stringify(["generator", models[0].source])]: { quantization: "Q4 QAT", notes: "" },
      },
    })
  );
  render(<RuntimeProfiles models={models} endpointKey="endpoint" onDetails={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: "Edit device" }));
  fireEvent.change(screen.getByLabelText("Chip / GPU"), { target: { value: "Apple M5" } });
  fireEvent.click(screen.getByRole("button", { name: "Save device" }));
  expect(screen.getByText("Apple M5")).toBeTruthy();
  expect(screen.queryByText("Q4 QAT")).toBeNull();
  expect(JSON.parse(localStorage.getItem(key("local")) ?? "{}").models).toEqual({});
});
