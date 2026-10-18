import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import InferenceGatewayPage from "./InferenceGatewayPage";

const { client } = vi.hoisted(() => ({
  client: { inference: { models: vi.fn(), invoke: vi.fn() } },
}));
vi.mock("@/hooks/use-api-config", () => ({ useApiConfig: () => ({ client }) }));
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

it("submits mixed batches without a selection and keeps the batch when selecting a default", async () => {
  client.inference.models.mockResolvedValue({
    data: [{ id: "generators/default", task: "generate" }],
  });
  client.inference.invoke.mockImplementation(
    async () =>
      new Response('{"object":"generate.batch"}', {
        headers: { "content-type": "application/json" },
      })
  );
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() =>
    expect(document.querySelector('option[value="generators/default"]')).not.toBeNull()
  );
  fireEvent.change(screen.getByLabelText("Operation"), { target: { value: "generate/batch" } });
  const requests = [
    { custom_id: "first", body: { model: "generators/first", messages: [] } },
    { custom_id: "second", body: { model: "chains/second", messages: [] } },
  ];
  fireEvent.change(screen.getByLabelText("JSON request"), {
    target: { value: JSON.stringify({ requests }) },
  });
  const run = screen.getByRole("button", { name: "Run" });
  expect((run as HTMLButtonElement).disabled).toBe(false);
  fireEvent.click(run);
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "generate/batch",
      { requests },
      expect.any(AbortSignal)
    )
  );
  await waitFor(() => expect((run as HTMLButtonElement).disabled).toBe(false));
  fireEvent.change(screen.getByLabelText("Default model (optional)"), {
    target: { value: "generators/default" },
  });
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("generate/batch");
  expect((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value).toBe(
    JSON.stringify({ requests })
  );
});
