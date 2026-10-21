import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Link, MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import InferenceGatewayPage from "./InferenceGatewayPage";

const { client, apiConfig } = vi.hoisted(() => {
  const client = { inference: { models: vi.fn(), invoke: vi.fn() } };
  return { client, apiConfig: { client } };
});
vi.mock("@/hooks/use-api-config", () => ({ useApiConfig: () => apiConfig }));
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  apiConfig.client = client;
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

it("selects embeddings for a physical embedding model and preserves explicit choice for ambiguous models", async () => {
  const id = "connections/runtime/models/mock-sparse";
  client.inference.models.mockResolvedValue({
    data: [
      { id, task: "embed", tasks: ["embed"] },
      { id: "connections/runtime/models/shared", tasks: ["embed", "generate"] },
    ],
  });
  client.inference.invoke.mockResolvedValue(
    new Response("{}", { headers: { "content-type": "application/json" } })
  );
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() => expect(document.querySelector(`option[value="${id}"]`)).not.toBeNull());
  fireEvent.change(screen.getByLabelText("Model"), { target: { value: id } });
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("embeddings");
  fireEvent.click(screen.getByRole("button", { name: "Run" }));
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "embeddings",
      expect.objectContaining({ model: id }),
      expect.any(AbortSignal)
    )
  );
  fireEvent.change(screen.getByLabelText("Model"), {
    target: { value: "connections/runtime/models/shared" },
  });
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("embeddings");
});

it("runs a discovered physical decider using its qualified model ID", async () => {
  const id = "connections/runtime/models/mock-decision";
  client.inference.models.mockResolvedValue({ data: [{ id, task: "decide", tasks: ["decide"] }] });
  client.inference.invoke.mockResolvedValue(
    new Response("{}", { headers: { "content-type": "application/json" } })
  );
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() => expect(document.querySelector(`option[value="${id}"]`)).not.toBeNull());
  fireEvent.change(screen.getByLabelText("Model"), { target: { value: id } });
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("decisions");
  fireEvent.click(screen.getByRole("button", { name: "Run" }));
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "decisions",
      expect.objectContaining({ model: id, questions: expect.any(Array) }),
      expect.any(AbortSignal)
    )
  );
});

it("selects prediction for a connection-qualified predictor and retains its public ID", async () => {
  const id = "connections/remote/models/predictors/risk";
  client.inference.models.mockResolvedValue({
    data: [{ id, task: "predict", tasks: ["predict"] }],
  });
  client.inference.invoke.mockResolvedValue(
    new Response("{}", { headers: { "content-type": "application/json" } })
  );
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() => expect(document.querySelector(`option[value="${id}"]`)).not.toBeNull());
  fireEvent.change(screen.getByLabelText("Model"), { target: { value: id } });
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("predict");
  fireEvent.click(screen.getByRole("button", { name: "Run" }));
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "predict",
      { model: id, input: [[1, 2]] },
      expect.any(AbortSignal)
    )
  );
});

const streamingCases = [
  { support: "supported", stream: true },
  { support: "unsupported", stream: false },
  { support: "unknown", stream: false },
  { support: undefined, stream: false },
] as const;

for (const entryPoint of ["selection", "deep link"] as const) {
  it.each(
    streamingCases
  )(`uses streaming=$stream for $support support through ${entryPoint}`, async ({
    support,
    stream,
  }) => {
    const id = "generators/chat";
    client.inference.models.mockResolvedValue({
      data: [
        {
          id,
          task: "generate",
          gateway_capabilities: support ? { streaming: support } : undefined,
        },
      ],
    });
    client.inference.invoke.mockImplementation(
      async () => new Response("{}", { headers: { "content-type": "application/json" } })
    );
    render(
      <MemoryRouter
        initialEntries={[entryPoint === "deep link" ? `/?model=${encodeURIComponent(id)}` : "/"]}
      >
        <InferenceGatewayPage />
      </MemoryRouter>
    );
    await waitFor(() => expect(document.querySelector(`option[value="${id}"]`)).not.toBeNull());
    if (entryPoint === "selection") {
      fireEvent.change(screen.getByLabelText("Model"), { target: { value: id } });
    }
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() =>
      expect(client.inference.invoke).toHaveBeenCalledWith(
        "chat/completions",
        { model: id, messages: [{ role: "user", content: "Hello" }], stream },
        expect.any(AbortSignal)
      )
    );
    await waitFor(() =>
      expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(
        false
      )
    );
    // Returning to chat uses the selected model's capabilities as well.
    fireEvent.change(screen.getByLabelText("Operation"), { target: { value: "decisions" } });
    fireEvent.change(screen.getByLabelText("Operation"), { target: { value: "chat/completions" } });
    expect(
      JSON.parse((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value).stream
    ).toBe(stream);
  });
}

it("refreshes default streaming on model changes without overriding an edited request", async () => {
  const id = "generators/streaming";
  const ambiguous = "connections/runtime/models/shared";
  client.inference.models.mockResolvedValue({
    data: [
      { id, task: "generate", gateway_capabilities: { streaming: "supported" } },
      { id: ambiguous, tasks: ["generate", "embed"] },
    ],
  });
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() => expect(document.querySelector(`option[value="${id}"]`)).not.toBeNull());
  const model = screen.getByLabelText("Model");
  const request = screen.getByLabelText("JSON request") as HTMLTextAreaElement;
  fireEvent.change(model, { target: { value: id } });
  expect(JSON.parse(request.value).stream).toBe(true);
  fireEvent.change(model, { target: { value: ambiguous } });
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("chat/completions");
  expect(JSON.parse(request.value).stream).toBe(false);
  fireEvent.change(model, { target: { value: id } });
  fireEvent.change(model, { target: { value: "manual-model" } });
  expect(JSON.parse(request.value).stream).toBe(false);
  const custom = JSON.stringify({ messages: [{ role: "user", content: "Custom" }], stream: true });
  fireEvent.change(request, { target: { value: custom } });
  fireEvent.change(model, { target: { value: ambiguous } });
  expect(request.value).toBe(custom);
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

type Catalog = {
  data: { id: string; task: string; gateway_capabilities?: { streaming: string } }[];
};

it("preserves and submits an edited prompt when deep-link discovery finishes", async () => {
  const pending = deferred<Catalog>();
  client.inference.models.mockReturnValue(pending.promise);
  client.inference.invoke.mockImplementation(async () => new Response("{}"));
  render(
    <MemoryRouter initialEntries={["/?model=generators%2Fchat"]}>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  const custom = { messages: [{ role: "user", content: "The actual prompt" }], stream: false };
  fireEvent.change(screen.getByLabelText("JSON request"), {
    target: { value: JSON.stringify(custom) },
  });
  await act(async () =>
    pending.resolve({
      data: [
        {
          id: "generators/chat",
          task: "generate",
          gateway_capabilities: { streaming: "supported" },
        },
      ],
    })
  );
  expect(JSON.parse((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value)).toEqual(
    custom
  );
  fireEvent.click(screen.getByRole("button", { name: "Run" }));
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "chat/completions",
      { ...custom, model: "generators/chat" },
      expect.any(AbortSignal)
    )
  );
});

it("retains an explicitly selected batch operation through delayed discovery", async () => {
  const pending = deferred<Catalog>();
  client.inference.models.mockReturnValue(pending.promise);
  client.inference.invoke.mockImplementation(async () => new Response("{}"));
  render(
    <MemoryRouter initialEntries={["/?model=generators%2Fchat"]}>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  fireEvent.change(screen.getByLabelText("Operation"), { target: { value: "generate/batch" } });
  const custom = {
    requests: [
      {
        custom_id: "actual",
        body: { messages: [{ role: "user", content: "Actual batch prompt" }] },
      },
    ],
  };
  fireEvent.change(screen.getByLabelText("JSON request"), {
    target: { value: JSON.stringify(custom) },
  });
  await act(async () => pending.resolve({ data: [{ id: "generators/chat", task: "generate" }] }));
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("generate/batch");
  fireEvent.click(screen.getByRole("button", { name: "Run" }));
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "generate/batch",
      {
        requests: [
          { custom_id: "actual", body: { ...custom.requests[0].body, model: "generators/chat" } },
        ],
      },
      expect.any(AbortSignal)
    )
  );
});

it("applies delayed catalog defaults to the current model instead of the original deep link", async () => {
  const pending = deferred<Catalog>();
  client.inference.models.mockReturnValue(pending.promise);
  render(
    <MemoryRouter initialEntries={["/?model=generators%2Fchat"]}>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  fireEvent.change(screen.getByLabelText("Model"), { target: { value: "embedders/current" } });
  await act(async () =>
    pending.resolve({
      data: [
        { id: "generators/chat", task: "generate" },
        { id: "embedders/current", task: "embed" },
      ],
    })
  );
  expect((screen.getByLabelText("Model") as HTMLInputElement).value).toBe("embedders/current");
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("embeddings");
  expect(JSON.parse((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value)).toEqual({
    input: ["Hello world"],
  });
});

it.each([
  "resolve",
  "reject",
] as const)("ignores a stale catalog %s after the API client changes", async (outcome) => {
  const pending = deferred<Catalog>();
  client.inference.models.mockReturnValue(pending.promise);
  const view = render(
    <MemoryRouter initialEntries={["/?model=generators%2Fchat"]}>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  const originalSignal = client.inference.models.mock.calls[0][0] as AbortSignal;
  const custom = JSON.stringify({
    messages: [{ role: "user", content: "Preserve on refresh" }],
    stream: false,
  });
  fireEvent.change(screen.getByLabelText("JSON request"), { target: { value: custom } });
  apiConfig.client = {
    inference: {
      models: vi.fn().mockResolvedValue({ data: [{ id: "generators/current", task: "generate" }] }),
      invoke: vi.fn(),
    },
  };
  view.rerender(
    <MemoryRouter initialEntries={["/?model=generators%2Fchat"]}>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() =>
    expect(document.querySelector('option[value="generators/current"]')).not.toBeNull()
  );
  expect(originalSignal.aborted).toBe(true);
  await act(async () => {
    if (outcome === "resolve")
      pending.resolve({ data: [{ id: "generators/stale", task: "generate" }] });
    else pending.reject(new Error("Stale discovery failure"));
  });
  expect(document.querySelector('option[value="generators/stale"]')).toBeNull();
  expect(document.querySelector('option[value="generators/current"]')).not.toBeNull();
  expect((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value).toBe(custom);
  expect(screen.queryByRole("alert")).toBeNull();
});

it("starts a fresh draft when navigating to another model deep link", async () => {
  const pending = deferred<Catalog>();
  client.inference.models
    .mockReturnValueOnce(pending.promise)
    .mockResolvedValueOnce({ data: [{ id: "embedders/new", task: "embed" }] });
  render(
    <MemoryRouter initialEntries={["/?model=generators%2Fold"]}>
      <Link to="/?model=embedders%2Fnew">Next model</Link>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  const originalSignal = client.inference.models.mock.calls[0][0] as AbortSignal;
  fireEvent.change(screen.getByLabelText("JSON request"), { target: { value: "old draft" } });
  fireEvent.click(screen.getByText("Next model"));
  await waitFor(() =>
    expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe("embeddings")
  );
  expect(originalSignal.aborted).toBe(true);
  await act(async () => pending.resolve({ data: [{ id: "generators/old", task: "generate" }] }));
  expect((screen.getByLabelText("Model") as HTMLInputElement).value).toBe("embedders/new");
  expect(document.querySelector('option[value="generators/old"]')).toBeNull();
  expect(JSON.parse((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value)).toEqual({
    input: ["Hello world"],
  });
});

it.each([
  "generate",
  "generate/batch",
] as const)("preserves the explicit %s operation even when its sample is untouched", async (operation) => {
  const pending = deferred<Catalog>();
  client.inference.models.mockReturnValue(pending.promise);
  render(
    <MemoryRouter initialEntries={["/?model=generators%2Fchat"]}>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  fireEvent.change(screen.getByLabelText("Operation"), { target: { value: operation } });
  const sample = (screen.getByLabelText("JSON request") as HTMLTextAreaElement).value;
  await act(async () => pending.resolve({ data: [{ id: "generators/chat", task: "generate" }] }));
  expect((screen.getByLabelText("Operation") as HTMLSelectElement).value).toBe(operation);
  expect((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value).toBe(sample);
});

it("shows deployment enablement instead of an executable playground when public inference is disabled", async () => {
  client.inference.models.mockResolvedValue({
    object: "list",
    data: [],
    public_inference_enabled: false,
  });
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await screen.findByText("Public inference is disabled");
  expect(screen.queryByRole("button", { name: "Run" })).toBeNull();
  expect(screen.queryByLabelText("Model")).toBeNull();
  expect(client.inference.invoke).not.toHaveBeenCalled();
});

it("runs multimodal decision evidence for an image-capable decider", async () => {
  const id = "deciders/visual";
  client.inference.models.mockResolvedValue({
    data: [{ id, task: "decide", gateway_capabilities: { images: "supported" } }],
  });
  client.inference.invoke.mockResolvedValue(
    new Response("{}", { headers: { "content-type": "application/json" } })
  );
  render(
    <MemoryRouter>
      <InferenceGatewayPage />
    </MemoryRouter>
  );
  await waitFor(() => expect(document.querySelector(`option[value="${id}"]`)).not.toBeNull());
  fireEvent.change(screen.getByLabelText("Model"), { target: { value: id } });
  fireEvent.click(screen.getByRole("button", { name: "Use image example" }));
  const body = JSON.parse((screen.getByLabelText("JSON request") as HTMLTextAreaElement).value);
  expect(body.input[0].content[1]).toEqual({
    type: "input_image",
    image_url: expect.stringMatching(/^data:image\/png;base64,/),
    detail: "auto",
  });
  fireEvent.click(screen.getByRole("button", { name: "Run" }));
  await waitFor(() =>
    expect(client.inference.invoke).toHaveBeenCalledWith(
      "decisions",
      { ...body, model: id },
      expect.any(AbortSignal)
    )
  );
});
