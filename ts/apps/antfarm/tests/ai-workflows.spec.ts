import { expect, type Page, test } from "@playwright/test";

const browserErrors = new WeakMap<Page, string[]>();

async function visibleStandaloneSemicolons(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const matches: string[] = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();
    while (node) {
      if (node.nodeValue?.trim() === ";") {
        const parent = node.parentElement;
        if (parent) {
          const style = getComputedStyle(parent);
          if (style.display !== "none" && style.visibility !== "hidden") {
            matches.push(parent.outerHTML.slice(0, 240));
          }
        }
      }
      node = walker.nextNode();
    }
    return matches;
  });
}

const routes = [
  ["/inference/models", "Models & Instances"],
  ["/inference/playground/chunk", "Chunking Playground"],
  ["/inference/playground/extract", "Extraction Playground"],
  ["/inference/playground/rewrite", "Rewrite"],
  ["/inference/playground/rerank", "Reranking Playground"],
  ["/inference/playground/kg", "Knowledge Graph Playground"],
  ["/inference/playground/embed", "Embedding Playground"],
  ["/inference/playground/read", "Reader Playground"],
  ["/inference/playground/transcribe", "Transcribe Playground"],
  ["/data/playground/chat", "Ask your data"],
  ["/data/playground/rag", "Answer pipeline"],
  ["/data/playground/evals", "Evaluate answers"],
  ["/data/playground/embed", "Embedding Explorer"],
  ["/data/playground/rerank", "Reranking Explorer"],
  ["/data/playground/chunk", "Chunking Playground"],
] as const;

const connections = {
  connections: [
    {
      id: "local-inference",
      name: "local-inference",
      display_name: "Local inference",
      kind: "inference",
      status: "connected",
      capabilities: [
        "models.embedder",
        "models.reranker",
        "models.chunker",
        "models.generator",
        "models.extractor",
        "models.reader",
        "models.transcriber",
      ],
      inference: {
        provider: "antfly",
        configured_model_types: [
          "embedder",
          "reranker",
          "chunker",
          "generator",
          "extractor",
          "reader",
          "transcriber",
        ],
        models: {
          embedders: [
            { name: "mock-embed", inputs: ["text"] },
            { name: "mock-embed-second", inputs: ["text"] },
          ],
          rerankers: [
            { name: "mock-rerank", inputs: ["text"] },
            { name: "mock-rerank-second", inputs: ["text"] },
          ],
          chunkers: [
            { name: "mock-semantic-chunker", inputs: ["text"] },
            { name: "mock-semantic-chunker-second", inputs: ["text"] },
            { name: "mock-vad", inputs: ["audio"] },
            { name: "fixed", inputs: ["text"] },
          ],
          generators: [{ name: "mock-generator", inputs: ["text"] }],
          extractors: [
            { name: "mock-extractor", inputs: ["text"], capabilities: ["relations"] },
            { name: "mock-extractor-second", inputs: ["text"], capabilities: ["relations"] },
          ],
          readers: [
            { name: "mock-reader", inputs: ["image"] },
            { name: "mock-reader-second", inputs: ["image"] },
          ],
          transcribers: [
            { name: "mock-transcriber", inputs: ["audio"] },
            { name: "mock-transcriber-second", inputs: ["audio"] },
          ],
        },
      },
    },
  ],
  supported: true,
};

async function installApiMocks(page: Page) {
  await page.addInitScript(() => {
    window.__ANTFARM_CONFIG__ = { authMode: "external" };
    if (!sessionStorage.getItem("antfarm-ai-test-initialized")) {
      localStorage.clear();
      sessionStorage.setItem("antfarm-ai-test-initialized", "true");
    }
    localStorage.setItem(
      "antfarm-dashboard-generator",
      JSON.stringify({ provider: "antfly", model: "mock-generator" })
    );
  });

  await page.route("**/healthz", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/ai/v1/_antfarm/readers", (route) =>
    route.fulfill({ json: { apple_ocr: { available: false } } })
  );
  await page.route("**/ai/v1/models/instances", (route) =>
    route.fulfill({ json: { object: "list", data: [] } })
  );
  await page.route("**/ai/v1/models", (route) =>
    route.fulfill({
      json: {
        allow_downloads: false,
        embedders: {
          "mock-embed": { inputs: ["text"] },
          "mock-embed-second": { inputs: ["text"] },
        },
        rerankers: {
          "mock-rerank": { inputs: ["text"] },
          "mock-rerank-second": { inputs: ["text"] },
        },
        chunkers: {
          "mock-semantic-chunker": { inputs: ["text"] },
          "mock-semantic-chunker-second": { inputs: ["text"] },
          "mock-vad": { inputs: ["audio"] },
          fixed: { inputs: ["text"] },
        },
        generators: { "mock-generator": { inputs: ["text"] } },
        extractors: {
          "mock-extractor": { inputs: ["text"], capabilities: ["relations"] },
          "mock-extractor-second": { inputs: ["text"], capabilities: ["relations"] },
        },
        readers: {
          "mock-reader": { inputs: ["image"] },
          "mock-reader-second": { inputs: ["image"] },
        },
        transcribers: {
          "mock-transcriber": { inputs: ["audio"] },
          "mock-transcriber-second": { inputs: ["audio"] },
        },
      },
    })
  );

  await page.route("**/db/v1/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/db/v1/status") {
      await route.fulfill({
        json: {
          auth_enabled: false,
          health: "healthy",
          connectors: { chatgpt: { enabled: false } },
        },
      });
    } else if (url.pathname === "/db/v1/connections/chatgpt/accounts") {
      await route.fulfill({ json: { accounts: [] } });
    } else if (url.pathname === "/db/v1/tables") {
      await route.fulfill({ json: [{ name: "docs" }, { name: "archive" }] });
    } else if (/\/db\/v1\/tables\/[^/]+\/indexes$/.test(url.pathname)) {
      await route.fulfill({
        json: [
          { config: { name: "semantic", type: "embedding" }, status: {} },
          { config: { name: "keywords", type: "full_text" }, status: {} },
        ],
      });
    } else if (url.pathname === "/db/v1/connections") {
      await route.fulfill({ json: connections });
    } else if (url.pathname === "/db/v1/agents/retrieval") {
      await route.fulfill({
        json: {
          generation: "Mock answer",
          responses: [],
          eval_result: { scores: { generation: { correctness: { score: 1, pass: true } } } },
        },
      });
    } else if (/\/db\/v1\/tables\/[^/]+\/query$/.test(url.pathname)) {
      await route.fulfill({ json: { responses: [{ status: 200, took: 1, hits: { hits: [] } }] } });
    } else {
      await route.fulfill({ json: {} });
    }
  });
}

async function installOpenRetrievalStream(page: Page, partial: string) {
  await page.addInitScript((generation) => {
    const originalFetch = window.fetch.bind(window);
    window.fetch = async (input, init) => {
      const url = new URL(typeof input === "string" ? input : input.url, window.location.href);
      if (url.pathname !== "/db/v1/agents/retrieval") return originalFetch(input, init);

      const encoder = new TextEncoder();
      let streamController: ReadableStreamDefaultController<Uint8Array>;
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          streamController = controller;
          controller.enqueue(
            encoder.encode(`event: generation\ndata: ${JSON.stringify(generation)}\n\n`)
          );
        },
      });
      init?.signal?.addEventListener(
        "abort",
        () => streamController.error(new DOMException("Aborted", "AbortError")),
        { once: true }
      );
      return new Response(stream, { headers: { "Content-Type": "text/event-stream" } });
    };
  }, partial);
}

test.describe("AI workflow routes", () => {
  test.beforeEach(async ({ page }) => {
    const errors: string[] = [];
    browserErrors.set(page, errors);
    page.on("pageerror", (error) => errors.push(`pageerror: ${error.message}`));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(`console: ${message.text()}`);
    });
    await installApiMocks(page);
  });

  test.afterEach(async ({ page }) => {
    expect(browserErrors.get(page) ?? []).toEqual([]);
  });

  for (const [path, heading] of routes) {
    test(`${path} renders its workflow and reset action`, async ({ page }) => {
      await page.goto(`${path}?table=docs&index=semantic`);
      await expect(page.getByRole("heading", { name: heading, exact: true }).first()).toBeVisible();
      expect(await visibleStandaloneSemicolons(page), `${path} rendered stray semicolons`).toEqual(
        []
      );
      const reset = page.getByRole("button", { name: "Reset", exact: true });
      if (await reset.count())
        await reset.first().evaluate((button: HTMLButtonElement) => button.click());
      await expect(page.locator(".af-ai-workflow")).toBeVisible();
    });
  }

  test("captures light, dark, and mobile workflow layouts", async ({ page }, testInfo) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    await page.goto("/inference/playground/extract");
    await expect(page.getByRole("heading", { name: "Extraction Playground" })).toBeVisible();
    await testInfo.attach("ai-workflow-light", {
      body: await page.screenshot({ fullPage: true }),
      contentType: "image/png",
    });

    const themeButton = page.getByTitle(/Switch to (Light|Dark|System) Mode/);
    while (
      !(await page.locator("html").evaluate((element) => element.classList.contains("dark")))
    ) {
      await themeButton.click();
    }
    await expect(page.locator("html")).toHaveClass(/dark/);
    await page.waitForTimeout(50);
    const sidebarTheme = await page.locator('[data-slot="sidebar-inner"]').evaluate((element) => {
      const style = getComputedStyle(element);
      const probe = document.createElement("div");
      probe.style.backgroundColor = style.getPropertyValue("--sidebar-expanded");
      document.body.appendChild(probe);
      const tokenColor = getComputedStyle(probe).backgroundColor;
      probe.remove();
      return { rendered: style.backgroundColor, token: tokenColor };
    });
    expect(sidebarTheme.rendered).toBe(sidebarTheme.token);
    await testInfo.attach("ai-workflow-dark", {
      body: await page.screenshot({ fullPage: true }),
      contentType: "image/png",
    });

    await page.setViewportSize({ width: 390, height: 844 });
    const configurationPanel = page.locator(".af-ai-panel").filter({ hasText: "Configuration" });
    await expect(configurationPanel).toBeVisible();
    expect(
      await configurationPanel.evaluate((panel) => panel.scrollWidth - panel.clientWidth)
    ).toBeLessThanOrEqual(1);
    await testInfo.attach("ai-workflow-mobile", {
      body: await page.screenshot({ fullPage: true }),
      contentType: "image/png",
    });
  });

  test("evaluation table changes keep controls usable", async ({ page }) => {
    await page.goto("/data/playground/evals?table=docs&index=semantic");
    await page.getByRole("button", { name: "Load Sample" }).click();
    await expect(page.getByText("Eval Set Items (3)")).toBeVisible();
    await expect(page.getByText("What is the capital of France?")).toBeVisible();
    await expect(page.getByText("When was the Eiffel Tower built?")).toBeVisible();
    await expect(page.getByText("Who designed the Eiffel Tower?")).toBeVisible();
    await expect(page.getByRole("button", { name: "Run Evals" })).toBeEnabled();

    await page.getByLabel("Table").click();
    await page.getByRole("option", { name: "archive" }).click();
    await expect(page).toHaveURL(/table=archive/);
    await expect(page.getByRole("button", { name: "Run Evals" })).toBeEnabled();
  });

  test("AI workflows do not overflow a narrow viewport", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    for (const [path, heading] of routes) {
      await page.goto(`${path}?table=docs&index=semantic`);
      await expect(page.getByRole("heading", { name: heading, exact: true }).first()).toBeVisible();
      const overflow = await page.evaluate(
        () => document.documentElement.scrollWidth - document.documentElement.clientWidth
      );
      expect(overflow, `${path} horizontal overflow`).toBeLessThanOrEqual(1);
    }
  });

  test("models can be warmed and released through lifecycle controls", async ({ page }) => {
    let loaded = false;
    const actions: string[] = [];
    const instance = {
      id: "instance-1",
      model: "mock-embed",
      tasks: ["embedder"],
      state: "loaded",
      active_handles: 0,
      startup_managed: false,
      unloadable: true,
      backend: "mock",
      residency_mode: "backend_managed",
      idle_ms: 0,
    };
    await page.route("**/ai/v1/models/instances", async (route) => {
      if (route.request().method() === "POST") {
        loaded = true;
        actions.push("load");
        await route.fulfill({ json: instance });
      } else {
        await route.fulfill({ json: { object: "list", data: loaded ? [instance] : [] } });
      }
    });
    await page.route("**/ai/v1/models/instances/instance-1", async (route) => {
      expect(route.request().method()).toBe("DELETE");
      loaded = false;
      actions.push("unload");
      await route.fulfill({ json: { outcome: "unloaded" } });
    });

    await page.goto("/inference/models");
    await page.getByRole("button", { name: "View details for mock-embed", exact: true }).click();
    await page.getByRole("button", { name: "Load model" }).click();
    await expect(page.getByRole("button", { name: "Unload" })).toBeVisible();
    await page.getByRole("button", { name: "Unload" }).click();
    await expect(page.getByRole("button", { name: "Load model" })).toBeVisible();
    expect(actions).toEqual(["load", "unload"]);
  });

  test("loading instances stay labeled Loading while they have load waiters", async ({ page }) => {
    await page.route("**/ai/v1/models/instances", (route) =>
      route.fulfill({
        json: {
          object: "list",
          data: [
            {
              id: "loading-1",
              model: "mock-embed",
              tasks: ["embedder"],
              state: "loading",
              active_handles: 1,
              startup_managed: false,
              unloadable: false,
              unload_blocked_reason: "loading",
              residency_mode: "backend_managed",
              idle_ms: 0,
            },
          ],
        },
      })
    );
    await page.goto("/inference/models");
    await page.getByRole("button", { name: "View details for mock-embed", exact: true }).click();
    const details = page.getByRole("dialog");
    await expect(details.getByText("Loading", { exact: true })).toHaveCount(2);
    await expect(details.getByRole("button", { name: "Unload", exact: true })).toBeDisabled();
  });

  test("built-in chunking does not offer model lifecycle actions", async ({ page }) => {
    await page.goto("/inference/models");
    await page.getByRole("button", { name: "View details for fixed" }).click();
    await expect(page.getByText("Built-in · Available")).toBeVisible();
    await expect(page.getByRole("button", { name: "Load model" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Unload" })).toHaveCount(0);
  });

  test("direct playgrounds honor second-model and connection deep links", async ({ page }) => {
    const deepLinks = [
      ["/inference/playground/chunk", "mock-semantic-chunker-second"],
      ["/inference/playground/extract", "mock-extractor-second"],
      ["/inference/playground/rerank", "mock-rerank-second"],
      ["/inference/playground/kg", "mock-extractor-second"],
      ["/inference/playground/embed", "mock-embed-second"],
      ["/inference/playground/read", "mock-reader-second"],
      ["/inference/playground/transcribe", "mock-transcriber-second"],
    ] as const;

    for (const [path, model] of deepLinks) {
      await page.goto(`${path}?model=${model}&connection=local-inference`);
      await expect(page.getByText(model, { exact: true }).first()).toBeVisible();
      expect(new URL(page.url()).searchParams.get("model")).toBe(model);
      expect(new URL(page.url()).searchParams.get("connection")).toBe("local-inference");
    }
  });

  test("chunker deep links survive delayed model discovery", async ({ page }) => {
    let release!: () => void;
    const catalogReady = new Promise<void>((resolve) => {
      release = resolve;
    });
    await page.route("**/db/v1/connections?**", async (route) => {
      await catalogReady;
      await route.fulfill({ json: connections });
    });
    try {
      await page.goto(
        "/inference/playground/chunk?model=mock-semantic-chunker-second&connection=local-inference&table=docs&index=semantic"
      );
      await expect(page.getByRole("heading", { name: "Chunking Playground" })).toBeVisible();
      await expect(page.getByRole("button", { name: "Chunk", exact: true })).toBeDisabled();
      expect(new URL(page.url()).searchParams.get("model")).toBe("mock-semantic-chunker-second");
    } finally {
      release();
    }
    await expect(
      page.getByText("mock-semantic-chunker-second", { exact: true }).first()
    ).toBeVisible();
    expect(new URL(page.url()).searchParams.get("model")).toBe("mock-semantic-chunker-second");
  });

  test("Models opens the selected model in its connection-aware playground", async ({ page }) => {
    await page.goto("/inference/models");
    await page.getByRole("button", { name: "View details for mock-reader-second" }).click();
    await page.getByRole("button", { name: "Open in Playground" }).click();
    await expect(page).toHaveURL(/\/inference\/playground\/read/);
    const url = new URL(page.url());
    expect(url.searchParams.get("model")).toBe("mock-reader-second");
    expect(url.searchParams.get("connection")).toBe("local-inference");
    await expect(page.getByText("mock-reader-second", { exact: true }).first()).toBeVisible();
  });

  for (const provider of ["antfly", "openrouter"] as const) {
    test(`Models download guidance follows the provider (${provider})`, async ({ page }) => {
      await page.route("**/db/v1/connections?**", (route) =>
        route.fulfill({
          json: {
            connections: [
              {
                ...connections.connections[0],
                inference: {
                  provider,
                  models: { generators: [{ name: "acme/chat" }] },
                },
              },
            ],
          },
        })
      );
      await page.route("https://huggingface.co/api/models/**", (route) =>
        route.fulfill({ json: { downloads: 0, likes: 0 } })
      );
      await page.goto("/inference/models");
      await page.getByRole("button", { name: "View details for acme/chat" }).click();
      if (provider === "antfly") {
        await expect(
          page.getByText("antfly inference pull acme/chat", { exact: true })
        ).toBeVisible();
      } else {
        await expect(page.getByText(/Manage this model through Local inference/)).toBeVisible();
        await expect(
          page.getByText("antfly inference pull acme/chat", { exact: true })
        ).toHaveCount(0);
        await expect(page.getByText("Download variant", { exact: true })).toHaveCount(0);
      }
    });
  }

  test("Apple OCR uses native controls and preserves text regions", async ({ page }) => {
    await page.route("**/ai/v1/_antfarm/readers", (route) =>
      route.fulfill({ json: { apple_ocr: { available: true } } })
    );
    await page.route("**/ai/v1/_antfarm/readers/apple", async (route) => {
      const body = route.request().postDataJSON();
      expect(body.recognition_level).toBe("accurate");
      expect(body.recognition_languages).toEqual(["en-US"]);
      expect(body.images).toHaveLength(1);
      expect(body.model).toBeUndefined();
      expect(body.prompt).toBeUndefined();
      expect(body.max_tokens).toBeUndefined();
      await route.fulfill({
        json: {
          object: "list",
          model: "Apple Vision OCR",
          data: [
            {
              object: "read",
              index: 0,
              text: "Invoice 12345",
              regions: [{ text: "Invoice 12345", bbox: [10, 10, 100, 30], confidence: 0.99 }],
            },
          ],
        },
      });
    });
    await page.goto("/inference/playground/read?model=apple-vision-ocr");
    await expect(page.getByLabel("Recognition")).toBeVisible();
    await expect(page.getByLabel("Prompt", { exact: true })).toHaveCount(0);
    await expect(page.getByLabel("Max Tokens", { exact: true })).toHaveCount(0);
    await page.getByRole("button", { name: "Load Sample" }).click();
    await page.getByRole("button", { name: "Read Images" }).click();
    await expect(page.getByText("Invoice 12345", { exact: true }).first()).toBeVisible();
    await expect(page.getByRole("tab", { name: /Regions/ })).toBeVisible();
  });

  test("Apple OCR is only offered on the local inference connection", async ({ page }) => {
    const remote = {
      ...connections.connections[0],
      id: "remote-inference",
      name: "remote-inference",
      display_name: "Remote inference",
    };
    await page.route("**/db/v1/connections?**", (route) =>
      route.fulfill({
        json: { connections: [connections.connections[0], remote], supported: true },
      })
    );
    await page.route("**/ai/v1/_antfarm/readers", (route) =>
      route.fulfill({ json: { apple_ocr: { available: true } } })
    );
    await page.goto(
      "/inference/playground/read?connection=remote-inference&model=apple-vision-ocr"
    );
    await expect(page.getByLabel("Inference connection")).toHaveValue("remote-inference");
    await expect(page.getByLabel("Reader", { exact: true })).toBeEnabled();
    await page.getByLabel("Reader", { exact: true }).click();
    await expect(page.getByRole("option", { name: "Apple Vision OCR" })).toHaveCount(0);
    await expect(page.getByRole("option", { name: "mock-reader", exact: true })).toBeVisible();
    await page.keyboard.press("Escape");
    await page.getByLabel("Inference connection").selectOption("local-inference");
    await expect(page.getByLabel("Reader", { exact: true })).toBeEnabled();
    await page.getByLabel("Reader", { exact: true }).click();
    await expect(page.getByRole("option", { name: "Apple Vision OCR" })).toBeVisible();
  });

  test("unavailable Apple OCR never becomes a selectable reader", async ({ page }) => {
    await page.route("**/ai/v1/_antfarm/readers", (route) =>
      route.fulfill({ json: { apple_ocr: { available: false } } })
    );
    await page.goto("/inference/playground/read");
    await page.getByLabel("Reader", { exact: true }).click();
    await expect(page.getByRole("option", { name: "Apple Vision OCR" })).toHaveCount(0);
    await expect(page.getByRole("option", { name: "mock-reader", exact: true })).toBeVisible();
  });

  test("reader maps reordered server results to the corresponding images", async ({ page }) => {
    await page.route("**/ai/v1/read", async (route) => {
      const body = route.request().postDataJSON();
      expect(body.images).toHaveLength(2);
      await route.fulfill({
        json: {
          object: "list",
          model: "mock-reader",
          usage: { completion_tokens: 8 },
          data: [
            { index: 1, text: "second image text" },
            { index: 0, text: "first image text" },
          ],
        },
      });
    });

    await page.goto("/inference/playground/read");
    const imageInput = page.locator('input[type="file"][accept="image/*"]');
    const png = Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
      "base64"
    );
    await imageInput.setInputFiles([
      { name: "first.png", mimeType: "image/png", buffer: png },
      { name: "second.png", mimeType: "image/png", buffer: png },
    ]);
    await expect(page.getByAltText("first.png").first()).toBeVisible();
    await expect(page.getByAltText("second.png").first()).toBeVisible();
    await page.getByRole("button", { name: "Read Images" }).click();

    await expect(page.getByText("8 output tokens")).toBeVisible();
    await expect(page.getByText("first image text")).toBeVisible();
    await page.getByRole("button", { name: "Image 2" }).click();
    await expect(page.getByText("second image text")).toBeVisible();
  });

  test("transcription consumes VAD media chunks in server order", async ({ page }) => {
    const transcribedAudio: string[] = [];
    await page.route("**/ai/v1/chunk", async (route) => {
      const body = route.request().postDataJSON();
      expect(body.config.model).toBe("mock-vad");
      await route.fulfill({
        json: {
          object: "list",
          model: "mock-vad",
          data: [
            { index: 0, data: "Y2h1bmstMQ==", start_time_ms: 100, end_time_ms: 900 },
            { index: 1, data: "Y2h1bmstMg==", start_time_ms: 1200, end_time_ms: 2200 },
          ],
        },
      });
    });
    await page.route("**/ai/v1/read", async (route) => {
      const body = route.request().postDataJSON();
      transcribedAudio.push(body.audio);
      await route.fulfill({
        json: {
          object: "list",
          model: "mock-transcriber",
          data: [
            { index: 0, text: body.audio === "Y2h1bmstMQ==" ? "first segment" : "second segment" },
          ],
        },
      });
    });

    await page.goto("/inference/playground/transcribe");
    await page.getByRole("button", { name: "File" }).click();
    await page.locator('input[type="file"][accept="audio/*"]').setInputFiles({
      name: "long.wav",
      mimeType: "audio/wav",
      buffer: Buffer.from("RIFFmockWAVE"),
    });
    const audio = page.locator("audio");
    await expect(audio).toBeVisible();
    await audio.evaluate((element) => {
      Object.defineProperty(element, "duration", { configurable: true, value: 31 });
      element.dispatchEvent(new Event("loadedmetadata"));
    });
    await page.getByLabel("VAD chunking").click();
    await page.getByRole("button", { name: "Transcribe", exact: true }).click();

    await expect(page.getByText("first segment")).toBeVisible();
    await expect(page.getByText("second segment")).toBeVisible();
    expect(transcribedAudio).toEqual(["Y2h1bmstMQ==", "Y2h1bmstMg=="]);
  });

  test("retryable model-busy responses retry and an in-flight inference can be stopped", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      const originalFetch = window.fetch.bind(window);
      let embedCalls = 0;
      Object.defineProperty(window, "__embedCalls", { get: () => embedCalls });
      window.fetch = async (input, init) => {
        const url = new URL(typeof input === "string" ? input : input.url, window.location.href);
        if (url.pathname !== "/ai/v1/embed") return originalFetch(input, init);
        embedCalls += 1;
        if (embedCalls === 1) {
          return Response.json(
            { error: "MODEL_RESOURCE_BUSY", retryable: true, retry_after_ms: 1 },
            { status: 503 }
          );
        }
        if (embedCalls === 2) {
          return Response.json({
            object: "list",
            model: "mock-embed",
            data: [
              { index: 0, embedding: [1, 0] },
              { index: 1, embedding: [1, 0] },
            ],
          });
        }
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => reject(new DOMException("Aborted", "AbortError")),
            { once: true }
          );
        });
      };
    });

    await page.goto("/inference/playground/embed");
    await page.getByLabel("Query").fill("query");
    await page.getByPlaceholder("Enter document text...").fill("document");
    await page.getByRole("button", { name: "Embed & Compare" }).click();
    await expect(page.getByText("Similarity Results")).toBeVisible();
    expect(
      await page.evaluate(() => (window as Window & { __embedCalls: number }).__embedCalls)
    ).toBe(2);

    await page.getByRole("button", { name: "Embed & Compare" }).click();
    await page.getByRole("button", { name: "Stop" }).click();
    await expect(page.getByRole("button", { name: "Embed & Compare" })).toBeEnabled();
    expect(
      await page.evaluate(() => (window as Window & { __embedCalls: number }).__embedCalls)
    ).toBe(3);
  });

  test("changing model discards a deferred embedding response without clearing inputs", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      const originalFetch = window.fetch.bind(window);
      window.fetch = async (input, init) => {
        const url = new URL(typeof input === "string" ? input : input.url, window.location.href);
        if (url.pathname !== "/ai/v1/embed") return originalFetch(input, init);
        return await new Promise<Response>((resolve) => {
          (window as Window & { __resolveEmbed?: () => void }).__resolveEmbed = () =>
            resolve(
              Response.json({
                object: "list",
                model: "mock-embed",
                data: [
                  { index: 0, embedding: [1, 0] },
                  { index: 1, embedding: [1, 0] },
                ],
              })
            );
        });
      };
    });

    await page.goto("/inference/playground/embed?model=mock-embed&connection=local-inference");
    await page.getByLabel("Query").fill("keep this query");
    await page.getByPlaceholder("Enter document text...").fill("keep this document");
    await page.getByRole("button", { name: "Embed & Compare" }).click();
    await page.getByLabel("Embedder Model").click();
    await page.getByRole("option", { name: "mock-embed-second" }).click();
    await page.evaluate(() =>
      (window as Window & { __resolveEmbed?: () => void }).__resolveEmbed?.()
    );

    await expect(page.getByLabel("Query")).toHaveValue("keep this query");
    await expect(page.getByPlaceholder("Enter document text...")).toHaveValue("keep this document");
    await expect(page.getByText("Similarity Results")).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Embed & Compare" })).toBeEnabled();
  });

  test("changing table discards a deferred data search without clearing its query", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      const originalFetch = window.fetch.bind(window);
      window.fetch = async (input, init) => {
        const url = new URL(typeof input === "string" ? input : input.url, window.location.href);
        if (url.pathname !== "/db/v1/tables/docs/query") return originalFetch(input, init);
        return await new Promise<Response>((resolve) => {
          (window as Window & { __resolveTableQuery?: () => void }).__resolveTableQuery = () =>
            resolve(
              Response.json({
                responses: [
                  {
                    status: 200,
                    took: 1,
                    hits: { hits: [{ _id: "stale", _score: 1, _source: { text: "old" } }] },
                  },
                ],
              })
            );
        });
      };
    });

    await page.goto("/data/playground/embed?table=docs&index=semantic");
    const query = page.getByLabel("Semantic Query");
    await query.fill("keep this semantic query");
    await page.getByRole("button", { name: "Search & Analyze" }).click();
    await page.getByLabel("Table").click();
    await page.getByRole("option", { name: "archive" }).click();
    await page.evaluate(() =>
      (window as Window & { __resolveTableQuery?: () => void }).__resolveTableQuery?.()
    );

    await expect(query).toHaveValue("keep this semantic query");
    await expect(page.getByText("1 hits")).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Search & Analyze" })).toBeEnabled();
  });

  test("chat keeps partial streamed text when stopped", async ({ page }) => {
    await installOpenRetrievalStream(page, "partial chat answer");
    await page.goto("/data/playground/chat?table=docs&index=semantic");
    await page.getByPlaceholder("Ask a question...").fill("What happened?");
    await page.getByRole("button", { name: "Submit" }).click();
    await expect(page.getByText("partial chat answer")).toBeVisible();
    await page.getByRole("button", { name: "Stop" }).click();
    await expect(page.getByText("partial chat answer")).toBeVisible();
    await expect(page.getByRole("button", { name: "Submit" })).toBeVisible();
  });

  test("RAG keeps partial streamed text when stopped", async ({ page }) => {
    await installOpenRetrievalStream(page, "partial RAG answer");
    await page.goto("/data/playground/rag?table=docs&index=semantic");
    await page.getByPlaceholder("Enter your question...").fill("What happened?");
    await page.getByRole("button", { name: "Run Query" }).click();
    await expect(page.getByText("partial RAG answer")).toBeVisible();
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    await expect(page.getByText("partial RAG answer")).toBeVisible();
    await expect(page.getByRole("button", { name: "Run Query" })).toBeEnabled();
  });

  for (const setting of ["result limit", "system prompt"] as const) {
    test(`RAG ${setting} changes cancel the stream and reset the pipeline`, async ({ page }) => {
      await installOpenRetrievalStream(page, "partial old settings answer");
      await page.goto("/data/playground/rag?table=docs&index=semantic");
      await page.getByPlaceholder("Enter your question...").fill("What happened?");
      await page.getByRole("button", { name: "Run Query", exact: true }).click();
      await expect(page.getByText("partial old settings answer")).toBeVisible();
      await page
        .getByRole("button", { name: "Settings", exact: true })
        .and(page.locator('[data-slot="collapsible-trigger"]'))
        .click();
      if (setting === "result limit") {
        await page
          .getByText("Result Limit", { exact: true })
          .locator("..")
          .getByRole("spinbutton")
          .fill("20");
      } else {
        await page.getByPlaceholder("Custom instructions for the generator...").fill("Be concise.");
      }
      await expect(page.getByText("partial old settings answer")).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Run Query", exact: true })).toBeEnabled();
      await expect(page.getByRole("button", { name: "Stop", exact: true })).toHaveCount(0);
      await page.getByRole("tab", { name: "Pipeline", exact: true }).click();
      await expect(page.getByText("Run a query to see the pipeline trace")).toBeVisible();
      await page.getByRole("tab", { name: "Response", exact: true }).click();
      await page.getByRole("button", { name: "Run Query", exact: true }).click();
      await expect(page.getByText("partial old settings answer")).toBeVisible();
      await page.getByRole("button", { name: "Stop", exact: true }).click();
      await expect(page.getByText("partial old settings answer")).toBeVisible();
    });
  }

  test("evaluation cancellation survives switching tables", async ({ page }) => {
    await page.addInitScript(() => {
      const originalFetch = window.fetch.bind(window);
      window.fetch = async (input, init) => {
        const url = new URL(typeof input === "string" ? input : input.url, window.location.href);
        if (url.pathname !== "/db/v1/agents/retrieval") return originalFetch(input, init);
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => reject(new DOMException("Aborted", "AbortError")),
            { once: true }
          );
        });
      };
    });
    await page.goto("/data/playground/evals?table=docs&index=semantic");
    await page.getByRole("button", { name: "Load Sample" }).click();
    await page.getByRole("button", { name: "Run Evals" }).click();
    await expect(page.getByRole("button", { name: "Stop evaluation" })).toBeVisible();
    await page.getByRole("button", { name: "Stop evaluation" }).click();
    await expect(
      page.getByText("Evaluation stopped. Completed results are retained below.")
    ).toBeVisible();

    await page.getByLabel("Table").click();
    await page.getByRole("option", { name: "archive" }).click();
    await expect(page).toHaveURL(/table=archive/);
    await expect(page.getByRole("button", { name: "Run Evals" })).toBeEnabled();
  });
});
