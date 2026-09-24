import { expect, test } from "@playwright/test";
test.setTimeout(300_000);
test.use({ actionTimeout: 150_000 });

test("native GLiNER2 entities resolve to canonical Autograph nodes", async ({
  page,
  request,
}, info) => {
  const pipeline = await (await request.get("/api/governance/pipeline")).json();
  expect(pipeline.extraction.model).toBe("antflydb/gliner2-base-v1");
  expect(pipeline.extraction.completed).toBe(pipeline.extraction.total);
  expect(pipeline.autograph_ready).toBe(true);
  const search = await (
    await request.get(
      "/api/governance/search?q=internal%20auditor&document=chapter&mode=keyword",
    )
  ).json();
  const passage = search.hits.find(
    (p: { page: number }) => p.page === 5 || p.page === 6,
  );
  expect(passage).toBeTruthy();
  const result = await (
    await request.get(`/api/governance/autograph?passage=${passage.id}`)
  ).json();
  expect(result.graph.nodes.length).toBeGreaterThan(0);
  for (const node of result.graph.nodes) {
    expect(node.key).toMatch(/^oregon\/2017R1\//);
    expect(node.document.canonical_name).toBeTruthy();
  }
  await page.goto(`/governance?passage=${passage.id}`);
  await page
    .getByRole("button", { name: "Inspect extracted entities" })
    .click();
  await expect(page.getByRole("dialog")).toContainText(
    "Department of Transportation",
  );
  await expect(page.getByRole("dialog")).toContainText(
    "Machine-extracted candidate relationships; unreviewed",
  );
  await page.screenshot({
    path: `test-results/governance-native-graph-${info.project.name}.png`,
    fullPage: true,
  });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("typing a question and pressing Enter opens its cited finding, not raw passage results", async ({
  page,
}) => {
  await page.goto("/governance");
  await page
    .getByRole("button", { name: "Search the record", exact: true })
    .click();
  await expect(page.getByLabel("Search task")).toHaveValue("question");
  await expect(page.getByLabel("Retrieval mode")).toHaveValue("keyword");
  await expect(page.getByLabel("Source filter")).toBeDisabled();
  const input = page.getByLabel("Search legislative evidence");
  await input.fill("When did HB 2017 take effect?");
  await input.press("Enter");
  await expect(page.locator(".gov-brief-heading")).toContainText(
    "CONFLICTING SOURCES",
  );
  await expect(
    page.locator(".gov-brief > .gov-citation").first(),
  ).toContainText("Effective date October 6, 2017");
  await expect(page.locator(".gov-brief > .gov-citation").nth(1)).toContainText(
    "Effective Date: August 6, 2017",
  );
  await expect(page.locator(".gov-results-meta")).toHaveCount(0);
  await expect(page.locator(".gov-brief > details")).not.toHaveAttribute(
    "open",
    "",
  );

  await input.fill("How much HB 2017 money was actually spent?");
  await page
    .getByRole("button", { name: "Build evidence brief", exact: true })
    .click();
  await expect(page.locator(".gov-brief-heading")).toContainText(
    "NOT ESTABLISHED BY THIS PILOT",
  );

  await page.getByLabel("Search task").selectOption("passages");
  await expect(page.locator(".gov-brief")).toHaveCount(0);
  await page.getByLabel("Source filter").selectOption("chapter");
  await input.fill("internal auditor");
  await input.press("Enter");
  await expect(page.locator(".gov-results-meta")).toContainText(
    "ranked passages",
  );
  await expect(page.locator(".gov-citation").first()).toContainText(
    "Chapter 750",
  );
  await expect(page.locator(".gov-brief")).toHaveCount(0);
});

test("decision timeline, contradictory evidence, source inspection and mobile layout", async ({
  page,
}, info) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto("/governance");
  await expect(
    page.getByRole("heading", { name: "The path to enactment" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "All 20 actions" }).click();
  await expect(page.locator(".gov-timeline > li")).toHaveCount(20);
  await page.getByRole("button", { name: "Show milestones" }).click();
  await page.screenshot({
    path: `test-results/governance-${info.project.name}.png`,
    fullPage: true,
  });
  await page
    .getByRole("button", { name: "When did HB 2017 take effect?", exact: true })
    .click();
  await expect(page.locator(".gov-brief-heading")).toContainText(
    "CONFLICTING SOURCES",
  );
  await expect(page.locator(".gov-brief-heading")).toContainText(
    "October 6, 2017",
  );
  await expect(
    page.getByRole("heading", { name: "Contrary evidence", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: `test-results/governance-evidence-${info.project.name}.png`,
    fullPage: true,
  });
  await page.locator(".gov-brief > .gov-citation").first().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.locator(".gov-page-text mark")).toContainText(
    "Effective date October 6, 2017",
  );
  await expect(
    page.getByRole("link", { name: "Preserved PDF", exact: true }),
  ).toHaveAttribute("href", /\.pdf#page=98$/);
  const deepLink = page.url();
  await page.getByRole("button", { name: "Close source inspector" }).click();
  await page.goto(deepLink);
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  expect(errors).toEqual([]);
});

test("real Antfly search, source filters and evidence graph", async ({
  page,
  request,
}, info) => {
  const manifest = await (await request.get("/api/governance/manifest")).json();
  const modes = manifest.semantic_ready ? ["keyword", "hybrid"] : ["keyword"];
  for (const mode of modes) {
    const response = await request.get(
      `/api/governance/search?q=internal%20auditor&document=chapter&mode=${mode}`,
    );
    expect(response.ok()).toBe(true);
    const result = await response.json();
    expect(result.hits.length).toBeGreaterThan(0);
    expect(
      result.hits.every(
        (p: { document_id: string }) => p.document_id === "chapter",
      ),
    ).toBe(true);
    expect(
      result.hits.some((p: { page: number }) => [5, 6].includes(p.page)),
    ).toBe(true);
  }
  const graph = await (
    await request.get("/api/governance/graph?node=claim-effective")
  ).json();
  expect(
    graph.graph.nodes.some(
      (n: { document: { document_id?: string } }) =>
        n.document.document_id === "chapter",
    ),
  ).toBe(true);
  expect(
    graph.graph.nodes.some(
      (n: { document: { document_id?: string } }) =>
        n.document.document_id === "summary",
    ),
  ).toBe(true);
  expect(
    graph.graph.nodes.some((n: { path_edges: { type: string }[] }) =>
      n.path_edges.some((e) => e.type === "contrary"),
    ),
  ).toBe(true);
  await page.goto("/governance");
  await page.getByRole("button", { name: "Connections", exact: true }).click();
  await expect(page.locator(".gov-graph-nodes > button")).toHaveCount(5);
  await page.screenshot({
    path: `test-results/governance-graph-${info.project.name}.png`,
    fullPage: true,
  });
  await page
    .locator(".gov-graph-nodes > button")
    .filter({ hasText: "p. 435" })
    .click();
  await expect(page.locator(".gov-page-text mark")).toContainText(
    "August 6, 2017",
  );
});

test("historical version comparison, explicit abstention and source ledger", async ({
  page,
}) => {
  await page.goto("/governance");
  await page
    .getByRole("button", { name: "Compare versions", exact: true })
    .click();
  await expect(page.locator(".gov-compare-pages article")).toHaveCount(2);
  await expect(
    page.locator(".gov-compare-pages article").first(),
  ).toContainText("September 15, 2019");
  await page.getByLabel("right version").selectOption("chapter");
  await page.getByLabel("right page").selectOption("98");
  await expect(page.locator(".gov-compare-pages article").last()).toContainText(
    "Effective date October 6, 2017",
  );
  await page
    .getByText("Show line-by-line differences", { exact: false })
    .click();
  await expect(page.locator(".gov-diff .added").first()).toBeVisible();
  await page
    .getByRole("button", { name: "Open questions", exact: false })
    .click();
  await page
    .locator(".gov-gap-grid article")
    .first()
    .getByRole("button")
    .click();
  await expect(page.locator(".gov-brief-heading")).toContainText(
    "NOT ESTABLISHED BY THIS PILOT",
  );
  await expect(page.locator(".gov-brief-heading")).toContainText(
    "Generated by",
  );
  await page
    .getByRole("button", { name: "Source ledger", exact: true })
    .click();
  await expect(page.locator(".gov-ledger tbody tr")).toHaveCount(9);
  await expect(page.locator(".gov-ledger-summary")).toContainText(
    "0 human-reviewed documents",
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("API rejects malformed filters and refuses unknown files and graph nodes", async ({
  request,
}) => {
  expect((await request.get("/api/governance/search?q=a&q=b")).status()).toBe(
    400,
  );
  expect(
    (await request.get("/api/governance/search?mode=made-up")).status(),
  ).toBe(400);
  expect(
    (await request.get("/api/governance/search?document=unknown")).status(),
  ).toBe(400);
  expect(
    (await request.get("/api/governance/graph?node=unknown")).status(),
  ).toBe(400);
  expect(
    (await request.get("/api/governance/compare?right_page=9999")).status(),
  ).toBe(400);
  expect(
    (await request.get("/api/governance/sources/not-a-source.pdf")).status(),
  ).toBe(404);
  expect((await request.get("/api/governance/passage/unknown")).status()).toBe(
    404,
  );
  const response = await request.post("/api/governance/brief", {
    data: { question: "Who secretly negotiated the bill?" },
  });
  expect(response.ok()).toBe(true);
  expect((await response.json()).status).toBe("not-established");
});
