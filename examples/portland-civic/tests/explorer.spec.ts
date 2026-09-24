import { expect, test } from "@playwright/test";

test("real Antfly search, filters, pagination, record details, citations, and responsive layout", async ({
  page,
  request,
}, testInfo) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const initial = await (await request.get("/api/search")).json();
  expect(initial.permits.length).toBeGreaterThan(0);
  expect(initial.exact).toBe(true);
  await page.goto("/");
  await expect(
    page.getByRole("heading", {
      name: `${initial.total.toLocaleString()} permits`,
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.locator(".permit-card")).toHaveCount(40);
  await expect(
    page.getByRole("button", { name: "Previous results" }),
  ).toBeDisabled();
  await page.getByRole("button", { name: "Next results" }).click();
  await expect(page.locator(".pagination")).toContainText("41–80");
  await page.getByRole("button", { name: "Previous results" }).click();
  await expect(page.locator(".pagination")).toContainText("1–40");
  await page.locator(".permit-card").first().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Open original city record" }),
  ).toHaveAttribute(
    "href",
    /^https:\/\/www\.portlandmaps\.com\/detail\/permit\//,
  );
  await page.getByRole("button", { name: "Close permit details" }).click();
  const neighborhood = initial.permits[0].neighborhood;
  await page
    .getByLabel("Neighborhood", { exact: true })
    .selectOption(neighborhood);
  await expect(page.locator(".loading")).toHaveCount(0);
  const filtered = await (
    await request.get(
      `/api/search?neighborhood=${encodeURIComponent(neighborhood)}`,
    )
  ).json();
  expect(
    filtered.permits.every(
      (p: { neighborhood: string }) => p.neighborhood === neighborhood,
    ),
  ).toBe(true);
  await expect(page.locator(".permit-card")).toHaveCount(
    filtered.permits.length,
  );
  await page.getByRole("button", { name: "Explain these results" }).click();
  await expect(page.locator(".answer-sources a").first()).toBeVisible();
  await expect(page.locator(".citation").first()).toHaveAttribute(
    "href",
    /^https:\/\/www\.portlandmaps\.com\//,
  );
  await page.getByLabel("Neighborhood", { exact: true }).selectOption("");
  await expect(page.locator(".answer")).toHaveCount(0);
  await page.getByLabel("Search permits").fill("zzznomatchingpermitzzzz");
  await page.getByRole("button", { name: "Explore", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "No permits found" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Reset search" }).click();
  await expect(page.locator(".permit-card")).toHaveCount(40);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  // Tiles/fonts are decorative network requests; the result assertions above use real Antfly.
  await expect(page.locator(".map")).toHaveAttribute(
    "data-tiles-ready",
    "true",
  );
  await page.screenshot({
    path: `test-results/portland-${testInfo.project.name}.png`,
    fullPage: true,
  });
  expect(errors).toEqual([]);
});

test("API enforces date and status filters and rejects malformed input", async ({
  request,
}) => {
  const all = await (await request.get("/api/search")).json();
  const p = all.permits.find((p: { created_at?: string }) => p.created_at);
  const day = p.created_at.slice(0, 10);
  const response = await request.get(
    `/api/search?from=${day}&to=${day}&status=${encodeURIComponent(p.status)}`,
  );
  expect(response.ok()).toBe(true);
  const selected = await response.json();
  expect(selected.permits.length).toBeGreaterThan(0);
  expect(
    selected.permits.every(
      (v: { created_at: string; status: string }) =>
        v.created_at.startsWith(day) && v.status === p.status,
    ),
  ).toBe(true);
  expect((await request.get("/api/search?from=2026-02-30")).status()).toBe(400);
  expect((await request.get("/api/search?q=a&q=b")).status()).toBe(400);
});

test("an unavailable backend produces an actionable state", async ({
  page,
}) => {
  await page.route("**/api/search?*", (route) =>
    route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ error: "Antfly unavailable" }),
    }),
  );
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Couldn’t load the permits" }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Try again" })).toBeVisible();
});

test("hybrid retrieval preserves neighborhood filters and labels candidate counts", async ({
  request,
}) => {
  const initial = await (await request.get("/api/search")).json();
  test.skip(!initial.manifest.semantic, "No embedding index configured");
  const neighborhood = initial.permits[0].neighborhood;
  const response = await request.get(
    `/api/search?q=building%20renovation&mode=hybrid&neighborhood=${encodeURIComponent(neighborhood)}`,
  );
  expect(response.ok()).toBe(true);
  const body = await response.json();
  expect(body.scope).toBe("ranked_candidates");
  expect(body.exact).toBe(false);
  expect(body.permits.length).toBeGreaterThan(0);
  expect(
    body.permits.every(
      (p: { neighborhood: string }) => p.neighborhood === neighborhood,
    ),
  ).toBe(true);
  expect(body.neighborhoods).toEqual([]);
});
