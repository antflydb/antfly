import { expect, test } from "@playwright/test";
import { installAntflyApiMocks } from "./helpers/antfly-api";

test.describe("tables flows", () => {
  test.beforeEach(async ({ page }) => {
    await installAntflyApiMocks(page);
  });

  test("shows the table list and applies a prefix filter", async ({ page }) => {
    await page.goto("/");

    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible();
    await expect(page.getByRole("link", { name: "books" })).toBeVisible();
    await expect(page.getByRole("link", { name: "authors" })).toBeVisible();

    await page.getByLabel("Filter by Prefix").fill("book");
    await page.getByRole("button", { name: "Apply" }).click();

    await expect(page.getByRole("link", { name: "books" })).toBeVisible();
    await expect(page.getByRole("link", { name: "authors" })).toHaveCount(0);
  });

  test("navigates from the tables list into a table details page", async ({ page }) => {
    await page.goto("/");

    await page.getByRole("link", { name: "books" }).click();

    await expect(page).toHaveURL(/\/tables\/books$/);
    await expect(page.getByRole("button", { name: "Create Index" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Vector Indexes" })).toBeVisible();
    await expect(page.getByRole("cell", { name: "title_embedding" })).toBeVisible();
  });

  test("loads an existing table page directly", async ({ page }) => {
    await page.goto("/tables/books");

    await expect(page).toHaveURL(/\/tables\/books$/);
    await expect(page.getByRole("navigation").getByText("books")).toBeVisible();
    await expect(page.getByRole("button", { name: "Create Index" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Full Text Index" })).toBeVisible();
    await expect(page.getByRole("cell", { name: "0" })).toBeVisible();
  });
});
