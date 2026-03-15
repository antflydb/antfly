import { expect, test } from "@playwright/test";

// =============================================================================
// Working Tests - These verify basic functionality without auth
// =============================================================================

test("loads app and shows dashboard or login", async ({ page }) => {
  await page.goto("/");

  // App should load - either dashboard (auth disabled) or login page (auth enabled)
  await expect(page).toHaveTitle(/Antfly/);

  // Wait for either login page or dashboard to be visible
  // The app may redirect to /login if auth is enabled (or backend unreachable)
  // Note: CardTitle renders as <div> not <h1>/<h2>, so use text matching for login
  const loginTitle = page.getByText("Sign in to Antfly");
  const dashboardHeading = page.getByRole("heading", { name: "Tables" });

  // Wait for either element to appear (handles async redirect)
  await expect(loginTitle.or(dashboardHeading)).toBeVisible({ timeout: 10000 });

  // Verify additional elements based on which page we're on
  if (await loginTitle.isVisible()) {
    await expect(page.getByLabel("Username")).toBeVisible();
    await expect(page.getByLabel("Password")).toBeVisible();
  }
});

test("can navigate to tables page", async ({ page }) => {
  await page.goto("/");

  // Wait for app to load
  await expect(page).toHaveTitle(/Antfly/);

  // Wait for either login page or dashboard to be visible
  const loginTitle = page.getByText("Sign in to Antfly");
  const dashboardHeading = page.getByRole("heading", { name: "Tables" });
  await expect(loginTitle.or(dashboardHeading)).toBeVisible({ timeout: 10000 });

  // If we're on login page (no backend or auth enabled), skip this test
  if (await loginTitle.isVisible()) {
    test.skip();
    return;
  }

  // Should see tables navigation
  await expect(page.getByRole("link", { name: "Tables" })).toBeVisible();

  // Click tables and verify we see the table list
  await page.getByRole("link", { name: "Tables" }).click();
  await expect(page.getByText("Filter Mode")).toBeVisible();
});

// =============================================================================
// Skipped Tests - Auth-specific tests (React 19 + Playwright compatibility)
// =============================================================================
// KNOWN ISSUE: React 19.2.0 + Playwright Compatibility
// These tests are currently skipped due to browser crashes when testing React 19.2.0 with Playwright.
// The authentication system works correctly (verified via diagnostic tests), but Playwright's
// headless browsers crash after React renders the page.
//
// Diagnostic output shows:
// - AuthProvider initializes correctly
// - PrivateRoute redirects to /login successfully
// - LoginPage renders
// - Browser crashes when Playwright tries to interact further
//
// Possible solutions:
// 1. Downgrade to React 18.x (most reliable)
// 2. Update Playwright when React 19 support stabilizes
// 3. Use real browser testing instead of headless

test.skip("shows login page when not authenticated", async ({ page }) => {
  await page.goto("/");

  // Wait directly for the login page title to appear (handles both redirect and render)
  // Use a longer timeout to account for the redirect + render time
  // Note: CardTitle renders as <div> not <h1>/<h2>, so use text matching
  await expect(page.getByText("Sign in to Antfly")).toBeVisible({
    timeout: 15000,
  });

  // Login form should be present
  await expect(page.getByLabel("Username")).toBeVisible();
  await expect(page.getByLabel("Password")).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();

  // Should show default credentials hint
  await expect(page.getByText("Default credentials: admin / admin")).toBeVisible();
});

test.skip("login form has proper HTML5 validation", async ({ page }) => {
  await page.goto("/login");

  // Wait for the submit button to be enabled (not in loading state)
  const submitButton = page.getByRole("button", { name: "Sign in" });
  await expect(submitButton).toBeEnabled({ timeout: 15000 });

  // Try to submit empty form - HTML5 validation should prevent it
  await submitButton.click();

  // Should still be on login page (HTML5 validation prevented submission)
  await expect(page).toHaveURL(/\/login$/);

  // Check that username field shows as invalid (HTML5 validation)
  const usernameInput = page.getByLabel("Username");
  await expect(usernameInput).toBeFocused(); // Browser focuses first invalid field
});
