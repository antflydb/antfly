import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PrivateRoute } from "./private-route";

const useAuthMock = vi.fn();

vi.mock("../hooks/use-auth", () => ({
  useAuth: () => useAuthMock(),
}));

function renderRoute(element: React.ReactNode) {
  return render(
    <MemoryRouter initialEntries={["/protected"]}>
      <Routes>
        <Route path="/login" element={<div>Login page</div>} />
        <Route path="/protected" element={element} />
      </Routes>
    </MemoryRouter>
  );
}

describe("PrivateRoute", () => {
  beforeEach(() => {
    useAuthMock.mockReset();
  });

  afterEach(() => {
    cleanup();
  });

  it("shows a loading state while auth is initializing", () => {
    useAuthMock.mockReturnValue({
      isAuthenticated: false,
      isLoading: true,
      hasPermission: vi.fn(),
      authEnabled: null,
    });

    renderRoute(
      <PrivateRoute>
        <div>Protected content</div>
      </PrivateRoute>
    );

    expect(screen.getByText("Loading...")).toBeTruthy();
  });

  it("renders children when auth is disabled", () => {
    useAuthMock.mockReturnValue({
      isAuthenticated: false,
      isLoading: false,
      hasPermission: vi.fn(),
      authEnabled: false,
    });

    renderRoute(
      <PrivateRoute>
        <div>Protected content</div>
      </PrivateRoute>
    );

    expect(screen.getByText("Protected content")).toBeTruthy();
  });

  it("redirects unauthenticated users to login", () => {
    useAuthMock.mockReturnValue({
      isAuthenticated: false,
      isLoading: false,
      hasPermission: vi.fn(),
      authEnabled: true,
    });

    renderRoute(
      <PrivateRoute>
        <div>Protected content</div>
      </PrivateRoute>
    );

    expect(screen.getByText("Login page")).toBeTruthy();
  });

  it("shows access denied when a required permission is missing", () => {
    useAuthMock.mockReturnValue({
      isAuthenticated: true,
      isLoading: false,
      hasPermission: vi.fn(() => false),
      authEnabled: true,
    });

    renderRoute(
      <PrivateRoute
        requiredPermission={{
          resource: "users",
          resourceType: "user",
          permissionType: "admin",
        }}
      >
        <div>Protected content</div>
      </PrivateRoute>
    );

    expect(screen.getByText("Access Denied")).toBeTruthy();
  });

  it("renders children when the required permission is present", () => {
    useAuthMock.mockReturnValue({
      isAuthenticated: true,
      isLoading: false,
      hasPermission: vi.fn(() => true),
      authEnabled: true,
    });

    renderRoute(
      <PrivateRoute
        requiredPermission={{
          resource: "users",
          resourceType: "user",
          permissionType: "admin",
        }}
      >
        <div>Protected content</div>
      </PrivateRoute>
    );

    expect(screen.getAllByText("Protected content")).toHaveLength(1);
  });
});
