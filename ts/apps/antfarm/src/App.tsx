import { SidebarInset, SidebarProvider } from "@antfly/design-system";
import { useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { ApiConfigProvider } from "@/components/api-config-provider";
import { AuthProvider } from "@/components/auth-provider";
import { CommandPaletteProvider } from "@/components/command-palette-provider";
import { ConnectionStatusBanner } from "@/components/connection-status-banner";
import { ErrorBoundary } from "@/components/error-boundary";
import { GeneratorPreferenceProvider } from "@/components/generator-preference-provider";
import { PrivateRoute } from "@/components/private-route";
import { AppSidebar } from "@/components/sidebar";
import { TableProvider } from "@/components/table-provider";
import { WorkspaceHeader } from "@/components/workspace-header";
import { ThemeProvider } from "@/hooks/use-theme";
import { isExternalAuthMode } from "@/runtime-config";
import ClusterPage from "./pages/ClusterPage";
import CreateTablePage from "./pages/CreateTablePage";
import HomePage from "./pages/HomePage";
import { LoginPage } from "./pages/LoginPage";
import AgentLabPage from "./pages/lab/AgentLabPage";
import GraphLabPage from "./pages/lab/GraphLabPage";
import IngestLabPage from "./pages/lab/IngestLabPage";
import LabPage from "./pages/lab/LabPage";
import ModelLabPage from "./pages/lab/ModelLabPage";
import SearchLabPage from "./pages/lab/SearchLabPage";
import ModelsPage from "./pages/ModelsPage";
import { SecretsPage } from "./pages/SecretsPage";
import TableDetailsPage from "./pages/TableDetailsPage";
import TablesListPage from "./pages/TablesListPage";
import { UsersPage } from "./pages/UsersPage";

function RedirectPreserveSearch({ to }: { to: string }) {
  const location = useLocation();
  return <Navigate to={`${to}${location.search}`} replace />;
}

function AppContent() {
  const [currentSection, setCurrentSection] = useState("indexes");
  const showLocalAdminRoutes = !isExternalAuthMode();

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <PrivateRoute>
            <SidebarProvider className="af-dashboard">
              <AppSidebar currentSection={currentSection} onSectionChange={setCurrentSection} />
              <SidebarInset>
                <WorkspaceHeader />
                <ConnectionStatusBanner />
                <div className="af-workspace-content flex-1 px-6 pt-4 pb-6">
                  <Routes>
                    <Route path="/" element={<HomePage />} />
                    <Route path="/tables" element={<TablesListPage />} />
                    <Route path="/create" element={<CreateTablePage />} />
                    <Route
                      path="/tables/:tableName"
                      element={<TableDetailsPage currentSection={currentSection} />}
                    />
                    {showLocalAdminRoutes && <Route path="/users" element={<UsersPage />} />}
                    {showLocalAdminRoutes && <Route path="/secrets" element={<SecretsPage />} />}
                    <Route path="/observe" element={<ClusterPage />} />
                    <Route path="/cluster" element={<Navigate to="/observe" replace />} />
                    <Route path="/lab" element={<LabPage />} />
                    <Route path="/lab/search" element={<SearchLabPage />} />
                    <Route path="/lab/ingest" element={<IngestLabPage />} />
                    <Route path="/lab/model" element={<ModelLabPage />} />
                    <Route path="/lab/agent" element={<AgentLabPage />} />
                    <Route path="/lab/graph" element={<GraphLabPage />} />
                    <Route
                      path="/data/playground/evals"
                      element={<RedirectPreserveSearch to="/lab/agent" />}
                    />
                    <Route
                      path="/data/playground/rag"
                      element={<RedirectPreserveSearch to="/lab/agent" />}
                    />
                    <Route
                      path="/data/playground/chat"
                      element={<RedirectPreserveSearch to="/lab/agent" />}
                    />
                    <Route
                      path="/data/playground/embed"
                      element={<RedirectPreserveSearch to="/lab/search" />}
                    />
                    <Route
                      path="/data/playground/rerank"
                      element={<RedirectPreserveSearch to="/lab/search" />}
                    />
                    <Route
                      path="/data/playground/chunk"
                      element={<RedirectPreserveSearch to="/lab/ingest" />}
                    />
                    <Route path="/inference/models" element={<ModelsPage />} />
                    <Route
                      path="/inference/playground/chunk"
                      element={<RedirectPreserveSearch to="/lab/ingest" />}
                    />
                    <Route
                      path="/inference/playground/extract"
                      element={<RedirectPreserveSearch to="/lab/ingest" />}
                    />
                    <Route
                      path="/inference/playground/rewrite"
                      element={<RedirectPreserveSearch to="/lab/model" />}
                    />
                    <Route
                      path="/inference/playground/rerank"
                      element={<RedirectPreserveSearch to="/lab/model" />}
                    />
                    <Route
                      path="/inference/playground/kg"
                      element={<RedirectPreserveSearch to="/lab/graph" />}
                    />
                    <Route
                      path="/inference/playground/embed"
                      element={<RedirectPreserveSearch to="/lab/model" />}
                    />
                    <Route
                      path="/inference/playground/read"
                      element={<RedirectPreserveSearch to="/lab/ingest" />}
                    />
                    <Route
                      path="/inference/playground/transcribe"
                      element={<RedirectPreserveSearch to="/lab/ingest" />}
                    />
                    <Route path="*" element={<Navigate to="/" replace />} />
                  </Routes>
                </div>
              </SidebarInset>
            </SidebarProvider>
          </PrivateRoute>
        }
      />
    </Routes>
  );
}

function App() {
  return (
    <ThemeProvider>
      <ErrorBoundary>
        <ApiConfigProvider>
          <AuthProvider>
            <GeneratorPreferenceProvider>
              <CommandPaletteProvider>
                <TableProvider>
                  <AppContent />
                </TableProvider>
              </CommandPaletteProvider>
            </GeneratorPreferenceProvider>
          </AuthProvider>
        </ApiConfigProvider>
      </ErrorBoundary>
    </ThemeProvider>
  );
}

export default App;
