import { SidebarInset, SidebarProvider } from "@antfly/design-system";
import { useEffect, useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { ApiConfigProvider } from "@/components/api-config-provider";
import { AppHeader } from "@/components/app-header";
import { AuthProvider } from "@/components/auth-provider";
import { CommandPaletteProvider } from "@/components/command-palette-provider";
import { ConnectionStatusBanner } from "@/components/connection-status-banner";
import { ErrorBoundary } from "@/components/error-boundary";
import { GeneratorPreferenceProvider } from "@/components/generator-preference-provider";
import { PrivateRoute } from "@/components/private-route";
import { AppSidebar } from "@/components/sidebar";
import { TableProvider } from "@/components/table-provider";
import { getDefaultRoute, isProductEnabled } from "@/config/products";
import { useTable } from "@/hooks/use-table";
import { ThemeProvider } from "@/hooks/use-theme";
import { isExternalAuthMode } from "@/runtime-config";
import AntflyChunkingPlaygroundPage from "./pages/AntflyChunkingPlaygroundPage";
import AntflyEmbeddingPlaygroundPage from "./pages/AntflyEmbeddingPlaygroundPage";
import AntflyRerankingPlaygroundPage from "./pages/AntflyRerankingPlaygroundPage";
import ChatPlaygroundPage from "./pages/ChatPlaygroundPage";
import ChunkingPlaygroundPage from "./pages/ChunkingPlaygroundPage";
import ClusterPage from "./pages/ClusterPage";
import ConnectionsPage from "./pages/ConnectionsPage";
import CreateTablePage from "./pages/CreateTablePage";
import EmbeddingPlaygroundPage from "./pages/EmbeddingPlaygroundPage";
import EvalsPlaygroundPage from "./pages/EvalsPlaygroundPage";
import ExtractionPlaygroundPage from "./pages/ExtractionPlaygroundPage";
import KnowledgeGraphPlaygroundPage from "./pages/KnowledgeGraphPlaygroundPage";
import { LoginPage } from "./pages/LoginPage";
import ModelsPage from "./pages/ModelsPage";
import RewritingPlaygroundPage from "./pages/QuestionPlaygroundPage";
import RagPlaygroundPage from "./pages/RagPlaygroundPage";
import ReaderPlaygroundPage from "./pages/ReaderPlaygroundPage";
import RerankingPlaygroundPage from "./pages/RerankingPlaygroundPage";
import { SecretsPage } from "./pages/SecretsPage";
import SqlWorkbenchPage from "./pages/SqlWorkbenchPage";
import TableDetailsPage from "./pages/TableDetailsPage";
import TablesListPage from "./pages/TablesListPage";
import TranscribePlaygroundPage from "./pages/TranscribePlaygroundPage";
import { UsersPage } from "./pages/UsersPage";

const tableSections = new Set([
  "overview",
  "indexes",
  "schema",
  "semantic",
  "graph",
  "faceted",
  "bulk",
  "document-builder",
  "artifacts",
  "reprocess",
]);

function TableSectionRedirect({ section, fallback = "/" }: { section: string; fallback?: string }) {
  const { selectedTable, tables, isLoadingTables } = useTable();
  const tableName = selectedTable || tables[0]?.name;

  if (tableName) {
    return (
      <Navigate
        to={`/tables/${encodeURIComponent(tableName)}?section=${encodeURIComponent(section)}`}
        replace
      />
    );
  }

  if (isLoadingTables) {
    return <div className="py-10 text-sm text-muted-foreground">Loading tables...</div>;
  }

  if (!tableName) {
    return <Navigate to={fallback} replace />;
  }

  return null;
}

function SelectedTableRoute({ route }: { route: string }) {
  const { selectedTable, tables, isLoadingTables } = useTable();
  const tableName = selectedTable || tables[0]?.name;

  if (!tableName && isLoadingTables) {
    return <div className="py-10 text-sm text-muted-foreground">Loading tables...</div>;
  }

  const suffix = tableName ? `?table=${encodeURIComponent(tableName)}` : "";
  return <Navigate to={`${route}${suffix}`} replace />;
}

function AppContent() {
  const [currentSection, setCurrentSection] = useState("overview");
  const location = useLocation();
  const showLocalAdminRoutes = !isExternalAuthMode();

  useEffect(() => {
    const isTableRoute = location.pathname.startsWith("/tables/");
    if (!isTableRoute) return;

    const section = new URLSearchParams(location.search).get("section");
    if (section && tableSections.has(section)) {
      setCurrentSection(section);
    } else {
      setCurrentSection("overview");
    }
  }, [location.pathname, location.search]);

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
                <AppHeader />
                <ConnectionStatusBanner />
                <div className="af-app-content flex-1 px-6 pt-4 pb-6">
                  <Routes>
                    {/* Antfly routes */}
                    {isProductEnabled("antfly") && (
                      <>
                        <Route path="/" element={<TablesListPage />} />
                        <Route path="/create" element={<CreateTablePage />} />
                        <Route
                          path="/tables/:tableName"
                          element={<TableDetailsPage currentSection={currentSection} />}
                        />
                        <Route
                          path="/retrieval"
                          element={<TableSectionRedirect section="semantic" />}
                        />
                        <Route
                          path="/retrieval/search"
                          element={<TableSectionRedirect section="semantic" />}
                        />
                        <Route
                          path="/retrieval/ask"
                          element={<SelectedTableRoute route="/data/playground/chat" />}
                        />
                        <Route
                          path="/retrieval/evaluate"
                          element={<SelectedTableRoute route="/data/playground/evals" />}
                        />
                        <Route
                          path="/retrieval/graph"
                          element={<TableSectionRedirect section="graph" />}
                        />
                        <Route path="/ingest" element={<TableSectionRedirect section="bulk" />} />
                        <Route
                          path="/ingest/upload"
                          element={<TableSectionRedirect section="bulk" />}
                        />
                        <Route
                          path="/ingest/manual"
                          element={<TableSectionRedirect section="document-builder" />}
                        />
                        <Route
                          path="/ingest/artifacts"
                          element={<TableSectionRedirect section="artifacts" />}
                        />
                        <Route
                          path="/ingest/reprocess"
                          element={<TableSectionRedirect section="reprocess" />}
                        />
                        {showLocalAdminRoutes && (
                          <Route
                            path="/users"
                            element={
                              <PrivateRoute
                                requiredPermission={{
                                  resource: "*",
                                  resourceType: "*",
                                  permissionType: "admin",
                                }}
                              >
                                <UsersPage />
                              </PrivateRoute>
                            }
                          />
                        )}
                        {showLocalAdminRoutes && (
                          <Route
                            path="/secrets"
                            element={
                              <PrivateRoute
                                requiredPermission={{
                                  resource: "*",
                                  resourceType: "*",
                                  permissionType: "admin",
                                }}
                              >
                                <SecretsPage />
                              </PrivateRoute>
                            }
                          />
                        )}
                        <Route path="/cluster" element={<ClusterPage />} />
                        <Route path="/connections" element={<ConnectionsPage />} />
                        <Route path="/sql" element={<SqlWorkbenchPage />} />
                        <Route path="/data/playground/evals" element={<EvalsPlaygroundPage />} />
                        <Route path="/data/playground/rag" element={<RagPlaygroundPage />} />
                        <Route path="/data/playground/chat" element={<ChatPlaygroundPage />} />
                        <Route
                          path="/data/playground/embed"
                          element={<AntflyEmbeddingPlaygroundPage />}
                        />
                        <Route
                          path="/data/playground/rerank"
                          element={<AntflyRerankingPlaygroundPage />}
                        />
                        <Route
                          path="/data/playground/chunk"
                          element={<AntflyChunkingPlaygroundPage />}
                        />
                      </>
                    )}

                    {/* Inference routes */}
                    {isProductEnabled("inference") && (
                      <>
                        <Route path="/inference/models" element={<ModelsPage />} />
                        <Route
                          path="/inference/playground/chunk"
                          element={<ChunkingPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/extract"
                          element={<ExtractionPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/rewrite"
                          element={<RewritingPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/rerank"
                          element={<RerankingPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/kg"
                          element={<KnowledgeGraphPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/embed"
                          element={<EmbeddingPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/read"
                          element={<ReaderPlaygroundPage />}
                        />
                        <Route
                          path="/inference/playground/transcribe"
                          element={<TranscribePlaygroundPage />}
                        />
                      </>
                    )}

                    {/* Default redirect based on enabled products */}
                    <Route path="*" element={<Navigate to={getDefaultRoute()} replace />} />
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
              <TableProvider>
                <CommandPaletteProvider>
                  <AppContent />
                </CommandPaletteProvider>
              </TableProvider>
            </GeneratorPreferenceProvider>
          </AuthProvider>
        </ApiConfigProvider>
      </ErrorBoundary>
    </ThemeProvider>
  );
}

export default App;
