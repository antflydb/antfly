import { useEffect, useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { ApiConfigProvider } from "@/components/api-config-provider";
import { AuthProvider } from "@/components/auth-provider";
import { CommandPaletteProvider } from "@/components/command-palette-provider";
import { ConnectionStatusBanner } from "@/components/connection-status-banner";
import { ContentWidthProvider, useContentWidth } from "@/components/content-width-provider";
import { ErrorBoundary } from "@/components/error-boundary";
import { PrivateRoute } from "@/components/private-route";
import { AppSidebar } from "@/components/sidebar";
import { TableProvider } from "@/components/table-provider";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { WorkspaceHeader } from "@/components/workspace-header";
import {
  defaultProduct,
  getDefaultRoute,
  isProductEnabled,
  type ProductId,
  productForPath,
} from "@/config/products";
import { ThemeProvider } from "@/hooks/use-theme";
import { cn } from "@/lib/utils";
import AntflyChunkingPlaygroundPage from "./pages/AntflyChunkingPlaygroundPage";
import AntflyEmbeddingPlaygroundPage from "./pages/AntflyEmbeddingPlaygroundPage";
import AntflyRerankingPlaygroundPage from "./pages/AntflyRerankingPlaygroundPage";
import ChatPlaygroundPage from "./pages/ChatPlaygroundPage";
import ChunkingPlaygroundPage from "./pages/ChunkingPlaygroundPage";
import ClusterPage from "./pages/ClusterPage";
import CreateTablePage from "./pages/CreateTablePage";
import EmbeddingPlaygroundPage from "./pages/EmbeddingPlaygroundPage";
import EvalsPlaygroundPage from "./pages/EvalsPlaygroundPage";
import KnowledgeGraphPlaygroundPage from "./pages/KnowledgeGraphPlaygroundPage";
import { LoginPage } from "./pages/LoginPage";
import ModelsPage from "./pages/ModelsPage";
import RecognizePlaygroundPage from "./pages/NERPlaygroundPage";
import RewritingPlaygroundPage from "./pages/QuestionPlaygroundPage";
import RagPlaygroundPage from "./pages/RagPlaygroundPage";
import ReaderPlaygroundPage from "./pages/ReaderPlaygroundPage";
import RerankingPlaygroundPage from "./pages/RerankingPlaygroundPage";
import { SecretsPage } from "./pages/SecretsPage";
import TableDetailsPage from "./pages/TableDetailsPage";
import TablesListPage from "./pages/TablesListPage";
import TranscribePlaygroundPage from "./pages/TranscribePlaygroundPage";
import { UsersPage } from "./pages/UsersPage";

function AppContent() {
  const [currentSection, setCurrentSection] = useState("indexes");
  const [currentProduct, setCurrentProduct] = useState<ProductId>(defaultProduct);
  const { contentWidth } = useContentWidth();
  const location = useLocation();

  // Sync currentProduct with the current route so direct navigation
  // (bookmarks, refresh, shared links) shows the correct sidebar.
  useEffect(() => {
    const product = productForPath(location.pathname);
    if (product && isProductEnabled(product)) {
      setCurrentProduct(product);
    }
  }, [location.pathname]);

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <PrivateRoute>
            <SidebarProvider>
              <AppSidebar
                currentSection={currentSection}
                onSectionChange={setCurrentSection}
                currentProduct={currentProduct}
                onProductChange={setCurrentProduct}
              />
              <SidebarInset>
                <WorkspaceHeader />
                <ConnectionStatusBanner />
                <div
                  className={cn(
                    "flex-1 p-4 transition-all",
                    contentWidth === "restricted" ? "container mx-auto max-w-7xl" : "w-full"
                  )}
                >
                  <Routes>
                    {/* Antfly routes */}
                    {isProductEnabled("antfly") && (
                      <>
                        <Route path="/" element={<TablesListPage />} />
                        <Route path="/create" element={<CreateTablePage />} />
                        <Route
                          path="/tables/:tableName"
                          element={
                            <TableDetailsPage
                              currentSection={currentSection}
                            />
                          }
                        />
                        <Route path="/users" element={<UsersPage />} />
                        <Route path="/secrets" element={<SecretsPage />} />
                        <Route path="/cluster" element={<ClusterPage />} />
                        <Route path="/playground/evals" element={<EvalsPlaygroundPage />} />
                        <Route path="/playground/rag" element={<RagPlaygroundPage />} />
                        <Route path="/playground/chat" element={<ChatPlaygroundPage />} />
                        <Route
                          path="/playground/embedding"
                          element={<AntflyEmbeddingPlaygroundPage />}
                        />
                        <Route
                          path="/playground/reranking"
                          element={<AntflyRerankingPlaygroundPage />}
                        />
                        <Route
                          path="/playground/chunking"
                          element={<AntflyChunkingPlaygroundPage />}
                        />
                      </>
                    )}

                    {/* Termite routes */}
                    {isProductEnabled("termite") && (
                      <>
                        <Route path="/models" element={<ModelsPage />} />
                        <Route path="/playground/chunk" element={<ChunkingPlaygroundPage />} />
                        <Route path="/playground/recognize" element={<RecognizePlaygroundPage />} />
                        <Route path="/playground/rewrite" element={<RewritingPlaygroundPage />} />
                        <Route path="/playground/rerank" element={<RerankingPlaygroundPage />} />
                        <Route path="/playground/kg" element={<KnowledgeGraphPlaygroundPage />} />
                        <Route path="/playground/embed" element={<EmbeddingPlaygroundPage />} />
                        <Route path="/playground/read" element={<ReaderPlaygroundPage />} />
                        <Route
                          path="/playground/transcribe"
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
            <ContentWidthProvider>
              <CommandPaletteProvider>
                <TableProvider>
                  <AppContent />
                </TableProvider>
              </CommandPaletteProvider>
            </ContentWidthProvider>
          </AuthProvider>
        </ApiConfigProvider>
      </ErrorBoundary>
    </ThemeProvider>
  );
}

export default App;
