import { Button } from "@antfly/design-system";
import { useNavigate } from "react-router-dom";
import AntflyEmbeddingPlaygroundPage from "../AntflyEmbeddingPlaygroundPage";
import AntflyRerankingPlaygroundPage from "../AntflyRerankingPlaygroundPage";
import { LabSection } from "./LabPage";

export default function SearchLabPage() {
  const navigate = useNavigate();
  return (
    <LabSection
      title="Search Lab"
      description="Compare semantic search and reranking on a selected table."
      promote={
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => navigate("/tables")}>
            Choose table
          </Button>
          <Button variant="outline" onClick={() => navigate("/tables?section=faceted")}>
            Export Search UI
          </Button>
        </div>
      }
    >
      <AntflyEmbeddingPlaygroundPage />
      <AntflyRerankingPlaygroundPage />
    </LabSection>
  );
}
