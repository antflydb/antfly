import { Button } from "@antfly/design-system";
import { useNavigate } from "react-router-dom";
import EmbeddingPlaygroundPage from "../EmbeddingPlaygroundPage";
import RewritingPlaygroundPage from "../QuestionPlaygroundPage";
import RerankingPlaygroundPage from "../RerankingPlaygroundPage";
import { LabSection } from "./LabPage";

export default function ModelLabPage() {
  const navigate = useNavigate();
  return (
    <LabSection
      title="Model Lab"
      description="Test generators, embedders, and rerankers with small inputs."
      promote={
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => navigate("/inference/models")}>
            Manage models
          </Button>
          <Button variant="outline" onClick={() => navigate("/tables")}>
            Use in index
          </Button>
        </div>
      }
    >
      <EmbeddingPlaygroundPage />
      <RerankingPlaygroundPage />
      <RewritingPlaygroundPage />
    </LabSection>
  );
}
