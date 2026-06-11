import { Button } from "@antfly/design-system";
import { useNavigate } from "react-router-dom";
import KnowledgeGraphPlaygroundPage from "../KnowledgeGraphPlaygroundPage";
import { LabSection } from "./LabPage";

export default function GraphLabPage() {
  const navigate = useNavigate();

  return (
    <LabSection
      title="Graph Lab"
      description="Extract entities and relationships, then promote graph setup into a table workflow."
      promote={
        <Button variant="outline" onClick={() => navigate("/tables")}>
          Choose table
        </Button>
      }
    >
      <KnowledgeGraphPlaygroundPage />
    </LabSection>
  );
}
