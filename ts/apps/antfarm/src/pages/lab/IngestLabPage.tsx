import { Button } from "@antfly/design-system";
import { useNavigate } from "react-router-dom";
import AntflyChunkingPlaygroundPage from "../AntflyChunkingPlaygroundPage";
import ChunkingPlaygroundPage from "../ChunkingPlaygroundPage";
import ExtractionPlaygroundPage from "../ExtractionPlaygroundPage";
import ReaderPlaygroundPage from "../ReaderPlaygroundPage";
import TranscribePlaygroundPage from "../TranscribePlaygroundPage";
import { LabSection } from "./LabPage";

export default function IngestLabPage() {
  const navigate = useNavigate();
  return (
    <LabSection
      title="Ingest Lab"
      description="Preview reading, chunking, extraction, and transcription before upload."
      promote={
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => navigate("/tables")}>
            Upload to table
          </Button>
        </div>
      }
    >
      <ReaderPlaygroundPage />
      <AntflyChunkingPlaygroundPage />
      <ChunkingPlaygroundPage />
      <ExtractionPlaygroundPage />
      <TranscribePlaygroundPage />
    </LabSection>
  );
}
