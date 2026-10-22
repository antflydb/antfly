import { Button } from "@antfly/design-system";
import { ArrowLeft, CircleSlash2 } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { AIWorkflowEmpty, AIWorkflowHeader, AIWorkflowPage } from "@/components/ai-workflow";

export default function RewritingUnavailablePage() {
  const navigate = useNavigate();
  return (
    <AIWorkflowPage>
      <AIWorkflowHeader
        title="Rewrite"
        description="Sequence-to-sequence rewriting is not supported by this Antfarm release."
      />
      <AIWorkflowEmpty
        icon={<CircleSlash2 className="size-5" />}
        title="Rewrite is unavailable"
        description="This route is kept so existing bookmarks remain understandable. Choose a supported model workflow from the AI workspace."
        action={
          <Button type="button" variant="outline" onClick={() => navigate("/inference/models")}>
            <ArrowLeft className="size-4" /> Back to models
          </Button>
        }
      />
    </AIWorkflowPage>
  );
}
