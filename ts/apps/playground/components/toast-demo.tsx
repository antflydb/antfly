"use client";

import { Button } from "@antfly/design-system";
import { toast } from "sonner";

export function ToastDemo() {
  return (
    <div className="flex flex-wrap gap-3">
      <Button variant="outline" onClick={() => toast("Document indexed successfully.")}>
        Default
      </Button>
      <Button
        variant="outline"
        onClick={() =>
          toast.success("Cluster provisioned", {
            description: "3 shards in us-east-1.",
          })
        }
      >
        Success
      </Button>
      <Button variant="outline" onClick={() => toast.error("Shard unreachable. Retrying in 5 s.")}>
        Error
      </Button>
      <Button
        variant="outline"
        onClick={() =>
          toast("Rotate credentials?", {
            action: { label: "Confirm", onClick: () => undefined },
          })
        }
      >
        With action
      </Button>
    </div>
  );
}
