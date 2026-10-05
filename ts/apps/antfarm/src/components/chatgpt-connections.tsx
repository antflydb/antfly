import { Button } from "@antfly/design-system";
import { useChatGPT } from "./chatgpt-provider";

export function ChatGPTConnections() {
  const chatgpt = useChatGPT();
  if (!chatgpt.supported) return null;
  return (
    <section className="border p-5 space-y-4">
      <div>
        <h2 className="font-aeonik text-lg">Use your ChatGPT plan</h2>
        <p className="text-sm text-muted-foreground">
          Power interactive chat and answers over your Antfly data. Prompts and retrieved context
          are sent to OpenAI. Usage counts toward your existing ChatGPT plan limits.
        </p>
      </div>
      {chatgpt.notice && (
        <p role="status" className="text-sm">
          {chatgpt.notice}
        </p>
      )}
      {chatgpt.error && (
        <p role="alert" className="text-sm text-destructive">
          {chatgpt.error}
        </p>
      )}
      {chatgpt.accounts.map((account) => (
        <div
          key={account.connection_id}
          className="flex flex-wrap items-center gap-3 border-t pt-3"
        >
          <div className="flex-1 min-w-0">
            <p className="text-sm truncate">{account.email || "ChatGPT account"}</p>
            <p className="text-xs text-muted-foreground truncate">
              {account.label} ·{" "}
              {account.connected
                ? account.plan_enabled
                  ? "Plan usage enabled"
                  : "Plan usage not enabled"
                : "Disconnected"}
            </p>
          </div>
          <Button
            size="sm"
            disabled={chatgpt.busy}
            onClick={() => void chatgpt.connect(account.connection_id)}
          >
            Continue with ChatGPT
          </Button>
          {account.connected && (
            <Button
              size="sm"
              variant="outline"
              disabled={chatgpt.busy}
              onClick={() => void chatgpt.disconnect(account.connection_id)}
            >
              Disconnect
            </Button>
          )}
        </div>
      ))}
      <div className="flex items-center gap-4">
        <Button disabled={chatgpt.busy} onClick={() => void chatgpt.connect()}>
          {chatgpt.busy ? "Connecting…" : "Continue with ChatGPT"}
        </Button>
        <a
          className="text-sm underline"
          href="https://chatgpt.com/settings/usage"
          target="_blank"
          rel="noopener noreferrer"
        >
          Manage usage
        </a>
      </div>
    </section>
  );
}
