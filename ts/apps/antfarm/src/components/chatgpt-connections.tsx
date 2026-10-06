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
      <div className="space-y-2 text-sm">
        <p>Connect from a terminal on the machine running your local Antfly server:</p>
        <code className="block">antfly connections login chatgpt</code>
        <p>Reload Antfarm after connecting, reconnecting, or signing out with the CLI.</p>
      </div>
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
        </div>
      ))}
      <div className="flex items-center gap-4">
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
