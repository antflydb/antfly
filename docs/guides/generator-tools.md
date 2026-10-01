# Generator tool support

Server-side agents support function tools through the Antfly, OpenAI, OpenRouter,
Ollama, Gemini, and Vertex generator adapters. Choose a model that supports
function calling; adapter support does not guarantee that every model does.

OpenRouter and Ollama use the OpenAI-compatible tool protocol. Gemini and Vertex
translate function definitions, tool choices, calls, and results to Google's
`generateContent` protocol. Google response parts, including thought signatures,
are retained for later turns and count toward the agent's context limit. Parallel
Google tool results are sent together, and calls without upstream IDs receive
internal IDs that remain distinct across turns.

Antfly's embedded runtime must expose its JSON generation callback to use tools.
Legacy text-only callbacks cannot preserve tool calls and results. The mock
provider is not a tool-capable server-side agent backend.
