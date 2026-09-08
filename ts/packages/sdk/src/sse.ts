// Shared bounded parser: only blank-line-terminated frames are dispatched.
const MAX_SSE_LINE_BYTES = 16 << 20;
const MAX_SSE_EVENT_CHARS = 16 << 20;

interface SSEFrame {
  event: string;
  data: string;
}

export async function* parseSSEFrames(
  body: ReadableStream<Uint8Array>,
  label = "Generation"
): AsyncGenerator<SSEFrame, void, void> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let event = "";
  let data: string[] = [];
  let dataChars = 0;
  let lineBytes = 0;
  let complete = false;
  let skipLF = false;

  try {
    while (true) {
      const result = await reader.read();
      if (!result.done && result.value.byteLength > MAX_SSE_LINE_BYTES) {
        throw new Error(`${label} SSE chunk exceeded ${MAX_SSE_LINE_BYTES} bytes`);
      }
      if (!result.done) {
        for (const byte of result.value) {
          if (byte === 0x0a || byte === 0x0d) {
            lineBytes = 0;
          } else {
            lineBytes += 1;
            if (lineBytes > MAX_SSE_LINE_BYTES) {
              throw new Error(`${label} SSE line exceeded ${MAX_SSE_LINE_BYTES} bytes`);
            }
          }
        }
      }
      try {
        let decoded = result.done
          ? decoder.decode()
          : decoder.decode(result.value, { stream: true });
        if (decoded.length > 0) {
          if (skipLF && decoded.startsWith("\n")) decoded = decoded.slice(1);
          skipLF = decoded.endsWith("\r");
          buffer += decoded.replace(/\r\n?/g, "\n");
        }
      } catch {
        throw new Error(`${label} stream contained invalid UTF-8`);
      }

      let newline = buffer.indexOf("\n");
      while (newline !== -1) {
        let line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);

        if (line === "") {
          if (data.length > 0) yield { event, data: data.join("\n") };
          event = "";
          data = [];
          dataChars = 0;
        } else if (!line.startsWith(":")) {
          const colon = line.indexOf(":");
          const field = colon === -1 ? line : line.slice(0, colon);
          let value = colon === -1 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          if (field === "event") event = value;
          if (field === "data") {
            dataChars += (data.length > 0 ? 1 : 0) + value.length;
            if (dataChars > MAX_SSE_EVENT_CHARS) {
              throw new Error(`${label} SSE event exceeded ${MAX_SSE_EVENT_CHARS} characters`);
            }
            data.push(value);
          }
        }
        newline = buffer.indexOf("\n");
      }

      if (buffer.length > MAX_SSE_EVENT_CHARS) {
        throw new Error(`${label} SSE line exceeded ${MAX_SSE_EVENT_CHARS} characters`);
      }
      if (result.done) {
        // EOF is not an SSE event delimiter; never synthesize a terminal frame.
        if (buffer.length > 0 || data.length > 0 || event !== "") {
          throw new Error(`${label} stream ended with an incomplete SSE event`);
        }
        complete = true;
        return;
      }
    }
  } finally {
    if (!complete) {
      try {
        await reader.cancel();
      } catch {
        // Preserve the parse, consumer, or abort error that ended iteration.
      }
    }
    reader.releaseLock();
  }
}
