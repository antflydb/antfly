#!/bin/bash

# Start Ollama in the background.
/bin/ollama serve &
# Record Process ID.
pid=$!

# Pause for Ollama to start.
sleep 5

echo "🔴 Retrieve ALL-MINILM model..."
ollama pull all-minilm
echo "🟢 Done!"

# echo "🔴 Retrieve LLAVA model..."
# ollama pull llava
# echo "🟢 Done!"

# Wait for Ollama process to finish.
wait $pid
