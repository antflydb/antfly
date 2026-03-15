# Antfly Quickstart Demo
# Usage: vhs quickstart.tape
# Requires: antfly binary in PATH, models pre-downloaded, swarm running on :8080

Output quickstart.gif

Set Shell "zsh"
Set FontSize 14
Set Width 1200
Set Height 600
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 50ms

Hide
Type `export PATH="/Users/ajroetker/go/src/github.com/antflydb/antfly:$PATH"`
Enter
Sleep 500ms
# Colored prompt + input text via zsh preexec/precmd
# Input text renders in lavender (183), output resets to default
Type `export PS1=$'\033[38;5;183m❯ '`
Enter
Type `export PS2=$'\033[38;5;183m  '`
Enter
Type `preexec() { printf "\033[0m"; }`
Enter
Sleep 500ms
Type `antfly table drop -t wikipedia 2>/dev/null; true`
Enter
Sleep 1s
Type `clear`
Enter
Sleep 500ms
Show

# --- Create table with text + image indexes ---

Type@75ms `antfly table create -t wikipedia -f wikipedia-config.json`
Enter
Sleep 2s

# --- Load data ---

Type@75ms `antfly load -t wikipedia -f wiki-articles.json --id-field title`
Enter
Sleep 2s

# Wait for embeddings to be indexed (hidden from gif)
Hide
Sleep 300s
Show

# --- Full-text search ---

Type@60ms `antfly query -t wikipedia \`
Enter
Type@60ms `  --full-text-search 'body:Einstein' \`
Enter
Type@60ms `  --fields "title,url" --limit 5`
Enter
Sleep 2s

# --- Semantic search ---

Type@60ms `antfly query -t wikipedia \`
Enter
Type@60ms `  --semantic-search "anatomy and physiology" \`
Enter
Type@60ms `  -i title_body --fields "title,url" --limit 5`
Enter
Sleep 2s

# --- Hybrid search with reranker ---

Type@60ms `antfly query -t wikipedia \`
Enter
Type@60ms `  --full-text-search 'body:Einstein' \`
Enter
Type@60ms `  --semantic-search "theory of relativity and physics" \`
Enter
Type@60ms `  -i title_body --fields "title,url" --limit 10 \`
Enter
Type@60ms `  --reranker '{"provider": "termite", "model": "mixedbread-ai/mxbai-rerank-base-v1", "field": "body"}' \`
Enter
Type@60ms `  --pruner '{"min_score_ratio": 0.01}'`
Enter
Sleep 5s

# --- RAG ---

Type@60ms `antfly agents retrieval -t wikipedia \`
Enter
Type@60ms `  --semantic-search "What are the major events in Korean history?" \`
Enter
Type@60ms `  -i title_body --fields "title,body" --limit 5 \`
Enter
Type@60ms `  --generator '{"provider": "gemini", "model": "gemini-2.5-flash"}' \`
Enter
Type@60ms `  --max-context-tokens 2048 \`
Enter
Type@60ms `  --classify --reasoning --generate --followup`
Enter
Wait@120s /❯/

Sleep 8s
