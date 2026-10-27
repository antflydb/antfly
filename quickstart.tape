# Wikipedia search demo. See scripts/quickstart/README.md for setup.
# Run from the repository root against a fresh, isolated local server.
# OpenAI generation is billed; embeddings, reranking and retrieval stay local.
Output .recordings/quickstart.mp4
Output .recordings/quickstart.txt

Set Shell "zsh"
Set FontSize 18
Set Width 1200
Set Height 640
Set Framerate 10
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 18ms
Hide
Type `setopt interactivecomments; export PATH="$(pwd):$PATH" ANTFLY_AGENT_MODE=0 COLUMNS=96; export ANTFLY_URL="${ANTFLY_URL:-http://127.0.0.1:18089}"; unset ANTFLY_TOKEN ANTFLY_USERNAME ANTFLY_PASSWORD ANTFLY_DATABASE ANTFLY_NAMESPACE OPENAI_API_KEY`
Enter
Type `antfly() { command antfly "$@"; local demo_rc=$?; printf '%s %s %d\n' "$1" "$2" "$demo_rc" >> .recordings/quickstart-commands.log; (( demo_rc == 0 )) || exit "$demo_rc"; }`
Enter
Type `export PS1=$'\033[38;5;183m❯ '; export PS2='  '; preexec() { printf '\033[0m'; }`
Enter
Type `clear`
Enter
Sleep 500ms
Show
Type `# Wikipedia search - waits shortened for this demo`
Enter
Type `antfly table create -t wikipedia -f wikipedia-config.json`
Enter
Sleep 700ms
Hide
Wait@60s /❯/
Show
Sleep 2s
Ctrl+L
Type `antfly load -t wikipedia -f wiki-articles.jsonl \`
Enter
Type `  --id-field title --sync-level full_text`
Enter
Sleep 1s
Hide
Wait@180s /❯/
Show
Sleep 2s
Type `antfly sql --statement 'SELECT count(*) FROM wikipedia'`
Enter
Sleep 500ms
Hide
Wait@60s /❯/
Show
Sleep 2s
Ctrl+L
Type `# Indexing wait shortened; ready at 10% source coverage`
Enter
Type `antfly index wait -t wikipedia -i title_body \`
Enter
Type `  --until source-covered=10% --timeout 45m`
Enter
Sleep 2s
Hide
Wait@2700s /❯/
Show
Sleep 2s
Ctrl+L
Type `antfly query -t wikipedia \`
Enter
Type `  --full-text-search 'body:Einstein' \`
Enter
Type `  --fields title,url --limit 5`
Enter
Sleep 500ms
Hide
Wait@60s /❯/
Show
Sleep 4s
Ctrl+L
Type `antfly query -t wikipedia \`
Enter
Type `  --semantic-search 'anatomy and physiology' \`
Enter
Type `  -i title_body --fields title,url --limit 5`
Enter
Sleep 500ms
Hide
Wait@120s /❯/
Show
Sleep 4s
Ctrl+L
Type `antfly query -t wikipedia \`
Enter
Type `  --full-text-search 'body:Einstein' \`
Enter
Type `  --semantic-search 'theory of relativity and physics' \`
Enter
Type `  -i title_body --fields title,url --limit 5 \`
Enter
Type `  --reranker '{"provider":"antfly","model":"mixedbread-ai/mxbai-rerank-base-v1","field":"body"}' \`
Enter
Type `  --pruner '{"min_score_ratio":0.01}'`
Enter
Sleep 500ms
Hide
Wait@120s /❯/
Show
Sleep 5s
Ctrl+L
Type `# Grounded answer: OpenAI generation, local retrieval`
Enter
Type `antfly agents retrieval -t wikipedia \`
Enter
Type `  --full-text-search 'title:"Korean history textbook controversies"' \`
Enter
Type `  --prompt 'Why are Korean history textbooks controversial?' \`
Enter
Type `  --fields title,body --limit 3 \`
Enter
Type `  --generator "$(cat scripts/quickstart/generator.json)" \`
Enter
Type `  --max-context-tokens 2048 \`
Enter
Type `  --system-prompt 'Use only sources. Answer in two short sentences and cite source titles.' \`
Enter
Type `  --generate`
Enter
Sleep 1s
Hide
Wait@120s /❯/
Show
Sleep 9s
