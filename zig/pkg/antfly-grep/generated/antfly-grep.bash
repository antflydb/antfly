_antfly_grep() {
 local candidate cur="${COMP_WORDS[COMP_CWORD]}" prev="${COMP_WORDS[COMP_CWORD-1]}" prefix=''
 COMPREPLY=()
 if [[ $cur == --*=* ]]; then prev=${cur%%=*}; prefix=$prev=; cur=${cur#*=}; fi
 case $prev in
 --after-context) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --agent) COMPREPLY=( $(compgen -W 'codex claude-code opencode' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --ask) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --backend) COMPREPLY=( $(compgen -W 'http antfly typesafe local' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --base-url) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --batch-size) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --before-context) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --cache-dir) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -d -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --cache-ttl) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --chunk-lines) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --color) COMPREPLY=( $(compgen -W 'auto always never ansi' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --colors) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --concurrency) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --context) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --context-lines) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --context-separator) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --decision-context) COMPREPLY=( $(compgen -W 'legacy shared-v1' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --dfa-size-limit) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --dictionary) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --dictionary-format) COMPREPLY=( $(compgen -W 'auto raw brotli brotli-google' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --encoding) COMPREPLY=( $(compgen -W 'auto none utf-8 utf-16le utf-16be utf-32le utf-32be windows-1252 shift_jis euc-jp euc-kr gb18030 big5 iso-2022-jp' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --endpoint) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --engine) COMPREPLY=( $(compgen -W 'default auto pcre2' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --exclude) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --field-context-separator) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --field-match-separator) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --file) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --file-threshold) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --filter) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --generate) COMPREPLY=( $(compgen -W 'man complete-bash complete-zsh complete-fish complete-powershell' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --glob) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --hostname-bin) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --hyperlink-format) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --iglob) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --ignore-file) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --library) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-columns) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-count) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-decompression-window) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-depth) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-dictionary-size) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-file-size) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-files) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-filesize) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-index-documents) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-output-bytes) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-pending-output) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-regions) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-requests) COMPREPLY=( $(compgen -W 'unlimited' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-result-files) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --max-source-bytes) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --model) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --model-revision) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --models-dir) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -d -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --not) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --only) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --path-separator) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --pcre2-match-limit) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --pre) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --pre-glob) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --provider) COMPREPLY=( $(compgen -W 'antfly typesafe vercel openrouter opencode local custom' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --query) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --regex-size-limit) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --regexp) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --replace) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --retries) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --sort) COMPREPLY=( $(compgen -W 'none path modified accessed created' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --sortr) COMPREPLY=( $(compgen -W 'none path modified accessed created' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --table) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --table-config) while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --table-filter) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --threads) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --threshold) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --timeout-ms) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --type) COMPREPLY=( $(compgen -W 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd ' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --type-add) COMPREPLY=(); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --type-clear) COMPREPLY=( $(compgen -W 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd ' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --type-not) COMPREPLY=( $(compgen -W 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd ' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 --unit) COMPREPLY=( $(compgen -W 'line block' -- "$cur") ); if [[ -n $prefix ]]; then COMPREPLY=( "${COMPREPLY[@]/#/$prefix}" ); fi; return;;
 esac
 case ${COMP_WORDS[1]} in cache) COMPREPLY=( $(compgen -W 'clear' -- "$cur") ); return;; esac
 local opts='--after-context --agent --ask --auto-hybrid-regex --backend --base-url --batch-size --before-context --binary --block-buffered --broad --byte-offset --cache-dir --cache-ttl --cached-only --case-sensitive --chunk-lines --color --colors --column --concurrency --context --context-lines --context-separator --count --count-matches --crlf --debug --decision-context --dfa-size-limit --dictionary --dictionary-format --encoding --endpoint --engine --exclude --field-context-separator --field-match-separator --file --file-threshold --files --files-with-matches --files-without-match --filter --fixed-strings --follow --generate --glob --glob-case-insensitive --global --heading --help --hidden --hostname-bin --hyperlink-format --iglob --ignore --ignore-case --ignore-dot --ignore-exclude --ignore-file --ignore-file-case-insensitive --ignore-files --ignore-global --ignore-parent --ignore-vcs --include-dependencies --include-sensitive --include-zero --invert-match --json --library --line-buffered --line-number --line-regexp --max-columns --max-columns-preview --max-count --max-decompression-window --max-depth --max-dictionary-size --max-file-size --max-files --max-filesize --max-index-documents --max-output-bytes --max-pending-output --max-regions --max-requests --max-result-files --max-source-bytes --messages --mmap --model --model-revision --models-dir --multiline --multiline-dotall --no-cache --no-config --no-context-separator --no-filename --no-follow --no-heading --no-hidden --no-ignore --no-ignore-case --no-ignore-dot --no-ignore-exclude --no-ignore-files --no-ignore-global --no-ignore-messages --no-ignore-parent --no-ignore-vcs --no-line-number --no-messages --no-mmap --no-pcre2 --no-pcre2-unicode --no-pre --no-rank --no-refine --no-require-git --no-search-zip --no-sort-files --no-unicode --not --null --null-data --one-file-system --only --only-matching --passthru --path-separator --pcre2 --pcre2-match-limit --pcre2-unicode --pcre2-version --pre --pre-glob --pretty --provider --query --quiet --refine --regex-size-limit --regexp --replace --require-git --retries --search-zip --smart-case --sort --sort-files --sortr --stats --stdin --stop-on-nonmatch --table --table-config --table-filter --text --threads --threshold --timeout-ms --trace --triage --trim --type --type-add --type-clear --type-list --type-not --unicode --unit --unrestricted --version --vimgrep --with-filename --word-regexp --yes ask files auth doctor skill cache'
 if [[ $cur == -* || $COMP_CWORD == 1 ]]; then COMPREPLY=( $(compgen -W "$opts" -- "$cur") ); fi
 if [[ ${#COMPREPLY[@]} == 0 && $cur != -* ]]; then while IFS= read -r candidate; do COMPREPLY+=("$candidate"); done < <(compgen -f -- "$cur"); fi
}
complete -o filenames -F _antfly_grep antfly-grep
