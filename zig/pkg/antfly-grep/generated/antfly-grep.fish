complete -c antfly-grep -n '__fish_use_subcommand' -a 'ask files auth doctor skill cache'
complete -c antfly-grep -n '__fish_seen_subcommand_from cache' -a clear
complete -c antfly-grep -l after-context -r
complete -c antfly-grep -l agent -r -f -a 'codex claude-code opencode'
complete -c antfly-grep -l ask -r
complete -c antfly-grep -l auto-hybrid-regex
complete -c antfly-grep -l backend -r -f -a 'http antfly typesafe local'
complete -c antfly-grep -l base-url -r
complete -c antfly-grep -l batch-size -r
complete -c antfly-grep -l before-context -r
complete -c antfly-grep -l binary
complete -c antfly-grep -l block-buffered
complete -c antfly-grep -l broad
complete -c antfly-grep -l byte-offset
complete -c antfly-grep -l cache-dir -r -a '(__fish_complete_directories)'
complete -c antfly-grep -l cache-ttl -r
complete -c antfly-grep -l case-sensitive
complete -c antfly-grep -l chunk-lines -r
complete -c antfly-grep -l color -r -f -a 'auto always never ansi'
complete -c antfly-grep -l colors -r
complete -c antfly-grep -l column
complete -c antfly-grep -l concurrency -r
complete -c antfly-grep -l context -r
complete -c antfly-grep -l context-lines -r
complete -c antfly-grep -l context-separator -r
complete -c antfly-grep -l count
complete -c antfly-grep -l count-matches
complete -c antfly-grep -l crlf
complete -c antfly-grep -l debug
complete -c antfly-grep -l decision-context -r -f -a 'legacy shared-v1'
complete -c antfly-grep -l dfa-size-limit -r
complete -c antfly-grep -l dictionary -r
complete -c antfly-grep -l dictionary-format -r -f -a 'auto raw brotli brotli-google'
complete -c antfly-grep -l encoding -r -f -a 'auto none utf-8 utf-16le utf-16be utf-32le utf-32be windows-1252 shift_jis euc-jp euc-kr gb18030 big5 iso-2022-jp'
complete -c antfly-grep -l endpoint -r
complete -c antfly-grep -l engine -r -f -a 'default auto pcre2'
complete -c antfly-grep -l exclude -r
complete -c antfly-grep -l field-context-separator -r
complete -c antfly-grep -l field-match-separator -r
complete -c antfly-grep -l file -r
complete -c antfly-grep -l file-threshold -r
complete -c antfly-grep -l files
complete -c antfly-grep -l files-with-matches
complete -c antfly-grep -l files-without-match
complete -c antfly-grep -l filter -r
complete -c antfly-grep -l fixed-strings
complete -c antfly-grep -l follow
complete -c antfly-grep -l generate -r -f -a 'man complete-bash complete-zsh complete-fish complete-powershell'
complete -c antfly-grep -l glob -r
complete -c antfly-grep -l glob-case-insensitive
complete -c antfly-grep -l global
complete -c antfly-grep -l heading
complete -c antfly-grep -l help
complete -c antfly-grep -l hidden
complete -c antfly-grep -l hostname-bin -r
complete -c antfly-grep -l hyperlink-format -r
complete -c antfly-grep -l iglob -r
complete -c antfly-grep -l ignore
complete -c antfly-grep -l ignore-case
complete -c antfly-grep -l ignore-dot
complete -c antfly-grep -l ignore-exclude
complete -c antfly-grep -l ignore-file -r
complete -c antfly-grep -l ignore-file-case-insensitive
complete -c antfly-grep -l ignore-files
complete -c antfly-grep -l ignore-global
complete -c antfly-grep -l ignore-parent
complete -c antfly-grep -l ignore-vcs
complete -c antfly-grep -l include-dependencies
complete -c antfly-grep -l include-sensitive
complete -c antfly-grep -l include-zero
complete -c antfly-grep -l invert-match
complete -c antfly-grep -l json
complete -c antfly-grep -l library -r
complete -c antfly-grep -l line-buffered
complete -c antfly-grep -l line-number
complete -c antfly-grep -l line-regexp
complete -c antfly-grep -l max-columns -r
complete -c antfly-grep -l max-columns-preview
complete -c antfly-grep -l max-count -r
complete -c antfly-grep -l max-decompression-window -r
complete -c antfly-grep -l max-depth -r
complete -c antfly-grep -l max-dictionary-size -r
complete -c antfly-grep -l max-file-size -r
complete -c antfly-grep -l max-files -r
complete -c antfly-grep -l max-filesize -r
complete -c antfly-grep -l max-index-documents -r
complete -c antfly-grep -l max-output-bytes -r
complete -c antfly-grep -l max-pending-output -r
complete -c antfly-grep -l max-regions -r
complete -c antfly-grep -l max-requests -r -f -a 'unlimited'
complete -c antfly-grep -l max-result-files -r
complete -c antfly-grep -l max-source-bytes -r
complete -c antfly-grep -l messages
complete -c antfly-grep -l mmap
complete -c antfly-grep -l model -r
complete -c antfly-grep -l models-dir -r -a '(__fish_complete_directories)'
complete -c antfly-grep -l multiline
complete -c antfly-grep -l multiline-dotall
complete -c antfly-grep -l no-cache
complete -c antfly-grep -l no-config
complete -c antfly-grep -l no-context-separator
complete -c antfly-grep -l no-filename
complete -c antfly-grep -l no-follow
complete -c antfly-grep -l no-heading
complete -c antfly-grep -l no-hidden
complete -c antfly-grep -l no-ignore
complete -c antfly-grep -l no-ignore-case
complete -c antfly-grep -l no-ignore-dot
complete -c antfly-grep -l no-ignore-exclude
complete -c antfly-grep -l no-ignore-files
complete -c antfly-grep -l no-ignore-global
complete -c antfly-grep -l no-ignore-messages
complete -c antfly-grep -l no-ignore-parent
complete -c antfly-grep -l no-ignore-vcs
complete -c antfly-grep -l no-line-number
complete -c antfly-grep -l no-messages
complete -c antfly-grep -l no-mmap
complete -c antfly-grep -l no-pcre2
complete -c antfly-grep -l no-pcre2-unicode
complete -c antfly-grep -l no-pre
complete -c antfly-grep -l no-rank
complete -c antfly-grep -l no-refine
complete -c antfly-grep -l no-require-git
complete -c antfly-grep -l no-search-zip
complete -c antfly-grep -l no-sort-files
complete -c antfly-grep -l no-unicode
complete -c antfly-grep -l not -r
complete -c antfly-grep -l null
complete -c antfly-grep -l null-data
complete -c antfly-grep -l one-file-system
complete -c antfly-grep -l only -r
complete -c antfly-grep -l only-matching
complete -c antfly-grep -l passthru
complete -c antfly-grep -l path-separator -r
complete -c antfly-grep -l pcre2
complete -c antfly-grep -l pcre2-match-limit -r
complete -c antfly-grep -l pcre2-unicode
complete -c antfly-grep -l pcre2-version
complete -c antfly-grep -l pre -r
complete -c antfly-grep -l pre-glob -r
complete -c antfly-grep -l pretty
complete -c antfly-grep -l provider -r -f -a 'antfly typesafe vercel openrouter opencode local custom'
complete -c antfly-grep -l query -r
complete -c antfly-grep -l quiet
complete -c antfly-grep -l refine
complete -c antfly-grep -l regex-size-limit -r
complete -c antfly-grep -l regexp -r
complete -c antfly-grep -l replace -r
complete -c antfly-grep -l require-git
complete -c antfly-grep -l retries -r
complete -c antfly-grep -l search-zip
complete -c antfly-grep -l smart-case
complete -c antfly-grep -l sort -r -f -a 'none path modified accessed created'
complete -c antfly-grep -l sort-files
complete -c antfly-grep -l sortr -r -f -a 'none path modified accessed created'
complete -c antfly-grep -l stats
complete -c antfly-grep -l stdin
complete -c antfly-grep -l stop-on-nonmatch
complete -c antfly-grep -l table -r
complete -c antfly-grep -l table-config -r
complete -c antfly-grep -l table-filter -r
complete -c antfly-grep -l text
complete -c antfly-grep -l threads -r
complete -c antfly-grep -l threshold -r
complete -c antfly-grep -l timeout-ms -r
complete -c antfly-grep -l trace
complete -c antfly-grep -l triage
complete -c antfly-grep -l trim
complete -c antfly-grep -l type -r -f -a 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd '
complete -c antfly-grep -l type-add -r
complete -c antfly-grep -l type-clear -r -f -a 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd '
complete -c antfly-grep -l type-list
complete -c antfly-grep -l type-not -r -f -a 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd '
complete -c antfly-grep -l unicode
complete -c antfly-grep -l unit -r -f -a 'line block'
complete -c antfly-grep -l unrestricted
complete -c antfly-grep -l version
complete -c antfly-grep -l vimgrep
complete -c antfly-grep -l with-filename
complete -c antfly-grep -l word-regexp
complete -c antfly-grep -l yes
