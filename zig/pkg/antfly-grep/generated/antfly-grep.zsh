#compdef antfly-grep
if [[ ${words[2]} == workspace && CURRENT == 3 ]]; then compadd serve; return; fi
_arguments -s \
 '--after-context[after-context]:value:' \
 '--agent[agent]:value:(codex claude-code opencode)' \
 '--ask[ask]:value:' \
 '--auto-hybrid-regex[auto-hybrid-regex]' \
 '--backend[backend]:value:(http antfly local)' \
 '--base-url[base-url]:value:' \
 '--batch-size[batch-size]:value:' \
 '--before-context[before-context]:value:' \
 '--binary[binary]' \
 '--block-buffered[block-buffered]' \
 '--broad[broad]' \
 '--byte-offset[byte-offset]' \
 '--cache-dir[cache-dir]:value:_files -/' \
 '--cache-ttl[cache-ttl]:value:' \
 '--cached-only[cached-only]' \
 '--case-sensitive[case-sensitive]' \
 '--chunk-lines[chunk-lines]:value:' \
 '--color[color]:value:(auto always never ansi)' \
 '--colors[colors]:value:' \
 '--column[column]' \
 '--concurrency[concurrency]:value:' \
 '--context[context]:value:' \
 '--context-lines[context-lines]:value:' \
 '--context-separator[context-separator]:value:' \
 '--count[count]' \
 '--count-matches[count-matches]' \
 '--crlf[crlf]' \
 '--debug[debug]' \
 '--decision-batch-wait-ms[decision-batch-wait-ms]:value:' \
 '--decision-concurrency[decision-concurrency]:value:' \
 '--decision-context[decision-context]:value:(legacy shared-v1)' \
 '--decision-transport[decision-transport]:value:(single antfly-batch)' \
 '--dfa-size-limit[dfa-size-limit]:value:' \
 '--dictionary[dictionary]:value:_files' \
 '--dictionary-format[dictionary-format]:value:(auto raw brotli brotli-google)' \
 '--encoding[encoding]:value:(auto none utf-8 utf-16le utf-16be utf-32le utf-32be windows-1252 shift_jis euc-jp euc-kr gb18030 big5 iso-2022-jp)' \
 '--endpoint[endpoint]:value:' \
 '--engine[engine]:value:(default auto pcre2)' \
 '--exclude[exclude]:value:' \
 '--field-context-separator[field-context-separator]:value:' \
 '--field-match-separator[field-match-separator]:value:' \
 '--file[file]:value:_files' \
 '--file-threshold[file-threshold]:value:' \
 '--files[files]' \
 '--files-with-matches[files-with-matches]' \
 '--files-without-match[files-without-match]' \
 '--filter[filter]:value:' \
 '--fixed-strings[fixed-strings]' \
 '--follow[follow]' \
 '--generate[generate]:value:(man complete-bash complete-zsh complete-fish complete-powershell)' \
 '--glob[glob]:value:' \
 '--glob-case-insensitive[glob-case-insensitive]' \
 '--global[global]' \
 '--heading[heading]' \
 '--help[help]' \
 '--hidden[hidden]' \
 '--hostname-bin[hostname-bin]:value:' \
 '--hyperlink-format[hyperlink-format]:value:' \
 '--iglob[iglob]:value:' \
 '--ignore[ignore]' \
 '--ignore-case[ignore-case]' \
 '--ignore-dot[ignore-dot]' \
 '--ignore-exclude[ignore-exclude]' \
 '--ignore-file[ignore-file]:value:_files' \
 '--ignore-file-case-insensitive[ignore-file-case-insensitive]' \
 '--ignore-files[ignore-files]' \
 '--ignore-global[ignore-global]' \
 '--ignore-parent[ignore-parent]' \
 '--ignore-vcs[ignore-vcs]' \
 '--include-dependencies[include-dependencies]' \
 '--include-sensitive[include-sensitive]' \
 '--include-zero[include-zero]' \
 '--invert-match[invert-match]' \
 '--json[json]' \
 '--json-compact[json-compact]' \
 '--library[library]:value:_files' \
 '--line-buffered[line-buffered]' \
 '--line-number[line-number]' \
 '--line-regexp[line-regexp]' \
 '--max-columns[max-columns]:value:' \
 '--max-columns-preview[max-columns-preview]' \
 '--max-count[max-count]:value:' \
 '--max-decompression-window[max-decompression-window]:value:' \
 '--max-depth[max-depth]:value:' \
 '--max-dictionary-size[max-dictionary-size]:value:' \
 '--max-file-size[max-file-size]:value:' \
 '--max-files[max-files]:value:' \
 '--max-filesize[max-filesize]:value:' \
 '--max-index-documents[max-index-documents]:value:' \
 '--max-output-bytes[max-output-bytes]:value:' \
 '--max-pending-output[max-pending-output]:value:' \
 '--max-regions[max-regions]:value:' \
 '--max-requests[max-requests]:value:(unlimited)' \
 '--max-result-files[max-result-files]:value:' \
 '--max-source-bytes[max-source-bytes]:value:' \
 '--max-spill-bytes[max-spill-bytes]:value:' \
 '--messages[messages]' \
 '--mmap[mmap]' \
 '--model[model]:value:' \
 '--model-revision[model-revision]:value:' \
 '--models-dir[models-dir]:value:_files -/' \
 '--multiline[multiline]' \
 '--multiline-dotall[multiline-dotall]' \
 '--no-cache[no-cache]' \
 '--no-config[no-config]' \
 '--no-context-separator[no-context-separator]' \
 '--no-filename[no-filename]' \
 '--no-follow[no-follow]' \
 '--no-heading[no-heading]' \
 '--no-hidden[no-hidden]' \
 '--no-ignore[no-ignore]' \
 '--no-ignore-case[no-ignore-case]' \
 '--no-ignore-dot[no-ignore-dot]' \
 '--no-ignore-exclude[no-ignore-exclude]' \
 '--no-ignore-files[no-ignore-files]' \
 '--no-ignore-global[no-ignore-global]' \
 '--no-ignore-messages[no-ignore-messages]' \
 '--no-ignore-parent[no-ignore-parent]' \
 '--no-ignore-vcs[no-ignore-vcs]' \
 '--no-line-number[no-line-number]' \
 '--no-messages[no-messages]' \
 '--no-mmap[no-mmap]' \
 '--no-pcre2[no-pcre2]' \
 '--no-pcre2-unicode[no-pcre2-unicode]' \
 '--no-pre[no-pre]' \
 '--no-rank[no-rank]' \
 '--no-refine[no-refine]' \
 '--no-require-git[no-require-git]' \
 '--no-search-zip[no-search-zip]' \
 '--no-sort-files[no-sort-files]' \
 '--no-unicode[no-unicode]' \
 '--not[not]:value:' \
 '--null[null]' \
 '--null-data[null-data]' \
 '--one-file-system[one-file-system]' \
 '--only[only]:value:' \
 '--only-matching[only-matching]' \
 '--passthru[passthru]' \
 '--path-separator[path-separator]:value:' \
 '--pcre2[pcre2]' \
 '--pcre2-match-limit[pcre2-match-limit]:value:' \
 '--pcre2-unicode[pcre2-unicode]' \
 '--pcre2-version[pcre2-version]' \
 '--pre[pre]:value:_files' \
 '--pre-glob[pre-glob]:value:' \
 '--pretty[pretty]' \
 '--provider[provider]:value:(antfly openai local custom)' \
 '--query[query]:value:' \
 '--quiet[quiet]' \
 '--refine[refine]' \
 '--regex-size-limit[regex-size-limit]:value:' \
 '--regexp[regexp]:value:' \
 '--replace[replace]:value:' \
 '--require-git[require-git]' \
 '--retries[retries]:value:' \
 '--search-zip[search-zip]' \
 '--smart-case[smart-case]' \
 '--sort[sort]:value:(none path modified accessed created)' \
 '--sort-files[sort-files]' \
 '--sortr[sortr]:value:(none path modified accessed created)' \
 '--stats[stats]' \
 '--stdin[stdin]' \
 '--stop-on-nonmatch[stop-on-nonmatch]' \
 '--table[table]:value:' \
 '--table-config[table-config]:value:_files' \
 '--table-filter[table-filter]:value:' \
 '--text[text]' \
 '--threads[threads]:value:' \
 '--threshold[threshold]:value:' \
 '--timeout-ms[timeout-ms]:value:' \
 '--trace[trace]' \
 '--triage[triage]' \
 '--trim[trim]' \
 '--type[type]:value:(ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd )' \
 '--type-add[type-add]:value:' \
 '--type-clear[type-clear]:value:(ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd )' \
 '--type-list[type-list]' \
 '--type-not[type-not]:value:(ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd )' \
 '--unicode[unicode]' \
 '--unit[unit]:value:(line block)' \
 '--unrestricted[unrestricted]' \
 '--version[version]' \
 '--vimgrep[vimgrep]' \
 '--with-filename[with-filename]' \
 '--word-regexp[word-regexp]' \
 '--workspace-max-files[workspace-max-files]:value:' \
 '--workspace-memory-limit[workspace-memory-limit]:value:' \
 '--workspace-parallel-open[workspace-parallel-open]' \
 '--workspace-watch[workspace-watch]' \
 '--workspace-watch-limit[workspace-watch-limit]:value:' \
 '--workspace-watch-memory-limit[workspace-watch-memory-limit]:value:' \
 '--yes[yes]' \
 '1:command or pattern:(ask files auth doctor skill cache workspace)' '*:path:_files'
