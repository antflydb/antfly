Register-ArgumentCompleter -Native -CommandName antfly-grep -ScriptBlock { param($wordToComplete, $commandAst, $cursorPosition)
 $elements=@($commandAst.CommandElements | Where-Object { $_.Extent.EndOffset -le $cursorPosition }); $previousIndex=$elements.Count-1; if($wordToComplete -ne ''){$previousIndex--}; $prev=if($previousIndex -ge 1){$elements[$previousIndex].Extent.Text}else{''}; $prefix=''
 if($wordToComplete -match '^(--[^=]+)=(.*)$'){$prev=$Matches[1];$prefix=$prev+'=';$wordToComplete=$Matches[2]}
 $choices=switch($prev){
 '--after-context' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--agent' { 'codex claude-code opencode'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--ask' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--backend' { 'http antfly typesafe local'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--base-url' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--batch-size' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--before-context' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--cache-dir' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--cache-ttl' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--chunk-lines' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--color' { 'auto always never ansi'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--colors' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--concurrency' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--context' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--context-lines' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--context-separator' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--decision-context' { 'legacy shared-v1'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--dfa-size-limit' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--dictionary' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--dictionary-format' { 'auto raw brotli brotli-google'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--encoding' { 'auto none utf-8 utf-16le utf-16be utf-32le utf-32be windows-1252 shift_jis euc-jp euc-kr gb18030 big5 iso-2022-jp'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--endpoint' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--engine' { 'default auto pcre2'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--exclude' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--field-context-separator' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--field-match-separator' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--file' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--file-threshold' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--filter' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--generate' { 'man complete-bash complete-zsh complete-fish complete-powershell'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--glob' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--hostname-bin' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--hyperlink-format' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--iglob' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--ignore-file' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--library' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-columns' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-count' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-decompression-window' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-depth' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-dictionary-size' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-file-size' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-files' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-filesize' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-index-documents' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-output-bytes' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-pending-output' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-regions' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-requests' { 'unlimited'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-result-files' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--max-source-bytes' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--model' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--models-dir' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--not' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--only' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--path-separator' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--pcre2-match-limit' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--pre' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--pre-glob' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--provider' { 'antfly typesafe vercel openrouter opencode local custom'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--query' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--regex-size-limit' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--regexp' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--replace' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--retries' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--sort' { 'none path modified accessed created'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--sortr' { 'none path modified accessed created'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--table' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--table-config' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--table-filter' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--threads' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--threshold' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--timeout-ms' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--type' { 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd '.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--type-add' { ''.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--type-clear' { 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd '.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--type-not' { 'ada agda aidl alire amake asciidoc asm asp ats avro awk bat batch bazel bitbake boxlang brotli buildstream bzip2 c cabal candid carp cbor ceylon cfml clojure cmake cmd cml coffeescript config container coq cpp creole crystal cs csharp cshtml csproj css csv cuda cython d dart devicetree dhall diff dita docker dockercompose dts dvc ebuild edn elisp elixir elm erb erlang fennel fidl fish flatbuffers fortran fsharp fut gap gdscript gleam gn go gprbuild gradle graphql groovy gzip h haml hare haskell hbs hs html hurl hy idris janet java jinja jl js json jsonl julia jupyter k kconfig kotlin lean less license lilypond lisp llvm lock log lua lz4 lzma m4 make mako man markdown matlab md meson minified mint mk ml mojo motoko msbuild nim nix objc objcpp ocaml org pants pascal pdf perl php pkgbuild po pod postscript prolog proto protobuf ps puppet purs py python qmake qml qrc qui r racket raku rdoc readme reasonml red rescript robot rocq rst ruby rust sass scala scdoc seed7 sh slim smarty sml solidity soy spark spec sql ssa stylus sv svelte svg swift swig systemd taskpaper tcl tex texinfo textile tf thrift toml ts twig txt typescript typoscript typst usd v vala vb vcl verilog vhdl vim vimscript vue webidl wgsl wiki xml xz yacc yaml yang z zig zsh zstd '.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 '--unit' { 'line block'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }
 default { @('--after-context','--agent','--ask','--auto-hybrid-regex','--backend','--base-url','--batch-size','--before-context','--binary','--block-buffered','--broad','--byte-offset','--cache-dir','--cache-ttl','--case-sensitive','--chunk-lines','--color','--colors','--column','--concurrency','--context','--context-lines','--context-separator','--count','--count-matches','--crlf','--debug','--decision-context','--dfa-size-limit','--dictionary','--dictionary-format','--encoding','--endpoint','--engine','--exclude','--field-context-separator','--field-match-separator','--file','--file-threshold','--files','--files-with-matches','--files-without-match','--filter','--fixed-strings','--follow','--generate','--glob','--glob-case-insensitive','--global','--heading','--help','--hidden','--hostname-bin','--hyperlink-format','--iglob','--ignore','--ignore-case','--ignore-dot','--ignore-exclude','--ignore-file','--ignore-file-case-insensitive','--ignore-files','--ignore-global','--ignore-parent','--ignore-vcs','--include-dependencies','--include-sensitive','--include-zero','--invert-match','--json','--library','--line-buffered','--line-number','--line-regexp','--max-columns','--max-columns-preview','--max-count','--max-decompression-window','--max-depth','--max-dictionary-size','--max-file-size','--max-files','--max-filesize','--max-index-documents','--max-output-bytes','--max-pending-output','--max-regions','--max-requests','--max-result-files','--max-source-bytes','--messages','--mmap','--model','--models-dir','--multiline','--multiline-dotall','--no-cache','--no-config','--no-context-separator','--no-filename','--no-follow','--no-heading','--no-hidden','--no-ignore','--no-ignore-case','--no-ignore-dot','--no-ignore-exclude','--no-ignore-files','--no-ignore-global','--no-ignore-messages','--no-ignore-parent','--no-ignore-vcs','--no-line-number','--no-messages','--no-mmap','--no-pcre2','--no-pcre2-unicode','--no-pre','--no-rank','--no-refine','--no-require-git','--no-search-zip','--no-sort-files','--no-unicode','--not','--null','--null-data','--one-file-system','--only','--only-matching','--passthru','--path-separator','--pcre2','--pcre2-match-limit','--pcre2-unicode','--pcre2-version','--pre','--pre-glob','--pretty','--provider','--query','--quiet','--refine','--regex-size-limit','--regexp','--replace','--require-git','--retries','--search-zip','--smart-case','--sort','--sort-files','--sortr','--stats','--stdin','--stop-on-nonmatch','--table','--table-config','--table-filter','--text','--threads','--threshold','--timeout-ms','--trace','--triage','--trim','--type','--type-add','--type-clear','--type-list','--type-not','--unicode','--unit','--unrestricted','--version','--vimgrep','--with-filename','--word-regexp','--yes','ask','files','auth','doctor','skill','cache') }
 }
 $choices | Where-Object { $_.StartsWith($wordToComplete,[System.StringComparison]::OrdinalIgnoreCase) } | ForEach-Object { [System.Management.Automation.CompletionResult]::new($prefix+$_,$_, 'ParameterValue',$_) }
}
